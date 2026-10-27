package com.cloudea.yangbase.common.metrics

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.DoubleAdder
import java.util.concurrent.atomic.LongAdder

/**
 * 指标原语与注册表（设计文档 §12.5）。
 *
 * ## 为什么是自研的，而不是 Micrometer / Prometheus 客户端
 *
 * 三者都要引入外部依赖，而本项目刻意保持依赖精简（CLAUDE.md「编码约定」）。
 * 我们需要的只是「几个线程安全的计数 + 按 Prometheus 文本格式导出」，
 * 这不到 200 行。引入 Micrometer 会顺带拖进它的注册表抽象、标签层与
 * 导出器插件体系——用一个框架解决一个 200 行的问题，代价是依赖面与
 * 版本升级面都变大。
 *
 * ## 前缀为什么是 `yb_` 而不是 `yangbase_`
 *
 * 与设计文档 §12.5 的指标名逐字一致。文档里的名字是评审时确认过的，
 * 改动它们会让「文档里说的指标」与「实际暴露的指标」对不上，
 * 而这份文档的价值恰恰在于它可以被逐条核对。
 *
 * ## 热路径开销
 *
 * 计数器是 `LongAdder`，直方图是 `LongAdder` 数组加一次线性扫描。
 * **热路径上不分配任何对象**：调用方把算好的数值传进来，`observe(v: Double)`
 * 与 `inc()` 都不建对象、不加锁。见 [Metrics] 的用法。
 */
class Counter internal constructor(
    val name: String,
    val labels: List<Pair<String, String>> = emptyList(),
) {
    private val adder = LongAdder()

    fun inc() = adder.increment()

    fun add(n: Long) = adder.add(n)

    fun value(): Long = adder.sum()
}

/**
 * 固定分桶直方图。
 *
 * 桶上界在注册时确定，之后不再变化——动态桶会让「两个时刻的 p99」不可比，
 * 也会让观测路径需要分配。观测值落在第一个 `v <= bounds[i]` 的桶里。
 */
class Histogram internal constructor(
    val name: String,
    val bounds: DoubleArray,
    val labels: List<Pair<String, String>> = emptyList(),
) {
    /** 最后一格是 `+Inf`。 */
    private val buckets = Array(bounds.size + 1) { LongAdder() }
    private val countAdder = LongAdder()
    private val sumAdder = DoubleAdder()

    fun observe(v: Double) {
        var i = 0
        while (i < bounds.size && v > bounds[i]) i++
        buckets[i].increment()
        countAdder.increment()
        sumAdder.add(v)
    }

    fun count(): Long = countAdder.sum()

    fun sum(): Double = sumAdder.sum()

    /** 累积计数（Prometheus 的 `_bucket` 语义是 `le`，即累积）。 */
    fun cumulative(): LongArray {
        val out = LongArray(buckets.size)
        var acc = 0L
        for (i in buckets.indices) {
            acc += buckets[i].sum()
            out[i] = acc
        }
        return out
    }

    fun reset() {
        buckets.forEach { it.reset() }
        countAdder.reset()
        sumAdder.reset()
    }
}

/** 一个不持有状态、每次抓取时求值的指标。用于「值在别处」的量（缓存、分片统计）。 */
class Gauge internal constructor(
    val name: String,
    val labels: List<Pair<String, String>>,
    private val supplier: () -> Double,
) {
    fun value(): Double = supplier()
}

enum class MetricType { COUNTER, GAUGE, HISTOGRAM }

/** 一个已经求出值的样本。抓取路径（冷路径）才有对象分配。 */
data class Sample(
    val name: String,
    val labels: List<Pair<String, String>>,
    val value: Double,
    val type: MetricType = MetricType.COUNTER,
)

/**
 * 指标注册表。线程安全；注册发生在启动期或有缓存的热路径（见 [counter]）。
 *
 * `render()` 输出 Prometheus 文本暴露格式：
 * ```
 * # TYPE yb_objectstore_requests_total counter
 * yb_objectstore_requests_total{op="get",cached="false"} 42
 * ```
 */
class MetricsRegistry {

    private val counters = ConcurrentHashMap<String, Counter>()
    private val histograms = ConcurrentHashMap<String, Histogram>()
    private val gauges = ConcurrentHashMap<String, Gauge>()
    private val help = ConcurrentHashMap<String, String>()
    private val collectors = CopyOnWriteArrayList<() -> Collection<Sample>>()

    fun help(name: String, text: String) {
        help[name] = text
    }

    fun counter(name: String, vararg labels: Pair<String, String>): Counter {
        val ls = labels.toList()
        return counters.computeIfAbsent(keyOf(name, ls)) { Counter(name, ls) }
    }

    fun histogram(name: String, bounds: DoubleArray, vararg labels: Pair<String, String>): Histogram {
        val ls = labels.toList()
        return histograms.computeIfAbsent(keyOf(name, ls)) { Histogram(name, bounds, ls) }
    }

    fun gauge(name: String, labels: List<Pair<String, String>>, supplier: () -> Double): Gauge =
        gauges.computeIfAbsent(keyOf(name, labels)) { Gauge(name, labels, supplier) }

    /**
     * 注册一个「一次产出多条样本」的采集器。
     *
     * 用于一次遍历就能算出多个指标的场景（例如遍历所有分片统计 `.idx` 常驻字节、
     * 或者把缓存命中折算成「避免掉的请求数」）。**采集器必须只读内存状态**——
     * 抓取 `/metrics` 绝不能产生对象存储请求（原则 P4）。
     */
    fun collector(fn: () -> Collection<Sample>) {
        collectors.add(fn)
    }

    /**
     * 全部样本。直方图展开成 `_bucket` / `_sum` / `_count`。
     *
     * **仅供测试与调试**——生产抓取走 [render]。
     */
    fun samples(): List<Sample> = families().values.flatten().sortedBy { it.name + labelKey(it.labels) }

    /**
     * 按**指标族**分组的样本。
     *
     * 直方图的 `_bucket` / `_sum` / `_count` 必须与基础名同族——否则暴露文件里会出现
     * `# TYPE foo_bucket histogram` 这种非标准声明，严格解析器会拒绝。
     */
    private fun families(): Map<String, MutableList<Sample>> {
        val byFamily = LinkedHashMap<String, MutableList<Sample>>()
        for (c in counters.values) {
            byFamily.getOrPut(c.name) { ArrayList() }
                .add(Sample(c.name, c.labels, c.value().toDouble(), MetricType.COUNTER))
        }
        for (g in gauges.values) {
            byFamily.getOrPut(g.name) { ArrayList() }.add(Sample(g.name, g.labels, g.value(), MetricType.GAUGE))
        }
        for (h in histograms.values) {
            val fam = byFamily.getOrPut(h.name) { ArrayList() }
            val cum = h.cumulative()
            for (i in cum.indices) {
                val le = if (i == h.bounds.size) "+Inf" else formatBound(h.bounds[i])
                fam.add(Sample("${h.name}_bucket", h.labels + ("le" to le), cum[i].toDouble(), MetricType.HISTOGRAM))
            }
            fam.add(Sample("${h.name}_sum", h.labels, h.sum(), MetricType.HISTOGRAM))
            fam.add(Sample("${h.name}_count", h.labels, h.count().toDouble(), MetricType.HISTOGRAM))
        }
        val histNames = histograms.values.map { it.name }.toSet()
        for (c in collectors) {
            for (s in runCatching { c() }.getOrDefault(emptyList())) {
                byFamily.getOrPut(familyOf(s.name, histNames)) { ArrayList() }.add(s)
            }
        }
        return byFamily
    }

    private fun familyOf(sampleName: String, histNames: Set<String>): String {
        for (suffix in listOf("_bucket", "_sum", "_count")) {
            if (sampleName.endsWith(suffix)) {
                val base = sampleName.removeSuffix(suffix)
                if (base in histNames) return base
            }
        }
        return sampleName
    }

    /** Prometheus 文本暴露格式。同一指标族的所有样本必须连续出现。 */
    fun render(): String {
        val byFamily = families()
        val sb = StringBuilder(4096)
        for (name in byFamily.keys.sorted()) {
            val samples = byFamily.getValue(name).sortedWith(compareBy({ it.name }, { labelKey(it.labels) }))
            val type = samples.firstOrNull()?.type ?: MetricType.GAUGE
            help[name]?.let { sb.append("# HELP ").append(name).append(' ').append(escapeHelp(it)).append('\n') }
            sb.append("# TYPE ").append(name).append(' ').append(typeWord(type)).append('\n')
            for (s in samples) {
                sb.append(s.name).append(formatLabels(s.labels)).append(' ')
                    .append(formatDouble(s.value)).append('\n')
            }
        }
        return sb.toString()
    }

    /**
     * 把所有计数归零。**仅供测试**——生产进程里 counter 是单调的，不该被重置。
     *
     * 刻意**不动** gauge 与 collector：它们是「值的引用」（读某个 `AtomicLong`、
     * 遍历某个引擎的状态），本身不持有累计值。把它们清掉只会让后续的抓取少了几行，
     * 却不会有任何数字变得正确。
     */
    fun reset() {
        counters.values.forEach { it.reset() }
        histograms.values.forEach { it.reset() }
    }

    private fun typeWord(t: MetricType) = when (t) {
        MetricType.COUNTER -> "counter"
        MetricType.GAUGE -> "gauge"
        MetricType.HISTOGRAM -> "histogram"
    }

    private fun keyOf(name: String, labels: List<Pair<String, String>>): String =
        if (labels.isEmpty()) name else name + "{" + labels.sortedBy { it.first }.joinToString(",") { "${it.first}=${it.second}" } + "}"

    private fun labelKey(labels: List<Pair<String, String>>): String =
        labels.sortedBy { it.first }.joinToString(",") { it.first }

    private fun formatLabels(labels: List<Pair<String, String>>): String =
        if (labels.isEmpty()) "" else labels.joinToString(",", "{", "}") { "${it.first}=\"${escapeLabel(it.second)}\"" }

    private fun escapeLabel(v: String): String {
        if (v.none { it == '\\' || it == '"' || it == '\n' }) return v
        val sb = StringBuilder(v.length + 8)
        for (ch in v) when (ch) {
            '\\' -> sb.append("\\\\")
            '"' -> sb.append("\\\"")
            '\n' -> sb.append("\\n")
            else -> sb.append(ch)
        }
        return sb.toString()
    }

    private fun escapeHelp(v: String): String = v.replace("\\", "\\\\").replace("\n", "\\n")

    companion object {
        /** 整数就按整数打印，小数按定点打印——避免 `1.0E-4` 这种 Prometheus 不接受的写法。 */
        fun formatDouble(v: Double): String = when {
            v.isNaN() -> "NaN"
            v == Double.POSITIVE_INFINITY -> "+Inf"
            v == Double.NEGATIVE_INFINITY -> "-Inf"
            v == Math.floor(v) && Math.abs(v) < 1e15 -> v.toLong().toString()
            else -> "%.6f".format(java.util.Locale.ROOT, v)
        }

        /** 桶上界：与 Prometheus 客户端的写法一致（`0.001` / `1` / `2.5`，不补零）。 */
        fun formatBound(v: Double): String {
            val s = "%.6f".format(java.util.Locale.ROOT, v)
            return if (s.contains('.')) s.trimEnd('0').trimEnd('.') else s
        }
    }
}

/** 对 `Counter` 的测试便利：直接重置（`LongAdder` 没有公开的 set）。 */
internal fun Counter.reset() {
    val v = value()
    if (v != 0L) add(-v)
}

// ── 全局注册表 ──────────────────────────────────────────────────────
//
// 为什么是全局单例而不是注入的依赖：
//
// 埋点必须落在**真正的调用点**上——WAL 写者、块缓存、compaction、GC。
// 其中 GC 的构造发生在 `Executor` 内部（本任务的约束下不能改动那个文件），
// 因此从一个地方把它「注入」进来是不可能的。要么全局，要么不埋。
// 全局单例在这里是**可观测性汇聚点**的常见做法（日志框架同理），
// 代价是测试之间共享计数——[MetricsRegistry.reset] 就是为它准备的。

/** 对象存储操作类别。与 [com.cloudea.yangbase.objectstore.ObjectStore] 的方法一一对应（条件写单列一档）。 */
enum class StoreOp(val wire: String) {
    GET("get"),
    GET_RANGE("get_range"),
    PUT("put"),
    PUT_CONDITIONAL("put_cond"),
    DELETE("delete"),
    DELETE_BATCH("delete_batch"),
    LIST("list"),
    HEAD("head"),
}

/** 查询类别（设计文档 §12.5 的 `type` 标签）。 */
enum class QueryType(val wire: String) {
    POINT("point"),
    RANGE("range"),
    SCAN("scan"),
    FULLTEXT("fulltext"),
    OTHER("other"),
}

/**
 * 直方图桶边界。
 *
 * **必须放在 [Metrics] 之外**：Kotlin 的顶层属性在首次访问时初始化，
 * 而对象内的属性按声明顺序初始化——如果把它们写在 `Metrics` 的末尾，
 * 前面那些用它们构造直方图的属性会读到未初始化的值（一个真实的坑）。
 */
private object Bounds {
    /** 对象存储：1ms–5s（S3 PUT p99 400–600ms，见设计文档 §14.1）。 */
    val OBJECTSTORE = doubleArrayOf(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)

    /** 提交延迟：用户感知的那条 SLO（p50 ≤ 50ms、p99 ≤ 500ms，见 §14.1）。 */
    val COMMIT = doubleArrayOf(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0)

    val QUERY =
        doubleArrayOf(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

    val BATCH = doubleArrayOf(1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0)

    /**
     * 每查询对象读取数。从 0 起分桶——「0 次 GET」是一个必须能被看见的类别
     * （`COUNT(*)` 的元数据路径与缓存命中的点查都在这一格）。
     */
    val OBJECTS = doubleArrayOf(0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0)
}

/**
 * 全局指标。所有埋点都走这里。
 *
 * 用法（热路径）：
 * ```
 * val t0 = System.nanoTime()
 * val data = delegate.get(key, range)
 * Metrics.onStoreRequest(StoreOp.GET, bytesOut = data.size.toLong(), nanos = System.nanoTime() - t0)
 * ```
 * 采集本身不引入任何对象存储请求，也**不产生跨线程同步**（`LongAdder` 按槽位分散写）。
 */
object Metrics {
    val registry = MetricsRegistry()

    // ── 对象存储 ──────────────────────────────────────────────────
    //
    // {cached} 维度的口径：一次**真正打到对象存储**的请求永远是 cached="false"；
    // cached="true" 表示「由缓存满足、因而没有发出的请求」——它由 yb-server 的
    // 采集器从块缓存/二级缓存的命中数折算出来（见 ServerMetrics）。
    // 这样过滤 `cached="false"` 得到的才是真实成本。

    private val requests: Array<Counter> = Array(StoreOp.entries.size) {
        registry.counter("yb_objectstore_requests_total", "op" to StoreOp.entries[it].wire, "cached" to "false")
    }
    private val bytes = Array(2) { dir ->
        Array(StoreOp.entries.size) {
            registry.counter(
                "yb_objectstore_bytes_total",
                "direction" to if (dir == 0) "in" else "out",
                "op" to StoreOp.entries[it].wire,
            )
        }
    }
    private val requestDuration: Array<Histogram> = Array(StoreOp.entries.size) {
        registry.histogram(
            "yb_objectstore_request_duration_seconds",
            Bounds.OBJECTSTORE,
            "op" to StoreOp.entries[it].wire,
        )
    }
    private val errors = ConcurrentHashMap<String, Counter>()

    // 重试层（L1 适配层，设计文档 §2.2）的计数。按需建键——重试是**失败路径**，
    // 不是热路径，这里的一次字符串拼接换来的是「不把 op×reason 的笛卡尔积
    // 在启动时全注册一遍」。成功路径上这两个 map 一次都不会被碰到。
    private val retries = ConcurrentHashMap<String, Counter>()
    private val retriesExhausted = ConcurrentHashMap<String, Counter>()

    init {
        registry.help(
            "yb_objectstore_requests_total",
            "对象存储请求数。cached=\"true\" 表示由缓存满足、未真正发出的请求（成本为 0）",
        )
        registry.help("yb_objectstore_bytes_total", "对象存储传输字节。direction=in 为写入/上行，out 为读出/下行")
        registry.help("yb_objectstore_request_duration_seconds", "对象存储单请求耗时，关注 p99")
        registry.help("yb_objectstore_errors_total", "对象存储调用抛出的异常数，code 为异常类型或 HTTP 状态")
        registry.help(
            "yb_objectstore_retries_total",
            "★ 重试次数。reason=transient（5xx/429/408/网络中断）| conflict（409 条件写并发冲突）。" +
                "持续升高说明后端在抖动，用户可见的尾延迟正在被它拉长",
        )
        registry.help(
            "yb_objectstore_retries_exhausted_total",
            "★ 重试耗尽后放弃的次数。**非零就是用户可见的失败**——重试层已经尽力，问题不在传输层",
        )
    }

    /** 一次对象存储请求。热路径：调用方传入已算好的数值，这里不做任何分配。 */
    fun onStoreRequest(op: StoreOp, bytesOut: Long = 0, bytesIn: Long = 0, nanos: Long = 0) {
        requests[op.ordinal].inc()
        if (bytesOut != 0L) bytes[1][op.ordinal].add(bytesOut)
        if (bytesIn != 0L) bytes[0][op.ordinal].add(bytesIn)
        if (nanos > 0) requestDuration[op.ordinal].observe(nanos / 1e9)
    }

    fun onStoreError(code: String) {
        errors.computeIfAbsent(code) { registry.counter("yb_objectstore_errors_total", "code" to it) }.inc()
    }

    /**
     * 一次重试（即：一次失败之后又要再发一次）。
     *
     * @param reason `transient`（5xx/429/408/网络中断）或 `conflict`（409 条件写并发冲突）。
     *   刻意不放 HTTP 状态码：那会让标签的取值随后端膨胀，而**调用方对这两类的反应
     *   本来就不一样**——transient 要担心后端健康，conflict 只说明并发度高。
     */
    fun onObjectStoreRetry(op: StoreOp, reason: String) {
        retries.computeIfAbsent("${op.wire}|$reason") {
            registry.counter("yb_objectstore_retries_total", "op" to op.wire, "reason" to reason)
        }.inc()
    }

    /** 重试预算用尽、把错误抛给调用方的那一刻。 */
    fun onObjectStoreRetryExhausted(op: StoreOp) {
        retriesExhausted.computeIfAbsent(op.wire) {
            registry.counter("yb_objectstore_retries_exhausted_total", "op" to op.wire)
        }.inc()
    }

    fun storeRequestCount(op: StoreOp): Long = requests[op.ordinal].value()

    /** 某类重试的累计次数。与 [storeRequestCount] 一样，是给测试与运维用的直读口。 */
    fun storeRetryCount(op: StoreOp, reason: String): Long =
        retries["${op.wire}|$reason"]?.value() ?: 0L

    fun storeRetryExhaustedCount(op: StoreOp): Long = retriesExhausted[op.wire]?.value() ?: 0L

    // ── 写入路径 ──────────────────────────────────────────────────

    /** ★ 用户感知的提交延迟（提交到持久化返回）。 */
    val commitDuration: Histogram = registry.histogram("yb_commit_duration_seconds", Bounds.COMMIT)

    /** 组提交每批的**批次数**——摊薄倍数的分布。 */
    val groupCommitBatchSize: Histogram = registry.histogram("yb_group_commit_batch_size", Bounds.BATCH)
    private val groupCommitBatches = registry.counter("yb_group_commit_batches_total")
    private val groupCommitGroups = registry.counter("yb_group_commit_groups_total")
    private val groupCommitOps = registry.counter("yb_group_commit_ops_total")
    val walObjects: Counter = registry.counter("yb_wal_objects_total")

    init {
        registry.help("yb_commit_duration_seconds", "一次事务提交的用户感知延迟（从投递到持久化返回）")
        registry.help("yb_group_commit_batch_size", "每个组提交批次里合并了多个事务（攒批效果）")
        registry.help("yb_wal_objects_total", "写出的 WAL 对象数。请求成本直接正比于它")
    }

    /**
     * 一次组提交（= 一次 PUT）。
     *
     * @param batches 这一批合并了多少个事务
     * @param ops 这一批合并了多少条行操作
     */
    fun onGroupCommit(batches: Int, ops: Long) {
        if (batches <= 0) return
        groupCommitBatchSize.observe(batches.toDouble())
        groupCommitBatches.add(batches.toLong())
        groupCommitGroups.inc()
        groupCommitOps.add(ops)
        walObjects.inc()
    }

    /** 组提交摊薄倍数 = 每个 PUT 摊掉了多少事务。1.0 表示完全没摊到（每次写都一次 PUT）。 */
    fun groupCommitDilution(): Double {
        val g = groupCommitGroups.value()
        return if (g == 0L) 0.0 else groupCommitBatches.value().toDouble() / g
    }

    // ── ASYNC 提交（commit_mode=async，RPO > 0）──────────────────
    //
    // 这组指标存在的唯一目的：让「已经 ACK 但还没落盘」这个状态**可观测**。
    // 没有它们，RPO > 0 就只是文档上的一句话——出了事没人看得到。

    private val asyncInflight = java.util.concurrent.atomic.AtomicLong(0)
    private val asyncPutFailures = registry.counter("yb_async_commit_put_failures_total")
    private val asyncLost = registry.counter("yb_async_commit_lost_total")
    private val rpoGt0 = java.util.concurrent.atomic.AtomicBoolean(false)

    init {
        registry.help(
            "yb_async_commit_inflight",
            "已向客户端 ACK、但 WAL 对象尚未落盘的批次数。仅 commit_mode=async 下非零；" +
                "它就是当前的失效窗口（进程此刻崩掉会丢这些）",
        )
        registry.help("yb_async_commit_put_failures_total", "async 模式落盘失败的重试次数（不丢数据，只是推迟）")
        registry.help(
            "yb_async_commit_lost_total",
            "★ async 模式被放弃的批次（关闭前重试仍失败）。非零 = 确实丢了已 ACK 的写入",
        )
        registry.help("yb_commit_mode_rpo_gt0", "1 表示当前部署的 commit_mode 是 async（RPO > 0）")
        registry.gauge("yb_async_commit_inflight", emptyList()) { asyncInflight.get().toDouble() }
        registry.gauge("yb_commit_mode_rpo_gt0", emptyList()) { if (rpoGt0.get()) 1.0 else 0.0 }
    }

    /** 由 `WalWriter` 在构造时上报：这是**部署级**语义，不是会话级的。 */
    fun setCommitModeRpoGt0(v: Boolean) = rpoGt0.set(v)

    /** 一批被 ACK（但还没落盘）。 */
    fun asyncCommitInFlight() = asyncInflight.incrementAndGet()

    /** 一批落盘成功。 */
    fun asyncCommitDurable() = asyncInflight.decrementAndGet()

    fun asyncCommitPutFailure() = asyncPutFailures.inc()

    fun asyncCommitLost() = asyncLost.inc()

    // ── 租约（§4.3 / §12.5）───────────────────────────────────────
    //
    // 只在 `--coord-mode objectstore` 下会被触发：单机模式用 SingleProcessLease，
    // 没有 CAS，也就没有相撞与丢失。所以这些计数在单机部署里恒为 0——
    // 一个恒为 0 的「丢租约」比一个压根不存在的指标更能说明问题。

    private val leaseAcquired = registry.counter("yb_lease_acquisitions_total", "result" to "ok")
    private val leaseContended = registry.counter("yb_lease_acquisitions_total", "result" to "contended")
    private val leaseFailed = registry.counter("yb_lease_acquisitions_total", "result" to "failed")
    private val leaseLost = registry.counter("yb_lease_lost_total")
    private val leaseRenewalFailed = registry.counter("yb_lease_renewal_failed_total")
    private val leaseRenewRetried = registry.counter("yb_lease_renewal_recovered_total")
    private val shardsEvicted = registry.counter("yb_shards_evicted_total")

    init {
        registry.help(
            "yb_lease_acquisitions_total",
            "取得分片写者租约的结果：ok / contended（CAS 相撞后重试）/ failed（重试耗尽）",
        )
        registry.help("yb_lease_lost_total", "★ 续期时发现租约已被别人接管。非零即告警（§4.3）")
        registry.help(
            "yb_lease_renewal_failed_total",
            "续期请求本身失败了（网络抖动、后端故障、凭证过期）。" +
                "与 yb_lease_lost_total 的区别：那个是**确认**被别人接管，这个是**不知道**——" +
                "两者都会让本节点停止写入，但排查方向完全不同（先看日志里的异常）",
        )
        registry.help(
            "yb_lease_renewal_recovered_total",
            "续期失败后**查询发现租约仍在自己手上**（典型：PUT 成功但响应丢了），" +
                "于是采纳刷新的凭据继续当写者、没有停写。非零说明网络在丢响应——" +
                "不是故障，但值得查（它以前会直接把分片误判成“被别人接管”）",
        )
    }

    fun onLeaseAcquired() = leaseAcquired.inc()

    /** CAS 相撞后重读重试。持续升高说明分片被抢得厉害（先去看分区键，§9.3）。 */
    fun onLeaseContended() = leaseContended.inc()

    fun onLeaseAcquireFailed() = leaseFailed.inc()

    /**
     * ★ 续期失败 = 我们的租约已经被别人抢走。
     *
     * 这是**必须告警**的事件：它意味着另一个 CN 正在写同一个分片，
     * 而本节点刚写的对象可能落在新旧两个 epoch 之间（§4.3 的 fencing）。
     */
    fun onLeaseLost() = leaseLost.inc()

    /**
     * 续期请求**本身**失败了（不是被别人抢走，而是根本没问出来）。
     *
     * 与 [onLeaseLost] 分开计数：前者要查网络与凭证，后者要查是不是有另一个写者。
     * 两者都会让本节点停止写入——但归因线索如果混在一个计数里，就只能靠猜。
     */
    fun onLeaseRenewalFailed() = leaseRenewalFailed.inc()

    /** 续期失败但确认租约仍在自己手上（响应丢了）：采纳新凭据继续，没停写。 */
    fun onLeaseRenewRetried() = leaseRenewRetried.inc()

    private val manifestPointerRaced = registry.counter("yb_manifest_pointer_raced_total")
    private val manifestOrphanSnapshots = registry.counter("yb_manifest_orphan_snapshots_total")

    init {
        registry.help(
            "yb_manifest_pointer_raced_total",
            "★ 弱化模式（后端无 ETag CAS）下，manifest 指针在提交期间被别人覆盖。" +
                "非零即说明这个后端上的指针切换确实没有原子性——只该出现在 OSS 这类后端上",
        )
        registry.help(
            "yb_manifest_orphan_snapshots_total",
            "接管时发现的孤儿 manifest 快照（写全了但没成为 current）。" +
                "良性来源是提交失败；非零时要结合日志判断是否发生过指针覆盖",
        )
    }

    /**
     * ★ 弱化模式下指针被别人覆盖（写后回读校验发现的）。
     *
     * 调用方会把它转成一个可重试的冲突，但这仍然是要看见的事件：
     * 它证明"这个后端上的指针切换确实没有原子性"，也就是理论风险已经发生过一次。
     */
    fun onManifestPointerRaced() = manifestPointerRaced.inc()

    /** 发现了多少个孤儿 manifest 快照（见 `ManifestStore.auditOrphanSnapshots`）。 */
    fun onManifestOrphanSnapshots(n: Int) { if (n > 0) manifestOrphanSnapshots.add(n.toLong()) }

    /** 空闲分片被淘汰的次数（见 `DatabaseEngine.startIdleEviction`）。 */
    fun onShardEvicted() = shardsEvicted.inc()

    // ── 事务 ──────────────────────────────────────────────────────

    private val txnCommit = registry.counter("yb_transactions_total", "result" to "commit")
    private val txnRollback = registry.counter("yb_transactions_total", "result" to "rollback")
    private val txnRetry = registry.counter("yb_transactions_total", "result" to "retry")
    private val txnSingleShard = registry.counter("yb_txn_single_shard_total")
    private val txn2pc = registry.counter("yb_txn_2pc_total")

    init {
        registry.help("yb_transactions_total", "事务计数。retry 为预留标签：本实现没有事务重试路径")
        registry.help("yb_txn_single_shard_total", "单分片事务数。它天然原子，一次 PUT 就是提交点")
        registry.help("yb_txn_2pc_total", "跨分片事务数（两阶段提交）。占比应远低于单分片")
    }

    fun onTxnCommit() = txnCommit.inc()
    fun onTxnRollback() = txnRollback.inc()

    /** 目前没有重试路径，保留接口以便将来接入。 */
    fun onTxnRetry() = txnRetry.inc()

    fun onTxnSingleShard() = txnSingleShard.inc()
    fun onTxn2pc() = txn2pc.inc()

    /** 单分片事务占比。设计文档 §12.5：这个值应 > 90%。 */
    fun singleShardRatio(): Double {
        val s = txnSingleShard.value()
        val t = s + txn2pc.value()
        return if (t == 0L) 0.0 else s.toDouble() / t
    }

    // ── 查询 ──────────────────────────────────────────────────────

    private val queryDuration: Array<Histogram> = Array(QueryType.entries.size) {
        registry.histogram("yb_query_duration_seconds", Bounds.QUERY, "type" to QueryType.entries[it].wire)
    }
    private val queryObjects: Array<Histogram> = Array(QueryType.entries.size) {
        registry.histogram("yb_query_objects_fetched", Bounds.OBJECTS, "type" to QueryType.entries[it].wire)
    }
    val slowQueries: Counter = registry.counter("yb_slow_queries_total")

    init {
        registry.help("yb_query_duration_seconds", "查询耗时（按访问路径分类）")
        registry.help(
            "yb_query_objects_fetched",
            "★ 每个查询读取的对象数——本系统最重要的成本指标。IO 预算见设计文档 §14.2",
        )
        registry.help("yb_slow_queries_total", "超过慢查询阈值的语句数")
    }

    fun onQuery(type: QueryType, seconds: Double, objectsFetched: Long, slow: Boolean) {
        queryDuration[type.ordinal].observe(seconds)
        queryObjects[type.ordinal].observe(objectsFetched.toDouble())
        if (slow) slowQueries.inc()
    }

    // ── 存储 ──────────────────────────────────────────────────────

    private val compactionActiveCounter = AtomicLong(0)
    val compactionBytesRead: Counter = registry.counter("yb_compaction_bytes_total", "direction" to "read")
    val compactionBytesWritten: Counter = registry.counter("yb_compaction_bytes_total", "direction" to "write")
    val flushCount: Counter = registry.counter("yb_flush_total")
    val flushBytes: Counter = registry.counter("yb_flush_bytes_total")
    val orphanObjects: Counter = registry.counter("yb_orphan_objects_total")

    /** 上一轮 GC 标记出的待回收对象数（`mark` 时更新，抓取时读）。 */
    val garbagePending = AtomicLong(0)

    /** 上次推进的 GC 水位（墙上毫秒）。用于算滞后。 */
    val gcWatermarkMillis = AtomicLong(0)

    init {
        registry.help("yb_compaction_bytes_total", "compaction 读写的字节数")
        registry.help("yb_flush_total", "MemTable 落成 SST 的次数")
        registry.help("yb_garbage_objects_pending", "上一轮 GC 标记出的待回收对象数")
        registry.help("yb_orphan_objects_total", "★ 无法归类的不可达对象数。非零需排查（设计文档 §6.9）")
        registry.gauge("yb_compaction_active", emptyList()) { compactionActiveCounter.get().toDouble() }
        registry.gauge("yb_garbage_objects_pending", emptyList()) { garbagePending.get().toDouble() }
        // ⚠️ 水位从未推进过（wm == 0）时**不能报 0**。
        //
        // 0 的意思是"GC 刚刚跑过"，而真相是"GC 从来没跑过"——那是完全相反的两件事。
        // 曾经的写法在默认部署（CN 不会自动推进水位，GC 由 `yb-cli gc` 手动触发）
        // 下恒报 0，于是**存储无限增长而监控显示一切正常**。
        // 现在用 -1 表示"尚无水位"，配合 yb_gc_watermark_millis 看实际值
        registry.gauge("yb_gc_watermark_lag_seconds", emptyList()) {
            val wm = gcWatermarkMillis.get()
            if (wm == 0L) -1.0 else (System.currentTimeMillis() - wm).coerceAtLeast(0L) / 1000.0
        }
        registry.help(
            "yb_gc_watermark_lag_seconds",
            "GC 安全水位的滞后秒数。**-1 表示水位从未推进过**（GC 一次都没跑），不是「刚刚跑过」",
        )
        registry.gauge("yb_group_commit_dilution_ratio", emptyList()) { groupCommitDilution() }
        registry.gauge("yb_txn_single_shard_ratio", emptyList()) { singleShardRatio() }
    }

    fun compactionStarted() = compactionActiveCounter.incrementAndGet()
    fun compactionFinished() = compactionActiveCounter.decrementAndGet()

    // ── 审计 ──────────────────────────────────────────────────────

    val auditDropped: Counter = registry.counter("yb_audit_dropped_total")
    val auditWritten: Counter = registry.counter("yb_audit_records_written_total")
    val auditWriteFailed: Counter = registry.counter("yb_audit_write_failed_total")

    init {
        registry.help("yb_audit_dropped_total", "审计日志因缓冲区溢出而丢弃的记录数。非零即需排查")
        registry.help("yb_audit_records_written_total", "已落成对象的审计记录数")
        registry.help("yb_audit_write_failed_total", "审计日志写出失败次数（失败不影响主流程）")
    }

    /** 测试用：清空所有计数。 */
    fun reset() = registry.reset()

}
