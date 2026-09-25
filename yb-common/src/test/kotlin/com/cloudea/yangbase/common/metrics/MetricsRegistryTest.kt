package com.cloudea.yangbase.common.metrics

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 指标注册表与 Prometheus 文本导出（设计文档 §12.5）。
 *
 * 这个文件守护的是**观测信息本身的可解析性**：暴露出来的文本必须是合法的
 * Prometheus 格式，否则抓取器会整段丢弃或报错——那时监控面板上什么都不会出现，
 * 而问题（「指标没暴露」）与「指标是 0」在告警规则里长得一模一样。
 *
 * 因此这里逐条断言的是格式细节：`# TYPE` 只出现一次、直方图的 `_bucket` 累积、
 * 标签转义、浮点数不出现科学计数法。
 */
class MetricsRegistryTest {

    @Test
    fun `计数器渲染成 Prometheus 文本`() {
        val reg = MetricsRegistry()
        val c = reg.counter("yb_demo_total", "op" to "get")
        c.inc()
        c.add(41)
        reg.help("yb_demo_total", "示例计数器")

        val text = reg.render()
        assertTrue(text.contains("# HELP yb_demo_total 示例计数器"), text)
        assertTrue(text.contains("# TYPE yb_demo_total counter"), text)
        assertEquals(42.0, parsePrometheus(text)["yb_demo_total{op=\"get\"}"])
    }

    @Test
    fun `直方图展开成 bucket sum count 且同族只声明一次 TYPE`() {
        val reg = MetricsRegistry()
        val h = reg.histogram("yb_demo_seconds", doubleArrayOf(0.1, 0.5, 1.0))
        h.observe(0.05)
        h.observe(0.2)
        h.observe(0.2)
        h.observe(3.0)

        val text = reg.render()
        assertEquals(
            1, text.lineSequence().count { it == "# TYPE yb_demo_seconds histogram" },
            "直方图的 _bucket/_sum/_count 必须与基础名同族，只声明一次 TYPE：\n$text",
        )
        val m = parsePrometheus(text)
        assertEquals(1.0, m["yb_demo_seconds_bucket{le=\"0.1\"}"], "bucket 是累积计数")
        assertEquals(3.0, m["yb_demo_seconds_bucket{le=\"0.5\"}"])
        assertEquals(4.0, m["yb_demo_seconds_bucket{le=\"+Inf\"}"])
        assertEquals(4.0, m["yb_demo_seconds_count"])
        assertEquals(3.45, m["yb_demo_seconds_sum"]!!, 1e-9)
    }

    @Test
    fun `标签里的引号反斜杠换行被转义`() {
        val reg = MetricsRegistry()
        reg.counter("yb_x_total", "k" to "a\"b\\c\nd").inc()
        // 转义错一个字符，整行就会被抓取器判为非法——指标静默消失
        assertTrue(
            reg.render().contains("yb_x_total{k=\"a\\\"b\\\\c\\nd\"} 1"),
            reg.render(),
        )
    }

    @Test
    fun `浮点数按 Prometheus 接受的写法输出`() {
        assertEquals("1", MetricsRegistry.formatDouble(1.0))
        assertEquals("0", MetricsRegistry.formatDouble(0.0))
        assertEquals("42", MetricsRegistry.formatDouble(42.0))
        assertEquals("0.000100", MetricsRegistry.formatDouble(0.0001))
        assertEquals("+Inf", MetricsRegistry.formatDouble(Double.POSITIVE_INFINITY))
        assertEquals("NaN", MetricsRegistry.formatDouble(Double.NaN))
        // 桶上界不补零（与官方客户端的 0.001 / 1 / 2.5 写法一致）
        assertEquals("0.1", MetricsRegistry.formatBound(0.1))
        assertEquals("1", MetricsRegistry.formatBound(1.0))
        assertEquals("2.5", MetricsRegistry.formatBound(2.5))
    }

    @Test
    fun `reset 只清计数与直方图，不动 gauge 与采集器`() {
        val reg = MetricsRegistry()
        reg.gauge("yb_g", emptyList()) { 7.0 }
        val c = reg.counter("yb_c_total")
        c.inc()
        val h = reg.histogram("yb_h_seconds", doubleArrayOf(1.0))
        h.observe(1.0)
        reg.collector { listOf(Sample("yb_collected", listOf("tier" to "mem"), 3.0, MetricType.GAUGE)) }

        reg.reset()
        val m = parsePrometheus(reg.render())
        assertEquals(0.0, m["yb_c_total"])
        assertEquals(0.0, m["yb_h_seconds_count"])
        // gauge 与采集器是「值的引用」，它们不持有累计值，清掉只会让输出少几行
        assertEquals(7.0, m["yb_g"])
        assertEquals(3.0, m["yb_collected{tier=\"mem\"}"])
    }

    @Test
    fun `采集器抛异常不会毁掉整次抓取`() {
        val reg = MetricsRegistry()
        reg.counter("yb_ok_total").inc()
        reg.collector { throw IllegalStateException("采集器炸了") }

        val m = parsePrometheus(reg.render())
        assertEquals(1.0, m["yb_ok_total"], "一个坏掉的采集器不该让其它指标一起消失")
    }

    @Test
    fun `全局 Metrics 暴露设计文档第 12 章要求的核心指标`() {
        Metrics.reset()
        Metrics.onStoreRequest(StoreOp.GET, bytesOut = 10, nanos = 1_000_000)
        Metrics.onStoreRequest(StoreOp.PUT_CONDITIONAL, bytesIn = 20, nanos = 2_000_000)
        Metrics.onGroupCommit(batches = 3, ops = 7)
        Metrics.onTxnSingleShard()
        Metrics.onTxnCommit()
        Metrics.onQuery(QueryType.POINT, 0.002, 1, slow = false)
        Metrics.onTxn2pc()
        Metrics.onTxnCommit()
        Metrics.onLeaseAcquired()
        Metrics.onLeaseContended()
        Metrics.onLeaseAcquireFailed()
        Metrics.onLeaseLost()

        val m = parsePrometheus(Metrics.registry.render())
        assertEquals(1.0, m["yb_objectstore_requests_total{op=\"get\",cached=\"false\"}"])
        assertEquals(1.0, m["yb_objectstore_requests_total{op=\"put_cond\",cached=\"false\"}"])
        assertEquals(10.0, m["yb_objectstore_bytes_total{direction=\"out\",op=\"get\"}"])
        assertEquals(20.0, m["yb_objectstore_bytes_total{direction=\"in\",op=\"put_cond\"}"])
        assertEquals(1.0, m["yb_objectstore_request_duration_seconds_count{op=\"get\"}"])
        assertEquals(3.0, m["yb_group_commit_dilution_ratio"], "摊薄倍数 = 3 个事务 / 1 次 PUT")
        assertEquals(1.0, m["yb_wal_objects_total"])
        assertEquals(2.0, m["yb_transactions_total{result=\"commit\"}"])
        assertEquals(1.0, m["yb_txn_single_shard_total"])
        assertEquals(1.0, m["yb_txn_2pc_total"])
        assertEquals(0.5, m["yb_txn_single_shard_ratio"])
        // 租约（§4.3）：只在 objectstore 协调模式下会产生非零值，
        // 单机部署里它们恒为 0——但**必须存在**，否则聚合侧分不清
        // 「没有发生过丢租约」与「这个版本根本没有这条指标」
        assertEquals(1.0, m["yb_lease_acquisitions_total{result=\"ok\"}"])
        assertEquals(1.0, m["yb_lease_acquisitions_total{result=\"contended\"}"])
        assertEquals(1.0, m["yb_lease_acquisitions_total{result=\"failed\"}"])
        assertEquals(1.0, m["yb_lease_lost_total"])
        assertEquals(1.0, m["yb_query_duration_seconds_count{type=\"point\"}"])
        assertEquals(1.0, m["yb_query_objects_fetched_count{type=\"point\"}"])
        // 编译期就存在的常量数组：桶边界必须与 §12.5 的成本叙事一致
        assertEquals(0.0, m["yb_query_objects_fetched_bucket{type=\"point\",le=\"0\"}"], "0 次 GET 是一个必须可分辨的类别")
    }

    @Test
    fun `查询类型枚举与文档的标签值一致`() {
        assertEquals(listOf("point", "range", "scan", "fulltext", "other"), QueryType.entries.map { it.wire })
        assertEquals(
            listOf("get", "get_range", "put", "put_cond", "delete", "list", "head"),
            StoreOp.entries.map { it.wire },
        )
    }
}

/**
 * 把 Prometheus 文本解析成 `序列 → 值`。
 *
 * **重复的序列会直接失败**——两个同名同标签的序列是非法的（抓取器会拒绝整次抓取），
 * 而它最容易由「同一个指标被注册两次」造成（例如两个采集器都产出一份）。
 * 把它做成解析期的硬断言，比在某个具体用例里逐个去查要可靠得多。
 */
internal fun parsePrometheus(text: String): Map<String, Double> {
    val out = HashMap<String, Double>()
    for (raw in text.lineSequence()) {
        val line = raw.trim()
        if (line.isEmpty() || line.startsWith("#")) continue
        val sp = line.lastIndexOf(' ')
        require(sp > 0) { "非法指标行：$line" }
        val series = line.substring(0, sp)
        val v = line.substring(sp + 1).toDoubleOrNull() ?: error("非法指标值：$line")
        require(out.put(series, v) == null) { "重复的指标序列：$series" }
    }
    return out
}
