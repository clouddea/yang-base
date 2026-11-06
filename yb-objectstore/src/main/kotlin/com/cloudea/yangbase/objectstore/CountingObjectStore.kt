package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.ObjectStoreException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder

/**
 * 装饰器：统计每类操作的**请求数**与**字节数**。
 *
 * 这是本项目最重要的测试夹具（CLAUDE.md 指定）。设计文档 §14.2 的 IO 预算
 * 靠它变成可执行断言——**比测延迟可靠得多，因为请求数是确定性的**。
 *
 * 用法：
 * ```
 * val counting = CountingObjectStore(minioStore)
 * val engine = openEngine(counting)
 * engine.pointGet(pk = 42)      // 预热，填充缓存
 * counting.reset()
 * engine.pointGet(pk = 43)      // 测量
 * assertEquals(1, counting.count("get"))   // ★ 这条断言守护整个存储层设计
 * ```
 */
class CountingObjectStore(
    private val delegate: ObjectStore,
    private val recordKeys: Boolean = false,
) : ObjectStore {

    override val caps: ObjectStoreCapabilities get() = delegate.caps

    private val ops = ConcurrentHashMap<String, LongAdder>()
    private val inBytes = ConcurrentHashMap<String, LongAdder>()
    private val outBytes = ConcurrentHashMap<String, LongAdder>()
    private val keyHits = ConcurrentHashMap<String, LongAdder>()

    /** 设为 true 后，任何 LIST 调用都会抛异常——用于在测试里封杀热路径上的 LIST。 */
    @Volatile
    var failOnList: Boolean = false

    /** 设为 true 后，任何 PUT 调用都会抛异常——用于断言只读路径不产生写。 */
    @Volatile
    var failOnPut: Boolean = false

    /**
     * 只让**键里包含这个子串**的 PUT 失败。
     *
     * 精确故障注入：测 2PC 时必须能在「阶段 1 成功、阶段 2 失败」这个
     * 精确缝隙上制造失败——那正是这套机制唯一要防的场景。
     * 用 `failOnPut` 会把阶段 1 也一起打掉，那就什么也没测到。
     */
    @Volatile
    var failOnPutMatching: String? = null

    private fun record(op: String, out: Long, ins: Long, key: String? = null) {
        ops.computeIfAbsent(op) { LongAdder() }.increment()
        outBytes.computeIfAbsent(op) { LongAdder() }.add(out)
        inBytes.computeIfAbsent(op) { LongAdder() }.add(ins)
        if (recordKeys && key != null) {
            keyHits.computeIfAbsent("$op:$key") { LongAdder() }.increment()
        }
    }

    override suspend fun get(key: String, range: ByteRange?): ByteArray {
        if (failOnList && false) { /* get 不受 failOnList 影响 */ }
        val data = delegate.get(key, range)
        // 出流量：实际传输的字节（范围读只计范围内的字节）
        record(if (range == null) "get" else "get_range", data.size.toLong(), 0, key)
        return data
    }

    override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag {
        if (failOnPut) throw AssertionError("本测试断言不应发生 PUT，但发生了: key=$key")
        failOnPutMatching?.let {
            if (key.contains(it)) throw ObjectStoreException("故障注入：拒绝写入 $key")
        }
        val etag = delegate.put(key, data, precondition)
        record(if (precondition == null) "put" else "put_cond", 0, data.size.toLong(), key)
        return etag
    }

    override suspend fun delete(key: String): Boolean {
        val r = delegate.delete(key)
        record("delete", 0, 0, key)
        return r
    }

    /**
     * 批量删除的请求数必须按**真实发出的 HTTP 请求**计，不能简单计成 1。
     *
     * 否则“1001 个键 → 2 个请求”这条断言就成了自证：夹具自己说一个请求就是一个请求。
     * 口径与后端声明一致：声明了 [ObjectStoreCapabilities.batchDeleteMaxKeys] 的
     * 按分页算，没声明的就是 N 次单键删除（默认实现走的就是那条路）。
     */
    override suspend fun deleteBatch(keys: Collection<String>): Int {
        val r = delegate.deleteBatch(keys)
        val max = caps.batchDeleteMaxKeys
        val requests = if (max > 0) (keys.size + max - 1) / max else keys.size
        repeat(requests) { record("delete_batch", 0, 0, null) }
        return r
    }

    override suspend fun list(prefix: String, pageToken: String?, limit: Int): ListPage {
        if (failOnList) throw AssertionError("本测试断言不应发生 LIST，但发生了: prefix=$prefix")
        val page = delegate.list(prefix, pageToken, limit)
        record("list", 0, 0, if (recordKeys) prefix else null)
        return page
    }

    override suspend fun head(key: String): ObjectMeta {
        val m = delegate.head(key)
        record("head", 0, 0, key)
        return m
    }

    override suspend fun exists(key: String): Boolean {
        // 让它走 head 以便被计数，而不是委托给 delegate 的默认实现（会绕过计数）
        return try {
            head(key); true
        } catch (_: com.cloudea.yangbase.common.ObjectNotFoundException) {
            false
        }
    }

    // ── 断言辅助 ────────────────────────────────────────────────

    /** 某类操作的次数。op ∈ {get, get_range, put, put_cond, delete, list, head} */
    fun count(op: String): Long = ops[op]?.sum() ?: 0L

    /** 全部读类操作之和（get + get_range + head）。 */
    fun totalReads(): Long = count("get") + count("get_range") + count("head")

    fun bytesOut(op: String): Long = outBytes[op]?.sum() ?: 0L
    fun bytesIn(op: String): Long = inBytes[op]?.sum() ?: 0L

    /** 全部操作次数，用于「这个查询总共打了多少次对象存储」。 */
    fun totalOps(): Long = ops.values.sumOf { it.sum() }

    fun snapshot(): Map<String, Long> = ops.mapValues { it.value.sum() }

    /**
     * 某类操作分别打了哪些对象（按次数降序）。
     *
     * 断言「一共几次请求」失败时，下一个问题必然是「那几次打在哪」——
     * 没有这个明细就只能靠猜。构造时传 `recordKeys = true` 才有数据。
     */
    fun keysFor(op: String): List<Pair<String, Long>> =
        keyHits.entries.filter { it.key.startsWith("$op:") }
            .map { it.key.removePrefix("$op:") to it.value.sum() }
            .sortedByDescending { it.second }

    /** 全部请求明细（key 级别），断言失败时贴出来能直接看出打在哪。 */
    fun keyReport(): String =
        if (keyHits.isEmpty()) {
            "(未开启 recordKeys)"
        } else {
            keyHits.entries.sortedByDescending { it.value.sum() }
                .joinToString("\n  ") { "${it.key} × ${it.value.sum()}" }
        }

    fun reset() {
        ops.clear(); inBytes.clear(); outBytes.clear(); keyHits.clear()
    }

    /** 生成人类可读的请求明细，断言失败时贴进消息里非常有用。 */
    fun report(): String = buildString {
        append("对象存储请求明细: ")
        if (ops.isEmpty()) append("(无)")
        else append(snapshot().entries.sortedByDescending { it.value }
            .joinToString(", ") { "${it.key}=${it.value}" })
        append(" | 出流量=${outBytes.values.sumOf { it.sum() }}B")
    }

    override fun toString(): String = "CountingObjectStore[${report()}]"
}
