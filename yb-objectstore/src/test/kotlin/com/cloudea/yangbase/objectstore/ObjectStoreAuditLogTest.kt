package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.audit.Audit
import com.cloudea.yangbase.common.audit.AuditActions
import com.cloudea.yangbase.common.audit.AuditEvent
import com.cloudea.yangbase.common.audit.AuditSink
import com.cloudea.yangbase.common.metrics.Metrics
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 审计日志（设计文档 §12.5）。
 *
 * 审计是**唯一被允许**在生产对象存储上产生写入的观测手段，因此它必须同时满足
 * 三条硬性要求，这个文件逐条把它们变成断言：
 *
 *  1. **批量**：N 条记录摊成一次 PUT，且空闲时一次请求都不发
 *  2. **失败隔离**：写出失败既不抛给调用方，也不丢记录（回到缓冲重试）
 *  3. **有界**：内存缓冲满了宁可丢最旧的记录，也不能把 CN 吃光——但丢弃必须可见
 */
class ObjectStoreAuditLogTest {

    private fun event(target: String, action: String = AuditActions.DDL_CREATE_TABLE) = AuditEvent(
        ts = 1_700_000_000_000L,
        action = action,
        database = "d",
        target = target,
        sql = "CREATE TABLE $target (id BIGINT)",
    )

    /** 固定日期，让对象键可预期（否则测试要跟着系统时区跑）。 */
    private fun audit(
        store: ObjectStore,
        scope: CoroutineScope,
        maxObjectBytes: Int = 8 * 1024 * 1024,
        maxBufferedBytes: Long = 1024 * 1024,
        log: (String) -> Unit = {},
    ) = ObjectStoreAuditLog(
        store = store,
        scope = scope,
        flushIntervalMillis = 60_000,
        maxObjectBytes = maxObjectBytes,
        maxBufferedBytes = maxBufferedBytes,
        log = log,
        nowMillis = { 1_700_000_000_000L },
        dateOf = { "2026-09-24" },
    )

    private val today = ObjectKeys.auditLog("2026-09-24")

    @Test
    fun `record 不产生任何请求，flush 才写一次 PUT`() = runTest {
        val counting = CountingObjectStore(MemoryObjectStore())
        val log = audit(counting, backgroundScope)

        log.record(event("t1"))
        log.record(event("t2"))
        assertEquals(0L, counting.totalOps(), "record 必须是纯内存操作——它被调用在 DDL 之后")

        assertTrue(log.flush())
        assertEquals(1L, counting.count("put"), "两条记录应摊成一次 PUT")
        val text = String(counting.get(today), Charsets.UTF_8)
        assertEquals(2, text.trim().lines().size, "JSONL：一行一条")
        assertTrue(text.contains("\"t1\""), text)
        assertTrue(text.contains("\"t2\""), text)

        // 第二天之前不再写：缓冲为空时 flush 直接返回，**一次请求都不发**
        counting.reset()
        assertFalse(log.flush())
        assertEquals(0L, counting.totalOps(), "没有待写记录时不该碰对象存储")
    }

    @Test
    fun `当日对象是覆盖写，第二次刷新保留已有内容`() = runTest {
        val mem = MemoryObjectStore()
        // 预置一条「上一个进程写的」记录：进程可以随时销毁重建，
        // 审计必须跨进程累积在同一天的对象里
        mem.put(today, "{\"ts\":1,\"action\":\"ddl.create_database\"}\n".toByteArray())
        val counting = CountingObjectStore(mem)
        val log = audit(counting, backgroundScope)

        log.record(event("t1"))
        assertTrue(log.flush())
        log.record(event("t2"))
        assertTrue(log.flush())

        val text = String(mem.get(today), Charsets.UTF_8)
        assertTrue(text.contains("ddl.create_database"), "已有内容不能被覆盖掉：$text")
        assertTrue(text.contains("\"t1\"") && text.contains("\"t2\""), text)
        // 成本模型：每天一次 GET 把已有内容读回来，之后每次刷新只有一次 PUT
        assertEquals(1L, counting.count("get"), counting.report())
        assertEquals(2L, counting.count("put"), counting.report())
    }

    @Test
    fun `写出失败时记录回到缓冲，且不影响主流程`() = runTest {
        val breaking = BreakingStore(MemoryObjectStore())
        val messages = ArrayList<String>()
        val log = audit(breaking, backgroundScope, log = { messages += it })
        val failedBefore = Metrics.auditWriteFailed.value()
        val writtenBefore = Metrics.auditWritten.value()

        log.record(event("t1"))
        breaking.failPuts = true

        // ① 不抛给调用方（DDL 不能因为审计写不出去而失败）
        assertFalse(log.flush())
        assertEquals(0L, log.writtenRecords)
        assertEquals(failedBefore + 1, Metrics.auditWriteFailed.value())
        assertTrue(messages.any { it.contains("写出失败") }, "失败必须可见：$messages")
        assertEquals(writtenBefore, Metrics.auditWritten.value(), "没写出去就不能算已写")

        // ② 记录没有丢：后端恢复后重试成功
        breaking.failPuts = false
        assertTrue(log.flush())
        assertEquals(1L, log.writtenRecords)
        assertTrue(String(breaking.get(today), Charsets.UTF_8).contains("\"t1\""))
    }

    @Test
    fun `缓冲满了丢最旧的记录并计数，最新的一定留下`() = runTest {
        val counting = CountingObjectStore(MemoryObjectStore())
        val droppedBefore = Metrics.auditDropped.value()
        val log = audit(counting, backgroundScope, maxBufferedBytes = 200)

        repeat(50) { log.record(event("t$it")) }
        assertTrue(
            Metrics.auditDropped.value() > droppedBefore,
            "丢弃审计记录是必须可见的事件（否则就是静默失效）",
        )

        assertTrue(log.flush())
        val text = String(counting.get(today), Charsets.UTF_8)
        assertTrue(text.contains("\"target\":\"t49\""), "最新的记录必须留下：$text")
        assertFalse(text.contains("\"target\":\"t0\""), "丢的应该是最旧的：$text")
    }

    @Test
    fun `当日对象超过上限后退化为不可变分片`() = runTest {
        val mem = MemoryObjectStore()
        val log = audit(mem, backgroundScope, maxObjectBytes = 10)

        log.record(event("t1"))
        assertTrue(log.flush())
        log.record(event("t2"))
        assertTrue(log.flush())

        val auditKeys = mem.keys().filter { it.startsWith(ObjectKeys.auditPrefix()) }.sorted()
        assertEquals(2, auditKeys.size, "超限后每次刷新落一个独立分片：$auditKeys")
        assertFalse(
            auditKeys.contains(today),
            "不再重写越来越大的当日对象——那是每次刷新都付一次全量上传",
        )
        for (k in auditKeys) {
            assertTrue(
                Regex("ops/audit/2026-09-24\\.\\d{20}\\.jsonl").matches(k),
                "分片键必须零填充（LIST 按字节字典序，否则时间顺序会乱）：$k",
            )
        }
    }

    @Test
    fun `shutdown 把残留的记录写出去`() = runBlocking {
        val counting = CountingObjectStore(MemoryObjectStore())
        val log = audit(counting, this)
        log.start()
        log.record(event("t1"))
        log.shutdown()

        assertEquals(1L, counting.count("put"), counting.report())
        assertTrue(String(counting.get(today), Charsets.UTF_8).contains("\"t1\""))
    }

    @Test
    fun `后台刷新按间隔把记录写出去`() = runBlocking {
        val counting = CountingObjectStore(MemoryObjectStore())
        val log = ObjectStoreAuditLog(
            store = counting,
            scope = this,
            flushIntervalMillis = 20,
            nowMillis = { 1_700_000_000_000L },
            dateOf = { "2026-09-24" },
            log = { },
        )
        log.start()
        log.record(event("t1"))

        val deadline = System.currentTimeMillis() + 5_000
        while (counting.count("put") == 0L && System.currentTimeMillis() < deadline) {
            kotlinx.coroutines.delay(10)
        }
        log.close()
        assertEquals(1L, counting.count("put"), "后台刷新应该在间隔到达后写出：" + counting.report())
    }

    @Test
    fun `Audit 全局落点吞掉异常且可安全卸载`() {
        try {
            Audit.install(object : AuditSink {
                override fun record(event: AuditEvent) = throw IllegalStateException("落点炸了")
            })
            // 审计失败绝不能让一次 DDL / 一次 GC 失败
            Audit.record(event("t1"))
            Audit.record(event("t2"))
        } finally {
            Audit.uninstall()
        }
        assertEquals(AuditSink.NOOP, Audit.sink, "卸载后必须回到「什么都不做」")

        // 没有安装落点时埋点依然安全（也依然零成本）
        Audit.record(event("t3"))
    }
}

/**
 * 可注入 PUT 失败的对象存储。
 *
 * 用真实的 `ObjectStore` 而不是 mock：审计的失败路径要验证的是
 * 「记录回到缓冲、下一次刷新重试成功」——这需要后端能真的坏掉又真的恢复。
 */
private class BreakingStore(private val delegate: ObjectStore) : ObjectStore {

    @Volatile
    var failPuts: Boolean = false

    override val caps: ObjectStoreCapabilities get() = delegate.caps

    override suspend fun get(key: String, range: ByteRange?): ByteArray = delegate.get(key, range)

    override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag {
        if (failPuts) throw IllegalStateException("故障注入：PUT 失败 $key")
        return delegate.put(key, data, precondition)
    }

    override suspend fun delete(key: String): Boolean = delegate.delete(key)

    override suspend fun list(prefix: String, pageToken: String?, limit: Int): ListPage =
        delegate.list(prefix, pageToken, limit)

    override suspend fun head(key: String): ObjectMeta = delegate.head(key)

    override suspend fun exists(key: String): Boolean = delegate.exists(key)
}
