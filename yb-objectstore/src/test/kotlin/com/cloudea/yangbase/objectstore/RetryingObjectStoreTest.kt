package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.ObjectStoreException
import com.cloudea.yangbase.common.PreconditionFailedException
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.common.metrics.StoreOp
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * 故障注入夹具：前 [failFirst] 次操作抛 [failure]，之后正常委托给 [backing]。
 *
 * 用「前 N 次失败、之后成功」而不是「一直失败」，是因为要同时看到两件事：
 * **重试发生了**（尝试次数 > 1）与**结果是对的**（返回值与后备存储一致）。
 * 只注入持续失败只能测到「它会放弃」。
 */
private class FlakyStore(
    val backing: ObjectStore = MemoryObjectStore(),
    private val failFirst: Int,
    private val failure: () -> Throwable,
) : ObjectStore {

    val attempts = AtomicInteger()

    override val caps: ObjectStoreCapabilities get() = backing.caps

    private fun gate() {
        if (attempts.incrementAndGet() <= failFirst) throw failure()
    }

    override suspend fun get(key: String, range: ByteRange?): ByteArray {
        gate(); return backing.get(key, range)
    }

    override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag {
        gate(); return backing.put(key, data, precondition)
    }

    override suspend fun delete(key: String): Boolean {
        gate(); return backing.delete(key)
    }

    override suspend fun list(prefix: String, pageToken: String?, limit: Int): ListPage {
        gate(); return backing.list(prefix, pageToken, limit)
    }

    override suspend fun head(key: String): ObjectMeta {
        gate(); return backing.head(key)
    }
}

/**
 * 重试 / 退避 / 限流层的行为测试。
 *
 * 这些断言是这一层**存在的理由**，不是装饰：它们逐条对应设计文档 §3.3.1 的错误码
 * 语义表，以及「重试不得改变条件写的结论」这条正确性要求。
 *
 * 退避参数在测试里一律调到毫秒级——否则测一次总时限要等 5 秒，
 * 而**被验证的是分类与边界，不是那几十毫秒**。
 */
class RetryingObjectStoreTest {

    @BeforeTest
    fun resetMetrics() {
        // 指标是全局单例（服务端的可观测性汇聚点），测试之间必须互不干扰
        Metrics.reset()
    }

    private fun store(
        delegate: ObjectStore,
        maxAttempts: Int = 4,
        deadlineMillis: Long = 5_000,
        maxConcurrentRequests: Int = 0,
    ) = RetryingObjectStore(
        delegate = delegate,
        maxAttempts = maxAttempts,
        baseDelayMillis = 1,
        maxDelayMillis = 2,
        deadlineMillis = deadlineMillis,
        maxConcurrentRequests = maxConcurrentRequests,
    )

    private fun transientError() = ObjectStoreException("503 SlowDown", retryable = true)

    // ── 可重试：重发到成功 ─────────────────────────────────────────

    @Test
    fun `可重试的失败最终成功且尝试次数 = 失败次数 + 1`() = runBlocking {
        val flaky = FlakyStore(failFirst = 2) { transientError() }
        flaky.backing.put("k", "v".toByteArray())

        val etag = store(flaky).put("k", "v2".toByteArray())

        assertEquals(3, flaky.attempts.get(), "两次瞬时失败 → 必须正好第三次成功")
        assertTrue(etag.value.isNotEmpty())
        assertContentEquals("v2".toByteArray(), flaky.backing.get("k"), "重试后的结果必须真的落地")
    }

    @Test
    fun `单独一次瞬时失败只多试一次`() = runBlocking {
        val flaky = FlakyStore(failFirst = 1) { transientError() }
        flaky.backing.put("k", "v".toByteArray())

        store(flaky).get("k")

        assertEquals(2, flaky.attempts.get())
        assertEquals(1, Metrics.storeRetryCount(StoreOp.GET, "transient"))
        assertEquals(0, Metrics.storeRetryExhaustedCount(StoreOp.GET))
    }

    // ── 对照组：成功路径一次就够 ───────────────────────────────────

    @Test
    fun `正常成功路径只尝试一次且不产生重试`() = runBlocking {
        val flaky = FlakyStore(failFirst = 0) { transientError() }
        flaky.backing.put("k", "v".toByteArray())

        store(flaky).get("k")

        assertEquals(1, flaky.attempts.get(), "没有失败就不该有第二次请求——重试层不该给每次读都加一次往返")
        assertEquals(0, Metrics.storeRetryCount(StoreOp.GET, "transient"))
    }

    // ── 不可重试：一次就抛，且异常原样 ──────────────────────────────

    @Test
    fun `412 只尝试一次且异常实例不被包装`() = runBlocking {
        val boom = PreconditionFailedException("前置条件不成立: key=k")
        val flaky = FlakyStore(failFirst = 1) { boom }

        val thrown = assertFailsWith<PreconditionFailedException> {
            store(flaky).put("k", ByteArray(0), Precondition.MustNotExist)
        }

        assertEquals(1, flaky.attempts.get(), "412 是确定的结论，重发只会浪费时间")
        assertSame(boom, thrown, "异常必须原样抛出：类型与内容都不能被这一层改写")
    }

    @Test
    fun `不可重试的存储错误与 404 都只尝试一次`() = runBlocking {
        val hard = FlakyStore(failFirst = 1) { ObjectStoreException("鉴权失败", retryable = false) }
        val e = assertFailsWith<ObjectStoreException> { store(hard).get("k") }
        assertEquals(1, hard.attempts.get(), "后端没标可重试的错误不容许这一层自作主张地重发")
        assertFalse(e.retryable)

        val missing = FlakyStore(failFirst = 1) { ObjectNotFoundException("k") }
        assertFailsWith<ObjectNotFoundException> { store(missing).get("k") }
        assertEquals(1, missing.attempts.get(), "404 是明确的业务语义（CAS 目标已消失），不是抖动")
    }

    // ── 重试耗尽 ──────────────────────────────────────────────────

    @Test
    fun `尝试次数耗尽时抛出且信息里带尝试次数`() = runBlocking {
        val flaky = FlakyStore(failFirst = 99) { transientError() }

        val e = assertFailsWith<ObjectStoreException> { store(flaky, maxAttempts = 3).get("k") }

        assertEquals(3, flaky.attempts.get())
        assertTrue(e.message!!.contains("已尝试 3 次"), "错误信息必须说明试了几次：${e.message}")
        assertFalse(e.retryable, "预算已用尽，不能再标成可重试去诱导上层再叠一层")
        assertEquals(1, Metrics.storeRetryExhaustedCount(StoreOp.GET))
        assertEquals(2, Metrics.storeRetryCount(StoreOp.GET, "transient"), "3 次尝试 = 2 次重试")
    }

    @Test
    fun `总时限到点就放弃而不是把尝试次数用满`() = runBlocking {
        val flaky = FlakyStore(failFirst = 99) { transientError() }

        val started = System.nanoTime()
        val e = assertFailsWith<ObjectStoreException> {
            // 尝试次数放到 1000，只能靠总时限收住
            RetryingObjectStore(
                delegate = flaky,
                maxAttempts = 1_000,
                baseDelayMillis = 20,
                maxDelayMillis = 40,
                deadlineMillis = 60,
            ).get("k")
        }
        val elapsedMillis = (System.nanoTime() - started) / 1_000_000

        assertTrue(
            flaky.attempts.get() in 2 until 20,
            "必须在时限附近放弃，而不是退化成上千次重试：实际 ${flaky.attempts.get()} 次",
        )
        assertTrue(e.message!!.contains("总时限"), "放弃原因要说清楚：${e.message}")
        assertTrue(elapsedMillis < 1_000, "总时限必须真的生效：实际 ${elapsedMillis}ms")
    }

    // ── 409 冲突：可重试，但必须保持类型 ────────────────────────────

    @Test
    fun `409 冲突会退避重试并以 conflict 记数`() = runBlocking {
        val flaky = FlakyStore(failFirst = 1) { ConcurrentModificationException("条件写并发冲突: key=k") }
        flaky.backing.put("k", "v".toByteArray())

        store(flaky).get("k")

        assertEquals(2, flaky.attempts.get(), "409 的结论未知（有并发写在途），重发会得到明确结论")
        assertEquals(1, Metrics.storeRetryCount(StoreOp.GET, "conflict"))
        assertEquals(0, Metrics.storeRetryCount(StoreOp.GET, "transient"), "冲突与瞬时故障要分开看")
    }

    @Test
    fun `冲突重试耗尽后仍然抛 ConcurrentModificationException`() = runBlocking {
        val flaky = FlakyStore(failFirst = 99) { ConcurrentModificationException("条件写并发冲突: key=k") }

        val e = assertFailsWith<ConcurrentModificationException> {
            store(flaky, maxAttempts = 3).get("k")
        }

        // 调用方（LeaseManager / AutoInc / Tso / Manifest）靠这个类型做「重读当前状态再 CAS」，
        // 换成别的类型会让它们的冲突处理静默失效——重试层自己制造出新的失败模式
        assertTrue(e.message!!.contains("已尝试 3 次"), e.message)
        assertEquals(1, Metrics.storeRetryExhaustedCount(StoreOp.GET))
    }

    // ── 重试不得改变条件写的结论 ───────────────────────────────────

    @Test
    fun `重试不会把失败的 CAS 变成成功`() = runBlocking {
        val backing = MemoryObjectStore()
        backing.put("k", "first".toByteArray())

        assertFailsWith<PreconditionFailedException> {
            store(backing).put("k", "second".toByteArray(), Precondition.MustNotExist)
        }

        assertContentEquals(
            "first".toByteArray(), backing.get("k"),
            "条件不成立时重发得到的仍是 412：重试只消除传输层的不确定性，不改变条件的结论",
        )
    }

    // ── 限流 ──────────────────────────────────────────────────────

    @Test
    fun `并发上限为 1 时两个操作被串行化`() = runBlocking {
        val inFlight = AtomicInteger()
        val maxObserved = AtomicInteger()
        val observed = object : ObjectStore by MemoryObjectStore() {
            override suspend fun get(key: String, range: ByteRange?): ByteArray {
                val now = inFlight.incrementAndGet()
                maxObserved.updateAndGet { maxOf(it, now) }
                delay(20)   // 让重叠有机会发生：不设上限时这里必然观察到 2
                inFlight.decrementAndGet()
                return ByteArray(0)
            }
        }

        // 必须是**同一个** store 实例：信号量是实例状态，每边一个新实例等于没设上限
        val limited = store(observed, maxConcurrentRequests = 1)
        coroutineScope {
            val a = async { limited.get("a") }
            val b = async { limited.get("b") }
            a.await(); b.await()
        }

        assertEquals(1, maxObserved.get(), "信号量必须把在途请求数压在 1，实际观察到 ${maxObserved.get()}")
    }

    @Test
    fun `并发上限为 0 表示不限流`() = runBlocking {
        val inFlight = AtomicInteger()
        val maxObserved = AtomicInteger()
        val observed = object : ObjectStore by MemoryObjectStore() {
            override suspend fun get(key: String, range: ByteRange?): ByteArray {
                val now = inFlight.incrementAndGet()
                maxObserved.updateAndGet { maxOf(it, now) }
                delay(20)
                inFlight.decrementAndGet()
                return ByteArray(0)
            }
        }

        val unlimited = store(observed, maxConcurrentRequests = 0)
        coroutineScope {
            (1..2).map { i -> async { unlimited.get("k$i") } }.forEach { it.await() }
        }

        assertEquals(2, maxObserved.get(), "<= 0 必须真的不限流（否则并行预取会被悄悄退化成串行）")
    }
}
