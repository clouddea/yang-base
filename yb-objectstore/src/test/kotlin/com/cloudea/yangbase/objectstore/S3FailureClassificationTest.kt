package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.ObjectStoreException
import com.cloudea.yangbase.common.PreconditionFailedException
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.common.metrics.StoreOp
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `S3ObjectStore` 的**失败分类**测试（设计文档 §3.3.1 的那张表）。
 *
 * 这一层是整个重试机制的知识来源：重试层只认
 * [ObjectStoreException.retryable] 与 409/412 两个专门类型，它**不知道** HTTP 状态码。
 * 所以「8 次失败里哪几种该重发」这个判断如果不在这里被验证，就等于没验证——
 * 上层写得再对，也是建立在一个没人测过的假设上。
 *
 * **必须走真实 HTTP**：自己 `new` 一个 `S3Exception` 只能测到我们手写的 `when` 分支，
 * 测不到 SDK 在真实状态码上怎么把它解出来、也测不到重发之后到底是成功还是又失败。
 * `EmbeddedS3Server` 因此带了一个故障注入开关（注入的是**状态码**）。
 */
class S3FailureClassificationTest {

    private lateinit var s3: EmbeddedS3Server
    private lateinit var raw: S3ObjectStore
    private lateinit var store: RetryingObjectStore

    @BeforeTest
    fun setUp() {
        Metrics.reset()
        s3 = EmbeddedS3Server("retry-test")
        s3.start()
        raw = S3ObjectStore.create(
            bucket = "retry-test",
            endpoint = s3.endpoint,
            region = "us-east-1",
            accessKey = "t",
            secretKey = "t",
            caps = ObjectStoreCapabilities.FULL,
        )
        store = RetryingObjectStore(
            delegate = raw,
            maxAttempts = 6,
            baseDelayMillis = 5,
            maxDelayMillis = 20,
            deadlineMillis = 2_000,
        )
    }

    @AfterTest
    fun tearDown() {
        runCatching { raw.close() }
        runCatching { s3.stop() }
    }

    // ── 绝不重试的两类 ────────────────────────────────────────────

    @Test
    fun `412 端到端不被重试`() = runBlocking {
        store.put("k", "v".toByteArray(), Precondition.MustNotExist)
        s3.resetCounters()

        val e = assertFailsWith<PreconditionFailedException> {
            store.put("k", "v2".toByteArray(), Precondition.MustNotExist)
        }

        assertTrue(e.message!!.contains("前置条件不成立"), e.message)
        assertEquals(1, s3.requestCount("PUT"), "412 是确定的结论：重发只会白花一次往返")
        assertEquals(0, Metrics.storeRetryCount(StoreOp.PUT_CONDITIONAL, "transient"))
    }

    /**
     * ★ **同一个 412 状态码，两种完全不同的语义**（issue #8）。
     *
     * S3 对 `If-None-Match` 不成立与 `If-Match` 不匹配都返回 412，
     * 光看状态码无法区分。以前一律归成「前置条件不成立（确定失败）」，
     * 后果是：manifest 指针 CAS 失败在 S3 上抛的异常与内存/本地 FS 不一致，
     * 而 `Manifest`/`Lease`/`AutoInc` 里的 fencing 分支接不到它。
     *
     * 能区分二者的信息只在**发起请求的那一侧**——所以分类必须看 `precondition`。
     */
    @Test
    fun `If-Match 不匹配是并发冲突，不是确定失败`() = runBlocking {
        store.put("cas", "v1".toByteArray())
        val stale = raw.head("cas").etag
        store.put("cas", "v2".toByteArray(), Precondition.MustMatch(stale)) // 这一下把 etag 换掉了

        val e = assertFailsWith<ConcurrentModificationException> {
            store.put("cas", "v3".toByteArray(), Precondition.MustMatch(stale))
        }
        assertTrue(e.message!!.contains("etag"), e.message)
    }

    @Test
    fun `If-Match 撞上不存在的对象仍然报对象不存在`() = runBlocking {
        // 这种情况下后端返回的是 **404**（不是 412）：既然对象都不在了，
        // 就没必要把它包成"并发冲突"假装还有救。在 S3 / 内嵌服务器上两边一致，
        // 而把 404 改写成冲突会让「CURRENT 指针真的没了」这种灾难变成一次无限重试。
        assertFailsWith<ObjectNotFoundException> {
            store.put("absent", "v".toByteArray(), Precondition.MustMatch(ETag("nope")))
        }
    }

    @Test
    fun `404 端到端不被重试`() = runBlocking {
        s3.resetCounters()

        assertFailsWith<ObjectNotFoundException> { store.get("no-such-object") }

        assertEquals(1, s3.requestCount("GET"), "对象不存在是业务语义，不是抖动")
    }

    // ── 可重试的两类 ──────────────────────────────────────────────

    @Test
    fun `503 被后端标成可重试`() = runBlocking {
        s3.injectFailures(1_000)

        val e = assertFailsWith<ObjectStoreException> { raw.get("k") }

        // 分类必须发生在后端：重试层拿到的就这一个布尔值，它看不到 HTTP 状态码
        assertTrue(e.retryable, "5xx 必须标成可重试，否则重试层无从判断：${e.message}")
    }

    /**
     * 这条断言守的是「重试只有**一个主人**」。
     *
     * SDK 标准模式默认自己重试 3 次，那样一来 `yb_objectstore_requests_total`
     * 记的 1 次后面其实发生了 3 次真实请求——按请求计费的预算表（§14.2）与
     * 成本估算（§13）会同时失真，而且我们的总时限也管不住它。
     * 所以 SDK 的重试被显式关掉了，SDK 只允许**如实报告**这一次失败。
     */
    @Test
    fun `SDK 不在我们背后自己重发`() = runBlocking {
        s3.injectFailures(1_000)

        assertFailsWith<ObjectStoreException> { raw.get("k") }

        // 注入的失败在进入 ops 之前就被拦下，所以它记在 Injected503 上而不是 GET 上
        assertEquals(
            1, s3.requestCount("Injected503"),
            "503 只能产生 1 次真实请求：重发是 L1 层的职责，不是 SDK 的",
        )
    }

    @Test
    fun `503 会被重发并最终成功`() = runBlocking {
        s3.injectFailures(3)
        Metrics.reset()

        store.put("k", "v".toByteArray())

        assertEquals(0, s3.pendingInjections(), "注入的失败必须被消费完，否则这次调用根本没跑到重试层")
        assertEquals(
            3, Metrics.storeRetryCount(StoreOp.PUT, "transient"),
            "3 次失败 → 3 次重发（第 4 次成功）：重发必须被计入指标，否则「后端在抖」在运维侧完全不可见",
        )
        assertEquals(0, Metrics.storeRetryExhaustedCount(StoreOp.PUT))
    }

    /**
     * ★ **分类必须沿 cause 链判定**——这是从真实后端上带回来的一条。
     *
     * 在阿里云 OSS 上，能力探测把 Range 读报成了「不支持」，而 OSS 的 Range 读
     * 完全正常：真实原因是那一次请求的 keep-alive 连接在**读响应体**时被掐断，
     * JDK 抛出的 `UncheckedIOException`（包着 `SocketException`）既不是
     * `IOException`、也不是 SDK 自己的异常，于是旧实现把它归成了**永久故障**：
     * L1 不重发、探测记成"后端不支持"。
     *
     * 端到端的那一半（真截断一条连接）没法用内嵌服务器稳定复现——
     * `com.sun.net.httpserver` 在提前关闭时会把消息**合法地**结束掉，
     * 客户端拿到的是一个短的 200，不报错。所以这里直接验证分类表本身：
     * 它正是重试层唯一的知识来源（重试层看不到 HTTP 状态码）。
     */
    @Test
    fun `按 cause 链判定瞬时故障`() {
        fun transient(e: Throwable) = S3ObjectStore.isTransient(e)

        // 包装在 JDK 的 UncheckedIOException 里的网络中断（OSS 上实测到的那一种）
        assertTrue(
            transient(
                java.io.UncheckedIOException(
                    java.net.SocketException("Unexpected end of file from server"),
                ),
            ),
            "响应体读到一半断连必须算瞬时故障",
        )
        // SDK 把底层 SocketException 包进自己的客户端异常
        assertTrue(
            transient(
                software.amazon.awssdk.core.exception.SdkClientException.builder()
                    .cause(java.net.SocketTimeoutException("read timed out")).build(),
            ),
        )
        val notFound = software.amazon.awssdk.services.s3.model.NoSuchKeyException.builder()
            .statusCode(404).message("no such key").build()
        assertFalse(transient(notFound), "404 是业务语义，包成什么类型都不该重发")
        val badRequest = software.amazon.awssdk.services.s3.model.S3Exception.builder()
            .statusCode(400).message("InvalidArgument").build()
        assertFalse(transient(badRequest), "400（如 OSS 不认 If-None-Match）是明确结论")
        assertFalse(transient(IllegalStateException("完全没见过的异常")), "未知异常不猜")
    }

    @Test
    fun `409 端到端被识别为冲突而不是普通失败`() = runBlocking<Unit> {
        s3.injectFailures(1, status = 409)

        // 409 = 条件结论未知（有并发写在途）。调用方靠这个**类型**决定
        // 「重读当前状态再 CAS」，所以它不能退化成 ObjectStoreException
        assertFailsWith<ConcurrentModificationException> {
            raw.put("k", "v".toByteArray(), Precondition.MustNotExist)
        }
    }

    @Test
    fun `409 会被重发并最终成功`() = runBlocking {
        s3.injectFailures(1, status = 409)
        Metrics.reset()

        store.put("k", "v".toByteArray(), Precondition.MustNotExist)

        assertEquals(0, s3.pendingInjections())
        assertTrue(
            Metrics.storeRetryCount(StoreOp.PUT_CONDITIONAL, "conflict") >= 1,
            "冲突重试要单独记一类：它与「后端在抖」是两回事（§3.3.1）",
        )
    }

    @Test
    fun `连接层失败也被标成可重试`() = runBlocking {
        // 真实的网络中断：端点在，但连不上。SDK 不会把它包装成 S3Exception，
        // 而是在异常类型上表现出来——这正是「网络中断」这一行最容易漏掉的地方
        s3.stop()

        val e = assertFailsWith<ObjectStoreException> { raw.get("k") }

        assertTrue(e.retryable, "网络中断是瞬时故障，必须可重试：${e.message}")
    }

    /**
     * ★ **「桶不存在」绝不能报成「对象不存在」。**
     *
     * 两者都是 404，但一个是**配置错误**（桶名写错/桶没建/地域不对），
     * 另一个是**正常状态**（恢复路径靠"这个 WAL 对象没有"来判断无事可做、
     * 目录靠它判断"这张表没有"）。
     *
     * 混起来的后果是静默的，也正是本项目最不能接受的那一类：桶名写错时，
     * 目录加载会读成"这个库一张表都没有"，GC 读成"没有东西要删"，
     * 崩溃恢复读成"没有 WAL 要重放"——系统带着**一个空世界**继续跑。
     *
     * 真事（2026-09-27）：`--s3-bucket my-yangbase`（桶没建）启动失败，
     * 报的是 `ObjectNotFoundException: 对象不存在: .yb-probe/…/probe`——
     * 指着探测用的临时对象，而问题在桶名上。
     */
    @Test
    fun `桶不存在必须报成配置错误而不是对象不存在`() = runBlocking {
        val missing = S3ObjectStore.create(
            bucket = "yangbase-missing-bucket",
            endpoint = s3.endpoint,
            region = "us-east-1",
            accessKey = "t", secretKey = "t",
            caps = ObjectStoreCapabilities.FULL,
        )
        try {
            for ((name, op) in listOf<Pair<String, suspend () -> Any?>>(
                "get" to { missing.get("k") },
                "put" to { missing.put("k", "v".toByteArray()) },
                "list" to { missing.list("") },
            )) {
                val e = assertFailsWith<ObjectStoreException>("$name 必须报配置错误") { op() }
                assertTrue(
                    e.message!!.contains("桶不存在"),
                    "$name 的错误必须点名**桶**（而不是「对象不存在」），实际：${e.message}",
                )
                assertFalse(e.retryable, "$name: 配置错误重试一万次也是同样的结果，不能标成可重试")
            }
        } finally {
            missing.close()
        }
    }

    /** 反过来：正常的"对象不存在"仍然是 [ObjectNotFoundException]，恢复路径靠它判断无事可做。 */
    @Test
    fun `对象真的不存在时仍是 ObjectNotFoundException`() = runBlocking {
        assertFailsWith<ObjectNotFoundException> { raw.get("never-written-key") }
        assertFailsWith<ObjectNotFoundException> { raw.head("never-written-key") }
    }
}
