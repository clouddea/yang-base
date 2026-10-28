package com.cloudea.yangbase.objectstore

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertTrue

/**
 * 内嵌 S3 服务器的**每次请求固定开销**门禁。
 *
 * 为什么值得一条测试：这个夹具的延迟会直接变成所有集成测试与基准的延迟。
 * 2026-10-28 的公开基准量到裸 GET **44ms** / 裸 PUT 4ms（issue #33）——
 * 10:1 的比例是 Nagle + 延迟 ACK 的签名。后果是冷点查 42.8ms ≈ 全是夹具，
 * 全表扫描 2 万行要 9 秒（216 次 GET × 44ms）。
 *
 * 这条测试不追求精确：它只回答一个问题 —— **夹具是不是又变慢了一个数量级**。
 * 阈值 15ms 比健康值（~1-4ms）宽松得多，但比病灶（44ms）低得多。
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EmbeddedS3LatencyTest {

    companion object {
        init {
            // JDK 的 HttpServer 默认开着 Nagle。小响应分两次写（头 + 体）时，
            // Nagle + 延迟 ACK 会稳定插入 ~40ms。属性在类初始化时读取，所以必须
            // 在第一次触碰 HttpServer 之前设置。
            if (System.getenv("YB_TEST_NODELAY") == "1") {
                System.setProperty("sun.net.httpserver.nodelay", "true")
            }
        }
    }

    private lateinit var server: EmbeddedS3Server
    private lateinit var store: S3ObjectStore

    @BeforeAll
    fun setUp() {
        server = EmbeddedS3Server("latency").also { it.start() }
        store = S3ObjectStore.create(
            bucket = "latency",
            endpoint = server.endpoint,
            region = "us-east-1",
            accessKey = "test",
            secretKey = "test",
        )
    }

    @AfterAll
    fun tearDown() {
        server.stop()
    }

    @Test
    fun `裸 GET 的固定开销不能比 PUT 高一个数量级`() {
        val payload = ByteArray(4096) { (it % 251).toByte() }
        runBlocking {
            repeat(5) { store.put("probe/$it", payload, null) }

            val put = LongArray(20)
            for (i in 0 until put.size) {
                val t = System.nanoTime()
                store.put("probe/put-$i", payload, null)
                put[i] = (System.nanoTime() - t) / 1000
            }

            val get = LongArray(20)
            for (i in 0 until get.size) {
                val t = System.nanoTime()
                store.get("probe/${i % 5}", null)
                get[i] = (System.nanoTime() - t) / 1000
            }

            val putP50 = put.sorted()[put.size / 2]
            val getP50 = get.sorted()[get.size / 2]
            println("裸请求 p50：PUT=${putP50}µs GET=${getP50}µs")

            assertTrue(
                getP50 < 15_000,
                "裸 GET p50=${getP50}µs，比健康值高一个数量级（issue #33）。" +
                    "内嵌 S3 服务器的每次请求开销会直接变成所有集成测试与基准的开销。",
            )
        }
    }
}
