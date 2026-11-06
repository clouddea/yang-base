package com.cloudea.yangbase.objectstore

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 批量删除的**请求数**门禁。
 *
 * 为什么单独一个测试类：`deleteBatch` 的正确性（键真的没了）由 conformance 套件按后端
 * 跑一遍，而这里要断言的是另一件事——**它到底发了几个 HTTP 请求**。
 * 后者才是这个改动的全部理由：清桶路径的 `2N` 请求把 `io-budget` 集成测试在真实
 * 对象存储上拖到了 23 分钟，而 GC / `DROP TABLE` 面对的是同样规模的键空间。
 *
 * 用内嵌 S3 服务器而不是内存后端：请求计数必须数到**网络边界**上，
 * 否则测的是我们自己的簿记而不是后端的实际行为。
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3BatchDeleteTest {

    private lateinit var server: EmbeddedS3Server
    private lateinit var store: S3ObjectStore
    private lateinit var counting: CountingObjectStore

    @BeforeAll
    fun setUp() {
        server = EmbeddedS3Server("yangbase-test").also { it.start() }
        store = S3ObjectStore.create(
            bucket = "yangbase-test",
            endpoint = server.endpoint,
            region = "us-east-1",
            accessKey = "test",
            secretKey = "test",
            caps = ObjectStoreCapabilities.FULL,
        )
        counting = CountingObjectStore(store)
    }

    @AfterAll
    fun tearDown() {
        runCatching { store.close() }
        server.stop()
    }

    @Test
    fun `1001 个键用两次请求删完，而不是每个键两次`() = runBlocking<Unit> {
        val n = 1001
        val keys = (0 until n).map { "batch/$it" }
        keys.forEach { store.put(it, ByteArray(1)) }
        assertEquals(n, server.objectCount())

        counting.reset()
        val deleted = counting.deleteBatch(keys)

        assertEquals(
            (n + 999) / 1000, counting.count("delete_batch").toInt(),
            "1001 个键应该只发 2 次 DELETE 请求（1000 + 1），实际（建表时 $n 个键逐个删要 $n 次）",
        )
        // 与旧的逐个删路径对比：那条路径是 HEAD + DELETE = 2N 次请求
        assertEquals(0, counting.count("delete"), "批量删除不应退化成单键删除")
        assertEquals(n, deleted, "返回的已删除数量应等于键数")
        assertEquals(0, server.objectCount(), "服务器上不应残留对象")
        assertEquals(2, server.requestCount("DeleteObjects").toInt(), "服务器侧应该真的收到 2 次批量删除请求")
    }

    @Test
    fun `声明不支持批量删除的后端退回到逐个删，且计数如实`() = runBlocking<Unit> {
        // 同一台服务器，但能力声明成「没有原生批量删除」——模拟一个只会 PUT/GET/DELETE
        // 的网关。此时 deleteBatch 必须仍然把键删干净，只是代价变回 N 次请求。
        val noBatchStore = S3ObjectStore.create(
            bucket = "yangbase-test",
            endpoint = server.endpoint,
            region = "us-east-1",
            accessKey = "test",
            secretKey = "test",
            caps = ObjectStoreCapabilities.FULL.copy(batchDeleteMaxKeys = 0),
        )
        try {
            val keys = (0 until 5).map { "fallback/$it" }
            keys.forEach { noBatchStore.put(it, ByteArray(1)) }

            val counted = CountingObjectStore(noBatchStore)
            val deleted = counted.deleteBatch(keys)

            assertEquals(5, deleted, "逐个删路径也应返回删掉的键数")
            assertEquals(5, counted.count("delete_batch").toInt(), "没有原生批量时，N 个键就是 N 次请求")
            keys.forEach { key ->
                assertTrue(runCatching { noBatchStore.head(key) }.isFailure, "键应已被删除：$key")
            }
        } finally {
            noBatchStore.close()
        }
    }
}
