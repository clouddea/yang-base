package com.cloudea.yangbase.server

import com.cloudea.yangbase.common.ObjectStoreException
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.ByteRange
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.Precondition
import java.net.ServerSocket
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * **启动失败的诊断质量**。
 *
 * 端口被占用是这套系统最常见的启动失败——每个开发机上几乎都跑着别的 MySQL
 * （3306 是它的默认端口），而这件事必须**一眼能看懂并照着做**。
 * 裸的 `BindException: 地址已在使用` 既不说是谁占的、也不说该怎么办，
 * 使用者只能去搜索引擎里拼线索。
 *
 * 这类断言不测"功能"，测的是**故障时的可操作性**——它们同样会回归。
 */
class StartupDiagnosticsTest {

    private fun occupyFreePort(): Pair<ServerSocket, Int> {
        val s = ServerSocket(0)
        return s to s.localPort
    }

    @Test
    fun `MySQL 端口被占用时给出可照做的提示`() {
        val (holder, port) = occupyFreePort()
        try {
            val server = YangBaseServer.forTesting(
                store = com.cloudea.yangbase.objectstore.LocalFileObjectStore(
                    java.nio.file.Files.createTempDirectory("yb-portconflict"),
                ),
                port = port,
            )
            val e = assertFailsWith<Exception> { server.start() }
            val msg = generateSequence<Throwable>(e) { it.cause }.joinToString(" | ") { it.message ?: "" }

            assertTrue(msg.contains("端口 $port 已被占用"), "要说清是哪个端口：$msg")
            assertTrue(msg.contains("ss -ltnp") || msg.contains("lsof"), "要给出查占用的命令：$msg")
            assertTrue(msg.contains("--port"), "要给出改端口这个出路：$msg")
        } finally {
            holder.close()
        }
    }

    /**
     * 模拟"后端连不上/没鉴权过"：**第一步 PUT 就失败**。
     *
     * 真实场景（2026-09-27，阿里云 OSS）：启动报
     * `读写=✗` + 五项"前置步骤失败"，**没有一行说明是什么错**；
     * 而收尾那句还在讲"本系统建立在条件写之上"并建议
     * `--skip-capability-probe`——对一次 403/404 来说，这两句都在指错方向。
     */
    private class PutAlwaysFails(private val delegate: ObjectStore) : ObjectStore {
        override val caps get() = delegate.caps
        override suspend fun get(key: String, range: ByteRange?) = delegate.get(key, range)
        override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag =
            throw ObjectStoreException(
                "PUT $key 失败: 404 NoSuchBucket (Service: S3, Status Code: 404)",
                java.io.UncheckedIOException("连接被对端重置", java.net.SocketException("Connection reset")),
            )

        override suspend fun delete(key: String) = delegate.delete(key)
        override suspend fun list(prefix: String, pageToken: String?, limit: Int) =
            delegate.list(prefix, pageToken, limit)

        override suspend fun head(key: String) = delegate.head(key)
    }

    @Test
    fun `后端连不上时启动错误要说清是什么错、且不要建议跳过探测`() {
        val server = YangBaseServer.forTesting(
            store = PutAlwaysFails(MemoryObjectStore()),
            port = 0,
        )
        val e = assertFailsWith<Exception> { server.start() }
        val msg = generateSequence<Throwable>(e) { it.cause }.joinToString(" | ") { it.message ?: "" }

        assertTrue(msg.contains("NoSuchBucket"), "要带上后端给的原话（404/NoSuchBucket）：$msg")
        assertTrue(msg.contains("Connection reset"), "最内层原因不能被丢掉：$msg")
        assertTrue(
            msg.contains("基础读写"),
            "要指明失败在**基础读写**这一层，而不是笼统地说'未通过探测'：$msg",
        )
        assertTrue(
            !msg.contains("--skip-capability-probe") || msg.contains("帮不上忙"),
            "基础读写失败时**不能**建议跳过探测——那会让人以为绕过去了，实际第一次写入还会撞上：$msg",
        )
    }

    @Test
    fun `观测端点端口被占用时给出可照做的提示`() {
        val (holder, port) = occupyFreePort()
        try {
            val e = assertFailsWith<Exception> {
                MetricsHttpServer(host = "127.0.0.1", port = port).start()
            }
            val msg = generateSequence<Throwable>(e) { it.cause }.joinToString(" | ") { it.message ?: "" }
            assertTrue(msg.contains("端口 $port 已被占用"), "要说清是哪个端口：$msg")
            assertTrue(msg.contains("--metrics-port") || msg.contains("--no-metrics"), "要给出出路：$msg")
        } finally {
            holder.close()
        }
    }
}
