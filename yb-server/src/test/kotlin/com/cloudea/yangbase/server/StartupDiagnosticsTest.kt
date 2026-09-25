package com.cloudea.yangbase.server

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
