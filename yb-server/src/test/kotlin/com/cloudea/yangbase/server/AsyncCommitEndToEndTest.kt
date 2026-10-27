package com.cloudea.yangbase.server

import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `--commit-mode async` 的端到端验证（issue #13）。
 *
 * 这里要证明的是**对外的三件事**，而不是内部实现：
 *  1. 服务端能用 async 启动，而且写入正常返回（不是「接上就挂死」）；
 *  2. `SHOW VARIABLES LIKE 'commit_mode'` **如实**报 `async` —— 这是使用者唯一的
 *     自查手段，它说谎的话整套 RPO 文档都白写；
 *  3. 会话级 `SET commit_mode = 'sync'` 仍然被拒绝（部署级语义没被这次改动破坏）。
 *
 * 「另一个 CN 暂时读不到」这一条不在这里断言：单机 e2e 造不出第二个 CN；
 * 它由 `WalAsyncCommitTest` 用「PUT 未完成时对象不存在」这个可观测事实来守。
 */
class AsyncCommitEndToEndTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-e2e-async")
        server = YangBaseServer(
            ServerConfig(
                host = "127.0.0.1",
                port = 0,
                nodeId = "test-node-async",
                dataDir = dataDir.toString(),
                allowNonDurableStorage = true,
                cacheBytes = 16L * 1024 * 1024,
                commitMode = "async",
            ),
        )
        server.start()
        port = server.boundPort
        Class.forName("com.mysql.cj.jdbc.Driver")
    }

    @AfterTest
    fun tearDown() {
        runCatching { server.stop() }
        runCatching { dataDir.toFile().deleteRecursively() }
    }

    private fun connect(db: String? = null): Connection {
        val url = buildString {
            append("jdbc:mysql://127.0.0.1:$port/")
            if (db != null) append(db)
            append("?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC")
            append("&characterEncoding=utf8&connectTimeout=5000&socketTimeout=30000")
        }
        return DriverManager.getConnection(url, "root", "")
    }

    private fun Statement.query(sql: String): List<List<Any?>> = executeQuery(sql).use { rs ->
        val cols = rs.metaData.columnCount
        val out = ArrayList<List<Any?>>()
        while (rs.next()) out.add((1..cols).map { rs.getObject(it) })
        out
    }

    @Test
    fun `async 档下写入正常返回，且 SHOW VARIABLES 如实报 async`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE adb")
                st.execute("USE adb")
                st.execute("CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(16), PRIMARY KEY (id))")

                // 写入必须**立刻**返回：async 的整个价值就在这里，挂死就是 bug
                repeat(50) { st.execute("INSERT INTO t (id, v) VALUES ($it, 'x$it')") }

                val vars = st.query("SHOW VARIABLES LIKE 'commit_mode'")
                assertEquals(
                    "async", vars[0][1],
                    "SHOW 必须如实回报部署值——它报 sync 时使用者会以为自己是 RPO=0",
                )

                // 本 CN 内「读自己的写」必须成立：MemTable 在 ACK 之前已应用
                assertEquals(50L, st.query("SELECT COUNT(*) FROM t")[0][0].let { (it as Number).toLong() })

                // 部署级语义没有被这次改动破坏：会话切不动的必须还是切不动
                val e = assertFailsWith<java.sql.SQLException> { st.execute("SET commit_mode = 'sync'") }
                assertTrue(
                    e.message!!.contains("部署级") || e.message!!.contains("commit_mode"),
                    "拒绝信息必须说清这是部署级设置，实际：${e.message}",
                )
                // 同值 SET 是无操作，不该报错
                st.execute("SET commit_mode = 'async'")
            }
        }
    }
}
