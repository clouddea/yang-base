package com.cloudea.yangbase.server

import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 并发下的唯一约束。
 *
 * 这个文件守护的是一个**实测过的严重缺陷**：在此之前，唯一约束是
 * 「读快照 → 检查 → 稍后写入」——两个并发事务各自在自己的快照上
 * 都看不到对方，于是双双通过检查、双双提交。
 *
 * 实测结果：8 个并发插入同一个唯一键，**8 条全部成功**，表里留下 8 行相同值。
 * 唯一约束在并发下完全不存在，而且不报任何错。
 *
 * 修法是让检查与写入原子：唯一索引本就住在独立分片里、那个分片只有一个写者，
 * 串行化点已经存在，缺的只是把检查放到那儿去。
 */
class ConcurrentUniqueTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-cu")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "cu-test", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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

    private fun connect(): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/cudb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun Connection.st(b: (Statement) -> Unit) = createStatement().use(b)

    private fun <T> Connection.q(b: (Statement) -> T): T = createStatement().use(b)

    private fun Statement.rows(sql: String): List<List<Any?>> = executeQuery(sql).use { rs ->
        val n = rs.metaData.columnCount
        val out = ArrayList<List<Any?>>()
        while (rs.next()) out.add((1..n).map { rs.getObject(it) })
        out
    }

    private fun setup(c: Connection, ddl: String) = c.st { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS cudb")
        s.execute("USE cudb")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute(ddl)
    }

    /** 并发跑 [n] 个插入，返回 (成功数, 失败数)。 */
    private fun raceInserts(n: Int, sql: (Int) -> String): Pair<Int, Int> {
        val start = CountDownLatch(1)
        val done = CountDownLatch(n)
        val ok = AtomicInteger()
        val failed = AtomicInteger()
        repeat(n) { i ->
            Thread {
                try {
                    connect().use { c ->
                        c.st { it.execute("USE cudb") }
                        start.await()
                        c.st { s ->
                            try { s.execute(sql(i)); ok.incrementAndGet() }
                            catch (_: Exception) { failed.incrementAndGet() }
                        }
                    }
                } catch (_: Exception) {
                    failed.incrementAndGet()
                } finally {
                    done.countDown()
                }
            }.apply { isDaemon = true }.start()
        }
        Thread.sleep(150)   // 尽量让它们真的撞在一起
        start.countDown()
        done.await()
        return ok.get() to failed.get()
    }

    // ── 核心 ──────────────────────────────────────────────────────

    @Test
    fun `并发插入同一个唯一键 - 只能成功一个`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, email VARCHAR(64), PRIMARY KEY (id), UNIQUE KEY uk (email))")
        }
        val (ok, failed) = raceInserts(8) { i -> "INSERT INTO t VALUES (${i + 1}, 'same@x')" }

        connect().use { c ->
            c.st { s -> s.execute("USE cudb") }
            val rows = c.q { s -> s.rows("SELECT id FROM t") }
            println("[并发唯一键] 成功 $ok 条，被拒 $failed 条，表里 ${rows.size} 行")
            assertEquals(
                1, rows.size,
                "同一个唯一键只允许留下一行——实际 ${rows.size} 行（成功 $ok 条，被拒 $failed 条）",
            )
            assertEquals(1, ok, "只应有一个插入成功")
            assertEquals(7, failed, "其余 7 个都应收到重复键错误")
        }
    }

    @Test
    fun `并发插入同一个唯一键但主键不同 - 一个成功一个被拒`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, email VARCHAR(64), PRIMARY KEY (id), UNIQUE KEY uk (email))")
        }
        val (ok, _) = raceInserts(2) { i -> "INSERT INTO t VALUES (${i + 1}, 'dup@x')" }
        connect().use { c ->
            c.st { s -> s.execute("USE cudb") }
            assertEquals(1, c.q { s -> s.rows("SELECT id FROM t") }.size)
            assertEquals(1, ok)
        }
    }

    @Test
    fun `并发插入不同的唯一键 - 全部成功`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, email VARCHAR(64), PRIMARY KEY (id), UNIQUE KEY uk (email))")
        }
        // 反向断言：修复不能把并发写入变成串行拒绝
        val (ok, failed) = raceInserts(8) { i -> "INSERT INTO t VALUES (${i + 1}, 'u$i@x')" }
        connect().use { c ->
            c.st { s -> s.execute("USE cudb") }
            assertEquals(8, c.q { s -> s.rows("SELECT id FROM t") }.size, "不同键必须全部成功")
            assertEquals(8, ok)
            assertEquals(0, failed)
        }
    }

    @Test
    fun `事务并发插入同一个唯一键 - 只能成功一个`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, email VARCHAR(64), PRIMARY KEY (id), UNIQUE KEY uk (email))")
        }
        val (ok, _) = raceInserts(6) { i ->
            "INSERT INTO t VALUES (${i + 1}, 'tx@x')"
        }
        connect().use { c ->
            c.st { s -> s.execute("USE cudb") }
            val rows = c.q { s -> s.rows("SELECT id FROM t") }
            assertEquals(1, rows.size, "并发插入同一唯一键只能留一行，实际 ${rows.size}")
            assertEquals(1, ok)
        }
    }

    @Test
    fun `复合唯一键的并发插入`() {
        connect().use { c ->
            setup(
                c,
                "CREATE TABLE t (id BIGINT NOT NULL, a INT, b INT, PRIMARY KEY (id), UNIQUE KEY uk (a, b))",
            )
        }
        val (ok, _) = raceInserts(6) { i -> "INSERT INTO t VALUES (${i + 1}, 7, 9)" }
        connect().use { c ->
            c.st { s -> s.execute("USE cudb") }
            assertEquals(1, c.q { s -> s.rows("SELECT id FROM t") }.size)
            assertEquals(1, ok)
        }
    }

    @Test
    fun `唯一约束不误伤覆盖写`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, email VARCHAR(64), v INT, PRIMARY KEY (id), UNIQUE KEY uk (email))")
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1, 'a@x', 1)")
                // 同一行改自己的值（含改回原值）不应被判成重复
                s.execute("UPDATE t SET v = 2 WHERE id = 1")
                s.execute("UPDATE t SET email = 'a@x' WHERE id = 1")
                assertEquals(1, s.rows("SELECT id FROM t").size)
                assertEquals("a@x", s.rows("SELECT email FROM t")[0][0])
            }
        }
    }

    @Test
    fun `唯一约束在并发下也不误伤不同表`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS cudb")
                s.execute("USE cudb")
                for (t in listOf("t1", "t2")) {
                    s.execute("DROP TABLE IF EXISTS $t")
                    s.execute("CREATE TABLE $t (id BIGINT NOT NULL, e VARCHAR(64), PRIMARY KEY (id), UNIQUE KEY uk (e))")
                    s.execute("INSERT INTO $t VALUES (1, 'same@x')")
                }
                // 两张表各有自己的索引分片，互不干扰
                assertEquals(1, s.rows("SELECT id FROM t1").size)
                assertEquals(1, s.rows("SELECT id FROM t2").size)
            }
        }
    }
}
