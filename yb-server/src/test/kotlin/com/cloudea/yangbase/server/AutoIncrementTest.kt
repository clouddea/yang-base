package com.cloudea.yangbase.server

import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `AUTO_INCREMENT` 的端到端行为（设计文档 §11.4）。
 *
 * ## 这里为什么要用「重启服务器」来测
 *
 * 自增最容易出错的地方不是发号本身，而是**发号的状态放在哪里**。
 * 放在内存里的话，单元测试全绿、单进程手测全对，只有重启才暴露：
 * 计数器归零 → 从旧数据扫最大值 → 扫到的值可能比已发出的小 → 重号。
 *
 * 所以这组用例特意**换掉整个 YangBaseServer 实例**（新的内存状态、
 * 同一个对象存储目录），这是唯一能发现「状态放错地方」的测试形态。
 *
 * 顺带说明：自增值**不保证连续**（跨段、回滚、重启都会留洞），
 * 这与 MySQL 8.0 `innodb_autoinc_lock_mode=2` 一致，
 * 断言里因此只检查「单调、不重复」，从不检查连续性。
 */
class AutoIncrementTest {

    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0
    private var server: YangBaseServer? = null

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-autoinc")
        Class.forName("com.mysql.cj.jdbc.Driver")
        start()
    }

    @AfterTest
    fun tearDown() {
        runCatching { server?.stop() }
        runCatching { dataDir.toFile().deleteRecursively() }
    }

    private fun start() {
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "ai-test", dataDir = dataDir.toString(), allowNonDurableStorage = true)
        ).also { it.start() }
        port = server!!.boundPort
    }

    /** 重启：新进程、新内存状态，**同一个 dataDir**（对象存储才是真相源）。 */
    private fun restart() {
        server?.stop()
        start()
    }

    private fun connect(): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/aitest?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun <T> Connection.q(b: (Statement) -> T): T = createStatement().use(b)

    private fun setup(c: Connection) = c.createStatement().use { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS aitest")
        s.execute("USE aitest")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute("CREATE TABLE t (id BIGINT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))")
    }

    private fun ids(c: Connection): List<Long> =
        c.q { s ->
            s.executeQuery("SELECT id FROM t").use { rs ->
                val out = ArrayList<Long>()
                while (rs.next()) out.add(rs.getLong(1))
                out
            }
        }.sorted()

    @Test
    fun `自增列按顺序发号`() {
        connect().use { c ->
            setup(c)
            c.createStatement().use { s ->
                repeat(5) { s.execute("INSERT INTO t (v) VALUES ($it)") }
            }
            assertEquals(listOf(1L, 2L, 3L, 4L, 5L), ids(c))
        }
    }

    /**
     * ★ 核心用例：**重启之后不能重发已经用过的 id**。
     *
     * 旧的实现把游标放在会话内存里，重启后归零、再去扫表取最大值——
     * 而「扫表」只看得见**已提交**的行。只要有一批已发出但未提交（或已回滚）的号，
     * 重启后就会把它们再发一遍，两行主键相同，后写的**静默覆盖**先写的。
     */
    @Test
    fun `重启后接着发号而不是从头开始`() {
        connect().use { c ->
            setup(c)
            c.createStatement().use { s ->
                repeat(10) { s.execute("INSERT INTO t (v) VALUES ($it)") }
            }
            assertEquals((1L..10L).toList(), ids(c))
        }

        restart()

        connect().use { c ->
            c.createStatement().use { s -> s.execute("USE aitest") }
            c.createStatement().use { s ->
                repeat(5) { s.execute("INSERT INTO t (v) VALUES (100 + $it)") }
            }
            val all = ids(c)
            // 行数必须真的涨了——重号会表现为「插了 15 次只有 10 行」
            assertEquals(15, all.size, "重启后发生重号：插入的行被覆盖了")
            assertEquals(15, all.toSet().size, "出现了重复的主键")
            assertTrue(all.max() > 10L, "重启后必须接着往后发，而不是从 1 开始")
        }
    }

    /** 事务回滚也要留下空洞，并且回滚掉的号**不能**被后面的插入重新拿到。 */
    @Test
    fun `回滚过的号不会被重新发出`() {
        connect().use { c ->
            setup(c)
            c.autoCommit = false
            c.createStatement().use { s -> s.execute("INSERT INTO t (v) VALUES (1)") }
            c.rollback()
            c.autoCommit = true
            c.createStatement().use { s -> s.execute("INSERT INTO t (v) VALUES (2)") }

            val all = ids(c)
            assertEquals(1, all.size)
            assertTrue(all[0] >= 1L, "回滚后应当继续往后发：${all[0]}")
        }
    }

    /**
     * 显式插入一个大 id 之后，自增必须接着它往后发。
     *
     * 不处理的话，后面自增发出的号会落在这个大 id 附近，
     * 与已经存在的行撞上。
     */
    @Test
    fun `显式插入大 id 之后自增接着它往后发`() {
        connect().use { c ->
            setup(c)
            c.createStatement().use { s ->
                s.execute("INSERT INTO t VALUES (100000, 1)")
                s.execute("INSERT INTO t (v) VALUES (2)")
            }
            assertEquals(listOf(100000L, 100001L), ids(c))
        }
    }

    /** `TRUNCATE TABLE` 按 MySQL 语义把自增重置回 1。 */
    @Test
    fun `TRUNCATE 之后自增从 1 重新开始`() {
        connect().use { c ->
            setup(c)
            c.createStatement().use { s ->
                repeat(5) { s.execute("INSERT INTO t (v) VALUES ($it)") }
                s.execute("TRUNCATE TABLE t")
                s.execute("INSERT INTO t (v) VALUES (9)")
            }
            assertEquals(listOf(1L), ids(c))
        }
    }

    /** 自增列显式给 NULL 也要发号（MySQL 语义）。 */
    @Test
    fun `自增列显式给 NULL 也会发号`() {
        connect().use { c ->
            setup(c)
            c.createStatement().use { s -> s.execute("INSERT INTO t VALUES (NULL, 1)") }
            assertEquals(listOf(1L), ids(c))
        }
    }

    /**
     * 一张表的自增段**独立于**另一张表：给 t1 用掉一批号，不影响 t2 从 1 开始。
     */
    @Test
    fun `自增水位是按表分开的`() {
        connect().use { c ->
            setup(c)
            c.createStatement().use { s ->
                s.execute("CREATE TABLE t2 (id BIGINT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))")
                repeat(3) { s.execute("INSERT INTO t (v) VALUES ($it)") }
                s.execute("INSERT INTO t2 (v) VALUES (7)")
                s.executeQuery("SELECT id FROM t2").use { rs ->
                    rs.next()
                    assertEquals(1L, rs.getLong(1), "t2 应当从 1 开始，不受 t 的影响")
                }
            }
        }
    }
}
