package com.cloudea.yangbase.server

import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 不支持的语法必须**报错**，不能静默忽略。
 *
 * 这个文件守护的是一整类最危险的缺陷：解析器不认识某个子句时，
 * 如果只是把残留 token 丢掉、让语句照常执行，使用者会拿到一个
 * **看起来完全正常、但是错的**结果——`GROUP BY` 没分组、
 * `FOR UPDATE` 没加锁、`INTO OUTFILE` 没导出。
 *
 * 一个明确的「不支持」比一个静默的错误答案好得多。
 */
class UnsupportedSyntaxTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-syn")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "syn-test", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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
        "jdbc:mysql://127.0.0.1:$port/syndb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun Connection.st(block: (Statement) -> Unit) = createStatement().use(block)

    private fun <T> Connection.q(block: (Statement) -> T): T = createStatement().use(block)

    private fun Statement.query(sql: String): List<List<Any?>> =
        executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            val out = ArrayList<List<Any?>>()
            while (rs.next()) out.add((1..n).map { rs.getObject(it) })
            out
        }

    private fun setup(c: Connection) = c.st { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS syndb")
        s.execute("USE syndb")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute("CREATE TABLE t (id BIGINT NOT NULL, city VARCHAR(16), PRIMARY KEY (id))")
        s.execute("INSERT INTO t VALUES (1,'bj'),(2,'bj'),(3,'sh')")
    }

    /**
     * 这些子句都会改变结果集语义。静默忽略 = 返回错误的行。
     *
     * ⚠️ **这份清单会随着能力补齐而变短**，而且它必须变短：
     * `GROUP BY` / `HAVING` / `JOIN` / `UNION` / `IN (子查询)` 都已经实现了，
     * 它们各自的端到端断言在 [SqlSurfaceTest] 里。仍然留在清单里的，
     * 是**真的没有实现**的那些——留着它们是为了防止哪天有人"顺手"把它们
     * 变成静默忽略（那会让 `FOR UPDATE` 少一层锁、`INTO OUTFILE` 少一个文件，
     * 而语句本身返回成功）。
     */
    private val mustFail = listOf(
        // `FOR UPDATE` / `LOCK IN SHARE MODE` **已经实现**（行锁走协调服务，见 SqlSurfaceTest
        // 的 `FOR UPDATE` 用例）。但它们的**修饰符**不支持，而且忽略它们会改变语义：
        // `NOWAIT` 会让"等不到"变成"立刻失败"，`SKIP LOCKED` 会让行被跳过
        "SELECT id FROM t ORDER BY id LIMIT 1 FOR UPDATE NOWAIT",
        "SELECT id FROM t FOR UPDATE SKIP LOCKED",
        "SELECT id FROM t INTO OUTFILE '/tmp/x'",
        // 相关子查询的**谓词位置**（改写成半/反连接）与**标量位置**
        // （按关联键建表再查，见 CorrelatedScalars）都已经支持，端到端断言在 SqlSurfaceTest。
        // 仍然不支持的是藏在 `OR` 分支里的相关子查询：`OR` 的两侧不能各自改写成连接，
        // 而它必须报错而不是给一个错的答案
        "SELECT id FROM t x WHERE id = 1 OR EXISTS (SELECT 1 FROM t y WHERE y.id = x.id)",
        // USING / NATURAL 改变的是"连接列怎么找"，忽略它们会把外连接降级成笛卡尔积
        "SELECT id FROM t JOIN t2 USING (id)",
    )

    @Test
    fun `不支持的子句必须报错而不是静默返回错误结果`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                for (sql in mustFail) {
                    val e = assertFailsWith<SQLException>("这条语句必须被拒绝: $sql") { s.query(sql) }
                    assertTrue(
                        e.message!!.contains("不支持") || e.message!!.contains("语法"),
                        "错误信息应说明是不支持的语法: $sql -> ${e.message}",
                    )
                }
            }
        }
    }

    @Test
    fun `被拒绝的语句不得改变任何数据`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                assertFailsWith<SQLException> { s.execute("DELETE FROM t WHERE id = 1 ORDER BY id LIMIT 1") }
                // 上面这条要么执行（删掉一行），要么报错。无论哪种，都不能
                // 出现「报错了但数据变了」或「没报错但语义不对」
                assertEquals(3, s.query("SELECT id FROM t").size)
            }
        }
    }

    @Test
    fun `支持的子句仍然正常`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                assertEquals(3, s.query("SELECT id FROM t").size)
                assertEquals(3, s.query("SELECT id FROM t ORDER BY id").size)
                assertEquals(2, s.query("SELECT id FROM t LIMIT 2").size)
                assertEquals(1, s.query("SELECT id FROM t LIMIT 1 OFFSET 1").size)
                assertEquals(2, s.query("SELECT id FROM t WHERE city = 'bj'").size)
                assertEquals(2L, s.query("SELECT COUNT(*) FROM t WHERE city = 'bj'")[0][0])
                // 带尾分号
                assertEquals(3, s.query("SELECT id FROM t;").size)
            }
        }
    }
}
