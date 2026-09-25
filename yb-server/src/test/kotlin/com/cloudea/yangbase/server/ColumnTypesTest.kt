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
 * `ENUM` / `SET` / `YEAR` / `JSON`（设计文档 §11.1 的类型一行）。
 *
 * ## 为什么这几类值的错误是「静默」的
 *
 * 值域封闭的列（ENUM/SET）如果**不校验**，非法值会被照收：写进去的时候不报错，
 * 之后 `WHERE col = 'x'` 永远查不到那一行——看起来就是"数据丢了"。
 * `SET` 还多一层：MySQL 的显示顺序恒为**声明顺序**，不规范化的话
 * 存 `'b,a'` 读回 `'b,a'`、而 `WHERE s = 'a,b'` 查不到。
 *
 * 所以这组用例的重点不在"能不能建表"，而在**非法值必须被拒、值必须被规范化**。
 */
class ColumnTypesTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-types")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "types", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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
        "jdbc:mysql://127.0.0.1:$port/tdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun <T> Connection.q(b: (Statement) -> T): T = createStatement().use(b)

    private fun setup(c: Connection, ddl: String) = c.q { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS tdb")
        s.execute("USE tdb")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute(ddl)
    }

    private fun count(c: Connection, sql: String): Long = c.q { s ->
        s.executeQuery(sql).use { rs -> if (rs.next()) rs.getLong(1) else 0L }
    }

    private fun str(c: Connection, sql: String): String? = c.q { s ->
        s.executeQuery(sql).use { rs -> if (rs.next()) rs.getString(1) else null }
    }

    // ── ENUM ──────────────────────────────────────────────────────

    @Test
    fun `ENUM 存取值与显示`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, s ENUM('active','paused','done'), PRIMARY KEY (id))")
            c.q { s -> s.execute("INSERT INTO t VALUES (1,'active'),(2,'done')") }
            assertEquals("active", str(c, "SELECT s FROM t WHERE id = 1"))
            assertEquals("done", str(c, "SELECT s FROM t WHERE id = 2"))
            // 等值查询必须命中
            assertEquals(1L, count(c, "SELECT COUNT(*) FROM t WHERE s = 'done'"))
        }
    }

    /**
     * ★ 非法成员必须**被拒**。
     *
     * 悄悄收下的后果：写进去不报错，之后按值查永远查不到——
     * 使用者会以为数据丢了，而不是以为写错了。
     */
    @Test
    fun `ENUM 的非成员值被拒绝`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, s ENUM('a','b'), PRIMARY KEY (id))")
            val e = assertFailsWith<SQLException> { c.q { s -> s.execute("INSERT INTO t VALUES (1,'c')") } }
            assertTrue(e.message!!.contains("ENUM"), "错误信息应指出列的类型：${e.message}")
            assertEquals(0L, count(c, "SELECT COUNT(*) FROM t"))
        }
    }

    /** ENUM 的 NULL 仍然允许（列可空时）。 */
    @Test
    fun `ENUM 允许 NULL`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, s ENUM('a','b'), PRIMARY KEY (id))")
            c.q { s -> s.execute("INSERT INTO t VALUES (1,NULL)") }
            assertEquals(null, str(c, "SELECT s FROM t WHERE id = 1"))
        }
    }

    // ── SET ───────────────────────────────────────────────────────

    /**
     * ★ `SET` 的值必须按**声明顺序**规范化。
     *
     * MySQL 的 `SET` 显示顺序恒为声明顺序。不规范化的直接后果是
     * `WHERE s = 'a,b'` 对存进去的 `'b,a'` 失配——按值查不到。
     */
    @Test
    fun `SET 的值按声明顺序规范化`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, s SET('a','b','c'), PRIMARY KEY (id))")
            c.q { s -> s.execute("INSERT INTO t VALUES (1,'c,a')") }
            assertEquals("a,c", str(c, "SELECT s FROM t WHERE id = 1"), "SET 必须按声明顺序显示")
            // 规范化之后，按规范化形式查询必须命中
            assertEquals(1L, count(c, "SELECT COUNT(*) FROM t WHERE s = 'a,c'"))
        }
    }

    @Test
    fun `SET 的非成员被拒绝`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, s SET('a','b'), PRIMARY KEY (id))")
            assertFailsWith<SQLException> { c.q { s -> s.execute("INSERT INTO t VALUES (1,'a,z')") } }
            c.q { s -> s.execute("INSERT INTO t VALUES (2,'a,b')") }     // 合法组合仍然可以
        }
    }

    // ── YEAR ──────────────────────────────────────────────────────

    @Test
    fun `YEAR 是整数且能比较与排序`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, y YEAR, PRIMARY KEY (id))")
            c.q { s -> s.execute("INSERT INTO t VALUES (1,1998),(2,2024),(3,2008)") }
            assertEquals(1998L, str(c, "SELECT y FROM t WHERE id = 1")?.toLong())
            c.q { s ->
                s.executeQuery("SELECT id FROM t WHERE y > 2000 ORDER BY y").use { rs ->
                    val ids = ArrayList<Long>()
                    while (rs.next()) ids.add(rs.getLong(1))
                    assertEquals(listOf(3L, 2L), ids, "YEAR 的比较必须按数值")
                }
            }
        }
    }

    // ── JSON ──────────────────────────────────────────────────────

    @Test
    fun `JSON 列存取合法文档`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, j JSON, PRIMARY KEY (id))")
            c.q { s -> s.execute("""INSERT INTO t VALUES (1,'{"a":1,"b":[2,3]}')""") }
            assertEquals("""{"a":1,"b":[2,3]}""", str(c, "SELECT j FROM t WHERE id = 1"))
        }
    }

    /**
     * ★ 非法 JSON 必须被拒。MySQL 在严格模式下也是报错的
     * （错误码 3140 / `Invalid JSON text`）。
     */
    @Test
    fun `非法 JSON 被拒绝`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, j JSON, PRIMARY KEY (id))")
            val e = assertFailsWith<SQLException> {
                c.q { s -> s.execute("INSERT INTO t VALUES (1,'{not json')") }
            }
            assertTrue(e.message!!.contains("JSON"), "错误信息应指出是 JSON 问题：${e.message}")
            // 标量也是合法 JSON（MySQL 允许）
            c.q { s -> s.execute("""INSERT INTO t VALUES (2,'42')""") }
        }
    }

    // ── 元数据 ────────────────────────────────────────────────────

    /**
     * `SHOW CREATE TABLE` 必须写出成员表。
     *
     * 漏掉的话，`mysqldump` 拿这份 DDL 去重建表会把整个值域丢掉——
     * 重建出来的表能存任意字符串，而使用者以为它还是 ENUM。
     */
    @Test
    fun `SHOW CREATE TABLE 与 information_schema 带出成员表`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, s ENUM('x','y'), PRIMARY KEY (id))")
            c.q { s ->
                s.executeQuery("SHOW CREATE TABLE t").use { rs ->
                    rs.next()
                    val ddl = rs.getString(2)
                    assertTrue(ddl.contains("enum('x','y')"), "DDL 里必须带上成员表：$ddl")
                }
            }
            assertEquals(
                "enum('x','y')",
                str(c, "SELECT COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME='t' AND COLUMN_NAME='s'"),
            )
        }
    }

    /** `DROP COLUMN` 保护要把 ENUM 列也算进去（它被索引引用时不能删）。 */
    @Test
    fun `ENUM 列可以建索引并走索引查找`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, s ENUM('a','b'), PRIMARY KEY (id), KEY is_ (s))")
            c.q { s -> s.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a')") }
            c.q { s ->
                s.executeQuery("SELECT id FROM t WHERE s = 'a' ORDER BY id").use { rs ->
                    val ids = ArrayList<Long>()
                    while (rs.next()) ids.add(rs.getLong(1))
                    assertEquals(listOf(1L, 3L), ids)
                }
            }
        }
    }
}
