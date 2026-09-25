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
 * `ALTER TABLE` 与 `SHOW CREATE TABLE`。
 *
 * 这两样此前**完全不存在**：解析器没有 ALTER 分支，`SHOW CREATE TABLE`
 * 落到兜底分支返回一个「内容写着暂未实现」的**成功结果集**——
 * 客户端会以为查到了 DDL，mysqldump 会拿它当真实建表语句用下去。
 *
 * schema 演化的范围严格按设计文档 §4.5：**只覆盖无重写的场景**
 * （加列加在末尾、删列删末尾）。行首记录了列数，因此这两种都是 O(1) 元数据操作，
 * 旧行读时自动补默认值或被截断。
 */
class AlterTableTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-alt")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "alt-test", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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
        "jdbc:mysql://127.0.0.1:$port/altdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
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

    private fun setup(c: Connection, ddl: String = "CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))") =
        c.st { s ->
            s.execute("CREATE DATABASE IF NOT EXISTS altdb")
            s.execute("USE altdb")
            s.execute("DROP TABLE IF EXISTS t")
            s.execute(ddl)
        }

    // ── ADD COLUMN ────────────────────────────────────────────────

    @Test
    fun `加列后旧行仍然读得出来 - 补默认值`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
                s.execute("ALTER TABLE t ADD COLUMN label VARCHAR(32) DEFAULT 'none'")
                s.execute("INSERT INTO t VALUES (3, 30, 'explicit')")

                // ★ 旧行必须读得出来，且新列拿到默认值
                assertEquals("none", s.rows("SELECT label FROM t WHERE id = 1")[0][0])
                assertEquals("none", s.rows("SELECT label FROM t WHERE id = 2")[0][0])
                assertEquals("explicit", s.rows("SELECT label FROM t WHERE id = 3")[0][0])
                assertEquals(3, s.rows("SELECT id FROM t").size)
                assertEquals(10L, s.rows("SELECT v FROM t WHERE id = 1")[0][0])
            }
        }
    }

    @Test
    fun `加可空列时旧行补 NULL`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1, 10)")
                s.execute("ALTER TABLE t ADD COLUMN extra INT")
                assertEquals(null, s.rows("SELECT extra FROM t WHERE id = 1")[0][0])
            }
        }
    }

    @Test
    fun `不可空又没默认值的加列被拒绝`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1, 10)")
                // 旧行没有这一列，补出来只能是 NULL，那会违反 NOT NULL。
                // 必须在 DDL 时拦住，而不是留下违反约束的数据
                val e = assertFailsWith<SQLException> { s.execute("ALTER TABLE t ADD COLUMN x INT NOT NULL") }
                assertTrue(e.message!!.contains("DEFAULT"), "错误信息应指出解法：${e.message}")
            }
        }
    }

    @Test
    fun `空表上加 NOT NULL 无默认值是允许的`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                // 没有旧行，就没有"补不出值"的问题
                s.execute("ALTER TABLE t ADD COLUMN x INT NOT NULL")
                s.execute("INSERT INTO t VALUES (1, 10, 5)")
                assertEquals(5L, s.rows("SELECT x FROM t WHERE id = 1")[0][0])
            }
        }
    }

    @Test
    fun `加在中间被明确拒绝`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, a INT, b INT, PRIMARY KEY (id))")
            c.st { s ->
                val e1 = assertFailsWith<SQLException> { s.execute("ALTER TABLE t ADD COLUMN x INT FIRST") }
                assertTrue(e1.message!!.contains("末尾"), "${e1.message}")
                // AFTER 只有指向末尾列时才可以
                assertFailsWith<SQLException> { s.execute("ALTER TABLE t ADD COLUMN x INT AFTER id") }
                s.execute("ALTER TABLE t ADD COLUMN x INT AFTER b")   // b 是末尾，允许
            }
        }
    }

    // ── DROP COLUMN ───────────────────────────────────────────────

    @Test
    fun `删末尾列后旧行仍然读得出来`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, a INT, b INT, PRIMARY KEY (id))")
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1, 10, 20)")
                s.execute("ALTER TABLE t DROP COLUMN b")
                // 旧行有 3 列而 schema 只有 2 列——多出来的字节直接忽略
                assertEquals(10L, s.rows("SELECT a FROM t WHERE id = 1")[0][0])
                s.execute("INSERT INTO t VALUES (2, 30)")
                assertEquals(2, s.rows("SELECT id FROM t").size)
            }
        }
    }

    @Test
    fun `删中间列被明确拒绝`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, a INT, b INT, PRIMARY KEY (id))")
            c.st { s ->
                val e = assertFailsWith<SQLException> { s.execute("ALTER TABLE t DROP COLUMN a") }
                assertTrue(e.message!!.contains("末尾"), "${e.message}")
            }
        }
    }

    @Test
    fun `不能删主键列或被索引引用的列`() {
        connect().use { c ->
            // 把要删的列放在**末尾**，这样才会走到「主键/索引」这两道校验，
            // 而不是先被「只能删末尾列」挡住
            setup(c, "CREATE TABLE t (a INT, id BIGINT NOT NULL, PRIMARY KEY (a, id))")
            c.st { s ->
                val e1 = assertFailsWith<SQLException> { s.execute("ALTER TABLE t DROP COLUMN id") }
                assertTrue(e1.message!!.contains("主键"), "${e1.message}")
            }
        }
        connect().use { c2 ->
            c2.st { s ->
                s.execute("DROP TABLE IF EXISTS t")
                s.execute("CREATE TABLE t (id BIGINT NOT NULL, a INT, PRIMARY KEY (id), KEY ia (a))")
                // a 在末尾且被索引引用
                val e2 = assertFailsWith<SQLException> { s.execute("ALTER TABLE t DROP COLUMN a") }
                assertTrue(e2.message!!.contains("索引"), "${e2.message}")
            }
        }
    }

    // ── 索引 DDL ──────────────────────────────────────────────────

    @Test
    fun `ALTER TABLE ADD INDEX 会回填已有数据`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,10),(2,20),(3,10)")
                s.execute("ALTER TABLE t ADD INDEX idx_v (v)")
                assertEquals(setOf(1L, 3L), s.rows("SELECT id FROM t WHERE v = 10").map { it[0] }.toSet())
            }
        }
    }

    @Test
    fun `ALTER TABLE DROP INDEX 之后查询仍然正确`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id), KEY idx_v (v))")
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,10),(2,20)")
                s.execute("ALTER TABLE t DROP INDEX idx_v")
                // 没有索引就走全表扫描，结果必须一样
                assertEquals(listOf(1L), s.rows("SELECT id FROM t WHERE v = 10").map { it[0] })
                assertFailsWith<SQLException> { s.execute("ALTER TABLE t DROP INDEX idx_v") }
            }
        }
    }

    // ── TRUNCATE ──────────────────────────────────────────────────

    @Test
    fun `TRUNCATE 清空数据但保留表`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,10),(2,20)")
                c.createStatement().use { st -> st.execute("TRUNCATE TABLE t") }
                assertEquals(0, s.rows("SELECT id FROM t").size)
                assertEquals(0L, s.rows("SELECT COUNT(*) FROM t")[0][0])
                // 表还在，还能插入
                s.execute("INSERT INTO t VALUES (3, 30)")
                assertEquals(1, s.rows("SELECT id FROM t").size)
            }
        }
    }

    // ── SHOW CREATE TABLE ─────────────────────────────────────────

    @Test
    fun `SHOW CREATE TABLE 返回可重新执行的 DDL`() {
        connect().use { c ->
            setup(
                c,
                """
                CREATE TABLE t (
                    id BIGINT NOT NULL, email VARCHAR(64), amount DECIMAL(12,2), n INT,
                    PRIMARY KEY (id), UNIQUE KEY uk_email (email), KEY idx_n (n)
                )
                """.trimIndent(),
            )
            c.st { s ->
                val ddl = s.rows("SHOW CREATE TABLE t")[0][1] as String
                assertTrue(ddl.contains("CREATE TABLE"), ddl)
                assertTrue(ddl.contains("`id`"), ddl)
                assertTrue(ddl.contains("PRIMARY KEY"), ddl)
                assertTrue(ddl.contains("UNIQUE KEY"), ddl)

                // ★ 真正的验收：把导出的 DDL 拿去建另一张表，必须成功
                val renamed = ddl.replaceFirst("CREATE TABLE `t`", "CREATE TABLE `t2`")
                s.execute(renamed)
                val cols = s.rows("SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = 't2'")
                assertEquals(listOf("id", "email", "amount", "n"), cols.map { it[0] })
            }
        }
    }

    @Test
    fun `SHOW CREATE TABLE 保留分区定义`() {
        connect().use { c ->
            setup(
                c,
                """
                CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))
                PARTITION BY RANGE (id) (
                    PARTITION p0 VALUES LESS THAN (100),
                    PARTITION pmax VALUES LESS THAN MAXVALUE
                )
                """.trimIndent(),
            )
            c.st { s ->
                val ddl = s.rows("SHOW CREATE TABLE t")[0][1] as String
                assertTrue(ddl.contains("PARTITION BY RANGE"), ddl)
                assertTrue(ddl.contains("p0"), ddl)
            }
        }
    }

    @Test
    fun `SHOW INDEX 与 SHOW COLUMNS 可用`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id), KEY idx_v (v))")
            c.st { s ->
                val idx = s.rows("SHOW INDEX FROM t")
                assertEquals(2, idx.size, "主键 + idx_v")
                assertTrue(idx.any { it[2] == "PRIMARY" })

                val cols = s.rows("SHOW COLUMNS FROM t")
                assertEquals(listOf("id", "v"), cols.map { it[0] })
            }
        }
    }

    @Test
    fun `不支持的 SHOW 明确报错而不是返回占位内容`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                // 静默返回「内容写着暂未实现」的成功结果集，会让工具链
                // 以为查到了东西，然后在很远的地方以更奇怪的方式失败
                assertFailsWith<SQLException> { s.executeQuery("SHOW GRANTS") }
            }
        }
    }
}
