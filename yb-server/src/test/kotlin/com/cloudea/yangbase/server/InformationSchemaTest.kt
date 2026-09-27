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
 * `information_schema` 测试。
 *
 * 这个文件存在的原因是：**数据库客户端连接后的第一件事就是查系统库**。
 * 在此之前 `SELECT ... FROM information_schema.tables` 会被当成用户表查找，
 * 直接报「表不存在」——协议层连得上，DataGrip / Navicat / DBeaver
 * 这类工具却完全用不了。
 */
class InformationSchemaTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-info")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "info-test", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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
        "jdbc:mysql://127.0.0.1:$port/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun Connection.st(block: (Statement) -> Unit) = createStatement().use(block)

    private fun Statement.query(sql: String): List<List<Any?>> =
        executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            val out = ArrayList<List<Any?>>()
            while (rs.next()) out.add((1..n).map { rs.getObject(it) })
            out
        }

    private fun Connection.st() = createStatement()

    // ── 它必须**像一个 schema**（不只是"能查"）────────────────────

    /**
     * ★ `information_schema` 必须能像真实 MySQL 那样被"进去"。
     *
     * 只支持 `SELECT ... FROM information_schema.x` 是不够的：真实 MySQL 里它就是一个
     * schema——`SHOW DATABASES` 列它、`USE information_schema` 进得去、进去之后
     * `SHOW TABLES` 与**不带库名的** `SELECT ... FROM tables` 都能用。
     * 手敲命令的人和靠"看得见的库列表"工作的工具都按这个来。
     */
    @Test
    fun `能像 schema 一样使用 information_schema`() {
        connect().use { c ->
            // 先造一张用户表：否则 `information_schema.tables` **本来就该是空的**，
            // 下面"不带库名的查询能取到行"这条断言会在一个正确实现上失败（第一版就是这样）
            c.st { it.execute("CREATE DATABASE IF NOT EXISTS isdb") }
            c.st { it.execute("USE isdb") }
            c.st { it.execute("DROP TABLE IF EXISTS probe") }
            c.st { it.execute("CREATE TABLE probe (id BIGINT PRIMARY KEY)") }

            // 出现在库列表里（`mysqldump --all-databases` 也靠这个名字跳过系统库）
            val dbs = c.st().query("SHOW DATABASES").map { it[0].toString() }
            assertTrue(dbs.contains("information_schema"), "SHOW DATABASES 应当列出系统库：$dbs")

            // 以它为默认库连上来 + USE 进去，两条路都要通
            val viaUrl = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:$port/information_schema?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "root", "",
            )
            viaUrl.use { ic ->
                assertEquals("information_schema", ic.st().query("SELECT DATABASE()")[0][0].toString())

                // 进去之后：SHOW TABLES 列出物化的虚拟表
                val tables = ic.st().query("SHOW TABLES").map { it[0].toString() }
                assertTrue(tables.contains("tables") && tables.contains("columns"), "应列出虚拟表：$tables")

                // **不带库名**的查询要能解析到虚拟表上
                assertTrue(ic.st().query("SELECT COUNT(*) FROM schemata")[0][0] is Number)
                assertTrue(ic.st().query("SELECT table_name FROM tables").isNotEmpty())

                // 限定名照旧
                assertTrue(ic.st().query("SELECT COUNT(*) FROM information_schema.columns")[0][0] is Number)
            }
            c.st { it.execute("USE information_schema") }
        }
    }

    @Test
    fun `系统库只读且错误指向真正的原因`() {
        connect().use { c ->
            c.st { it.execute("USE information_schema") }
            for (sql in listOf(
                "INSERT INTO information_schema.tables VALUES (1)",
                "UPDATE information_schema.tables SET table_name = 'x'",
                "DROP TABLE information_schema.tables",
            )) {
                val e = runCatching { c.st { it.execute(sql) } }.exceptionOrNull()
                assertTrue(e is java.sql.SQLException, "写系统库必须报错：$sql")
                // 不能是"没有选中数据库"——那会让人以为是自己忘了 USE，
                // 而 `USE information_schema` 那一步其实成功了
                assertTrue(
                    e.message!!.contains("只读的系统库"),
                    "错误要指向真正的原因（只读系统库），实际：${e.message}",
                )
            }
        }
    }

    @Test
    fun `USE 一个不存在的库要报错而不是静默成功`() {
        connect().use { c ->
            val e = runCatching { c.st { it.execute("USE no_such_db_here") } }.exceptionOrNull()
            assertTrue(e is java.sql.SQLException, "USE 不存在的库必须报错（静默成功会让后面每条语句都报表不存在）")
        }
    }

    @Test
    fun `SCHEMATA 列出全部库`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS info_a")
                s.execute("CREATE DATABASE IF NOT EXISTS info_b")
                val names = s.query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA")
                    .map { it[0] as String }
                assertTrue("info_a" in names && "info_b" in names, "应列出刚建的两个库，实际 $names")
            }
        }
    }

    @Test
    fun `TABLES 与 COLUMNS 反映真实 schema`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS info_c")
                s.execute("USE info_c")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute(
                    """
                    CREATE TABLE t (
                        id BIGINT NOT NULL,
                        email VARCHAR(64),
                        amount DECIMAL(12,2),
                        PRIMARY KEY (id),
                        UNIQUE KEY uk_email (email)
                    )
                    """.trimIndent()
                )

                val tbl = s.query(
                    "SELECT TABLE_NAME, TABLE_TYPE, ENGINE FROM information_schema.TABLES " +
                        "WHERE TABLE_SCHEMA = 'info_c'"
                )
                assertEquals(1, tbl.size, "info_c 下应只有一张表")
                assertEquals("t", tbl[0][0])
                assertEquals("BASE TABLE", tbl[0][1])

                val cols = s.query(
                    "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_TYPE " +
                        "FROM information_schema.COLUMNS WHERE TABLE_NAME = 't' ORDER BY ORDINAL_POSITION"
                )
                assertEquals(listOf("id", "email", "amount"), cols.map { it[0] })
                assertEquals(listOf("bigint", "varchar", "decimal"), cols.map { it[1] })
                assertEquals(listOf("NO", "YES", "YES"), cols.map { it[2] })
                assertEquals("PRI", cols[0][3])
                assertEquals("UNI", cols[1][3], "唯一索引列应标为 UNI")
                assertEquals("varchar(64)", cols[1][4])
                assertEquals("decimal(12,2)", cols[2][4])
            }
        }
    }

    @Test
    fun `STATISTICS 列出主键与二级索引`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS info_d")
                s.execute("USE info_d")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute(
                    """
                    CREATE TABLE t (
                        id BIGINT NOT NULL, a INT, b INT,
                        PRIMARY KEY (id), KEY idx_a (a), KEY idx_ab (a, b)
                    )
                    """.trimIndent()
                )
                val rows = s.query(
                    "SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, INDEX_TYPE " +
                        "FROM information_schema.STATISTICS WHERE TABLE_NAME = 't' ORDER BY INDEX_NAME, SEQ_IN_INDEX"
                )
                val byIndex = rows.groupBy { it[0] as String }
                assertTrue("PRIMARY" in byIndex, "必须报告主键索引")
                assertEquals(listOf("a", "b"), byIndex["idx_ab"]!!.map { it[1] })
                assertEquals(listOf(1L, 2L), byIndex["idx_ab"]!!.map { it[2] })
                assertEquals(0L, byIndex["PRIMARY"]!![0][3], "主键是唯一的，NON_UNIQUE 应为 0")
                assertEquals(1L, byIndex["idx_ab"]!![0][3], "非唯一索引的 NON_UNIQUE 应为 1")
            }
        }
    }

    @Test
    fun `KEY_COLUMN_USAGE 与 TABLE_CONSTRAINTS`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS info_e")
                s.execute("USE info_e")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute(
                    """
                    CREATE TABLE t (
                        id BIGINT NOT NULL, email VARCHAR(64),
                        PRIMARY KEY (id), UNIQUE KEY uk_email (email)
                    )
                    """.trimIndent()
                )
                val kcu = s.query(
                    "SELECT CONSTRAINT_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE " +
                        "WHERE TABLE_NAME = 't' ORDER BY CONSTRAINT_NAME"
                )
                assertEquals(listOf("PRIMARY", "uk_email"), kcu.map { it[0] })

                val tc = s.query(
                    "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM information_schema.TABLE_CONSTRAINTS " +
                        "WHERE TABLE_NAME = 't' ORDER BY CONSTRAINT_NAME"
                )
                assertEquals(listOf("PRIMARY", "uk_email"), tc.map { it[0] })
                assertEquals(listOf("PRIMARY KEY", "UNIQUE"), tc.map { it[1] })
            }
        }
    }

    @Test
    fun `PARTITIONS 反映分区表`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS info_f")
                s.execute("USE info_f")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute(
                    """
                    CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))
                    PARTITION BY RANGE (id) (
                        PARTITION p0 VALUES LESS THAN (100),
                        PARTITION p1 VALUES LESS THAN (200),
                        PARTITION pmax VALUES LESS THAN MAXVALUE
                    )
                    """.trimIndent()
                )
                val rows = s.query(
                    "SELECT PARTITION_NAME, PARTITION_METHOD, PARTITION_EXPRESSION, PARTITION_DESCRIPTION " +
                        "FROM information_schema.PARTITIONS WHERE TABLE_NAME = 't' " +
                        "ORDER BY PARTITION_ORDINAL_POSITION"
                )
                assertEquals(listOf("p0", "p1", "pmax"), rows.map { it[0] })
                assertEquals("RANGE", rows[0][1])
                assertEquals("id", rows[0][2])
                assertEquals("MAXVALUE", rows[2][3])
            }
        }
    }

    @Test
    fun `系统库上的 WHERE 与 LIMIT 生效`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS info_g")
                s.execute("CREATE DATABASE IF NOT EXISTS info_h")
                val all = s.query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").size
                val limited = s.query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA LIMIT 1").size
                assertEquals(1, limited)
                assertTrue(all >= 2)

                val one = s.query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'info_g'")
                assertEquals(1, one.size)
                assertEquals("info_g", one[0][0])
            }
        }
    }

    @Test
    fun `限定名写法可用`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS info_i")
                s.execute("USE info_i")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute("CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id))")
                // 客户端做 introspection 时的常见写法：显式限定库名
                val r = s.query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'info_i'")
                assertEquals(listOf("t"), r.map { it[0] })
            }
        }
    }

    @Test
    fun `未知的系统表返回空集而不是报错`() {
        connect().use { c ->
            c.st { s ->
                // 客户端会探测各种表；报「表不存在」会让它们反复重试甚至放弃连接
                assertEquals(0, s.query("SELECT * FROM information_schema.SOMETHING_NEW").size)
                assertEquals(0, s.query("SELECT * FROM performance_schema.SESSION_VARIABLES").size)
                assertEquals(0, s.query("SELECT * FROM mysql.user").size)
            }
        }
    }

    @Test
    fun `ENGINES 报告引擎存在`() {
        connect().use { c ->
            c.st { s ->
                val r = s.query("SELECT ENGINE, SUPPORT FROM information_schema.ENGINES")
                assertEquals(1, r.size)
                assertEquals("DEFAULT", r[0][1], "必须有一个 DEFAULT 引擎，否则客户端认为没有可用引擎")
            }
        }
    }

    @Test
    fun `TABLE_ROWS 在分片已打开时给出精确值`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS info_j")
                s.execute("USE info_j")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute("CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id))")
                s.execute("INSERT INTO t VALUES (1),(2),(3)")
                // 查询本身会打开分片，因此随后 information_schema 能给出精确行数
                assertEquals(3, s.query("SELECT id FROM t").size)
                val rows = s.query("SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_NAME = 't'")
                assertEquals(3L, rows[0][0])
            }
        }
    }
}
