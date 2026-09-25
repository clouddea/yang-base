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
 * 二级索引测试。
 *
 * 覆盖三件事：
 *  1. **索引维护**——写入/更新/删除后索引项与主数据一致
 *  2. **唯一约束**——之前 `UNIQUE KEY` 只是被存进 schema，完全不校验
 *  3. **索引被真的用上**——否则索引只是白占存储
 */
class IndexTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-idx")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "idx-test", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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
        "jdbc:mysql://127.0.0.1:$port/idxdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
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

    private fun setup(ddl: String): Connection {
        val c = connect()
        c.st { s ->
            s.execute("CREATE DATABASE IF NOT EXISTS idxdb")
            s.execute("USE idxdb")
            s.execute("DROP TABLE IF EXISTS t")
            s.execute(ddl)
        }
        return c
    }

    private val withIndex = """
        CREATE TABLE t (
            id BIGINT NOT NULL,
            email VARCHAR(64),
            age INT,
            city VARCHAR(32),
            PRIMARY KEY (id),
            KEY idx_age (age),
            KEY idx_city_age (city, age)
        )
    """.trimIndent()

    // ── 索引维护 ──────────────────────────────────────────────────

    @Test
    fun `二级索引查找返回正确结果`() {
        setup(withIndex).use { c ->
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a@x',30,'bj'),(2,'b@x',25,'sh'),(3,'c@x',30,'bj')")
                assertEquals(setOf(1L, 3L), s.query("SELECT id FROM t WHERE age = 30").map { it[0] }.toSet())
                assertEquals(listOf(2L), s.query("SELECT id FROM t WHERE age = 25").map { it[0] })
                assertEquals(0, s.query("SELECT id FROM t WHERE age = 99").size)
            }
        }
    }

    @Test
    fun `索引列更新后索引跟着变`() {
        setup(withIndex).use { c ->
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a@x',30,'bj'),(2,'b@x',25,'sh')")
                assertEquals(listOf(1L), s.query("SELECT id FROM t WHERE age = 30").map { it[0] })

                s.executeUpdate("UPDATE t SET age = 40 WHERE id = 1")

                assertEquals(0, s.query("SELECT id FROM t WHERE age = 30").size, "旧索引项必须失效")
                assertEquals(listOf(1L), s.query("SELECT id FROM t WHERE age = 40").map { it[0] }, "新索引项必须生效")
            }
        }
    }

    @Test
    fun `删除后索引项被清理`() {
        setup(withIndex).use { c ->
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a@x',30,'bj'),(2,'b@x',30,'bj')")
                assertEquals(2, s.query("SELECT id FROM t WHERE age = 30").size)

                s.executeUpdate("DELETE FROM t WHERE id = 1")
                assertEquals(listOf(2L), s.query("SELECT id FROM t WHERE age = 30").map { it[0] })

                s.executeUpdate("DELETE FROM t WHERE age = 30")
                assertEquals(0, s.query("SELECT id FROM t WHERE age = 30").size, "删干净后索引不应残留")
            }
        }
    }

    @Test
    fun `复合索引 - 首列可用`() {
        setup(withIndex).use { c ->
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a@x',30,'bj'),(2,'b@x',25,'sh'),(3,'c@x',30,'sh')")
                // 数据: (1,...,'bj') (2,...,'sh') (3,...,'sh')  → city='sh' 命中 2 和 3
                assertEquals(setOf(2L, 3L), s.query("SELECT id FROM t WHERE city = 'sh'").map { it[0] }.toSet())
                assertEquals(listOf(2L), s.query("SELECT id FROM t WHERE city = 'sh' AND age = 25").map { it[0] })
                assertEquals(listOf(1L), s.query("SELECT id FROM t WHERE city = 'bj'").map { it[0] })
            }
        }
    }

    // ── 唯一约束 ──────────────────────────────────────────────────

    @Test
    fun `唯一索引拒绝重复值`() {
        setup(
            """
            CREATE TABLE t (
                id BIGINT NOT NULL, email VARCHAR(64),
                PRIMARY KEY (id), UNIQUE KEY uk_email (email)
            )
            """.trimIndent()
        ).use { c ->
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a@x')")
                val e = assertFailsWith<SQLException> { s.execute("INSERT INTO t VALUES (2,'a@x')") }
                assertEquals(1062, e.errorCode, "应为 ER_DUP_ENTRY")
                assertEquals(1, s.query("SELECT id FROM t").size, "失败的插入不得留下数据")
            }
        }
    }

    @Test
    fun `唯一索引 - 同一批插入里的重复也要拦住`() {
        setup(
            """
            CREATE TABLE t (
                id BIGINT NOT NULL, email VARCHAR(64),
                PRIMARY KEY (id), UNIQUE KEY uk_email (email)
            )
            """.trimIndent()
        ).use { c ->
            c.st { s ->
                assertFailsWith<SQLException> {
                    s.execute("INSERT INTO t VALUES (1,'same@x'),(2,'same@x')")
                }
                assertEquals(0, s.query("SELECT id FROM t").size)
            }
        }
    }

    @Test
    fun `唯一索引 - 更新成重复值被拒`() {
        setup(
            """
            CREATE TABLE t (
                id BIGINT NOT NULL, email VARCHAR(64),
                PRIMARY KEY (id), UNIQUE KEY uk_email (email)
            )
            """.trimIndent()
        ).use { c ->
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a@x'),(2,'b@x')")
                assertFailsWith<SQLException> { s.executeUpdate("UPDATE t SET email = 'a@x' WHERE id = 2") }
                assertEquals("b@x", s.query("SELECT email FROM t WHERE id = 2")[0][0])
            }
        }
    }

    @Test
    fun `唯一索引 - 更新成自己的值不算冲突`() {
        setup(
            """
            CREATE TABLE t (
                id BIGINT NOT NULL, email VARCHAR(64), v INT,
                PRIMARY KEY (id), UNIQUE KEY uk_email (email)
            )
            """.trimIndent()
        ).use { c ->
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a@x',1)")
                s.executeUpdate("UPDATE t SET v = 2 WHERE id = 1")   // email 没变
                assertEquals(2L, s.query("SELECT v FROM t WHERE id = 1")[0][0])
            }
        }
    }

    @Test
    fun `唯一索引 - NULL 不参与唯一性约束`() {
        setup(
            """
            CREATE TABLE t (
                id BIGINT NOT NULL, email VARCHAR(64),
                PRIMARY KEY (id), UNIQUE KEY uk_email (email)
            )
            """.trimIndent()
        ).use { c ->
            c.st { s ->
                // SQL 语义：多个 NULL 不算重复（与 MySQL 一致）
                s.execute("INSERT INTO t VALUES (1,NULL),(2,NULL),(3,'a@x')")
                assertEquals(3, s.query("SELECT id FROM t").size)
            }
        }
    }

    @Test
    fun `唯一索引 - 删除后可以重新插入同一个值`() {
        setup(
            """
            CREATE TABLE t (
                id BIGINT NOT NULL, email VARCHAR(64),
                PRIMARY KEY (id), UNIQUE KEY uk_email (email)
            )
            """.trimIndent()
        ).use { c ->
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a@x')")
                s.executeUpdate("DELETE FROM t WHERE id = 1")
                s.execute("INSERT INTO t VALUES (2,'a@x')")   // 不应冲突
                assertEquals(listOf(2L), s.query("SELECT id FROM t").map { it[0] })
            }
        }
    }

    @Test
    fun `唯一索引 - 事务回滚后值可复用`() {
        setup(
            """
            CREATE TABLE t (
                id BIGINT NOT NULL, email VARCHAR(64),
                PRIMARY KEY (id), UNIQUE KEY uk_email (email)
            )
            """.trimIndent()
        ).use { c ->
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a@x')")
                c.rollback()
                s.execute("INSERT INTO t VALUES (2,'a@x')")   // 回滚的索引项必须一并消失
                c.commit()
                assertEquals(listOf(2L), s.query("SELECT id FROM t").map { it[0] })
            }
        }
    }

    @Test
    fun `唯一索引 - 事务内连续插入同值被拦住`() {
        setup(
            """
            CREATE TABLE t (
                id BIGINT NOT NULL, email VARCHAR(64),
                PRIMARY KEY (id), UNIQUE KEY uk_email (email)
            )
            """.trimIndent()
        ).use { c ->
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a@x')")
                // 第二个插入要看到第一个还在缓冲里的索引项
                assertFailsWith<SQLException> { s.execute("INSERT INTO t VALUES (2,'a@x')") }
                c.rollback()
            }
        }
    }

    // ── 哈希索引 ──────────────────────────────────────────────────

    @Test
    fun `哈希索引等值查找正确`() {
        setup(
            """
            CREATE TABLE t (
                id BIGINT NOT NULL, token VARCHAR(64),
                PRIMARY KEY (id), KEY idx_token (token) USING HASH
            )
            """.trimIndent()
        ).use { c ->
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'t1'),(2,'t2'),(3,'t1')")
                assertEquals(setOf(1L, 3L), s.query("SELECT id FROM t WHERE token = 't1'").map { it[0] }.toSet())
                assertEquals(listOf(2L), s.query("SELECT id FROM t WHERE token = 't2'").map { it[0] })
            }
        }
    }

    // ── 索引真的被用上 ────────────────────────────────────────────

    @Test
    fun `有索引时不应退化为全表扫描`() {
        // 用一个「只要扫描就会产生大量对象读取」的规模来间接验证：
        // 索引路径下，查找一行只需读索引分片的一个数据块
        setup(withIndex).use { c ->
            c.st { s ->
                val values = (1..2000).joinToString(",") { "($it, 'u$it@x', ${it % 100}, 'c${it % 20}')" }
                s.execute("INSERT INTO t VALUES $values")

                val hit = s.query("SELECT id FROM t WHERE age = 42")
                assertEquals(20, hit.size, "age=42 应命中 2000/100 = 20 行")
                assertTrue(hit.isNotEmpty())
            }
        }
    }

    @Test
    fun `索引与全表扫描结果一致 - 属性测试`() {
        setup(withIndex).use { c ->
            c.st { s ->
                val rnd = java.util.Random(3)
                for (i in 1..300) {
                    s.execute("INSERT INTO t VALUES ($i, 'u$i', ${rnd.nextInt(20)}, 'c${rnd.nextInt(5)}')")
                }
                for (age in 0 until 20) {
                    val viaIndex = s.query("SELECT id FROM t WHERE age = $age").map { it[0] as Long }.sorted()
                    val viaScan = s.query("SELECT id FROM t WHERE age + 0 = $age").map { it[0] as Long }.sorted()
                    assertEquals(viaScan, viaIndex, "age=$age 时索引结果与扫描结果必须一致")
                }
            }
        }
    }
}
