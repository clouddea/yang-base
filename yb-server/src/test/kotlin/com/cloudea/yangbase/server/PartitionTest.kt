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
 * 分区测试（设计文档 §9）。
 *
 * 在此之前 `PARTITION BY RANGE/LIST` 的语法**能解析但完全不起作用**——
 * 建表语句被接受，数据却全部落在同一个分片里，`PARTITION BY HASH` 也是
 * 唯一真正生效的形式。那是最糟的一种状态：使用者以为自己分区了。
 */
class PartitionTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-part")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "part-test", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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
        "jdbc:mysql://127.0.0.1:$port/partdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
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

    private fun Connection.setup(ddl: String) = st { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS partdb")
        s.execute("USE partdb")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute(ddl)
    }

    // ── RANGE ─────────────────────────────────────────────────────

    private val rangeDdl = """
        CREATE TABLE t (
            id BIGINT NOT NULL,
            region VARCHAR(16),
            amount INT,
            PRIMARY KEY (id, region)
        )
        PARTITION BY RANGE (id) (
            PARTITION p0 VALUES LESS THAN (100),
            PARTITION p1 VALUES LESS THAN (200),
            PARTITION p2 VALUES LESS THAN (300),
            PARTITION pmax VALUES LESS THAN MAXVALUE
        )
    """.trimIndent()

    @Test
    fun `RANGE 分区 - 数据按值落到不同分区`() {
        connect().use { c ->
            c.setup(rangeDdl)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a',10),(150,'b',20),(250,'c',30),(9999,'d',40)")
                // 每个区间都必须能查到自己的数据——路由错了这里立刻暴露
                assertEquals(listOf(1L), s.query("SELECT id FROM t WHERE id = 1").map { it[0] })
                assertEquals(listOf(150L), s.query("SELECT id FROM t WHERE id = 150").map { it[0] })
                assertEquals(listOf(250L), s.query("SELECT id FROM t WHERE id = 250").map { it[0] })
                assertEquals(listOf(9999L), s.query("SELECT id FROM t WHERE id = 9999").map { it[0] })
                assertEquals(4, s.query("SELECT id FROM t").size, "全表扫描必须跨所有分区")
            }
        }
    }

    @Test
    fun `RANGE 分区 - 区间扫描只返回区间内的行`() {
        connect().use { c ->
            c.setup(rangeDdl)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a',1),(150,'b',2),(250,'c',3),(9999,'d',4)")
                val r = s.query("SELECT id FROM t WHERE id >= 100 AND id < 300").map { it[0] as Long }.sorted()
                assertEquals(listOf(150L, 250L), r)
            }
        }
    }

    @Test
    fun `RANGE 分区 - 未覆盖的值被拒绝`() {
        connect().use { c ->
            c.setup(
                """
                CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))
                PARTITION BY RANGE (id) (
                    PARTITION p0 VALUES LESS THAN (100),
                    PARTITION p1 VALUES LESS THAN (200)
                )
                """.trimIndent()
            )
            c.st { s ->
                s.execute("INSERT INTO t VALUES (50, 1)")
                // 没有 MAXVALUE 分区时，超范围的值必须报错而不是随便找个地方塞
                val e = assertFailsWith<SQLException> { s.execute("INSERT INTO t VALUES (500, 1)") }
                assertEquals(1526, e.errorCode, "应为 ER_NO_PARTITION_FOR_GIVEN_VALUE")
            }
        }
    }

    // ── LIST ──────────────────────────────────────────────────────

    @Test
    fun `LIST 分区 - 等值查找正确`() {
        connect().use { c ->
            c.setup(
                """
                CREATE TABLE t (
                    id BIGINT NOT NULL, region VARCHAR(16), v INT,
                    PRIMARY KEY (id, region)
                )
                PARTITION BY LIST (region) (
                    PARTITION p_cn VALUES IN ('cn', 'hk'),
                    PARTITION p_us VALUES IN ('us', 'ca')
                )
                """.trimIndent()
            )
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'cn',1),(2,'us',2),(3,'hk',3),(4,'ca',4)")
                assertEquals(listOf(1L), s.query("SELECT id FROM t WHERE id = 1").map { it[0] })
                assertEquals(listOf(3L), s.query("SELECT id FROM t WHERE id = 3").map { it[0] })
                assertEquals(setOf(1L, 3L), s.query("SELECT id FROM t WHERE region = 'cn' OR region = 'hk'").map { it[0] }.toSet())
                assertEquals(4, s.query("SELECT id FROM t").size)
            }
        }
    }

    @Test
    fun `LIST 分区 - 集合外的值被拒绝`() {
        connect().use { c ->
            c.setup(
                """
                CREATE TABLE t (id BIGINT NOT NULL, region VARCHAR(16), PRIMARY KEY (id, region))
                PARTITION BY LIST (region) (
                    PARTITION p_cn VALUES IN ('cn'),
                    PARTITION p_us VALUES IN ('us')
                )
                """.trimIndent()
            )
            c.st { s ->
                assertFailsWith<SQLException> { s.execute("INSERT INTO t VALUES (1, 'jp')") }
            }
        }
    }

    // ── DDL 校验 ──────────────────────────────────────────────────

    @Test
    fun `分区列不在主键里时建表失败`() {
        connect().use { c ->
            c.setup("CREATE TABLE t (id BIGINT NOT NULL, region VARCHAR(16), PRIMARY KEY (id))")
            c.st { s ->
                // 点查只拿得到主键；分区列不在主键里，主键点查就无从确定分区
                val e = assertFailsWith<SQLException> {
                    s.execute(
                        """
                        CREATE TABLE bad (id BIGINT NOT NULL, region VARCHAR(16), PRIMARY KEY (id))
                        PARTITION BY LIST (region) (PARTITION p0 VALUES IN ('cn'))
                        """.trimIndent()
                    )
                }
                assertTrue(e.message!!.contains("主键"), "错误信息应说明分区列必须是主键的一部分: ${e.message}")
            }
        }
    }

    @Test
    fun `RANGE 边界不递增时建表失败`() {
        connect().use { c ->
            c.setup("CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id))")
            c.st { s ->
                assertFailsWith<SQLException> {
                    s.execute(
                        """
                        CREATE TABLE bad (id BIGINT NOT NULL, PRIMARY KEY (id))
                        PARTITION BY RANGE (id) (
                            PARTITION p0 VALUES LESS THAN (200),
                            PARTITION p1 VALUES LESS THAN (100)
                        )
                        """.trimIndent()
                    )
                }
            }
        }
    }

    @Test
    fun `LIST 值重复时建表失败`() {
        connect().use { c ->
            c.setup("CREATE TABLE t (id BIGINT NOT NULL, region VARCHAR(16), PRIMARY KEY (id, region))")
            c.st { s ->
                assertFailsWith<SQLException> {
                    s.execute(
                        """
                        CREATE TABLE bad (id BIGINT NOT NULL, region VARCHAR(16), PRIMARY KEY (id, region))
                        PARTITION BY LIST (region) (
                            PARTITION p0 VALUES IN ('cn'),
                            PARTITION p1 VALUES IN ('cn')
                        )
                        """.trimIndent()
                    )
                }
            }
        }
    }

    @Test
    fun `MAXVALUE 不在最后时建表失败`() {
        connect().use { c ->
            c.setup("CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id))")
            c.st { s ->
                assertFailsWith<SQLException> {
                    s.execute(
                        """
                        CREATE TABLE bad (id BIGINT NOT NULL, PRIMARY KEY (id))
                        PARTITION BY RANGE (id) (
                            PARTITION p0 VALUES LESS THAN MAXVALUE,
                            PARTITION p1 VALUES LESS THAN (100)
                        )
                        """.trimIndent()
                    )
                }
            }
        }
    }

    // ── 与其它特性协同 ────────────────────────────────────────────

    @Test
    fun `分区表的 COUNT 跨分区求和`() {
        connect().use { c ->
            c.setup(rangeDdl)
            c.st { s ->
                val values = (1..500).joinToString(",") { "($it,'r',${it % 10})" }
                s.execute("INSERT INTO t VALUES $values")
                assertEquals(500L, s.query("SELECT COUNT(*) FROM t")[0][0])
                // 带上分区列上的约束，计数必须仍然正确（裁剪放宽了范围，不能收紧）
                assertEquals(500L, s.query("SELECT COUNT(*) FROM t WHERE id >= 0")[0][0])
            }
        }
    }

    @Test
    fun `分区表上的删除与更新`() {
        connect().use { c ->
            c.setup(rangeDdl)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a',10),(150,'b',20),(250,'c',30)")
                s.executeUpdate("UPDATE t SET amount = 99 WHERE id = 150")
                assertEquals(99L, s.query("SELECT amount FROM t WHERE id = 150")[0][0])
                s.executeUpdate("DELETE FROM t WHERE id = 250")
                assertEquals(2, s.query("SELECT id FROM t").size)
                assertEquals(0, s.query("SELECT id FROM t WHERE id = 250").size)
            }
        }
    }

    @Test
    fun `分区表上的二级索引仍然正确`() {
        connect().use { c ->
            c.setup(
                """
                CREATE TABLE t (
                    id BIGINT NOT NULL, region VARCHAR(16), amount INT,
                    PRIMARY KEY (id, region), KEY idx_amount (amount)
                )
                PARTITION BY RANGE (id) (
                    PARTITION p0 VALUES LESS THAN (100),
                    PARTITION p1 VALUES LESS THAN (200),
                    PARTITION pmax VALUES LESS THAN MAXVALUE
                )
                """.trimIndent()
            )
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a',10),(150,'b',10),(250,'c',20)")
                assertEquals(setOf(1L, 150L), s.query("SELECT id FROM t WHERE amount = 10").map { it[0] }.toSet())
                assertEquals(listOf(250L), s.query("SELECT id FROM t WHERE amount = 20").map { it[0] })
            }
        }
    }

    @Test
    fun `TO_DAYS 分区表达式端到端`() {
        connect().use { c ->
            c.setup(
                """
                CREATE TABLE t (
                    id BIGINT NOT NULL, ts DATETIME, amount INT,
                    PRIMARY KEY (id, ts)
                )
                PARTITION BY RANGE (TO_DAYS(ts)) (
                    PARTITION p2026q1 VALUES LESS THAN (TO_DAYS('2026-04-01')),
                    PARTITION p2026q2 VALUES LESS THAN (TO_DAYS('2026-07-01')),
                    PARTITION pmax VALUES LESS THAN MAXVALUE
                )
                """.trimIndent()
            )
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'2026-02-01 10:00:00',1),(2,'2026-05-01 10:00:00',2)")
                // 边界是「天数」而列是时间戳——路由必须先把列值过一遍 TO_DAYS，
                // 否则就是拿时间戳和天数比较，数据会落进错的分区
                assertEquals(listOf(1L), s.query("SELECT id FROM t WHERE id = 1")[0].let { listOf(it[0]) })
                assertEquals(listOf(2L), s.query("SELECT id FROM t WHERE id = 2")[0].let { listOf(it[0]) })
                assertEquals(2, s.query("SELECT id FROM t").size)
                assertEquals(listOf(1L), s.query("SELECT id FROM t WHERE ts < '2026-04-01 00:00:00'").map { it[0] })
            }
        }
    }

    @Test
    fun `分区列的日期表达式 TO_DAYS`() {
        connect().use { c ->
            c.setup(
                """
                CREATE TABLE t (
                    id BIGINT NOT NULL, ts DATETIME, amount INT,
                    PRIMARY KEY (id, ts)
                )
                PARTITION BY RANGE (ts) (
                    PARTITION p2026q1 VALUES LESS THAN ('2026-04-01 00:00:00'),
                    PARTITION p2026q2 VALUES LESS THAN ('2026-07-01 00:00:00'),
                    PARTITION pmax VALUES LESS THAN MAXVALUE
                )
                """.trimIndent()
            )
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'2026-02-01 10:00:00',1),(2,'2026-05-01 10:00:00',2)")
                assertEquals(listOf(1L), s.query("SELECT id FROM t WHERE ts = '2026-02-01 10:00:00'").map { it[0] })
                assertEquals(listOf(2L), s.query("SELECT id FROM t WHERE ts = '2026-05-01 10:00:00'").map { it[0] })
            }
        }
    }

    @Test
    fun `分区表在事务中回滚`() {
        connect().use { c ->
            c.setup(rangeDdl)
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a',10),(150,'b',20)")
                assertEquals(2, s.query("SELECT id FROM t").size)
                c.rollback()
                assertEquals(0, s.query("SELECT id FROM t").size, "跨分区的回滚必须干净")
            }
        }
    }

    @Test
    fun `HASH 分区仍然工作 - 回归`() {
        connect().use { c ->
            c.setup(
                """
                CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))
                PARTITION BY HASH (id) PARTITIONS 4
                """.trimIndent()
            )
            c.st { s ->
                val values = (1..200).joinToString(",") { "($it, $it)" }
                s.execute("INSERT INTO t VALUES $values")
                assertEquals(200L, s.query("SELECT COUNT(*) FROM t")[0][0])
                assertEquals(listOf(42L), s.query("SELECT id FROM t WHERE id = 42").map { it[0] })
                assertEquals(11, s.query("SELECT id FROM t WHERE id >= 100 AND id < 111").size)
            }
        }
    }
}
