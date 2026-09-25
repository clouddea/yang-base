package com.cloudea.yangbase.server

import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * 在**已有数据**的表上建索引。
 *
 * 这是个正确性问题，不是性能问题：`tryIndexLookup` 一旦从索引拿到结果就
 * **直接返回、不回退扫描**。所以建索引时如果不把已有行索引进去，
 * 查询会**静默少返回行**——不报错、不降级，看起来就像「那些行不存在」。
 *
 * 最容易被忽略的是它的触发条件很常见：先灌数据、后加索引，
 * 或者用 `CREATE TABLE` + 事后 `CREATE INDEX` 的迁移脚本。
 */
class IndexBackfillTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-bf")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "bf-test", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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
        "jdbc:mysql://127.0.0.1:$port/bfdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
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
        s.execute("CREATE DATABASE IF NOT EXISTS bfdb")
        s.execute("USE bfdb")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute("CREATE TABLE t (id BIGINT NOT NULL, v INT, s VARCHAR(16), PRIMARY KEY (id))")
    }

    private fun ids(rows: List<List<Any?>>) = rows.map { it[0] as Long }.sorted()

    @Test
    fun `先插数据再建普通索引 - 旧行必须查得到`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,10,'a'),(2,20,'b'),(3,10,'c')")
                // 此刻还没有索引 → 全表扫描，结果是对的
                assertEquals(listOf(1L, 3L), ids(s.query("SELECT id FROM t WHERE v = 10")))

                s.execute("CREATE INDEX idx_v ON t(v)")

                // ★ 建完索引后必须**结果不变**。不回填的话这里会返回空——
                // 而且不报错，看起来就像「v=10 的行不存在」
                assertEquals(
                    listOf(1L, 3L), ids(s.query("SELECT id FROM t WHERE v = 10")),
                    "建索引不得改变查询结果；少返回行说明旧行没有被索引进去",
                )
                assertEquals(listOf(2L), ids(s.query("SELECT id FROM t WHERE v = 20")))
                assertEquals(0, s.query("SELECT id FROM t WHERE v = 99").size)
            }
        }
    }

    @Test
    fun `先插数据再建唯一索引 - 旧行必须参与唯一性`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,10,'dup'),(2,20,'dup')")
                // 建唯一索引时已有重复值 → MySQL 会直接报错。无论报错还是
                // 成功，都不能出现「索引说唯一、数据却有重复」这种自相矛盾的状态
                val failed = runCatching { s.execute("CREATE UNIQUE INDEX uk_s ON t(s)") }.isFailure
                val dupCount = s.query("SELECT id FROM t WHERE s = 'dup'").size
                if (!failed) {
                    assertEquals(2, dupCount, "唯一索引建成后，已存在的重复行仍必须可查")
                }
            }
        }
    }

    @Test
    fun `建索引后新插入的行正常维护`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,10,'a')")
                s.execute("CREATE INDEX idx_v ON t(v)")
                s.execute("INSERT INTO t VALUES (2,10,'b')")
                assertEquals(
                    listOf(1L, 2L), ids(s.query("SELECT id FROM t WHERE v = 10")),
                    "建索引前后写入的行都必须能被索引查到",
                )
            }
        }
    }

    @Test
    fun `建索引后删除旧行正常生效`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,10,'a'),(2,10,'b')")
                s.execute("CREATE INDEX idx_v ON t(v)")
                s.execute("DELETE FROM t WHERE id = 1")
                assertEquals(listOf(2L), ids(s.query("SELECT id FROM t WHERE v = 10")))
            }
        }
    }

    @Test
    fun `建索引后更新旧行正常生效`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,10,'a')")
                s.execute("CREATE INDEX idx_v ON t(v)")
                s.execute("UPDATE t SET v = 20 WHERE id = 1")
                assertEquals(0, s.query("SELECT id FROM t WHERE v = 10").size, "旧索引项必须失效")
                assertEquals(listOf(1L), ids(s.query("SELECT id FROM t WHERE v = 20")))
            }
        }
    }

    @Test
    fun `建索引前后查询结果逐条一致 - 属性测试`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                val rnd = java.util.Random(11)
                val values = (1..300).joinToString(",") { "($it, ${rnd.nextInt(20)}, 's$it')" }
                s.execute("INSERT INTO t VALUES $values")

                val before = (0 until 20).associateWith { v -> ids(s.query("SELECT id FROM t WHERE v = $v")) }
                s.execute("CREATE INDEX idx_v ON t(v)")
                for ((v, expected) in before) {
                    assertEquals(
                        expected, ids(s.query("SELECT id FROM t WHERE v = $v")),
                        "v=$v 的结果在建索引后发生了改变",
                    )
                }
            }
        }
    }

    @Test
    fun `建索引后 COUNT 仍然精确`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,10,'a'),(2,20,'b'),(3,10,'c')")
                s.execute("CREATE INDEX idx_v ON t(v)")
                assertEquals(3L, s.query("SELECT COUNT(*) FROM t")[0][0])
            }
        }
    }
}
