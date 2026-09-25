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
 * 事务语义测试。
 *
 * 这个文件存在的原因是：**在此之前 `ROLLBACK` 什么也没撤销**。
 * 写入是直接落 WAL + MemTable 的，`ROLLBACK` 只清空了读快照——
 * 使用者会以为回滚了，但数据已经生效且可见。那是个语义错误，不是缺功能。
 *
 * 现在写入先进入 [com.cloudea.yangbase.txn.TransactionBuffer]，
 * COMMIT 才落地，ROLLBACK 直接丢弃。
 */
class TransactionTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-tx")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "tx-test", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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
        "jdbc:mysql://127.0.0.1:$port/txdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
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

    private fun Connection.setup() = st { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS txdb")
        s.execute("USE txdb")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute("CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), PRIMARY KEY (id))")
    }

    // ── 核心：回滚必须真的回滚 ────────────────────────────────────

    @Test
    fun `ROLLBACK 之后数据必须不存在`() {
        connect().use { c ->
            c.setup()
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1, 'should-gone'), (2, 'also-gone')")
                // 事务内看得到自己的写
                assertEquals(2, s.query("SELECT id FROM t").size, "事务内应看得到未提交的写入")
                c.rollback()
                // 回滚之后必须干干净净
                assertEquals(0, s.query("SELECT id FROM t").size, "ROLLBACK 之后数据必须不存在")
            }
        }
    }

    @Test
    fun `COMMIT 之后数据必须存在`() {
        connect().use { c ->
            c.setup()
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1, 'kept')")
                c.commit()
                assertEquals(1, s.query("SELECT id FROM t").size)
                assertEquals("kept", s.query("SELECT v FROM t WHERE id = 1")[0][0])
            }
        }
    }

    @Test
    fun `回滚不影响事务之外的已提交数据`() {
        connect().use { c ->
            c.setup()
            c.st { s -> s.execute("INSERT INTO t VALUES (1, 'outside')") }

            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (2, 'inside')")
                c.rollback()
                assertEquals(1, s.query("SELECT id FROM t").size, "只应剩事务外插入的那一行")
                assertEquals(1L, s.query("SELECT id FROM t")[0][0])
            }
        }
    }

    // ── 读自己的写 ────────────────────────────────────────────────

    @Test
    fun `事务内插入后立刻查得到`() {
        connect().use { c ->
            c.setup()
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (7, 'mine')")
                assertEquals("mine", s.query("SELECT v FROM t WHERE id = 7")[0][0], "点查要看到自己的写")
                assertEquals(1, s.query("SELECT id FROM t").size, "范围扫描也要看到自己的写")
                c.rollback()
            }
        }
    }

    @Test
    fun `事务内删除后立刻查不到 - 回滚后又能查到`() {
        connect().use { c ->
            c.setup()
            c.st { s -> s.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')") }

            c.autoCommit = false
            c.st { s ->
                s.execute("DELETE FROM t WHERE id = 1")
                assertEquals(1, s.query("SELECT id FROM t").size, "事务内应看不到被删的行")
                c.rollback()
                assertEquals(2, s.query("SELECT id FROM t").size, "回滚后删除必须失效")
            }
        }
    }

    @Test
    fun `事务内改同一行多次 - 只保留最后一次`() {
        connect().use { c ->
            c.setup()
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1, 'v1')")
                s.execute("UPDATE t SET v = 'v2' WHERE id = 1")
                s.execute("UPDATE t SET v = 'v3' WHERE id = 1")
                assertEquals("v3", s.query("SELECT v FROM t WHERE id = 1")[0][0])
                c.commit()
                assertEquals("v3", s.query("SELECT v FROM t WHERE id = 1")[0][0])
                assertEquals(1, s.query("SELECT id FROM t").size, "多次修改不应产生多行")
            }
        }
    }

    @Test
    fun `事务内插入再删除 - 提交后什么也没有`() {
        connect().use { c ->
            c.setup()
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1, 'x')")
                s.execute("DELETE FROM t WHERE id = 1")
                c.commit()
                assertEquals(0, s.query("SELECT id FROM t").size, "插入再删除应互相抵消")
            }
        }
    }

    // ── 隔离 ──────────────────────────────────────────────────────

    @Test
    fun `未提交的写对其他连接不可见`() {
        connect().use { writer ->
            writer.setup()
            writer.autoCommit = false
            writer.st { s -> s.execute("INSERT INTO t VALUES (1, 'uncommitted')") }

            connect().use { reader ->
                reader.st { s ->
                    reader.createStatement().use { rs -> rs.execute("USE txdb") }
                    assertEquals(
                        0, s.query("SELECT id FROM t").size,
                        "其他连接不应看到未提交的写入",
                    )
                }
            }
            writer.rollback()
        }
    }

    @Test
    fun `提交后其他连接立刻可见`() {
        connect().use { writer ->
            writer.setup()
            writer.autoCommit = false
            writer.st { s ->
                s.execute("INSERT INTO t VALUES (1, 'committed')")
                writer.commit()
            }
            connect().use { reader ->
                reader.st { s ->
                    reader.createStatement().use { rs -> rs.execute("USE txdb") }
                    assertEquals(1, s.query("SELECT id FROM t").size, "提交后应立即可见（强一致读）")
                }
            }
        }
    }

    // ── 边界 ──────────────────────────────────────────────────────

    @Test
    fun `空事务提交与回滚都是安全的`() {
        connect().use { c ->
            c.setup()
            c.autoCommit = false
            c.commit()
            c.rollback()
            c.st { s -> assertEquals(0, s.query("SELECT id FROM t").size) }
        }
    }

    @Test
    fun `回滚后可以继续开启新事务`() {
        connect().use { c ->
            c.setup()
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1, 'gone')")
                c.rollback()
                s.execute("INSERT INTO t VALUES (2, 'kept')")
                c.commit()
                assertEquals(listOf(2L), s.query("SELECT id FROM t").map { it[0] })
            }
        }
    }

    @Test
    fun `大事务 - 单分片内的原子性`() {
        connect().use { c ->
            c.setup()
            c.autoCommit = false
            val n = 500
            c.st { s ->
                val values = (1..n).joinToString(",") { "($it, 'v$it')" }
                s.execute("INSERT INTO t VALUES $values")
                assertEquals(n, s.query("SELECT id FROM t").size, "事务内应看到全部 $n 行")
                c.rollback()
                assertEquals(0, s.query("SELECT id FROM t").size, "回滚后必须一行不剩")
            }
        }
    }

    @Test
    fun `事务内的 COUNT 必须包含未提交的写`() {
        connect().use { c ->
            c.setup()
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
                // COUNT 走的是元数据快速路径，必须叠加事务缓冲，
                // 否则事务内计数会与自己看到的数据不自洽
                val n = s.query("SELECT COUNT(*) FROM t")[0][0]
                assertEquals(3L, n, "事务内的 COUNT 必须包含未提交的写入")
                c.rollback()
            }
        }
    }

    @Test
    fun `自动提交模式下每条语句自成事务`() {
        connect().use { c ->
            c.setup()
            // autoCommit 默认 true
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1, 'a')")
                s.execute("INSERT INTO t VALUES (2, 'b')")
                assertEquals(2, s.query("SELECT id FROM t").size)
            }
        }
    }
}
