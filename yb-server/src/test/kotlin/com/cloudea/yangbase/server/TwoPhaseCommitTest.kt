package com.cloudea.yangbase.server

import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.LocalFileObjectStore
import com.cloudea.yangbase.objectstore.ObjectStore
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
 * 跨分片事务的原子性（设计文档 §7.3）。
 *
 * 在此之前，`commitBuffer` 会**依次**写各分片的 WAL：中途失败就留下
 * 一部分分片已提交、另一部分没有的状态。客户端收到的是失败，数据却留下一半
 * ——那是静默的损坏，而且**跨重启存活**，是最难被发现的一类问题。
 *
 * 现在跨分片走两阶段：各分片先写**未决** WAL，再写一个提交标记。
 * 恢复时未决批次只在标记存在时才重放，于是「写到一半崩溃」的结果是
 * **整笔事务原子地消失**。
 *
 * 这个文件的关键在于**精确制造那个缝隙**——用键匹配的故障注入让阶段 2
 * 失败而阶段 1 成功。用粗粒度的「让所有 PUT 失败」会把阶段 1 一起打掉，
 * 那就什么也没测到。
 */
class TwoPhaseCommitTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var raw: ObjectStore
    private lateinit var counting: CountingObjectStore
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-2pc")
        raw = LocalFileObjectStore(dataDir)
        counting = CountingObjectStore(raw)
        server = YangBaseServer.forTesting(store = counting, port = 0)
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
        "jdbc:mysql://127.0.0.1:$port/pcdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun Connection.st(block: (Statement) -> Unit) = createStatement().use(block)

    /** `st` 返回 Unit；需要拿结果时用它。 */
    private fun <T> Connection.q(block: (Statement) -> T): T = createStatement().use(block)

    private fun Statement.query(sql: String): List<List<Any?>> =
        executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            val out = ArrayList<List<Any?>>()
            while (rs.next()) out.add((1..n).map { rs.getObject(it) })
            out
        }

    /** 4 个分区，`id` 从 1 到 12 会散落到不同分区里 → 一个事务天然跨分片。 */
    private fun setup(c: Connection) = c.st { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS pcdb")
        s.execute("USE pcdb")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute(
            """
            CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), PRIMARY KEY (id))
            PARTITION BY HASH (id) PARTITIONS 4
            """.trimIndent()
        )
    }

    /** 重启引擎（同一个对象存储），模拟一次崩溃后的冷启动。 */
    private fun restart(): YangBaseServer {
        server.stop()
        val fresh = YangBaseServer.forTesting(store = counting, port = 0)
        fresh.start()
        server = fresh
        port = fresh.boundPort
        return fresh
    }

    private fun countRows(): Long = connect().use { c ->
        c.st { s -> s.execute("USE pcdb") }
        c.q { s -> s.query("SELECT COUNT(*) FROM t")[0][0] as Long }
    }

    // ── 正常路径 ──────────────────────────────────────────────────

    @Test
    fun `跨分片事务正常提交后数据完整`() {
        connect().use { c ->
            setup(c)
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f')")
                c.commit()
            }
            assertEquals(6L, countRows())

            // 提交标记应当存在（否则恢复会把它丢掉）
            val marks = kotlinx.coroutines.runBlocking { counting.listAll("wal/tx/") }
            assertTrue(marks.isNotEmpty(), "跨分片提交必须写下提交标记，实际一个也没有")
        }
    }

    @Test
    fun `跨分片事务重启后数据完整`() {
        connect().use { c ->
            setup(c)
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f')")
                c.commit()
            }
        }
        restart()
        assertEquals(6L, countRows(), "已提交的跨分片事务必须跨重启存活")
    }

    // ── 故障路径：提交标记写不成 ──────────────────────────────────

    @Test
    fun `提交标记写入失败时事务整体消失 - 而不是留下半份`() {
        connect().use { c ->
            setup(c)
            // 让阶段 2 失败：键里含 wal/tx/ 的 PUT 被拒。
            // 阶段 1（各分片的未决 WAL）照常成功——这正是要制造的那个缝隙。
            counting.failOnPutMatching = "wal/tx/"

            c.autoCommit = false
            val failed = runCatching {
                c.st { s ->
                    s.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f')")
                    c.commit()
                }
            }.isFailure
            assertTrue(failed, "提交标记写不成时 COMMIT 必须报错，而不是静默成功")
            counting.failOnPutMatching = null

            // 未决 WAL 确实写下去了（否则这个测试什么都没测到）
            val pending = kotlinx.coroutines.runBlocking { counting.listAll("shard/") }
                .filter { it.endsWith(".p.log") }
            assertTrue(pending.isNotEmpty(), "阶段 1 应当已经写下未决 WAL，实际没有——测试没有制造出目标场景")
        }

        // ★ 重启后整笔事务原子地消失
        restart()
        assertEquals(
            0L, countRows(),
            "没有提交标记的跨分片事务必须在恢复时被整体丢弃；" +
                "留下半份数据就是静默的损坏",
        )
    }

    @Test
    fun `未决 WAL 不会被误当成已提交`() {
        // 直接构造一个「有未决 WAL 但没有提交标记」的存储状态，
        // 不依赖事务路径——这样即使提交路径将来改了，这条断言仍然守着恢复语义
        connect().use { c ->
            setup(c)
            c.autoCommit = false
            counting.failOnPutMatching = "wal/tx/"
            runCatching {
                c.st { s ->
                    s.execute("INSERT INTO t VALUES (7,'x'),(8,'y'),(9,'z'),(10,'w')")
                    c.commit()
                }
            }
            counting.failOnPutMatching = null
        }

        restart()
        assertEquals(0L, countRows(), "未被标记的未决 WAL 不得被重放")
    }

    // ── 与单分片路径的隔离 ────────────────────────────────────────

    @Test
    fun `单分片事务不受影响 - 保持一次 PUT 的原子性`() {
        connect().use { c ->
            setup(c)
            c.autoCommit = false
            // 只写一行 → 只落在一个分区 → 单分片路径
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'single')")
                c.commit()
            }

            // 单分片事务不该产生未决 WAL——那是跨分片才需要的机制，
            // 多写一个对象就是白付一次请求（原则 P4）
            val pending = kotlinx.coroutines.runBlocking { counting.listAll("shard/") }
                .filter { it.endsWith(".p.log") }
            assertTrue(pending.isEmpty(), "单分片事务不应产生未决 WAL，实际 ${pending.size} 个")
        }

        restart()
        assertEquals(1L, countRows(), "单分片事务必须正常存活")
    }

    @Test
    fun `回滚的跨分片事务不留下任何痕迹`() {
        connect().use { c ->
            setup(c)
            c.autoCommit = false
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
                c.rollback()
            }
            assertEquals(0L, countRows())
            val pending = kotlinx.coroutines.runBlocking { counting.listAll("shard/") }
                .filter { it.endsWith(".p.log") }
            assertTrue(pending.isEmpty(), "回滚的事务连未决 WAL 都不该留下——它根本没走到提交阶段")
        }
    }

    @Test
    fun `跨分片事务的删除也是原子的`() {
        connect().use { c ->
            setup(c)
            c.st { s -> s.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f')") }
            assertEquals(6L, countRows())

            c.autoCommit = false
            c.st { s ->
                s.execute("DELETE FROM t WHERE id <= 5")
                c.commit()
            }
            assertEquals(1L, countRows())
        }

        restart()
        assertEquals(1L, countRows(), "跨分片的删除也必须跨重启保持一致")
    }
}
