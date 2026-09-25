package com.cloudea.yangbase.server

import com.cloudea.yangbase.txn.GarbageCollector
import com.cloudea.yangbase.txn.TimeTravel
import kotlinx.coroutines.runBlocking
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
 * 时间旅行与 GC（设计文档 §4.2、§6.9）。
 *
 * 本系统的时间戳高位编码物理毫秒，因此「回到某个时刻」是一次位运算。
 * 这让时间旅行几乎是免费的——但**只有当历史还在**的时候。
 * GC 一旦回收了旧版本，那段历史就是断的；此时必须**明确报错**，
 * 而不是返回一个看不出问题的残缺结果。
 */
class TimeTravelTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-tt")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "tt-test", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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
        "jdbc:mysql://127.0.0.1:$port/ttdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
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
        s.execute("CREATE DATABASE IF NOT EXISTS ttdb")
        s.execute("USE ttdb")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute("CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), PRIMARY KEY (id))")
    }

    // ── 位运算的正确性 ────────────────────────────────────────────

    @Test
    fun `时间戳与墙上时钟可以互相转换`() {
        val millis = 1_767_225_600_000L   // 2026-01-01T00:00:00Z
        val ts = TimeTravel.readTsFor(millis)
        assertEquals(millis, TimeTravel.millisOf(ts), "物理毫秒必须能从时间戳无损还原")
        assertTrue(ts > millis, "时间戳要左移逻辑位，必须比毫秒大")
    }

    @Test
    fun `AS OF TSO 与 AS OF TIMESTAMP 指向同一时刻`() {
        val millis = 1_767_225_600_000L
        val viaTs = TimeTravel.parse("TSO", TimeTravel.readTsFor(millis).toString())
        val viaTime = TimeTravel.parse("TIMESTAMP", "2026-01-01 00:00:00")
        assertEquals(viaTime, viaTs, "两种写法必须指向同一个时间戳")
    }

    // ── 端到端的时间旅行 ──────────────────────────────────────────

    @Test
    fun `可以读到历史版本`() {
        connect().use { c ->
            c.setup()

            c.st { s -> s.execute("INSERT INTO t VALUES (1, 'v1')") }
            val t1 = currentTso(c)

            Thread.sleep(20)   // 让物理时间前进，两个时间点才分得开
            c.st { s -> s.execute("UPDATE t SET v = 'v2' WHERE id = 1") }
            val t2 = currentTso(c)

            c.st { s ->
                assertEquals("v2", s.query("SELECT v FROM t WHERE id = 1")[0][0], "当前读应看到最新值")
                assertEquals(
                    "v1", s.query("SELECT v FROM t AS OF TSO $t1 WHERE id = 1")[0][0],
                    "回到 t1 应看到旧值——否则时间旅行没有真正生效",
                )
                assertEquals(
                    "v2", s.query("SELECT v FROM t AS OF TSO $t2 WHERE id = 1")[0][0],
                )
            }
        }
    }

    @Test
    fun `时间旅行可以读到已被删除的行`() {
        connect().use { c ->
            c.setup()
            c.st { s -> s.execute("INSERT INTO t VALUES (1,'a'),(2,'b')") }
            val before = currentTso(c)

            Thread.sleep(20)
            c.st { s -> s.execute("DELETE FROM t WHERE id = 1") }

            c.st { s ->
                assertEquals(1, s.query("SELECT id FROM t").size, "当前只有一行")
                val old = s.query("SELECT id FROM t AS OF TSO $before ORDER BY id")
                assertEquals(listOf(1L, 2L), old.map { it[0] }, "回到删除前应看到两行")
            }
        }
    }

    @Test
    fun `时间旅行可以用 TIMESTAMP 写法`() {
        connect().use { c ->
            c.setup()
            c.st { s -> s.execute("INSERT INTO t VALUES (1, 'now')") }
            c.st { s ->
                // 未来时刻 = 看到当前全部数据（不报错，只是没有任何过滤效果）
                val future = s.query("SELECT id FROM t AS OF TIMESTAMP '2099-01-01 00:00:00'")
                assertEquals(1, future.size)
            }
        }
    }

    @Test
    fun `非法的时间点被拒绝`() {
        connect().use { c ->
            c.setup()
            c.st { s ->
                assertFailsWith<SQLException> { s.query("SELECT id FROM t AS OF TSO abc") }
                assertFailsWith<SQLException> { s.query("SELECT id FROM t AS OF TIMESTAMP 'not-a-date'") }
                assertFailsWith<SQLException> { s.query("SELECT id FROM t AS OF BOGUS 1") }
            }
        }
    }

    // ── GC 水位 ───────────────────────────────────────────────────

    @Test
    fun `早于 GC 水位的时间点明确报错 - 而不是返回残缺结果`() {
        connect().use { c ->
            c.setup()
            c.st { s -> s.execute("INSERT INTO t VALUES (1,'a')") }

            // 把水位推到很晚：模拟「安全窗口已经过去，更早的历史都被回收了」
            val future = System.currentTimeMillis() + 30L * 24 * 60 * 60 * 1000
            runBlocking { GarbageCollector(server.store).advanceWatermark(future) }

            c.st { s ->
                val e = assertFailsWith<SQLException> {
                    s.query("SELECT id FROM t AS OF TIMESTAMP '2020-01-01 00:00:00'")
                }
                assertTrue(
                    e.message!!.contains("GC 水位"),
                    "错误信息必须说明是水位问题，而不是让使用者去猜：${e.message}",
                )
            }
        }
    }

    @Test
    fun `水位之上的时间点仍然可读`() {
        connect().use { c ->
            c.setup()
            c.st { s -> s.execute("INSERT INTO t VALUES (1,'a')") }
            c.st { s ->
                // 未来时刻：数据已在其中，应看得到。
                // 注意不能拿「插入之前的某个过去时刻」当正例——那时这行还不存在，
                // 返回空是**正确的**，不是 bug（这条曾经把我自己绕进去过一次）。
                assertEquals(
                    1,
                    s.query("SELECT id FROM t AS OF TIMESTAMP '2099-01-01 00:00:00'").size,
                )
                // 插入之前：正确地返回空，且因为没越过水位，不报错
                assertEquals(
                    0,
                    s.query("SELECT id FROM t AS OF TIMESTAMP '2020-01-01 00:00:00'").size,
                )
            }
        }
    }

    // ── GC 标记与清扫 ─────────────────────────────────────────────

    @Test
    fun `GC 标记不会误删可达对象`() {
        connect().use { c ->
            c.setup()
            c.st { s -> s.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')") }
            runBlocking { server.engine.flushAll() }

            val table = runBlocking { server.engine.catalog.allTables().first { it.name == "t" } }
            val gc = GarbageCollector(server.store)

            val before = runBlocking { server.store.listAll("") }.size
            val marks = runBlocking {
                server.engine.allShards(table).map { gc.mark(table, it) }
            }
            // 标记阶段**不做任何删除**——这是两阶段的核心
            val after = runBlocking { server.store.listAll("") }.size
            assertEquals(before, after, "标记阶段绝不能删除任何对象")

            // 数据必须仍然完整（防的是「可达集合算错，把活文件标成垃圾」）
            c.st { s -> assertEquals(3, s.query("SELECT id FROM t").size) }
            assertTrue(marks.all { it.reachable > 0 }, "每个分片都应至少有 manifest 可达")
        }
    }

    @Test
    fun `GC 清扫在安全窗口内不删除任何东西`() {
        connect().use { c ->
            c.setup()
            c.st { s -> s.execute("INSERT INTO t VALUES (1,'a')") }
            runBlocking { server.engine.flushAll() }
            val table = runBlocking { server.engine.catalog.allTables().first { it.name == "t" } }
            val gc = GarbageCollector(server.store)

            val marks = runBlocking { server.engine.allShards(table).map { gc.mark(table, it) } }
            runBlocking { gc.writeManifest(System.currentTimeMillis(), marks) }
            // 造一份「假垃圾」，把真实对象写进清单，验证窗口能拦住误删
            val victim = runBlocking { server.store.listAll("").first { it.contains("/sst/") } }
            runBlocking {
                gc.writeManifest(
                    System.currentTimeMillis(),
                    listOf(
                        GarbageCollector.MarkResult("fake", garbage = listOf(victim), fencedWal = emptyList(), reachable = 0)
                    ),
                )
            }

            val swept = runBlocking { gc.sweep() }
            assertEquals(0, swept.deleted, "安全窗口内的清单一个对象都不该删")
            assertTrue(
                runBlocking { server.store.listAll("") }.contains(victim),
                "被误标进清单的对象必须还在——安全窗口是它的保护伞",
            )
            // 数据完好
            c.st { s -> assertEquals(1, s.query("SELECT id FROM t").size) }
        }
    }

    @Test
    fun `GC 清扫能删除超出安全窗口的垃圾`() {
        connect().use { c ->
            c.setup()
            c.st { s -> s.execute("INSERT INTO t VALUES (1,'a')") }
            runBlocking { server.engine.flushAll() }

            // 造一个真正的孤儿对象，并把时间「拨到」很久以后
            val orphan = "shard/999999/0/sst/L0/orphan.dat"
            runBlocking { server.store.put(orphan, ByteArray(16)) }

            val gc = GarbageCollector(server.store)
            val now = System.currentTimeMillis()
            // 清单的**名字里带的是它的创建时刻**，清扫按它判断「等够安全窗口没有」。
            // 因此这里把清单写成 48 小时前创建的——模拟一轮真正的延迟删除。
            runBlocking {
                gc.writeManifest(
                    now - 48L * 60 * 60 * 1000,
                    listOf(GarbageCollector.MarkResult("x", listOf(orphan), emptyList(), 0)),
                )
            }

            // 还没等够窗口时不能删
            assertEquals(0, runBlocking { gc.sweep(now - 47L * 60 * 60 * 1000) }.deleted,
                "安全窗口未到时一个对象都不该删")

            val swept = runBlocking { gc.sweep(now) }
            assertEquals(1, swept.deleted, "超出安全窗口的垃圾应被删除")
            assertTrue(
                !runBlocking { server.store.listAll("") }.contains(orphan),
                "孤儿对象应已被删除",
            )
            // 真实数据不能受影响
            c.st { s -> assertEquals(1, s.query("SELECT id FROM t").size) }
        }
    }

    /** 当前 TSO——用它做时间旅行的锚点，比墙上时钟精确（同一毫秒内的并发也能分开）。 */
    private fun currentTso(c: Connection): Long {
        val r = c.createStatement().use { s -> s.query("SHOW STATUS") }
        // SHOW STATUS 不含 TSO，直接从引擎读——测试与引擎同进程，这样最直接
        return runBlocking { server.engine.currentReadTs().value }
    }
}
