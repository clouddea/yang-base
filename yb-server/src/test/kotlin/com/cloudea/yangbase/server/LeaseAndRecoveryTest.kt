package com.cloudea.yangbase.server

import com.cloudea.yangbase.objectstore.LocalFileObjectStore
import com.cloudea.yangbase.txn.LeaseManager
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.metrics.Metrics
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * 分片写者租约（设计文档 §4.3）。
 *
 * 在此之前 `epoch` **恒为 1**：新建分片写死 `Epoch(1)`，已有分片沿用 manifest 里的
 * 那个值，全仓库没有任何一处递增它。于是 `wal/e{epoch}/` 那道
 * 「旧写者天然被忽略」的防线从未真正生效过，`LeaseLostException` 也没有抛出点。
 *
 * 没有租约时，两个 CN 可以同时打开同一个分片、各自以为自己是写者，
 * 往**同一个 WAL 前缀**下写对象——输的一方写下的日志仍会被下一个恢复者重放。
 * 那是静默的数据错乱，两边都不报错。
 */
class LeaseAndRecoveryTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var store: LocalFileObjectStore

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-lease")
        store = LocalFileObjectStore(dataDir)
    }

    @AfterTest
    fun tearDown() {
        runCatching { dataDir.toFile().deleteRecursively() }
    }

    private val shard = ShardId(TableId(1001), PartitionId(0))

    // ── 互斥 ──────────────────────────────────────────────────────

    @Test
    fun `同一分片同时只有一个写者`() = runBlocking {
        val a = LeaseManager(store, "cn-a", ttlMillis = 60_000)
        val b = LeaseManager(store, "cn-b", ttlMillis = 60_000)

        val okBefore = series("yb_lease_acquisitions_total{result=\"ok\"}")
        val la = a.acquire(shard)
        assertEquals(1L, la.epoch, "第一次取得租约应当是 epoch 1")

        // ★ b 必须被明确拒绝，而不是也拿到租约
        val e = assertFailsWith<Exception> { b.acquire(shard) }
        assertTrue(e.message!!.contains("cn-a"), "错误信息应指出谁占着：${e.message}")

        a.release(la)
        // 释放之后 b 可以拿，但 **epoch 必须更大**
        val lb = b.acquire(shard)
        assertTrue(lb.epoch > la.epoch, "epoch 必须单调递增：${la.epoch} -> ${lb.epoch}")

        // §12.5 要求租约可被观察。这里断言的是**埋点接在真实调用点上**：
        // 两次成功取得（a 一次、释放后 b 一次）必须被记下来。
        // 「被别人持有」是调用方的检查（没走到 CAS），因此不计进 failed。
        assertEquals(
            2.0, series("yb_lease_acquisitions_total{result=\"ok\"}") - okBefore,
            "两次成功取得租约必须各计一次",
        )
        assertEquals(0.0, series("yb_lease_acquisitions_total{result=\"failed\"}"), "重试没有耗尽，不该计 failed")
    }

    /** 从 Prometheus 文本取一个序列的值（不存在当 0）——用于断言点数在真实调用点上。 */
    private fun series(name: String): Double = Metrics.registry.render().lineSequence()
        .firstOrNull { it.startsWith("$name ") }
        ?.substringAfterLast(' ')?.toDoubleOrNull() ?: 0.0

    @Test
    fun `epoch 永不重置 - 释放后也不回退`() = runBlocking {
        val a = LeaseManager(store, "cn-a", ttlMillis = 60_000)
        var last = 0L
        repeat(5) {
            val l = a.acquire(shard)
            assertTrue(l.epoch > last, "第 $it 次取得的 epoch 必须严格更大：$last -> ${l.epoch}")
            last = l.epoch
            a.release(l)
        }
        // 这一条是正确性的关键：epoch 一旦被重用，旧写者留在
        // wal/e{旧epoch}/ 下的对象就会被新写者当成自己的日志重放
        assertTrue(last >= 5, "五次取得至少应产生 5 个不同的 epoch，实际最大 $last")
    }

    @Test
    fun `续期不改变 epoch`() = runBlocking {
        val a = LeaseManager(store, "cn-a", ttlMillis = 300)
        val l = a.acquire(shard)
        val before = l.epoch
        kotlinx.coroutines.delay(200)
        val renewed = a.renew(l)
        assertEquals(before, renewed.epoch, "续期是同一个写者继续持有，不是换了人")
        assertTrue(renewed.remainingMillis() > l.remainingMillis() - 1)
    }

    @Test
    fun `租约过期后别人可以接管 - 且 epoch 更大`() = runBlocking {
        val a = LeaseManager(store, "cn-a", ttlMillis = 100)
        val b = LeaseManager(store, "cn-b", ttlMillis = 60_000)
        val la = a.acquire(shard)
        kotlinx.coroutines.delay(250)   // 让 a 的租约过期
        val lb = b.acquire(shard)
        assertTrue(lb.epoch > la.epoch, "接管必须产生更大的 epoch（否则 fencing 失效）")
        assertEquals("cn-b", b.current(shard)!!.holder)
    }

    @Test
    fun `续期失败会抛 LeaseLost - 而不是静默继续`() = runBlocking {
        val a = LeaseManager(store, "cn-a", ttlMillis = 100)
        val b = LeaseManager(store, "cn-b", ttlMillis = 60_000)
        val la = a.acquire(shard)
        kotlinx.coroutines.delay(250)
        b.acquire(shard)   // b 抢走了
        // a 再续期必须被明确告知「你已经不是写者了」
        val lostBefore = series("yb_lease_lost_total")
        assertFailsWith<com.cloudea.yangbase.common.LeaseLostException> { a.renew(la) }
        // ★ 这条指标非零即告警（§12.5）：它意味着可能有第二个 CN 在写同一个分片
        assertEquals(
            1.0, series("yb_lease_lost_total") - lostBefore,
            "丢租约必须被计数——否则「另一个 CN 抢走了分片」在监控上完全看不见",
        )
    }

    // ── 端到端：租约模式下的分片写者 ──────────────────────────────

    @Test
    fun `objectstore 协调模式下 CN 能正常读写`() {
        val dataDir2 = Files.createTempDirectory("yangbase-lease-e2e")
        var server: YangBaseServer? = null
        try {
            server = YangBaseServer(
                ServerConfig(
                    host = "127.0.0.1", port = 0, nodeId = "lease-cn",
                    dataDir = dataDir2.toString(), allowNonDurableStorage = true, coordMode = "objectstore",
                )
            )
            server.start()
            Class.forName("com.mysql.cj.jdbc.Driver")
            DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:${server.boundPort}/ldb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "root", "",
            ).use { c ->
                c.createStatement().use { s ->
                    s.execute("CREATE DATABASE IF NOT EXISTS ldb")
                    s.execute("USE ldb")
                    s.execute("DROP TABLE IF EXISTS t")
                    s.execute("CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
                    s.execute("INSERT INTO t VALUES (1,10),(2,20)")
                    val n = s.executeQuery("SELECT COUNT(*) FROM t").use { it.next(); it.getLong(1) }
                    assertEquals(2L, n)
                }
            }
        } finally {
            runCatching { server?.stop() }
            runCatching { dataDir2.toFile().deleteRecursively() }
        }
    }

    @Test
    fun `分片打开后 epoch 被写进 WAL 前缀`() = runBlocking {
        val d = Files.createTempDirectory("yangbase-lease-wal")
        var server: YangBaseServer? = null
        try {
            server = YangBaseServer(
                ServerConfig(
                    host = "127.0.0.1", port = 0, nodeId = "lease-cn2",
                    dataDir = d.toString(), allowNonDurableStorage = true, coordMode = "objectstore",
                )
            )
            server.start()
            Class.forName("com.mysql.cj.jdbc.Driver")
            DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:${server.boundPort}/ldb2?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "root", "",
            ).use { c ->
                c.createStatement().use { s ->
                    s.execute("CREATE DATABASE IF NOT EXISTS ldb2")
                    s.execute("USE ldb2")
                    s.execute("CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id))")
                    s.execute("INSERT INTO t VALUES (1)")
                }
            }
            // 租约对象必须存在，且 epoch 非 0
            val keys = store2(d).listAll("")
            assertTrue(keys.any { it.endsWith("lease.json") }, "应当写下租约对象，实际：$keys")
            assertTrue(
                keys.any { it.contains("/wal/e1/") } || keys.any { it.contains("/wal/e2/") },
                "WAL 应当落在某个具体 epoch 前缀下，实际：${keys.filter { it.contains("/wal/") }}",
            )
        } finally {
            runCatching { server?.stop() }
            runCatching { d.toFile().deleteRecursively() }
        }
    }

    /**
     * ★ **重启之后仍然能写一个已经 flush 过的分片**。
     *
     * 这条用例是补出来的，因为漏掉的正是这个组合：`LsmStore.open()` 在
     * 「manifest 已存在」那条分支上一路走到 `opened = true`，
     * 却**没有启动 WAL 写者**——启动写者那一行只写在「分片是全新的」分支里。
     * 于是分片看起来是打开的，`wal` 还是 null，
     * 重启后的第一次写入直接 `NullPointerException`。
     *
     * 之所以一直没有被测出来：其他恢复类用例要么只读，要么写的是刚建的分片
     * （那条路径恰好调了 `startWriter`）。必须**先 flush 让 manifest 落盘、
     * 再重启、再写**，才会踩到。
     */
    @Test
    fun `重启后仍能写已经 flush 过的分片`() = runBlocking {
        val d = Files.createTempDirectory("yangbase-restart-write")
        var server: YangBaseServer? = null
        try {
            Class.forName("com.mysql.cj.jdbc.Driver")
            server = YangBaseServer(
                ServerConfig(host = "127.0.0.1", port = 0, nodeId = "rw-cn", dataDir = d.toString(), allowNonDurableStorage = true)
            )
            server.start()
            insert(server.boundPort, "rwdb", "INSERT INTO t VALUES (1,10)")
            // flush：manifest 从此存在，重启后走的就是「已存在」那条分支
            server.engine.flushAll()
            server.stop()

            server = YangBaseServer(
                ServerConfig(host = "127.0.0.1", port = 0, nodeId = "rw-cn", dataDir = d.toString(), allowNonDurableStorage = true)
            )
            server.start()
            insert(server.boundPort, "rwdb", "INSERT INTO t VALUES (2,20)")   // ← 曾经在这里 NPE

            val rows = query(server.boundPort, "rwdb", "SELECT id FROM t")
            assertEquals(listOf(1L, 2L), rows.sorted(), "重启后的写入必须成功且不丢数据")
        } finally {
            runCatching { server?.stop() }
            runCatching { d.toFile().deleteRecursively() }
        }
    }

    private fun connect(port: Int, db: String): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/$db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun insert(port: Int, db: String, sql: String) = connect(port, db).use { c ->
        c.createStatement().use { s ->
            s.execute("CREATE DATABASE IF NOT EXISTS $db")
            s.execute("USE $db")
            s.execute("CREATE TABLE IF NOT EXISTS t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
            s.execute(sql)
        }
    }

    private fun query(port: Int, db: String, sql: String): List<Long> = connect(port, db).use { c ->
        c.createStatement().use { s ->
            s.execute("USE $db")
            s.executeQuery(sql).use { rs ->
                val out = ArrayList<Long>()
                while (rs.next()) out.add(rs.getLong(1))
                out
            }
        }
    }

    private fun store2(d: java.nio.file.Path) = LocalFileObjectStore(d)
}
