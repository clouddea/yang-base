package com.cloudea.yangbase.server

import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.LocalFileObjectStore
import com.cloudea.yangbase.txn.ChangeStream
import com.cloudea.yangbase.txn.SnapshotManager
import kotlinx.coroutines.runBlocking
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
 * 备份 / 时间点恢复（§12.3）与变更流（§17 M5，Q5 决策）。
 *
 * 这两件事在这个架构里几乎是免费的，原因同一个：**数据不可变**。
 *  - 备份 = 记录「那一刻每个分片的 manifest 快照是哪一个」，不拷贝数据
 *  - PITR = 找到时刻 T 之前最近的快照，把 CURRENT 指回去
 *  - CDC  = 读已有的 WAL（它本来就是不可变、有序、带 commit_ts 的日志）
 */
class BackupAndCdcTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var counting: CountingObjectStore
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-bk")
        counting = CountingObjectStore(LocalFileObjectStore(dataDir))
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
        "jdbc:mysql://127.0.0.1:$port/bkdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
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

    private fun setup(c: Connection) = c.st { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS bkdb")
        s.execute("USE bkdb")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute("CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
    }

    private fun count(): Long = connect().use { c ->
        c.st { s -> s.execute("USE bkdb") }
        c.q { s -> s.rows("SELECT COUNT(*) FROM t")[0][0] as Long }
    }

    // ── 备份 ──────────────────────────────────────────────────────

    @Test
    fun `备份的成本与数据量无关`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                val values = (1..2000).joinToString(",") { "($it, $it)" }
                s.execute("INSERT INTO t VALUES $values")
            }
        }
        val mgr = SnapshotManager(counting, server.engine)
        counting.reset()
        val key = runBlocking { mgr.backup("test", "cost-check") }
        val writes = counting.count("put")

        // ★ 这是「数据不可变」的直接推论：备份只写清单，不拷贝数据
        assertTrue(
            writes <= 8,
            "备份应当只写 O(分片数) 个对象（这里是 1 个分片），实际写了 $writes 个：" +
                counting.keyReport(),
        )
        val m = runBlocking { mgr.loadBackup(key) }
        assertTrue(m.shards.isNotEmpty(), "备份清单里应当有分片引用")
    }

    @Test
    fun `可以从备份恢复`() {
        connect().use { c ->
            setup(c)
            c.st { s ->
                s.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
                s.execute("USE bkdb")
            }
        }
        val mgr = SnapshotManager(counting, server.engine)
        val key = runBlocking { mgr.backup("test") }

        // 备份之后再写一批
        connect().use { c ->
            c.st { s ->
                s.execute("USE bkdb")
                s.execute("INSERT INTO t VALUES (4,40),(5,50)")
            }
        }
        assertEquals(5L, count())

        // 恢复
        runBlocking { mgr.restore(key) }
        assertEquals(
            3L, count(),
            "恢复后应当回到备份时的行数（备份后写入的数据不再可见）",
        )
    }

    // ── PITR ──────────────────────────────────────────────────────

    /**
     * PITR 的粒度是 **manifest 边界**，不是任意时刻。
     *
     * 理由：manifest 是「哪些文件构成某一刻状态」的完整描述，
     * 而 MemTable 里尚未 flush 的数据还没有 manifest 可以指回去。
     * 所以恢复点必须是某次 flush 之后。
     */
    @Test
    fun `可以恢复到某个 manifest 边界`() {
        connect().use { c ->
            setup(c)
            c.st { s -> s.execute("INSERT INTO t VALUES (1,10)") }
        }
        runBlocking { server.engine.flushAll() }
        Thread.sleep(20)
        val t1 = System.currentTimeMillis()

        connect().use { c ->
            c.st { s -> s.execute("USE bkdb"); s.execute("INSERT INTO t VALUES (2,20)") }
        }
        runBlocking { server.engine.flushAll() }
        assertEquals(2L, count())

        val table = server.engine.catalog.allTables().first { it.name == "t" }
        val mgr = SnapshotManager(counting, server.engine)
        assertTrue(
            runBlocking { mgr.earliestRestorableMillis(table) } != null,
            "flush 之后应当能算出最早可恢复时刻",
        )

        runBlocking { mgr.restoreTableTo(table, t1) }
        assertEquals(1L, count(), "恢复到 t1 时应当只有第一行")
    }

    @Test
    fun `早于 GC 水位的时刻会被明确拒绝`() {
        connect().use { c ->
            setup(c)
            c.st { s -> s.execute("INSERT INTO t VALUES (1,10)") }
        }
        runBlocking { server.engine.flushAll() }

        // 把 GC 水位推到很晚：模拟「安全窗口已过，更早的历史都回收了」
        val future = System.currentTimeMillis() + 30L * 24 * 60 * 60 * 1000
        runBlocking { com.cloudea.yangbase.txn.GarbageCollector(counting).advanceWatermark(future) }

        val table = server.engine.catalog.allTables().first { it.name == "t" }
        val mgr = SnapshotManager(counting, server.engine)
        val e = runCatching { runBlocking { mgr.restoreTableTo(table, 946684800000L) } }.exceptionOrNull()
        assertTrue(e != null, "恢复到已回收的时刻必须报错")
        assertTrue(
            e!!.message!!.contains("GC 水位"),
            "错误信息应说明是水位问题：${e.message}",
        )
    }

    // ── 变更流 ────────────────────────────────────────────────────

    @Test
    fun `变更流能读出插入与删除`() {
        connect().use { c ->
            setup(c)
            c.st { s -> s.execute("INSERT INTO t VALUES (1,10),(2,20)") }
        }
        val table = server.engine.catalog.allTables().first { it.name == "t" }
        val stream = ChangeStream(counting, listOf(table))

        val first = runBlocking { stream.poll(100) }
        assertEquals(2, first.size, "两次插入应当产生两条变更")
        assertTrue(first.all { it.op == ChangeStream.Change.Op.INSERT })

        connect().use { c ->
            c.st { s -> s.execute("USE bkdb"); s.execute("DELETE FROM t WHERE id = 1") }
        }
        val second = runBlocking { stream.poll(100) }
        assertEquals(1, second.size, "删除应当产生一条变更")
        assertEquals(ChangeStream.Change.Op.DELETE, second[0].op)

        // 游标推进之后不该重复读到旧变更
        val third = runBlocking { stream.poll(100) }
        assertEquals(0, third.size, "没有新写入时不应重复产出变更")
    }

    @Test
    fun `变更流能解码出行内容`() {
        connect().use { c ->
            setup(c)
            c.st { s -> s.execute("INSERT INTO t VALUES (7, 77)") }
        }
        val table = server.engine.catalog.allTables().first { it.name == "t" }
        val stream = ChangeStream(counting, listOf(table))
        val changes = runBlocking { stream.poll(10) }
        assertEquals(1, changes.size)
        val row = changes[0].decodeRow(table)
        assertTrue(row != null)
        assertEquals(7L, (row!![0] as com.cloudea.yangbase.common.Datum.I64).v)
        assertEquals(77L, (row[1] as com.cloudea.yangbase.common.Datum.I64).v)
    }

    @Test
    fun `变更流只订阅数据分片 - 不产出索引项`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS bkdb")
                s.execute("USE bkdb")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute("CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id), KEY idx_v (v))")
            }
        }
        connect().use { c ->
            c.st { s -> s.execute("USE bkdb"); s.execute("INSERT INTO t VALUES (1, 10)") }
        }
        val table = server.engine.catalog.allTables().first { it.name == "t" }
        val stream = ChangeStream(counting, listOf(table))
        val changes = runBlocking { stream.poll(100) }
        // 索引项落在负数 TableId 的独立分片里，对使用者没有意义，必须被过滤掉
        assertEquals(1, changes.size, "只应当产出数据分片的变更，实际 ${changes.size} 条")
        assertTrue(changes[0].tableId > 0)
    }
}
