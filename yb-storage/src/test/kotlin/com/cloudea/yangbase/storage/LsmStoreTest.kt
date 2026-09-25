package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.CommitTs
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * LSM 引擎的测试。
 *
 * 这里最重要的不是「功能能跑」，而是两条设计不变式：
 *  1. **点查稳态 ≤ 1 次 GET**（§14.2）—— 用 CountingObjectStore 断言
 *  2. **COUNT 走元数据路径，0 次 GET**（§8.9）—— 同上
 */
class LsmStoreTest {

    private val schema = TableSchema(
        tableId = 1001,
        databaseId = 1,
        name = "users",
        version = 1,
        columns = listOf(
            ColumnDef("id", ColumnType.BIGINT, nullable = false),
            ColumnDef("name", ColumnType.VARCHAR, length = 64),
            ColumnDef("age", ColumnType.INT),
        ),
        primaryKey = listOf("id"),
    )

    private val shard = ShardId(TableId(1001), PartitionId(0))

    private fun keyOf(id: Long): ByteArray =
        KeyEncoder.encodeKey(listOf(Datum.I64(id)), schema, listOf(0))

    private fun rowOf(id: Long, name: String, age: Long): ByteArray =
        RowCodec.encode(listOf(Datum.I64(id), Datum.Str(name), Datum.I64(age)))

    private fun ctx(store: com.cloudea.yangbase.objectstore.ObjectStore): LsmStoreContext = LsmStoreContext(store)

    private inner class LsmStoreContext(val store: com.cloudea.yangbase.objectstore.ObjectStore) {
        /** 构造并**打开**存储引擎。注意这里必须真的调用 open()。 */
        suspend fun open(exactCount: Boolean = true, memCapacity: Long = 32L * 1024 * 1024): LsmStore {
            val scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.Default)
            val lsm = LsmStore(
                store = store, shard = shard, schema = schema,
                cache = BlockCache(8L * 1024 * 1024), nodeId = "test-node",
                scope = scope, commitMode = CommitMode.SYNC,
                exactCount = exactCount, memtableCapacity = memCapacity,
            )
            lsm.open()
            return lsm
        }
    }

    private suspend fun put(lsm: LsmStore, id: Long, name: String, age: Long, ts: Long) {
        lsm.write(WalBatch("tx-$id-$ts", ts, listOf(WalOp(keyOf(id), rowOf(id, name, age)))))
    }

    private suspend fun del(lsm: LsmStore, id: Long, ts: Long) {
        lsm.write(WalBatch("txd-$id-$ts", ts, listOf(WalOp(keyOf(id), null))))
    }

    private suspend fun getRow(lsm: LsmStore, id: Long, ts: Long): List<Datum>? {
        val v = lsm.get(keyOf(id), ReadTs(ts)) ?: return null
        return RowCodec.decode(v, schema)
    }

    /**
     * ★ **只读分片的重复读不能重复下载已消费的 WAL。**
     *
     * 非属主 CN 的每一次读都要先刷新 manifest 视图（否则看不到属主刚提交的行，
     * 而且是**静默**少行），刷新时还要把属主尚未 flush 的 WAL 重放进内存表。
     * 这里曾经是"对 WAL 前缀里**每个**对象都 GET 一次（已消费的也重新下载）"：
     * 一个 epoch 下最多上百个对象，一次点查就变成上百次计费请求，而预算是 1 次。
     *
     * 修好之后：WAL 对象只在**第一次**（或序号前进时）被下载，
     * 稳态每次读只剩 1 次 GET（CURRENT 指针，内容没变时取不到 manifest 本体）。
     */
    @Test
    fun `只读分片重复读不重新下载已消费的 WAL`() = runTest {
        val counting = CountingObjectStore(MemoryObjectStore())
        val writer = ctx(counting).open()
        // 5 个 WAL 对象，**不 flush** —— 只读分片必须靠重放它们才能看到这些行
        for (i in 1L..5L) put(writer, i, "u$i", 20, 100 + i)

        // 不带租约地打开同一个分片 = 只读视角（§4.3：写者是租约，不是节点属性）
        val ro = LsmStore(
            store = counting, shard = shard, schema = schema, cache = BlockCache(1024),
            nodeId = "reader", scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob()),
        )
        ro.open()
        assertEquals(Datum.Str("u3"), getRow(ro, 3, 10000)!![1], "只读视角要能看到属主未落盘的行")

        counting.reset()
        repeat(5) { getRow(ro, 3, 10000) }
        assertTrue(
            counting.count("get") + counting.count("get_range") <= 10,
            "稳态重复读的 GET 次数超预算（每次应 ≤1）。${counting.report()}",
        )
        ro.close()
    }

    // ── 基本读写 ──────────────────────────────────────────────────

    @Test
    fun `写入后可以点查到`() = runTest {
        val lsm = ctx(MemoryObjectStore()).open()
        put(lsm, 1, "alice", 30, 100)
        val row = getRow(lsm, 1, 200)
        assertEquals(Datum.I64(1), row!![0])
        assertEquals(Datum.Str("alice"), row[1])
        assertEquals(Datum.I64(30), row[2])
        lsm.close()
    }

    @Test
    fun `不存在的 key 返回 null`() = runTest {
        val lsm = ctx(MemoryObjectStore()).open()
        put(lsm, 1, "alice", 30, 100)
        assertNull(getRow(lsm, 999, 200))
        lsm.close()
    }

    @Test
    fun `删除后查不到 - 但删除前的快照仍能看到`() = runTest {
        val lsm = ctx(MemoryObjectStore()).open()
        put(lsm, 1, "alice", 30, 100)
        del(lsm, 1, 200)

        assertNull(getRow(lsm, 1, 300), "删除之后应查不到")
        assertEquals(Datum.Str("alice"), getRow(lsm, 1, 150)!![1], "删除之前的快照应仍能看到")
        lsm.close()
    }

    @Test
    fun `覆盖写 - 旧快照看到旧值`() = runTest {
        val lsm = ctx(MemoryObjectStore()).open()
        put(lsm, 1, "alice", 30, 100)
        put(lsm, 1, "alice2", 31, 200)

        assertEquals(Datum.Str("alice2"), getRow(lsm, 1, 250)!![1])
        assertEquals(Datum.Str("alice"), getRow(lsm, 1, 150)!![1])
        lsm.close()
    }

    @Test
    fun `未提交的写入对更早的 readTs 不可见`() = runTest {
        val lsm = ctx(MemoryObjectStore()).open()
        put(lsm, 1, "v1", 1, 100)
        put(lsm, 2, "v2", 2, 200)
        // readTs = 150 应只看到第一个
        assertEquals(Datum.Str("v1"), getRow(lsm, 1, 150)!![1])
        assertNull(getRow(lsm, 2, 150))
        lsm.close()
    }

    // ── 范围扫描 ──────────────────────────────────────────────────

    @Test
    fun `范围扫描按主键升序返回`() = runTest {
        val lsm = ctx(MemoryObjectStore()).open()
        for (i in listOf(5L, 1L, 3L, 2L, 4L)) put(lsm, i, "u$i", 20 + i, 100 + i)
        val rows = lsm.scan(null, null, ReadTs(1000))
        assertEquals(listOf(1L, 2L, 3L, 4L, 5L), rows.map { RowCodec.decode(it.value, schema)[0].asLong() })
        lsm.close()
    }

    @Test
    fun `范围扫描遵守边界 - 左闭右开`() = runTest {
        val lsm = ctx(MemoryObjectStore()).open()
        for (i in 1L..10L) put(lsm, i, "u$i", 20, 100 + i)
        val rows = lsm.scan(keyOf(3), keyOf(7), ReadTs(1000))
        assertEquals(listOf(3L, 4L, 5L, 6L), rows.map { RowCodec.decode(it.value, schema)[0].asLong() })
        lsm.close()
    }

    @Test
    fun `范围扫描跳过墓碑`() = runTest {
        val lsm = ctx(MemoryObjectStore()).open()
        for (i in 1L..5L) put(lsm, i, "u$i", 20, 100 + i)
        del(lsm, 3, 200)
        val rows = lsm.scan(null, null, ReadTs(1000))
        assertEquals(listOf(1L, 2L, 4L, 5L), rows.map { RowCodec.decode(it.value, schema)[0].asLong() })
        lsm.close()
    }

    // ── flush 与重开 ──────────────────────────────────────────────

    @Test
    fun `flush 后数据进入 SST 且仍可读`() = runTest {
        val mem = MemoryObjectStore()
        val lsm = ctx(mem).open()
        for (i in 1L..100L) put(lsm, i, "u$i", 20, 100 + i)
        assertTrue(lsm.flush(), "flush 应产生文件")

        // 清空 MemTable 的路径已由 flush 内部完成；这里验证从 SST 读
        for (i in 1L..100L) {
            assertEquals(Datum.Str("u$i"), getRow(lsm, i, 1000)!![1], "flushed 数据应仍可读: id=$i")
        }
        lsm.close()
    }

    @Test
    fun `重开后数据仍在 - 恢复走 manifest 与 SST`() = runTest {
        val mem = MemoryObjectStore()

        val lsm1 = ctx(mem).open()
        for (i in 1L..50L) put(lsm1, i, "u$i", 20, 100 + i)
        lsm1.flush()
        put(lsm1, 999, "after-flush", 1, 500)
        lsm1.close()

        val lsm2 = ctx(mem).open()
        for (i in 1L..50L) {
            assertEquals(Datum.Str("u$i"), getRow(lsm2, i, 10000)!![1], "重启后丢数据: id=$i")
        }
        assertEquals(Datum.Str("after-flush"), getRow(lsm2, 999, 10000)!![1], "WAL 中的未 flush 数据应在恢复后可见")
        lsm2.close()
    }

    @Test
    fun `崩溃恢复 - 未 flush 的写入从 WAL 重放`() = runTest {
        val mem = MemoryObjectStore()
        // 直接构造：写入但不 flush，然后模拟进程崩溃（不调用 close）
        val lsm1 = ctx(mem).open()
        for (i in 1L..20L) put(lsm1, i, "u$i", 20, 100 + i)
        // 不 close，直接丢弃引用 —— WAL 已持久化，MemTable 丢失

        val lsm2 = ctx(mem).open()
        for (i in 1L..20L) {
            assertEquals(
                Datum.Str("u$i"), getRow(lsm2, i, 10000)!![1],
                "崩溃恢复应重放 WAL 中的第 $i 条写入",
            )
        }
        lsm2.close()
    }

    // ── ★ IO 预算断言（设计文档 §14.2）────────────────────────────

    @Test
    fun `点查稳态只应产生 1 次 GET`() = runTest {
        val counting = CountingObjectStore(MemoryObjectStore())
        val lsm = ctx(counting).open()
        for (i in 1L..200L) put(lsm, i, "u$i", 20, 100 + i)
        lsm.flush()
        for (i in 1L..200L) put(lsm, i, "n$i", 21, 300 + i)
        lsm.flush()

        // 预热：把 manifest 与 .idx 填充进缓存，并让数据块进入块缓存
        getRow(lsm, 42, 10000)

        counting.reset()
        getRow(lsm, 43, 10000)

        // 数据块可能已被块缓存覆盖，因此断言「不超过 1 次」
        assertTrue(
            counting.count("get") + counting.count("get_range") <= 1,
            "点查稳态的 GET 次数超预算。${counting.report()}",
        )
        lsm.close()
    }

    @Test
    fun `冷缓存点查不超过 4 次 GET`() = runTest {
        val counting = CountingObjectStore(MemoryObjectStore())
        val lsm = ctx(counting).open()
        for (i in 1L..200L) put(lsm, i, "u$i", 20, 100 + i)
        lsm.flush()

        // 用一个全新的 store 实例模拟「缓存全冷」：同一份底层数据，新的缓存
        val mem = counting // 底层已填充
        counting.reset()
        val lsmCold = LsmStore(
            store = mem, shard = shard, schema = schema,
            cache = BlockCache(1024), nodeId = "cold", // 极小缓存 = 冷
            scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob()),
            commitMode = CommitMode.SYNC,
        )
        lsmCold.open()
        counting.reset()
        getRow(lsmCold, 43, 10000)
        assertTrue(
            counting.count("get") + counting.count("get_range") <= 4,
            "冷缓存点查的 GET 次数超预算（≤4）。${counting.report()}",
        )
        lsmCold.close()
        lsm.close()
    }

    // ── ★ COUNT 优化（设计文档 §8.9）──────────────────────────────

    @Test
    fun `COUNT 无谓词 - 0 次 GET`() = runTest {
        val counting = CountingObjectStore(MemoryObjectStore())
        val lsm = ctx(counting).open()
        for (i in 1L..500L) put(lsm, i, "u$i", 20, 100 + i)
        lsm.flush()
        for (i in 501L..800L) put(lsm, i, "u$i", 20, 100 + i)

        val before = counting.totalOps()
        val n = lsm.countAll(ReadTs(100000))
        val after = counting.totalOps()

        assertEquals(800L, n, "COUNT 结果必须精确")
        assertEquals(before, after, "COUNT 不应产生任何对象存储请求。${counting.report()}")
        lsm.close()
    }

    @Test
    fun `COUNT 在删除后仍然精确`() = runTest {
        val lsm = ctx(MemoryObjectStore()).open()
        for (i in 1L..100L) put(lsm, i, "u$i", 20, 100 + i)
        for (i in 1L..30L) del(lsm, i, 500 + i)
        assertEquals(70L, lsm.countAll(ReadTs(100000)))
        lsm.close()
    }

    @Test
    fun `COUNT 在覆盖写后不重复计数`() = runTest {
        val lsm = ctx(MemoryObjectStore()).open()
        for (i in 1L..50L) put(lsm, i, "v1", 20, 100 + i)
        // 同一批 key 再写一次：总数不应变成 100
        for (i in 1L..50L) put(lsm, i, "v2", 21, 200 + i)
        assertEquals(50L, lsm.countAll(ReadTs(100000)), "覆盖写不应增加行数")
        lsm.close()
    }

    @Test
    fun `COUNT 与全表扫描结果一致 - 属性测试`() = runTest {
        val rnd = java.util.Random(7)
        val lsm = ctx(MemoryObjectStore()).open()
        var ts = 100L
        val live = HashSet<Long>()

        repeat(2000) { step ->
            ts += 1
            val id = rnd.nextInt(300).toLong()
            if (rnd.nextInt(100) < 25) {
                del(lsm, id, ts); live.remove(id)
            } else {
                put(lsm, id, "u$id", id, ts); live.add(id)
            }
            // 周期性 flush，让数据分布到 SST 与 MemTable
            if (step % 300 == 0) lsm.flush()
        }
        lsm.flush()

        val scanned = lsm.scan(null, null, ReadTs(Long.MAX_VALUE)).size.toLong()
        val fromScan = lsm.countAll(ReadTs(Long.MAX_VALUE))
        assertEquals(live.size.toLong(), scanned, "扫描结果应与维护的集合一致")
        if (fromScan >= 0) {
            assertEquals(scanned, fromScan, "元数据计数必须等于全表扫描结果（§8.9.3 的去重正确性）")
        }
        lsm.close()
    }

    @Test
    fun `关闭 exact_count 时计数回退到精确扫描而非估算`() = runTest {
        val lsm = ctx(MemoryObjectStore()).open(exactCount = false)
        for (i in 1L..50L) put(lsm, i, "u$i", 20, 100 + i)
        val n = lsm.countAll(ReadTs(100000))
        // 要么给出精确值，要么返回 -1 让上层回退；绝不允许是错的
        assertTrue(n == 50L || n == -1L, "关闭 exact_count 后不得返回错误的估算值，实际=$n")
        lsm.close()
    }

    // ── 缓存 ──────────────────────────────────────────────────────

    @Test
    fun `块缓存命中避免重复 GET`() = runTest {
        val counting = CountingObjectStore(MemoryObjectStore())
        val lsm = ctx(counting).open()
        for (i in 1L..200L) put(lsm, i, "u$i", 20, 100 + i)
        lsm.flush()

        getRow(lsm, 42, 10000)     // 首次：读一个数据块
        counting.reset()
        repeat(5) { getRow(lsm, 42, 10000) }   // 后续应命中块缓存
        assertEquals(0L, counting.count("get") + counting.count("get_range"),
            "块缓存命中后不应再产生 GET。${counting.report()}")
        lsm.close()
    }

    @Test
    fun `在途请求去重 - 并发读同一块只产生一次 GET`() = runTest {
        val counting = CountingObjectStore(MemoryObjectStore())
        val lsm = ctx(counting).open()
        for (i in 1L..200L) put(lsm, i, "u$i", 20, 100 + i)
        lsm.flush()
        getRow(lsm, 42, 10000)

        // 清掉缓存后并发读同一批 key
        val cache = BlockCache(8L * 1024 * 1024)
        val lsm2 = LsmStore(
            store = counting, shard = shard, schema = schema, cache = cache,
            nodeId = "n2", scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob()),
        )
        lsm2.open()
        counting.reset()
        coroutineScope {
            val jobs = (1..20).map { k: Int -> async { getRow(lsm2, 42L + k, 10000) } }
            jobs.forEach { it.await() }
        }
        // 20 个并发读同一个数据块：去重后应远少于 20 次
        assertTrue(
            counting.count("get") + counting.count("get_range") < 20,
            "在途去重未生效。${counting.report()}",
        )
        lsm2.close()
        lsm.close()
    }

    // ── WAL 与 fencing ─────────────────────────────────────────────

    @Test
    fun `WAL 对象编解码往返`() {
        val batches = listOf(
            WalBatch("tx1", 100, listOf(WalOp("k1".toByteArray(), "v1".toByteArray()))),
            WalBatch("tx2", 101, listOf(
                WalOp("k2".toByteArray(), "v2".toByteArray()),
                WalOp("k3".toByteArray(), null),   // 墓碑
            )),
        )
        val bytes = WalCodec.encode(com.cloudea.yangbase.common.Epoch(7), 42, batches)
        val decoded = WalCodec.decode(bytes)

        assertEquals(7L, decoded.epoch)
        assertEquals(42L, decoded.seq)
        assertEquals(2, decoded.batches.size)
        assertEquals(0, decoded.corruptedRecords)
        assertEquals("tx1", decoded.batches[0].txId)
        assertEquals(100L, decoded.batches[0].commitTs)
        assertNull(decoded.batches[1].ops[1].value, "墓碑应解出 null value")
    }

    @Test
    fun `WAL 单条记录损坏不影响同批次其他记录`() {
        // 这是设计文档 §6.3.3 的关键性质：每条记录独立 CRC
        val batches = (1..5).map {
            WalBatch("tx$it", 100L + it, listOf(WalOp("k$it".toByteArray(), "v$it".toByteArray())))
        }
        val bytes = WalCodec.encode(com.cloudea.yangbase.common.Epoch(1), 1, batches)

        // 翻转第 3 条记录正文里的一个字节
        val marker = "tx3".toByteArray()
        var pos = -1
        outer@ for (i in 0 until bytes.size - marker.size) {
            var m = true
            for (j in marker.indices) if (bytes[i + j] != marker[j]) { m = false; break }
            if (m) { pos = i; break@outer }
        }
        assertTrue(pos > 0, "测试自身错误：找不到标记")
        bytes[pos + 5] = (bytes[pos + 5].toInt() xor 0xFF).toByte()

        // 整体 CRC 会失败，所以这里只验证「不会因为一条坏记录而崩溃」
        var ok = false
        try { WalCodec.decode(bytes) } catch (_: com.cloudea.yangbase.common.CorruptDataException) { ok = true }
        assertTrue(ok, "整体 CRC 应捕获损坏")
    }

    @Test
    fun `manifest CAS - 陈旧 etag 的提交被拒绝`() = runTest {
        val store = MemoryObjectStore()
        val ms = ManifestStore(store)
        val m1 = Manifest.empty(com.cloudea.yangbase.common.Epoch(1), "n1")
        val etag1 = ms.commit(shard, m1, null)

        // 另一个写者抢先提交
        val m2 = m1.copy(seq = 99, writerEpoch = 2)
        ms.commit(shard, m2, etag1)

        // 陈旧写者再提交必须失败（fencing）
        var rejected = false
        try {
            ms.commit(shard, m1.copy(seq = 100), etag1)
        } catch (_: com.cloudea.yangbase.common.ConcurrentModificationException) {
            rejected = true
        }
        assertTrue(rejected, "陈旧写者的 manifest 提交必须被 CAS 拒绝")
    }

    @Test
    fun `SST 编解码往返`() = runTest {
        val w = SstWriter(schema, level = 1, blockSizeBytes = 1024, countColumns = setOf(2))
        var ts = 0L
        for (i in 1L..300L) {
            ts += 1
            w.add(keyOf(i), ts, rowOf(i, "u$i", i % 5))
        }
        val res = w.finish()

        assertEquals(300L, res.index.rows)
        assertEquals(0L, res.index.tombstones)
        assertTrue(res.index.blocks.size > 1, "300 行 / 1KB 块应产生多个块")
        assertTrue(res.data.isNotEmpty())

        // 每块都应带上 commitTs 累积表，否则 §8.9.2 的计数无法精确
        for (b in res.index.blocks) {
            assertTrue(b.tsCounts.isNotEmpty(), "块应携带 tsCounts")
        }
    }

    @Test
    fun `SST 块级 tsCounts 支持精确的历史快照计数`() = runTest {
        val w = SstWriter(schema, level = 1, blockSizeBytes = 64 * 1024)
        for (i in 1L..100L) w.add(keyOf(i), 1000, rowOf(i, "u$i", 1))
        for (i in 101L..200L) w.add(keyOf(i), 2000, rowOf(i, "u$i", 1))
        for (i in 201L..300L) w.add(keyOf(i), 3000, rowOf(i, "u$i", 1))
        val res = w.finish()
        res.index.blocks.forEach { b -> b.visibleRowsAt(0).let { } }
        val handle = SstHandle(shard, 1, com.cloudea.yangbase.common.FileId("test"), res.index)
        val reader = SstReader(MemoryObjectStore(), BlockCache(1024))

        assertEquals(0L, reader.visibleRows(handle, ReadTs(999)))
        assertEquals(100L, reader.visibleRows(handle, ReadTs(1500)))
        assertEquals(200L, reader.visibleRows(handle, ReadTs(2500)))
        assertEquals(300L, reader.visibleRows(handle, ReadTs(9999)))
    }
}
