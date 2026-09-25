package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.stats.ColumnStats
import com.cloudea.yangbase.common.stats.HistogramBuilder
import com.cloudea.yangbase.common.stats.NdvSource
import com.cloudea.yangbase.common.stats.TableStats
import com.cloudea.yangbase.storage.BlockIndexEntry
import com.cloudea.yangbase.storage.BloomData
import com.cloudea.yangbase.storage.ColumnStat
import com.cloudea.yangbase.storage.SstIndex
import com.cloudea.yangbase.storage.ValueCount
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `ANALYZE` 的**纯聚合**测试（设计文档 §10.3）。
 *
 * 这里喂的是合成出来的 [SstIndex]——不碰对象存储、不碰 `.dat`。这既是测试的
 * 需要，也正是这一层被设计成「无 IO」的原因（§13.3）：采集的全部输入是常驻内存的
 * 块级元数据，因此「ANALYZE 不读数据块」是**结构上**成立的，而不是靠小心。
 *
 * 重点断言的是**ndv 的来源与诚实边界**：什么时候精确、什么时候是估计、
 * 什么时候只是上界、什么时候干脆不知道。这些区分不对，代价模型就会把
 * 一个上界当成真实基数用，进而选错访问路径。
 */
class StatsCollectorTest {

    private val schema = TableSchema(
        tableId = 7, databaseId = 1, name = "t", version = 3,
        columns = listOf(
            ColumnDef("id", ColumnType.BIGINT, nullable = false),
            ColumnDef("status", ColumnType.VARCHAR, nullable = true, length = 16),
            ColumnDef("n", ColumnType.INT, nullable = true),
            ColumnDef("note", ColumnType.VARCHAR, nullable = true, length = 255),
            ColumnDef("flag", ColumnType.BOOLEAN, nullable = true),
        ),
        primaryKey = listOf("id"),
    )

    private fun i64(v: Long): ByteArray = KeyEncoder.encodeDatum(Datum.I64(v), ColumnType.BIGINT)
    private fun str(v: String): ByteArray = KeyEncoder.encodeDatum(Datum.Str(v), ColumnType.VARCHAR)

    private fun block(
        rows: Int,
        idMin: Long,
        idMax: Long,
        statuses: Map<String, Long>?,
        nMin: Int,
        nMax: Int,
        nulls: Long = 0,
        flagCounts: Map<String, Long>? = null,
    ) = BlockIndexEntry(
        offset = 0, length = 0, entries = rows, rows = rows, tombstones = 0,
        minTs = 1, maxTs = 1,
        firstKey = i64(idMin),
        colStats = listOf(
            ColumnStat(min = i64(idMin), max = i64(idMax)),
            ColumnStat(
                min = statuses?.keys?.minOrNull()?.let { str(it) },
                max = statuses?.keys?.maxOrNull()?.let { str(it) },
                nullCount = if (statuses == null) rows.toLong() else nulls,
                valueCounts = statuses?.map { ValueCount(str(it.key), it.value) },
            ),
            ColumnStat(min = i64(nMin.toLong()), max = i64(nMax.toLong())),
            // VARCHAR(255)：**不收集** valueCounts（只有 BOOLEAN 与 VARCHAR(≤64) 收集）
            ColumnStat(min = str("aaa"), max = str("zzz")),
            ColumnStat(
                min = i64(0), max = i64(1),
                valueCounts = flagCounts?.map { ValueCount(i64(it.key.toLong()), it.value) },
            ),
        ),
    )

    private fun index(vararg blocks: BlockIndexEntry, rows: Long = blocks.sumOf { it.rows }.toLong()) = SstIndex(
        level = 1,
        rows = rows,
        tombstones = 0,
        minKey = blocks.first().firstKey,
        maxKey = blocks.last().firstKey,
        minTs = 1, maxTs = 1,
        blocks = blocks.toList(),
        bloom = BloomData(ByteArray(0), 0, 0),
    )

    @Test
    fun `行数与块数来自 manifest 与 idx - 与数据块无关`() {
        val c = StatsCollector(schema)
        val p = c.collect(
            partitionId = 0, partitionName = "p0",
            indexes = listOf(
                index(
                    block(1000, 1, 1000, mapOf("ok" to 900L, "bad" to 100L), 0, 999),
                    block(1000, 1001, 2000, mapOf("ok" to 1000L), 0, 999),
                )
            ),
            exactRows = 2000, memtableEntries = 0,
        )
        assertEquals(2000L, p.summary.rowCount)
        assertEquals(2000L, p.summary.flushedRows)
        assertEquals(2, p.summary.blockCount)
        assertEquals(1, p.summary.fileCount)
        assertEquals(2000L, c.tableRowCount())
        assertTrue(c.rowsExact(), "精确行数来自 manifest 的计数，应当被相信")
    }

    @Test
    fun `低基数列的 ndv 精确 - 来源标为 values`() {
        val c = StatsCollector(schema)
        // 800 个 ok + 100 个 bad + 100 个 NULL = 1000 行
        c.collect(
            0, "p0",
            listOf(index(block(1000, 1, 1000, mapOf("ok" to 800L, "bad" to 100L), 0, 999, nulls = 100L))),
            exactRows = 1000, memtableEntries = 0,
        )
        val status = c.mergedColumns().first { it.name == "status" }
        assertEquals(2L, status.ndv)
        assertTrue(status.ndvExact, "块级 valueCounts 的并集是精确的")
        assertEquals(NdvSource.VALUES, status.ndvSource)
        assertEquals(100L, status.nullCount)
        assertEquals("bad", status.min)
        assertEquals("ok", status.max)
    }

    @Test
    fun `整列为 NULL 时 ndv 是精确的 0`() {
        val c = StatsCollector(schema)
        c.collect(
            0, "p0",
            listOf(index(block(500, 1, 500, statuses = null, nMin = 1, nMax = 500))),
            exactRows = 500, memtableEntries = 0,
        )
        val status = c.mergedColumns().first { it.name == "status" }
        assertEquals(0L, status.ndv, "整列为 NULL ⇒ 不同值个数恰好是 0，这是精确结论")
        assertTrue(status.ndvExact)
        assertEquals(1.0, status.nullFraction, 0.0001)
    }

    @Test
    fun `不同值超过精确上限后改用 HLL - 并如实标注是下界`() {
        // 20 个块 × 300 个不同值 = 6000 个不同值 > EXACT_DISTINCT_LIMIT(4096)
        val blocks = (0 until 20).map { b ->
            block(
                rows = 300, idMin = b * 300L + 1, idMax = b * 300L + 300,
                statuses = (0 until 300).associate { "s${b}_$it" to 1L },
                nMin = 0, nMax = 299,
            )
        }
        val c = StatsCollector(schema)
        c.collect(0, "p0", listOf(index(*blocks.toTypedArray())), exactRows = 6000, memtableEntries = 0)

        val status = c.mergedColumns().first { it.name == "status" }
        assertEquals(NdvSource.HLL, status.ndvSource)
        assertTrue(!status.ndvExact, "HLL 是估计，不能标成精确")
        val rel = kotlin.math.abs(status.ndv - 6000).toDouble() / 6000
        assertTrue(rel < 0.15, "HLL 估计 ${status.ndv} 偏离 6000 达 ${"%.3f".format(rel)}")
    }

    @Test
    fun `不收集值计数的列只能是上界 - 且必须留下说明`() {
        val c = StatsCollector(schema)
        c.collect(
            0, "p0",
            listOf(index(block(1000, 1, 1000, mapOf("ok" to 1000L), nMin = 0, nMax = 999))),
            exactRows = 1000, memtableEntries = 0,
        )
        val n = c.mergedColumns().first { it.name == "n" }
        assertEquals(NdvSource.RANGE, n.ndvSource)
        assertTrue(!n.ndvExact)
        assertEquals(1000L, n.ndv, "min(行数, max-min+1) 是上界")
        assertTrue(
            c.notes().any { it.contains("列 n") && it.contains("上界") },
            "上界这件事必须出现在 notes 里，否则使用者会把 1000 当成真实基数：${c.notes()}",
        )

        // VARCHAR(255) 本来就不收集值计数——说明文字应当指出是**列类型**的原因，
        // 而不是「超限被放弃」（两者都是 ndv 未知，但修法完全不同）
        val note = c.mergedColumns().first { it.name == "note" }
        assertEquals(NdvSource.UNKNOWN, note.ndvSource)
        assertEquals(ColumnStats.NDV_UNKNOWN, note.ndv)
        assertTrue(
            c.notes().any { it.contains("列 note") && it.contains("不收集") },
            "应当区分「列类型不收集」与「超限放弃」：${c.notes()}",
        )
    }

    @Test
    fun `最低基数的布尔列也是精确 ndv`() {
        val c = StatsCollector(schema)
        c.collect(
            0, "p0",
            listOf(index(block(1000, 1, 1000, mapOf("ok" to 1000L), 0, 999, flagCounts = mapOf("1" to 300L, "0" to 700L)))),
            exactRows = 1000, memtableEntries = 0,
        )
        val flag = c.mergedColumns().first { it.name == "flag" }
        assertEquals(2L, flag.ndv)
        assertTrue(flag.ndvExact)
        assertEquals(1.0 / 2, flag.equalitySelectivity(), 0.0001)
    }

    @Test
    fun `数值列有直方图 - 且它的总行数与观测行数一致`() {
        val c = StatsCollector(schema)
        c.collect(
            0, "p0",
            listOf(
                index(
                    block(1000, 1, 1000, mapOf("ok" to 1000L), nMin = 0, nMax = 999),
                    block(1000, 1001, 2000, mapOf("ok" to 1000L), nMin = 1000, nMax = 1999),
                )
            ),
            exactRows = 2000, memtableEntries = 0,
        )
        val n = c.mergedColumns().first { it.name == "n" }
        val h = n.histogram
        assertTrue(h != null, "数值列应当有直方图")
        assertEquals(2000L, h!!.totalCount, "直方图的总行数必须等于观测到的非空行数")
        assertEquals(2000L, h.buckets.sumOf { it.count })
        assertTrue(h.bucketCount <= HistogramBuilder.DEFAULT_BUCKETS)
        // 均匀分布下前半段应占一半
        val firstHalf = n.rangeSelectivity(0.0, 999.0)
        assertTrue(firstHalf in 0.4..0.6, "前半段应约 50%，实际 ${"%.3f".format(firstHalf)}")
    }

    @Test
    fun `MemTable 里未落盘的行不参与列统计 - 但必须留下说明`() {
        val c = StatsCollector(schema)
        c.collect(
            0, "p0",
            listOf(index(block(1000, 1, 1000, mapOf("ok" to 1000L), 0, 999))),
            exactRows = 1200, memtableEntries = 17,
        )
        assertEquals(1200L, c.tableRowCount(), "行数用 manifest 的精确值")
        assertEquals(1000L, c.tableFlushedRows(), "已落盘行数只算 .idx")
        assertTrue(
            c.notes().any { it.contains("200") && it.contains("MemTable") },
            "有 200 行未参与列统计，这件事必须写明（否则 min/max/ndv 看起来像是全表的）：${c.notes()}",
        )
    }

    @Test
    fun `没有精确行数时退回文件行数之和 - 并标注不精确`() {
        val c = StatsCollector(schema)
        c.collect(
            0, "p0",
            listOf(index(block(1000, 1, 1000, mapOf("ok" to 1000L), 0, 999))),
            exactRows = null, memtableEntries = 0,
        )
        assertEquals(1000L, c.tableRowCount())
        assertTrue(!c.rowsExact(), "没有精确计数时必须标为不精确，不能让使用者以为它是准的")
        assertTrue(c.notes().any { it.contains("exact_count") })
    }

    @Test
    fun `没有块级列统计的旧文件不会把统计带偏`() {
        // 模拟旧格式：块里一个 colStats 都没有
        val legacy = BlockIndexEntry(
            offset = 0, length = 0, entries = 500, rows = 500, tombstones = 0,
            minTs = 1, maxTs = 1, firstKey = i64(1), colStats = emptyList(),
        )
        val c = StatsCollector(schema)
        c.collect(0, "p0", listOf(index(legacy)), exactRows = 500, memtableEntries = 0)

        assertTrue(
            c.notes().any { it.contains("不含块级列统计") },
            "旧格式文件必须被指出来：${c.notes()}",
        )
        // 行数不受影响——它来自 manifest 的精确计数，与列统计是否完整无关
        assertEquals(500L, c.tableRowCount())
    }

    @Test
    fun `合并后的表级统计能直接算选择率 - 这是代价模型唯一的输入`() {
        val c = StatsCollector(schema)
        val p = c.collect(
            0, "p0",
            listOf(index(block(2000, 1, 2000, mapOf("ok" to 1500L, "bad" to 500L), nMin = 0, nMax = 1999))),
            exactRows = 2000, memtableEntries = 0,
        )
        val stats = TableStats(
            tableId = schema.tableId, tableName = schema.name, statsVersion = schema.version,
            rowCount = c.tableRowCount(), rowCountExact = c.rowsExact(),
            partitions = listOf(p.summary), columns = c.mergedColumns(), notes = c.notes(),
        )
        assertEquals(0.5, stats.column("status")!!.equalitySelectivity(), 0.0001)
        // rowsPerBlock 是「行数 → 请求数」的换算系数（R1），必须来自真实块数
        assertEquals(2000.0, stats.rowsPerBlock, 0.0001)
        assertEquals(1L, stats.blockCount)
    }

    @Test
    fun `布尔列的直方图来自精确值计数 - 不是块内均匀的猜测`() {
        val c = StatsCollector(schema)
        c.collect(
            0, "p0",
            listOf(index(block(1000, 1, 1000, mapOf("ok" to 1000L), 0, 999, flagCounts = mapOf("1" to 300L, "0" to 700L)))),
            exactRows = 1000, memtableEntries = 0,
        )
        val flag = c.mergedColumns().first { it.name == "flag" }
        assertEquals(1000L, flag.histogram!!.totalCount)
        // 只有 300/1000 落在 true 那一侧——「块内均匀」的近似给出的数字会完全不同
        assertEquals(0.3, flag.histogram!!.selectivity(0.5, 1.0), 0.01)
    }
}
