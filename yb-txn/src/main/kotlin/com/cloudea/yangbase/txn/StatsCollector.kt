package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.stats.ColumnStats
import com.cloudea.yangbase.common.stats.HistogramBuilder
import com.cloudea.yangbase.common.stats.HllSketch
import com.cloudea.yangbase.common.stats.NdvSource
import com.cloudea.yangbase.common.stats.PartitionStats
import com.cloudea.yangbase.common.stats.StatsCodec
import com.cloudea.yangbase.storage.BlockIndexEntry
import com.cloudea.yangbase.storage.SstIndex

/** `ANALYZE` 对**一个分区**的采集结果。 */
data class CollectedPartition(
    val summary: PartitionStats,
    /** 该分区的列统计（会写成 `ObjectKeys.columnStats(...)` 那个对象）。 */
    val columns: List<ColumnStats>,
    val notes: List<String>,
)

/**
 * 从 `.idx` 聚合出统计信息（设计文档 §10.3、§13.3）。
 *
 * **这个类不做任何 IO**：它的输入是已经常驻内存的 [SstIndex] 列表
 * （由 `LsmStore.loadedIndexes()` 给出），输出是可以直接落盘的统计模型。
 * 因此它既能在真实引擎上跑，也能在单元测试里用合成索引喂数据。
 *
 * 采集的诚实边界（都在 `notes` 里如实写出，不静默）：
 *  - 列统计只覆盖**已落盘**的 SST：MemTable 里还没 flush 的行不参与
 *    （行数本身仍然是精确的——那是 manifest 的精确计数，§8.9.1）；
 *  - `ndv` 只有收集了块级 `valueCounts` 的列才可能精确，其余列是估计或上界；
 *  - 不同文件对同一列的收集状态可能不同（块内基数超限会**按文件**放弃），
 *    这种情况下 HLL 覆盖不全，是**下界**。
 */
class StatsCollector(
    private val schema: TableSchema,
    private val buckets: Int = HistogramBuilder.DEFAULT_BUCKETS,
) {

    private val tableAcc = Array(schema.columns.size) { ColumnAccumulator(it) }
    private val noteList = LinkedHashSet<String>()
    private var tableRows = 0L
    private var tableFlushedRows = 0L
    private var partitionsSeen = 0
    private var allExact = true

    /** 采集一个分区。`indexes` 是该分片当前 manifest 引用的全部 `.idx`。 */
    fun collect(
        partitionId: Long,
        partitionName: String,
        indexes: List<SstIndex>,
        exactRows: Long?,
        memtableEntries: Long,
    ): CollectedPartition {
        val acc = Array(schema.columns.size) { ColumnAccumulator(it) }
        var flushedRows = 0L
        var blocks = 0
        var filesWithoutColStats = 0

        for (idx in indexes) {
            flushedRows += idx.rows
            blocks += idx.blocks.size
            if (idx.blocks.any { it.colStats.isEmpty() }) filesWithoutColStats++
            for (block in idx.blocks) {
                for (i in schema.columns.indices) {
                    observeBlock(acc[i], block, i)
                    observeBlock(tableAcc[i], block, i)
                }
            }
        }
        if (filesWithoutColStats > 0) {
            noteList += "有 $filesWithoutColStats 个文件不含块级列统计（旧格式），未参与列统计：min/max/ndv 可能偏小"
        }

        val rows = exactRows ?: flushedRows
        if (exactRows != null && exactRows != flushedRows) {
            noteList += "分区 p$partitionId：精确行数 $exactRows，已落盘 $flushedRows" +
                "（差 ${exactRows - flushedRows} 行在 MemTable 中，未参与列统计）"
        }
        if (exactRows == null) {
            allExact = false
            noteList += "分区 p$partitionId 未维护精确行数（exact_count=false），行数取自分片文件"
        }
        if (memtableEntries > 0 && exactRows == null) {
            noteList += "分区 p$partitionId 的 MemTable 中还有 $memtableEntries 个条目，未参与统计"
        }

        val keyCol = schema.partitionColumn?.let { name -> schema.columns.indexOfFirst { it.name.equals(name, ignoreCase = true) } } ?: -1
        val summary = PartitionStats(
            partitionId = partitionId,
            partitionName = partitionName,
            rowCount = rows,
            flushedRows = flushedRows,
            fileCount = indexes.size,
            blockCount = blocks,
            keyColumn = if (keyCol >= 0) schema.columns[keyCol].name else null,
            keyMin = if (keyCol >= 0) StatsCodec.render(acc[keyCol].minBytes, schema, keyCol) else null,
            keyMax = if (keyCol >= 0) StatsCodec.render(acc[keyCol].maxBytes, schema, keyCol) else null,
        )
        tableRows += rows
        tableFlushedRows += flushedRows
        partitionsSeen++
        return CollectedPartition(summary, acc.mapIndexed { i, a -> a.build(schema, i, buckets, noteList) }, noteList.toList())
    }

    /** 跨分区合并后的列统计（表级对象里的 `columns`）。 */
    fun mergedColumns(): List<ColumnStats> {
        val notes = LinkedHashSet<String>()
        return tableAcc.mapIndexed { i, a -> a.build(schema, i, buckets, notes) }
    }

    fun tableRowCount(): Long = tableRows

    fun tableFlushedRows(): Long = tableFlushedRows

    fun partitionCount(): Int = partitionsSeen

    /** 全部分区的行数是否都来自精确计数（否则是「文件行数之和」，偏小）。 */
    fun rowsExact(): Boolean = allExact

    fun notes(): List<String> = noteList.toList()

    // ── 内部 ──────────────────────────────────────────────────────

    private fun observeBlock(acc: ColumnAccumulator, block: BlockIndexEntry, ordinal: Int) {
        val cs = block.colStats
        if (cs.size <= ordinal) {
            acc.blocksWithoutValues++
            return
        }
        val stat = cs[ordinal]
        acc.observedRows += block.rows
        acc.nullCount += stat.nullCount
        stat.min?.let { acc.observeMin(it) }
        stat.max?.let { acc.observeMax(it) }
        val vc = stat.valueCounts
        if (vc == null) {
            acc.blocksWithoutValues++
        } else {
            acc.blocksWithValues++
            for (v in vc) {
                acc.observeValue(v.value)
                // 只有收集了值计数的列（BOOLEAN，以及将来可能的其他低基数列）才有
                // **精确**的「值 → 行数」。把它的数值形式攒起来，直方图就不必退化成
                // 「块内均匀」的近似——对 BOOLEAN 这种两三个值的列，近似会把
                // `flag = true` 的选择率算错一半。
                StatsCodec.numericOf(v.value, schema, ordinal)?.let { n ->
                    acc.histValues[n] = (acc.histValues[n] ?: 0L) + v.count
                }
            }
        }
        // 直方图的原料：数值列的块级 [min,max] + 非空行数
        val lo = StatsCodec.numericOf(stat.min, schema, ordinal)
        val hi = StatsCodec.numericOf(stat.max, schema, ordinal)
        if (lo != null && hi != null) {
            val nonNull = (block.rows - stat.nullCount).coerceAtLeast(0L)
            acc.histBlocks.add(Triple(lo, hi, nonNull))
        }
    }

    private class ColumnAccumulator(val ordinal: Int) {
        var observedRows = 0L
        var nullCount = 0L
        var minBytes: ByteArray? = null
        var maxBytes: ByteArray? = null
        var blocksWithValues = 0
        var blocksWithoutValues = 0
        var exact: HashSet<ByteKey>? = HashSet()
        val hll: HllSketch = HllSketch.of()
        var sawValueCounts = false
        val histValues = HashMap<Double, Long>()
        val histBlocks = ArrayList<Triple<Double, Double, Long>>()

        fun observeMin(b: ByteArray) {
            val cur = minBytes
            if (cur == null || ByteArrayComparator.compare(b, cur) < 0) minBytes = b
        }

        fun observeMax(b: ByteArray) {
            val cur = maxBytes
            if (cur == null || ByteArrayComparator.compare(b, cur) > 0) maxBytes = b
        }

        fun observeValue(encoded: ByteArray) {
            sawValueCounts = true
            hll.add(encoded)
            val set = exact
            if (set != null) {
                set.add(ByteKey(encoded))
                if (set.size > HistogramBuilder.EXACT_DISTINCT_LIMIT) exact = null
            }
        }

        fun build(schema: TableSchema, index: Int, buckets: Int, notes: MutableSet<String>): ColumnStats {
            val col = schema.columns[index]
            val minNum = StatsCodec.numericOf(minBytes, schema, index)
            val maxNum = StatsCodec.numericOf(maxBytes, schema, index)

            val exactSet = exact
            val ndv: Long
            val ndvExact: Boolean
            val source: String
            when {
                // 整列为 NULL：不同值个数恰好是 0，这是**精确**结论，不是「不知道」
                observedRows > 0L && nullCount >= observedRows -> {
                    ndv = 0L; ndvExact = true; source = NdvSource.VALUES
                }
                exactSet != null && sawValueCounts -> {
                    ndv = exactSet.size.toLong(); ndvExact = true; source = NdvSource.VALUES
                }
                sawValueCounts -> {
                    ndv = hll.estimate(); ndvExact = false; source = NdvSource.HLL
                }
                minNum != null && maxNum != null -> {
                    val span = maxNum - minNum + 1.0
                    ndv = minOf(observedRows, if (span >= 9.2e18) Long.MAX_VALUE else span.toLong())
                        .coerceAtLeast(if (observedRows > 0) 1L else 0L)
                    ndvExact = false; source = NdvSource.RANGE
                }
                else -> {
                    ndv = ColumnStats.NDV_UNKNOWN; ndvExact = false; source = NdvSource.UNKNOWN
                }
            }

            if (source == NdvSource.HLL && blocksWithoutValues > 0) {
                notes += "列 ${col.name}：ndv 是 HLL 估计，但只覆盖了 $blocksWithValues 个含值计数的块" +
                    "（另有 $blocksWithoutValues 个块没有值计数）——该数字是**下界**"
            }
            if (source == NdvSource.RANGE) {
                notes += "列 ${col.name}：ndv 是数值域上界 min(行数, max-min+1)，不是估计值"
            }
            if (source == NdvSource.UNKNOWN && observedRows > 0) {
                val potentialCountColumn = col.type == ColumnType.BOOLEAN ||
                    (col.type == ColumnType.VARCHAR && (col.length ?: 255) <= 64)
                notes += if (potentialCountColumn) {
                    "列 ${col.name}：块内不同值超过上限（256）而被放弃收集值计数 → ndv 未知"
                } else {
                    "列 ${col.name}：该列类型不收集块级值计数（只收集 BOOLEAN 与 VARCHAR(≤64)）→ ndv 未知"
                }
            }

            val histogram = when {
                // 值计数**覆盖了全部块**时才用精确权重：只覆盖一部分块的话，
                // 直方图的总数会偏小，而选择率是拿它除以总数算出来的——
                // 那会系统性低估范围谓词，进而选错访问路径
                histValues.isNotEmpty() && minNum != null && maxNum != null && blocksWithoutValues == 0 ->
                    HistogramBuilder.fromWeightedValues(minNum, maxNum, histValues, buckets)
                histBlocks.isNotEmpty() && minNum != null && maxNum != null ->
                    HistogramBuilder.fromBlockRanges(minNum, maxNum, histBlocks, buckets)
                else -> null
            }

            return ColumnStats(
                name = col.name,
                ordinal = index,
                type = col.type.name,
                observedRows = observedRows,
                nullCount = nullCount,
                min = StatsCodec.render(minBytes, schema, index),
                max = StatsCodec.render(maxBytes, schema, index),
                minNum = minNum,
                maxNum = maxNum,
                ndv = ndv,
                ndvExact = ndvExact,
                ndvSource = source,
                histogram = histogram,
                blocksWithValues = blocksWithValues,
                blocksWithoutValues = blocksWithoutValues,
            )
        }
    }

    /** 字节串的比较键：`ByteArray` 的 `equals` 是引用相等，直接放进 Set 会去重失败。 */
    private class ByteKey(val b: ByteArray) {
        override fun hashCode(): Int = b.contentHashCode()
        override fun equals(other: Any?): Boolean = other is ByteKey && b.contentEquals(other.b)
    }
}
