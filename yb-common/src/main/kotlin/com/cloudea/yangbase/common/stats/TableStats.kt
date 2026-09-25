package com.cloudea.yangbase.common.stats

import kotlinx.serialization.Serializable

/**
 * 优化器统计信息（设计文档 §10.3）。
 *
 * 它是**对象存储上的普通对象**（原则 P1）：`stats/table/{table_id}/partitions.v{n}.json`
 * 与 `stats/table/{table_id}/{partition_id}/colstats.v{n}.json`（键由
 * `ObjectKeys.partitionStats` / `ObjectKeys.columnStats` 生成，见 §5.1）。
 * 没有任何一份权威统计留在本地磁盘或内存里——本地只有可丢弃的缓存。
 *
 * 体积要求（原则 P3）：表级对象必须小到能整块读进内存供**每一次查询规划**使用，
 * 因此：
 *  - 列级统计在表级对象里是**跨分区合并后的结果**，规划只需 1 次 GET；
 *  - 空桶不落盘、草图不落盘（只需要数字），一个几十列的表仍在几十 KB 量级。
 *
 * ⚠️ 与 `com.cloudea.yangbase.storage.ColumnStat`（**块级**、在 `.idx` 里）不是一回事：
 * 那个是采集的原料，这个是聚合后的产物。
 */
@Serializable
data class TableStats(
    val formatVersion: Int = STATS_FORMAT_VERSION,
    val tableId: Long,
    val tableName: String = "",
    /** 统计对应的**表元数据版本**。任何 DDL 都会 +1，从而让旧统计自动失效（§5.2 规则 3）。 */
    val statsVersion: Long,
    val rowCount: Long,
    /** `rowCount` 是否精确（来自 manifest 的精确行数）。false 表示它是 `Σ 文件行数`，可能偏小。 */
    val rowCountExact: Boolean = false,
    val collectedAtMs: Long = 0L,
    val analyzer: String = "",
    /** 是否覆盖了应当被统计的全部分片/文件。false ⇒ 数字偏小，只能当上界用。 */
    val complete: Boolean = true,
    val partitions: List<PartitionStats> = emptyList(),
    /** 全表合并后的列统计。**代价模型只读这一份**，避免逐分区加载。 */
    val columns: List<ColumnStats> = emptyList(),
    /** 采集过程中的说明（缺什么、为什么缺）。让「统计不全」这件事是可见的，而不是静默。 */
    val notes: List<String> = emptyList(),
) {

    fun column(name: String): ColumnStats? =
        columns.firstOrNull { it.name.equals(name, ignoreCase = true) }

    /** 合并后的块总数（Σ 分区）。 */
    val blockCount: Long get() = partitions.sumOf { it.blockCount.toLong() }

    val fileCount: Int get() = partitions.sumOf { it.fileCount }

    /**
     * 平均每块行数——把「行数」换算成「对象存储请求数」的关键系数（R1）。
     * 没有统计时用 64 KB 块的经验值兜底。
     */
    val rowsPerBlock: Double
        get() {
            val rows = partitions.sumOf { it.flushedRows }
            val blocks = partitions.sumOf { it.blockCount.toLong() }
            if (rows <= 0L || blocks <= 0L) return FALLBACK_ROWS_PER_BLOCK
            return (rows.toDouble() / blocks).coerceAtLeast(1.0)
        }

    /** 粗略的对象体积（JSON 形态），用于报告与体积断言。 */
    fun estimatedBytes(): Long {
        var n = 400L + notes.sumOf { it.length.toLong() + 8 }
        for (p in partitions) n += 160L
        for (c in columns) {
            n += 220L
            n += (c.min?.length ?: 0) + (c.max?.length ?: 0)
            // 直方图存的是扁平的 [下标, 计数] 序列（见 Histogram 的 KDoc），一项约 7 字节
            n += (c.histogram?.counts?.size ?: 0) * 7L
        }
        return n
    }

    companion object {
        /** 统计对象的格式版本。字段只增不改，读到未知字段忽略（`ignoreUnknownKeys`）。 */
        const val STATS_FORMAT_VERSION = 1

        /** 没有统计时「一块有多少行」的兜底值（块目标 64 KB，行按 ~64 字节估）。 */
        const val FALLBACK_ROWS_PER_BLOCK = 1000.0
    }
}

/**
 * 分区级摘要。**分区裁剪（§9.2）读的就是这里的 `keyMin`/`keyMax`**：
 * 一个分区对应一个分片，能整片裁掉就一次请求都不用发。
 */
@Serializable
data class PartitionStats(
    val partitionId: Long,
    val partitionName: String = "",
    /** 含 MemTable 的精确行数（若可得），否则为 `flushedRows`。 */
    val rowCount: Long,
    /** 已落盘部分的行数（`Σ .idx.rows`）。列统计只覆盖这一部分。 */
    val flushedRows: Long,
    val fileCount: Int = 0,
    val blockCount: Int = 0,
    /** 分区键列的取值区间（仅 RANGE/LIST 之类可排序分区才有）。 */
    val keyColumn: String? = null,
    val keyMin: String? = null,
    val keyMax: String? = null,
)

/**
 * 单个分区的列统计对象（`stats/table/{table_id}/{partition_id}/colstats.v{n}.json`，§10.3）。
 *
 * 与 [TableStats] 是**一对多**：表级那份是给优化器规划用的（1 次 GET、跨分区合并），
 * 分区级这份是给「想知道详情」的场景用的（例如将来做分区级选择性、或者人工排查
 * 「为什么优化器选错了」）。规划路径**不读**它们。
 */
@Serializable
data class PartitionColumnStats(
    val formatVersion: Int = TableStats.STATS_FORMAT_VERSION,
    val tableId: Long,
    val partitionId: Long,
    val partitionName: String = "",
    val statsVersion: Long,
    val rowCount: Long,
    val flushedRows: Long,
    val fileCount: Int = 0,
    val blockCount: Int = 0,
    val columns: List<ColumnStats> = emptyList(),
    val notes: List<String> = emptyList(),
)

/** `ndv` 的来源。用来区分「算出来的」与「猜的」，避免把估计值当精确值用。 */
object NdvSource {
    /** 完全不知道。 */
    const val UNKNOWN = "unknown"
    /** 由块级 `valueCounts` 精确并集得到（只可能是低基数列）。 */
    const val VALUES = "values"
    /** 由 HyperLogLog 草图估计。 */
    const val HLL = "hll"
    /** 由数值域 `min`/`max` 推出的上界 `min(rows, max-min+1)`——**是上界，不是估计**。 */
    const val RANGE = "range"
}

/**
 * 列级统计（§10.3 的 `columns` 元素）。
 *
 * ⭐ 哪些字段真的能填出来，取决于 `.idx` 里有什么，这里如实标注（详见交付报告）：
 *  - `min`/`max`/`nullCount`：所有列都有（块级 `ColumnStat` 无条件收集）
 *  - `ndv`：只有**收集了 `valueCounts`** 的列才可能精确（低基数列，
 *    见 `DatabaseEngine.countColumnsOf`）；其余列只能是 HLL/上界/未知
 *  - `histogram`：数值列才有（低基数列来自精确值，其余来自块级 min/max + 均匀假设）
 */
@Serializable
data class ColumnStats(
    val name: String,
    val ordinal: Int,
    val type: String,
    /** `Σ 块行数`——列统计覆盖到的行数（不含未落盘的 MemTable 行）。 */
    val observedRows: Long,
    val nullCount: Long,
    val min: String? = null,
    val max: String? = null,
    /** 数值列才有的可比形式（`DECIMAL` 是未缩放整数，`DATETIME` 是毫秒）。 */
    val minNum: Double? = null,
    val maxNum: Double? = null,
    /** 不同值个数；[NDV_UNKNOWN] 表示不知道。 */
    val ndv: Long = NDV_UNKNOWN,
    val ndvExact: Boolean = false,
    val ndvSource: String = NdvSource.UNKNOWN,
    val histogram: Histogram? = null,
    /** 有 `valueCounts` 的块数 / 没有的块数——区分「没收集」与「超限放弃了」。 */
    val blocksWithValues: Int = 0,
    val blocksWithoutValues: Int = 0,
) {

    /** 数值域已知（能拿来做范围估算）。 */
    val isNumeric: Boolean get() = minNum != null && maxNum != null

    val ndvKnown: Boolean get() = ndv >= 0L

    val nullFraction: Double
        get() = if (observedRows <= 0L) 0.0 else (nullCount.toDouble() / observedRows).coerceIn(0.0, 1.0)

    /**
     * 等值谓词 `col = ?`（或 `IN` 的单个值）的**保留比例**。
     *
     * `ndv` 已知时是 `1/ndv`——这正是低基数列（状态、类型这种）的典型场景，
     * 也是「谓词计数可以走元数据」的前提：`status='failed'` 选择率 1/4 时，
     * 扫描要付 1/4 张表的 GET，而元数据计数付 0 次（§8.9.4 / R6）。
     */
    fun equalitySelectivity(): Double {
        if (ndvKnown && ndv > 0L) return (1.0 / ndv).coerceIn(0.0, 1.0)
        // 数值列还有一个退路：域宽（上界），至少比瞎猜好
        if (isNumeric) {
            val span = maxNum!! - minNum!! + 1.0
            if (span > 0.0 && observedRows > 0L) {
                return (1.0 / minOf(span, observedRows.toDouble())).coerceIn(0.0, 1.0)
            }
        }
        return DEFAULT_EQ_SELECTIVITY
    }

    /** 范围谓词 `col >= lo AND col <= hi`（任一端可空）的保留比例。 */
    fun rangeSelectivity(lo: Double?, hi: Double?): Double {
        val h = histogram
        if (h != null && h.totalCount > 0L) return h.selectivity(lo, hi)
        if (isNumeric) {
            val mn = minNum!!
            val mx = maxNum!!
            if (mx <= mn) {
                val hit = (lo == null || lo <= mn) && (hi == null || mn <= hi)
                return if (hit) 1.0 else 0.0
            }
            val from = if (lo == null) mn else maxOf(lo, mn)
            val to = if (hi == null) mx else minOf(hi, mx)
            if (to < from) return 0.0
            return ((to - from) / (mx - mn)).coerceIn(0.0, 1.0)
        }
        return DEFAULT_RANGE_SELECTIVITY
    }

    companion object {
        const val NDV_UNKNOWN = -1L

        /** 没有任何信息时的等值选择率（沿用 MySQL 的经验默认值）。 */
        const val DEFAULT_EQ_SELECTIVITY = 0.1

        /** 没有任何信息时的范围选择率。 */
        const val DEFAULT_RANGE_SELECTIVITY = 0.333
    }
}
