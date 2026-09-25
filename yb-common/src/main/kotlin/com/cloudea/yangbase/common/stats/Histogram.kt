package com.cloudea.yangbase.common.stats

import kotlinx.serialization.Serializable

/**
 * 等宽直方图的一个桶。`[lo, hi)`，最后一个桶为闭区间。
 *
 * 注意它**不落盘**：桶边界是推导出来的（见 [Histogram]），这里只是一个便于阅读
 * 与断言的视图。
 */
class HistogramBucket(val lo: Double, val hi: Double, val count: Long) {
    override fun toString() = "[$lo, $hi): $count"
}

/**
 * 列上的等宽直方图。**只对数值列构建**：字符串列的等值选择率由 `ndv` 给出
 * （1/ndv），而按字典序切直方图对 `WHERE s > 'm'` 这类谓词的增益不值得那份体积。
 *
 * ## 为什么桶边界不落盘
 *
 * 等宽 ⇒ 第 i 个桶的 `[lo, hi)` 完全由 `[min, max]`、`bucketCount` 与 i 决定，
 * 逐桶把两个 double 写下来是**纯粹的冗余**——而这是**每一次查询规划**都要读进
 * 内存的那一个对象（原则 P3），冗余的字节数直接落在规划路径上。实测一张 20 列
 * 的表：「逐桶 lo/hi/count 三元组」形态的表级统计对象 113 KB，只写非空桶的下标与
 * 计数后降到 46 KB（其中直方图 17 KB）；估计出的选择率一字不差。
 *
 * 稀疏存储（只写非空桶）保留：`counts` 是 `[下标, 计数, 下标, 计数, …]` 的扁平序列。
 * 它在两种情形下都最小——桶全满时是 32 个整数，只有一个峰时是 2 个数。
 *
 * ⚠️ 编码形态在此定型（此前没有任何已发布的统计对象，格式版本仍是 v1）。
 */
@Serializable
data class Histogram(
    /** 数值域。与 `ColumnStats.minNum`/`maxNum` 相同，放在这里是为了让桶边界**可推导**。 */
    val min: Double = 0.0,
    val max: Double = 0.0,
    /** 等宽桶数（含空桶）。 */
    val bucketCount: Int = 0,
    /** 非空桶的 `[下标, 计数]` 扁平序列。 */
    val counts: List<Long> = emptyList(),
    val totalCount: Long = 0L,
) {

    /** 第 i 个桶的宽度（单点域为 0）。 */
    private val width: Double get() = if (max > min) (max - min) / bucketCount else 0.0

    /** 第 i 个桶的下界（闭）。 */
    private fun lower(i: Int): Double = if (i == 0) min else min + i * width

    /** 第 i 个桶的上界（开；最后一个桶闭）。 */
    private fun upper(i: Int): Double = if (i == bucketCount - 1) max else min + (i + 1) * width

    /** 非空桶的视图（顺序与下标一致）。**规划路径不遍历它**——见 [countIn]。 */
    val buckets: List<HistogramBucket>
        get() = ArrayList<HistogramBucket>(counts.size / 2).apply {
            var i = 0
            while (i + 1 < counts.size) {
                val idx = counts[i].toInt()
                add(HistogramBucket(lower(idx), upper(idx), counts[i + 1]))
                i += 2
            }
        }

    /**
     * 落在 `[lo, hi]` 内的估计行数，按桶内均匀假设在部分重叠的桶上按比例分摊。
     *
     * 直接遍历扁平的 `counts`，不构造桶对象——选择率估算在每次规划里都要跑几次，
     * 而这里是唯一的热路径（CLAUDE.md「热路径零分配」）。
     */
    fun countIn(lo: Double?, hi: Double?): Long {
        if (totalCount <= 0L || counts.isEmpty()) return 0L
        var sum = 0.0
        var i = 0
        while (i + 1 < counts.size) {
            val bLo = lower(counts[i].toInt())
            val bHi = upper(counts[i].toInt())
            val c = counts[i + 1]
            val w = bHi - bLo
            val share = if (w <= 0.0) {
                val hit = (lo == null || lo <= bLo) && (hi == null || bLo <= hi)
                if (hit) 1.0 else 0.0
            } else {
                val from = if (lo == null) bLo else maxOf(lo, bLo)
                val to = if (hi == null) bHi else minOf(hi, bHi)
                val overlap = to - from
                if (overlap <= 0.0) 0.0 else (overlap / w).coerceAtMost(1.0)
            }
            sum += c * share
            i += 2
        }
        return Math.round(sum)
    }

    /** 谓词选择率（保留比例，0.0..1.0）。 */
    fun selectivity(lo: Double?, hi: Double?): Double {
        if (totalCount <= 0L) return 0.0
        return (countIn(lo, hi).toDouble() / totalCount).coerceIn(0.0, 1.0)
    }
}

/**
 * 直方图构建。两种数据来源，对应 `.idx` 能提供的两种块级信息：
 *  - [fromWeightedValues]：块级 `valueCounts`（**精确**，只覆盖低基数列）
 *  - [fromBlockRanges]：块级 `min`/`max` + 块行数（**近似**：假设块内均匀分布）
 *
 * ⚠️ 后者的前提是「块内均匀」，这对聚簇写入（例如按自增主键顺序灌数据）成立得
 * 很好，对随机分布也还算合理——它总比「全表均匀」准确，因为块的 `[min,max]`
 * 是真实信息，而全表 min/max 会把中间的空白也算成有数据。
 */
object HistogramBuilder {

    /** 默认桶数。§10.3 写的是「32 桶」，照做。 */
    const val DEFAULT_BUCKETS = 32

    /** 不同值的精确收集上限：超过它就不再保留精确值集合，改用 HLL 估计。 */
    const val EXACT_DISTINCT_LIMIT = 4096

    /** 由「值 → 出现次数」构建（精确）。`min`/`max` 为数值域边界。 */
    fun fromWeightedValues(min: Double, max: Double, values: Map<Double, Long>, buckets: Int = DEFAULT_BUCKETS): Histogram {
        if (values.isEmpty()) return Histogram()
        val n = buckets.coerceAtLeast(1)
        val counts = DoubleArray(n)
        if (max <= min) {
            counts[0] = values.values.sumOf { it }.toDouble()
        } else {
            val width = (max - min) / n
            for ((v, c) in values) {
                var idx = ((v - min) / width).toInt()
                if (idx < 0) idx = 0
                if (idx > n - 1) idx = n - 1
                counts[idx] += c.toDouble()
            }
        }
        return build(min, max, counts)
    }

    /**
     * 由块级 `[min, max]` + 块行数构建（近似）。
     * 每个块的 `rows` 按它跨越的桶**按宽度比例**分摊，因此总行数守恒。
     */
    fun fromBlockRanges(min: Double, max: Double, blocks: List<Triple<Double, Double, Long>>, buckets: Int = DEFAULT_BUCKETS): Histogram {
        if (blocks.isEmpty()) return Histogram()
        val n = buckets.coerceAtLeast(1)
        val counts = DoubleArray(n)
        if (max <= min) {
            // 单点域：全部落在唯一的桶里
            counts[0] = blocks.sumOf { it.third }.toDouble()
            return build(min, max, counts)
        }
        val width = (max - min) / n
        for ((lo0, hi0, rows) in blocks) {
            if (rows <= 0L) continue
            val lo = lo0.coerceIn(min, max)
            val hi = hi0.coerceIn(min, max)
            val span = hi - lo
            if (span <= 0.0) {
                var idx = ((lo - min) / width).toInt()
                if (idx < 0) idx = 0
                if (idx > n - 1) idx = n - 1
                counts[idx] += rows.toDouble()
                continue
            }
            val first = ((lo - min) / width).toInt().coerceIn(0, n - 1)
            val last = ((hi - min) / width).toInt().coerceIn(0, n - 1)
            for (i in first..last) {
                val bLo = min + i * width
                val bHi = if (i == n - 1) max else bLo + width
                val overlap = minOf(hi, bHi) - maxOf(lo, bLo)
                if (overlap <= 0.0) continue
                counts[i] += rows.toDouble() * (overlap / span)
            }
        }
        return build(min, max, counts)
    }

    /** 把浮点计数落成整数桶，**让总和精确守恒**（余数塞进计数最大的桶，避免出现空桶）。 */
    private fun build(min: Double, max: Double, counts: DoubleArray): Histogram {
        val n = counts.size
        val ints = LongArray(n)
        var assigned = 0L
        for (i in 0 until n) {
            ints[i] = Math.round(counts[i])
            assigned += ints[i]
        }
        val exactTotal = Math.round(counts.sum())
        val diff = exactTotal - assigned
        if (diff != 0L) {
            var target = 0
            for (i in 1 until n) if (ints[i] > ints[target]) target = i
            ints[target] = (ints[target] + diff).coerceAtLeast(0L)
        }
        // 稀疏 + 省掉桶边界：只写 [下标, 计数]。边界由 min/max/桶数推导（见 Histogram 的 KDoc）
        val flat = ArrayList<Long>()
        for (i in 0 until n) {
            if (ints[i] == 0L) continue
            flat.add(i.toLong())
            flat.add(ints[i])
        }
        return Histogram(min = min, max = max, bucketCount = n, counts = flat, totalCount = ints.sum())
    }
}
