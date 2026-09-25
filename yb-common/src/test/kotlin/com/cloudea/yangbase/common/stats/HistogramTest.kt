package com.cloudea.yangbase.common.stats

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 直方图与选择率的测试（设计文档 §10.3）。
 *
 * 这里最要紧的一条是**行数守恒**：直方图是范围选择率的唯一输入，而选择率
 * 直接乘进行数估算、再乘成对象存储请求数（R1）。桶计数一旦在取整时漏掉行，
 * 估算就会系统性偏低，优化器会因此错误地选择索引路径——那比选扫描糟得多。
 */
class HistogramTest {

    @Test
    fun `等宽直方图把均匀分布摊平`() {
        val values = (0 until 100).associate { it.toDouble() to 1L }
        val h = HistogramBuilder.fromWeightedValues(0.0, 99.0, values, buckets = 10)
        assertEquals(100L, h.totalCount)
        // 每个桶约 10 行，且桶宽度相等（等宽直方图的定义，不是「桶数对就行」）
        assertTrue(h.buckets.size in 9..11, "桶数应接近 10，实际 ${h.buckets.size}")
        val widths = h.buckets.map { it.hi - it.lo }
        val w0 = widths.first()
        assertTrue(
            widths.all { kotlin.math.abs(it - w0) < 0.05 },
            "所有桶的宽度必须一致（等宽），实际 $widths",
        )
    }

    @Test
    fun `桶计数之和精确等于总行数`() {
        // 43 行 / 7 个桶 = 6.14 行每桶，取整必然有误差——余数必须被塞进某个桶，
        // 不能凭空消失
        val values = (1..43).associate { it.toDouble() to 1L }
        val h = HistogramBuilder.fromWeightedValues(1.0, 43.0, values, buckets = 7)
        assertEquals(43L, h.buckets.sumOf { it.count }, "桶计数之和必须等于总行数")
        assertEquals(43L, h.totalCount)
    }

    @Test
    fun `块级区间分摊也守恒`() {
        // 每个块 [10k, 10k+99]，100 行；10 个块 → 1000 行
        val blocks = (0 until 10).map { Triple(it * 100.0, it * 100.0 + 99.0, 100L) }
        val h = HistogramBuilder.fromBlockRanges(0.0, 999.0, blocks, buckets = 16)
        assertEquals(1000L, h.totalCount)
        assertEquals(1000L, h.buckets.sumOf { it.count })
    }

    @Test
    fun `范围选择率与真实比例一致`() {
        // 0..999 均匀分布 1000 行 → [200,399] 应为 20%
        val values = (0 until 1000).associate { it.toDouble() to 1L }
        val h = HistogramBuilder.fromWeightedValues(0.0, 999.0, values, buckets = 32)
        val sel = h.selectivity(200.0, 399.0)
        assertTrue(
            sel in 0.17..0.23,
            "区间 [200,399] 应有约 20% 的行，实际 ${"%.3f".format(sel)}",
        )
        assertEquals(1.0, h.selectivity(null, null), 0.001)
        assertEquals(0L, h.countIn(2000.0, 3000.0), "域外区间必须给出 0 行")
    }

    @Test
    fun `偏斜分布能被直方图看出来 - 这是它相对 min-max 的全部价值`() {
        // 99% 的行挤在 0..9，剩下的散在 10..1000。
        // 只有 min/max 的话 `x > 500` 会被估成 ~50%；直方图应当给出个位数百分比。
        val values = HashMap<Double, Long>()
        for (i in 0..9) values[i.toDouble()] = 99L
        for (i in 10..1000 step 100) values[i.toDouble()] = 1L
        val h = HistogramBuilder.fromWeightedValues(0.0, 1000.0, values, buckets = 32)

        val sel = h.selectivity(500.0, null)
        assertTrue(sel < 0.10, "偏斜分布下 x>500 的选择率应远低于均匀假设的 50%，实际 ${"%.3f".format(sel)}")

        // 对照：没有直方图时，ColumnStats 只能按全域均匀估
        val noHist = ColumnStats(
            name = "x", ordinal = 0, type = "BIGINT", observedRows = 1000, nullCount = 0,
            minNum = 0.0, maxNum = 1000.0,
        )
        assertTrue(
            noHist.rangeSelectivity(500.0, null) > 0.4,
            "没有直方图时只能按均匀假设估——这条对照说明直方图确实带来了新信息",
        )
    }

    @Test
    fun `单点域不产生除零`() {
        val values = mapOf(7.0 to 5L)
        val h = HistogramBuilder.fromWeightedValues(7.0, 7.0, values, buckets = 8)
        assertEquals(5L, h.totalCount)
        assertEquals(5L, h.countIn(7.0, 7.0))
        assertEquals(0L, h.countIn(8.0, 9.0))
        assertEquals(1.0, h.selectivity(7.0, 7.0), 0.001)
    }

    @Test
    fun `空输入得到空直方图而不是抛异常`() {
        assertEquals(0, HistogramBuilder.fromWeightedValues(0.0, 1.0, emptyMap()).bucketCount)
        assertEquals(0, HistogramBuilder.fromBlockRanges(0.0, 1.0, emptyList()).bucketCount)
        assertEquals(0.0, Histogram().selectivity(null, null), 0.001)
    }

    @Test
    fun `列统计的选择率回退链`() {
        // 1) ndv 已知 → 1/ndv
        val withNdv = ColumnStats(
            name = "status", ordinal = 0, type = "VARCHAR", observedRows = 1000,
            nullCount = 0, ndv = 4, ndvExact = true, ndvSource = NdvSource.VALUES,
        )
        assertEquals(0.25, withNdv.equalitySelectivity(), 0.0001)

        // 2) ndv 未知但是数值列 → 用域宽上界（至少比默认值有依据）
        val numeric = ColumnStats(
            name = "n", ordinal = 1, type = "INT", observedRows = 1000, nullCount = 0,
            minNum = 0.0, maxNum = 999.0, ndv = ColumnStats.NDV_UNKNOWN,
        )
        assertEquals(0.001, numeric.equalitySelectivity(), 0.0001)

        // 3) 什么都不知道 → 保守的默认值（估高比估低安全：宁可选扫描）
        val blind = ColumnStats(
            name = "s", ordinal = 2, type = "VARCHAR", observedRows = 1000, nullCount = 0,
            ndv = ColumnStats.NDV_UNKNOWN,
        )
        assertEquals(ColumnStats.DEFAULT_EQ_SELECTIVITY, blind.equalitySelectivity(), 0.0001)
        assertEquals(ColumnStats.DEFAULT_RANGE_SELECTIVITY, blind.rangeSelectivity(1.0, 2.0), 0.0001)
    }

    /**
     * **落盘形态**（原则 P3）。统计对象是每一次查询规划都要读进内存的那一个，
     * 所以它的字节数就是规划路径的成本。
     *
     * 桶边界（`[lo, hi)`）在等宽直方图里完全由 `min`/`max`/`bucketCount` 推导，
     * 逐桶写下来是纯冗余——实测一张 20 列的表，去掉这份冗余让表级统计对象从
     * 113 KB 降到 46 KB。这条测试就是那个结论的守卫：它同时钉住「不写边界」
     * 与「只写非空桶」，并证明去掉之后选择率**一字不差**。
     */
    @Test
    fun `落盘形态不写桶边界 - 且往返后选择率完全一致`() {
        // 1000 行挤在 0..9，100 行在 999——32 个桶里只有两个非空
        val values = HashMap<Double, Long>()
        for (i in 0..9) values[i.toDouble()] = 100L
        values[999.0] = 100L
        val h = HistogramBuilder.fromWeightedValues(0.0, 999.0, values, buckets = 32)

        val json = kotlinx.serialization.json.Json { prettyPrint = true; encodeDefaults = true }
        val text = json.encodeToString(Histogram.serializer(), h)
        assertTrue(!text.contains("\"lo\""), "桶边界必须可推导，不能落盘：\n$text")
        assertTrue(!text.contains("\"hi\""), "桶边界必须可推导，不能落盘：\n$text")
        assertTrue(h.counts.size % 2 == 0, "扁平序列必须是 [下标, 计数] 对：${h.counts}")
        assertEquals(h.buckets.size * 2, h.counts.size, "只写非空桶")
        assertTrue(h.buckets.size < 32, "这份数据有空桶，非空桶数应当小于桶总数，实际 ${h.buckets.size}")

        val back = json.decodeFromString(Histogram.serializer(), text)
        assertEquals(h, back, "往返必须无损")
        assertEquals(h.buckets.map { it.lo }, back.buckets.map { it.lo }, "推导出的桶下界必须一致")
        listOf(null to 500.0, 900.0 to null, 100.0 to 200.0, null to null).forEach { (lo, hi) ->
            assertEquals(
                h.selectivity(lo, hi), back.selectivity(lo, hi), 1e-12,
                "选择率在往返前后必须一致（[$lo, $hi]）",
            )
        }
    }

    @Test
    fun `NULL 比例来自块级 nullCount`() {
        val c = ColumnStats(
            name = "a", ordinal = 0, type = "INT", observedRows = 1000, nullCount = 250,
        )
        assertEquals(0.25, c.nullFraction, 0.0001)
        assertEquals(0.0, c.copy(observedRows = 0, nullCount = 0).nullFraction, 0.0001)
    }
}
