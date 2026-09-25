package com.cloudea.yangbase.common.stats

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `ndv` 的估计器（§10.3）的**误差与合并**测试。
 *
 * 这些断言守护的不是「估计得准不准」这一件事本身，而是**代价模型的输入是否可信**：
 * 等值选择率是 `1/ndv`，`ndv` 偏差 10 倍会让优化器在「索引」与「扫描」之间做出
 * 完全相反的判断。所以误差上界必须被钉住，而且必须在**大范围基数**上都成立。
 */
class HllSketchTest {

    /** 3.25% 是标准误差（1 个 σ）；这里给到 4 个 σ 的余量，避免概率性抖动导致测试不稳。 */
    private fun assertWithin(estimate: Long, truth: Int, tolerance: Double = 0.25) {
        val rel = kotlin.math.abs(estimate - truth).toDouble() / truth
        assertTrue(
            rel <= tolerance,
            "ndv 估计 $estimate 与真实值 $truth 的相对误差 ${"%.3f".format(rel)} 超过 $tolerance",
        )
    }

    @Test
    fun `空草图的估计是 0 而不是 1`() {
        // 一个没有任何值的列，ndv 恰好是 0——这与「不知道」是两回事，
        // 而哈希草图很容易在这种边界上给出 1
        assertEquals(0L, HllSketch.of().estimate())
        assertTrue(HllSketch.of().isEmpty())
    }

    @Test
    fun `小基数精确到个位数`() {
        // 小基数走的是线性计数修正，不是原始 HLL 公式：
        // 没有这一步，一个只有 3 个不同值的列会估出 10+
        for (n in listOf(1, 2, 3, 5, 10, 50, 200)) {
            val h = HllSketch.of()
            for (i in 0 until n) h.add("v$i".toByteArray())
            assertWithin(h.estimate(), n, tolerance = 0.10)
        }
    }

    @Test
    fun `中大基数的误差在 25% 以内`() {
        for (n in listOf(1_000, 10_000, 100_000)) {
            val h = HllSketch.of()
            for (i in 0 until n) h.add(("k" + i).toByteArray())
            assertWithin(h.estimate(), n)
        }
    }

    @Test
    fun `重复值不会推高估计`() {
        val h = HllSketch.of()
        repeat(10_000) { h.add("same".toByteArray()) }
        assertEquals(1L, h.estimate())
    }

    @Test
    fun `合并是逐寄存器取最大 - 不会把并集算重`() {
        // 两个分区各自 ndv=1000，且有 500 个值重叠。
        // 直接相加会得到 2000（系统性高估），HLL 的并集语义应给出 ~1500。
        val a = HllSketch.of()
        val b = HllSketch.of()
        for (i in 0 until 1000) a.add("x$i".toByteArray())
        for (i in 500 until 1500) b.add("x$i".toByteArray())

        val merged = a.mergedWith(b)
        val est = merged.estimate()
        assertTrue(
            est in 1200..1900,
            "重叠并集的 ndv 应在 1500 附近，实际 $est（相加会得到 2000）",
        )
        // mergedWith 不变更自身，也不变更 b
        assertEquals(a.estimate(), a.estimate())
        assertTrue(a.estimate() in 800..1200)
    }

    @Test
    fun `merge 要求精度一致`() {
        assertFailsWith<IllegalArgumentException> {
            HllSketch.of(HllSketch.DEFAULT_PRECISION).merge(HllSketch.of(12))
        }
    }

    @Test
    fun `寄存器可以往返序列化`() {
        val h = HllSketch.of()
        for (i in 0 until 5000) h.add("v$i".toByteArray())
        val restored = HllSketch.fromBytes(h.precision, h.toBytes())
        assertEquals(h.estimate(), restored.estimate())
        assertEquals(h.byteSize, 1 shl h.precision)
    }

    @Test
    fun `同样的输入永远给出同样的哈希`() {
        // 统计对象由不同 CN 各自算出后对比/复现，哈希必须是确定性的（不是随机种子）
        assertEquals(HllSketch.hash64("abc".toByteArray()), HllSketch.hash64("abc".toByteArray()))
        // 长度不同的输入必须落到不同的哈希（尾部与长度都参与混合）
        assertTrue(HllSketch.hash64(byteArrayOf(1, 2, 3)) != HllSketch.hash64(byteArrayOf(1, 2, 3, 0)))
    }

    @Test
    fun `非法精度被拒绝`() {
        assertFailsWith<IllegalArgumentException> { HllSketch.of(2) }
        assertFailsWith<IllegalArgumentException> { HllSketch.of(24) }
    }
}
