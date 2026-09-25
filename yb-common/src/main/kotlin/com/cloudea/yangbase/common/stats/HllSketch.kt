package com.cloudea.yangbase.common.stats

/**
 * HyperLogLog 基数估计草图（设计文档 §10.3：`ndv` **允许是估计值**）。
 *
 * 为什么需要它：`.idx` 里的块级 `valueCounts` **只对低基数列收集**（见
 * [com.cloudea.yangbase.storage.ColumnStat]），而且一旦块内不同值超过
 * `maxDistinctPerBlock`（256）就会被放弃。也就是说 `ANALYZE` 能拿到的
 * 「精确不同值」天然只覆盖低基数列；高基数列必须靠草图估计，否则只能
 * 报 `ndv = 未知`，代价模型就失去最关键的等值选择率输入。
 *
 * 参数取舍：
 *  - 精度 `p = 10` → `m = 1024` 个寄存器 → 标准误差 `1.04/√m ≈ 3.25%`；
 *    对选择率估算完全够用（代价模型要的是「1 万 vs 100 万」这种量级判断）。
 *  - 内存/体积 `m` 字节 = 1 KB/列，且**只在收集期存在**：
 *    [com.cloudea.yangbase.txn.TableStatsStore] 落盘的是估计出的数字，不是草图
 *    （原则 P3：元数据要小。草图能省下的唯一一件事是「不重扫就能增量合并」，
 *    而当前没有增量 ANALYZE，所以这笔体积不值得付）。
 *
 * 这里**不是**精确算法：它给出的是有 3.25% 标准误差的估计。凡是需要精确值的
 * 场合（例如 §8.9 的计数）都必须走元数据计数，绝不能拿这个数字当答案。
 */
class HllSketch private constructor(
    /** 精度 p，寄存器数 = `1 shl p`。 */
    val precision: Int,
    private val registers: ByteArray,
) {

    /** 寄存器数的倒数常量，估计时反复用到。 */
    private val m: Int get() = 1 shl precision

    /** 加入一个已经算好的 64 位哈希。 */
    fun addHash(hash: Long) {
        val idx = (hash ushr (64 - precision)).toInt()          // 高 p 位选桶
        val w = hash shl precision                               // 剩余位用来数前导零
        val rho = if (w == 0L) (64 - precision + 1) else (java.lang.Long.numberOfLeadingZeros(w) + 1)
        val v = if (rho > 63) 63 else rho                        // 6 bit 足够（64-p+1 ≤ 61）
        if (v > registers[idx]) registers[idx] = v.toByte()
    }

    /** 加入一个值（原样字节，例如保序编码后的键或列值）。 */
    fun add(value: ByteArray) = addHash(hash64(value))

    /** 基数估计（含小基数修正）。空草图为 0。 */
    fun estimate(): Long {
        val m = this.m
        var sum = 0.0
        var zeros = 0
        for (r in registers) {
            val v = r.toInt()
            if (v == 0) zeros++
            sum += 1.0 / (1L shl v)
        }
        val alpha = when (m) {
            16 -> 0.673
            32 -> 0.697
            64 -> 0.709
            else -> 0.7213 / (1.0 + 1.079 / m)
        }
        var est = alpha * m * m / sum
        // 小基数修正：估计值不大且仍有空寄存器时，线性计数（Linear Counting）准得多。
        // 这是 HLL 的标准做法，也是「小表也要算得准」的关键——统计恰恰常常采集在
        // 空表或小表上，没有这一步会出现 ndv=1 而真实值 0 这种荒唐结果。
        if (est <= 2.5 * m && zeros > 0) {
            est = m * Math.log(m.toDouble() / zeros)
        }
        return Math.round(est).coerceAtLeast(if (zeros == m) 0L else 1L)
    }

    /** 是否一个值都没加过。 */
    fun isEmpty(): Boolean = registers.all { it == 0.toByte() }

    /** 寄存器副本（落盘/调试用，调用方拥有所有权）。 */
    fun toBytes(): ByteArray = registers.copyOf()

    /** 寄存器数（用于估算体积）。 */
    val byteSize: Int get() = registers.size

    /**
     * 并入另一个草图（并集 = 逐寄存器取最大值）。
     *
     * 这正是「分区级 ndv 合并成表级 ndv」要用的操作：同一个值可能出现在多个分区，
     * 直接把 ndv 相加会系统性高估，而 HLL 的逐寄存器 max 恰好给出并集的估计。
     */
    fun merge(other: HllSketch) {
        require(other.precision == precision) {
            "HllSketch 精度不一致：$precision vs ${other.precision}"
        }
        for (i in registers.indices) {
            val o = other.registers[i]
            if (o > registers[i]) registers[i] = o
        }
    }

    /** 并集副本，不变更自身。 */
    fun mergedWith(other: HllSketch): HllSketch = copy().also { it.merge(other) }

    fun copy(): HllSketch = HllSketch(precision, registers.copyOf())

    override fun toString(): String = "HllSketch(p=$precision, ndv≈${estimate()})"

    companion object {
        /** 默认精度：3.25% 标准误差。 */
        const val DEFAULT_PRECISION = 10

        private const val MIN_PRECISION = 4
        private const val MAX_PRECISION = 16

        fun of(precision: Int = DEFAULT_PRECISION): HllSketch {
            require(precision in MIN_PRECISION..MAX_PRECISION) {
                "HLL 精度必须在 $MIN_PRECISION..$MAX_PRECISION 之间：$precision"
            }
            return HllSketch(precision, ByteArray(1 shl precision))
        }

        /** 从 [toBytes] 的寄存器副本还原。 */
        fun fromBytes(precision: Int, registers: ByteArray): HllSketch {
            require(precision in MIN_PRECISION..MAX_PRECISION) { "HLL 精度非法：$precision" }
            require(registers.size == (1 shl precision)) {
                "HLL 寄存器数 ${registers.size} 与精度 $precision 不符"
            }
            return HllSketch(precision, registers.copyOf())
        }

        /**
         * 稳定且均匀的 64 位哈希（MurmurHash3 的 fmix64 尾部混合，按 8 字节分组）。
         *
         * 这里不追求密码学强度，只要求**同样输入永远给同样结果**（跨进程、跨机器），
         * 否则统计对象在不同 CN 上算出来的 ndv 会漂移，EXPLAIN 的结果就不可复现了。
         */
        fun hash64(value: ByteArray): Long {
            var h = 0xcbf29ce484222325uL.toLong()                      // FNV 偏移基址
            var i = 0
            while (i + 8 <= value.size) {
                var k = 0L
                for (j in 0 until 8) k = (k shl 8) or (value[i + j].toLong() and 0xFF)
                h = h xor mix64(k)
                h = java.lang.Long.rotateLeft(h, 27) * 5 + 0x52dce729L
                i += 8
            }
            var tail = 0L
            var shift = 0
            while (i < value.size) {
                tail = tail or ((value[i].toLong() and 0xFF) shl shift)
                shift += 8
                i++
            }
            h = h xor mix64(tail)
            h = h xor value.size.toLong()
            return mix64(h)
        }

        private fun mix64(x0: Long): Long {
            var x = x0
            x = x xor (x ushr 33)
            x *= 0xff51afd7ed558ccduL.toLong()
            x = x xor (x ushr 33)
            x *= 0xc4ceb9fe1a85ec53uL.toLong()
            x = x xor (x ushr 33)
            return x
        }
    }
}
