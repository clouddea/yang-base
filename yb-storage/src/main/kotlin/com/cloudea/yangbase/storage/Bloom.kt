package com.cloudea.yangbase.storage

import kotlinx.serialization.Serializable
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.min

/**
 * 布隆过滤器。
 *
 * 关键性质：**只有假阳性，没有假阴性**——这正是它能安全用于对象级剪枝的原因
 * （设计文档 §8.5）。说「一定不存在」就是真的不存在；说「可能存在」需要回源确认。
 *
 * 用于两处：
 *  - 文件级主键 bloom（点查路由）
 *  - 块级列 bloom（跳数索引，`WHERE col = v` 剪枝）
 */
@Serializable
data class BloomData(
    val bits: ByteArray,
    val numHashes: Int,
    val numKeys: Int,
) {
    fun mightContain(key: ByteArray): Boolean {
        if (bits.isEmpty() || numHashes == 0) return true // 无 bloom 时保守返回「可能存在」
        val h = XxHash64.hash(key, SEED)
        val h2 = XxHash64.hash(key, h)
        val numBits = bits.size.toLong() * 8
        for (i in 0 until numHashes) {
            // Kirsch-Mitzenmacher 双哈希：用两个独立哈希构造 k 个哈希
            val pos = ((h + i.toLong() * h2) % numBits + numBits) % numBits
            val byteIdx = (pos ushr 3).toInt()
            val bit = (1 shl (pos and 7L).toInt())
            if ((bits[byteIdx].toInt() and bit) == 0) return false
        }
        return true
    }

    companion object {
        private val SEED = -0x61c8864680b583ebL // 0x9E3779B97F4A7C15

        /**
         * @param bitsPerKey 每个 key 分配的位数。10 位约 1% 假阳性率；
         *                   12 位约 0.3%（`.idx` 的体积代价仍很小）
         */
        fun build(keys: Collection<ByteArray>, bitsPerKey: Int = 10): BloomData {
            if (keys.isEmpty()) return BloomData(ByteArray(0), 0, 0)
            val n = keys.size
            val requestedBits = max(64L, n.toLong() * bitsPerKey)
            val numBytes = ((requestedBits + 7) / 8).toInt()
            // ⚠️ 必须用「实际分配出的位数」取模，而不是【请求的位数】。
            // 查询侧只能看到 bits.size，会用 bits.size*8 取模；两侧模数不一致
            // 会产生**假阴性**——这会让存在的行静默读不到，直接违反布隆过滤器
            // 「只有假阳性」的前提。（实现期间踩到的真实缺陷：500 vs 504。）
            val numBits = numBytes.toLong() * 8
            val bits = ByteArray(numBytes)

            // 最优哈希个数：k = (m/n) * ln2
            val k = max(1, min(30, (bitsPerKey * 0.6931472).toInt()))

            for (key in keys) {
                val h = XxHash64.hash(key, SEED)
                val h2 = XxHash64.hash(key, h)
                for (i in 0 until k) {
                    val pos = ((h + i.toLong() * h2) % numBits + numBits) % numBits
                    val byteIdx = (pos ushr 3).toInt()
                    bits[byteIdx] = (bits[byteIdx].toInt() or (1 shl (pos and 7L).toInt())).toByte()
                }
            }
            return BloomData(bits, k, n)
        }

        /** 理论假阳性率（用于选择 bitsPerKey）。 */
        fun falsePositiveRate(numBits: Long, numKeys: Int, k: Int): Double {
            if (numKeys == 0) return 0.0
            return Math.pow(1.0 - Math.exp(-k.toDouble() * numKeys / numBits), k.toDouble())
        }

        fun optimalBitsPerKey(targetFp: Double): Int {
            // m/n = -ln(p) / ln(2)^2
            return max(4, (ln(targetFp) / (ln(2.0) * ln(2.0)) * -1).toInt())
        }
    }
}

/**
 * xxHash64。选它是因为：实现短、速度快、雪崩性好，
 * 且不依赖任何外部库（避免为一个小哈希引入依赖）。
 */
object XxHash64 {
    private const val P1 = -7046029288634856825L // 0x9E3779B185EBCA87
    private const val P2 = -4417276706812531889L // 0xC2B2AE3D27D4EB4F
    private const val P3 = 1609587929392839161L  // 0x165667B19E3779F9
    private const val P4 = -8796714831421723037L // 0x85EBCA77C2B2AE63
    private const val P5 = 2870177450012600261L  // 0x27D4EB2F165667C5

    fun hash(data: ByteArray, seed: Long): Long {
        var h: Long
        var i = 0
        val len = data.size

        if (len >= 32) {
            var v1 = seed + P1 + P2
            var v2 = seed + P2
            var v3 = seed
            var v4 = seed - P1
            while (i + 32 <= len) {
                v1 = round(v1, readLong(data, i)); i += 8
                v2 = round(v2, readLong(data, i)); i += 8
                v3 = round(v3, readLong(data, i)); i += 8
                v4 = round(v4, readLong(data, i)); i += 8
            }
            h = java.lang.Long.rotateLeft(v1, 1) + java.lang.Long.rotateLeft(v2, 7) +
                java.lang.Long.rotateLeft(v3, 12) + java.lang.Long.rotateLeft(v4, 18)
            h = mergeRound(h, v1); h = mergeRound(h, v2)
            h = mergeRound(h, v3); h = mergeRound(h, v4)
        } else {
            h = seed + P5
        }

        h += len.toLong()

        while (i + 8 <= len) {
            h = (h xor round(0, readLong(data, i))) * P1 + P4
            h = java.lang.Long.rotateLeft(h, 27) * P1 + P4
            i += 8
        }
        if (i + 4 <= len) {
            h = (h xor (readInt(data, i).toLong() and 0xFFFFFFFFL) * P1)
            h = java.lang.Long.rotateLeft(h, 23) * P2 + P3
            i += 4
        }
        while (i < len) {
            h = (h xor (data[i].toLong() and 0xFF) * P5)
            h = java.lang.Long.rotateLeft(h, 11) * P1
            i++
        }

        h = h xor (h ushr 33)
        h *= P2
        h = h xor (h ushr 29)
        h *= P3
        h = h xor (h ushr 32)
        return h
    }

    private fun round(acc: Long, input: Long): Long {
        var a = acc + input * P2
        a = java.lang.Long.rotateLeft(a, 31)
        return a * P1
    }

    private fun mergeRound(acc: Long, v: Long): Long {
        var a = acc xor round(0, v)
        a = a * P1 + P4
        return a
    }

    private fun readLong(b: ByteArray, i: Int): Long {
        var v = 0L
        for (k in 0 until 8) v = (v shl 8) or (b[i + k].toLong() and 0xFF)
        return v
    }

    private fun readInt(b: ByteArray, i: Int): Int {
        var v = 0
        for (k in 0 until 4) v = (v shl 8) or (b[i + k].toInt() and 0xFF)
        return v
    }
}
