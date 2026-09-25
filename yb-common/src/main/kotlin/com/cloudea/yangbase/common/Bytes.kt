package com.cloudea.yangbase.common

/**
 * 保序字节编码。
 *
 * LSM 的正确性完全依赖「**字节序 == 逻辑序**」这条不变式。所有进入 key 的分量
 * 都必须经过这里的编码，否则范围扫描会静默漏行。
 *
 * 编码规则：
 *  - 有符号整数：大端 + 翻转符号位 ⇒ 负数排在正数之前（与 SQL 的数值序一致）
 *  - 字符串/字节串：UTF-8 原始字节 + 保序转义（`0x00` → `0x00 0xFF`，结尾 `0x00 0x00`）
 *
 * 转义方案的正确性：分隔符 `0x00 0x00` 永远小于任何「转义后的 `0x00`」
 * （即 `0x00 0xFF`），因此短串永远排在以它为前缀的长串之前，字典序得以保持。
 */
object OrderPreservingCodec {

    /** 编码有符号 Long，8 字节。 */
    fun encodeLong(value: Long): ByteArray {
        val out = ByteArray(8)
        // 翻转符号位后按大端写出：Long.MIN_VALUE -> 0x00.., Long.MAX_VALUE -> 0xFF..
        val v = value xor Long.MIN_VALUE
        for (i in 0 until 8) {
            out[i] = ((v ushr (56 - i * 8)) and 0xFF).toByte()
        }
        return out
    }

    /** 编码有符号 Int，4 字节。 */
    fun encodeInt(value: Int): ByteArray {
        val out = ByteArray(4)
        val v = value xor Int.MIN_VALUE
        for (i in 0 until 4) {
            out[i] = ((v ushr (24 - i * 8)) and 0xFF).toByte()
        }
        return out
    }

    /** 编码 Short，2 字节。 */
    fun encodeShort(value: Short): ByteArray {
        val out = ByteArray(2)
        val v = value.toInt() xor 0x8000
        out[0] = ((v ushr 8) and 0xFF).toByte()
        out[1] = (v and 0xFF).toByte()
        return out
    }

    fun decodeLong(bytes: ByteArray, offset: Int = 0): Long {
        var v = 0L
        for (i in 0 until 8) v = (v shl 8) or (bytes[offset + i].toLong() and 0xFF)
        return v xor Long.MIN_VALUE
    }

    fun decodeInt(bytes: ByteArray, offset: Int = 0): Int {
        var v = 0
        for (i in 0 until 4) v = (v shl 8) or (bytes[offset + i].toInt() and 0xFF)
        return v xor Int.MIN_VALUE
    }

    fun decodeShort(bytes: ByteArray, offset: Int = 0): Short {
        val v = ((bytes[offset].toInt() and 0xFF) shl 8) or (bytes[offset + 1].toInt() and 0xFF)
        return (v xor 0x8000).toShort()
    }

    /** 编码字节串，带保序转义。结果以 `0x00 0x00` 结尾。 */
    fun encodeBytes(raw: ByteArray): ByteArray {
        // 最坏情况：每个 0x00 膨胀成 2 字节，再加 2 字节终结符
        var zeros = 0
        for (b in raw) if (b == 0.toByte()) zeros++
        val out = ByteArray(raw.size + zeros + 2)
        var p = 0
        for (b in raw) {
            out[p++] = b
            if (b == 0.toByte()) out[p++] = 0xFF.toByte()
        }
        out[p++] = 0
        out[p] = 0
        return out
    }

    fun encodeString(s: String): ByteArray = encodeBytes(s.toByteArray(Charsets.UTF_8))

    /**
     * 解出转义字节串，返回 (原始字节, 消耗的字节数)。
     * 输入必须是以 `0x00 0x00` 终结的合法编码。
     */
    fun decodeBytes(buf: ByteArray, offset: Int = 0): Pair<ByteArray, Int> {
        val out = java.io.ByteArrayOutputStream()
        var p = offset
        while (true) {
            if (p >= buf.size) throw CorruptDataException("保序字节串缺少终结符")
            val b = buf[p++]
            if (b != 0.toByte()) {
                out.write(b.toInt())
            } else {
                if (p >= buf.size) throw CorruptDataException("保序字节串在转义中途截断")
                val next = buf[p++]
                when (next) {
                    0xFF.toByte() -> out.write(0)
                    0.toByte() -> return out.toByteArray() to (p - offset)
                    else -> throw CorruptDataException("非法的保序转义序列: 0x00 0x%02x".format(next))
                }
            }
        }
    }

    fun decodeString(buf: ByteArray, offset: Int = 0): Pair<String, Int> {
        val (bytes, n) = decodeBytes(buf, offset)
        return String(bytes, Charsets.UTF_8) to n
    }
}

/** 无符号字典序比较。LSM 的所有有序结构都用它。 */
object ByteArrayComparator : Comparator<ByteArray> {
    override fun compare(a: ByteArray, b: ByteArray): Int = compare(a, 0, a.size, b, 0, b.size)

    fun compare(a: ByteArray, aOff: Int, aLen: Int, b: ByteArray, bOff: Int, bLen: Int): Int {
        val n = minOf(aLen, bLen)
        for (i in 0 until n) {
            val x = a[aOff + i].toInt() and 0xFF
            val y = b[bOff + i].toInt() and 0xFF
            if (x != y) return x - y
        }
        return aLen - bLen
    }
}

/** 可比较的字节数组包装，用于 ConcurrentSkipListMap 与 Map 的 key。 */
class Bytes(val data: ByteArray) : Comparable<Bytes> {
    override fun compareTo(other: Bytes): Int = ByteArrayComparator.compare(data, other.data)
    override fun equals(other: Any?): Boolean = other is Bytes && data.contentEquals(other.data)
    override fun hashCode(): Int = data.contentHashCode()
    override fun toString(): String = "Bytes(${data.size}B)"
    fun size(): Int = data.size
}

/** 定长/Leb128 变长整数的读写。用于 SST/WAL 的热路径格式。 */
object Varint {
    /** 无符号 LEB128 编码 Int，写入 [out]。 */
    fun writeUnsigned(out: java.io.ByteArrayOutputStream, value: Int) {
        require(value >= 0) { "无符号 varint 不接受负值: $value" }
        var v = value
        while (true) {
            if ((v and 0x7F.inv()) == 0) {
                out.write(v)
                return
            }
            out.write((v and 0x7F) or 0x80)
            v = v ushr 7
        }
    }

    /**
     * 无符号 LEB128 编码 Long。
     *
     * **刻意不拒绝负数**：zigzag 编码 `Long.MIN_VALUE` 得到的位模式恰好是
     * `0xFFFF...FF`，作为 64 位无符号数完全合法，但作为有符号 Long 就是 -1。
     * 若在这里拒绝，`Long.MIN_VALUE` 就无法被编码（实现期间踩到的真实缺陷）。
     */
    fun writeUnsigned(out: java.io.ByteArrayOutputStream, value: Long) {
        var v = value
        while (true) {
            if ((v and 0x7F.inv()) == 0L) {
                out.write(v.toInt())
                return
            }
            out.write(((v and 0x7F) or 0x80).toInt())
            v = v ushr 7
        }
    }

    fun readUnsigned(buf: ByteArray, offset: Int): Pair<Int, Int> {
        var result = 0
        var shift = 0
        var p = offset
        while (true) {
            if (p >= buf.size) throw CorruptDataException("varint 截断")
            val b = buf[p++].toInt() and 0xFF
            result = result or ((b and 0x7F) shl shift)
            if ((b and 0x80) == 0) return result to (p - offset)
            shift += 7
            if (shift > 35) throw CorruptDataException("varint 过长")
        }
    }

    fun readUnsignedLong(buf: ByteArray, offset: Int): Pair<Long, Int> {
        var result = 0L
        var shift = 0
        var p = offset
        while (true) {
            if (p >= buf.size) throw CorruptDataException("varint 截断")
            val b = buf[p++].toInt() and 0xFF
            result = result or ((b and 0x7F).toLong() shl shift)
            if ((b and 0x80) == 0) return result to (p - offset)
            shift += 7
            if (shift > 70) throw CorruptDataException("varint 过长")
        }
    }

    fun sizeOf(value: Int): Int {
        var v = value
        var n = 1
        while (v >= 0x80) { v = v ushr 7; n++ }
        return n
    }
}

/** 定长大端读写（用于 SST/WAL 的固定字段）。 */
object BigEndian {
    fun writeLong(out: java.io.ByteArrayOutputStream, v: Long) {
        for (i in 7 downTo 0) out.write(((v ushr (i * 8)) and 0xFF).toInt())
    }

    fun writeInt(out: java.io.ByteArrayOutputStream, v: Int) {
        for (i in 3 downTo 0) out.write(((v ushr (i * 8)) and 0xFF).toInt())
    }

    fun writeShort(out: java.io.ByteArrayOutputStream, v: Int) {
        out.write((v ushr 8) and 0xFF)
        out.write(v and 0xFF)
    }

    fun readLong(buf: ByteArray, off: Int): Long {
        var v = 0L
        for (i in 0 until 8) v = (v shl 8) or (buf[off + i].toLong() and 0xFF)
        return v
    }

    fun readInt(buf: ByteArray, off: Int): Int {
        var v = 0
        for (i in 0 until 4) v = (v shl 8) or (buf[off + i].toInt() and 0xFF)
        return v
    }

    fun readShort(buf: ByteArray, off: Int): Int =
        ((buf[off].toInt() and 0xFF) shl 8) or (buf[off + 1].toInt() and 0xFF)
}

/** CRC32C 校验（JDK 9+ 内置）。 */
object Checksums {
    fun crc32c(data: ByteArray, offset: Int = 0, length: Int = data.size - offset): Int {
        val crc = java.util.zip.CRC32C()
        crc.update(data, offset, length)
        return crc.value.toInt()
    }

    fun crc32cHex(data: ByteArray): String = "%08x".format(crc32c(data))
}
