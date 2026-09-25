package com.cloudea.yangbase.common

import java.io.ByteArrayOutputStream

/**
 * 把一行中的若干列编码成**保序的**主键字节串。
 *
 * 这是 LSM 排序与范围扫描的正确性基础（设计文档 §8.3「索引键的编码」）。
 * 每一个分量都用 [OrderPreservingCodec]，因此 `byteCompare(a,b)` 的结果
 * 与 SQL 语义下的元组比较一致。
 */
object KeyEncoder {

    /** 单值编码，保序。 */
    fun encodeDatum(d: Datum, type: ColumnType): ByteArray = when (d) {
        is Datum.Null -> NULL_MARKER
        is Datum.I64 -> OrderPreservingCodec.encodeLong(d.v)
        is Datum.Bool -> OrderPreservingCodec.encodeLong(if (d.v) 1L else 0L)
        is Datum.Ts -> OrderPreservingCodec.encodeLong(d.millis)
        is Datum.Dec -> OrderPreservingCodec.encodeLong(d.unscaled)
        is Datum.F64 -> encodeDoubleOrdered(d.v)
        is Datum.Str -> if (type == ColumnType.TEXT || type == ColumnType.VARCHAR || type == ColumnType.BLOB) {
            OrderPreservingCodec.encodeBytes(d.v.toByteArray(Charsets.UTF_8))
        } else {
            // 字符串列收到非字符串值时按数值编码，保证同一列的类型一致
            OrderPreservingCodec.encodeBytes(d.asString().toByteArray(Charsets.UTF_8))
        }
        is Datum.Bin -> OrderPreservingCodec.encodeBytes(d.v)
    }

    /**
     * IEEE754 双精度的保序编码：翻转符号位（正数）或全部取反（负数）。
     * 这样字节序与浮点数的数值序一致，包括 -0.0 < 0.0 与 NaN 的边界处理。
     */
    fun encodeDoubleOrdered(v: Double): ByteArray {
        var bits = java.lang.Double.doubleToLongBits(v)
        bits = if (bits < 0) bits.inv() else bits xor Long.MIN_VALUE
        val out = ByteArray(8)
        for (i in 0 until 8) out[i] = ((bits ushr (56 - i * 8)) and 0xFF).toByte()
        return out
    }

    fun decodeDoubleOrdered(bytes: ByteArray, offset: Int = 0): Double {
        var bits = 0L
        for (i in 0 until 8) bits = (bits shl 8) or (bytes[offset + i].toLong() and 0xFF)
        bits = if (bits < 0) bits xor Long.MIN_VALUE else bits.inv()
        return java.lang.Double.longBitsToDouble(bits)
    }

    /** NULL 的编码必须小于任何非 NULL 值（与 MySQL 的 NULL 排序一致）。 */
    private val NULL_MARKER = byteArrayOf(0)

    /** 主键（或索引键）的完整编码。 */
    fun encodeKey(row: List<Datum>, schema: TableSchema, columnIndexes: List<Int>): ByteArray {
        val out = ByteArrayOutputStream(32)
        for (idx in columnIndexes) {
            out.write(encodeDatum(row[idx], schema.columns[idx].type))
        }
        return out.toByteArray()
    }

    /** 解码主键分量，返回值的个数（用于调试与部分键比较）。 */
    fun decodeComponents(key: ByteArray, schema: TableSchema, columnIndexes: List<Int>): List<Datum> {
        var p = 0
        val out = ArrayList<Datum>(columnIndexes.size)
        for (idx in columnIndexes) {
            val type = schema.columns[idx].type
            if (p < key.size && key[p] == 0.toByte() && type.let { !it.isString }) {
                out.add(Datum.Null); p++; continue
            }
            when {
                type.isInteger || type == ColumnType.BOOLEAN ||
                    type == ColumnType.DATETIME || type == ColumnType.TIMESTAMP || type == ColumnType.DATE -> {
                    val v = OrderPreservingCodec.decodeLong(key, p); p += 8
                    out.add(if (type == ColumnType.BOOLEAN) Datum.Bool(v != 0L) else Datum.I64(v))
                }
                type == ColumnType.DECIMAL -> {
                    val v = OrderPreservingCodec.decodeLong(key, p); p += 8
                    out.add(Datum.Dec(v, schema.columns[idx].scale ?: 0))
                }
                type == ColumnType.FLOAT || type == ColumnType.DOUBLE -> {
                    out.add(Datum.F64(decodeDoubleOrdered(key, p))); p += 8
                }
                else -> {
                    val (b, n) = OrderPreservingCodec.decodeBytes(key, p); p += n
                    out.add(Datum.Str(String(b, Charsets.UTF_8)))
                }
            }
        }
        return out
    }

    /** 主键前缀比较：key 是否以 prefix 的分量开头。 */
    fun hasPrefix(key: ByteArray, prefix: ByteArray): Boolean =
        key.size >= prefix.size &&
            ByteArrayComparator.compare(key, 0, prefix.size, prefix, 0, prefix.size) == 0
}
