package com.cloudea.yangbase.common.stats

import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.OrderPreservingCodec
import com.cloudea.yangbase.common.TableSchema

/**
 * `.idx` 里存的列值是**保序编码后的字节**（`KeyEncoder.encodeDatum`），
 * 而统计对象走 kotlinx-serialization 的**可读 JSON**（§10.3 的取舍：元数据可读性优先）。
 * 这个对象是两者之间唯一的转换处。
 *
 * 刻意**不走** `KeyEncoder.decodeComponents`：那个函数把「首字节为 0」当作 NULL
 * 标记，对「整条主键的第一个分量」是合理的启发式，但用在单个列值上会误判——
 * `Long.MIN_VALUE` 的保序编码恰好是 8 个 0 字节。这里的解码按列类型逐一进行，
 * 没有这个歧义。
 */
object StatsCodec {

    /** 渲染文本形式的上限（统计对象的体积要可控，P3）。 */
    const val MAX_TEXT = 128

    /** 解码单个列值并渲染成文本；无法解码时返回 null（例如空块、NULL 标记）。 */
    fun render(encoded: ByteArray?, schema: TableSchema, columnIndex: Int): String? {
        val d = datumOf(encoded, schema, columnIndex) ?: return null
        if (d is Datum.Null) return null
        val s = d.asString()
        return if (s.length > MAX_TEXT) s.substring(0, MAX_TEXT) + "…" else s
    }

    /**
     * 数值形式（用于直方图与范围估算）。
     *
     * `DECIMAL` 用未缩放整数、`DATETIME` 用毫秒——都不再乘/除回原比例，
     * 因为直方图只关心**序关系与相对宽度**，用单调的整数刻度反而更精确。
     * 字符串列返回 null（不做数值猜测，§10.3 的直方图本来也只针对数值列）。
     */
    fun numericOf(encoded: ByteArray?, schema: TableSchema, columnIndex: Int): Double? {
        return when (val d = datumOf(encoded, schema, columnIndex)) {
            is Datum.I64 -> d.v.toDouble()
            is Datum.F64 -> d.v
            is Datum.Bool -> if (d.v) 1.0 else 0.0
            is Datum.Ts -> d.millis.toDouble()
            is Datum.Dec -> d.unscaled.toDouble()
            else -> null
        }
    }

    /** 按列类型解码。返回 null 表示字节不足以解码（旧文件、空统计）。 */
    fun datumOf(encoded: ByteArray?, schema: TableSchema, columnIndex: Int): Datum? {
        if (encoded == null) return null
        val col = schema.columns.getOrNull(columnIndex) ?: return null
        val type = col.type
        return when {
            type.isInteger || type == ColumnType.BOOLEAN ||
                type == ColumnType.DATETIME || type == ColumnType.TIMESTAMP || type == ColumnType.DATE -> {
                if (encoded.size < 8) return null
                val v = OrderPreservingCodec.decodeLong(encoded)
                if (type == ColumnType.BOOLEAN) Datum.Bool(v != 0L) else Datum.I64(v)
            }
            type == ColumnType.DECIMAL -> {
                if (encoded.size < 8) return null
                Datum.Dec(OrderPreservingCodec.decodeLong(encoded), col.scale ?: 0)
            }
            type == ColumnType.FLOAT || type == ColumnType.DOUBLE -> {
                if (encoded.size < 8) return null
                Datum.F64(KeyEncoder.decodeDoubleOrdered(encoded))
            }
            type == ColumnType.BLOB -> Datum.Bin(OrderPreservingCodec.decodeBytes(encoded).first)
            else -> Datum.Str(String(OrderPreservingCodec.decodeBytes(encoded).first, Charsets.UTF_8))
        }
    }
}
