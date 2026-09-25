package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.IndexDef
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.OrderPreservingCodec
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.common.TableSchema

/**
 * 索引项的编解码。
 *
 * **索引项 key 的布局**：
 * ```
 * [索引列 1][索引列 2]...[主键列 1][主键列 2]...
 * ```
 * 主键放在末尾有两个作用：
 *  1. 同一个索引值下的多行在 LSM 中**相邻**，一次前缀扫描就能取全
 *  2. 索引项天然唯一（索引值 + 主键），不需要额外的去重逻辑
 *
 * 全部用保序编码，因此索引的**范围扫描语义与 SQL 一致**——
 * `WHERE age > 30` 在索引上就是一段连续区间。
 */
object IndexCodec {

    /**
     * 索引的存储分片。
     *
     * 用**负数 TableId** 把索引的键空间与表的键空间彻底分开。
     * 表的 id 恒为正（catalog 从 1000 开始分配），因此不会碰撞。
     */
    fun shardOf(index: IndexDef): ShardId = ShardId(TableId(-index.id), PartitionId(0))

    fun isIndexShard(shard: ShardId): Boolean = shard.table.value < 0

    fun indexIdOf(shard: ShardId): Long = -shard.table.value

    /** 索引列在表中的下标。 */
    fun columnIndexes(table: TableSchema, index: IndexDef): List<Int> =
        index.columns.map { table.indexOf(it) }

    /** 一条完整的索引项 key。 */
    fun entryKey(table: TableSchema, index: IndexDef, row: List<Datum>): ByteArray {
        val idxCols = columnIndexes(table, index)
        val valuePart = KeyEncoder.encodeKey(row, table, idxCols)
        val pkPart = KeyEncoder.encodeKey(row, table, table.primaryKeyIndexes)
        return valuePart + pkPart
    }

    /**
     * 索引值部分的前缀，用于按索引值扫描。
     *
     * ⚠️ 不能直接复用 `KeyEncoder.encodeKey`——它按**表列下标**索引整行，
     * 而这里拿到的是「只有索引列的值」。曾经因此越界
     * （`IndexOutOfBoundsException: Index 1 out of bounds for length 1`）。
     */
    fun valuePrefix(table: TableSchema, index: IndexDef, values: List<Datum>): ByteArray {
        val idxCols = columnIndexes(table, index)
        require(values.size <= idxCols.size) {
            "索引 ${index.name} 有 ${idxCols.size} 列，但传入了 ${values.size} 个值"
        }
        val out = java.io.ByteArrayOutputStream(32)
        for (i in values.indices) {
            out.write(KeyEncoder.encodeDatum(values[i], table.columns[idxCols[i]].type))
        }
        return out.toByteArray()
    }

    /**
     * 从索引项 key 里解出主键部分。
     *
     * 索引列部分是自描述且可定长的（保序编码），因此可以先跳过它，
     * 剩下的就是主键。
     */
    fun primaryKeyOf(table: TableSchema, index: IndexDef, entryKey: ByteArray): ByteArray {
        var p = 0
        for (colIdx in columnIndexes(table, index)) {
            p += encodedWidth(entryKey, p, table.columns[colIdx].type)
        }
        return entryKey.copyOfRange(p, entryKey.size)
    }

    /**
     * 从一个**值前缀**解出索引列的值。
     *
     * ⚠️ 只接受前缀，不接受完整索引项：从完整项里减掉主键需要知道主键的字节宽度，
     * 而变长主键（VARCHAR）的宽度是算不出来的——之前这里用 `widthOf` 得到 -1，
     * 于是 `copyOfRange(0, size+1)` 悄悄切错。传前缀就没有这个问题。
     *
     * ⚠️ 含 NULL 的前缀解不出来（字符串列的 NULL 编码 `0x00` 与保序转义的
     * 前缀冲突）。**需要判断 NULL 时请解码整行**，不要用这个。
     */
    fun valuesOfPrefix(table: TableSchema, index: IndexDef, valuePrefix: ByteArray): List<Datum> =
        KeyEncoder.decodeComponents(valuePrefix, table, columnIndexes(table, index))

    /** 一个保序编码的分量占多少字节。 */
    private fun encodedWidth(buf: ByteArray, offset: Int, type: com.cloudea.yangbase.common.ColumnType): Int {
        return when {
            type.isInteger || type == com.cloudea.yangbase.common.ColumnType.BOOLEAN ||
                type == com.cloudea.yangbase.common.ColumnType.DATETIME ||
                type == com.cloudea.yangbase.common.ColumnType.TIMESTAMP ||
                type == com.cloudea.yangbase.common.ColumnType.DATE ||
                type == com.cloudea.yangbase.common.ColumnType.DECIMAL -> 8
            type == com.cloudea.yangbase.common.ColumnType.FLOAT ||
                type == com.cloudea.yangbase.common.ColumnType.DOUBLE -> 8
            else -> OrderPreservingCodec.decodeBytes(buf, offset).second
        }
    }

    private fun widthOf(type: com.cloudea.yangbase.common.ColumnType): Int = when {
        type.isNumeric || type.isTemporal -> 8
        else -> -1 // 变长，无法静态计算
    }

    /**
     * 拆开一条索引项：返回 (索引值前缀, 主键)。
     *
     * 唯一约束的原子检查需要这两半：前缀用来找「同一个索引值的其他行」，
     * 主键用来判断找到的那行是不是自己。
     */
    fun splitEntry(table: TableSchema, index: IndexDef, entryKey: ByteArray): Pair<ByteArray, ByteArray> {
        val pk = primaryKeyOf(table, index, entryKey)
        val valueOnly = entryKey.copyOfRange(0, entryKey.size - pk.size)
        return valueOnly to pk
    }

    /** 把一个索引值前缀解回可读文本，仅用于错误信息。 */
    fun describeValues(valuePrefix: ByteArray): String =
        runCatching { String(valuePrefix, Charsets.UTF_8).trimEnd('\u0000') }.getOrDefault("<binary>")

    /** 索引项是否落在给定的值前缀内。 */
    fun matchesPrefix(entryKey: ByteArray, prefix: ByteArray): Boolean =
        entryKey.size >= prefix.size &&
            ByteArrayComparator.compare(entryKey, 0, prefix.size, prefix, 0, prefix.size) == 0

    /** 该索引是否可用于等值查找（三类索引都支持等值）。 */
    fun supportsEquality(index: IndexDef): Boolean = when (index.kind) {
        IndexKind.PRIMARY, IndexKind.ORDERED, IndexKind.HASH -> true
        else -> false
    }

    /** 该索引是否支持范围扫描（哈希索引不支持）。 */
    fun supportsRange(index: IndexDef): Boolean = when (index.kind) {
        IndexKind.PRIMARY, IndexKind.ORDERED -> true
        IndexKind.HASH -> false
        else -> false
    }
}
