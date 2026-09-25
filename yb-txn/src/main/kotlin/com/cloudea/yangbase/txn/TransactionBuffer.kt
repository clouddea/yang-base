package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.storage.WalOp
import java.util.Base64

/**
 * 事务写缓冲。
 *
 * **为什么必须有它**：在此之前，写入是直接落 WAL 与 MemTable 的，
 * 于是 `ROLLBACK` 只清空了读快照，**没有撤销任何写入**——那是个语义错误，
 * 会直接误导使用者（以为回滚了，其实数据已经生效且可见）。
 *
 * 现在所有写先进入缓冲，COMMIT 时才一次性落到存储层，ROLLBACK 直接丢弃。
 *
 * 两个必须支持的能力：
 *  1. **读自己的写** —— 事务内 `INSERT` 后立刻 `SELECT` 必须看得到
 *  2. **后写覆盖先写** —— 同一行在事务内被改两次，只保留最后一次
 *
 * ⚠️ **跨分片原子性尚未保证**：COMMIT 会依次写各分片的 WAL，
 * 中途失败会留下部分分片已提交的状态。彻底解决需要 2PC（设计文档 §7.3），
 * 尚未实现。单分片事务（最常见的情形）是原子的——它只产生一个 WAL 对象。
 */
class TransactionBuffer(
    /** 事务开始时分配的读时间戳，用于判断「这条记录在我开始之前是否已存在」。 */
    val startTs: Long,
) {
    private data class Key(val shard: ShardId, val pk: String) {
        override fun toString() = "$shard#$pk"
    }

    /** 按键去重：同一行在事务内改多次只保留最后一次（后写覆盖先写）。 */
    private val ops = LinkedHashMap<Key, WalOp>()

    /** 事务内受影响的表的行数净变化，提交时用于维护精确计数。 */
    private var touched = 0

    fun put(shard: ShardId, pk: ByteArray, value: ByteArray?) {
        val k = Key(shard, encode(pk))
        if (ops.put(k, WalOp(pk, value)) == null) touched++
    }

    /** 读自己的写。返回 null 表示「本事务没改过这一行」。 */
    fun lookup(shard: ShardId, pk: ByteArray): LookupResult? {
        val op = ops[Key(shard, encode(pk))] ?: return null
        return if (op.value == null) LookupResult.Deleted else LookupResult.Found(op.value!!)
    }

    /** 本事务是否写过这个分片。 */
    fun touches(shard: ShardId): Boolean = ops.keys.any { it.shard == shard }

    val isEmpty: Boolean get() = ops.isEmpty()
    val size: Int get() = ops.size

    /** 按分片分组，供 COMMIT 时逐分片落 WAL。 */
    fun byShard(): Map<ShardId, List<WalOp>> {
        val out = LinkedHashMap<ShardId, MutableList<WalOp>>()
        for ((k, op) in ops) out.getOrPut(k.shard) { ArrayList() }.add(op)
        return out
    }

    /** 该行在缓冲中的最终状态，供唯一约束校验使用。 */
    fun allOps(): Collection<WalOp> = ops.values

    /** 只取某张表的操作（同一个事务可能跨表）。 */
    fun opsForTable(tableId: Long): List<WalOp> =
        ops.filterKeys { it.shard.table.value == tableId }.values.toList()

    /** 某个分片上的全部操作。索引项落在索引分片（负数 TableId）上。 */
    fun opsForShard(shard: ShardId): List<WalOp> =
        ops.filterKeys { it.shard == shard }.values.toList()

    /** 该表在缓冲中被改过几行。 */
    fun sizeForTable(tableId: Long): Int =
        ops.keys.count { it.shard.table.value == tableId }

    fun clear() = ops.clear()

    private fun encode(pk: ByteArray): String = Base64.getEncoder().encodeToString(pk)

    override fun toString() = "TxBuffer(${ops.size} ops, startTs=$startTs)"
}

/** 缓冲内的查找结果。与存储层的 `LookupResult` 同义，但区分「本事务未触及」。 */
sealed interface LookupResult {
    data class Found(val value: ByteArray) : LookupResult
    data object Deleted : LookupResult
}
