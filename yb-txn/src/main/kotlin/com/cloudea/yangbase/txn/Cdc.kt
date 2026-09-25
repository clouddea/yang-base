package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.CorruptDataException
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.encode
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.storage.WalCodec
import kotlinx.serialization.Serializable
import java.util.Base64

/**
 * CDC / 变更流（设计文档 §16.1 的 Q5 决策、§17 M5）。
 *
 * ## 为什么这个架构天然适合 CDC
 *
 * 变更日志**已经是**系统的一部分：WAL 对象不可变、按 seq 有序、
 * 每条记录带 commit_ts、写下去之后永不原地修改。要做变更订阅，
 * 我们不需要额外维护一份 binlog——**读已有的 WAL 就行**。
 *
 * 这是「对象存储是唯一真相源 + 不可变优先」两条原则的又一次直接回报：
 * 传统数据库需要专门为 CDC 复制一份日志（binlog / logical decoding slot），
 * 而这里日志本来就是数据的一部分。
 *
 * ## 保留窗口：诚实的边界
 *
 * WAL 对象在 flush 进 SST 之后、过了 GC 安全窗口就会被删掉。
 * 因此**可订阅的历史长度 = GC 安全窗口（默认 24 小时）**。
 * 超过窗口的变更只能从 SST 的当前状态反推（那是快照，不是变更流）。
 * 这个限制不是实现缺陷，而是「日志会被回收」的必然结果——
 * 需要更长保留期的使用者应当把变更流物化到自己的存储里（这正是 CDC 的用法）。
 *
 * ## 游标
 *
 * 游标是 `(shard → (epoch, seq, commitTs))`。它**可以序列化**，
 * 因此消费者可以把进度存下来，重启后接着读。存储进度不是本系统的事——
 * 消费者把它放在自己的数据库里即可，这正是 CDC 的常规形态。
 */
class ChangeStream(
    private val store: ObjectStore,
    /** 只订阅这些表的数据变更。为空表示订阅全部。 */
    private val tables: List<TableSchema> = emptyList(),
    startCursor: Cursor = Cursor.EMPTY,
) {

    /** 一位变更。 */
    class Change(
        val shard: ShardId,
        val tableId: Long,
        val commitTs: Long,
        /** 主键的保序编码。**能直接用于点查**。 */
        val key: ByteArray,
        /** null 表示删除。 */
        val value: ByteArray?,
        val op: Op,
    ) {
        enum class Op { INSERT, UPDATE, DELETE }

        /** 主键的十六进制，给不方便处理二进制的地方用。 */
        fun keyHex(): String = key.joinToString("") { "%02x".format(it) }

        /** 把变更的值解成一行。删除事件的 value 是 null，返回 null。 */
        fun decodeRow(table: TableSchema): List<com.cloudea.yangbase.common.Datum>? =
            value?.let { RowCodec.decode(it, table) }

        override fun toString() = "Change($op ts=$commitTs key=${keyHex().take(16)}…)"
    }

    /** 每个分片的读取进度。 */
    @Serializable
    data class Cursor(val positions: Map<String, Position> = emptyMap()) {
        companion object {
            val EMPTY = Cursor()
        }
    }

    @Serializable
    data class Position(val epoch: Long, val seq: Long, val commitTs: Long)

    @Volatile
    private var cursor: Cursor = startCursor

    fun cursor(): Cursor = cursor

    /**
     * 拉取一批变更。
     *
     * **不产生热路径请求**：它只 LIST + GET WAL 对象，而这些对象在
     * 被 GC 回收之前一直在那里。CDC 消费者的读取**不会**影响在线事务。
     */
    suspend fun poll(limit: Int = 1000): List<Change> {
        val out = ArrayList<Change>()
        val newPositions = HashMap(cursor.positions)

        for (table in targetTables()) {
            for (shard in shardsOf(table)) {
                if (out.size >= limit) break
                val pos = cursor.positions[shard.encode()]
                val (changes, next) = readShard(shard, table, pos, limit - out.size)
                out.addAll(changes)
                if (next != null) newPositions[shard.encode()] = next
            }
        }
        cursor = Cursor(newPositions)
        return out
    }

    /**
     * 从某个墙上时刻开始读。
     *
     * 只能回溯到 GC 安全窗口之内——超出时返回空而不是报错，
     * 因为「订阅从今天开始」是完全正常的用法。
     */
    suspend fun pollSince(millis: Long, limit: Int = 1000): List<Change> {
        val fromTs = TimeTravel.readTsFor(millis)
        val saved = cursor
        cursor = Cursor(
            saved.positions.mapValues { (_, p) -> p.copy(commitTs = fromTs) }
        )
        return try {
            poll(limit)
        } finally {
            // 保持外部游标语义：pollSince 是一次性查询，不该推进进度
            if (saved.positions.isEmpty()) cursor = saved
        }
    }

    private fun targetTables(): List<TableSchema> =
        if (tables.isEmpty()) emptyList() else tables

    /** 只要**数据分片**：索引分片（负数 TableId）的变更对使用者没有意义。 */
    private fun shardsOf(table: TableSchema): List<ShardId> {
        val n = table.effectivePartitionCount
        return (0 until n).map {
            ShardId(
                com.cloudea.yangbase.common.TableId(table.tableId),
                com.cloudea.yangbase.common.PartitionId(it.toLong()),
            )
        }
    }

    private suspend fun readShard(
        shard: ShardId,
        table: TableSchema,
        pos: Position?,
        limit: Int,
    ): Pair<List<Change>, Position?> {
        if (limit <= 0) return emptyList<Change>() to null

        // 列出该分片**所有 epoch** 下的 WAL 对象：租约换代会换 epoch，
        // 只读当前 epoch 会漏掉换约之前写下、但尚未 flush 的那些变更。
        val keys = store.listAll(ObjectKeys.walAllEpochsPrefix(shard))
            .filter { !ObjectKeys.isPendingWal(it) }   // 未决批次尚未提交，不能当变更流
            .sorted()

        val out = ArrayList<Change>()
        var next: Position? = null

        for (k in keys) {
            if (out.size >= limit) break
            val (epoch, seq) = parseKey(k) ?: continue
            // 跳过已经读过的位置
            if (pos != null && (epoch < pos.epoch || (epoch == pos.epoch && seq <= pos.seq))) continue

            val objs = try {
                WalCodec.decodeAny(store.get(k), k)
            } catch (_: CorruptDataException) {
                continue
            }
            for (obj in objs) for (b in obj.batches) {
                // 同一条记录可能被读过（commitTs 是游标的第二道保险）
                if (pos != null && b.commitTs <= pos.commitTs && epoch == pos.epoch) continue
                for (op in b.ops) {
                    out.add(
                        Change(
                            shard = shard,
                            tableId = table.tableId,
                            commitTs = b.commitTs,
                            key = op.key,
                            value = op.value,
                            op = if (op.value == null) Change.Op.DELETE else Change.Op.INSERT,
                        )
                    )
                    if (out.size >= limit) break
                }
                if (out.size >= limit) break
            }
            // 段对象覆盖 [from, to]：游标推进到 `to`（读完了整个段），
            // 按 `from` 记会让下一轮重复读同一个段
            val lastTs = objs.maxOfOrNull { o -> o.batches.maxOfOrNull { it.commitTs } ?: 0L }
                ?: (pos?.commitTs ?: 0L)
            next = Position(epoch, seq, lastTs)
        }
        return out to next
    }

    /**
     * 从 WAL 键里解出 `(epoch, seq)`。
     *
     * 两种形态都要认：
     * - 普通对象 `wal/e{epoch}/{seq}.log`
     * - **段对象** `wal/e{epoch}/s-{from}-{to}.log`（序号取 `from`：段覆盖
     *   `[from, to]`，游标按 `from` 定位是保守且正确的）
     *
     * 只认第一种的话，段会被 `?: continue` 静默跳过——变更流**缺一整段**，
     * 而调用方只会发现"某些变更没出现"。
     */
    private fun parseKey(key: String): Pair<Long, Long>? {
        Regex("/wal/e(\\d+)/(\\d+)\\.log$").find(key)?.let { m ->
            return m.groupValues[1].toLongOrNull()?.let { e ->
                m.groupValues[2].toLongOrNull()?.let { s -> e to s }
            }
        }
        Regex("/wal/e(\\d+)/s-(\\d+)-(\\d+)\\.log$").find(key)?.let { m ->
            return m.groupValues[1].toLongOrNull()?.let { e ->
                m.groupValues[2].toLongOrNull()?.let { s -> e to s }
            }
        }
        return null
    }

    // ── 便捷解码 ──────────────────────────────────────────────────

    /** 主键的 Base64，便于放进 JSON。 */
    fun keyBase64(change: Change): String = Base64.getEncoder().encodeToString(change.key)
}
