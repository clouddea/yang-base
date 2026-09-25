package com.cloudea.yangbase.common

/** 表 ID。全局单调分配，永不复用（设计文档 §5.2 规则 3）。 */
@JvmInline
value class TableId(val value: Long) {
    override fun toString(): String = "t$value"
}

/** 分区 ID。分区即写入分片（设计文档 §9.3）。 */
@JvmInline
value class PartitionId(val value: Long) {
    override fun toString(): String = "p$value"
}

/**
 * 存储分片 = (表, 分区)。这是写入与租约的最小单元。
 * 任一时刻只有一个写者持有它的租约（设计文档 §4.3）。
 */
data class ShardId(val table: TableId, val partition: PartitionId) {
    override fun toString(): String = "$table/$partition"
}

/**
 * 分片的**持久化形式**。
 *
 * 刻意与 `toString()` 分开。`toString()` 是给人看的诊断信息，随时可以为了
 * 好看而改；一旦有人把它写进备份清单、CDC 游标这类**跨进程跨版本**的地方，
 * 改一次格式就会让旧数据静默读不出来——不是报错，是「解析不出来就跳过」。
 * 这里的格式是契约：`{tableId}/{partitionId}`，只有数字和斜杠。
 */
fun ShardId.encode(): String = "${table.value}/${partition.value}"

/** [encode] 的逆。解析不出来返回 null，由调用方决定是报错还是跳过。 */
fun decodeShardId(s: String): ShardId? {
    val parts = s.split('/')
    if (parts.size != 2) return null
    val t = parts[0].toLongOrNull() ?: return null
    val p = parts[1].toLongOrNull() ?: return null
    return ShardId(TableId(t), PartitionId(p))
}

/**
 * 提交时间戳。全局单调递增，由 TSO 分配。
 * 可见性判断的唯一依据：行版本对 read_ts = T 可见 ⟺ commitTs <= T。
 */
@JvmInline
value class CommitTs(val value: Long) : Comparable<CommitTs> {
    override fun compareTo(other: CommitTs): Int = value.compareTo(other.value)
    override fun toString(): String = "ts($value)"
}

/** 读时间戳。只读事务与读写事务开始时取得。 */
@JvmInline
value class ReadTs(val value: Long) : Comparable<ReadTs> {
    override fun compareTo(other: ReadTs): Int = value.compareTo(other.value)
    override fun toString(): String = "read($value)"
}

/** 索引 ID。 */
@JvmInline
value class IndexId(val value: Long) {
    override fun toString(): String = "idx$value"
}

/**
 * 写者代际。随租约转移单调递增，永不重置。
 * fencing 的第一道防线：WAL 对象的 key 里带 epoch（设计文档 §4.3）。
 */
@JvmInline
value class Epoch(val value: Long) : Comparable<Epoch> {
    override fun compareTo(other: Epoch): Int = value.compareTo(other.value)
    override fun toString(): String = "e$value"
}

/** 事务 ID。用于幂等重放（设计文档 §7.5）。 */
@JvmInline
value class TxId(val value: String)

/**
 * SST 文件 ID。ULID 风格：48 位毫秒时间戳 + 80 位随机。
 * 字典序即时间序，且高熵后缀避免同前缀热点（设计文档 §5.3）。
 */
@JvmInline
value class FileId(val value: String) {
    override fun toString(): String = value
}
