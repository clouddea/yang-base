package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.Epoch
import com.cloudea.yangbase.common.FileId
import com.cloudea.yangbase.common.IndexId
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId

/**
 * 对象键的唯一生成处。**禁止手写字符串拼接**——零填充是正确性要求，不是风格问题：
 * 对象存储的 LIST 按**字节字典序**返回，`10.log` 会排在 `9.log` 之前，
 * 漏掉零填充会导致恢复时日志乱序。
 *
 * 布局见设计文档 §5.1，三条键设计规则见 §5.2。
 */
object ObjectKeys {

    /** 固定宽度十进制，用于所有序号型分量。 */
    fun pad(value: Long, width: Int = 12): String {
        val s = value.toString()
        require(s.length <= width) { "序号 $value 超出 $width 位宽度" }
        return "0".repeat(width - s.length) + s
    }

    // ── shard: 分片数据（= 一个独立 LSM 实例）────────────────────────

    private fun shardPrefix(shard: ShardId) = "shard/${shard.table.value}/${shard.partition.value}/"

    /** 分片根前缀，用于 GC 的可达性分析。 */
    fun shardRoot(shard: ShardId) = shardPrefix(shard)

    // manifest：唯一的可变「根指针」
    fun manifestCurrent(shard: ShardId) = shardPrefix(shard) + "manifest/CURRENT"

    /**
     * 全量 manifest 的对象键。
     *
     * ⚠️ 带 **epoch** 后缀：`seq` 只表示"第几代 manifest"，而它在三处会被复用
     * 而不自增（启动写者、快照恢复、分片接管）——只用 `seq` 命名会让两次不同的
     * 提交**写到同一个对象**，于是 CURRENT 指向的内容可能来自另一个写者
     * （指针与内容不是一次原子写），时间旅行按 `M-{seq}` 反查历史也会取到
     * 不对应的状态。加上 epoch 之后每次提交都是**新对象**，符合 P2。
     *
     * 前缀仍是 `M-{seq}`，所以"按 seq 排序取历史版本"的解析不受影响。
     */
    fun manifestSnapshot(shard: ShardId, seq: Long, epoch: Long = 0L) =
        shardPrefix(shard) + "manifest/M-${pad(seq)}-e${pad(epoch, 5)}.mf"

    fun manifestDelta(shard: ShardId, seq: Long) =
        shardPrefix(shard) + "manifest/d-${pad(seq)}.mf"

    fun manifestPrefix(shard: ShardId) = shardPrefix(shard) + "manifest/"

    /** manifest 快照（`M-*`）的前缀——与 `CURRENT`、`d-*` 区分开。 */
    fun manifestSnapshotPrefix(shard: ShardId) = manifestPrefix(shard) + "M-"

    /**
     * 从快照键解析出 `seq`（解析不出返回 null）。
     *
     * 与 [manifestSnapshot] 是**同一份格式的两半**，放在一起免得漂移：
     * 「接管审计」要靠它判断某个快照是否比当前指针更新。
     */
    fun snapshotSeqOf(key: String): Long? =
        key.substringAfterLast("M-").substringBefore('-').toLongOrNull()

    // WAL：epoch 在路径中，是 fencing 的第一道防线
    fun walPrefix(shard: ShardId, epoch: Epoch) = shardPrefix(shard) + "wal/e${epoch.value}/"

    fun walBatch(shard: ShardId, epoch: Epoch, seq: Long) =
        walPrefix(shard, epoch) + "${pad(seq)}.log"

    /** 后台合并后的日志段。 */
    fun walSegment(shard: ShardId, epoch: Epoch, from: Long, to: Long) =
        walPrefix(shard, epoch) + "s-${pad(from)}-${pad(to)}.log"

    fun walAllEpochsPrefix(shard: ShardId) = shardPrefix(shard) + "wal/"

    /**
     * **未决** WAL 对象（跨分片 2PC 的第一阶段）。
     *
     * 用**后缀**而不是独立前缀来区分：独立前缀会让恢复路径多一次 LIST，
     * 而恢复是「每个分片每次打开」都要走的路——多一次 LIST 就是每个分片
     * 每次启动多一次请求，白白付出去只为了一类罕见的事务。
     *
     * 语义仍然完全由对象名表达（`.p.log` = pending），不碰二进制格式，
     * 所以已经写下的旧 WAL 对象照样可读。
     *
     * 一个未决对象只有在 `wal/tx/{txId}.commit` 存在时才被重放（设计文档 §7.3）。
     */
    const val PENDING_SUFFIX = ".p.log"

    fun walPending(shard: ShardId, epoch: Epoch, seq: Long) =
        walPrefix(shard, epoch) + "${pad(seq)}$PENDING_SUFFIX"

    /** 对象键是否是一个未决 WAL。 */
    fun isPendingWal(key: String) = key.endsWith(PENDING_SUFFIX)

    /**
     * 事务的提交标记。**这是跨分片事务唯一的原子提交点**。
     *
     * 它放在集群级前缀下而不是某个分片里：提交决策属于事务，不属于任何一个分片。
     * 放在分片里会让「谁是决定者」变成一个需要额外协商的问题。
     */
    fun txCommit(txId: String) = "wal/tx/${txId.replace('/', '_')}.commit"

    fun txCommitPrefix() = "wal/tx/"

    // SST：索引块与数据块**分离成两个对象**（设计文档 §6.4，决策 D6）
    private fun sstLevelPrefix(shard: ShardId, level: Int) =
        shardPrefix(shard) + "sst/L$level/"

    fun sstData(shard: ShardId, level: Int, fid: FileId) = sstLevelPrefix(shard, level) + "${fid.value}.dat"

    fun sstIndex(shard: ShardId, level: Int, fid: FileId) = sstLevelPrefix(shard, level) + "${fid.value}.idx"

    fun sstLevel(shard: ShardId, level: Int) = sstLevelPrefix(shard, level)

    fun sstAll(shard: ShardId) = shardPrefix(shard) + "sst/"

    /** 分片状态（写者、水位、TTL）。 */
    fun shardState(shard: ShardId) = shardPrefix(shard) + "state.json"

    /**
     * 分片写者租约（设计文档 §4.3）。
     *
     * ⚠️ 这个对象**从不被删除**——释放只是把 holder 清空。
     * epoch 一旦被回收重用，旧写者留在 `wal/e{旧epoch}/` 下的对象
     * 就会被新写者当成自己的日志重放进去，那是静默的数据损坏。
     */
    fun shardLease(shard: ShardId) = shardPrefix(shard) + "lease.json"

    // ── catalog: 库表元数据（设计文档 §4.5）─────────────────────────

    fun database(dbId: Long) = "catalog/db/$dbId.json"

    fun databasePrefix() = "catalog/db/"

    fun tablePrefix(table: TableId) = "catalog/table/${table.value}/"

    /** 不可变；每次 DDL 生成新版本。 */
    fun schema(table: TableId, version: Long) = tablePrefix(table) + "schema.v${pad(version)}.json"

    /** 指针 → 当前 schema 版本。 */
    fun schemaCurrent(table: TableId) = tablePrefix(table) + "CURRENT"

    fun indexes(table: TableId) = tablePrefix(table) + "indexes.json"

    fun tablePrefixAll() = "catalog/table/"

    /** 表名 → table_id 的映射，供 `USE db` 后按名解析。 */
    fun tableByName(dbId: Long, tableName: String) = "catalog/byname/$dbId/$tableName.json"

    fun tableByNamePrefix(dbId: Long) = "catalog/byname/$dbId/"

    // ── stats: 优化器统计与分区裁剪 ────────────────────────────────

    fun partitionStats(table: TableId, version: Long) =
        "stats/table/${table.value}/partitions.v${pad(version)}.json"

    fun columnStats(table: TableId, partition: PartitionId, version: Long) =
        "stats/table/${table.value}/${partition.value}/colstats.v${pad(version)}.json"

    // ── 独立索引（设计文档 §8.1：一切索引 = 不可变文件 + manifest 指针）──

    fun indexRoot(index: IndexId) = "index/${index.value}/"

    fun indexManifestCurrent(index: IndexId) = indexRoot(index) + "manifest/CURRENT"

    fun indexManifestSnapshot(index: IndexId, seq: Long) = indexRoot(index) + "manifest/M-${pad(seq)}.mf"

    /** 倒排索引段（§8.6）。 */
    fun indexSegment(index: IndexId, segId: String) = indexRoot(index) + "seg/$segId/"

    // ── ops ──────────────────────────────────────────────────────

    fun garbageManifest(ts: Long) = "ops/garbage/${pad(ts, 20)}/${java.util.UUID.randomUUID()}.json"

    fun garbagePrefix() = "ops/garbage/"

    /**
     * 备份清单（设计文档 §12.3）。
     *
     * 备份记录的**不是数据**，而是「那一刻每个分片的 manifest 快照是哪一个」。
     * 数据对象本身已经由对象存储负责持久性，因此一份备份的成本是
     * O(分片数) 次对象写，与数据量无关。
     */
    fun backupManifest(ts: Long) = "ops/backups/${pad(ts, 20)}.json"

    fun backupsPrefix() = "ops/backups/"

    /**
     * 已删除对象的墓碑（设计文档 §12.3「误删表恢复」）。
     *
     * `DROP TABLE` 只写墓碑、把 schema 与 CURRENT 留在原地，30 天内可恢复。
     * 在此之前 `dropTableInternal` 是**直接删**——一次误操作就永久丢表，
     * 而文档明确要求保留 30 天。
     */
    fun tableTombstone(table: TableId) = tablePrefix(table) + "DELETED.json"

    /** GC 水位：早于它的历史版本可能已被回收。时间旅行查询必须先比对它（设计文档 §6.9）。 */
    fun gcWatermark() = "meta/gc/watermark.json"

    /**
     * 表的自增水位对象（设计文档 §11.4）。
     *
     * 放在 `catalog/table/{id}/` 下而不是 `shard/` 下，因为自增是**表级**的：
     * 自增列常常就是分区列，按分片各维护一份水位会让两个分片各自发出同一个 id，
     * 而它们哈希到同一个分片——一次静默的主键覆盖（P1 那条「不能有静默损坏」）。
     */
    fun autoIncWatermark(table: TableId) = tablePrefix(table) + "autoinc.json"

    // ── ops/audit：审计日志（设计文档 §12.5）─────────────────────────
    //
    // 审计落在**对象存储**上而不是进程日志里：CN 可以随时销毁重建，
    // stderr 上的东西一样都留不下来（原则 P1）。这是唯一被允许写对象的
    // 观测手段，因此它的写必须是批量、异步、可失败的。

    /** 当日审计日志，JSONL：一行一条记录。**按天分片**，因此单个对象是有界的。 */
    fun auditLog(date: String): String {
        require(DATE_PATTERN.matches(date)) { "审计日期必须是 yyyy-MM-dd: '$date'" }
        return "ops/audit/$date.jsonl"
    }

    /**
     * 滚动出来的审计分片。
     *
     * 当日对象超过大小上限后，新的记录不再追加到当日对象（避免每次都重写一个
     * 越来越大的对象），而是各自落成不可变分片。零填充保证 LIST 顺序即时间顺序。
     */
    fun auditRolled(date: String, seq: Long): String {
        require(DATE_PATTERN.matches(date)) { "审计日期必须是 yyyy-MM-dd: '$date'" }
        return "ops/audit/$date.${pad(seq, 20)}.jsonl"
    }

    /** 审计日志的公共前缀。读审计（运维路径，允许 LIST）时按它列举。 */
    fun auditPrefix() = "ops/audit/"

    private val DATE_PATTERN = Regex("\\d{4}-\\d{2}-\\d{2}")
}
