package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.Epoch
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.decodeShardId
import com.cloudea.yangbase.common.encode
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.storage.Manifest
import com.cloudea.yangbase.storage.ManifestStore

/**
 * 备份、PITR 与闪回（设计文档 §12.3、§7.6）。
 *
 * ## 为什么这三件事在这个架构里几乎是同一件事
 *
 * 对象存储上的数据**不可变**（原则 P2）：SST 一旦写出就不再改动，
 * manifest 快照 `M-{seq}.mf` 也只增不改。于是：
 *
 * ```
 *   一份备份   = 一份「哪些 manifest 快照构成那一刻的状态」的清单
 *   PITR      = 找到时刻 T 之前最近的那个 manifest 快照，把 CURRENT 指回去
 *   FLASHBACK = 同一件事，只是粒度到单张表
 * ```
 *
 * 三者都不需要拷贝数据。**这就是不可变 + 指针切换带来的直接回报**：
 * 备份是 O(分片数) 次对象写（不是 O(数据量)），恢复是 O(1) 次指针切换。
 *
 * ## 恢复之后为什么必须换 epoch
 *
 * 恢复会把 `manifest/CURRENT` 指回一个**旧**的快照，那个快照的
 * `walFlushedThroughSeq` 也旧。如果不做任何额外处理，写者恢复时会去重放
 * 恢复点**之后**写下的 WAL 对象——于是刚恢复的数据又被覆盖回去，恢复等于没做。
 *
 * 解法是让分片换一个新的租约 epoch：`wal/e{新}/` 与旧前缀互不相干，
 * 恢复点之后的一切写入天然被忽略。**租约的 epoch 机制在这里第二次发挥了作用**
 * （第一次是 fencing 旧写者）。
 */
class SnapshotManager(
    private val store: ObjectStore,
    private val engine: DatabaseEngine,
    /**
     * 后端没有 ETag CAS 时是否允许降级——与 `DatabaseEngine` /
     * `--i-accept-nonatomic-pointer-switch` 是**同一个开关**。
     *
     * ⚠️ 这里**必须自己接上**，不能只给引擎开：恢复真正动的是 `manifest/CURRENT`，
     * 而切换指针的是本类内部这个 `ManifestStore`。只把开关传给引擎的后果是
     * 「引擎开好了、备份也做了，`restore --yes` 却在切换指针时报
     * `CapabilityNotSupportedException`」——一个与"我要恢复"毫无关系的错误。
     */
    private val allowNonAtomicPointerSwitch: Boolean = false,
) {

    private val manifestStore = ManifestStore(store, allowNonAtomicPointerSwitch)

    /**
     * 一份备份清单。
     *
     * 它记录的**不是数据**，而是「那一刻每个分片的 manifest 快照是哪一个」——
     * 数据对象本身已经由对象存储负责持久性，备份只需要记住从哪儿能找到它们。
     */
    @kotlinx.serialization.Serializable
    data class BackupManifest(
        val createdAt: Long,
        val createdBy: String,
        /** 分片 key → manifest 快照的 seq。 */
        val shards: Map<String, Long>,
        /** 备份时 catalog 的 schema 版本，用于诊断。 */
        val catalogTables: Map<String, Long> = emptyMap(),
        val note: String = "",
    )

    // ── 备份 ──────────────────────────────────────────────────────

    /**
     * 创建一份备份。
     *
     * 成本是 **O(分片数) 次对象写**，与数据量无关。这不是优化出来的结果，
     * 而是「数据不可变」这个前提的直接推论。
     */
    suspend fun backup(nodeId: String, note: String = ""): String {
        val shards = LinkedHashMap<String, Long>()
        val tables = LinkedHashMap<String, Long>()
        for (t in engine.catalog.allTables()) {
            tables[t.name] = t.version
            for (shard in engine.allShards(t)) {
                // 必须先 flush：否则 MemTable 里那部分数据还没有对应的 manifest
                engine.shardStore(shard, t).flush()
                val mf = manifestStore.load(shard)?.first ?: continue
                shards[shard.encode()] = mf.seq
            }
        }
        val m = BackupManifest(
            createdAt = System.currentTimeMillis(),
            createdBy = nodeId,
            shards = shards,
            catalogTables = tables,
            note = note,
        )
        val key = ObjectKeys.backupManifest(m.createdAt)
        store.put(key, json.encodeToString(BackupManifest.serializer(), m).toByteArray())
        return key
    }

    /** 列出可用的备份，按时间升序。对象名的零填充让 LIST 的字典序即时间序。 */
    suspend fun listBackups(): List<String> = store.listAll(ObjectKeys.backupsPrefix()).sorted()

    suspend fun loadBackup(key: String): BackupManifest =
        json.decodeFromString(BackupManifest.serializer(), String(store.get(key), Charsets.UTF_8))

    // ── 恢复 ──────────────────────────────────────────────────────

    /**
     * 从一份备份恢复。
     *
     * 步骤：把每个分片的 CURRENT 指回备份时记录的那个 manifest 快照，
     * 然后**换掉分片的租约 epoch**（见类注释：不换 epoch 的话，
     * 恢复点之后的 WAL 会在下次打开时被重放，把数据又覆盖回去）。
     */
    suspend fun restore(backupKey: String): RestoreReport {
        val m = loadBackup(backupKey)
        // 水位对象是单调的，不会跟着数据回退。本地缓存的段里可能还留着
        // "属于未来"的 id——丢掉它只是浪费几个号，继续用则可能和恢复回来的行撞上。
        engine.autoIncrement.invalidate()
        var restored = 0
        val missing = ArrayList<String>()
        for ((shardKey, seq) in m.shards) {
            // 用 encode() 的逆，**不要**去解 toString()：
            // 备份清单是跨版本读的，而 toString() 只是给人看的
            val shard = decodeShardId(shardKey) ?: continue
            // 备份清单里记的是 seq；实际键还带 epoch 后缀，所以这里按前缀找
            val snap = store.listAll(ObjectKeys.manifestPrefix(shard))
                .filter { it.contains("/M-${ObjectKeys.pad(seq, 12)}") }
                .maxOrNull()
            val bytes = try {
                store.get(snap ?: ObjectKeys.manifestSnapshot(shard, seq))
            } catch (_: ObjectNotFoundException) {
                missing.add("$shardKey@$seq")
                continue
            }
            val old = json.decodeFromString(Manifest.serializer(), String(bytes, Charsets.UTF_8))
            // 关键：**先让分片下线**，再切指针。分片还开着的话，
            // 它内存里的 manifest 与 WAL 写者会继续按旧视图工作。
            engine.evictShard(shard)
            manifestStore.commit(shard, bumpEpoch(shard, old), currentEtagOf(shard))
            restored++
        }
        return RestoreReport(restored, missing)
    }

    data class RestoreReport(val restoredShards: Int, val missingSnapshots: List<String>)

    /**
     * 恢复时把 manifest 的 **walEpoch 抬高一位**。
     *
     * 这是恢复能生效的关键。恢复会把 CURRENT 指回一个旧快照，那个快照的
     * `walFlushedThroughSeq` 也旧；如果不抬 epoch，写者下次打开时会去重放
     * 恢复点**之后**写下的 WAL 对象——刚恢复的数据又被覆盖回去，恢复等于没做。
     *
     * 抬 epoch 之后 `wal/e{新}/` 与旧前缀互不相干，恢复点之后的一切写入
     * 天然被忽略。这是租约的 epoch 机制**第二次**发挥作用（第一次是 fencing 旧写者）。
     */
    private suspend fun bumpEpoch(shard: ShardId, old: Manifest): Manifest {
        val current = manifestStore.load(shard)?.first
        val base = maxOf(old.walEpoch, current?.walEpoch ?: 0L)
        return old.copy(walEpoch = base + 1)
    }

    private suspend fun currentEtagOf(shard: ShardId): com.cloudea.yangbase.objectstore.ETag? =
        try {
            store.head(ObjectKeys.manifestCurrent(shard)).etag
        } catch (_: ObjectNotFoundException) {
            null
        }

    // ── 时间点恢复 ────────────────────────────────────────────────

    /**
     * 找到「**不晚于** [millis] 的最新 manifest 快照」的 seq。
     *
     * 判据用的是 manifest 里**数据本身**的水位
     * （`levels` 中所有文件的 `maxTs` 的最大值），而不是墙上时钟：
     * 时间戳的高位虽然就是物理毫秒，但批量预取会让它略微跑在前面，
     * 用数据水位判断才与「那一刻能看到哪些行」严格一致。
     *
     * 返回 null 表示这个分片在那一刻还没有任何数据。
     */
    suspend fun manifestSeqAt(shard: ShardId, millis: Long): Long? {
        val targetTs = TimeTravel.readTsFor(millis)
        // 键名形如 `M-{seq:012d}-e{epoch:05d}.mf`（epoch 保证同 seq 的不同提交
        // 落在不同对象上）。这里**按实际键读**，不自己拼——同一 seq 可能有多个 epoch
        val bySeq = store.listAll(ObjectKeys.manifestPrefix(shard))
            .filter { it.contains("/M-") }
            .sortedBy { it }
            .mapNotNull { key ->
                // seq 取 `M-` 之后的连续数字；epoch 是其后 `-e` 的部分
                val tail = key.substringAfterLast("/M-")
                val seq = tail.takeWhile { it.isDigit() }.toLongOrNull() ?: return@mapNotNull null
                seq to key
            }
        var best: Long? = null
        for ((seq, key) in bySeq) {
            val bytes = try {
                store.get(key)
            } catch (_: ObjectNotFoundException) {
                continue
            }
            val mf = runCatching {
                json.decodeFromString(Manifest.serializer(), String(bytes, Charsets.UTF_8))
            }.getOrNull() ?: continue
            if (mf.dataWatermark <= targetTs) best = seq else break
        }
        return best
    }

    /**
     * 把一张表恢复到某个墙上时刻。
     *
     * 找不到合适的快照时**明确报错**，而不是恢复到「最接近的」那一个——
     * 恢复了错误的时刻会给出一个看起来正常、但不属于那个时间点的数据库，
     * 那比直接失败危险得多。
     */
    suspend fun restoreTableTo(table: TableSchema, millis: Long): RestoreReport {
        // 早于 GC 水位的时刻**必须明确报错**：那部分历史已经被回收，
        // 恢复到「最接近的」那一个会给出一个看起来正常、但不属于那个时间点的数据库。
        // 那比直接失败危险得多（设计文档 §7.6）。
        val safe = com.cloudea.yangbase.txn.GarbageCollector(store).safeReadTs()
        if (safe > 0 && TimeTravel.readTsFor(millis) < safe) {
            throw SqlException(
                MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                "无法恢复到该时刻：它早于 GC 水位 ${
                    java.time.Instant.ofEpochMilli(TimeTravel.millisOf(safe))
                }，那部分历史已被回收。",
            )
        }
        // 同 restore()：恢复点之后领到的号段必须作废
        engine.autoIncrement.invalidate(table.tableId)
        var restored = 0
        val missing = ArrayList<String>()
        for (shard in engine.allShards(table)) {
            val seq = manifestSeqAt(shard, millis)
            if (seq == null) {
                missing.add("$shard（该时刻无数据）")
                continue
            }
            // 键名带 epoch 后缀，按前缀找实际键（同一 seq 可能有多个）
            val key = store.listAll(ObjectKeys.manifestPrefix(shard))
                .filter { it.contains("/M-${ObjectKeys.pad(seq, 12)}") }
                .maxOrNull() ?: ObjectKeys.manifestSnapshot(shard, seq)
            val bytes = store.get(key)
            val old = json.decodeFromString(Manifest.serializer(), String(bytes, Charsets.UTF_8))
            engine.evictShard(shard)
            manifestStore.commit(shard, bumpEpoch(shard, old), currentEtagOf(shard))
            restored++
        }
        if (restored == 0) {
            throw SqlException(
                MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                "表 '${table.name}' 无法恢复到该时刻：没有任何分片在该时刻之前存在过。" +
                    "（更早的历史可能已被 GC 回收，见 gc watermark）",
            )
        }
        return RestoreReport(restored, missing)
    }

    /**
     * 各分片当前的数据水位（毫秒）。用于告诉使用者「最早能恢复到什么时候」。
     *
     * ⚠️ **必须跳过没有任何数据的 manifest 快照**（`dataWatermark == 0`）。
     * 分片的第一个 manifest 是建分片时写的空清单——它的水位是 0，
     * 直接取「seq 最小的那个」会得到 0，于是"最早可恢复时刻"算出来是个
     * 无意义的数字，或者干脆被判成「没有历史」。那不是数据的问题，
     * 是取错了快照。
     */
    suspend fun earliestRestorableMillis(table: TableSchema): Long? {
        var earliest: Long? = null
        for (shard in engine.allShards(table)) {
            // `M-{seq}-e{epoch}.mf`：取 `M-` 之后的连续数字部分（seq），但读的时候
            // 用**实际键**（同一 seq 可能有多个 epoch 的提交）
            val keys = store.listAll(ObjectKeys.manifestPrefix(shard))
                .filter { it.contains("/M-") }
                .sorted()
            for (key in keys) {
                val bytes = runCatching { store.get(key) }.getOrNull() ?: continue
                val mf = runCatching {
                    json.decodeFromString(Manifest.serializer(), String(bytes, Charsets.UTF_8))
                }.getOrNull() ?: continue
                val ms = TimeTravel.millisOf(mf.dataWatermark)
                if (ms <= 0) continue          // 空清单，不是"历史的最早时刻"
                if (earliest == null || ms < earliest!!) earliest = ms
                break                          // seq 升序，第一个有数据的即本分片最早
            }
        }
        return earliest
    }

    private val json = kotlinx.serialization.json.Json {
        prettyPrint = true; encodeDefaults = true; ignoreUnknownKeys = true
    }
}

/**
 * manifest 里**数据本身**的提交时间戳水位。
 *
 * 取所有文件 `maxTs` 的最大值：这就是「这个 manifest 能看到的最新版本」。
 * PITR 用它来判断「某个时刻能不能用这个快照表示」。
 *
 * 注意 `Manifest.commitTsWatermark` 那个字段是设计文档留下的占位，
 * 从来没有被写入过；这里刻意**不去修它**而是即时计算——文件级的 maxTs
 * 是数据自己的事实，比一个写入时拍下来的数字更可靠。
 */
val Manifest.dataWatermark: Long
    get() = allFiles().maxOfOrNull { it.maxTs } ?: 0L
