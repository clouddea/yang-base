package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.audit.Audit
import com.cloudea.yangbase.common.audit.AuditActions
import com.cloudea.yangbase.common.audit.AuditEvent
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.storage.Manifest
import com.cloudea.yangbase.storage.ManifestStore
import com.cloudea.yangbase.storage.WalCodec
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * 垃圾回收与时间旅行水位（设计文档 §6.9、§12.3）。
 *
 * ## 为什么必须是两阶段的
 *
 * 对象存储没有「删除即释放」的强一致保证：删除一个对象后，**已经开始的读**
 * 可能仍然读得到它。更麻烦的是反过来——一个正在被长读事务使用的文件，
 * 如果被删掉了，那个事务会读到一个残缺的版本链，而且不会报错。
 *
 * 所以删除分成两步：
 * ```
 * 阶段 1（标记）：算出「不可达」的对象，写进 ops/garbage/{ts}/{uuid}.json
 * 阶段 2（删除）：**安全窗口之后**才按清单真删
 * ```
 * 安全窗口默认 24 小时。它的作用不是「等读者读完」，而是**把误判的代价
 * 从「静默读到错数据」降级为「多占一天存储」**。
 *
 * ## 三个必须区分的孤儿来源
 *
 * 设计文档 §6.9 特别强调它们对应不同的动作，混在一起看会得出错误结论：
 *  1. compaction 留下的中间文件——**正常**，是 GC 的主要目标
 *  2. 被 fence 掉的旧写者留下的 WAL——**正常且预期**（§4.3 的代价）
 *  3. 真正的泄漏——**是 bug，必须告警**
 *
 * 本实现把 1 和 2 分开报数，就是为了让第 3 类能被看见。
 */
class GarbageCollector(
    private val store: ObjectStore,
    /** 安全窗口：对象必须「不晚于这个时刻创建」才会被删。默认 24 小时。 */
    private val safetyWindowMillis: Long = 24L * 60 * 60 * 1000,
) {

    private val mutex = Mutex()

    // ── 水位 ──────────────────────────────────────────────────────

    /**
     * GC 水位：**早于这个时间戳的版本可能已被回收**。
     *
     * 时间旅行查询必须先与它比对——落在水位之下的读取会读到不完整的历史。
     * 那种情况下必须**明确报错**，而不是返回一个看似正常的残缺结果：
     * 后者会让使用者以为「那个时刻的数据就是这样的」。
     */
    suspend fun safeReadTs(): Long = mutex.withLock {
        try {
            val bytes = store.get(ObjectKeys.gcWatermark())
            Regex("\"safeReadTs\"\\s*:\\s*(\\d+)").find(String(bytes, Charsets.UTF_8))
                ?.groupValues?.get(1)?.toLong() ?: 0L
        } catch (_: ObjectNotFoundException) {
            0L
        }
    }

    /**
     * 推进 GC 水位。
     *
     * 水位取「当前时刻 - 安全窗口」对应的 TSO。这样任何在窗口内开始的读事务
     * 都不会被回收影响——**这正是安全窗口存在的意义**。
     */
    suspend fun advanceWatermark(nowMillis: Long = System.currentTimeMillis()): Long = mutex.withLock {
        val wall = nowMillis - safetyWindowMillis
        val ts = wall shl TSO_LOGICAL_BITS
        store.put(ObjectKeys.gcWatermark(), """{"safeReadTs":$ts,"wallMillis":$wall}""".toByteArray())
        // 水位滞后（yb_gc_watermark_lag_seconds）= 现在 - 水位对应时刻。
        // 它同时也是「历史版本还能保留多久」的度量：滞后越大，越早的历史已被回收，
        // 时间旅行能回到的过去就越近。这里只记内存值，绝不为了指标去读对象（P4）。
        Metrics.gcWatermarkMillis.set(wall)
        Audit.record(
            AuditEvent(
                ts = nowMillis,
                action = AuditActions.GC_ADVANCE_WATERMARK,
                target = "gc/watermark",
                detail = mapOf("safeReadTs" to ts.toString(), "wallMillis" to wall.toString()),
            )
        )
        ts
    }

    // ── 阶段 1：标记 ──────────────────────────────────────────────

    /** 一次标记的结果。 */
    data class MarkResult(
        val shard: String,
        /** 不可达且已超过安全窗口的对象键。 */
        val garbage: List<String>,
        /** 被 fence 的旧写者留下的 WAL——正常现象，单独计数（§6.9）。 */
        val fencedWal: List<String>,
        /** 当前仍被 manifest 引用的对象数，用来判断「一次 GC 是否异常地多」。 */
        val reachable: Int,
        /** 分片根下**无法归类**的不可达对象数。非零即疑似泄漏（设计文档 §6.9 的第 3 类）。 */
        val orphans: Int = 0,
    ) {
        val totalGarbage: Int get() = garbage.size + fencedWal.size
    }

    /**
     * 这个对象键是否属于分片根下**已知**的几类。
     *
     * 已知：`sst/L{n}/...`、`wal/e{n}/...`、`manifest/...`、`state.json`。
     * 其余一律算「不认识」——那才是需要人去查的东西。
     */
    private fun isKnownShardObject(key: String): Boolean =
        key.contains("/sst/") || key.contains("/wal/") || key.contains("/manifest/") ||
            key.endsWith("/state.json")

    /**
     * **被删表遗留的分片**：`DROP TABLE` / `DROP DATABASE` 之后，数据对象还留在桶里。
     *
     * ## 为什么单列一个方法
     *
     * GC 的遍历集合一直是 `catalog.allTables()`（只含**活表**），于是被删表的分片
     * **根本不在 GC 的视野里**——它的 SST/WAL 永远不会进入任何清单，永远不回收。
     * 而"删表不回收"在一个以"消解固定存储成本"为存在理由的系统里是最贵的失效模式：
     * 用户以为删掉的数据不再付钱，实际上一直在付。
     *
     * 墓碑里记着 `droppedAt`；超过保留期（默认与安全窗口一致，30 天）之后，
     * 那些分片下的对象就是**确定的可回收物**——没有读者可能引用它们
     * （表已经不存在，没有任何路径能生成指向它们的 read_ts）。
     *
     * @return 每个被删表的分片键 → 它的对象键（交由调用方写进回收清单）
     */
    suspend fun droppedTableObjects(retentionMillis: Long = 30L * 24 * 60 * 60 * 1000): Map<String, List<String>> {
        val now = System.currentTimeMillis()
        val out = LinkedHashMap<String, List<String>>()
        for (tombKey in store.listAll(ObjectKeys.tablePrefixAll())) {
            if (!tombKey.endsWith("/DELETED.json")) continue
            val text = runCatching { String(store.get(tombKey), Charsets.UTF_8) }.getOrNull() ?: continue
            val droppedAt = Regex("\"droppedAt\"\\s*:\\s*(\\d+)").find(text)?.groupValues?.get(1)?.toLongOrNull()
                ?: continue
            if (now - droppedAt < retentionMillis) continue          // 还在保留期内：可恢复
            val tableId = tombKey.removePrefix(ObjectKeys.tablePrefixAll())
                .substringBefore('/').toLongOrNull() ?: continue
            // 分片根形如 `shard/{tableId}/{partition}/...`；这里按前缀列举每个分片
            val root = ObjectKeys.tablePrefix(com.cloudea.yangbase.common.TableId(tableId)) + "shard/"
            val keys = store.listAll(root)
            if (keys.isNotEmpty()) out[root] = keys
        }
        return out
    }

    /**
     * **MS 的 Raft 日志对象**：快照边界之下的那些永远不会再被读。
     *
     * `RaftNode.loadLogAfterSnapshot` 只加载 `index > snapshotLastIndex` 的条目，
     * 所以边界之下的是**确定的死重量**——而它每条日志一个对象、永不删除，
     * 按租约续期与 TSO 预留的频率算，规模是每小时上万条。
     *
     * @return 可以删除的日志对象键（快照边界之下的）
     */
    suspend fun msLogObjects(): List<String> {
        val out = ArrayList<String>()
        for (nodeKey in store.listAll("meta/ms/snapshot/")) {
            val text = runCatching { String(store.get(nodeKey), Charsets.UTF_8) }.getOrNull() ?: continue
            val lastIncluded = Regex("\"lastIncludedIndex\"\\s*:\\s*(\\d+)")
                .find(text)?.groupValues?.get(1)?.toLongOrNull() ?: continue
            val nodeId = nodeKey.removePrefix("meta/ms/snapshot/").removeSuffix(".json")
            for (k in store.listAll("meta/ms/log/$nodeId/")) {
                val idx = k.substringAfterLast('/').removeSuffix(".json").toLongOrNull() ?: continue
                if (idx <= lastIncluded) out.add(k)
            }
        }
        return out
    }

    /**
     * 标记一个分片里的不可达对象。
     *
     * **完全不做任何删除**——这是标记阶段，写清单是下一步。
     */
    suspend fun mark(table: TableSchema, shard: ShardId, nowMillis: Long = System.currentTimeMillis()): MarkResult {
        val manifest = loadManifest(shard)
        val cutoff = nowMillis - safetyWindowMillis

        // 可达集合：当前 manifest 引用的全部 SST + manifest 自身 + 状态对象
        val reachable = HashSet<String>()
        reachable += ObjectKeys.manifestCurrent(shard)
        for (lvl in manifest.levels.indices) {
            for (ref in manifest.level(lvl)) {
                reachable += ObjectKeys.sstData(shard, lvl, com.cloudea.yangbase.common.FileId(ref.fileId))
                reachable += ObjectKeys.sstIndex(shard, lvl, com.cloudea.yangbase.common.FileId(ref.fileId))
            }
        }
        reachable += ObjectKeys.shardState(shard)

        val unreachable = ArrayList<String>()
        val fenced = ArrayList<String>()
        for (key in store.listAll(ObjectKeys.shardRoot(shard))) {
            if (key in reachable) continue
            if (key.startsWith(ObjectKeys.manifestPrefix(shard))) continue   // manifest 历史很小，保留

            // WAL 分两类：当前 epoch 里「尚未 flush」的是**活的**，必须留；
            // 旧 epoch 的一定是被 fence 的旧写者留下的，可以回收。
            if (key.startsWith(ObjectKeys.walAllEpochsPrefix(shard))) {
                val epoch = epochOf(key) ?: continue
                if (epoch != manifest.walEpoch) {
                    if (olderThan(key, cutoff)) fenced.add(key)
                    continue
                }
                val seq = seqOf(key) ?: continue
                // ★ 按**覆盖区间的上界**判断，而不是起始序号：段对象 `s-1-128` 可能
                // 覆盖 1..128，只看 from 会以为它整段都已 flush。生产中段只由已 flush
                // 的对象构成，所以两者等价——但 GC 是唯一真正删数据的地方，
                // 它不该依赖别的模块的隐式前提：判错时这里只该少删，绝不该多删
                val through = WalCodec.segmentRangeOfKey(key)?.second ?: seq
                // 已 flush 且超过安全窗口的 WAL 才能删——窗口内可能还有读者在重放它
                if (through <= manifest.walFlushedThroughSeq && olderThan(key, cutoff)) unreachable.add(key)
                continue
            }

            if (olderThan(key, cutoff)) unreachable.add(key)
        }

        // 真正的泄漏与「正常残留」必须分开（设计文档 §6.9 的三个来源）：
        //   - compaction 残留、被 fence 的旧写者 WAL：**预期之内**，是 GC 的日常目标
        //   - 分片根下**不认识的对象**：既不是 SST、也不是 WAL 或 manifest，
        //     那就是没人认领的东西——非零即 bug，必须能被看见
        var orphans = 0
        for (k in unreachable) {
            if (!isKnownShardObject(k)) {
                orphans++
                Metrics.orphanObjects.inc()
                System.err.println("[yb-gc] 发现无法归类的不可达对象（疑似泄漏）: $k")
            }
        }

        return MarkResult(
            shard = shard.toString(),
            garbage = unreachable.sorted(),
            fencedWal = fenced.sorted(),
            reachable = reachable.size,
            orphans = orphans,
        )
    }

    /** 把一次标记的清单落到对象存储。清单本身也是对象——它可以被审计。 */
    suspend fun writeManifest(nowMillis: Long, results: List<MarkResult>): String? {
        val garbage = results.flatMap { it.garbage + it.fencedWal }
        if (garbage.isEmpty()) return null
        val key = ObjectKeys.garbageManifest(nowMillis)
        val json = buildString {
            append("""{"createdAt":$nowMillis,"objects":[""")
            append(garbage.joinToString(",") { "\"$it\"" })
            append("]}")
        }
        store.put(key, json.toByteArray())
        // 「待回收对象数」= 已标记但还在安全窗口内、尚未删除的对象数（§12.5）
        Metrics.garbagePending.set(garbage.size.toLong())
        Audit.record(
            AuditEvent(
                ts = nowMillis,
                action = AuditActions.GC_MARK,
                target = key,
                detail = mapOf(
                    "objects" to garbage.size.toString(),
                    "shards" to results.size.toString(),
                    "fencedWal" to results.sumOf { it.fencedWal.size }.toString(),
                    "orphans" to results.sumOf { it.orphans }.toString(),
                ),
            )
        )
        return key
    }

    // ── 阶段 2：删除 ──────────────────────────────────────────────

    /**
     * 删除**超过安全窗口**的清单里列出的对象。
     *
     * 逐对象删除，失败了就跳过——留下给下一轮，而不是让整轮 GC 失败。
     * 一个删不掉的对象不值得阻塞其它几千个。
     */
    suspend fun sweep(nowMillis: Long = System.currentTimeMillis()): SweepResult {
        val cutoff = nowMillis - safetyWindowMillis
        var deleted = 0
        var failed = 0
        var manifests = 0

        val sweptManifests = ArrayList<String>()
        for (key in store.listAll(ObjectKeys.garbagePrefix())) {
            // 清单的命名里带创建时间（零填充，LIST 字典序即时间序）——
            // 这正是 CLAUDE.md 强调「对象名零填充是正确性要求」的一个实例
            val created = createdMillisOf(key)
            if (created == null || created > cutoff) continue      // 还在安全窗口内
            manifests++

            val text = try {
                String(store.get(key), Charsets.UTF_8)
            } catch (_: ObjectNotFoundException) {
                continue
            }
            for (obj in extractObjects(text)) {
                try {
                    store.delete(obj)
                    deleted++
                } catch (_: Exception) {
                    failed++
                }
            }
            sweptManifests.add(key)
            runCatching { store.delete(key) }
        }
        // GC 删除是**不可逆**的，因此它是最需要审计的动作（设计文档 §12.5）：
        // 谁在什么时候删掉了多少对象，必须能从对象存储里查回来。
        // 注意审计记录里只放计数与清单键，不放每个被删对象的键——
        // 清单对象本身就是那份明细，审计引用它就够了。
        if (manifests > 0) {
            Audit.record(
                AuditEvent(
                    ts = nowMillis,
                    action = AuditActions.GC_SWEEP_DELETE,
                    target = sweptManifests.joinToString(","),
                    detail = mapOf(
                        "manifests" to manifests.toString(),
                        "deleted" to deleted.toString(),
                        "failed" to failed.toString(),
                    ),
                )
            )
            // 清单已被消费，待回收数归零
            Metrics.garbagePending.set(0)
        }
        return SweepResult(manifests = manifests, deleted = deleted, failed = failed)
    }

    data class SweepResult(val manifests: Int, val deleted: Int, val failed: Int)

    // ── 辅助 ──────────────────────────────────────────────────────

    private suspend fun loadManifest(shard: ShardId): Manifest =
        ManifestStore(store).load(shard)?.first
            ?: Manifest.empty(com.cloudea.yangbase.common.Epoch(0), "gc")

    /**
     * 对象是否在 cutoff 之前就已存在。
     *
     * 取不到元信息时返回 **false**（=不删）——GC 的默认答案永远是「保守地留着」。
     */
    private suspend fun olderThan(key: String, cutoffMillis: Long): Boolean {
        val meta = runCatching { store.head(key) }.getOrNull() ?: return false
        return meta.lastModifiedMillis <= cutoffMillis
    }

    private fun epochOf(key: String): Long? =
        Regex("/wal/e(\\d+)/").find(key)?.groupValues?.get(1)?.toLongOrNull()

    /**
     * WAL 键 → 它覆盖的起始序号。
     *
     * ⚠️ **必须走 `WalCodec.seqOfKey`，不能在这里另写一份解析。**
     * 这里曾经是 `substringAfterLast('/').substringBefore('.')`，它对普通对象
     * （`{seq}.log`）成立，而**段对象**（`s-{from}-{to}.log`）解析出来是 null——
     * 于是 `?: continue` 把段整个跳过去：既不算活、也不算垃圾，**永远留在桶里**。
     * 这正是本项目反复强调的那类缺陷：不报错、只是慢慢变贵。
     */
    private fun seqOf(key: String): Long? =
        WalCodec.seqOfKey(key)

    private val createdMillisRegex = Regex("/garbage/(\\d{20})/")
    private fun createdMillisOf(key: String): Long? =
        createdMillisRegex.find(key)?.groupValues?.get(1)?.toLongOrNull()

    private val objectKeyRegex = Regex("\"([^\"]+)\"")
    private fun extractObjects(json: String): List<String> {
        val after = json.substringAfter("\"objects\":[", "")
        if (after.isEmpty()) return emptyList()
        return objectKeyRegex.findAll(after.substringBeforeLast(']')).map { it.groupValues[1] }.toList()
    }
}

/**
 * 时间旅行（设计文档 §4.2）。
 *
 * 时间戳的高位编码了物理毫秒，因此「回到某个时刻」是一次纯粹的位运算，
 * 不需要额外的时间索引。这是把物理时间放进 TSO 的直接回报。
 */
object TimeTravel {

    /** 把墙上时钟换成读时间戳。 */
    fun readTsFor(millis: Long): Long = millis shl TSO_LOGICAL_BITS

    fun millisOf(readTs: Long): Long = readTs ushr TSO_LOGICAL_BITS

    /**
     * 解析 `AS OF` 的目标。
     *
     * 两种写法都支持：
     *  - `AS OF TIMESTAMP '2026-01-01 10:00:00'` —— 人写的
     *  - `AS OF TSO 123456789` —— 从日志/监控里拿到确切时间戳时用，无歧义
     */
    fun parse(kind: String, text: String): Long {
        val k = kind.uppercase()
        if (k == "TSO") {
            return text.trim().toLongOrNull() ?: throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000", "AS OF TSO 需要一个整数时间戳，收到 '$text'"
            )
        }
        val millis = com.cloudea.yangbase.common.Datum.parseDateTime(text)
            ?: throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "无法解析的时间点 '$text'")
        return readTsFor(millis)
    }

    /**
     * 校验时间点是否落在 GC 水位之上。
     *
     * **必须报错而不是返回残缺结果**：早于水位的历史已经被回收，
     * 那部分版本链是断的。返回一个「看起来正常」的结果会让使用者
     * 以为那个时刻的数据就是那样——这是最难发现也最危险的一类错误。
     */
    fun requireAboveWatermark(readTs: Long, safeReadTs: Long) {
        if (safeReadTs > 0 && readTs < safeReadTs) {
            val wanted = java.time.Instant.ofEpochMilli(millisOf(readTs))
            val safe = java.time.Instant.ofEpochMilli(millisOf(safeReadTs))
            throw SqlException(
                MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                "时间点 $wanted 早于 GC 水位 $safe，那部分历史已被回收。" +
                    "可查询的最早时间点是 $safe（安全窗口可通过 gc_safety_window 调整）。",
            )
        }
    }
}

/** TSO 的逻辑位宽，与 [PersistentTso] 共用。 */
const val TSO_LOGICAL_BITS = 18
