package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.Epoch
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.Precondition
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

/** manifest 中一个数据文件的引用。刻意保持小——它每次查询都要被读。 */
@Serializable
data class SstFileRef(
    val fileId: String,
    val level: Int,
    val size: Long,
    val rows: Long,
    val tombstones: Long,
    val minKey: ByteArray,
    val maxKey: ByteArray,
    val minTs: Long,
    val maxTs: Long,
    val deduplicated: Boolean = false,
) {
    fun overlaps(lo: ByteArray, hi: ByteArray): Boolean =
        ByteArrayComparator.compare(maxKey, lo) >= 0 && ByteArrayComparator.compare(minKey, hi) <= 0
}

/**
 * 分片的清单（设计文档 §6.5）。
 *
 * 两级结构：
 * ```
 * manifest/CURRENT  ← 唯一可变的根指针（用 If-Match 保护）
 * manifest/M-*.mf   ← 全量快照（L1+ 的文件列表）
 * manifest/d-*.mf   ← 增量（L0 的变化）
 * ```
 * 当前实现把快照与增量合并成「一个对象 + CURRENT 指针」，
 * 增量拆分留待 L0 文件数变大后再引入（见 §6.5 的演进路径）。
 *
 * ⚠️ **本对象每次查询都要读**，必须保持小。L1+ 的文件数由 leveled compaction 约束。
 */
@Serializable
data class Manifest(
    val formatVersion: Int = 1,
    val writerEpoch: Long,
    val writerNode: String,
    val seq: Long,
    /** WAL 的 epoch。旧写者的对象落在旧 epoch 前缀下，天然被忽略（fencing 第一道防线）。 */
    val walEpoch: Long,
    /** seq ≤ 该值的 WAL 对象已被 flush 进 SST，恢复时可以跳过。 */
    val walFlushedThroughSeq: Long,
    /** levels[i] = 第 i 层的文件列表。 */
    val levels: List<List<SstFileRef>>,
    val schemaVersion: Long,
    val commitTsWatermark: Long,
    /**
     * `rowCountAtTs` 时刻的**精确**可见行数（设计文档 §8.9.1）。
     * 这是 `SELECT COUNT(*)` 做到 0 次 GET 的主要机制——由单写者在写入时维护。
     * 若未启用 exact_count，值为 -1，计数必须回退到扫描。
     */
    val rowCountExact: Long = -1,
    val rowCountAtTs: Long = -1,
    val updatedAt: Long = System.currentTimeMillis(),
) {
    fun level(level: Int): List<SstFileRef> = levels.getOrElse(level) { emptyList() }

    fun allFiles(): List<SstFileRef> = levels.flatten()

    val totalRows: Long get() = levels.flatten().sumOf { it.rows }

    fun withLevels(newLevels: List<List<SstFileRef>>): Manifest = copy(levels = newLevels, seq = seq + 1)

    companion object {
        fun empty(epoch: Epoch, node: String): Manifest = Manifest(
            writerEpoch = epoch.value,
            writerNode = node,
            seq = 0,
            walEpoch = epoch.value,
            walFlushedThroughSeq = -1,
            levels = List(MAX_LEVELS) { emptyList() },
            schemaVersion = 0,
            commitTsWatermark = 0,
            // 全新分片的精确行数是已知的 0，而不是「未知的 -1」——
            // 否则第一次 COUNT 会白白回退到扫描
            rowCountExact = 0,
            rowCountAtTs = 0,
        )

        const val MAX_LEVELS = 7
    }
}

/** CURRENT 指针对象的内容。刻意极小。 */
@Serializable
data class ManifestPointer(
    val seq: Long,
    val path: String,
    /** 写者代际。用于 fencing：值不匹配说明已被新写者取代。 */
    val writerEpoch: Long,
)

/**
 * manifest 的读写。
 *
 * 写路径用 `If-Match` 保证 CAS：切换 CURRENT 时如果 etag 已变，
 * 说明有更新的写者存在，本次写入必须失败（fencing 第二道防线）。
 */
class ManifestStore(
    private val store: ObjectStore,
    /**
     * 允许「指针切换没有原子性保证」的后端继续工作（阿里云 OSS 这一类）。
     *
     * 默认 **false**：没有 ETag CAS 的后端上，[commit] 会直接抛
     * `CapabilityNotSupportedException`——宁可拒绝服务，也不要静默地失去互斥（P6）。
     * 置为 true 表示**显式接受**下面这个弱化（设计文档 §3.3 有完整论证）：
     *
     *  - 切指针退化为「无前置条件的 PUT + **写后回读校验**」；
     *  - 因此理论上存在一个极窄的窗口：两个写者的 PUT 交错，后者覆盖前者，
     *    而前者在校验时**还没被覆盖**（于是它以为提交成功）。
     *
     * **为什么这个弱化是可接受的**：被覆盖掉的那次提交丢失的是**SST 引用**，
     * 不是数据。不变式是「任何已确认的写入，要么在 current manifest 引用的
     * 某个 SST 里，要么在 current manifest 的水位之上的 WAL 里」——
     * 指针被覆盖时水位**一起**回退了，于是那批 WAL 不会被 GC 删除，
     * 恢复时会被重放。代价是恢复变慢、以及那批 SST 可能被当成孤儿回收（白写一次）。
     *
     * 真正的防线仍然是**租约 + epoch**：旧写者的提交仍会被 [commit] 开头的
     * 代际检查挡掉，只是这个检查不再与写入原子。
     */
    private val allowNonAtomicPointerSwitch: Boolean = false,
) {

    private val json = Json { prettyPrint = false; encodeDefaults = true; ignoreUnknownKeys = true }

    /**
     * 读取当前 manifest。
     *
     * 缓存策略见 §6.5：稳态下只做一次 `head` 校验（限频），
     * 未变化则复用内存中的视图——**这就是「0 次 GET」的那条路径**。
     */
    suspend fun load(shard: ShardId): Pair<Manifest, ETag>? {
        val curKey = ObjectKeys.manifestCurrent(shard)
        val meta = try {
            store.head(curKey)
        } catch (_: ObjectNotFoundException) {
            return null
        }
        // ⚡ **指针没变就直接复用**：只读分片的每一次读都会走这里，
        // 而稳态下指针几乎从不变化——那三次无条件 GET（CURRENT + manifest 本体）
        // 完全可以被一次 head 挡住。
        // 判据是 (etag, path)：etag 相同意味着 CURRENT 对象一字未改，
        // 于是它指向的 manifest 本体也一定还是那一份（本体是不可变的）
        cachedPointer?.let { (etag, ptr, mf) ->
            if (etag == meta.etag) return mf to meta.etag
        }
        val ptrBytes = store.get(curKey)
        val ptr = json.decodeFromString(ManifestPointer.serializer(), String(ptrBytes, Charsets.UTF_8))
        val mfBytes = store.get(ptr.path)
        val mf = json.decodeFromString(Manifest.serializer(), String(mfBytes, Charsets.UTF_8))
        cachedPointer = Triple(meta.etag, ptr, mf)
        return mf to meta.etag
    }

    /** 上一次 `load()` 的结果：`(CURRENT 的 etag, 指针, 全量 manifest)`。 */
    private var cachedPointer: Triple<ETag, ManifestPointer, Manifest>? = null

    /** 提交之后必须让缓存失效——否则本进程会看到自己刚覆盖掉的那一份。 */
    private fun invalidateCache() {
        cachedPointer = null
    }

    /** 仅读 CURRENT 指针（不读全量 manifest），用于低成本的存在性检查。 */
    suspend fun loadPointer(shard: ShardId): ManifestPointer? {
        val curKey = ObjectKeys.manifestCurrent(shard)
        return try {
            val bytes = store.get(curKey)
            json.decodeFromString(ManifestPointer.serializer(), String(bytes, Charsets.UTF_8))
        } catch (_: ObjectNotFoundException) {
            null
        }
    }

    /**
     * 原子切换 CURRENT 指针。
     *
     * @param expectedEtag 为 null 表示「必须不存在」（首次创建）；非 null 则做 CAS。
     *
     * @throws ConcurrentModificationException 指针已被别人改过（CAS 失败），或
     *   **当前指针里的写者代际比自己新**（fencing —— 见下）
     */
    suspend fun commit(shard: ShardId, manifest: Manifest, expectedEtag: ETag?): ETag {
        val curKey = ObjectKeys.manifestCurrent(shard)

        // ★ **fencing：提交前先看指针里记的是谁的代际**。
        //
        // 只用 eTag 做 CAS 是不够的：一个**已经失去租约但进程还在跑**的旧写者，
        // 如果新写者还没提交过（eTag 没变），它的这次提交**会成功**——
        // 于是它把自己那份陈旧 manifest 写回 CURRENT，新写者已经 flush 的 SST
        // 变成无人引用（随后被 GC 当真垃圾回收，那是真丢数据），
        // 而新写者下一次提交拿到 CAS 失败，按指示"停止写入"——
        // **合法写者被自己的防线踢下线**。`ManifestPointer.writerEpoch` 的注释
        // 一直写着"用于 fencing"，但这里从来没比较过它。
        //
        // 代际单调：新写者的 epoch 一定**大于**旧的（见 `LeaseManager` 的 epoch 分配），
        // 所以"指针里的 epoch 更大"就足以判定自己已被取代
        val existing = loadPointer(shard)
        if (existing != null && existing.writerEpoch > manifest.writerEpoch) {
            throw ConcurrentModificationException(
                "分片 $shard 的 manifest 已由更代际的写者（epoch=${existing.writerEpoch}）接管，" +
                    "本节点（epoch=${manifest.writerEpoch}）的提交被拒绝（fencing）",
            )
        }

        // snapshot 对象按 **(seq, epoch)** 命名：`seq` 在三处会被复用而不自增
        // （启动写者、快照恢复、接管），只用 seq 命名会让两次不同的提交
        // **写到同一个对象**——那时 CURRENT 指向的内容可能是另一个写者写的
        // （指针与内容不是一次原子写），而时间旅行按 `M-{seq}.mf` 反查历史时刻
        // 也会取到不对应的状态
        val mfPath = ObjectKeys.manifestSnapshot(shard, manifest.seq, manifest.writerEpoch)

        // 先写不可变的全量 manifest，再切指针——顺序不能反
        store.put(mfPath, json.encodeToString(manifest).toByteArray(Charsets.UTF_8))

        val ptr = ManifestPointer(manifest.seq, mfPath, manifest.writerEpoch)
        invalidateCache()
        val ptrBytes = json.encodeToString(ptr).toByteArray(Charsets.UTF_8)
        val pre = if (expectedEtag == null) Precondition.MustNotExist else Precondition.MustMatch(expectedEtag)

        // ── 弱化路径：后端没有 ETag CAS ──────────────────────────────
        //
        // 只在**显式开启**时走，且用「写后回读」把绝大多数交错变成一次
        // 可重试的冲突（见类注释里的风险说明与不变式论证）
        if (expectedEtag != null && !store.caps.conditionalUpdate) {
            if (!allowNonAtomicPointerSwitch) {
                throw com.cloudea.yangbase.common.CapabilityNotSupportedException(
                    "后端不支持 If-Match（ETag CAS），无法原子切换 manifest 指针：" +
                        "分片 $shard。若确认要在这种后端上运行（单写者、并接受" +
                        "指针切换无原子性），请显式打开 allowNonAtomicPointerSwitch",
                )
            }
            store.put(curKey, ptrBytes, null)
            // 回读校验：如果ours不是当前值，说明有人在我们之间提交了
            val back = runCatching { store.get(curKey) }.getOrNull()
            val mine = back != null && String(back, Charsets.UTF_8) == String(ptrBytes, Charsets.UTF_8)
            if (!mine) {
                Metrics.onManifestPointerRaced()
                throw ConcurrentModificationException(
                    "分片 $shard 的 manifest 指针在提交期间被别人覆盖（后端不支持 CAS，" +
                        "已按 allowNonAtomicPointerSwitch 降级）：本节点的提交未生效",
                )
            }
            return ETag("") // 无 CAS 的后端上 eTag 不参与后续比较
        }

        return try {
            store.put(curKey, ptrBytes, pre)
        } catch (e: ConcurrentModificationException) {
            // 已被新写者取代 —— fencing 生效，调用方必须停止写入
            throw e
        }
    }

    /**
     * **接管审计**：找出「已经写全、但没有成为 current」的 manifest 快照。
     *
     * 两种来源：提交失败（写快照成功、切指针失败——良性的，调用方已经知道了）
     * 与**弱化模式下的指针被覆盖**（那次提交在对方看来是成功的）。
     * 两者的区别只有写者自己知道，所以这里只做一件事：**把它报出来**。
     * 在一个"指针切换不原子"的后端上，这是把「静默」变成「可发现」的那一步。
     *
     * 只在弱化模式开启时才调用（正常情况下它是纯粹的请求浪费，P4）。
     */
    suspend fun auditOrphanSnapshots(shard: ShardId): List<String> {
        val cur = loadPointer(shard) ?: return emptyList()
        val prefix = ObjectKeys.manifestSnapshotPrefix(shard)
        return store.listAll(prefix).filter { key ->
            val seq = ObjectKeys.snapshotSeqOf(key) ?: return@filter false
            seq > cur.seq
        }
    }
}
