package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.Epoch
import com.cloudea.yangbase.common.FileId
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.objectstore.ObjectStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicLong

/**
 * 一个分片的 LSM 存储引擎（设计文档 §6）。
 *
 * 它把所有部件串起来：MemTable → WAL（组提交）→ SST（`.idx`/`.dat` 分离）
 * → Manifest（不可变 + 指针切换）。
 *
 * 所有写操作都经过 [write]，它做三件事：
 *  1. 判断每个 op 是「插入」还是「覆盖/删除」，用来维护**精确行数**（§8.9.1）
 *  2. 提交到 WAL（组提交保证 RPO=0）
 *  3. 应用到 MemTable
 */
class LsmStore(
    private val store: ObjectStore,
    val shard: ShardId,
    val schema: TableSchema,
    private val cache: BlockCache,
    private val nodeId: String,
    private val scope: CoroutineScope,
    private val commitMode: CommitMode = CommitMode.SYNC,
    private val exactCount: Boolean = true,
    /**
     * 跳数索引的装配钩子（设计文档 §8.5）。
     *
     * 每次 flush 需要**新的一批**观察者（每个 SST 一份统计量），因此传的是工厂
     * 而不是实例。返回空列表表示这个表没有跳数索引——此时整个机制零开销。
     */
    private val skipObservers: (TableSchema) -> List<SstBlockObserver> = { emptyList() },
    /**
     * 块级剪枝器工厂。参数是**本次扫描**的列约束——剪枝器与约束一一对应，
     * 不能跨扫描复用（约束不同，能剪的块也不同）。
     */
    private val skipSkipper: ((TableSchema, List<ColumnRange>) -> BlockSkipper?)? = null,
    private val memtableCapacity: Long = 32L * 1024 * 1024,
    private val blockSize: Int = 64 * 1024,
    /**
     * 是否对 SST 数据块做块级压缩（设计文档 §6.4，默认开启）。
     *
     * 关掉的两个正当理由：数据本身已经压过（图片、归档文本），再压只是白烧 CPU；
     * 排查格式问题时需要「所见即所得」。**这是写侧开关**——读侧没有开关，
     * 一份文件压没压由它自己的格式决定（P2）。因此一个 `compress=true` 的
     * 分片与一个 `compress=false` 的分片能互相读对方的文件。
     */
    private val compress: Boolean = true,
    /** 需要收集「值→计数」的列下标，用于 §8.9.4 的 0-GET 谓词计数。 */
    private val countColumns: Set<Int> = emptySet(),
    /** L0 文件数达到该值时触发 compaction（设计文档 §6.6 建议 4）。 */
    private val l0CompactionTrigger: Int = 4,
    /**
     * 写者租约的提供者（设计文档 §4.3）。
     *
     * 默认是 [SingleProcessLease]——它**不是"没有协调"，而是"协调由部署方式保证"**：
     * 它假设同一分片只有一个进程在写。多 CN 部署必须换成基于对象存储 CAS 的实现
     * （§4.7 的 `ObjectStoreCoordination` 模式）。
     */
    private val leaseProvider: LeaseProvider = SingleProcessLease,
    /**
     * 允许在**没有 ETag CAS** 的对象存储上做 manifest 指针切换（阿里云 OSS 这一类）。
     *
     * 默认 false：这种后端上分片会在第一次提交时明确报错。置为 true 表示
     * 显式接受「指针切换不原子」的弱化——风险与不变式见
     * `ManifestStore.allowNonAtomicPointerSwitch` 的注释。
     */
    private val allowNonAtomicPointerSwitch: Boolean = false,
    /**
     * 回收水位：**早于它的旧版本可以被物理丢弃**（设计文档 §6.6）。
     *
     * 在此之前 compaction 恒传 `protectReadTs = 0`，那个条件「保留 ts >= 0 的版本」
     * 恒真 ⇒ **所有历史版本都被保留下来**，存储只增不减。
     * 对一个「为省钱而存在」的系统，这是最讽刺的一类缺陷。
     *
     * 默认返回 0 表示**不回收**（保守）。生产上应当接上 `GarbageCollector`
     * 的安全水位：任何比它更老的版本都不可能有活跃读者了。
     */
    private val gcWatermark: suspend () -> Long = { 0L },
    /**
     * 日志段合并的触发阈值（设计文档 §6.3.2）。
     *
     * 已 flush 的 WAL 对象恢复时**永远不会再被读**（`readWalObjects` 按
     * `walFlushedThroughSeq` 过滤掉了），但它们仍然躺在前缀里，
     * 让恢复路径的 LIST **随写入量线性增长**——而 LIST 是最该避开的慢操作
     * （CLAUDE.md 的反模式清单第一条）。
     *
     * 超过这个数量就把它们合并成一个段对象并删掉原件。
     */
    private val walSegmentTrigger: Int = 128,
) {
    private val manifestStore = ManifestStore(store, allowNonAtomicPointerSwitch)
    private val sstReader = SstReader(store, cache)

    private val writeMutex = Mutex()

    /**
     * 保护「应用已提交的变更」与 flush。
     *
     * 与 [writeMutex] 分开是**必需的**：写入路径绝不能在等待 WAL 提交时持有锁，
     * 否则并发写者被串行化，组提交的攒批窗口里永远只有一笔事务
     * （基准测试暴露的真实问题：1000 个事务产生 1000 次 PUT，摊薄倍数 1.0）。
     */
    private val applyMutex = Mutex()
    private var memtable = MemTable(memtableCapacity)
    private lateinit var manifest: Manifest
    private var currentEtag: ETag? = null
    private var wal: WalWriter? = null
    private var walJob: Job? = null

    /** 本分片的写者租约。非 null 表示我们确实是当前的写者。 */
    @Volatile
    private var lease: ShardLease? = null

    /**
     * 本节点**不是**这个分片的写者（没拿到租约）。
     *
     * 读照常服务；写会先尝试抢一次租约，仍拿不到就明确报错（§4.3：同一分片任一时刻
     * 只能有一个写者）。这不是降级，而是多 CN 部署下的正式形态。
     */
    var readOnlyShard: Boolean = false
        private set

    /** 只读分片已经重放到的 WAL 序号（增量重放用）。 */
    private var replayedThroughSeq: Long = -1

    /** 上次重放时 manifest 的落盘边界（变了就重建视图）。 */
    private var replayedFlushedThrough: Long = -1
    private var leaseJob: Job? = null

    /** 租约是否已经失效（被别人抢走或续期失败）。失效后写入必须停下。 */
    @Volatile
    private var leaseLost: Boolean = false

    /** 已加载的 `.idx` 句柄。**常驻内存**——这是「点查不读索引」的前提（§6.7 L3）。 */
    private val handles = java.util.concurrent.ConcurrentHashMap<String, SstHandle>()

    private val nextFileId = AtomicLong(1)
    private val flushes = AtomicLong()
    private val compactions = AtomicLong()

    val memtableSize: Long get() = memtable.approximateSize
    var opened: Boolean = false
        private set

    // ── 打开与恢复 ────────────────────────────────────────────────

    /**
     * 打开分片。若存在未 flush 的 WAL，重放它——这就是崩溃恢复（设计文档 §6.8）。
     */
    suspend fun open(): Unit = writeMutex.withLock {
        if (opened) return@withLock
        // ★ 先取租约，**再**读 manifest——但**拿不到租约不再是致命错误**。
        //
        // 写者身份是租约不是节点属性（原则 P5），所以"取不到租约"只说明
        // **本节点不是这个分片的写者**，它仍然可以读：多 CN 部署下，
        // 任何一个 CN 都要能服务任意分片的读（否则每个查询都得路由到分片属主，
        // 那正是无状态计算要避免的事）。
        //
        // 曾经的实现是"取不到租约就抛异常"，后果是**第二个 CN 连读都读不了**——
        // 表现为 `SELECT` 报一句"租约被 cn-1 持有"，看起来像写冲突，实际是读被打死。
        // 这个缺陷是"多 CN 端到端"那条用例第一次跑就抓出来的。
        lease = runCatching { leaseProvider.acquire(shard) }.getOrNull()
        readOnlyShard = lease == null
        leaseLost = false

        val loaded = manifestStore.load(shard)
        if (loaded == null) {
            // 分片从没被写过。**只读模式下不创建 manifest**：创建它就是宣布
            // "我是写者"，而我们要么没有租约，要么（单进程模式）租约恒在手上。
            // 只读时给一个空视图，读会得到空集——与"这张表还没有数据"一致
            manifest = Manifest.empty(Epoch(lease?.epoch ?: 0), nodeId)
            if (readOnlyShard) {
                opened = true
                return@withLock
            }
            startWriter(manifest.walEpoch)
            currentEtag = manifestStore.commit(shard, manifest, null)
            preloadIndexes()
            startLeaseRenewal()
            opened = true
            return@withLock
        }
        manifest = loaded.first
        currentEtag = loaded.second
        // ⚠️ 取**两者中较大的那个**：
        //  - 租约的 epoch 保证「换了写者就换前缀」，fencing 生效
        //  - manifest 里的 walEpoch 保证「RESTORE/PITR 主动抬高的 epoch」被尊重
        //    （恢复会把 walEpoch +1，让恢复点之后写下的 WAL 不再被重放）
        // 只取租约那个的话，单进程模式（epoch 恒为 1）下 RESTORE 会失效——
        // 恢复点之后的 WAL 会被重放，数据又被覆盖回去。
        val myEpoch = maxOf(lease?.epoch ?: manifest.walEpoch, manifest.walEpoch)

        // 只读分片：不启动写者、不续租。但**必须重放 WAL**——
        // 写者尚未 flush 的数据只存在于 `wal/e{epoch}/` 里，而那是**已提交**的数据
        // （WAL 对象的 PUT 成功就是提交点）。不重放的话，读到的会是"属主还没写"的
        // 假象：查询照常返回、只是少了一批行——静默的少行。
        if (readOnlyShard) {
            replayWalInto(memtable, manifest.walFlushedThroughSeq)
            preloadIndexes()
            opened = true
            return@withLock
        }

        // 恢复：读取 WAL 中尚未 flush 的对象并重放。
        // ⚠️ `walFlushedThroughSeq == -1` 表示「从未 flush 过」，此时**必须重放全部**——
        // 曾经写成 `if (>= 0)` 导致全新分片崩溃后完全不做恢复（实现期间踩到的真实缺陷）。
        replayWalInto(memtable, manifest.walFlushedThroughSeq)
        preloadIndexes()
        // ★ 启动写者。**这一步在「manifest 已存在」这条分支上曾经漏掉**，
        // 于是「CN 重启之后再写一个已经 flush 过的分片」会直接 NPE——
        // `opened` 是 true，`wal` 却是 null，`wal!!.submit` 当场炸。
        // 只有在「分片是全新的」那条分支上被调用，是因为那时才会走到下面
        // 那条 return；已存在的分支一路走到底，谁也没叫它。
        // 恢复（重放 WAL）必须在启动写者**之前**完成：
        // 写者的起始序号来自 manifest，与重放出来的批次互不干扰。
        startWriter(myEpoch)
        // 恢复后重算 MemTable 相对已 flush 状态的净行数变化，
        // 使「重启后 COUNT 依然精确且 0 GET」
        if (exactCount && manifest.rowCountExact >= 0 && memtable.entryCount > 0) {
            writeCountDelta = recomputeWriteCountDelta()
        }
        startLeaseRenewal()
        opened = true
    }

    private suspend fun startWriter(epoch: Long) {
        val w = WalWriter(
            store = store,
            shard = shard,
            epoch = Epoch(epoch),
            // 起始序号必须**高于前缀里已有的最大序号**，否则会覆盖尚未 flush 的
            // WAL 对象（恢复时那些内容只在内存里，再崩一次就没了）。
            // 同时也要高于 `manifest.seq`——manifest 代际与 WAL 序号是两个计数器，
            // 各自单调但互不相等（见 flushLocked 里对 walFlushedThroughSeq 的说明）。
            // 取两者的最大值是最省事且安全的下界：序号跳过几个值没有任何代价
            startSeq = maxOf(manifest.seq, maxSeenWalSeq, appliedThroughSeq) + 1,
            mode = commitMode,
            scope = scope,
            onCommitted = { batches -> applyCommitted(batches) },
            // 每次组提交前校验一次租约（§7.2）。这是 fencing 的最后一道：
            // 前面几道拦的是"旧写者的对象不被采纳"，这一道拦的是"旧写者根本别写"。
            beforeWrite = { checkLease() },
        )
        wal = w
        walJob = w.start()
        // manifest 记录当前写者的 epoch，恢复时据此判断哪些 WAL 前缀属于自己
        if (manifest.walEpoch != epoch) {
            manifest = manifest.copy(walEpoch = epoch)
            currentEtag = manifestStore.commit(shard, manifest, currentEtag)
        }
        // ★ 接管审计：只有在"指针切换不原子"的弱化模式下才有意义（正常后端上
        // 这只是一次白花的 LIST，P4）。发现孤儿快照说明有人写过一份 manifest
        // 却没成为 current——那正是弱化模式下最该被看见的事件
        if (allowNonAtomicPointerSwitch) {
            val orphans = runCatching { manifestStore.auditOrphanSnapshots(shard) }.getOrDefault(emptyList())
            if (orphans.isNotEmpty()) {
                com.cloudea.yangbase.common.metrics.Metrics.onManifestOrphanSnapshots(orphans.size)
                System.err.println(
                    "[yb] 警告: 分片 $shard 接管时发现 ${orphans.size} 个孤儿 manifest 快照" +
                        "（写全了但没成为 current）：${orphans.take(3)}。" +
                        "良性解释是「上一次提交失败」；在无 CAS 的后端上，也可能是「指针被覆盖」",
                )
            }
        }
    }

    /**
     * 租约校验。失败即抛 [com.cloudea.yangbase.common.LeaseLostException]。
     *
     * **宁可让写入失败，也不要让一个已经不是写者的进程继续写**——
     * 后者产生的对象会被下一个恢复者当成有效日志重放，那是静默的数据错乱。
     */
    private fun checkLease() {
        val l = lease ?: return
        if (leaseLost || l.isExpired()) {
            leaseLost = true
            throw com.cloudea.yangbase.common.LeaseLostException(shard, Epoch(l.epoch))
        }
    }

    /**
     * 后台续期。
     *
     * 续期周期取 TTL 的三分之一：一次续期失败还有两次机会，
     * 不会因为一次网络抖动就把自己判死。
     *
     * ## 失败时先「确认」，不要直接自杀
     *
     * 曾经的写法是一次 renew 抛异常就 `leaseLost = true` 停写。问题在于
     * **一次失败的 renew 有两种完全不同的含义**：
     *
     *  - 真的被接管了（别人过了 TTL 抢走）→ 必须停写；
     *  - PUT 已经成功、只是响应丢了 → 我们**仍然是写者**，但本地记的 etag 过期了。
     *
     * 第二种情形旧代码会把自己误杀：停写 → 下一次写重新 acquire → 租约对象上
     * 写的持有者就是自己 → 报「被 'cn-a' 持有」——**被自己持有**，而该分片
     * 会一直不可写到 TTL 过期。现在先 `confirm()`：还是自己就采纳刷新的凭据，
     * 继续续期；确实不是自己了才停写，并且把原因区分开写进日志（§12.5）。
     */
    private fun startLeaseRenewal() {
        // 无租约模式下没有东西可续
        if (leaseProvider === SingleProcessLease) return
        leaseJob = scope.launch {
            while (!leaseLost) {
                val cur = lease ?: return@launch
                delay((cur.remainingMillis().coerceAtLeast(0) / 2).coerceAtLeast(200))
                var attempt = 0
                while (true) {
                    try {
                        lease = leaseProvider.renew(lease ?: return@launch)
                        break
                    } catch (t: Throwable) {
                        // 先问清楚“还是不是我的”，再决定要不要停写
                        val confirmed = runCatching { leaseProvider.confirm(lease!!) }.getOrNull()
                        if (confirmed != null) {
                            // 仍是我们自己的：采纳新凭据（etag 变了、到期时间已推后），继续
                            lease = confirmed
                            Metrics.onLeaseRenewRetried()
                            break
                        }
                        val mine = lease
                        attempt++
                        val expired = mine == null || mine.isExpired()
                        if (attempt >= 3 || expired) {
                            // ★ 停写。但**绝不能把异常丢掉**：这里曾经是 `catch (_: Throwable)`，
                            // 于是凭证轮换、代理抖动之后该分片不再续约，而**归因线索已经断了**——
                            // 直到有人来写这一分片才会收到一个 `LeaseLost`，那时早已无从判断
                            // 是"被抢走"还是"根本没问出来"。日志 + 计数都要留下（§12.5）。
                            leaseLost = true
                            Metrics.onLeaseRenewalFailed()
                            System.err.println(
                                "[yb] 警告: 分片 $shard 续租失败，停止写入（${t::class.simpleName}: ${t.message}；" +
                                    "尝试 $attempt 次，租约${if (expired) "已到期" else "尚未到期"}）",
                            )
                            return@launch
                        }
                        delay(200L * attempt)
                    }
                }
            }
        }
    }

    /** 把所有 `.idx` 加载进内存。总量可控：约每 MiB 数据 3.5 KiB（§6.7）。 */
    private suspend fun preloadIndexes() {
        for (ref in manifest.allFiles()) {
            val fid = FileId(ref.fileId)
            handles[fid.value] = sstReader.loadIndex(shard, ref.level, fid)
        }
    }

    fun residentIndexBytes(): Long = handles.values.sumOf { it.index.encodedSizeEstimate() }

    /**
     * 当前 manifest 引用的全部 `.idx`。**`ANALYZE TABLE` 的原料**（设计文档 §13.3）。
     *
     * 两个要点：
     *  1. 与 `manifest.allFiles()` **求交**，而不是直接遍历 `handles.values`：
     *     compaction 删除的文件会残留在 handles 里，照它统计会把已经删掉的数据
     *     算进行数——统计必须只能看见**当前活着的文件**。
     *  2. 这是一个**纯内存**操作（`.idx` 常驻内存，§6.7）：正常路径 0 次对象存储
     *     请求；只有被缓存淘汰掉的 `.idx` 才会补一次 GET。**永远不碰 `.dat`**，
     *     这正是 §13.3 的要求——统计从索引块里聚合，不从数据块里捞。
     */
    suspend fun loadedIndexes(): List<SstIndex> {
        refreshReadOnlyView()
        val files = manifest.allFiles()
        val out = ArrayList<SstIndex>(files.size)
        for (ref in files) {
            val h = handles[ref.fileId]
                ?: sstReader.loadIndex(shard, ref.level, FileId(ref.fileId))
                    .also { handles[ref.fileId] = it }
            out.add(h.index)
        }
        return out
    }

    // ── 读路径 ────────────────────────────────────────────────────

    /**
     * 点查。
     *
     * 设计目标（§14.2）：**稳态 1 次 GET**。因为 `.idx` 与 manifest 都常驻内存，
     * 只有最后一跳的数据块需要真的去对象存储取。
     */
    suspend fun get(key: ByteArray, readTs: ReadTs): ByteArray? {
        touch()
        refreshReadOnlyView()
        // 1) MemTable —— 0 次 IO
        when (val r = memtable.get(key, readTs)) {
            is LookupResult.Found -> return r.value
            LookupResult.Deleted -> return null
            LookupResult.NotFound -> {}
        }
        return getFromSstOnly(key, readTs)
    }

    /**
     * 只查 SST 层（跳过 MemTable）。恢复时重算行数增量需要这个语义。
     *
     * 层序即新旧序：L0 最新、L1 次之。**L0 内部必须从新到旧检查**——
     * 它允许文件间重叠，后 flush 的文件更新。正序检查会返回已删除 key 的旧版本
     * （实现期间踩到的真实缺陷）。
     */
    private suspend fun getFromSstOnly(key: ByteArray, readTs: ReadTs): ByteArray? {
        for (level in manifest.levels.indices) {
            val refs = if (level == 0) manifest.level(0).asReversed() else manifest.level(level)
            for (ref in refs) {
                if (ByteArrayComparator.compare(key, ref.minKey) < 0) continue
                if (ByteArrayComparator.compare(key, ref.maxKey) > 0) continue
                val h = handles[ref.fileId] ?: continue
                if (!h.index.bloom.mightContain(key)) continue   // 内存判定，0 次 IO
                when (val r = sstReader.get(h, key, readTs)) {
                    is LookupResult.Found -> return r.value
                    LookupResult.Deleted -> return null
                    LookupResult.NotFound -> {}
                }
            }
        }
        return null
    }

    /** 点查，但区分「不存在」与「已删除」——写路径维护行数时需要。 */
    suspend fun contains(key: ByteArray, readTs: ReadTs): Boolean = get(key, readTs) != null

    /**
     * 范围扫描。产出行值（已做 MVCC 可见性过滤，墓碑已跳过）。
     * 用 K 路归并而非逐层串行查找，避免 O(层数 × 行数)。
     */
    suspend fun scan(
        from: ByteArray?,
        to: ByteArray?,
        readTs: ReadTs,
        /** 可下推到块级的列约束（跳数索引剪枝用）。为空时不产生任何开销。 */
        pruneRanges: List<ColumnRange> = emptyList(),
        /** 上界是否为闭区间。SQL 的 BETWEEN 两端都闭，而 LSM 的原生语义是左闭右开。 */
        toInclusive: Boolean = false,
    ): List<RowVersion> {
        touch()   // 见 lastUsedMillis：空闲分片淘汰要靠它
        refreshReadOnlyView()
        val sources = ArrayList<RowIterator>()
        sources.add(MemTableIterator(memtable))
        // 块级剪枝器：每次 scan 一个实例（它内部按文件缓存 `.skp`，
        // 跨 scan 复用会让索引内容与 manifest 变化脱节）
        val skipper = if (pruneRanges.isEmpty()) null else skipSkipper?.invoke(schema, pruneRanges)
        for (level in manifest.levels.indices) {
            for (ref in manifest.level(level)) {
                if (from != null && to != null && !ref.overlaps(from, to)) continue
                val h = handles[ref.fileId] ?: continue
                sources.add(sstReader.iterator(h, from, to, toInclusive, skipper))
            }
        }
        val merged = MergingIterator(sources, readTs)
        val out = ArrayList<RowVersion>()
        while (true) {
            val e = merged.peek() ?: break
            if (to != null) {
                val c = ByteArrayComparator.compare(e.userKey, to)
                if (c > 0 || (c == 0 && !toInclusive)) break
            }
            // 下界必须在这里统一过滤：MemTableIterator 不带边界，
            // 只在 SST 迭代器里过滤会让 MemTable 中的靠前 key 漏进来
            if (from != null && ByteArrayComparator.compare(e.userKey, from) < 0) {
                merged.advance()
                continue
            }
            out.add(RowVersion(e.userKey, e.value!!))
            merged.advance()
        }
        merged.close()
        return out
    }

    // ── 计数（设计文档 §8.9）──────────────────────────────────────

    /**
     * `SELECT COUNT(*)` 的 0-GET 路径。
     *
     * 返回 -1 表示**无法从元数据精确得出**——调用方必须回退到精确扫描，
     * 绝不能拿估算值糊弄（§8.9.7「宁可慢，不可错」）。
     */
    /**
     * 当前精确行数（**含尚未 flush 的 MemTable**），零 IO。
     *
     * `manifest.rowCountExact` 只覆盖已落盘部分，`writeCountDelta` 是 MemTable
     * 相对它的净变化——两者相加才是此刻的真实行数。只取前者会让
     * `information_schema.TABLE_ROWS` 在刚插入完时显示 0。
     *
     * 返回 null 表示这个分片不维护精确计数（`exact_count = false`）——
     * 调用方必须如实报告「不知道」，不能拿估算值冒充（设计文档 §8.9.7）。
     */
    fun exactRowCount(): Long? =
        if (exactCount && manifest.rowCountExact >= 0) {
            maxOf(0L, manifest.rowCountExact + writeCountDelta)
        } else {
            null
        }

    suspend fun countAll(readTs: ReadTs): Long = applyMutex.withLock {
        touch()
        refreshReadOnlyView()
        // 机制 1：manifest 上由单写者维护的精确行数（§8.9.1）
        if (exactCount && manifest.rowCountExact >= 0 && readTs.value >= manifest.rowCountAtTs) {
            return manifest.rowCountExact + writeCountDelta
        }
        // 机制 2：文件级 MVCC 感知计数（§8.9.2）。
        // 要求：所有文件都已去重（leveled），且 MemTable 为空——否则无法精确，
        // 必须返回 -1 让调用方回退到精确扫描（§8.9.7「宁可慢，不可错」）。
        if (memtable.entryCount > 0) return -1L
        var sum = 0L
        for (ref in manifest.allFiles()) {
            val h = handles[ref.fileId] ?: return -1L
            val r = sstReader.visibleRows(h, readTs)
            if (r < 0) return -1L
            sum += r
        }
        return sum
    }

    /**
     * `SELECT COUNT(*) WHERE col = v` 的 **0 次 GET** 路径（设计文档 §8.9.4）。
     *
     * 靠的是写入时收集的块级「值 → 计数」。返回 -1 表示该列未收集计数、
     * 或存在多版本文件——调用方必须回退到精确扫描，不得使用估算值。
     */
    suspend fun countByColumnEquals(columnIndex: Int, encodedValue: ByteArray, readTs: ReadTs): Long {
        refreshReadOnlyView()
        if (memtable.entryCount > 0) return -1L
        var sum = 0L
        for (ref in manifest.allFiles()) {
            val h = handles[ref.fileId] ?: return -1L
            // L0 可能含同一 key 的多个版本，按块计数会重复
            if (!ref.deduplicated) return -1L
            for (b in h.index.blocks) {
                if (b.maxTs <= readTs.value) {
                    // 整块可见
                    val stat = b.colStats.getOrNull(columnIndex) ?: return -1L
                    val c = stat.countOf(encodedValue)
                    if (c < 0) return -1L
                    sum += c
                } else if (b.minTs <= readTs.value) {
                    // 部分可见：块级计数无法拆分，必须回退
                    return -1L
                }
                // minTs > readTs：整块不可见，贡献 0
            }
        }
        return sum
    }

    /**
     * 相对 manifest 行数水位的净变化。
     *
     * 恢复后重算：对 MemTable 中的每个 key，检查它在**已 flush 的 SST 中**是否存在。
     * bloom 过滤在内存中完成，绝大多数判定是 0 次 IO。恢复路径不是热路径，
     * 这个代价换的是「重启后 COUNT 依然精确且快」。
     */
    private suspend fun recomputeWriteCountDelta(): Long {
        var delta = 0L
        for ((k, v) in memtable.latestValues()) {
            val existedBefore = getFromSstOnly(k.data, ReadTs(Long.MAX_VALUE)) != null
            val nowExists = v != null
            when {
                nowExists && !existedBefore -> delta++
                !nowExists && existedBefore -> delta--
            }
        }
        return delta
    }

    // ── 写路径 ────────────────────────────────────────────────────

    /**
     * 提交一个事务的写集。
     *
     * 返回即代表**已持久化**（[CommitMode.SYNC] / [CommitMode.BATCH] 下）。
     */
    /**
     * 提交一个事务的写集。
     *
     * **刻意不持有任何锁**：写入者只把事务投进 WAL 的组提交队列然后等待。
     * 并发写者因此能在同一个攒批窗口里汇合，组提交才真正起到摊薄 PUT 的作用。
     *
     * 变更的实际应用由 [applyCommitted] 在 WAL 提交序上完成。
     */
    suspend fun write(batch: WalBatch): CommitResult {
        touch()
        check(opened) { "分片 $shard 尚未打开" }
        ensureWriter()
        return wal!!.submit(batch)
    }

    /**
     * 确认本节点是分片的写者；不是就**明确报错**（§4.3：同一分片任一时刻只能有一个写者）。
     *
     * 只读分片在这里**再抢一次**租约：属主可能刚好退出/过期，那样本节点就能顺势
     * 变成写者（拿到更大的 epoch）。抢不到时给一句能直接定位的话——
     * 使用者在多 CN 部署下看到的应当是"这个分片现在由别的 CN 写"，
     * 而不是一个 NPE 或者含糊的 IO 错误。
     *
     * ## 为什么不把异常吞成 null（issue #14）
     *
     * 之前这里是 `runCatching { acquire() }.getOrNull()`，两个问题：
     *  1. **诊断信息丢了**：`LeaseManager` 明明知道**谁**持有、还有多久到期（它刚从租约对象里读的，
     *     MS 模式则是共识状态给的），却在这里被吞掉，只留下一句笼统的“被其他节点持有”。
     *     使用者拿到这句话没法行动：该去哪台机器？等多久？
     *  2. **坐标错了**：对象存储瞬时故障（超时/5xx）也被归成"不是写者"，
     *     于是真正的 IO 问题被报成了一个永远不会自愈的伪结论。
     *
     * 现在：协调层的异常**原样上抛**，只换错误码（见下）。写入尚未开始（WAL 还没收到这个 batch），
     * 所以它是一个**确定可重试**的失败。
     *
     * ## 为什么是 1205（`ER_LOCK_WAIT_TIMEOUT`）而不是 1105
     *
     *  - 语义对得上：分片写权是一个**被别人占着的互斥资源**，1205 正是 MySQL 表达这件事的方式；
     *  - 客户端能懂：Connector/J 把 1205/1213 映射成可重试的事务异常，HikariCP 等连接池据此重试；
     *    1105 只是个"未知错误"，任何框架都只能把它当致命错。
     *  - 不假装是死锁/串行化冲突：SQLState 仍用 `HY000`。用 `40001` 会让客户端以为
     *    "事务被回滚、可以整体重跑"——而这里事务根本没开始执行，两者对重试的约束不同。
     */
    private suspend fun ensureWriter() {
        if (lease != null && !leaseLost && !readOnlyShard) return
        val acquired = try {
            leaseProvider.acquire(shard)
        } catch (e: com.cloudea.yangbase.common.SqlException) {
            throw com.cloudea.yangbase.common.SqlException(
                com.cloudea.yangbase.common.MySqlErrors.ER_LOCK_WAIT_TIMEOUT, "HY000",
                "本节点不是分片 $shard 的写者：${e.message}。" +
                    "写入尚未开始，重试是安全的；若始终打不通，说明写请求应当发给属主 CN" +
                    "（`yb-cli shards --owner` 可查当前属主与到期时间）",
            )
        }
        // 拿到租约：升格成写者。**必须换 epoch 前缀**（`wal/e{epoch}/`），
        // 否则本节点会往旧写者的前缀下写对象，而旧写者的日志会被下一个恢复者重放
        lease = acquired
        readOnlyShard = false
        leaseLost = false
        val epoch = maxOf(acquired.epoch, manifest.walEpoch)
        // 换 epoch ⇒ 换 manifest 的 walEpoch，并重新加载一次视图（别人的提交要看见）
        manifestStore.load(shard)?.let { manifest = it.first; currentEtag = it.second }
        manifest = manifest.copy(walEpoch = epoch)
        currentEtag = manifestStore.commit(shard, manifest, currentEtag)
        startWriter(epoch)
        startLeaseRenewal()
    }

    /**
     * 只读分片在**每次读之前**刷新 manifest 视图。
     *
     * 为什么必须刷新（而不是缓存一次就够）：manifest 是数据的指针，属主每次 flush
     * 都会让它前进。缓存住一个旧视图会让本节点**看不到属主已经提交的行**——
     * 而且是静默的（查询照常返回、只是少了一批行）。
     *
     * 代价：只在**本节点不是写者**时才有，且是 1 次 GET（CURRENT 指针；
     * 内容变了才会再取一次 manifest 本体）。单 CN 部署下永远不触发。
     */
    private suspend fun refreshReadOnlyView() {
        if (!readOnlyShard) return
        val loaded = manifestStore.load(shard) ?: return
        manifest = loaded.first
        currentEtag = loaded.second
        preloadIndexes()
        // 属主可能在两次读之间又写了东西（还没 flush，因此只在 WAL 里）。
        // 落盘边界一变就**重建**本地视图：那之前的 WAL 条目已经被 SST 覆盖，
        // 留着它们只会让内存表无限长大
        if (manifest.walFlushedThroughSeq != replayedFlushedThrough) {
            memtable.clear()
            replayedFlushedThrough = manifest.walFlushedThroughSeq
            replayedThroughSeq = manifest.walFlushedThroughSeq
        }
        replayWalInto(memtable, replayedThroughSeq)
    }

    /**
     * 把 `wal/e{epoch}/` 中 `afterSeq` 之后的批次重放进内存表。
     *
     * 恢复路径与"只读分片看属主未落盘的数据"共用这一份实现——两处的语义完全相同
     * （WAL 对象存在即已提交），分成两份必然漂移。
     *
     * @return 重放的最大序号
     */
    /**
     * 重放 WAL 前缀里 `seq > afterSeq` 的对象。
     *
     * 顺带记下**前缀里见过的最大序号**（[maxSeenWalSeq]）：启动写者时它的起始序号
     * 必须**超过**它，否则新写入会用已经存在的对象键
     * （`wal/e1/5.log`）覆盖掉尚未 flush 的旧内容——而那些内容只在内存里，
     * 再崩一次就永久消失。
     */
    private suspend fun replayWalInto(target: MemTable, afterSeq: Long): Long {
        val objs = readWalObjects(store, shard, Epoch(manifest.walEpoch), afterSeq)
        if (objs.isEmpty()) return afterSeq
        var corrupted = 0
        var maxSeq = afterSeq
        for (o in objs) {
            corrupted += o.corruptedRecords
            for (b in o.batches) {
                for (op in b.ops) {
                    target.put(op.key, com.cloudea.yangbase.common.CommitTs(b.commitTs), op.value)
                }
            }
            maxSeq = maxOf(maxSeq, o.seq)
            if (o.seq > maxSeenWalSeq) maxSeenWalSeq = o.seq
        }
        if (corrupted > 0) {
            System.err.println("[yb] 警告: 分片 $shard 重放时跳过 $corrupted 条损坏的 WAL 记录")
        }
        if (readOnlyShard) {
            replayedThroughSeq = maxSeq
            // ★ 重放进来的行必须计入 COUNT 的净变化量。
            //
            // 只读分片的 MemTable 里**只有**重放出来的行，而它们还没进 SST。
            // 不重算这个量的话，`COUNT(*)` 的元数据路径（`rowCountExact + writeCountDelta`）
            // 会报出一个**静默错误的数字**：属主写了 3 行、还没 flush，这里报 0 ——
            // 而这正是本项目最引以为傲的"0 次 GET 的 COUNT"。错在这里格外讽刺，
            // 也格外危险：使用者拿它对账与分页。
            //
            // 用与恢复路径**同一个**函数重算（`recomputeWriteCountDelta`），
            // 两处的语义完全相同（MemTable 相对已落盘状态的净增量）
            if (exactCount) writeCountDelta = recomputeWriteCountDelta()
        }
        return maxSeq
    }

    /** 本分片的 WAL epoch（跨分片 2PC 要把未决对象写进同一前缀）。 */
    val walEpochValue: Long get() = manifest.walEpoch

    /**
     * 未决对象的键。
     *
     * 由 [DatabaseEngine.commitBuffer] 直接 `store.put`，**不经过 [WalWriter]**——
     * 它必须能在「提交点之前」被写下，而 WalWriter 的 `submit` 语义是
     * 「返回即已提交」，两者是不同的东西。
     */
    fun pendingWalKey(seq: Long): String =
        ObjectKeys.walPending(shard, Epoch(manifest.walEpoch), seq)

    /** 预留下一个 WAL 序号。与普通批次共用一套序号，恢复时才能正确排序。 */
    fun reserveWalSeq(): Long = wal!!.reserveSeq()

    /**
     * 把一批已持久化的事务按序应用到 MemTable，并维护精确行数。
     *
     * 由 [WalWriter] 在**同一批 PUT 成功之后、通知提交者之前**按序回调，
     * 因此这里的「读旧值判断插入/覆盖」看到的是确定的提交序——
     * 两笔并发事务写同一个 key 时也不会算错行数。
     */
    private suspend fun applyCommitted(batches: List<WalBatch>) = applyMutex.withLock {
        for (batch in batches) {
            // 记下"这批变更真的进了 MemTable"的序号。flush 时用它写
            // `walFlushedThroughSeq`——只有**确实进了 SST** 的序号才能被恢复跳过
            if (batch.seq > appliedThroughSeq) appliedThroughSeq = batch.seq
            var delta = 0L
            if (exactCount) {
                for (op in batch.ops) {
                    val exists = get(op.key, ReadTs(batch.commitTs - 1)) != null
                    val nowTombstone = op.value == null
                    if (!exists && !nowTombstone) delta += 1
                    else if (exists && nowTombstone) delta -= 1
                }
            }
            val ts = com.cloudea.yangbase.common.CommitTs(batch.commitTs)
            for (op in batch.ops) memtable.put(op.key, ts, op.value)
            if (exactCount && delta != 0L) writeCountDelta += delta
        }
        if (memtable.isFull) flushLocked()
    }

    /**
     * 该 key 在 commitTs 之前是否可见。
     *
     * 这是写路径为读路径付的代价（§8.9.1）：表带二级索引时本来就要回表取旧值，
     * 无额外成本；无索引时靠 bloom 在内存中判定，多数情况 0 次 IO。
     */
    private suspend fun existsBefore(key: ByteArray, beforeTs: Long): Boolean {
        val r = get(key, ReadTs(beforeTs))
        return r != null
    }

    /** 未落进 manifest 的 MemTable 净行数变化。flush 时折叠进 manifest。 */
    private var writeCountDelta: Long = 0

    // ── Flush 与 Compaction ───────────────────────────────────────


    // ── Compaction（设计文档 §6.6）────────────────────────────────

    /**
     * 后台 compaction：把 L0 与重叠的 L1 文件归并成新的 L1 文件。
     *
     * 两条规则：
     *  1. **保留所有 `commitTs >= protectReadTs` 的版本**——活跃快照仍需读它们
     *  2. **每个 key 额外保留一个 `commitTs < protectReadTs` 的最新版本**——
     *     它是更早快照的「基础版本」，丢掉它会让 `readTs >= protectReadTs`
     *     的读看不到该 key 在 protectReadTs 之前的历史
     *
     * 归并后的文件天然去重（每 key 一个版本），因此 COUNT 的元数据路径
     * （§8.9.2 / §8.9.4）才能在 L1+ 上生效。
     *
     * ⚠️ 尚未实现的优化（见设计文档 §6.6）：compaction 收益预估、带宽限速。
     * 同节的「compose 免流量拼接」已评估为**当前不做**（两层布局下没有可原样保留的
     * 区间），理由见设计文档 §3.2 的决策记录。当前实现代价正确但流量未优化。
     */
    suspend fun compact(protectReadTs: Long = 0L): Boolean = applyMutex.withLock {
        compactLocked(protectReadTs)
    }

    private suspend fun compactLocked(protectReadTs: Long): Boolean {
        val l0 = manifest.level(0)
        if (l0.size < l0CompactionTrigger) return false
        // 进入即置位：`yb_compaction_active` 非零表示「正在搬数据」，
        // 长时间不为零要警惕 compaction 跟不上写入
        Metrics.compactionStarted()
        try {
            return compactBody(protectReadTs, l0)
        } finally {
            Metrics.compactionFinished()
        }
    }

    private suspend fun compactBody(protectReadTs: Long, l0: List<SstFileRef>): Boolean {

        // 输入范围 = 所有 L0 文件的键跨度；L1 中与之重叠的文件也要一并归并
        var lo = l0.first().minKey
        var hi = l0.first().maxKey
        for (f in l0) {
            if (ByteArrayComparator.compare(f.minKey, lo) < 0) lo = f.minKey
            if (ByteArrayComparator.compare(f.maxKey, hi) > 0) hi = f.maxKey
        }
        val l1Overlap = manifest.level(1).filter { it.overlaps(lo, hi) }
        val inputs = l0 + l1Overlap
        // compaction 的读放大：输入文件整体被读出来归并
        Metrics.compactionBytesRead.add(inputs.sumOf { it.size })

        val sources = inputs.mapNotNull { ref ->
            handles[ref.fileId]?.let { sstReader.iterator(it, null, null) }
        }
        if (sources.isEmpty()) return false

        // 用 readTs = MAX 归并，得到每个 key 从新到旧的版本序列
        val merged = MergingIterator(sources, ReadTs(Long.MAX_VALUE))

        val outputs = ArrayList<SstFileRef>()
        var writer = newCompactionWriter(level = 1)
        var emittedAny = false

        // 逐 key 处理：保留 >= protectReadTs 的版本 + 一个更早的「基础版本」
        while (true) {
            val e = merged.peek() ?: break
            val key = e.userKey
            var keptBase = false
            while (true) {
                val cur = merged.peek() ?: break
                if (!cur.userKey.contentEquals(key)) break
                val ts = cur.commitTs.value
                // 新于水位的一律保留；更早的只保留最新的那一个作为基础版本
                if (ts >= protectReadTs || !keptBase) {
                    writer.add(cur.userKey, ts, cur.value)
                    emittedAny = true
                    if (ts < protectReadTs) keptBase = true
                }
                merged.advance()
            }
            // 只在 key 边界处轮转输出文件，避免把一个 key 的版本拆到两个文件里
            if (writer.currentSize() >= compactionTargetBytes) {
                outputs.add(writeAndPublish(writer, level = 1))
                writer = newCompactionWriter(level = 1)
            }
        }
        merged.close()

        if (emittedAny) outputs.add(writeAndPublish(writer, level = 1))
        if (outputs.isEmpty()) return false

        // 切换 manifest：移除输入文件、加入输出文件
        val inputIds = inputs.map { it.fileId }.toSet()
        val newL0 = manifest.level(0).filter { it.fileId !in inputIds }
        val newL1 = manifest.level(1).filter { it.fileId !in inputIds } + outputs
        val newLevels = manifest.levels.mapIndexed { i, l ->
            when (i) { 0 -> newL0; 1 -> newL1; else -> l }
        }

        val newManifest = manifest.copy(
            levels = newLevels,
            seq = manifest.seq + 1,
            // compaction 之后 L1+ 是去重的，行数可以从文件元数据精确重建
            rowCountExact = if (exactCount) {
                newLevels.drop(1).sumOf { l -> l.sumOf { it.rows - it.tombstones } } + writeCountDelta
            } else -1L,
            rowCountAtTs = if (exactCount) System.currentTimeMillis() shl 18 else -1L,
        )
        manifest = newManifest
        currentEtag = manifestStore.commit(shard, newManifest, currentEtag)
        compactions.incrementAndGet()
        return true
    }

    /** 每次写 SST 都要一批**新的**观察者：统计量是按文件累计的，不能跨文件复用。 */
    private fun freshObservers(): List<SstBlockObserver> =
        if (hasSkipIndex) skipObservers(schema) else emptyList()

    /** 这个表有没有跳数索引。没有的话整条机制零开销（连解码都不会发生）。 */
    private val hasSkipIndex: Boolean by lazy { skipObservers(schema).isNotEmpty() }

    private fun newCompactionWriter(level: Int) =
        SstWriter(schema, level, blockSize, compress, countColumns, blockObservers = freshObservers())

    private var compactionTargetBytes: Long = 16L * 1024 * 1024

    /**
     * 写出一份 compaction 产物并注册句柄，返回其引用。
     *
     * **只有 compaction 用它**（flush 有自己的写出路径，因为两者写出的
     * 元信息不同）。因此这里的字节数直接记到 compaction 的账上。
     */
    private suspend fun writeAndPublish(writer: SstWriter, level: Int): SstFileRef {
        // 用 writer 自己那批观察者——它们身上才有这次收集到的统计量
        val observers = writer.blockObservers
        val res = writer.finish()
        val fid = FileId(newFileId())
        val idxBytes = cborEncode(res.index)
        store.put(ObjectKeys.sstData(shard, level, fid), res.data)
        store.put(ObjectKeys.sstIndex(shard, level, fid), idxBytes)
        Metrics.compactionBytesWritten.add((res.dataSize + idxBytes.size).toLong())
        handles[fid.value] = sstReader.loadIndex(shard, level, fid)
        persistSkipIndexes(observers, fid.value, level)
        return SstFileRef(
            fileId = fid.value, level = level, size = res.dataSize,
            rows = res.index.rows, tombstones = res.index.tombstones,
            minKey = res.index.minKey, maxKey = res.index.maxKey,
            minTs = res.index.minTs, maxTs = res.index.maxTs,
            deduplicated = res.index.deduplicated,
        )
    }

    /**
     * 把观察者收集到的统计量落成 `.skp` 对象。
     *
     * 失败**不抛异常**：跳数索引只是加速手段，写不出来最多是查询慢一点，
     * 而让一次 flush 因为附属索引失败而失败，会把「可选优化」变成「可用性风险」。
     * 但失败要留痕——静默失效的索引会让人以为剪枝没效果。
     */
    private suspend fun persistSkipIndexes(observers: List<SstBlockObserver>, fileId: String, level: Int) {
        for (o in observers) {
            if (o.isEmpty) continue
            runCatching { o.persist(fileId, level) }.onFailure {
                System.err.println("[yb] 跳数索引写出失败（file=$fileId level=$level）：${it.message}")
            }
        }
    }

    /**
     * 把已 flush 的 WAL 对象合并成一个段，并删掉原件（设计文档 §6.3.2）。
     *
     * 收益是**恢复路径的 LIST 次数不再随写入量增长**：合并之后那个 epoch 下
     * 只剩下少数几个对象，恢复时一次 LIST 就列完了。
     *
     * 安全前提：这些对象的内容已经在 SST 里，且恢复路径本来就按
     * `walFlushedThroughSeq` 跳过它们——所以删除它们不会丢任何数据。
     * 之所以可以**立即**删而不是等 GC 的 24 小时窗口：那个窗口保护的是
     * 长读事务可能还在引用的**数据文件**，而 WAL 只在恢复时被读，
     * 恢复又只发生在持有租约的那一个写者打开分片时。
     */
    private suspend fun compactWalSegments() {
        val drained = wal ?: return
        val epoch = manifest.walEpoch
        val through = manifest.walFlushedThroughSeq
        if (through < 0) return

        val prefix = ObjectKeys.walPrefix(shard, Epoch(epoch))
        val stale = store.listAll(prefix)
            .filter { !ObjectKeys.isPendingWal(it) }
            .mapNotNull { k ->
                val seq = k.substringAfterLast('/').removeSuffix(".log").toLongOrNull() ?: return@mapNotNull null
                // 只动**真正已 flush** 的，且**不动正在进行中的**（seq >= 写者当前 seq）
                if (seq > through || seq >= drained.currentSeq) null else seq to k
            }
            .sortedBy { it.first }

        if (stale.size < walSegmentTrigger) return

        val from = stale.first().first
        val to = stale.last().first
        // 段是**原样拼接**若干个完整 WAL 对象，外面包一层段头（见 WalCodec.encodeSegment）。
        // 每个内嵌对象保留自己的头与 CRC——单对象的完整性校验因此仍然成立，
        // 而 [WalCodec.decodeAny] 能把段切回对象列表
        val parts = ArrayList<ByteArray>()
        for ((_, k) in stale) {
            val bytes = runCatching { store.get(k) }.getOrNull() ?: continue
            parts.add(bytes)
        }
        if (parts.isEmpty()) return
        // 段对象用 s-{from}-{to} 命名：恢复路径一眼能看出它覆盖哪个区间
        store.put(ObjectKeys.walSegment(shard, Epoch(epoch), from, to), WalCodec.encodeSegment(parts))
        // 先写段、后删原件——顺序反了会在中间崩溃时丢日志
        for ((_, k) in stale) runCatching { store.delete(k) }
    }

    /** 把 MemTable 落成一个 L0 SST 文件，并切换 manifest 指针。 */
    suspend fun flush(): Boolean = applyMutex.withLock { flushLocked() }

    /**
     * 已应用进 MemTable 的最大 WAL 序号（`-1` 表示还没有任何批次被应用）。
     *
     * 与 `wal.currentSeq` 是**两件事**：后者是"已经分配出去的序号"，
     * 而组提交在 PUT 之前就自增它。拿"已分配"当"已落进 SST"，
     * 会让 flush 窗口内并发写入的批次被记成"已 flush"——它们在 MemTable 换新之后
     * 落进**新** MemTable，而恢复时又因为 `seq <= walFlushedThroughSeq` 被跳过：
     * 崩溃一次就**静默丢掉一批已经告诉客户端"提交成功"的写入**。
     */
    private var appliedThroughSeq: Long = -1L

    /** WAL 前缀里见过的最大对象序号（见 [replayWalInto]）。 */
    private var maxSeenWalSeq: Long = -1L

    private suspend fun flushLocked(): Boolean {
        if (memtable.entryCount == 0L) return false

        val roMem = memtable
        val fid = FileId(newFileId())
        val observers = freshObservers()
        val writer = SstWriter(
            schema, level = 0, blockSizeBytes = blockSize, compress = compress, countColumns = countColumns,
            blockObservers = observers,
        )
        for ((ik, v) in roMem.allEntries()) {
            val uk = InternalKey.userKeyOf(ik)
            val ts = InternalKey.commitTsOf(ik)
            writer.add(uk, ts.value, v)
        }
        val res = writer.finish()

        val idxBytes = cborEncode(res.index)
        store.put(ObjectKeys.sstData(shard, 0, fid), res.data)
        store.put(ObjectKeys.sstIndex(shard, 0, fid), idxBytes)
        Metrics.flushCount.inc()
        Metrics.flushBytes.add((res.dataSize + idxBytes.size).toLong())
        handles[fid.value] = sstReader.loadIndex(shard, 0, fid)
        // 跳数索引与 SST 一起落地。**顺序不能反**：先有 SST 再有 .skp，
        // 否则一份指向不存在文件的索引会在崩溃后被读到
        persistSkipIndexes(observers, fid.value, 0)

        val ref = SstFileRef(
            fileId = fid.value, level = 0, size = res.dataSize,
            rows = res.index.rows, tombstones = res.index.tombstones,
            minKey = res.index.minKey, maxKey = res.index.maxKey,
            minTs = res.index.minTs, maxTs = res.index.maxTs,
            // 必须用文件真实的去重状态，不能硬编码 —— COUNT 的元数据路径依赖它
            deduplicated = res.index.deduplicated,
        )

        val newLevels = manifest.levels.mapIndexed { i, l -> if (i == 0) l + ref else l }
        // 把 MemTable 的净变化折叠进精确行数——这是 §8.9.1 的落地点
        val newExact = if (exactCount) maxOf(0L, manifest.rowCountExact + writeCountDelta) else -1L
        val newManifest = manifest.copy(
            levels = newLevels,
            seq = manifest.seq + 1,
            // ⚠️ 必须是"确实进了这个 SST 的最大序号"，不是"已分配的最大序号"。
            // 两者在 flush 与写入并发时会分叉，而分叉的后果是崩溃后丢已确认的写入
            walFlushedThroughSeq = appliedThroughSeq,
            rowCountExact = newExact,
            rowCountAtTs = if (exactCount) roMem.maxCommitTs() else -1L,
        )
        manifest = newManifest
        currentEtag = manifestStore.commit(shard, newManifest, currentEtag)

        writeCountDelta = 0
        memtable = MemTable(memtableCapacity)
        flushes.incrementAndGet()

        // 已 flush 的 WAL 攒够就合并成段：让恢复路径的 LIST 不随写入量增长
        runCatching { compactWalSegments() }

        // flush 之后顺手做一次 compaction —— 此时正在写盘，时机自然
        if (newLevels[0].size >= l0CompactionTrigger) {
            // ★ 传入**回收水位**而不是 0。传 0 会让「保留 ts >= 0 的版本」
            // 恒真，于是所有历史版本永远留着——存储只增不减。
            val protect = runCatching { gcWatermark() }.getOrDefault(0L)
            runCatching { compactLocked(protect) }
        }
        return true
    }

    private fun newFileId(): String = "%013d%05d".format(
        System.currentTimeMillis(), nextFileId.incrementAndGet() % 100000
    )

    private fun cborEncode(idx: SstIndex): ByteArray =
        kotlinx.serialization.cbor.Cbor.encodeToByteArray(SstIndex.serializer(), idx)

    // ── 生命周期 ──────────────────────────────────────────────────

    /**
     * 最后一次被读/写访问的时刻。用于**空闲分片淘汰**（见 `DatabaseEngine` 的淘汰循环）。
     *
     * 一个被碰过一次的分片会永久常驻：内存（MemTable + 常驻 `.idx`）与
     * **每 7.5 秒一次的租约续期**都跟着它一起常驻——`objectstore` 模式下
     * 那是 480 次条件写/小时/分片，`ms` 模式下 1440 次（还要经共识日志）。
     * 对一个"没有客户就不花钱"的系统来说，这是纯粹的白付。
     */
    @Volatile
    var lastUsedMillis: Long = System.currentTimeMillis()
        private set

    /** 访问点统一打点（读、写、flush 都算"用到"）。 */
    fun touch() {
        lastUsedMillis = System.currentTimeMillis()
    }

    suspend fun close() {
        if (!opened) return
        var released: ShardLease? = null
        try {
            // 先停掉 WAL 写者，确保不再有新的 applyCommitted 进来，再落盘
            wal?.close()
            walJob?.cancelAndJoin()
            leaseJob?.cancel()
            applyMutex.withLock {
                runCatching { flushLocked() }
                opened = false
            }
            released = lease
        } finally {
            // ★ 释放租约必须在**落盘之后**：先释放的话，别人可能在我们
            // 还在 flush 的时候就接管了分片，于是两个写者同时改 manifest。
            released?.let { runCatching { leaseProvider.release(it) } }
            lease = null
        }
    }

    /** 本分片当前的租约世代。诊断与测试用。 */
    val leaseEpoch: Long? get() = lease?.epoch

    /** 租约是否已失效（被别人抢走或续期失败）。 */
    val isLeaseLost: Boolean get() = leaseLost

    /**
     * 各层 SST 的**数据字节数**（`.dat`）之和。
     *
     * 直接来自内存里的 manifest（文件条目自带 `size`），**不产生任何对象存储请求**。
     * `SHOW TABLE STATUS` 的 `Data_length` 用它——那是 mysqldump 会读的元数据字段之一。
     */
    fun dataBytes(): Long = manifest.allFiles().sumOf { it.size }

    /**
     * 这个分片最后一次提交 manifest 的时刻（毫秒）。
     *
     * `SHOW TABLE STATUS` 的 `Update_time` 用它。注意它来自**内存里的 manifest**，
     * 因此是本节点看到的时间（多 CN 下别的节点刚写完、本节点还没刷新视图时会有滞后）——
     * 这个字段本来就是"最近一次写入的大致时间"，MySQL 那边同样是尽力而为。
     */
    fun lastUpdatedMillis(): Long = manifest.updatedAt

    fun stats(): LsmStats = LsmStats(
        shard = shard.toString(),
        memtableBytes = memtable.approximateSize,
        memtableEntries = memtable.entryCount,
        levelFiles = manifest.levels.map { it.size },
        levelRows = manifest.levels.map { l -> l.sumOf { it.rows } },
        residentIndexBytes = residentIndexBytes(),
        rowCountExact = manifest.rowCountExact,
        flushes = flushes.get(),
        compactions = compactions.get(),
        cacheHitRate = cache.hitRate(),
    )
}

data class LsmStats(
    val shard: String,
    val memtableBytes: Long,
    val memtableEntries: Long,
    val levelFiles: List<Int>,
    val levelRows: List<Long>,
    val residentIndexBytes: Long,
    val rowCountExact: Long,
    val flushes: Long,
    val compactions: Long,
    val cacheHitRate: Double,
)
