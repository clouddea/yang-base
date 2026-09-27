package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.CommitTs
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.IndexDef
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.PartitionKind
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.storage.WalCodec
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.storage.BlockCache
import com.cloudea.yangbase.storage.CommitMode
import com.cloudea.yangbase.storage.LsmStore
import com.cloudea.yangbase.storage.RowVersion
import com.cloudea.yangbase.storage.WalBatch
import com.cloudea.yangbase.storage.WalOp
import com.cloudea.yangbase.storage.XxHash64
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * 数据库引擎：把 Catalog、TSO、LSM 存储与分区串起来的门面。
 *
 * 设计文档 §9.3「分区即写入分片」在这里落地：每个 (表, 分区) 是一个
 * 独立的 [LsmStore]，拥有各自的写者与 manifest。
 */
class DatabaseEngine(
    /** 对象存储。公开只读——GC 与能力探测需要直接访问它。 */
    val store: ObjectStore,
    private val scope: CoroutineScope,
    val nodeId: String = "node-1",
    cacheBytes: Long = 256L * 1024 * 1024,
    /**
     * 提交档。
     *
     * ⚠️ **这是部署级设置，不是会话级的**：组提交的 WalWriter 是**每个分片一个**、
     * 被所有会话共享的。某个会话单独切档在架构上无法表达——同一批里混着
     * 不同持久性承诺的事务，攒批就没法定义。设计文档 §6.3.4 的「会话 > 表 > 部署」
     * 优先级链与「每分片一个组提交写者」是冲突的，这里选择后者。
     */
    val commitMode: CommitMode = CommitMode.SYNC,
    /**
     * **只读模式**：打开分片时**不去抢写者租约**，只用只读路径读。
     *
     * ## 为什么这是安全要求而不是优化
     *
     * `yb-cli` 的巡检类命令（`ls` / `shards` / `cost` / `recover-check` / `backup`）
     * 只需读，但它们走的是同一个引擎装配路径——**在 `single` 协调模式下，
     * "抢租约"永远成功、epoch 恒为 1**（单进程假设）。于是对线上桶执行一次
     * "只读检查"就会让 CLI 以**写者身份**打开每个分片，并在提交 manifest 时
     * 与线上 CN 争同一个 `CURRENT` 指针：输的那一方写下的 WAL 对象会落在
     * 同一个 `wal/e1/` 前缀下，被对方的下一次恢复当成有效日志重放——
     * **静默的数据互相覆盖**。
     *
     * 只读模式下 `LsmStore` 走"租约拿不到"的那条路（它本来就存在，
     * 供非属主 CN 使用），因此这条路径是有测试覆盖的。
     */
    val readOnly: Boolean = false,
    /**
     * 允许在**没有 ETag CAS** 的后端上切换 manifest 指针（阿里云 OSS 这一类）。
     *
     * 默认 false = 这种后端上写入会明确失败。打开它表示显式接受「指针切换不原子」
     * 的弱化，论证与风险见 `ManifestStore.allowNonAtomicPointerSwitch`。
     */
    val allowNonAtomicPointerSwitch: Boolean = false,
    private val memtableCapacity: Long = 32L * 1024 * 1024,
    /**
     * 二级缓存（本地 NVMe，设计文档 §6.7）。
     *
     * 没有它时，CN 一重启 BlockCache 就全空，重建之后的一段时间里所有读都打回
     * 对象存储。有了它，这段「重新变热」的过程从几百次网络往返变成几百次本地读
     * ——**省的是请求数，延迟只是顺带**。
     */
    diskCache: com.cloudea.yangbase.storage.DiskCache? = null,
    /**
     * 跳数索引的装配钩子（设计文档 §8.5）。
     *
     * 由装配层（yb-server）从 yb-index 注入——这样 yb-storage / yb-txn
     * 都不需要反向依赖索引实现，而索引又能落在真正的读写路径上。
     */
    /**
     * **协调能力**（设计文档 §4.7 的 Q2 决策）。
     *
     * 时间戳、分片租约、行锁、catalog 版本四件事全部从这里取。业务代码只认这个接口，
     * 具体是"对象存储 CAS"还是"3 节点 MS"由装配层决定——这是 §4.7 的硬性约束
     * （任何业务代码里出现具体实现类型，日后就无法回收）。
     *
     * 传 null 时退化成 [SingleProcessCoordination]：单 CN、单机部署的形态，
     * 时间戳仍然走对象存储水位（重启不回退），但没有租约竞争与行锁。
     */
    private val coordination: CoordinationService? = null,
    private val skipObserverFactory: ((TableSchema) -> List<com.cloudea.yangbase.storage.SstBlockObserver>)? = null,
    private val skipSkipperFactory: ((TableSchema, List<com.cloudea.yangbase.storage.ColumnRange>)
        -> com.cloudea.yangbase.storage.BlockSkipper?)? = null,
) {
    val catalog: Catalog by lazy { Catalog(store, coord) }

    /**
     * 自增 id 的分配器（设计文档 §11.4）。
     *
     * **每个引擎一份**，不是每个会话一份：段是本地缓存，多开几份会各自去领段，
     * 白白多花对象存储请求，还把可用的 id 摊得更散。
     */
    val autoIncrement: AutoIncAllocator by lazy { AutoIncAllocator(store) }

    /** 在线索引构建（设计文档 §8.7）。每个引擎一份。 */
    val onlineIndexBuilder: OnlineIndexBuilder by lazy { OnlineIndexBuilder(this) }

    /** GC 水位与两阶段回收（设计文档 §6.9）。 */
    private val gc = GarbageCollector(store)
    /** 装配好的协调实现（日志与 `SHOW VARIABLES` 用它报告当前模式）。 */
    val coord: CoordinationService by lazy {
        coordination ?: SingleProcessCoordination(nodeId, store, clusterId = "default")
    }

    val leaseProvider: com.cloudea.yangbase.storage.LeaseProvider get() = coord.leases

    private val tso get() = coord.tso
    private val cache = BlockCache(cacheBytes).also { it.secondLevel = diskCache }
    private val shards = ConcurrentHashMap<String, LsmStore>()
    private val openMutex = Mutex()
    private val txCounter = AtomicLong(0)

    /** 是否启用 COUNT 的元数据快速路径（对应 schema.exactCount）。 */
    val cache_: BlockCache get() = cache

    suspend fun open() {
        tso.recover()
        catalog.load()
    }

    suspend fun close() {
        for (s in shards.values) runCatching { s.close() }
        shards.clear()
    }

    // ── 分片路由（设计文档 §9.3）──────────────────────────────────

    /**
     * 由**主键的保序编码**决定分片。
     *
     * 因为分区函数是确定性的，同一行的归属永不改变——不存在数据搬迁。
     *
     * RANGE/LIST 分区按分区列的**值**路由。之所以能只凭主键编码算出来，
     * 是因为 DDL 强制要求分区列属于主键（见 [Partitioning.validate]）——
     * 那条约束存在的唯一理由就是让这个函数成立。
     */
    fun shardFor(table: TableSchema, encodedPk: ByteArray): ShardId {
        return shard(table, partitionOf(table, encodedPk))
    }

    /** 行 → 分区号。写入路径手里有整行，不需要绕道主键编码。 */
    fun partitionOfRow(table: TableSchema, row: List<Datum>): Int {
        if (table.partitionKind == PartitionKind.NONE) return 0
        if (table.isEnumeratedPartition) return Partitioning.partitionIndexFor(table, row)
        if (table.effectivePartitionCount <= 1) return 0
        val pk = KeyEncoder.encodeKey(row, table, table.primaryKeyIndexes)
        return hashPartition(table, pk)
    }

    /** 主键编码 → 分区号。 */
    fun partitionOf(table: TableSchema, encodedPk: ByteArray): Int {
        if (table.partitionKind == PartitionKind.NONE) return 0
        if (table.isEnumeratedPartition) return Partitioning.partitionIndexFromPk(table, encodedPk)
        return hashPartition(table, encodedPk)
    }

    private fun hashPartition(table: TableSchema, encodedPk: ByteArray): Int {
        val n = table.effectivePartitionCount
        if (n <= 1) return 0
        val h = XxHash64.hash(encodedPk, 0x5DEECE66DL)
        return (((h % n) + n) % n).toInt()
    }

    fun allShards(table: TableSchema): List<ShardId> {
        val n = table.effectivePartitionCount
        return (0 until n).map { shard(table, it) }
    }

    /** 指定分区号的存储（分区裁剪后只打开命中的那些）。 */
    fun shard(table: TableSchema, partition: Int): ShardId =
        ShardId(TableId(table.tableId), PartitionId(table.storagePartitionId(partition)))

    suspend fun shardStore(shard: ShardId, table: TableSchema): LsmStore {
        val k = shard.toString()
        shards[k]?.let { if (it.opened) return it }
        return openMutex.withLock {
            shards[k]?.let { if (it.opened) return@withLock it }
            val s = LsmStore(
                store = store,
                shard = shard,
                schema = table,
                cache = cache,
                nodeId = nodeId,
                scope = scope,
                commitMode = commitMode,
                exactCount = table.exactCount,
                memtableCapacity = memtableCapacity,
                countColumns = countColumnsOf(table),
                // 只读模式给一个"永远拿不到租约"的提供者：`LsmStore` 会因此
                // 走只读分片路径（不抢租约、不写 manifest、每次读刷新视图）
                leaseProvider = if (readOnly) NoLeaseProvider else leaseProvider,
                allowNonAtomicPointerSwitch = allowNonAtomicPointerSwitch,
                // compaction 的回收水位接上 GC 的安全水位：任何比它更老的版本
                // 都不可能有活跃读者了，可以物理丢弃（§6.6）
                gcWatermark = { gc.safeReadTs() },
                skipObservers = { t -> skipObserverFactory?.invoke(t) ?: emptyList() },
                skipSkipper = skipSkipperFactory,
            )
            s.open()
            shards[k] = s
            s
        }
    }

    /**
     * 只读模式下的租约提供者：**永远拿不到租约**（`acquire` 直接抛）。
     *
     * `LsmStore.open` 把"`acquire` 抛出"解释为"本节点不是这个分片的写者"，
     * 于是走只读分片路径——那正是只读模式要的语义，而且那条路径本来就存在、
     * 有测试覆盖（非属主 CN 读分片走的就是它）。
     */
    private object NoLeaseProvider : com.cloudea.yangbase.storage.LeaseProvider {
        override suspend fun acquire(shard: ShardId): com.cloudea.yangbase.storage.ShardLease =
            throw com.cloudea.yangbase.common.SqlException(
                com.cloudea.yangbase.common.MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                "本引擎以只读模式打开（不参与写者选举），不申请分片 $shard 的租约",
            )

        override suspend fun renew(lease: com.cloudea.yangbase.storage.ShardLease) = lease

        override suspend fun release(lease: com.cloudea.yangbase.storage.ShardLease) = Unit
    }

    /**
     * **空闲分片淘汰**：碰过一次的分片不再永久常驻。
     *
     * ## 为什么必须有
     *
     * `shards` 只增不减。一个 HASH 128 分区的表被扫一遍，128 个 LsmStore 就永久留下：
     * 每个都抱着 MemTable（默认 32MiB）与常驻 `.idx`，而且**每 7.5 秒续一次租约**
     * （`objectstore` 模式 480 次条件写/小时/分片；`ms` 模式经共识日志更贵）。
     * 对一个以"没有客户就不花钱"为存在理由的系统，这笔钱是纯浪费。
     *
     * ## 淘汰做什么
     *
     * 调 `LsmStore.close()`：**先落盘（flush）再释放租约**——顺序反了会与接管者
     * 同时改 manifest。之后从 `shards` 摘掉，下次访问时重新打开（读路径本来就是
     * 幂等的）。写者被淘汰之后再回来会重新竞选租约，epoch 单调递增——那正是
     * fencing 的常规路径，不是特例。
     *
     * @param idleMillis 多久没用过就淘汰。0 表示关闭淘汰（测试与小规模部署可用）
     */
    fun startIdleEviction(idleMillis: Long = 15 * 60 * 1000, intervalMillis: Long = 60_000): Job? {
        if (idleMillis <= 0) return null
        return scope.launch {
            while (true) {
                delay(intervalMillis)
                val now = System.currentTimeMillis()
                for ((k, lsm) in shards.entries.toList()) {
                    if (now - lsm.lastUsedMillis < idleMillis) continue
                    // 从表里摘掉再关：close 期间可能有并发读进来，让它重新开一个即可
                    if (!shards.remove(k, lsm)) continue
                    runCatching { lsm.close() }
                        .onSuccess { Metrics.onShardEvicted() }
                        .onFailure {
                            System.err.println("[yb] 淘汰空闲分片 $k 失败（下次访问会重新打开）：${it.message}")
                        }
                }
            }
        }
    }

    /**
     * 哪些列收集「值 → 计数」。
     *
     * 只对**低基数列**收集（设计文档 §8.9.4）：内存占用正比于 ndv × 块数，
     * 高基数列会让 `.idx` 膨胀到不可接受。布尔列与短字符串列是典型目标。
     */
    private fun countColumnsOf(table: TableSchema): Set<Int> =
        table.columns.indices.filter { i ->
            val c = table.columns[i]
            c.type == com.cloudea.yangbase.common.ColumnType.BOOLEAN ||
                (c.type == com.cloudea.yangbase.common.ColumnType.VARCHAR && (c.length ?: 255) <= 64)
        }.toSet()

    // ── 写路径 ────────────────────────────────────────────────────

    /**
     * 规划一批行变更对应的**全部**存储操作——数据分片与索引分片都在内。
     *
     * 索引项的价值就是整行，见 [indexEntryValue] 的说明。
     */
    fun planWrites(
        table: TableSchema,
        rows: List<List<Datum>>,
        tombstone: Boolean,
    ): Map<ShardId, List<WalOp>> {
        if (rows.isEmpty()) return emptyMap()
        val byShard = LinkedHashMap<ShardId, MutableList<WalOp>>()

        for (row in rows) {
            val pk = KeyEncoder.encodeKey(row, table, table.primaryKeyIndexes)
            byShard.getOrPut(shardFor(table, pk)) { ArrayList() }
                .add(WalOp(pk, if (tombstone) null else RowCodec.encode(row)))

            // 二级索引：每个索引一条项。删除时写墓碑。
            for (idx in table.indexes) {
                if (idx.kind != IndexKind.ORDERED && idx.kind != IndexKind.HASH) continue
                val entryKey = IndexCodec.entryKey(table, idx, row)
                byShard.getOrPut(IndexCodec.shardOf(idx)) { ArrayList() }
                    .add(WalOp(entryKey, if (tombstone) null else RowCodec.encode(row)))
            }
        }
        return byShard
    }

    // ── 唯一约束的原子化 ──────────────────────────────────────────
    //
    // 唯一约束**不能**只靠「读快照 + 检查 + 稍后写入」实现：两个并发事务
    // 各自在自己的快照上都看不到对方，于是双双通过检查、双双提交——
    // 唯一键就形同虚设。实测：8 个并发插入同一个唯一键，8 条全部成功。
    //
    // 正确做法是让检查与写入**原子**。这里不需要引入锁服务，因为
    // **唯一索引本来就住在自己的分片里、那个分片只有一个写者**——
    // 串行化点已经存在，缺的只是把检查放到那儿去。
    //
    // 跨 CN 的部分由租约保证（同一分片任一时刻只有一个写者），
    // 进程内由下面这把 per-shard 互斥量保证。

    /** 每个索引分片一把锁。只在有唯一索引时才会被用到。 */
    private val constraintLocks = ConcurrentHashMap<ShardId, kotlinx.coroutines.sync.Mutex>()

    private fun lockFor(shard: ShardId) =
        constraintLocks.computeIfAbsent(shard) { kotlinx.coroutines.sync.Mutex() }

    /**
     * 在唯一索引分片的锁下执行 [block]，执行前做一次**基于最新已提交状态**的冲突检查。
     *
     * 锁按分片名的字典序获取——两笔事务涉及多个唯一索引时，
     * 顺序不一致就会死锁。
     */
    private suspend fun <T> guardUniqueness(
        writesByShard: Map<ShardId, List<WalOp>>,
        @Suppress("UNUSED_PARAMETER") commitTs: Long,
        block: suspend () -> T,
    ): T {
        // 收集本批涉及的唯一索引分片
        val guarded = ArrayList<Pair<ShardId, Pair<TableSchema, IndexDef>>>()
        for ((shard, ops) in writesByShard) {
            if (ops.isEmpty()) continue
            val table = tableOfShard(shard) ?: continue
            // 唯一性只由 **ACTIVE** 的索引强制：BUILDING 期间索引项还不完整，
            // 拿它判重会把合法写入拦下来；构建完成时的回填自检会兜住真正的重复
            // （那才是 MySQL 的行为——建唯一索引时发现重复就失败）。
            if (table.activeIndexes.none { it.unique }) continue
            for (idx in table.activeIndexes) {
                if (!idx.unique || IndexCodec.shardOf(idx) != shard) continue
                guarded.add(shard to (table to idx))
            }
        }
        if (guarded.isEmpty()) return block()

        // 同一分片可能因多个唯一索引出现多次——去重后按名字排序，保证全局一致的获取顺序
        val distinct = guarded.distinctBy { it.first }.sortedBy { it.first.toString() }
        val locks = distinct.map { lockFor(it.first) }

        // 逐把获取（顺序已固定），任一失败要释放已持有的
        val held = ArrayList<kotlinx.coroutines.sync.Mutex>(locks.size)
        try {
            for (l in locks) { l.lock(); held.add(l) }
            for ((_, pair) in distinct) checkUniqueAgainstCommitted(pair.first, pair.second, writesByShard)
            return block()
        } finally {
            for (l in held.asReversed()) runCatching { l.unlock() }
        }
    }

    /**
     * 与**最新已提交状态**比对。
     *
     * ⚠️ 读时间戳用 `Long.MAX_VALUE`，既不是事务的 read_ts，也不是自己的 commitTs。
     *
     * 两个都不能用：
     *  - 事务 read_ts 是原来的 bug：它在事务开始时固定，看不到之后提交的同值行
     *  - 自己的 commitTs **也**不够：时间戳是先取的、锁是后拿的。若我先取了 ts=100、
     *    对方后取 ts=101 却先拿到锁并提交，那么它的条目在 `ReadTs(100)` 下**不可见**，
     *    我就通过了检查——实测 8 个并发插入里有 2 个会这样漏过去。
     *
     * 拿着锁读「全部已应用的数据」才是正确的语义：**锁保证了没有人在写**，
     * 所以「现在能读到的全部」就是「所有已提交的」。已删除的行不在结果里
     * （索引项被写成墓碑，scan 会跳过），因此不会误判。
     */
    private suspend fun checkUniqueAgainstCommitted(
        table: TableSchema,
        index: IndexDef,
        writesByShard: Map<ShardId, List<WalOp>>,
    ) {
        val shard = IndexCodec.shardOf(index)
        val ops = writesByShard[shard] ?: return

        // 本批内出现的 (索引列的值 → 主键)。同一批里两个不同主键占同一个值也是冲突。
        //
        // ⚠️ 判空与取值一律**解码整行**，绝不回头去解索引项的 key。
        // 索引项的 key 是「保序编码的索引列 + 主键」，而 NULL 在里面的编码是
        // 单字节 `0x00`——它和保序编码里「转义序列以 0x00 开头」的形式撞车，
        // 于是按 key 走一遍宽度就会把后面的字节读串，轻则抛
        // `CorruptDataException`，重则**静默地把主键切错**。
        // 行值（RowCodec）是自描述且有空值位图的，用它没有这个问题。
        val mine = LinkedHashMap<List<Datum>, ByteArray>()
        for (op in ops) {
            val raw = op.value ?: continue          // 删除不产生冲突
            val decoded = runCatching { RowCodec.decode(raw, table) }.getOrNull() ?: continue
            val idxVals = index.columns.map { decoded.getOrNull(table.indexOf(it)) ?: Datum.Null }
            // SQL 语义：唯一键里含 NULL 的行**不参与**唯一性约束——
            // 多个 NULL 是允许并存的。索引项本身照写（非唯一索引要靠它），
            // 但检查必须跳过。漏掉这一步会把合法的多行 NULL 判成冲突。
            if (idxVals.any { it is Datum.Null }) continue
            val pk = KeyEncoder.encodeKey(decoded, table, table.primaryKeyIndexes)
            val prev = mine.put(idxVals, pk)
            if (prev != null && !prev.contentEquals(pk)) {
                throw duplicateEntry(index, IndexCodec.valuePrefix(table, index, idxVals))
            }
        }
        if (mine.isEmpty()) return

        val lsm = shardStore(shard, table)
        for ((idxVals, pk) in mine) {
            val prefix = IndexCodec.valuePrefix(table, index, idxVals)
            val rows = lsm.scan(prefix, prefixUpperBound(prefix), ReadTs(Long.MAX_VALUE))
            for (rv in rows) {
                val raw = rv.value ?: continue
                val other = runCatching { RowCodec.decode(raw, table) }.getOrNull() ?: continue
                val otherVals = index.columns.map { other.getOrNull(table.indexOf(it)) ?: Datum.Null }
                if (otherVals != idxVals) continue      // 同前缀但值不同（NULL 等情况）
                val otherPk = KeyEncoder.encodeKey(other, table, table.primaryKeyIndexes)
                // 主键相同 = 同一行，不冲突（这是覆盖写，不是重复插入）
                if (!otherPk.contentEquals(pk)) throw duplicateEntry(index, prefix)
            }
        }
    }

    private fun duplicateEntry(index: IndexDef, valuePrefix: ByteArray): com.cloudea.yangbase.common.SqlException =
        com.cloudea.yangbase.common.SqlException(
            com.cloudea.yangbase.common.MySqlErrors.ER_DUP_ENTRY, "23000",
            "Duplicate entry '${IndexCodec.describeValues(valuePrefix)}' for key '${index.name}'",
        )

    /**
     * 一次写入的结果。
     *
     * 返回 commitTs 而不是只返回操作数，是因为**下游需要它**：
     * 倒排索引的文档要带时间戳才能参与 MVCC 可见性判断。
     * 让调用方自己去 `peek()` 是错的——并发下那个值和这次写入的并不相同。
     */
    data class WriteOutcome(val commitTs: Long, val opCount: Int)

    /** 把一组操作按分片写入存储层。所有分片共用同一个 commitTs。 */
    suspend fun applyWrites(table: TableSchema, writes: Map<ShardId, List<WalOp>>): WriteOutcome {
        if (writes.isEmpty()) return WriteOutcome(-1, 0)
        val ts = tso.next()
        // 唯一约束的检查与写入必须在同一把锁下完成——否则两个并发事务
        // 会双双通过检查（见 `guardUniqueness` 的说明）
        return guardUniqueness(writes, ts.value) {
            var n = 0
            for ((shard, ops) in writes) {
                shardStore(shard, table).write(
                    WalBatch("tx-${nodeId}-${txCounter.incrementAndGet()}", ts.value, ops)
                )
                n += ops.size
            }
            // 一次 applyWrites = 一次提交：同一个 commitTs，一组 WAL 对象。
            //
            // ★ 这里必须计数。**自动提交的单语句写入走的就是这条路**（SQL 执行器的
            // 快路径，不经 TransactionBuffer），漏了它会让 yb_transactions_total 在
            // 最常见的写入路径上恒为 0——指标看起来「有」，实际在骗人。
            // 索引回填与按行删除也走这里：它们同样真的提交了一个 TSO 批次。
            Metrics.onTxnCommit()
            // 单分片是常态；`writes.size > 1` 说明这次写跨了多个分片，但它**没有**
            // 走 2PC（各分片的 WAL 各自独立提交）。因此既不计单分片也不计 2PC——
            // 宁可让「单分片 : 2PC」的比值只反映真正走了原子提交路径的事务，
            // 也不要把它算成单分片来伪造一个好看的比例。
            if (writes.size <= 1) Metrics.onTxnSingleShard()
            WriteOutcome(ts.value, n)
        }
    }

    /**
     * 为一个**新建的**索引回填已有数据。
     *
     * 为什么必须有：`tryIndexLookup` 一旦从索引拿到结果就**直接返回、不回退扫描**。
     * 所以「先灌数据、后建索引」如果不回填，查询会**静默少返回行**——
     * 不报错、不降级，看起来就像那些行不存在。这是本系统里最危险的一类缺陷。
     *
     * 只写索引项，不重写数据：数据本来就在，重写一遍只会白白产生新版本。
     *
     * ⚠️ 回填是**同步且非原子的**：中途崩溃会留下一个部分填充的索引，而它看起来
     * 与完整索引没有区别。设计文档 §8.7 的「在线索引构建」用构建状态位解决这个问题，
     * 尚未实现——在那之前，建索引期间应避免并发写入。
     */
    suspend fun backfillIndex(table: TableSchema, index: IndexDef, rows: List<List<Datum>>): Int {
        if (rows.isEmpty()) return 0
        val shard = IndexCodec.shardOf(index)

        if (index.unique) {
            // 唯一索引遇到已有重复值必须报错（MySQL 的行为）。
            // 悄悄建成功会让「索引说唯一、数据却有重复」长期共存——
            // 之后任何一次通过索引的查询都只返回其中一行。
            val seen = HashMap<String, ByteArray>()
            for (row in rows) {
                // 含 NULL 的行不参与唯一性约束（SQL 语义），否则两行 NULL 会被
                // 当成"重复值"，把一个完全合法的表判成建不了索引
                if (index.columns.any { table.indexOf(it).let { i -> row.getOrNull(i) is Datum.Null } }) continue
                val key = IndexCodec.valuePrefix(table, index, indexColumnsOf(table, index, row))
                val k = java.util.Base64.getEncoder().encodeToString(key)
                val pk = KeyEncoder.encodeKey(row, table, table.primaryKeyIndexes)
                val prev = seen.put(k, pk)
                if (prev != null && !prev.contentEquals(pk)) {
                    throw com.cloudea.yangbase.common.SqlException(
                        com.cloudea.yangbase.common.MySqlErrors.ER_DUP_ENTRY, "23000",
                        "无法创建唯一索引 '${index.name}'：已有数据中存在重复值",
                    )
                }
            }
        }

        val ts = tso.next()
        val ops = rows.map { row ->
            WalOp(IndexCodec.entryKey(table, index, row), RowCodec.encode(row))
        }
        shardStore(shard, table).write(
            WalBatch("idx-backfill-${nodeId}-${txCounter.incrementAndGet()}", ts.value, ops)
        )
        return ops.size
    }

    /** 该行的索引列取值（按索引列顺序）。 */
    private fun indexColumnsOf(
        table: TableSchema,
        index: IndexDef,
        row: List<Datum>,
    ): List<Datum> = IndexCodec.columnIndexes(table, index).map { row.getOrElse(it) { Datum.Null } }

    /** 分配一个提交时间戳。给「不写数据但要写索引」的场景用（如建索引回填）。 */
    suspend fun allocateCommitTs(): Long = tso.next().value

    suspend fun insert(table: TableSchema, rows: List<List<Datum>>): Int {
        if (rows.isEmpty()) return 0
        applyWrites(table, planWrites(table, rows, tombstone = false))
        return rows.size
    }

    /**
     * 删除一批**已知内容**的行：数据与索引一起删。
     *
     * 必须传整行而不是只传主键——否则无法计算索引项 key，索引会残留脏项。
     */
    suspend fun deleteRows(table: TableSchema, rows: List<List<Datum>>): Int {
        if (rows.isEmpty()) return 0
        applyWrites(table, planWrites(table, rows, tombstone = true))
        return rows.size
    }

    suspend fun delete(table: TableSchema, rows: List<List<Datum>>): Int {
        if (rows.isEmpty()) return 0
        val ts = tso.next()
        val byShard = LinkedHashMap<ShardId, MutableList<WalOp>>()
        for (row in rows) {
            val pk = KeyEncoder.encodeKey(row, table, table.primaryKeyIndexes)
            val shard = shardFor(table, pk)
            byShard.getOrPut(shard) { ArrayList() }.add(WalOp(pk, null))
        }
        for ((shard, ops) in byShard) {
            shardStore(shard, table).write(WalBatch("tx-${nodeId}-${txCounter.incrementAndGet()}", ts.value, ops))
        }
        return rows.size
    }

    /**
     * 提交一个事务缓冲：把它按分片写成 WAL 批次。
     *
     * 所有分片共用**同一个 commitTs**——这是 MVCC 可见性的基础，
     * 也是「同一个事务的所有写入在同一个快照点上可见」的前提。
     *
     * ## 跨分片的原子性（设计文档 §7.3）
     *
     * **单分片事务天然原子**：它只产生一个 WAL 对象，一次 PUT 就是提交点。
     * 绝大多数事务属于这一类（§9.3：选对分区键，跨分片事务基本消失）。
     *
     * **跨分片事务走两阶段**：
     * ```
     *   阶段 1  各分片把自己的那部分写成**未决** WAL（wal/e{n}/p/）
     *   阶段 2  写提交标记 wal/tx/{txId}.commit   ← 唯一的原子提交点
     * ```
     * 恢复时未决批次**只在提交标记存在时才重放**，因此「写到一半崩溃」
     * 的结果是**整笔事务原子地消失**，而不是留下半份数据。
     *
     * ## 读可见性：靠"压低读时间戳"保证看不到半笔事务
     *
     * 阶段 3 是**逐分片**写入的，所以"`ts` 已分配、阶段 3 还没跑完"的这段时间里，
     * 一个刚开始的读事务（`read_ts > ts`）会看到分片 A 的新行、分片 B 的旧行——
     * 一笔事务在结构上不可能出现的中间态。
     *
     * 修法不需要改协议：在途事务的 ts **本进程就知道**（事务缓冲就在本 CN），
     * 所以 [currentReadTs] 会把读时间戳压到"在途 2PC 的最小 ts"之下——
     * 两个分片都看不到它。代价是那几毫秒里读看到的是略旧的快照
     * （快照隔离本来就允许），而绝大多数事务是单分片，完全不受影响。
     */
    suspend fun commitBuffer(buffer: TransactionBuffer): WriteOutcome {
        if (buffer.isEmpty) return WriteOutcome(-1, 0)
        val ts = tso.next()
        val byShard = buffer.byShard()

        // 单分片：一次 PUT 就是提交点。绝大多数事务走这条路（§9.3）。
        if (byShard.size <= 1) {
            return guardUniqueness(byShard, ts.value) {
                var total = 0
                for ((shard, ops) in byShard) {
                    val table = tableOfShard(shard) ?: continue
                    shardStore(shard, table).write(
                        WalBatch("tx-${nodeId}-${txCounter.incrementAndGet()}", ts.value, ops)
                    )
                    total += ops.size
                }
                // 单分片事务天然原子（一次 PUT 就是提交点），它应当是压倒性多数。
                // 这个计数与 yb_txn_2pc_total 的比值就是「分区键选得对不对」的直接证据。
                Metrics.onTxnSingleShard()
                Metrics.onTxnCommit()
                WriteOutcome(ts.value, total)
            }
        }

        // ── 跨分片：两阶段 ────────────────────────────────────────
        //
        //  阶段 1  各分片把写集另存一份**未决**对象（此时尚未提交）
        //  阶段 2  写提交标记 wal/tx/{txId}.commit     ← 唯一的原子提交点
        //  阶段 3  走**正常**写入路径落 WAL + MemTable（顺序与行数都正确）
        //  阶段 4  删掉未决对象（尽力而为；没删掉也不会出错，见恢复逻辑）
        //
        // 为什么阶段 3 还要走正常路径，而不是直接应用未决对象：
        //   - 正常路径的「按提交序应用」与行数增量计算是经过验证的，
        //     另开一条应用路径就得把那些不变量再证明一遍
        //   - 未决对象因此只承担**持久性保险**：崩溃发生在阶段 2 之后、
        //     阶段 3 之前时，恢复靠它把数据找回来
        // 代价是跨分片事务每个分片多写一个对象。这是罕见路径，值得。
        // ★ 唯一性检查与整个两阶段必须在**同一把锁下**完成。
        // 只在开头检查一次是不够的：检查与写入之间被别的写入插进来，
        // 那个窗口正好就是原来那个 bug（8 个并发插入全部成功）。
        return guardUniqueness(byShard, ts.value) {
        val txId = "2pc-${nodeId}-${ts.value}-${txCounter.incrementAndGet()}"
        val staged = ArrayList<Pair<com.cloudea.yangbase.storage.LsmStore, String>>()
        var total = 0
        // 登记为"在途"：读路径据此把快照压到它之下（见 currentReadTs）。
        // 用 try/finally 保证**任何**出口都会摘掉它——漏摘的后果是读永远停在旧快照上
        inFlight2pc[ts.value] = txId
        try {

        for ((shard, ops) in byShard) {
            val table = tableOfShard(shard) ?: continue
            val lsm = shardStore(shard, table)
            val seq = lsm.reserveWalSeq()
            val key = lsm.pendingWalKey(seq)
            // 未决对象与普通对象在同一个前缀下（只差一个后缀），
            // 因此恢复时一次 LIST 就能同时看到两者
            store.put(
                key,
                com.cloudea.yangbase.storage.WalCodec.encode(
                    com.cloudea.yangbase.common.Epoch(lsm.walEpochValue), seq,
                    listOf(WalBatch(txId, ts.value, ops)),
                ),
            )
            staged.add(lsm to key)
            total += ops.size
        }

        if (staged.isEmpty()) return@guardUniqueness WriteOutcome(ts.value, 0)

        // ★ 原子提交点。这一步之前的任何失败都等于「事务没发生」——
        //   未决对象会被恢复逻辑丢弃，因为找不到提交标记。
        store.put(ObjectKeys.txCommit(txId), ts.value.toString().toByteArray())

        // 阶段 3：走正常路径。崩溃在这里之前 → 恢复重放未决对象；
        // 崩溃在这里之后 → 恢复看到普通对象里有这个 txId，跳过未决对象。
        for ((shard, ops) in byShard) {
            val table = tableOfShard(shard) ?: continue
            shardStore(shard, table).write(WalBatch(txId, ts.value, ops))
        }

        // 阶段 4：尽力而为。删不掉只是多占一点存储，不影响正确性。
        for ((_, key) in staged) runCatching { store.delete(key) }

        // 跨分片不仅多一轮往返，每个分片还要多写一个未决对象。
        // 这个数字升高应当先去看分区键（§9.3），而不是去优化 2PC 本身。
        Metrics.onTxn2pc()
        Metrics.onTxnCommit()
        WriteOutcome(ts.value, total)
        } finally {
            // 无论成功、失败还是异常，都要摘掉"在途"标记
            inFlight2pc.remove(ts.value)
        }
        }
    }

    /**
     * 让一个分片下线：关掉它的 LsmStore 并从缓存里移除。
     *
     * RESTORE / PITR 必须用它。恢复之后必须换一个新的租约 epoch，
     * 而 epoch 是在分片**打开时**取得的——不把分片关掉，内存里那个旧写者会继续
     * 按旧视图工作，恢复点之后写下的 WAL 也会在下次重放时把数据覆盖回去，
     * 恢复等于没做。
     */
    suspend fun evictShard(shard: ShardId) {
        val lsm = shards.remove(shard.toString()) ?: return
        runCatching { lsm.close() }
    }

    /**
     * 删掉一个索引的全部分片对象。
     *
     * DROP INDEX / TRUNCATE 之后这些对象再也不会被引用，但**必须显式删**：
     * 索引分片的编号是单调分配的，同名索引重建会拿到新 id、新分片，
     * 旧对象就成了永久垃圾——而 GC 只扫 `shard/` 前缀下的可达性，
     * 对索引分片用的是同一套逻辑，留在这里反而更省事。
     */
    suspend fun dropIndexData(index: IndexDef) {
        val shard = IndexCodec.shardOf(index)
        val lsm = shards.remove(shard.toString())
        runCatching { lsm?.close() }
        runCatching { store.listAll(ObjectKeys.shardRoot(shard)).forEach { store.delete(it) } }
    }

    /**
     * 清掉重分区之后**不再被新布局引用**的分片对象。
     *
     * 重分区会改变行的归属，旧分片里可能还留着按旧分区函数放进去的数据。
     * 只删「旧布局有、新布局没有」的那些，避免误删仍然有效的分片。
     */
    suspend fun dropOrphanShards(table: TableSchema, oldShardKeys: Set<String>) {
        val live = allShards(table).map { it.toString() }.toSet()
        for (k in oldShardKeys) {
            if (k in live) continue
            val lsm = shards.remove(k)
            runCatching { lsm?.close() }
            runCatching { store.listAll(k).forEach { store.delete(it) } }
        }
    }

    /**
     * 由分片反查它属于哪张表（索引分片返回它所属的表）。
     *
     * ⚠️ 必须同时认索引分片：索引分片的 TableId 是**负数**，
     * 只按正数查表会让 `commitBuffer` 把索引项当成「无主分片」直接 `continue`——
     * 于是**事务内写入的索引项在提交时被静默丢弃**，索引与数据从此不一致。
     * 非事务路径不经过这里，所以只有显式事务才会踩到。
     */
    private fun tableOfShard(shard: ShardId): TableSchema? {
        val t = shard.table.value
        if (t > 0) return catalog.allTables().firstOrNull { it.tableId == t }
        val indexId = IndexCodec.indexIdOf(shard)
        return catalog.allTables().firstOrNull { table -> table.indexes.any { it.id == indexId } }
    }

    /**
     * 按主键删除（**不维护二级索引**）。
     *
     * ⚠️ 只有确认表上没有二级索引时才可以用它——否则索引会残留脏项。
     * 正常路径请用 [deleteRows]。
     */
    suspend fun deleteByKeys(table: TableSchema, encodedPks: List<ByteArray>): Int {
        if (encodedPks.isEmpty()) return 0
        check(table.indexes.isEmpty()) {
            "表 ${table.name} 上有二级索引，删除必须走 deleteRows 以维护索引项"
        }
        val ts = tso.next()
        val byShard = LinkedHashMap<ShardId, MutableList<WalOp>>()
        for (pk in encodedPks) {
            byShard.getOrPut(shardFor(table, pk)) { ArrayList() }.add(WalOp(pk, null))
        }
        for ((shard, ops) in byShard) {
            shardStore(shard, table).write(WalBatch("tx-${nodeId}-${txCounter.incrementAndGet()}", ts.value, ops))
        }
        return encodedPks.size
    }

    // ── 索引查找 ──────────────────────────────────────────────────

    /**
     * 按索引值查找行。
     *
     * **索引项的值就是整行**，因此这里不需要回表——直接解码即可。
     *
     * 这是一个有意的取舍：对象存储上「多存一份行」的成本是每 GB 每月几分钱，
     * 而「回表」的成本是**每个结果行一次 GET**（按请求计费，且每次几十毫秒）。
     * 设计文档 §10.2 R2 明确要求偏好 covering index；这里把它做成了默认行为。
     */
    suspend fun lookupByIndex(
        table: TableSchema,
        index: IndexDef,
        values: List<Datum>,
        readTs: ReadTs,
        limit: Int = Int.MAX_VALUE,
    ): List<List<Datum>> {
        val shard = IndexCodec.shardOf(index)
        val lsm = shardStore(shard, table)
        val prefix = IndexCodec.valuePrefix(table, index, values)
        val out = ArrayList<List<Datum>>()

        // 用前缀做范围扫描：同一索引值下的所有行在 LSM 中相邻
        val upper = prefixUpperBound(prefix)
        val rows = lsm.scan(prefix, upper, readTs, toInclusive = false)
        for (rv in rows) {
            out.add(RowCodec.decode(rv.value, table))
            if (out.size >= limit) break
        }
        return out
    }

    /**
     * 按**主键**取一行（点查）。不存在（或不可见）时返回 null。
     *
     * ## 为什么需要它：`INSERT` 撞主键必须报 1062，而不是覆盖
     *
     * LSM 的写入天然是"同 key 覆盖"——`INSERT INTO t VALUES (1, …)` 在 id=1
     * 已存在时会把旧行**静默覆盖掉**。那是 MySQL 语义里不存在的行为：
     * MySQL 报 1062，而应用正是靠这个错误来决定"要不要走更新路径"。
     * 覆盖不报错意味着一次"我以为插入失败了"的操作实际改掉了别人的数据。
     *
     * 唯一索引的判重一直有（`checkUnique` + `findUniqueConflict`），
     * 但**主键不在 `activeIndexes` 里**，所以主键这一路一直没有判重。
     */
    suspend fun lookupByPk(table: TableSchema, row: List<Datum>, readTs: ReadTs): List<Datum>? {
        val pk = KeyEncoder.encodeKey(row, table, table.primaryKeyIndexes)
        val bytes = shardStore(shardFor(table, pk), table).get(pk, readTs) ?: return null
        return RowCodec.decode(bytes, table)
    }

    /**
     * 某个索引值上是否已有其他行——唯一约束检查用。
     *
     * @return 冲突行的主键，没有冲突时返回 null
     */
    suspend fun findUniqueConflict(
        table: TableSchema,
        index: IndexDef,
        row: List<Datum>,
        readTs: ReadTs,
    ): ByteArray? {
        val conflict = lookupByIndex(table, index, index.columns.map { row[table.indexOf(it)] }, readTs, limit = 2)
        val selfPk = KeyEncoder.encodeKey(row, table, table.primaryKeyIndexes)
        for (r in conflict) {
            val pk = KeyEncoder.encodeKey(r, table, table.primaryKeyIndexes)
            // 自己不算冲突（更新场景）
            if (!pk.contentEquals(selfPk)) return pk
        }
        return null
    }

    /**
     * 前缀的「下一个」key，作为左闭右开扫描的上界。
     *
     * 保序编码下，把最后一个字节加一直到不进位即可；全 0xFF 时退化为无上界。
     */
    private fun prefixUpperBound(prefix: ByteArray): ByteArray {
        val out = prefix.copyOf()
        for (i in out.indices.reversed()) {
            val v = out[i].toInt() and 0xFF
            if (v != 0xFF) {
                out[i] = (v + 1).toByte()
                return out.copyOf(i + 1)
            }
        }
        // 全是 0xFF：没有更大的前缀，返回比任意 key 都大的哨兵
        return prefix + byteArrayOf(0xFF.toByte(), 0xFF.toByte())
    }

    // ── 读路径 ────────────────────────────────────────────────────

    suspend fun now(): CommitTs = tso.next()

    /**
     * **在读路径上排除"正在进行中的跨分片事务"**。
     *
     * ## 它修的是什么
     *
     * 2PC 的可见性是逐分片决定的（`commitTs <= read_ts`）：阶段 3 按分片依次写入，
     * 于是"`ts` 已分配、阶段 3 还没跑完"的这段时间里，一个刚开始的读事务
     * （`read_ts > ts`）会看到**分片 A 的新行、分片 B 的旧行**——
     * 一笔事务在结构上不可能出现的中间态，而且没有任何提示。
     *
     * ## 做法：把读时间戳压在"在途 2PC 的最小 ts"之下
     *
     * 在途事务的 ts 是**本进程**知道的（事务缓冲就在本 CN），所以不需要改协议：
     * 只要有跨分片事务在提交中，读就退到一个**早于它**的快照上——
     * 两个分片都看不到它 ✓ 一致。提交完成后在途集合清空，读恢复正常。
     *
     * 代价：跨分片提交的那几毫秒里，读看到的是一个**略旧的快照**。
     * 这是安全的（快照隔离本来就允许读到较早的时刻），而"看到半笔事务"
     * 是不安全的。绝大多数事务是单分片（§9.3），那条路完全不受影响。
     *
     * ## 为什么要与本会话自己的提交取 max
     *
     * 否则"自己刚提交、紧接着读"会因为**别人的**在途事务而看不到自己的写入——
     * 那是使用者一眼就能看见的错（"我明明提交了"）。
     */
    suspend fun currentReadTs(ownLastCommitTs: Long = -1): ReadTs {
        val peeked = tso.peek().value
        val oldestInflight = inFlight2pc.keys.minOrNull() ?: return ReadTs(maxOf(peeked, ownLastCommitTs))
        // 压在在途事务之下，但**不低于自己已提交的**（读自己的写是硬保证）
        return ReadTs(maxOf(ownLastCommitTs, minOf(peeked, oldestInflight - 1L)))
    }

    /**
     * 在途的跨分片事务（`ts → txId`）。
     *
     * 每个在途事务的 ts 都不同（TSO 保证严格递增），所以按 ts 排序取最小即可。
     */
    private val inFlight2pc = java.util.concurrent.ConcurrentHashMap<Long, String>()

    suspend fun get(table: TableSchema, encodedPk: ByteArray, readTs: ReadTs): List<Datum>? {
        val shard = shardFor(table, encodedPk)
        val lsm = shardStore(shard, table)
        val v = lsm.get(encodedPk, readTs) ?: return null
        return RowCodec.decode(v, table)
    }

    /**
     * 全表扫描（跨所有分区），按分片顺序拼接。
     *
     * [shards] 允许调用方传入**分区裁剪后的子集**——这是分区裁剪真正省下 IO 的地方：
     * 被裁掉的分区连 manifest 都不会被读取（设计文档 §9.2）。
     */
    suspend fun scanAll(
        table: TableSchema,
        readTs: ReadTs,
        limit: Int = Int.MAX_VALUE,
        shards: List<ShardId> = allShards(table),
        /** 可下推的列约束（跳数索引块剪枝）。 */
        pruneRanges: List<com.cloudea.yangbase.storage.ColumnRange> = emptyList(),
    ): List<List<Datum>> {
        val out = ArrayList<List<Datum>>()
        for (shard in shards) {
            if (out.size >= limit) break
            val lsm = shardStore(shard, table)
            val rows = lsm.scan(null, null, readTs, pruneRanges)
            for (r in rows) {
                out.add(RowCodec.decode(r.value, table))
                if (out.size >= limit) break
            }
        }
        return out
    }

    /**
     * 单分片范围扫描。**这是优化器在能确定分区时必须走的路径**——
     * 它只读一个分片的 manifest 与数据块。
     */
    suspend fun scanShard(
        table: TableSchema,
        shard: ShardId,
        from: ByteArray?,
        to: ByteArray?,
        readTs: ReadTs,
        limit: Int = Int.MAX_VALUE,
        toInclusive: Boolean = false,
        /** 可下推的列约束（跳数索引块剪枝）。 */
        pruneRanges: List<com.cloudea.yangbase.storage.ColumnRange> = emptyList(),
    ): List<RowVersion> {
        val lsm = shardStore(shard, table)
        val rows = lsm.scan(from, to, readTs, pruneRanges, toInclusive)
        return if (rows.size > limit) rows.subList(0, limit) else rows
    }

    // ── 计数（设计文档 §8.9）──────────────────────────────────────

    /**
     * 全表 `COUNT(*)`。
     *
     * 分区表可以直接把各分片的元数据计数相加——**每个分片 0 次 GET**（§8.9.5）。
     */
    suspend fun countAll(
        table: TableSchema,
        readTs: ReadTs,
        shards: List<ShardId> = allShards(table),
    ): Long {
        var sum = 0L
        for (shard in shards) {
            val lsm = shardStore(shard, table)
            val n = lsm.countAll(readTs)
            if (n < 0) return -1L        // 无法从元数据精确得出，让上层回退到扫描
            sum += n
        }
        return sum
    }

    /**
     * `COUNT(*) WHERE col = v`：优先走块级的「值→计数」元数据（0 次 GET）。
     * 返回 -1 表示无法从元数据精确得出。
     */
    suspend fun countByColumnEquals(
        table: TableSchema,
        columnIndex: Int,
        encodedValue: ByteArray,
        readTs: ReadTs,
        shards: List<ShardId> = allShards(table),
    ): Long {
        var sum = 0L
        for (shard in shards) {
            val lsm = shardStore(shard, table)
            val n = lsm.countByColumnEquals(columnIndex, encodedValue, readTs)
            if (n < 0) return -1L
            sum += n
        }
        return sum
    }

    /** 清空一张表的所有行。返回删除的行数。 */
    suspend fun deleteAll(
        table: TableSchema,
        readTs: ReadTs,
        shards: List<ShardId> = allShards(table),
    ): Long {
        var total = 0L
        for (shard in shards) {
            val lsm = shardStore(shard, table)
            val rows = lsm.scan(null, null, readTs)
            if (rows.isEmpty()) continue
            // 分区删除/清空也必须删除二级索引中的完整行副本。
            val decoded = rows.map { RowCodec.decode(it.value, table) }
            applyWrites(table, planWrites(table, decoded, tombstone = true))
            total += rows.size
        }
        return total
    }

    /**
     * 刷一个索引分片的盘。
     *
     * 索引项写在**索引自己的分片**里（`ShardId(TableId(-indexId), 0)`），
     * 它不在 [allShards] 的返回里——`flush(table)` 那类按数据分片遍历的方法
     * 一个都碰不到它。回填之后必须显式刷，否则索引项还留在 MemTable 里，
     * 而 `flushAll()` 之外没有任何地方会去落它。
     */
    suspend fun flushIndexShard(table: TableSchema, index: IndexDef): Boolean =
        shards[IndexCodec.shardOf(index).toString()]?.flush() ?: false

    /**
     * 一个数据分片里 `commit_ts > sinceTs` 的那些写入，返回**受影响的主键**。
     *
     * 在线索引构建的追平阶段用它取出「需要重算索引项的键」（设计文档 §8.7 步骤 5）。
     * 只返回主键、不返回新旧值：WAL 里本来就没有旧值，而索引项的去留要靠
     * 「旧值 vs 新值」的差，所以调用方拿到键之后要自己按两个时间戳各读一次
     * （见 [OnlineIndexBuilder.applyDelta]）。
     *
     * 顺带一提，这也就是「变更流读已有的 WAL」那条决策（§16.1 Q5）的又一次落地：
     * 系统里**没有**第二份变更日志。
     */
    suspend fun walKeysSince(shard: ShardId, sinceTs: Long): List<ByteArray> {
        val keys = store.listAll(ObjectKeys.walAllEpochsPrefix(shard))
            .filter { !ObjectKeys.isPendingWal(it) }   // 未决批次还没提交，不算变更
            .sorted()
        val out = ArrayList<ByteArray>()
        for (k in keys) {
            // decodeAny：WAL 前缀里可能有段对象（见 WalCodec.decodeAny 的说明）
            val objs = runCatching { WalCodec.decodeAny(store.get(k), k) }.getOrNull() ?: continue
            for (obj in objs) {
                for (b in obj.batches) {
                    if (b.commitTs <= sinceTs) continue
                    for (op in b.ops) out.add(op.key)
                }
            }
        }
        return out
    }

    /** 刷盘（把 MemTable 落成 SST）。返回是否真的产生了文件。 */
    suspend fun flush(table: TableSchema): Boolean {
        var any = false
        for (shard in allShards(table)) {
            val lsm = shards[shard.toString()] ?: continue
            if (lsm.flush()) any = true
        }
        return any
    }

    suspend fun flushAll(): Boolean {
        var any = false
        for (s in shards.values) if (s.flush()) any = true
        return any
    }

    /**
     * **只读已经打开的分片**，统计精确行数；有分片未打开时返回 null。
     *
     * 刻意不在这里打开分片：`information_schema.tables` 会被客户端在连接后
     * 立刻对**所有表**查一遍，为了填一个展示用的数字去打开每一张表的所有分片，
     * 会让「连上数据库」这件事产生成百上千次对象存储请求。
     * 看不到就如实返回 null，比为了好看而制造 IO 更符合原则 P4。
     */
    fun peekRowCount(table: TableSchema): Long? {
        var sum = 0L
        for (shard in allShards(table)) {
            val lsm = shards[shard.toString()] ?: return null
            if (!lsm.opened) return null
            val n = lsm.exactRowCount() ?: return null
            sum += n
        }
        return sum
    }

    /** 该表是否已经打开过（information_schema 用它判断能否给出实时统计）。 */
    fun isOpen(table: TableSchema): Boolean =
        allShards(table).all { shards[it.toString()]?.opened == true }

    fun stats(): EngineStats = EngineStats(
        openShards = shards.size,
        cacheHitRate = cache.hitRate(),
        databases = catalog.stats().databases,
        tables = catalog.stats().tables,
    )

    fun shardStats(): List<com.cloudea.yangbase.storage.LsmStats> = shards.values.map { it.stats() }
}

data class EngineStats(
    val openShards: Int,
    val cacheHitRate: Double,
    val databases: Int,
    val tables: Int,
)
