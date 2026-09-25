package com.cloudea.yangbase.server

import com.cloudea.yangbase.common.audit.Audit
import com.cloudea.yangbase.mysql.MysqlServer
import com.cloudea.yangbase.mysql.MysqlServerConfig
import com.cloudea.yangbase.objectstore.InstrumentedObjectStore
import com.cloudea.yangbase.objectstore.LocalFileObjectStore
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.ObjectStoreCapabilities
import com.cloudea.yangbase.objectstore.ObjectStoreAuditLog
import com.cloudea.yangbase.objectstore.RetryingObjectStore
import com.cloudea.yangbase.objectstore.S3ObjectStore
import com.cloudea.yangbase.txn.DatabaseEngine
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.nio.file.Path

/**
 * YangBase 计算节点（CN）主程序。
 *
 * 它是一个**无状态**进程：全部持久状态都在对象存储上，
 * 因此可以随时被销毁重建（设计文档 §1.2、§12.1）。
 */
class YangBaseServer(
    private val config: ServerConfig,
    /** 测试用：直接注入 ObjectStore（例如套了计数器的 S3 后端）。 */
    private val injectedStore: ObjectStore? = null,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    lateinit var store: ObjectStore
        private set
    lateinit var engine: DatabaseEngine
        private set

    /** 查询观测与指标采集。引擎起来之后构造（它要读 engine 的状态）。 */
    lateinit var metrics: ServerMetrics
        private set

    /** 审计落点；未启用审计时为 null。 */
    var auditLog: ObjectStoreAuditLog? = null
        private set

    private var http: MetricsHttpServer? = null
    private var mysql: MysqlServer? = null

    /**
     * 进程内传输：`coordMode = "single"/"objectstore"` 时只用于本进程内的 MS（如果有），
     * 与 CN 客户端共用一个实例。多进程部署走 TCP，这个字段为 null。
     */
    private var inProcessTransport: com.cloudea.yangbase.ms.InProcessMetaTransport? = null

    /** 本进程内运行的 MS 节点（`runMetaService = true` 时）。 */
    private var msNode: com.cloudea.yangbase.ms.MetaServiceNode? = null

    /** 装配好的协调实现（也是 `SHOW VARIABLES` 报告模式的地方）。 */
    var coordination: com.cloudea.yangbase.txn.CoordinationService? = null
        private set

    /** catalog 失效轮询任务（§4.5：MS 广播失败时的兜底）。 */
    private var catalogWatch: kotlinx.coroutines.Job? = null

    /** 空闲分片淘汰任务（见 `DatabaseEngine.startIdleEviction`）。 */
    private var shardEvictor: kotlinx.coroutines.Job? = null

    /** `--role ms` 时本进程的 MS 节点：观测端点从它读角色/任期/提交位点。 */
    @Volatile
    private var msMetrics: com.cloudea.yangbase.ms.MetaServiceNode? = null

    /**
     * MS 进程的健康报告。
     *
     * 判据与 CN 侧一致：**非阻塞、无 IO**（抓取不能产生对象存储请求）。
     * 只有"没有领导者"算不健康——那是共识层唯一真正需要报警的状态
     */
    private fun msHealth(): HealthReport {
        val n = msMetrics ?: return HealthReport(true, "status=ok\nrole=none\n")
        val raft = n.raft
        val ok = raft.currentRole == com.cloudea.yangbase.ms.RaftNode.Role.LEADER || raft.currentRole == com.cloudea.yangbase.ms.RaftNode.Role.FOLLOWER
        return HealthReport(
            ok,
            "status=" + (if (ok) "ok" else "degraded") + "\n" +
                "role=" + raft.currentRole + "\n" +
                "term=" + raft.term + "\n" +
                "committed=" + raft.committed + "\n",
        )
    }

    /** 实际监听的端口。传 port=0 时由系统分配，测试据此拿到真实端口。 */
    val boundPort: Int get() = mysql?.boundPort ?: -1

    /** 观测端点的实际端口；未启用时为 -1。 */
    val metricsBoundPort: Int get() = http?.boundPort ?: -1

    /**
     * 按 [ServerConfig.coordMode] 装配协调实现（§4.7 的两种模式 + 单进程模式）。
     *
     * 这个过程里唯一需要小心的是**顺序**：进程内 MS 必须先起来并选出领导者，
     * CN 的客户端才能拿到时间戳区间；否则启动期的第一条 DDL 会撞上"MS 未就绪"。
     */
    private suspend fun startCoordination(store: ObjectStore) {
        val mode = config.coordMode.lowercase()
        val batchMillis = config.tsoBatchMillis ?: defaultTsoBatchMillis(mode)
        coordination = when (mode) {
            "single" -> {
                if (config.runMetaService) startInProcessMeta(store)
                com.cloudea.yangbase.txn.SingleProcessCoordination(
                    config.nodeId, store, batchMillis = batchMillis,
                )
            }
            "objectstore", "object-store", "object_store" -> {
                if (config.runMetaService) startInProcessMeta(store)
                com.cloudea.yangbase.txn.ObjectStoreCoordination(
                    store = store,
                    clusterId = "default",
                    holder = config.nodeId,
                    leaseTtlMillis = config.leaseTtlMillis,
                    batchMillis = batchMillis,
                )
            }
            "ms" -> {
                // ⚠️ 顺序：**先**起进程内的 MS 节点，再取传输。
                // 反过来的话 `coordinationTransport()` 会看到 inProcessTransport 还是 null，
                // 于是回退成 TCP，然后拿节点标识（"ms-1"）去当 host:port 解析——
                // 报一句"MS 地址格式应为 host:port"，与真正的原因（顺序）毫无关系
                if (config.runMetaService) startInProcessMeta(store)
                val transport = coordinationTransport()
                val peers = if (config.msPeers.isNotEmpty()) config.msPeers
                else listOf(config.msNodeId)
                com.cloudea.yangbase.ms.MetaServiceClient(
                    cluster = com.cloudea.yangbase.ms.MetaCluster(peers),
                    transport = transport,
                    nodeId = config.nodeId,
                    leaseTtlMillis = config.leaseTtlMillis,
                    batchMillis = batchMillis,
                )
            }
            else -> throw IllegalArgumentException(
                "未知的 coord-mode '$mode'（支持 single / objectstore / ms）",
            )
        }
    }

    /**
     * `--role ms`：这个进程**只**跑元数据服务（不建引擎、不开 MySQL 端口）。
     *
     * 它读的是同一个对象存储（日志与快照都落在 `meta/ms/`），所以三个 MS 进程
     * 可以是三台小规格实例，随时被抢占/迁移（§4.6 的冷启动目标 <3s）。
     */
    private suspend fun startMetaServiceOnly() {
        requireDurableStorageUnlessAllowed()
        val raw = injectedStore ?: buildStore(config)
        store = RetryingObjectStore(InstrumentedObjectStore(raw))
        val peers = config.msPeers
        val transport = com.cloudea.yangbase.ms.TcpMetaTransport()
        val node = com.cloudea.yangbase.ms.MetaServiceNode(
            nodeId = config.msNodeId,
            address = config.msAdvertiseAddress,
            peers = peers.filter { it != config.msAdvertiseAddress },
            store = store,
            scope = scope,
            transport = transport,
            serveTcp = true,
            bindHost = config.msBindHost,
            bindPort = config.msBindPort,
            electionTimeoutMillis = config.msElectionTimeoutMillis,
        )
        node.start()
        msNode = node
        msBoundPort = node.boundPort
        println("[YangBase] MS 节点已启动：${node.describe()}，监听 ${config.msBindHost}:$msBoundPort")
        println("[YangBase] 集群成员：${(peers + config.msAdvertiseAddress).distinct().joinToString(",")}")

        // ★ MS 进程**也要有观测端点**。
        //
        // 曾经 `--role ms` 在这里就结束了：三个 MS 节点既没有 `/metrics` 也没有
        // `/healthz`，于是"谁在当领导者、任期涨了几次、日志积压多少"全都看不见——
        // 而这些恰恰是共识层唯一需要盯着的东西。`/metrics` 里现在暴露
        // 本节点的角色、任期、已提交位点（`yb_ms_*`，由 Metrics 的探针读节点状态）
        config.metricsPort?.let { p ->
            msMetrics = node
            val h = MetricsHttpServer(config.metricsHost, p, health = { msHealth() })
            h.start()
            http = h
            println("[YangBase] 观测端点: http://${config.metricsHost}:${h.boundPort}/metrics | /healthz")
        }
    }

    /** CN 客户端与 MS 节点共用的传输：进程内优先，否则走 TCP。 */
    private fun coordinationTransport(): com.cloudea.yangbase.ms.MetaTransport =
        inProcessTransport ?: com.cloudea.yangbase.ms.TcpMetaTransport()

    private suspend fun startInProcessMeta(store: ObjectStore) {
        val transport = com.cloudea.yangbase.ms.InProcessMetaTransport()
        inProcessTransport = transport
        val peers = config.msPeers.ifEmpty { listOf(config.msNodeId) }
        val node = com.cloudea.yangbase.ms.MetaServiceNode(
            nodeId = config.msNodeId,
            address = config.msNodeId,
            peers = peers.filter { it != config.msNodeId },
            store = store,
            scope = scope,
            transport = transport,
            serveTcp = config.msBindPort > 0 || config.msPeers.isNotEmpty(),
            bindHost = config.msBindHost,
            bindPort = config.msBindPort,
        )
        node.start()
        msNode = node
        // 等它选出领导者再继续：否则启动期的第一条 DDL 会撞上"集群正在选举"
        // （单节点时选举只需要一个超时周期，几十到几百毫秒）
        val deadline = System.currentTimeMillis() + 5_000
        while (System.currentTimeMillis() < deadline &&
            node.raft.currentRole != com.cloudea.yangbase.ms.RaftNode.Role.LEADER
        ) {
            kotlinx.coroutines.delay(20)
        }
        println("[YangBase] 进程内 MS 节点已启动：${node.describe()}")
    }

    /**
     * catalog 失效轮询（§4.5 的兜底路径）。
     *
     * MS 模式的正常路径是**广播**失效，但广播会丢（进程刚好不在、网络抖动），
     * 而丢了的后果是静默的：那个 CN 一直按旧 schema 解码新行，多出来的列被丢掉。
     * 所以每 5 秒对一次版本号——它是一次极小的读，换的是"缓存不会永久陈旧"。
     */
    /**
     * 起空闲分片淘汰。
     *
     * **必须真的调起来**——一个不被调用的"清理机制"等于没有（PROJECT_GUIDE 里
     * 「建了不接」那条反模式）。默认 15 分钟空闲即淘汰：它释放的是内存
     * （MemTable + 常驻 `.idx`）与**每 7.5 秒一次的租约续期**。
     * `--shard-idle-evict-min` 传 0 可关闭（测试与小规模部署）。
     */
    private fun startShardEviction() {
        shardEvictor = engine.startIdleEviction(
            idleMillis = config.shardIdleEvictMillis,
            intervalMillis = 60_000,
        )
    }

    private fun startCatalogWatch() {
        val coord = coordination ?: return
        catalogWatch = scope.launch {
            // ⚠️ 失败**不能计入 `seen`**。曾经的写法先把版本号记账再重载，
            // 于是"这一次重载失败了"就永远不会重试——那个 CN 会一直停在旧目录上
            // （表显示不出来、或已删的表还看得见），直到下一次 DDL 顺带把它带过来。
            // 现在只有**重载成功**才推进 seen
            var seen = runCatching { coord.catalogVersion() }.getOrDefault(-1)
            while (true) {
                delay(5_000)
                val now = runCatching { coord.catalogVersion() }.getOrNull() ?: continue
                if (now == seen) continue
                runCatching { engine.catalog.load() }
                    .onSuccess { seen = now }
                    .onFailure {
                        // 失败要保持可见：这类"目录陈旧"的症状是"表莫名不见了/还在"，
                        // 与真正的原因（重载失败）隔得很远。指标让它在 /metrics 上可见
                        System.err.println(
                            "[YangBase] catalog 重新加载失败（下次轮询会重试）：${it::class.simpleName}: ${it.message}",
                        )
                        metrics.onCatalogReloadFailed()
                    }
            }
        }
    }

    /** MS 实际监听的端口（`role = ms` 时）。 */
    var msBoundPort: Int = -1
        private set

    fun start() = runBlocking {
        if (config.role.lowercase() == "ms") {
            startMetaServiceOnly()
            return@runBlocking
        }
        requireDurableStorageUnlessAllowed()

        // ── 装饰顺序：重试在外，埋点在内 ──────────────────────────────
        //
        // 重试层套在埋点**外面**，因为埋点要回答的问题是「这个进程**真实**向对象存储
        // 发了几次请求、传了多少字节」——而一次重试就是一次真实请求、一次真实计费。
        // 反过来套（埋点在外）会得到「调用方发起了几次操作」，重试三次也只记一次，
        // 于是 §14.2 的 IO 预算、§13 的成本估算与重试告警会**同时**失真，
        // 而这三者正是这个项目最依赖的反馈信号。
        //
        // 反过来的代价是：`yb_objectstore_requests_total` 会因重试而高于逻辑操作数。
        // 这不是缺点，是它应有的口径——它衡量的是成本，不是调用次数。
        //
        // 注入的 store（测试夹具）照样被套在里面，CountingObjectStore 的计数不受影响。
        val raw = injectedStore ?: buildStore(config)
        store = RetryingObjectStore(InstrumentedObjectStore(raw))

        // 实测后端能力，而不是相信厂商文档（设计文档 §3.3.1）。
        // 这一步是必须的：某些 S3 兼容后端会**静默忽略**条件头，
        // 在那种后端上运行会在并发下损坏数据却不报错。
        if (config.probeCapabilities) {
            val probe = com.cloudea.yangbase.objectstore.CapabilityProbe.probeAndMerge(store, store.caps)
            println("[YangBase] ${probe.report()}")
            // ★ 「关键项失败」还要再分一层：失败是**后端如实声明不支持**，
            // 还是**声称支持却没做到**？前者可以在知情的前提下降级（OSS 的 ETag CAS），
            // 后者必须拒绝启动——GCS 互操作层那种"静默忽略条件头"正是后者，
            // 在它上面跑会在并发下静默损坏数据
            val fatal = probe.findings.filter { it.critical && !it.passed }
                .filterNot { it.degradable && config.allowNonAtomicPointerSwitch }
            if (fatal.isNotEmpty()) {
                throw IllegalStateException(
                    "对象存储后端未通过关键能力探测，拒绝启动。\n" +
                        fatal.joinToString("\n") { "  - ${it.name}: ${it.detail}" } +
                        "\n本系统的正确性建立在条件写之上；在这种后端上运行会静默损坏数据。\n" +
                        (if (probe.findings.any { it.degradable && !it.passed })
                            "若这是**后端如实声明不支持 ETag CAS**（阿里云 OSS 就是这样），" +
                                "且你接受 manifest 指针切换失去原子性，可加 " +
                                "--i-accept-nonatomic-pointer-switch（见设计文档 §3.3）。\n"
                        else "") +
                        "若只是想做实验，传 --skip-capability-probe 显式跳过。",
                )
            }
            if (probe.findings.any { it.degradable && !it.passed } && config.allowNonAtomicPointerSwitch) {
                System.err.println(
                    "[YangBase] ⚠️⚠️ 已按 --i-accept-nonatomic-pointer-switch 启动：" +
                        "本后端没有 ETag CAS，manifest 指针切换**没有原子性保证**。" +
                        "只应在单写者部署下使用；风险与不变式见设计文档 §3.3 与 ManifestStore 的注释。",
                )
            }
        }

        startCoordination(store)
        engine = DatabaseEngine(
            store = store,
            allowNonAtomicPointerSwitch = config.allowNonAtomicPointerSwitch,
            scope = scope,
            nodeId = config.nodeId,
            cacheBytes = config.cacheBytes,
            commitMode = parseCommitMode(config.commitMode),
            coordination = coordination,
            diskCache = config.diskCacheDir?.let {
                com.cloudea.yangbase.storage.DiskCache(java.nio.file.Path.of(it), config.diskCacheBytes)
            },
            // 跳数索引的装配点：在这里把 yb-index 的实现注入存储层。
            // yb-storage / yb-txn 都不依赖索引实现，依赖方向因此保持单向
            skipObserverFactory = { table ->
                com.cloudea.yangbase.index.skip.SkipIndexBlockSkipper.skipIndexesOf(table).map { def ->
                    com.cloudea.yangbase.index.skip.SkipIndexBlockObserver(
                        store = store,
                        // 用 engine.cache_ 而不是当场取：lambda 是在
                        // DatabaseEngine 构造**期间**创建的，那时 engine 还没赋值。
                        // 它要到 flush / scan 时才被调用，那时一定已经就绪。
                        cache = engine.cache_,
                        def = com.cloudea.yangbase.index.skip.SkipIndexDef.of(
                            com.cloudea.yangbase.common.IndexId(def.id), def, table,
                        ),
                        schema = table,
                    )
                }
            },
            skipSkipperFactory = { table, ranges ->
                val idx = com.cloudea.yangbase.index.skip.SkipIndexBlockSkipper.skipIndexesOf(table)
                if (idx.isEmpty() || ranges.isEmpty()) null
                else com.cloudea.yangbase.index.skip.SkipIndexBlockSkipper(
                    store = store,
                    cache = engine.cache_,
                    indexes = idx,
                    schema = table,
                    ranges = ranges,
                )
            },
            memtableCapacity = config.memtableBytes,
        )
        engine.open()
        // 空闲分片淘汰：必须真的调起来（否则就是一个"建了不接"的清理机制）
        startShardEviction()

        // 若配置了初始库，自动创建，让第一次连接就能用
        config.defaultDatabase?.let { dbName ->
            if (engine.catalog.findDatabase(dbName) == null) {
                engine.catalog.createDatabase(dbName)
            }
        }

        // 可观测性（设计文档 §12.5）：观测点装在真实的调用路径上，
        // 而不是只把指标名注册出来。顺序有讲究——
        // 审计必须在 DDL 可能发生之前就装好（否则最早的几条 DDL 一个都不会被记下）。
        metrics = ServerMetrics(engine, slowQueryMillis = config.slowQueryMillis)
        if (config.auditEnabled) {
            val audit = ObjectStoreAuditLog(
                store = store,
                scope = scope,
                flushIntervalMillis = config.auditFlushMillis,
            )
            Audit.install(audit)
            audit.start()
            auditLog = audit
        }

        val server = MysqlServer(engine, MysqlServerConfig(config.host, config.port), metrics)
        server.start()
        mysql = server
        metrics.connectionCount = { server.liveConnectionCount }

        config.metricsPort?.let { p ->
            val h = MetricsHttpServer(config.metricsHost, p, health = { metrics.health() })
            h.start()
            http = h
            println("[YangBase] 观测端点: http://${config.metricsHost}:${h.boundPort}/metrics | /healthz")
        }

        println("[YangBase] 已启动，监听 ${config.host}:${server.boundPort}")
        println("[YangBase] 对象存储后端: ${store.capabilitiesSummary()}")
        println("[YangBase] 节点 ID: ${config.nodeId}")
        println("[YangBase] 审计: ${if (config.auditEnabled) "${ObjectKeys.auditPrefix()}{date}.jsonl（批量异步，失败不影响主流程）" else "已关闭"}")
    }

    fun stop() = runBlocking {
        http?.stop()
        http = null
        mysql?.stop()
        // 先把残留的审计写出去，再摘落点：反过来的话关闭期间的最后几条记录会丢。
        auditLog?.let {
            auditLog = null
            it.shutdown()
            Audit.uninstallIf(it)
        }
        if (::metrics.isInitialized) metrics.close()
        catalogWatch?.cancel()
        shardEvictor?.cancel()
        catalogWatch = null
        runCatching { msNode?.stop() }
        msNode = null
        runCatching { coordination?.close() }
        engine.close()
    }

    /**
     * 本地文件系统后端的**生产守卫**（设计文档 §3.3）。
     *
     * 本地 FS 后端存在的唯一目的是让开发和单元测试不需要真实对象存储——这句话
     * 反过来读就是它**不提供**多副本持久性，也只能在**单进程内**模拟条件写的原子性。
     * 而本系统全部的正确性论证（写者选举、manifest 指针切换、fencing、崩溃恢复）
     * 都建立在对象存储语义之上。「本地 FS 通过测试、生产环境却依赖对象存储语义」
     * 是最危险的一类缺陷来源：测试全绿，数据却没有第二份副本。
     *
     * 所以走这条路必须由操作者**显式声明**，参数名刻意写得刺眼——
     * 让它不可能被顺手写进启动脚本，而只能是有意为之。
     */
    private fun requireDurableStorageUnlessAllowed() {
        if (config.s3Bucket != null) return
        // 两个后端都没配时让 buildStore 去报错，这里不抢它的活
        if (config.dataDir == null) return
        if (config.allowNonDurableStorage) {
            println(
                "[YangBase] ⚠️⚠️ 警告：正在用**本地文件系统**当对象存储 " +
                    "(data-dir=${config.dataDir})：没有多副本、没有跨进程的条件写保证，" +
                    "进程一挂数据就可能没了。**仅限开发与测试**，不要用它承载任何真实数据。"
            )
            return
        }
        throw IllegalStateException(
            "拒绝启动：--data-dir 使用的是本地文件系统后端，它不是持久存储。\n" +
                "  它不提供条件写的跨进程原子性，也没有多副本；而本系统的写者选举、" +
                "manifest 指针切换、fencing 与崩溃恢复全部依赖对象存储的语义（设计文档 §3.3）。\n" +
                "  生产环境请改用 --s3-bucket（配合 --s3-endpoint / --s3-region / 密钥）。\n" +
                "  若确实只是开发或测试、且接受数据随时可能丢失，请显式加上：\n" +
                "      --i-know-this-is-not-durable"
        )
    }

    /** 阻塞直到进程被终止。 */
    fun awaitShutdown() {
        Runtime.getRuntime().addShutdownHook(Thread {
            println("[YangBase] 正在关闭…")
            runCatching { runBlocking { stop() } }
        })
        Thread.currentThread().join()
    }

    companion object {
        /** 测试入口：注入既有的 ObjectStore，端口传 0 由系统分配。 */
        fun forTesting(
            store: ObjectStore,
            port: Int = 0,
            /**
             * 块缓存容量。传 0 表示**完全不缓存**——IO 预算测试需要这个：
             * 缓存命中时两条不同的访问路径都表现为 0 次请求，
             * 于是「谁读得更少」变成 0 与 0 的比较，测了个寂寞。
             */
            cacheBytes: Long = 256L * 1024 * 1024,
            /** 二级缓存目录。测试「重启后缓存仍在」时必须传。 */
            diskCacheDir: String? = null,
            /** 观测端点端口。传 0 表示**由系统分配**（测试用它拿随机端口）；null 表示不启动。 */
            metricsPort: Int? = null,
            /** 慢查询阈值（毫秒）。 */
            slowQueryMillis: Long = 500,
            /**
             * 审计。**默认关闭**——既有测试里有大量「IO 预算」断言（断言请求数、
             * 断言对象数），审计会额外产生写入，把那些断言搅乱。要测审计的用例
             * 显式打开它，生产由 CLI 默认打开。
             */
            auditEnabled: Boolean = false,
            /**
             * 显式接受「manifest 指针切换没有原子性」——测 OSS 这类后端时必须传 true，
             * 否则分片会在第一次提交时明确失败（那是刻意的默认行为）。
             */
            allowNonAtomicPointerSwitch: Boolean = false,
        ): YangBaseServer = YangBaseServer(
            config = ServerConfig(
                host = "127.0.0.1", port = port, nodeId = "test-node",
                defaultDatabase = null, cacheBytes = cacheBytes, diskCacheDir = diskCacheDir,
                metricsPort = metricsPort, metricsHost = "127.0.0.1",
                slowQueryMillis = slowQueryMillis, auditEnabled = auditEnabled,
                allowNonAtomicPointerSwitch = allowNonAtomicPointerSwitch,
            ),
            injectedStore = store,
        )

        fun buildStore(config: ServerConfig): ObjectStore = when {
            config.s3Bucket != null -> S3ObjectStore.create(
                bucket = config.s3Bucket,
                endpoint = config.s3Endpoint,
                region = config.s3Region,
                accessKey = config.s3AccessKey,
                secretKey = config.s3SecretKey,
                // 阿里云 OSS 要求三级域名（虚拟主机式），MinIO 要求路径式——见 S3ObjectStore.create
                pathStyle = if (config.s3Oss) (config.s3PathStyle ?: false) else config.s3PathStyle,
                // OSS 的原语与 S3 不同：原子创建走 x-oss-forbid-overwrite、没有 ETag CAS。
                // 声明成 OSS 预设之后，能力探测会**实测**这些声明，而不是相信它们
                caps = if (config.s3Oss) ObjectStoreCapabilities.OSS else ObjectStoreCapabilities.FULL,
            )
            config.dataDir != null -> LocalFileObjectStore(Path.of(config.dataDir))
            else -> throw IllegalArgumentException(
                "必须指定对象存储后端：--data-dir（本地，仅开发）或 --s3-bucket（生产）"
            )
        }
    }
}

/**
 * 时间戳窗口的**按模式默认值**。
 *
 * 为什么不是一个统一的值：窗口同时决定两件相反的事——
 *
 * - **协调请求频率**（每写满一个窗口一次）；
 * - **跨 CN 可见性延迟 ≤ 一个窗口**（窗口是独占的，见 `WindowTso.batchMillis`）。
 *
 * 而这两件事只在**多 CN** 时才冲突：单 CN 部署下没有任何"另一个 CN"，窗口开多大
 * 都不会有可见性问题，于是取 64 秒把请求数压到最低（一次冷启动点查的请求预算
 * `IoBudgetIntegrationTest` 就是按这个口径定的）。多 CN 模式取 1 秒：
 * 可见性延迟 ≤1 秒，代价是写入期间每秒一次协调请求（MS 模式是内存里的共识轮次，
 * 对象存储模式是一次条件写）。
 */
private fun defaultTsoBatchMillis(coordMode: String): Long = when (coordMode) {
    "single" -> 64_000L
    else -> 1_000L
}

/** 服务端配置。生产环境必须走 S3 分支。 */
data class ServerConfig(
    val host: String = "0.0.0.0",
    val port: Int = 3306,
    val nodeId: String = "cn-1",
    /** 本地文件系统后端（仅开发测试）。 */
    val dataDir: String? = null,
    /**
     * 允许用**非持久**的本地文件系统后端启动（设计文档 §3.3）。
     *
     * 默认 `false`：本地 FS 后端没有多副本，也只在单进程内模拟条件写。默认关闭意味着
     * 「不小心把 --data-dir 带进了生产启动脚本」会**拒绝启动**，而不是静默地在一个
     * 没有第二份副本的地方跑起一个数据库——后者在出事之前看起来一切正常。
     *
     * 测试必须显式写 `true`：让每个用例自己声明「我知道这个后端不可持久」，
     * 比让守卫退化成一句警告诚实得多。
     */
    val allowNonDurableStorage: Boolean = false,
    val s3Bucket: String? = null,
    val s3Endpoint: String? = null,
    val s3Region: String = "us-east-1",
    val s3AccessKey: String? = null,
    val s3SecretKey: String? = null,
    /** 寻址风格：null = 自动（给了 endpoint 用路径式）；OSS 要显式传 false。 */
    val s3PathStyle: Boolean? = null,
    /**
     * 后端是阿里云 OSS：改用它的原语（`x-oss-forbid-overwrite` 做原子创建、
     * 三级域名寻址），并如实声明「没有 ETag CAS」。
     */
    val s3Oss: Boolean = false,
    /**
     * ★ 显式接受「manifest 指针切换没有原子性」的后端（OSS 这一类）。
     *
     * 不打开它，OSS 上会在能力探测阶段就拒绝启动——这是刻意的默认。
     * 打开它的含义与风险见 `ManifestStore` 的注释：宁可拒绝服务，也不要**悄悄**
     * 失去互斥；所以要的是"知情同意"，而不是一个悄悄生效的兼容开关。
     */
    val allowNonAtomicPointerSwitch: Boolean = false,
    val cacheBytes: Long = 256L * 1024 * 1024,
    /** 二级缓存目录（本地 NVMe）。为 null 表示不启用（设计文档 §6.7）。 */
    val diskCacheDir: String? = null,
    val diskCacheBytes: Long = 10L * 1024 * 1024 * 1024,
    val memtableBytes: Long = 32L * 1024 * 1024,
    /**
     * 提交档：`sync`（默认，RPO=0）/ `batch`（高吞吐导入，RPO=0）。
     * `async` 会带来 RPO > 0，目前**未实现**（见 MysqlServer/Executor 的说明）。
     */
    val commitMode: String = "sync",
    /**
     * 协调模式（设计文档 §4.7 的 Q2 决策）。
     *
     *  - `single`（默认）：假设同一分片只有一个 CN 在写。正确、零开销，
     *    对应单 CN 部署——**协调由部署方式保证**。
     *  - `objectstore`：用对象存储的条件写做分片写者选举与授时。
     *    多 CN 部署必须用它，否则两个 CN 会往同一个 WAL 前缀下写对象。
     */
    /**
     * 进程角色：`cn`（默认，计算节点）/ `ms`（只跑元数据服务）/ `both`（同一进程里两者都有）。
     *
     * 设计文档 §2.1 的部署视图是 `CN × N + MS × 3` 两个独立的进程组。把角色做成
     * 一个开关而不是两个可执行文件，是因为它们的代码路径共享同一个 `ObjectStore`
     * 装配与能力探测——而"MS 跑在哪个进程里"是部署形态，不是代码结构。
     */
    val role: String = "cn",
    val coordMode: String = "single",
    /**
     * MS 集群的地址列表（`host:port`）。`coordMode = "ms"` 时必填。
     *
     * 空列表 + [runMetaService] = 本进程内起一个**单节点 MS**：最小部署形态
     * （1 CN + 1 MS 同进程），不需要任何网络配置。
     */
    val msPeers: List<String> = emptyList(),
    /**
     * 本进程是否同时运行一个 MS 节点。
     *
     * 生产上 MS 应当是**独立进程**（3 台小规格实例，§13 的成本量化在那里）。
     * 但对单机部署与测试，"CN 里带一个 MS"是合法且方便的最小形态：
     * 协调与数据的边界仍然画在 `CoordinationService` 上，换成 3 个独立 MS
     * 只需要改地址列表。
     */
    val runMetaService: Boolean = false,
    /** MS 节点的监听地址（独立部署时用）。 */
    val msBindHost: String = "0.0.0.0",
    val msBindPort: Int = 0,
    /** 本节点在 MS 集群里的标识。 */
    val msNodeId: String = "ms-1",
    /** 本 MS 节点对外宣告的地址（`host:port`），其他节点与 CN 用它连过来。 */
    val msAdvertiseAddress: String = "",
    /**
     * 空闲分片多久之后被淘汰（毫秒）。默认 15 分钟；**0 表示关闭淘汰**。
     *
     * 淘汰会 `flush` 并释放租约，下次访问时重新打开（并重新竞选租约，epoch 递增）。
     * 对"曾经碰过、现在没人用"的分区，它省下的是常驻内存与持续的续约请求。
     */
    val shardIdleEvictMillis: Long = 15 * 60 * 1000,

    /**
     * MS 的选举超时区间（毫秒）。生产用默认值即可；测试调小以加快故障切换。
     *
     * ⚠️ 默认值比"内存共识"的经验值大一个量级，因为**MS 的日志落在对象存储上**：
     * 一次日志追加 = 一次 PUT（跟随者同样要 PUT 才回包），往返是几毫秒到几十毫秒。
     * 选举超时只有 150ms 时，一次慢复制就足以让跟随者发起选举，集群会在负载下
     * 反复换领导者——不报错，只是慢，以及每次重新选举都会丢掉锁表（§4.4）。
     * 复制的等待上限由这里推导（见 `RaftNode.replicateTimeoutMillis`），
     * 所以调这一个参数就够了。
     */
    val msElectionTimeoutMillis: LongRange = DEFAULT_MS_ELECTION_TIMEOUT,
    /** 租约时长（毫秒）。必须**显著大于**分片恢复时间（§6.8 估算是 3–5s）。 */
    val leaseTtlMillis: Long = 15_000,
    /**
     * 一次时间戳预取覆盖多少**毫秒**（不是多少个号，见 `TimestampSource`）。
     *
     * 它同时决定「协调请求频率」与「跨 CN 的可见性延迟」：后者 **≤ 一个窗口**，
     * 因为窗口是独占的——把水位推给 CN-A 一整个窗口，CN-B 的下一个窗口就落在
     * 未来最多一个窗口处（详见 `WindowTso.batchMillis` 的说明）。
     *
     * 默认按模式取（见 [defaultTsoBatchMillis]）：**单 CN 用 64 秒**（没有跨 CN
     * 可见性问题，越省越好），**多 CN 模式用 1 秒**（可见性延迟 ≤1 秒，
     * 代价是写入期间每窗口一次协调请求）。
     * 显式设 0 用于**验证降级**：每次取号都要问权威方，MS 挂了立刻可见。
     */
    val tsoBatchMillis: Long? = null,
    val defaultDatabase: String? = "default",
    /** 启动时实测对象存储能力。默认开启——关键能力不通过就拒绝启动。 */
    val probeCapabilities: Boolean = true,

    // ── 可观测性（设计文档 §12.5）───────────────────────────────────

    /**
     * 观测端点（`/metrics` + `/healthz`）的端口。**null 表示不启动**（默认）。
     *
     * 传 0 表示由系统分配端口（测试用）。默认关闭是刻意的：数据库不该在运维
     * 没要求时多开一个监听端口。
     */
    val metricsPort: Int? = null,
    /** 观测端点绑定地址。默认只绑回环——指标里带库名表名，不该默认暴露到公网。 */
    val metricsHost: String = "127.0.0.1",
    /**
     * 慢查询阈值（毫秒）。超过它的语句会打一行日志，内容是
     * SQL、耗时、对象存储读取明细、行数（§12.5 要求能回答
     * 「为什么这个查询读了 500 个对象」）。
     */
    val slowQueryMillis: Long = 500,
    /**
     * 审计日志（`ops/audit/{date}.jsonl`）。默认**开启**：
     * 只有 DDL 与 GC 会产生记录，而空闲时一次请求都不会发（缓冲为空时
     * 刷新直接返回）。它是唯一被允许写对象的观测手段（原则 P4 的例外）。
     */
    val auditEnabled: Boolean = true,
    /** 审计的批量刷新间隔（毫秒）。攒批就是为了让 N 条记录只花 1 次 PUT。 */
    val auditFlushMillis: Long = 2_000,
) {
    companion object {
        /**
         * MS 选举超时的默认区间（毫秒）。见 [msElectionTimeoutMillis] 的说明——
         * 它比"内存共识"的经验值大一个量级，因为 MS 的日志落在对象存储上。
         */
        val DEFAULT_MS_ELECTION_TIMEOUT: LongRange = 500L..1_000L
    }
}

private fun ObjectStore.capabilitiesSummary(): String = buildString {
    append("条件写=").append(if (caps.conditionalCreate) "是" else "否")
    append(" 强一致读=").append(if (caps.strongReadAfterWrite) "是" else "否")
    append(" 强一致LIST=").append(if (caps.strongListConsistency) "是" else "否")
    append(" 条件写已验证=").append(if (caps.conditionalWriteVerified) "是" else "否")
}

/** 命令行入口。 */
fun main(args: Array<String>) {
    val opts = parseArgs(args)
    if (opts.containsKey("help")) {
        println(
            """
            用法: yangbase-server [选项]
              --host <addr>        监听地址（默认 0.0.0.0）
              --port <n>           监听端口（默认 3306）
              --node-id <id>       节点标识（默认 cn-1）
              --data-dir <path>    使用本地文件系统作为对象存储（仅开发测试）。
                                   必须同时显式加上 --i-know-this-is-not-durable
              --i-know-this-is-not-durable
                                   确认接受「本地文件系统后端」：它没有多副本持久性，
                                   也只在本进程内模拟条件写。生产环境永远不要加它
              --s3-bucket <name>   使用 S3 兼容对象存储（生产）
              --s3-endpoint <url>  S3 端点（MinIO 等自建网关需要）
              --s3-region <reg>    S3 区域
              --s3-access-key <k>  访问密钥
              --s3-secret-key <s>  私钥
              --s3-path-style <b>  寻址风格：true=路径式（MinIO 默认如此）/ false=虚拟主机式
                                   （**阿里云 OSS 必须传 false**，它拒绝路径式：SecondLevelDomainForbidden）
              --s3-oss             后端是阿里云 OSS：改用它的原子创建原语
                                   （x-oss-forbid-overwrite）并如实声明「无 ETag CAS」，
                                   寻址默认切到三级域名
              --i-accept-nonatomic-pointer-switch
                                   ★ 显式接受「manifest 指针切换没有原子性」。
                                   OSS 这类后端没有 ETag CAS，而每次 flush/compaction
                                   都要切指针；不开这个开关，服务端会在能力探测阶段
                                   拒绝启动（默认行为）。打开前请读设计文档 §3.3
              --cache-mb <n>       块缓存大小，单位 MiB（默认 256）
              --commit-mode <m>    提交档：sync（默认，RPO=0）/ batch（高吞吐导入，RPO=0）
              --role <r>           进程角色：cn（默认）/ ms（只跑元数据服务）/ both
              --coord-mode <m>     协调模式：single（默认，单 CN）/ objectstore / ms
              --tso-batch-ms <n>   时间戳窗口（毫秒）。默认：单 CN 模式 64000，多 CN 模式 1000。
                                   它是「协调请求频率」与「跨 CN 可见性延迟」的取舍：
                                   窗口是独占的，跨 CN 写入的可见延迟 ≤ 一个窗口
              --ms-peers <list>    MS 集群地址，逗号分隔（host:port）。coord-mode=ms 时必填，
                                   留空 + --run-meta-service 表示本进程内起一个单节点 MS
              --run-meta-service   本进程内同时运行一个 MS 节点（最小部署与测试用）
              --ms-node-id <id>    本 MS 节点的标识（默认 ms-1）
              --ms-bind-host <h>   MS 监听地址（默认 0.0.0.0）
              --ms-bind-port <n>   MS 监听端口（--role ms 时必填）
              --ms-election-timeout-ms <a-b>
                                   MS 选举超时区间，毫秒（默认 500-1000）。写单个数字
                                   表示区间为 [n, 2n]。复制的等待上限由它推导：
                                   后端慢（跨 Region、高延迟对象存储）就调大，
                                   调小只会在负载下反复重新选举并丢掉锁表
              --shard-idle-evict-min <n>
                                   空闲分片淘汰阈值，分钟（默认 15，0 = 关闭）。淘汰会
                                   flush 并释放租约，省下的是常驻内存与每 7.5 秒一次的续约
              --lease-ttl-ms <n>   分片写者租约时长，默认 15000
              --disk-cache-dir <d> 二级缓存目录（本地 NVMe）。CN 重启后缓存不丢，
                                   能让「重新变热」不产生对象存储请求
              --disk-cache-mb <n>  二级缓存容量上限，单位 MiB（默认 10240）
              --skip-capability-probe  跳过启动时的对象存储能力实测（不推荐）
              --metrics-port <n>  打开观测端点 /metrics 与 /healthz（默认关闭）。
                                  0 表示由系统分配端口
              --metrics-host <a>  观测端点绑定地址（默认 127.0.0.1，只绑回环）
              --slow-query-ms <n> 慢查询阈值，单位毫秒（默认 500）
              --no-audit          关闭审计日志 ops/audit/{date}.jsonl（默认开启）
            """.trimIndent()
        )
        return
    }

    val metricsPort = opts["metrics-port"]?.let { raw ->
        val p = raw.toIntOrNull()
        require(p != null && p in 0..65535) {
            "无效的 --metrics-port '$raw'（0 表示由系统分配端口）"
        }
        p
    }

    val config = ServerConfig(
        host = opts["host"] ?: "0.0.0.0",
        port = opts["port"]?.toIntOrNull() ?: 3306,
        nodeId = opts["node-id"] ?: "cn-1",
        dataDir = opts["data-dir"],
        allowNonDurableStorage = opts.containsKey("i-know-this-is-not-durable"),
        s3Bucket = opts["s3-bucket"],
        s3Endpoint = opts["s3-endpoint"],
        s3Region = opts["s3-region"] ?: "us-east-1",
        s3AccessKey = opts["s3-access-key"],
        s3SecretKey = opts["s3-secret-key"],
        s3PathStyle = opts["s3-path-style"]?.toBooleanStrictOrNull(),
        s3Oss = opts.containsKey("s3-oss"),
        allowNonAtomicPointerSwitch = opts.containsKey("i-accept-nonatomic-pointer-switch"),
        cacheBytes = (opts["cache-mb"]?.toLongOrNull() ?: 256) * 1024 * 1024,
        commitMode = opts["commit-mode"] ?: "sync",
        role = opts["role"] ?: "cn",
        coordMode = opts["coord-mode"] ?: "single",
        msPeers = (opts["ms-peers"] ?: "").split(",").map { it.trim() }.filter { it.isNotEmpty() },
        tsoBatchMillis = opts["tso-batch-ms"]?.toLongOrNull(),
        runMetaService = opts.containsKey("run-meta-service") || opts.containsKey("run-ms"),
        msNodeId = opts["ms-node-id"] ?: "ms-1",
        msBindHost = opts["ms-bind-host"] ?: "0.0.0.0",
        msBindPort = opts["ms-bind-port"]?.toIntOrNull() ?: 0,
        msElectionTimeoutMillis = parseMillisRange(
            opts["ms-election-timeout-ms"],
            ServerConfig.DEFAULT_MS_ELECTION_TIMEOUT,
            "--ms-election-timeout-ms",
        ),
        msAdvertiseAddress = opts["ms-advertise"] ?: (opts["ms-bind-port"]?.let {
            "${opts["ms-bind-host"] ?: "127.0.0.1"}:$it"
        } ?: ""),
        leaseTtlMillis = (opts["lease-ttl-ms"]?.toLongOrNull() ?: 15000),
        shardIdleEvictMillis = (opts["shard-idle-evict-min"]?.toLongOrNull() ?: 15L) * 60_000,
        diskCacheDir = opts["disk-cache-dir"]?.takeIf { it.isNotEmpty() },
        diskCacheBytes = (opts["disk-cache-mb"]?.toLongOrNull() ?: 10240) * 1024 * 1024,
        probeCapabilities = !opts.containsKey("skip-capability-probe"),
        metricsPort = metricsPort,
        metricsHost = opts["metrics-host"] ?: "127.0.0.1",
        slowQueryMillis = opts["slow-query-ms"]?.toLongOrNull() ?: 500,
        auditEnabled = !opts.containsKey("no-audit"),
    )

    val server = YangBaseServer(config)
    server.start()
    server.awaitShutdown()
}

/**
 * 解析一个毫秒区间：`500-1000` 或单个 `800`（表示 `[800, 1600]`）。
 *
 * 与提交档同样的立场：**不认识的值宁可拒绝启动，也不要悄悄退回默认**——
 * 「配了 A 却按 B 跑」是比「不支持 A」更难发现的问题。选举超时尤其如此：
 * 它被调小之后的症状是"集群时不时重新选举"，看起来与配置毫无关系。
 */
internal fun parseMillisRange(raw: String?, default: LongRange, flag: String): LongRange {
    if (raw == null) return default
    val parts = raw.split("-").map { it.trim() }
    val nums = parts.map { it.toLongOrNull() }
    require(nums.isNotEmpty() && nums.size <= 2 && nums.all { it != null && it > 0 }) {
        "无效的 $flag '$raw'（期望 `a-b` 或单个数字 n，单位毫秒）"
    }
    return if (nums.size == 1) nums[0]!!..(nums[0]!! * 2) else nums[0]!!..nums[1]!!
}

/**
 * 解析提交档。
 *
 * **不认识的值宁可拒绝启动，也不要悄悄退回默认**——「配了 A 却按 B 跑」
 * 是比「不支持 A」更难发现的问题。
 */
/**
 * 构造租约提供者（设计文档 §4.3 / §4.7）。
 *
 * 模式不认识时**拒绝启动**而不是静默退回 single——
 * 「配了 objectstore 却按 single 跑」意味着多 CN 下会静默损坏数据：
 * 两个 CN 会往同一个 WAL 前缀下写对象，输的一方写下的日志
 * 仍会被下一个恢复者当成有效日志重放。
 */
private fun buildLeaseProvider(
    config: ServerConfig,
    store: com.cloudea.yangbase.objectstore.ObjectStore,
): com.cloudea.yangbase.storage.LeaseProvider = when (config.coordMode.lowercase()) {
    "single" -> com.cloudea.yangbase.storage.SingleProcessLease
    "objectstore", "object-store", "object_store" ->
        com.cloudea.yangbase.txn.LeaseManager(store, config.nodeId, config.leaseTtlMillis)
    else -> throw IllegalArgumentException(
        "未知的 coord-mode '${config.coordMode}'（支持 single / objectstore / ms）"
    )
}

private fun parseCommitMode(s: String): com.cloudea.yangbase.storage.CommitMode = when (s.lowercase()) {
    "sync" -> com.cloudea.yangbase.storage.CommitMode.SYNC
    "batch" -> com.cloudea.yangbase.storage.CommitMode.BATCH
    "async" -> throw IllegalArgumentException(
        "commit_mode=async 尚未实现：它要求「不等 PUT 完成就 ACK」，" +
            "而当前组提交写者会等待落盘。开启它会得到 RPO=0 的行为却对外声称 RPO>0。" +
            "见设计文档 §6.3.4。"
    )
    else -> throw IllegalArgumentException("未知的 commit_mode '$s'（支持 sync / batch）")
}

private fun parseArgs(args: Array<String>): Map<String, String> {
    val out = HashMap<String, String>()
    var i = 0
    while (i < args.size) {
        val a = args[i]
        if (a.startsWith("--")) {
            val key = a.substring(2)
            val next = args.getOrNull(i + 1)
            if (next != null && !next.startsWith("--")) {
                out[key] = next; i += 2
            } else {
                out[key] = ""; i += 1
            }
        } else i++
    }
    return out
}
