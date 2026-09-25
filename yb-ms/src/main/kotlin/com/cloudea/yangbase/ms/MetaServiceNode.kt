package com.cloudea.yangbase.ms

import com.cloudea.yangbase.objectstore.ObjectStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel

/**
 * 一个 MS 进程（设计文档 §2.1：`Meta Service (MS) × 3`，Raft，可整体重启）。
 *
 * 它把三样东西装在一起：
 *
 * ```
 *   RaftNode ──(日志/快照)──▶ 对象存储（meta/ms/...）
 *      │
 *      ├─ MetaStateMachine（水位 / 租约 / catalog 版本）—— 进共识
 *      └─ MsLockTable（行锁 / 表锁）—— **不进共识**（§4.4）
 *            ▲
 *            │  MetaRpcHandler
 *      MetaTransport（进程内 / TCP）
 * ```
 *
 * ## 它为什么可以"整体重启"
 *
 * §4.1 的关键设计点：MS 的四个职责**没有一个持有权威副本**——
 * catalog 的真相在对象存储上，租约与锁是短期的，时间戳水位只要单调。所以
 * 三个 MS 全部挂掉再起来，系统能回到一致状态（代价是期间写事务不可用）。
 *
 * ## 单进程部署时它也可以活在 CN 里
 *
 * [InProcessMetaTransport] 让 MS 与 CN 同进程，于是"1 个 CN + 1 个 MS"这种
 * 最小部署不需要任何网络配置。这不是玩具模式：低频应用的单节点部署完全可以这样，
 * 而它仍然把"协调"与"数据"的边界画在了接口上——换成 3 个独立 MS 只需要换传输。
 */
class MetaServiceNode(
    val nodeId: String,
    /** 自己的地址。进程内模式是任意标识串，TCP 模式是 `host:port`。 */
    val address: String,
    /** 其他 MS 节点的地址。 */
    peers: List<String>,
    store: ObjectStore,
    scope: CoroutineScope,
    private val transport: MetaTransport,
    /** 是否起 TCP 服务端（多进程部署为 true；单进程/测试为 false）。 */
    private val serveTcp: Boolean = false,
    private val bindHost: String = "0.0.0.0",
    private val bindPort: Int = 0,
    // 默认值与 `RaftNode` 一致：日志在对象存储上，超时取内存共识的量级会让集群
    // 在负载下反复重新选举（理由见 `RaftNode.electionTimeoutMillis`）
    electionTimeoutMillis: LongRange = 500L..1_000L,
    heartbeatMillis: Long = 50,
) : MetaRpcHandler {

    val raft = RaftNode(
        nodeId = nodeId,
        peers = peers,
        selfAddress = address,
        transport = transport,
        store = store,
        scope = scope,
        electionTimeoutMillis = electionTimeoutMillis,
        heartbeatMillis = heartbeatMillis,
    )

    private var tcp: TcpMetaServer? = null

    /**
     * 这个节点是否已经停掉。
     *
     * 停掉的节点**仍然是 LEADER**（`stop()` 不改角色），而集群视角里它已经不是成员了。
     * 少了这个标记，测试助手会把一个已经停掉的节点当成"当前领导者"——
     * 表现为"故障切换之后还是原来的领导者"，看起来像共识没生效。
     */
    @Volatile
    var stopped: Boolean = false
        private set

    /** TCP 实际监听的端口（`bindPort = 0` 时由系统分配）。 */
    val boundPort: Int get() = tcp?.port ?: bindPort

    suspend fun start() {
        // 先注册进传输层再启动 Raft：否则选举的第一轮消息会打到一个还不存在的节点上
        (transport as? InProcessMetaTransport)?.register(address, this)
        raft.start()
        if (serveTcp) {
            tcp = TcpMetaServer(bindHost, bindPort, this).also { it.start() }
        }
    }

    suspend fun stop() {
        stopped = true
        tcp?.stop()
        tcp = null
        (transport as? InProcessMetaTransport)?.unregister(address)
        raft.stop()
    }

    override suspend fun handle(request: MetaRequest): MetaResponse = raft.handle(request)

    override suspend fun handleRaft(message: RaftMessage): RaftMessage = raft.handleRaft(message)

    fun describe(): String = raft.describe()
}

/** 一组 MS 节点的**无状态**视图：CN 侧用它做重定向与降级。 */
class MetaCluster(val addresses: List<String>) {

    init {
        require(addresses.isNotEmpty()) { "MS 集群至少要有一个地址" }
    }

    /** 多数派大小。用于诊断"当前有几个 MS 活着"。 */
    val quorum: Int get() = addresses.size / 2 + 1

    override fun toString() = "MetaCluster(${addresses.joinToString(",")})"
}

/**
 * 起一个**完整的进程内 MS 集群**：`nodes` 个节点 + 共享的传输层。
 *
 * 这是单机部署与全部自动化测试用的入口。它把"3 个节点"变成纯内存对象，
 * 于是故障切换测试是确定性的、毫秒级的——不需要等 TCP 超时，也不需要 3 个进程。
 */
class InProcessMetaCluster(
    val nodes: List<MetaServiceNode>,
    val transport: InProcessMetaTransport,
) {
    companion object {
        suspend fun start(
            count: Int,
            store: ObjectStore,
            scope: CoroutineScope,
            nodePrefix: String = "ms",
            // 默认值与 `RaftNode` 一致：日志在对象存储上，超时取内存共识的量级会让集群
    // 在负载下反复重新选举（理由见 `RaftNode.electionTimeoutMillis`）
    electionTimeoutMillis: LongRange = 500L..1_000L,
            heartbeatMillis: Long = 50,
        ): InProcessMetaCluster {
            val transport = InProcessMetaTransport()
            val addresses = (1..count).map { "$nodePrefix-$it" }
            val nodes = addresses.mapIndexed { i, addr ->
                MetaServiceNode(
                    nodeId = addr,
                    address = addr,
                    peers = addresses.filter { it != addr },
                    store = store,
                    scope = scope,
                    transport = transport,
                    electionTimeoutMillis = electionTimeoutMillis,
                    heartbeatMillis = heartbeatMillis,
                )
            }
            nodes.forEach { it.start() }
            return InProcessMetaCluster(nodes, transport)
        }
    }

    val addresses: List<String> get() = nodes.map { it.address }

    /**
     * 等到有一个领导者为止。
     *
     * 测试里**必须**用它而不是 `delay(500)` 之类的固定等待：固定等待在慢机器上会
     * 随机失败，而失败的表现是"写入超时"，看起来像被测代码的缺陷。
     */
    suspend fun awaitLeader(timeoutMillis: Long = 5_000): MetaServiceNode {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline) {
            nodes.firstOrNull { !it.stopped && it.raft.currentRole == RaftNode.Role.LEADER }?.let { return it }
            kotlinx.coroutines.delay(10)
        }
        throw IllegalStateException("$timeoutMillis ms 内没有选出领导者：${nodes.map { it.describe() }}")
    }

    /** 当前领导者（没有就返回 null）。 */
    fun leaderOrNull(): MetaServiceNode? =
        nodes.firstOrNull { !it.stopped && it.raft.currentRole == RaftNode.Role.LEADER }

    suspend fun stopAll() {
        nodes.forEach { runCatching { it.stop() } }
    }
}
