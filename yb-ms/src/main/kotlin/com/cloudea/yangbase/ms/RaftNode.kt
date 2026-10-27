package com.cloudea.yangbase.ms

import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.objectstore.ObjectStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.max
import kotlin.random.Random

/**
 * 元数据服务的 Raft 节点（设计文档 §4、§4.6）。
 *
 * ## 为什么 MS 需要共识，而数据面不需要
 *
 * 数据面（CN）靠"每分片单写者"消解了几乎所有并发冲突，所以它不需要共识——
 * 这是 §6.3 的核心收益。剩下的三件事**必须**有一个全系统唯一的判断：
 *
 * 1. **时间戳水位**：两个 CN 各自预留出重叠的区间 → 重复的 `commit_ts` → MVCC 失去全序；
 * 2. **租约**：两个 CN 都以为自己持有同一分片 → 往同一个 WAL 前缀下写对象；
 * 3. **DDL 的串行化**：两个并发的 `ALTER` 各自基于旧 schema 生成新版本。
 *
 * 这三件的共同点是"必须有一个权威的**先后**"，而这正是共识能提供的唯一东西。
 *
 * ## 实现取舍（与设计文档的差异都列在这里）
 *
 * | 项 | 这里的做法 | 与文档的差异与理由 |
 * |---|---|---|
 * | 日志持久化 | 每条日志项单独 PUT，**在回包前等它落盘** | 文档说可以攒批 100ms。攒批会让"多数派已确认"的日志在崩溃后可能消失——那是丢数据，不是省请求 |
 * | 日志布局 | `meta/ms/log/{nodeId}/...`（**按节点分目录**） | §4.6 的布局看起来是共享的 `log/{term}/{index}`。标准 Raft 的每个节点有**自己的**日志；共享日志是另一种算法（Warp/LogCabin 那一类） |
 * | 快照安装 | 一次发完（不分块） | MS 的状态只有水位 + 租约表 + catalog 版本，快照几 KB |
 * | 日志压缩 | 每 [snapshotThreshold] 条做一次快照并截断 | 文档未定；不压缩会让冷启动重放变慢（§4.6 目标是 <3s） |
 *
 * ## 与"选举风暴"有关的一个细节
 *
 * 选举超时取随机区间（默认 500ms–1s，见下方参数说明），并且**每个节点不同**。
 * 写在注释里是因为它看起来像个小参数，实则是 Raft 唯一防"选票被反复瓜分"的机制：
 * 所有节点用同一个超时值会让它们同时变成候选人，然后同时失败、同时重试，
 * 永远选不出领导者。
 */
class RaftNode(
    val nodeId: String,
    /** 其他节点的地址（不含自己）。 */
    val peers: List<String>,
    val selfAddress: String,
    private val transport: MetaTransport,
    private val store: ObjectStore,
    private val scope: CoroutineScope,
    /**
     * 选举超时区间（毫秒）。随机取值，且**每个节点不同**（见类注释的最后一段）。
     *
     * 默认值比"内存共识"的经验值大一个量级，因为**日志与硬状态都落在对象存储上**：
     * 一次追加就是一次 PUT。取 150ms 时，一次慢复制就足以让跟随者发起选举，
     * 集群会在负载下反复换领导者——不报错，只是慢，以及每次重选都丢掉锁表。
     */
    private val electionTimeoutMillis: LongRange = 500L..1_000L,
    private val heartbeatMillis: Long = 50,
    private val snapshotThreshold: Int = 64,
    /**
     * 一轮复制的等待上限。超时视为该轮失败（领导者会重试）。
     *
     * **由选举超时推导，不是独立参数。** 两者必须满足「复制等待 ≪ 选举超时」：
     * 不满足的后果是一个慢跟随者把领导者的复制循环堵住（见 [replicate] 的并发说明），
     * 其他跟随者等不到心跳 → 发起选举 → 领导者被换掉，表现为「集群时不时重新选举」。
     *
     * 写成推导式而不是两个独立数字，是因为它们**曾经**是两个独立数字：默认的一对
     * （选举 150–300ms、复制等待 200ms）就违反了这个关系。协调模式基准把它测了出来
     * ——MS 的日志在对象存储上，一次 PUT 就是几毫秒，一个往返很容易逼近 150ms，
     * 于是集群在租约授予时反复重新选举（一次 acquire+release 产生 20 次对象存储请求，
     * 而该有的只有 6 次）。
     */
    private val replicateTimeoutMillis: Long = (electionTimeoutMillis.first / 2).coerceAtLeast(50),
    /**
     * 客户端等"这条日志被多数派确认"的上限（毫秒）。超时回 [MetaResponse.Error]（提交超时）。
     *
     * 默认由 [replicateTimeoutMillis] 推导（两轮复制的量），**生产行为不变**。
     * 留成可覆盖的参数，是因为这个值只对"一轮复制能在多久内完成"有意义，
     * 而这件事取决于**后端存储的延迟**，不是共识算法：内存 store 是纳秒级，
     * 内嵌 S3 走 HTTP + 真实 PUT 是毫秒级，CI 上 2 核 runner 跑集成测试时还会更慢。
     * 集成测试因此需要比默认值宽松得多的预算，而**不应该**为此去放大选举超时
     * （那会同时改变集群的重选行为，测试就不再测它本来要测的东西）。
     */
    commitTimeoutMillisOverride: Long? = null,
) : MetaRpcHandler {

    /** 提交等待的实际预算：[commitTimeoutMillisOverride] 优先，否则由复制超时推导。 */
    private val commitTimeoutMillis: Long = commitTimeoutMillisOverride ?: (replicateTimeoutMillis * 2)

    /**
     * 一轮拉票的等待上限。
     *
     * **不能**沿用 [replicateTimeoutMillis]（那是「把一批日志推给一个跟随者」的预算）：
     * 一轮投票对跟随者来说是「落盘硬状态 + 回包」，即一次完整的 PUT。高延迟后端上
     * 这就是几百毫秒，而 [replicateTimeoutMillis] 是按「选举超时的一半」推的，拿它当
     * 投票截止时间，等于要求跟随者在半个选举窗口内完成一次落盘——超时的那一票直接丢，
     * 集群一直凑不齐多数派。
     *
     * 上限取一个完整的最小选举窗口：再长就没意义了，因为超过它之后候选者自己就会
     * 重新发起一轮选举。
     */
    private val voteTimeoutMillis: Long = electionTimeoutMillis.first

    enum class Role { FOLLOWER, CANDIDATE, LEADER }

    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    private val mutex = Mutex()

    // ── 持久状态（每次变更都要先落对象存储，再回包）────────────────
    private var currentTerm: Long = 0
    private var votedFor: String? = null
    /** 日志，下标从 1 开始。`log[0]` 是哨兵，代表快照的边界。 */
    private val log = ArrayList<LogEntry>()
    private var snapshotLastIndex: Long = 0
    private var snapshotLastTerm: Long = 0

    /**
     * 本节点**自己的**日志已经落盘到哪个下标。
     *
     * **持久性**：Raft 的「多数派已确认」里，领导者自己那一票必须建立在
     * "我的副本已经在对象存储里"之上。`log.add` 只是一次内存操作。
     *
     * 拿内存里的那一条当自己那一票，单节点部署（自己是唯一多数派）就会出现
     * **「状态机已经应用、而日志还没落到对象存储」**：水位、租约 epoch、catalog 版本
     * 都往前走了，而读请求（它们不进日志、因此不等任何 PUT）**立刻看得见**这些新值；
     * 此时进程崩掉，重启后的状态会**退回**到那一步之前——发出去的水位区间、
     * 授出去的 epoch 都可能被重新发一遍。
     *
     * ⚠️ 这条不变式守的**不是**"回包太早"：写请求自己的响应本来就晚于它那次 PUT
     * （`persistEntryRecorded` 是 await 的，等待者也在那之后才被等到）。守的是
     * "状态机不许跑到持久性前面"——这一条只有读路径能看见，所以很容易被漏掉。
     *
     * 单调不减——但落盘失败的下标要单独记（见 [locallyUnpersisted]）。
     */
    private var persistedIndex: Long = 0

    /**
     * 进了日志、但本节点落盘失败的条目下标。
     *
     * 它们**不能**被算进"领导者自己已复制"那一票，否则就是上面第 1 条的反面：
     * 领导者给一个自己都没存下来的条目投了票。落盘失败时条目仍然留在日志里
     * （删掉会破坏"下标连续"，而并发追加的条目可能已经在它后面），
     * 于是用一个集合把它排除在投票之外——多数派由跟随者凑齐即可。
     */
    private val locallyUnpersisted = HashSet<Long>()

    // ── 易失状态 ──────────────────────────────────────────────────
    private var role = Role.FOLLOWER
    private var leaderId: String? = null
    private var commitIndex: Long = 0

    /**
     * 本任期是否已经**提交过至少一条日志**（= 这个领导者可以服务读了）。
     *
     * 这是 Raft 的 read-index 要求，写成一条注释是因为它极易被忽略：
     * 新领导者选出来的**那一刻**，它的状态机可能还没追上"已提交"的位置——
     * 上一个任期的日志要等**本任期**的第一条日志被提交时才能一并提交（Raft §5.4.2）。
     * 在这之前服务读，会把一个**已经提交**的写入读成不存在：
     * 客户端看到"我刚写的东西没了"，而系统不报任何错。
     */
    private var leaderReadyTerm: Long = 0

    /** 领导者用：每个跟随者已复制的最大下标。 */
    private val matchIndex = HashMap<String, Long>()
    private val nextIndex = HashMap<String, Long>()

    private val stateMachine = MetaStateMachine()

    /** 已提交、等待客户端取结果的响应（下标 → 回包）。 */
    private val pending = ConcurrentHashMap<Long, CompletableDeferred<MetaResponse>>()

    /** 领导者本地的锁表（§4.4：**不进共识日志**）。 */
    val locks = com.cloudea.yangbase.txn.LocalLockTable()

    private var electionJob: Job? = null
    private var heartbeatJob: Job? = null
    private var snapshotJob: Job? = null
    private var lastHeartbeatAt = System.currentTimeMillis()

    /**
     * 选举计时器被重置过几次。
     *
     * 用来让 [electionLoop] 判断"自己手上那次抽签是不是已经作废"。比拿
     * [lastHeartbeatAt] 当标记可靠：心跳与投票落在同一毫秒时时间戳不变，
     * 标记就会漏掉一次重置。
     */
    private var heartbeatEpoch = 0L

    /** 记一次"确认了领导者 / 投出了一票"——选举计时器从这里重新起算。 */
    private fun markHeartbeat() {
        lastHeartbeatAt = System.currentTimeMillis()
        heartbeatEpoch++
    }

    /** 只用于诊断：`/metrics` 与测试断言。 */
    val term: Long get() = currentTerm
    val currentRole: Role get() = role
    val committed: Long get() = commitIndex
    val lockTable: com.cloudea.yangbase.txn.LocalLockTable get() = locks

    private fun stateKey() = "meta/ms/state/$nodeId.json"
    private fun logKey(index: Long) = "meta/ms/log/$nodeId/${"%012d".format(index)}.json"
    private fun snapshotKey() = "meta/ms/snapshot/$nodeId.json"

    @Serializable
    private data class HardState(val currentTerm: Long, val votedFor: String? = null)

    @Serializable
    private data class Snapshot(
        val lastIncludedIndex: Long,
        val lastIncludedTerm: Long,
        val state: MetaState,
    )

    // ── 启动 ──────────────────────────────────────────────────────

    /**
     * 从对象存储恢复：先加载快照，再**只重放快照之后的**日志。
     *
     * 日志键是零填充的（`{index:012d}`），所以字典序就是下标序——于是"从快照边界之后
     * 继续"只需要一次前缀列举加一次字符串比较。**绝不列举整个 bucket**：
     * 设计文档把「恢复路径上的全量 LIST」列为禁止项（分页 + 请求数爆炸，
     * 而恢复期恰恰是最不能慢的时候）。
     */
    suspend fun start() {
        loadHardState()
        loadSnapshot()
        loadLogAfterSnapshot()
        // 从对象存储恢复出来的东西当然都在对象存储里——这正是 [persistedIndex] 的初值
        persistedIndex = lastLogIndex()
        // 恢复后重放已提交但还没应用的条目：commitIndex 从快照的边界继续
        commitIndex = snapshotLastIndex
        mutex.withLock { applyCommittedLocked() }
        electionJob = scope.launch { electionLoop() }
        snapshotJob = scope.launch { snapshotLoop() }
    }

    suspend fun stop() {
        electionJob?.cancel()
        heartbeatJob?.cancel()
        snapshotJob?.cancel()
    }

    /**
     * 后台快照：日志超过 [snapshotThreshold] 条就落一次快照并截断内存日志。
     *
     * **PUT 在锁外做**。持锁做一次对象存储写会把心跳停住几十毫秒，而选举超时是
     * 一个数量级——那会让集群在每次快照时都抖一次领导权（表现为"偶发的写入超时"）。
     * 先取一份状态、锁外写、再拿锁装结果，是这代码库里所有"持锁 + IO"的统一做法。
     */
    private suspend fun snapshotLoop() {
        while (true) {
            delay(200)
            val target = mutex.withLock {
                if (stateMachine.appliedIndex - snapshotLastIndex >= snapshotThreshold) stateMachine.appliedIndex else -1L
            }
            if (target <= 0) continue
            val snap = mutex.withLock {
                val term = entryTermAt(target) ?: return@withLock null
                Snapshot(target, term, stateMachine.snapshot())
            } ?: continue
            runCatching {
                store.put(snapshotKey(), json.encodeToString(Snapshot.serializer(), snap).toByteArray())
            }.onFailure { return@onFailure }   // 快照失败不影响服务：下次再试，日志还在
            mutex.withLock {
                if (snap.lastIncludedIndex > snapshotLastIndex) {
                    snapshotLastIndex = snap.lastIncludedIndex
                    snapshotLastTerm = snap.lastIncludedTerm
                    while (log.isNotEmpty() && log.first().index <= snap.lastIncludedIndex) log.removeAt(0)
                }
            }
        }
    }

    private suspend fun loadHardState() {
        val text = runCatching { String(store.get(stateKey()), Charsets.UTF_8) }.getOrNull() ?: return
        runCatching { json.decodeFromString(HardState.serializer(), text) }.getOrNull()?.let {
            currentTerm = it.currentTerm
            votedFor = it.votedFor
        }
    }

    private suspend fun loadSnapshot() {
        val text = runCatching { String(store.get(snapshotKey()), Charsets.UTF_8) }.getOrNull() ?: return
        val snap = runCatching { json.decodeFromString(Snapshot.serializer(), text) }.getOrNull() ?: return
        snapshotLastIndex = snap.lastIncludedIndex
        snapshotLastTerm = snap.lastIncludedTerm
        stateMachine.restore(snap.state, snap.lastIncludedIndex)
    }

    /**
     * 只加载快照边界**之后**的日志。
     *
     * ## 读不出来的条目一律**拒绝启动**，不跳过
     *
     * 恢复路径上"跳过一条读不出来的日志"看起来是"尽力而为"，实际是**静默丢一条
     * 已提交的写入**：这个节点会带着一个更短的日志继续跑，既可能把一个已提交的
     * 写入读成不存在，也可能在当选后把它覆盖掉。而对象存储不会自己丢对象——
     * 读不出来意味着权限、网络或数据损坏，三件都必须让人看见。
     *
     * 漏条目与漏页的后果一样（LIST 中途失败会把日志截断在中间），所以列举失败也
     * 同样拒绝启动。**清单是一次性的启动成本**：与其带着一个不完整的日志开始服务，
     * 不如让运维在启动时就收到一个说得清原因的错误。
     */
    private suspend fun loadLogAfterSnapshot() {
        val prefix = "meta/ms/log/$nodeId/"
        val after = logKey(snapshotLastIndex)        // 零填充 ⇒ 字典序即下标序
        val keys = ArrayList<String>()
        var token: String? = null
        while (true) {
            val page = runCatching { store.list(prefix, token) }.getOrElse { e ->
                throw IllegalStateException(
                    "无法列举本节点的日志（$prefix，pageToken=$token）：${e.message}。" +
                        "带着一个不完整的日志启动会丢掉已提交的写入，因此拒绝启动。",
                    e,
                )
            }
            keys.addAll(page.keys.filter { it > after })
            token = page.nextToken
            if (token == null) break
        }
        for (k in keys.sorted()) {
            val text = runCatching { String(store.get(k), Charsets.UTF_8) }.getOrElse { e ->
                throw IllegalStateException(
                    "日志条目 $k 读不出来：${e.message}。跳过它等于丢一条已提交的写入，因此拒绝启动。",
                    e,
                )
            }
            val e = runCatching { json.decodeFromString(LogEntry.serializer(), text) }.getOrElse { ex ->
                throw IllegalStateException(
                    "日志条目 $k 解析失败（对象在，但内容不是本系统写的格式）：${ex.message}",
                    ex,
                )
            }
            if (e.index > snapshotLastIndex) log.add(e)
        }
    }

    private suspend fun persistHardState() {
        store.put(stateKey(), json.encodeToString(HardState.serializer(), HardState(currentTerm, votedFor)).toByteArray())
    }

    private suspend fun persistEntry(entry: LogEntry) {
        store.put(logKey(entry.index), json.encodeToString(LogEntry.serializer(), entry).toByteArray())
    }

    /**
     * 落盘一条日志项，并把结果记进持久性账本（[persistedIndex] / [locallyUnpersisted]）。
     *
     * **返回异常而不抛**：调用方才知道该把它怎么办——客户端请求要把失败交还回去
     * （写入没落地，不能假装成功），而领导者上任时的那条 no-op 失败并不致命
     * （跟随者凑齐多数派仍可提交，重试由下一轮复制承担）。
     */
    private suspend fun persistEntryRecorded(entry: LogEntry): Throwable? {
        val err = runCatching { persistEntry(entry) }.exceptionOrNull()
        mutex.withLock {
            if (err == null) {
                if (entry.index > persistedIndex) persistedIndex = entry.index
            } else {
                locallyUnpersisted.add(entry.index)
            }
        }
        return err
    }

    // ── 日志访问（调用方持锁）──────────────────────────────────────

    private fun lastLogIndex(): Long = log.lastOrNull()?.index ?: snapshotLastIndex

    private fun lastLogTerm(): Long = log.lastOrNull()?.term ?: snapshotLastTerm

    /** 取某下标的条目，快照边界内的返回哨兵（term 用于一致性检查）。 */
    private fun entryTermAt(index: Long): Long? = when {
        index == snapshotLastIndex -> snapshotLastTerm
        index < snapshotLastIndex -> null
        else -> log.getOrNull((index - snapshotLastIndex - 1).toInt())?.term
    }

    private fun entriesFrom(index: Long, maxCount: Int): List<LogEntry> {
        if (index > lastLogIndex()) return emptyList()
        val from = maxOf(index, snapshotLastIndex + 1)
        val start = (from - snapshotLastIndex - 1).toInt()
        return log.subList(start, minOf(start + maxCount, log.size)).toList()
    }

    // ── 选举 ──────────────────────────────────────────────────────

    /**
     * 选举计时器。
     *
     * ★ **随机超时每次重置只抽一次**，不是每个 tick 重抽。
     *
     * 曾经写成每 20ms 重抽一次，看起来"更随机"，实际效果恰好相反：每个 tick 都是
     * 一次"现在够久了吗"的重新掷骰，于是**所有节点的实际等待时间都收敛到区间的下沿**
     * （时间越久，抽到一个"已经满足"的值的概率越高，先抽中的那个 tick 就触发了）。
     * 同时启动的三个节点于是在同一时刻变成候选人、各投自己、再互相拒绝——选票被
     * 反复瓜分，任期一路涨到几十也选不出领导者。
     *
     * 这个缺陷只在**高延迟后端**上暴露：内嵌存储的往返是亚毫秒，节点之间那点
     * 抽签差异（几十毫秒）远大于一次落盘 + 一轮投票的耗时，谁先抽中就谁当选；
     * 而真实对象存储一次 PUT 就是几百毫秒，先抽中的优势被抹平，锁步重新回到
     * 同一时刻，于是永远平票。真实阿里云 OSS 上 `MultiCnCoordinationTest` 的
     * 5 个用例全部卡在 `MS 集群没有选出领导者` 就是这个原因。
     */
    private suspend fun electionLoop() {
        var drawnAtEpoch = -1L          // 手上这次抽签是基于哪一次重置算的
        var deadline = Long.MAX_VALUE   // 抽签结果：到点就发起选举
        while (true) {
            delay(20)
            var fire = false
            mutex.withLock {
                if (role == Role.LEADER) {
                    // 领导者自己就是心跳源，不参与计时；下台之后重新抽签
                    drawnAtEpoch = -1L
                    deadline = Long.MAX_VALUE
                } else {
                    if (drawnAtEpoch != heartbeatEpoch) {
                        drawnAtEpoch = heartbeatEpoch
                        deadline = lastHeartbeatAt + Random.nextLong(
                            electionTimeoutMillis.first,
                            electionTimeoutMillis.last + 1,
                        )
                    }
                    fire = System.currentTimeMillis() >= deadline
                }
            }
            if (fire) startElection()
        }
    }

    private suspend fun startElection() {
        val (term, lastIdx, lastTerm) = mutex.withLock {
            currentTerm += 1
            role = Role.CANDIDATE
            votedFor = nodeId
            markHeartbeat()
            Triple(currentTerm, lastLogIndex(), lastLogTerm())
        }
        // 自己的任期与选票必须先落盘，才能向别人拉票（Raft §5.2）。但落盘**不持锁**：
        // 一次 PUT 在高延迟后端上是几百毫秒，持锁等它会把这段时间内到达的心跳与投票
        // 请求全部堵在队列里——它们等不到，就各自发起选举，集群于是永远在选。
        // 锁外落盘是安全的：内存里的 votedFor 在锁内就已经写好，并发的投票请求会看到
        // "本任期已投给自己"从而拒绝，不会出现同一个任期投两票。
        if (runCatching { persistHardState() }.isFailure) {
            // 连自己的选票都存不下来：这一轮不拉票，等下一次超时重来
            mutex.withLock { if (role == Role.CANDIDATE && currentTerm == term) role = Role.FOLLOWER }
            return
        }
        val need = (peers.size + 1) / 2 + 1             // 多数派
        var votes = 1                                   // 自己投自己
        // ⚠️ 这个检查必须**在拉票之前**也做一次：单节点集群（peers 为空）时下面的
        // 并发拉票一次都不执行，于是"自己就是多数派"这件事永远不会被检查到——
        // 表现为任期一路涨到几十却永远选不出领导者（而且不报错）
        if (votes >= need) {
            becomeLeader(term)
            return
        }
        // ★ 拉票**并发**发出去，与复制走同一条理由：串行拉票时，一个慢节点会把
        // 整轮选举拖到超过选举超时，其他人又开始新一轮——这是"高延迟后端选不出
        // 领导者"的直接放大器。
        val responses = kotlinx.coroutines.coroutineScope {
            peers.map { peer ->
                async {
                    withTimeoutOrNull(voteTimeoutMillis) {
                        runCatching {
                            transport.raft(peer, RaftMessage.RequestVote(term, nodeId, lastIdx, lastTerm))
                        }.getOrNull()
                    } as? RaftMessage.VoteResponse
                }
            }.map { it.await() }
        }
        for (resp in responses) {
            if (resp == null) continue                       // 超时/不可达：这一票没有
            if (resp.term > term) {
                mutex.withLock { stepDown(resp.term) }
                return
            }
            if (resp.granted) votes++
        }
        if (votes >= need) {
            becomeLeader(term)
            return
        }
        // 没选上：回到跟随者，等下一个（随机）超时再来
        mutex.withLock { if (role == Role.CANDIDATE && currentTerm == term) role = Role.FOLLOWER }
    }

    private suspend fun becomeLeader(term: Long) {
        val noop = mutex.withLock {
            if (currentTerm != term) return
            role = Role.LEADER
            leaderId = selfAddress
            val lastIdx = lastLogIndex() + 1
            for (p in peers) {
                nextIndex[p] = lastIdx
                matchIndex[p] = 0
            }
            // 空条目（no-op）：Raft 要求新领导者先提交一条**本任期**的日志，
            // 才能把之前任期的条目一并提交。少了它，上一个任期的日志可能永远
            // 停留在"已复制但未提交"，而"已复制"对读是不可见的——表现为
            // 「写入成功过但重启后读不到」，且不报错
            LogEntry(lastIdx, currentTerm, null, System.currentTimeMillis(), "leader-noop")
                .also { log.add(it) }
        }
        heartbeatJob?.cancel()
        heartbeatJob = scope.launch { heartbeatLoop(term) }
        // 落盘在锁外（一次 PUT 不该把心跳堵在锁上）。失败**不致命**：跟随者凑齐
        // 多数派照样能提交它；而重试由下一轮复制承担——这条 no-op 的唯一作用是
        // "开启本任期的提交"，为它放弃领导权反而会让选举反复发生
        persistEntryRecorded(noop)
        // 立即复制一轮，把 no-op 推出去。**等它完成**：这条日志一提交，
        // 本领导者才能服务读（见 leaderReadyTerm），所以尽早做完它
        replicate(term)
    }

    private suspend fun stepDown(newTerm: Long) {
        if (newTerm > currentTerm) {
            currentTerm = newTerm
            votedFor = null
            persistHardState()
        }
        demoteLocked(newTerm)
    }

    /**
     * 从领导者变回跟随者时**必须**做的三件事（调用方必须已持 [mutex]）。
     *
     * 这是 issue #5（CI flaky，“提交超时 5000ms：多数派未能确认日志 2”）的真身：
     * 领导者受理了一笔写入、还没复制给多数派就因别的原因下台了；新领导者的日志里
     * 没有这一条，它会直接覆盖那个下标——旧等待者永远等不到唤醒，挂满整个提交预算
     * 才报「多数派未能确认」。**集群是健康的，真正发生的事是“换人了”**，
     * 而错误信息把人指向网络与共识。
     *
     * 所以“下台”这动作必须包含：取消心跳（不再对外代表本任期）、
     * 把未确认的等待者当场失败掉（带上如实的原因），而不是靠超时兜底。
     *
     * 语义上要说清两件事：① 这笔写入**结果未知**（可能已被多数派复制并提交，
     * 只是本节点没来得及得知）；② 所以它是**可重试**的——MS 的写请求全是
     * "分配/抬水位"这一类，重试最多浪费一段号段或抬一次 epoch，不会重复生效。
     * 用 `NotLeader` 表达是不诚实的（那是“我没做”），所以用带说明的 `Error`。
     */
    private fun demoteLocked(term: Long) {
        val wasLeader = role == Role.LEADER
        role = Role.FOLLOWER
        if (!wasLeader) return
        heartbeatJob?.cancel()
        heartbeatJob = null
        failPending("领导者已变更（任期 $term）：本节点未确认的请求结果未知，请重试", term)
    }

    /**
     * 把当前所有等待者以同一个错误唤醒并清空。
     *
     * 只在下台时调用：**已提交的条目在下台前就已经 `complete` 了**
     * （`applyCommittedLocked` 与 `stepDown` 都在同一把锁下），所以这里清掉的
     * 一定都是"没等到确认"的。
     */
    private fun failPending(message: String, term: Long) {
        if (pending.isEmpty()) return
        val err = MetaResponse.Error(term, message)
        for (key in pending.keys.toList()) pending.remove(key)?.complete(err)
    }

    private suspend fun heartbeatLoop(term: Long) {
        while (true) {
            delay(heartbeatMillis)
            if (!mutex.withLock { role == Role.LEADER && currentTerm == term }) return
            replicate(term)
        }
    }

    // ── 复制 ──────────────────────────────────────────────────────

    /**
     * 把日志推给所有跟随者，并在多数派确认后推进 `commitIndex`。
     *
     * **不持锁做网络 IO**：持有锁去等 RPC 会让两个节点互相等待（A 等 B 的响应、
     * B 等 A 的锁）——那是一个只在压力下才出现的死锁，排查成本极高。
     * 所以这里只在取状态与更新状态时持锁。
     */
    private suspend fun replicate(term: Long) {
        val (snapshotIdx, snapshotTerm) = mutex.withLock { snapshotLastIndex to snapshotLastTerm }
        // ★ 对各个跟随者的复制**并发**发出去。
        //
        // 串行发的代价是：一个慢跟随者（对象存储抖动、GC、网络）会把整个循环堵住，
        // 期间**其他跟随者收不到心跳** → 它们超过选举超时 → 发起选举 →
        // 领导者被换掉。表现为「集群时不时重新选举」，而根因只是"排队等一个慢节点"。
        // 这个缺陷是协调模式基准跑出来的（租约授予报"多数派未能确认"，而集群里
        // 另外两个节点其实好好的）。
        val results = kotlinx.coroutines.coroutineScope {
            peers.map { peer ->
                async {
                    replicateTo(peer, term, snapshotIdx, snapshotTerm)
                }
            }.map { it.await() }
        }
        for (r in results) {
            if (r == ReplicateOutcome.STEPPED_DOWN) return
        }
        // 循环**之后**还要再推进一次提交位点：单节点集群（peers 为空）时上面的
        // for 循环一次都不执行，而"自己就是多数派"这条路径只在循环体内被检查过——
        // 于是日志永远不提交，客户端一直等到超时（表现为"写入挂住"，且只在
        // 单节点部署上出现，而单节点正是最小部署的形态）
        mutex.withLock {
            if (role == Role.LEADER && currentTerm == term) advanceCommit(term)
        }
    }

    private enum class ReplicateOutcome { OK, STEPPED_DOWN }

    /** 向一个跟随者推一轮。**不持锁做 IO**（见 [replicate] 的说明）。 */
    private suspend fun replicateTo(peer: String, term: Long, snapshotIdx: Long, snapshotTerm: Long): ReplicateOutcome {
        val (prevIdx, prevTerm, entries, commit) = mutex.withLock {
            if (role != Role.LEADER || currentTerm != term) return ReplicateOutcome.STEPPED_DOWN
            val next = nextIndex[peer] ?: (lastLogIndex() + 1)
            val prev = next - 1
            // 跟随者落在快照之前 → 直接装快照（见 InstallSnapshot 的说明）
            if (prev < snapshotIdx) {
                Quad(0L, 0L, emptyList(), 0L)
            } else {
                Quad(prev, entryTermAt(prev) ?: 0L, entriesFrom(next, 64), commitIndex)
            }
        }
        if (prevIdx == 0L && entries.isEmpty()) {
            sendSnapshot(peer, term, snapshotIdx, snapshotTerm)
            return ReplicateOutcome.OK
        }

        val resp = withTimeoutOrNull(replicateTimeoutMillis) {
            runCatching {
                transport.raft(peer, RaftMessage.AppendEntries(term, selfAddress, prevIdx, prevTerm, entries, commit))
            }.getOrNull()
        } as? RaftMessage.AppendResponse ?: return ReplicateOutcome.OK   // 超时/失败：下一轮重试

        if (resp.term > term) {
            mutex.withLock { stepDown(resp.term) }
            return ReplicateOutcome.STEPPED_DOWN
        }
        mutex.withLock {
            if (role != Role.LEADER || currentTerm != term) return@withLock
            if (resp.success) {
                matchIndex[peer] = max(matchIndex[peer] ?: 0, resp.matchIndex)
                nextIndex[peer] = (matchIndex[peer] ?: 0) + 1
            } else {
                // 日志不一致：回退一格重试（简化版：Raft 里可以一次跳过一个任期，
                // 这里日志很短，逐条回退的代价可以忽略）
                nextIndex[peer] = max(1, (nextIndex[peer] ?: 1) - 1)
            }
        }
        return ReplicateOutcome.OK
    }

    /** 四元组（局部小工具，避免为了一处包装引入 Pair 套 Pair）。 */
    private data class Quad<A, B, C, D>(val a: A, val b: B, val c: C, val d: D)

    /**
     * 给落后的跟随者装一份快照。
     *
     * ⚠️ 下标与状态的关系是「**状态 ≥ 下标**」：快照打的是 [snapshotLastIndex]，而
     * `stateMachine.snapshot()` 反映的是 `appliedIndex`（可能更大）。朝向是安全的
     * ——跟随者装完会把 `appliedIndex = idx`，随后重放 `idx+1..commitIndex` 的日志，
     * 于是多应用一次中间的条目。之所以可以接受，是因为**状态机的应用都是单调的**：
     * 水位只往前、epoch 只往上、catalog 版本只往上（重复应用的结果是一个更大的值，
     * 而更大的值在这三件事上都是安全的）。反过来（状态落后于下标）才是真错误，
     * 那种情况在这里不可能出现。
     */
    private suspend fun sendSnapshot(peer: String, term: Long, idx: Long, snapTerm: Long) {
        val snap = mutex.withLock {
            Snapshot(idx, snapTerm, stateMachine.snapshot())
        }
        val bytes = json.encodeToString(Snapshot.serializer(), snap).toByteArray()
        val resp = withTimeoutOrNull(replicateTimeoutMillis) {
            runCatching {
                transport.raft(peer, RaftMessage.InstallSnapshot(term, selfAddress, idx, snapTerm, bytes))
            }.getOrNull()
        } as? RaftMessage.InstallSnapshotResponse ?: return
        mutex.withLock {
            if (resp.term > currentTerm) { stepDown(resp.term); return@withLock }
            if (resp.success) {
                nextIndex[peer] = idx + 1
                matchIndex[peer] = idx
                advanceCommit(term)
            }
        }
    }

    /** 多数派已复制的最大下标，且**只提交本任期的条目**。 */
    private suspend fun advanceCommit(term: Long) {
        val n = lastLogIndex()
        var i = n
        while (i > commitIndex) {
            // 领导者自己那一票 = "我的副本已经在对象存储里"，不是"我内存里有它"。
            // 少这个判断，单节点部署会把一次尚未落盘的写入**应用**到状态机上
            // （读请求立刻看得见，而它还没落到对象存储——见 [persistedIndex]）
            val own = if (i <= persistedIndex && i !in locallyUnpersisted) 1 else 0
            val replicated = own + peers.count { (matchIndex[it] ?: 0) >= i }
            if (replicated >= (peers.size + 1) / 2 + 1 && entryTermAt(i) == term) {
                commitIndex = i
                // 本任期的日志被提交了 ⇒ 从此刻起，本领导者的状态机可以认为
                // "已经包含了所有已提交的写入"（见 leaderReadyTerm 的说明）
                leaderReadyTerm = term
                applyCommittedLocked()
                return
            }
            i--
        }
    }

    /**
     * 把 `commitIndex` 之内的条目应用掉，并唤醒等待中的客户端。
     *
     * ⚠️ **调用方必须已持有 [mutex]**。
     *
     * 这不是风格问题：kotlinx 的 `Mutex` **不可重入**，在里面再 `withLock` 会直接
     * 死锁（而且死锁的是 Raft 的心跳路径——表现为"写入超时"，看起来像网络问题）。
     * 这个函数曾经自己拿锁，于是第一次写请求就把整个集群挂死了；
     * 现在把"谁持锁"写在签名上，由调用方保证。
     */
    private suspend fun applyCommittedLocked() {
        while (stateMachine.appliedIndex < commitIndex) {
            val before = stateMachine.appliedIndex
            val entry = entriesFrom(before + 1, 1).firstOrNull() ?: break
            // 取到的条目必须**真的推进**状态机。不推进就原地打转，而这个循环里
            // 没有挂起点——它会把这个节点的事件循环整个占满（其它协程、心跳、
            // 选举全部得不到调度），表现为「进程 100% CPU、集群没有任何响应」，
            // 比抛一个异常严重得多。所以这里宁可失败，也不转圈
            check(entry.index > before) {
                "日志与状态机对不上：取到的条目下标 ${entry.index} 没有推进 appliedIndex=$before " +
                    "（commitIndex=$commitIndex lastLogIndex=${lastLogIndex()} " +
                    "snapshotLastIndex=$snapshotLastIndex 日志下标=${log.map { it.index }})"
            }
            val resp = stateMachine.apply(entry)
            pending.remove(entry.index)?.complete(resp)
        }
    }

    // ── 处理客户端请求（领导者才受理）──────────────────────────────

    override suspend fun handle(request: MetaRequest): MetaResponse {
        // 锁**不进日志**（§4.4）：它是短期的、丢了可以重试的状态，而每次加锁都落一次
        // 共识日志会把 FOR UPDATE 的延迟推高一个数量级
        if (request is MetaRequest.Lock || request is MetaRequest.Unlock) {
            return handleLock(request)
        }
        val t = mutex.withLock { currentTerm }
        if (role != Role.LEADER) {
            return MetaResponse.NotLeader(t, leaderId ?: "")
        }
        // 只读请求：领导者**已经应用到自己提交位点**，所以它的状态机一定不比已提交的新
        // （见 applyCommitted：commitIndex 一推进就立刻应用）。这就是"读领导者的状态机"
        // 足够线性化的原因，不需要额外一轮 read-index 探测
        if (request is MetaRequest.CatalogVersion || request is MetaRequest.ReadWatermark) {
            // 读请求要等"领导者就绪"：见 leaderReadyTerm 的说明。
            // 等不到就明确失败（客户端可重试）——**不能**返回可能陈旧的值：
            // 把一个已提交的写入读成不存在是静默的错误答案
            if (!awaitLeaderReady(t)) {
                return MetaResponse.Error(
                    t,
                    "领导者刚选出、尚未提交本任期的第一条日志，暂不能服务读（请重试）",
                )
            }
            return mutex.withLock {
                when (request) {
                    is MetaRequest.CatalogVersion -> MetaResponse.CatalogVersionValue(currentTerm, stateMachine.catalogVersion())
                    else -> MetaResponse.WatermarkValue(currentTerm, stateMachine.watermark())
                }
            }
        }
        // 写请求：进日志 → 复制 → 等提交
        //
        // ★ 等待者必须与「进日志」在**同一个临界区**里注册。
        //
        // 这不是风格问题，是协调模式基准跑出来的一个真实故障：注册放在落盘之后时，
        // 中间那段（一次对象存储 PUT，几十毫秒）里心跳协程会把这条日志推给跟随者
        // 并提交它 → [applyCommittedLocked] 去 `pending` 里找等待者，而它还没注册。
        // 于是等待者挂在一个**已经被应用过**的下标上，永远等不到唤醒，400ms 后
        // 报「多数派未能确认日志 N」——**而集群完全健康、日志也确实提交了**。
        // 错误信息把人指向网络或共识，真正的原因只是注册晚了一步。
        val waiter = CompletableDeferred<MetaResponse>()
        val entry = mutex.withLock {
            if (role != Role.LEADER) return MetaResponse.NotLeader(currentTerm, leaderId ?: "")
            LogEntry(lastLogIndex() + 1, currentTerm, request, System.currentTimeMillis(), request::class.simpleName ?: "")
                .also { log.add(it); pending[it.index] = waiter }
        }
        // 落盘在锁外：索引已经在锁内定好，而一次 PUT 不该让心跳排在这里
        persistEntryRecorded(entry)?.let { err ->
            // 日志没落到对象存储里 → 这次写入没有发生。**必须让客户端知道**
            // （它已经进了内存日志，但对多数派不可见，也可能被后来的日志覆盖）
            pending.remove(entry.index)
            throw err
        }
        // 落盘之后才让"自己那一票"生效，并顺手推进一次提交——单节点集群
        // （自己是唯一多数派）靠的就是这里，而不是等下一轮心跳
        mutex.withLock { advanceCommit(entry.term) }
        replicate(entry.term)
        val result = withTimeoutOrNull(commitTimeoutMillis) { waiter.await() }
        pending.remove(entry.index)
        // 注意：如果期间下过台，`stepDown` 已经把等待者当场完成掉了（带着
        // “领导者已变更”的说明），所以这里拿到的**不是**超时——只有集群一直
        // 健康但多数派真的凑不齐时，才会走到这条超时分支
        return result ?: MetaResponse.Error(
            entry.term,
            "提交超时（${commitTimeoutMillis}ms）：多数派未能确认日志 ${entry.index}",
        )
    }

    /** 等到"本任期的第一条日志已提交"为止。见 [leaderReadyTerm] 的说明。 */
    private suspend fun awaitLeaderReady(term: Long, timeoutMillis: Long = 1_000): Boolean {
        if (mutex.withLock { leaderReadyTerm == term }) return true
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline) {
            delay(5)
            if (mutex.withLock { role != Role.LEADER || currentTerm != term }) return false
            if (mutex.withLock { leaderReadyTerm == term }) return true
        }
        return false
    }

    private suspend fun handleLock(request: MetaRequest): MetaResponse {
        // 锁是**领导者本地**的：跟随者直接拒绝，客户端会重定向到领导者
        if (role != Role.LEADER) return MetaResponse.NotLeader(currentTerm, leaderId ?: "")
        return when (request) {
            is MetaRequest.Lock -> {
                val key = com.cloudea.yangbase.txn.LockKey(request.tableId, request.pkHash)
                val mode = runCatching { com.cloudea.yangbase.txn.LockMode.valueOf(request.mode) }
                    .getOrDefault(com.cloudea.yangbase.txn.LockMode.EXCLUSIVE)
                try {
                    val h = locks.lock(key, mode, request.owner, request.ttlMillis, request.waitMillis)
                    MetaResponse.LockGranted(currentTerm, h.token, h.expiresAtMillis)
                } catch (e: SqlException) {
                    MetaResponse.Failure(currentTerm, e.errorCode, e.message ?: "加锁失败")
                }
            }
            is MetaRequest.Unlock -> {
                locks.unlock(request.token)
                MetaResponse.LockReleased(currentTerm)
            }
            else -> MetaResponse.Error(currentTerm, "不该走到这里")
        }
    }

    // ── 处理 Raft 消息 ────────────────────────────────────────────

    override suspend fun handleRaft(message: RaftMessage): RaftMessage = when (message) {
        is RaftMessage.RequestVote -> handleRequestVote(message)
        is RaftMessage.AppendEntries -> handleAppendEntries(message)
        is RaftMessage.InstallSnapshot -> handleInstallSnapshot(message)
        is RaftMessage.VoteResponse, is RaftMessage.AppendResponse, is RaftMessage.InstallSnapshotResponse ->
            MetaResponse.Error(currentTerm, "响应不该发给服务端").let { RaftMessage.AppendResponse(currentTerm, false, 0) }
    }

    private suspend fun handleRequestVote(m: RaftMessage.RequestVote): RaftMessage {
        // ★ 锁内只做**决定**，落盘放到锁外。
        //
        // 与 [startElection] 同一条理由：一次 PUT 在高延迟后端上是几百毫秒，持锁等它
        // 会把同时到达的心跳、投票、复制请求全部堵在这把锁上——它们等不到就各自发起
        // 选举。而“不重复授予”这件事**不需要**靠锁 + 落盘来保证：votedFor 在锁内就
        // 已经写好了，并发的投票请求会看到它。
        var canVote = false
        var needPersist = false
        mutex.withLock {
            if (m.term < currentTerm) return RaftMessage.VoteResponse(currentTerm, false)
            if (m.term > currentTerm) {
                currentTerm = m.term
                votedFor = null
                // 选举请求来自更高任期 ⇒ 本节点（若是领导者）当场下台，
                // 未确认的写入必须立刻失败（否则要挂满整个提交预算才知道换人了）
                demoteLocked(m.term)
                needPersist = true
            }
            // "日志至少一样新"：候选人的日志更旧就不投它——否则已提交的条目可能被覆盖
            val atLeastAsNew = m.lastLogTerm > lastLogTerm() ||
                (m.lastLogTerm == lastLogTerm() && m.lastLogIndex >= lastLogIndex())
            canVote = (votedFor == null || votedFor == m.candidateId) && atLeastAsNew
            if (canVote) {
                votedFor = m.candidateId
                markHeartbeat()
                needPersist = true
            }
        }
        if (needPersist) {
            // 投出去的票必须先落盘再回包（Raft §5.2）：若先回包再崩，重启后的节点会把
            // 同一任期的票再投给另一个候选人，从而选出一个**少数派**领导者。
            // 落盘失败就返回拒绝——没存下来的票不能算数。
            if (runCatching { persistHardState() }.isFailure) {
                return RaftMessage.VoteResponse(currentTerm, false)
            }
        }
        return RaftMessage.VoteResponse(currentTerm, canVote)
    }

    private suspend fun handleAppendEntries(m: RaftMessage.AppendEntries): RaftMessage {
        mutex.withLock {
            if (m.term < currentTerm) return@withLock RaftMessage.AppendResponse(currentTerm, false, 0)
            if (m.term > currentTerm) {
                currentTerm = m.term
                votedFor = null
                persistHardState()
            }
            // 收到领导者的心跳：承认它，并重置选举计时器
            //
            // ★ 这里曾经直接 `role = Role.FOLLOWER`，于是「领导者收到更高任期的
            // AppendEntries」这条最常见的换人路径**不算下台**：心跳不停、
            // 未确认的写入一直挂到提交超时。issue #5 的 flaky 就长这个样子
            demoteLocked(m.term)
            leaderId = m.leaderId
            markHeartbeat()
            if (m.prevLogIndex > 0) {
                val termAtPrev = entryTermAt(m.prevLogIndex)
                    ?: return@withLock RaftMessage.AppendResponse(currentTerm, false, 0)
                if (termAtPrev != m.prevLogTerm) {
                    return@withLock RaftMessage.AppendResponse(currentTerm, false, 0)
                }
            }
        }
        // 落盘与截断放在锁外：一个对象存储 PUT 要几十毫秒，持有锁会让心跳全部排队
        for (e in m.entries) {
            mutex.withLock {
                val existing = log.getOrNull((e.index - snapshotLastIndex - 1).toInt())
                if (existing != null && existing.term != e.term) {
                    // 冲突：丢掉这条及其之后的所有条目（Raft §5.3）
                    while (log.isNotEmpty() && log.last().index >= e.index) log.removeAt(log.size - 1)
                }
            }
            persistEntry(e)
            mutex.withLock {
                if (log.none { it.index == e.index }) {
                    // ★ 追加必须是**紧接着的下一个**下标。
                    //
                    // Raft 里日志永远连续，所以这句看起来是废话——但"按下标算位置"
                    // 的换算（`log[i - snapshotLastIndex - 1]`）只在连续时成立：一旦
                    // 日志出现空洞，一致性检查会莫名其妙地通过，而"取 appliedIndex+1"
                    // 会取到一条不推进状态机的条目——那是 [applyCommittedLocked] 里的
                    // **忙等**（整个节点的事件循环被占满）。在这里失败，报错时手上
                    // 还有完整的上下文；在那里失败，只剩一堆下标
                    val expected = (log.lastOrNull()?.index ?: snapshotLastIndex) + 1
                    check(e.index == expected) {
                        "日志出现空洞：准备追加下标 ${e.index}，而当前末尾是 $expected-1" +
                            "（snapshotLastIndex=$snapshotLastIndex 日志尾部=${log.takeLast(3).map { it.index }}）"
                    }
                    log.add(e)
                }
                // 落盘成功了才记账（`persistEntry` 没抛就说明成功了）：这些下标将来
                // 本节点当上领导者时要算进"自己已复制"那一票。要求它是**连续**的下一项
                // ——日志中间有洞时不能把账记到洞的后面去
                if (e.index > persistedIndex && e.index - snapshotLastIndex == log.size.toLong()) {
                    persistedIndex = e.index
                }
            }
        }
        mutex.withLock {
            if (m.leaderCommit > commitIndex) {
                commitIndex = minOf(m.leaderCommit, lastLogIndex())
                applyCommittedLocked()
            }
        }
        return RaftMessage.AppendResponse(currentTerm, true, mutex.withLock { lastLogIndex() })
    }

    private suspend fun handleInstallSnapshot(m: RaftMessage.InstallSnapshot): RaftMessage {
        mutex.withLock { if (m.term < currentTerm) return RaftMessage.InstallSnapshotResponse(currentTerm, false) }
        val snap = runCatching { json.decodeFromString(Snapshot.serializer(), String(m.snapshot, Charsets.UTF_8)) }
            .getOrNull() ?: return RaftMessage.InstallSnapshotResponse(currentTerm, false)
        store.put(snapshotKey(), m.snapshot)
        mutex.withLock {
            if (m.term > currentTerm) { currentTerm = m.term; votedFor = null; persistHardState() }
            demoteLocked(m.term)
            leaderId = m.leaderId
            markHeartbeat()
            snapshotLastIndex = snap.lastIncludedIndex
            snapshotLastTerm = snap.lastIncludedTerm
            stateMachine.restore(snap.state, snap.lastIncludedIndex)
            while (log.isNotEmpty() && log.first().index <= snap.lastIncludedIndex) log.removeAt(0)
            commitIndex = max(commitIndex, snap.lastIncludedIndex)
        }
        return RaftMessage.InstallSnapshotResponse(currentTerm, true)
    }

    /**
     * 诊断：日志的下标是否与位置严格对应（`snapshotLastIndex + 1` 起、逐个 +1）。一致返回 null。
     *
     * 这条不变式看起来像废话（Raft 的日志当然是连续的），但代码里所有的"取下标 i 的条目"
     * 都是**按下标算位置**（`log[i - snapshotLastIndex - 1]`）。一旦某个条目被追加到
     * 比它大的下标之后（位置与下标错位），这个换算就会指向另一个条目：一致性检查会
     * 通过得莫名其妙，而"取 appliedIndex+1" 会取到一条不推进状态机的条目——
     * 那是 [applyCommittedLocked] 里的**忙等**，会把整个节点的事件循环占满。
     * 所以它值得有一条能断言的出口，而不是只写在注释里。
     */
    suspend fun logIndexDiagnosis(): String? = mutex.withLock {
        val indices = log.map { it.index }
        val expected = (snapshotLastIndex + 1..snapshotLastIndex + log.size).toList()
        if (indices == expected) null
        else "日志下标与位置错位：实际=$indices 期望=$expected (snapshotLastIndex=$snapshotLastIndex)"
    }

    /** 诊断：一行说明这个节点在干什么。 */
    fun describe(): String =
        "RaftNode($nodeId role=$role term=$currentTerm commit=$commitIndex applied=${stateMachine.appliedIndex} " +
            "log=${log.size} leader=${leaderId ?: "-"})"
}

/** MS 参数不合法。 */
fun msError(message: String) = SqlException(MySqlErrors.ER_UNKNOWN_ERROR, "HY000", message)
