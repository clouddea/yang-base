package com.cloudea.yangbase.ms

import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.encode
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.storage.LeaseProvider
import com.cloudea.yangbase.storage.ShardLease
import com.cloudea.yangbase.txn.CoordinationService
import com.cloudea.yangbase.txn.LockHandle
import com.cloudea.yangbase.txn.LockKey
import com.cloudea.yangbase.txn.LockMode
import com.cloudea.yangbase.txn.LockTable
import com.cloudea.yangbase.txn.TimestampSource
import com.cloudea.yangbase.txn.TimestampWindow
import com.cloudea.yangbase.txn.Tso
import com.cloudea.yangbase.txn.WindowTso
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * CN 侧的 MS 客户端：`CoordinationService` 的 **MS 模式**实现（§4.7 的
 * `MetaServiceClient`）。
 *
 * ## 三件事它必须处理对，否则故障时会静默出错
 *
 * 1. **重定向**：只有领导者能写。跟随者回 `NotLeader(hint)`，客户端换到 hint 上重试。
 *    换目标时必须**重置本地对领导者的认知**，否则会一直撞同一个跟随者直到超时。
 * 2. **降级（§4.2）**：MS 全部不可用时，CN 用**剩余的时间戳区间**继续服务读写；
 *    区间用尽后写事务失败并明确报错，而**已经取到的读快照仍然可读**。
 *    这是刻意的语义：读是缓存友好的、用户可感知代价小的操作。
 * 3. **任期（term）**：任何响应都带领导者任期。客户端发现任期**变大**时，
 *    必须认为此前拿到的锁与租约**可能已经失效**——领导者一换，新领导者的锁表是空的
 *    （§4.4 明确锁不持久化）。静默地继续用一把已经不在的锁 = 静默的丢更新。
 */
class MetaServiceClient(
    private val cluster: MetaCluster,
    private val transport: MetaTransport,
    /** 本 CN 的标识，写进租约与锁，用于诊断"谁占着"。 */
    private val nodeId: String,
    private val leaseTtlMillis: Long = 15_000,
    batchMillis: Long = 1_000,
    logicalBits: Int = 18,
) : CoordinationService {

    override val kind = "ms"

    private val timestampSource = MsTimestampSource()

    override val tso: Tso = WindowTso(timestampSource, logicalBits, batchMillis)

    override val leases: LeaseProvider = MsLeaseProvider()

    override val locks: LockTable = MsLockTableClient()

    /** 当前已知的领导者地址。**必须可变**：它会在每次响应后被纠正。 */
    @Volatile
    private var leader: String? = null

    /** 见过的最大任期。锁与租约的有效性判定全靠它。 */
    private val maxSeenTerm = AtomicLong(0)

    /**
     * "没有领导者"时的重试预算。
     *
     * ⚠️ 它必须**大于 MS 集群的选举超时上限**（默认 500ms–1s，见
     * `ServerConfig.DEFAULT_MS_ELECTION_TIMEOUT`）。取小了会把一次**正在进行**的
     * 正常选举当成故障报给用户：那条 SQL 收到的是一句"集群没有领导者"，
     * 而真实情况是"再等半秒就好"。所以这里按选举超时的两倍再留一点余量。
     *
     * 也不能无限等：再长就是在等一个真正故障的集群，那时应当把失败交还给调用方，
     * 而不是让一条 SQL 挂在这里。
     */
    private val noLeaderRetryMillis: Long = 3_000

    /** 通信统计（诊断用；也用于测试断言"重试了几次"）。 */
    val calls = AtomicLong(0)
    val redirects = AtomicLong(0)
    val unreachable = AtomicLong(0)

    /**
     * 发一个请求到集群，自动处理重定向与跟随者。
     *
     * 顺序是刻意的：**先试已知领导者**（稳态下一次成功），失败再按顺序试其他节点
     * （领导者可能刚挂、或本地认知过时）。
     *
     * ## 两种失败必须分开报，而且"没有领导者"要重试
     *
     * - **没人应答**（进程挂了/网络断了）→ [MetaUnreachableException]；
     * - **都回 NotLeader**（集群正在选举）→ 短暂重试，仍无领导者则报
     *   [MetaNoLeaderException]。
     *
     * 第一版把两者混在一起报"无法连接 ms-1"，而真实的场景是"MS 刚启动、还在选举"——
     * 错误信息把排查方向指到了网络，离真正的原因（等一会儿就好）很远。
     * 一次选举是几百毫秒到一秒量级，所以这里自带一次有界的重试
     * （预算见 [noLeaderRetryMillis]，它必须覆盖一次完整的选举）。
     */
    private suspend fun call(request: MetaRequest): MetaResponse {
        val deadline = System.currentTimeMillis() + noLeaderRetryMillis
        var sawNotLeader = false
        var lastError: Exception? = null
        while (true) {
            try {
                return callOnce(request)
            } catch (e: MetaNoLeaderException) {
                sawNotLeader = true
                lastError = e
            } catch (e: MetaUnreachableException) {
                lastError = e
            }
            if (System.currentTimeMillis() >= deadline) break
            // 选举窗口内的重试间隔：选举超时是 500ms–1s，取 50ms 足够细，
            // 而 3 秒的预算意味着这里有大约 60 次机会——够跨过一次完整的选举
            kotlinx.coroutines.delay(50)
        }
        if (sawNotLeader && lastError !is MetaUnreachableException) {
            throw MetaNoLeaderException(
                cluster.addresses.joinToString(","),
                "元数据服务集群当前没有领导者（可能正在选举或多数派不可用）。" +
                    "已重试 ${noLeaderRetryMillis}ms。请稍后重试，或检查 MS 进程与网络。",
            )
        }
        throw lastError as? MetaUnreachableException
            ?: MetaUnreachableException(cluster.addresses.joinToString(","))
    }

    /** 一轮尝试：先已知领导者，再其他节点。 */
    private suspend fun callOnce(request: MetaRequest): MetaResponse {
        val tried = LinkedHashSet<String>()
        val known = leader
        val order = if (known != null) listOf(known) + cluster.addresses.filter { it != known }
        else cluster.addresses
        var lastError: Exception? = null

        for (target in order) {
            if (!tried.add(target)) continue
            calls.incrementAndGet()
            val resp = try {
                transport.call(target, request)
            } catch (e: MetaUnreachableException) {
                unreachable.incrementAndGet()
                lastError = e
                if (leader == target) leader = null      // 已知领导者挂了，别再往那儿发
                continue
            }
            observeTerm(resp.term)
            when (resp) {
                is MetaResponse.NotLeader -> {
                    redirects.incrementAndGet()
                    // 跟随者告诉我们领导者是谁；不告诉就清空本地认知，下次轮询
                    leader = resp.leaderHint.ifEmpty { null }
                    continue
                }
                else -> {
                    // 谁能正确服务，谁就是领导者（稳态下省掉一次重定向）
                    leader = target
                    return resp
                }
            }
        }
        // 全部都是"我不是领导者" → 集群暂时没有领导者（选举中）
        if (lastError == null) throw MetaNoLeaderException(order.joinToString(","), "所有节点都回了 NotLeader（集群正在选举）")
        throw MetaUnreachableException(order.joinToString(","), lastError)
    }

    /**
     * 更新见过的最大任期。
     *
     * 任期变大意味着**领导权换过**，而 §4.4 的锁表不持久化、§4.3 的租约虽然持久化
     * 但持有者的续期会失败。客户端在这里只是记下来，由持锁的一方自己去查
     * （见 [MsLockTableClient.holdsValidLocks]）——因为只有它知道哪些锁要作废。
     */
    private fun observeTerm(term: Long) {
        while (true) {
            val seen = maxSeenTerm.get()
            if (term <= seen) return
            if (maxSeenTerm.compareAndSet(seen, term)) return
        }
    }

    fun currentTerm(): Long = maxSeenTerm.get()

    override suspend fun catalogVersion(): Long = when (val r = call(MetaRequest.CatalogVersion)) {
        is MetaResponse.CatalogVersionValue -> r.version
        is MetaResponse.Error -> throw msError("MS 返回错误：${r.message}")
        is MetaResponse.Failure -> throw r.toException()
        else -> throw msError("MS 返回了意外的响应：$r")
    }

    override suspend fun bumpCatalogVersion(): Long = when (val r = call(MetaRequest.BumpCatalogVersion)) {
        is MetaResponse.CatalogVersionValue -> r.version
        is MetaResponse.Error -> throw msError("MS 返回错误：${r.message}")
        is MetaResponse.Failure -> throw r.toException()
        else -> throw msError("MS 返回了意外的响应：$r")
    }

    override fun describe() = "MetaServiceClient(${cluster.addresses.joinToString(",")}, term=${maxSeenTerm.get()})"

    // ── 时间戳（§4.2）────────────────────────────────────────────

    private inner class MsTimestampSource : TimestampSource {

        override suspend fun reserve(atLeast: Long, span: Long): TimestampWindow {
            val resp = try {
                call(MetaRequest.ReserveTimestamp(atLeast, span))
            } catch (e: MetaUnreachableException) {
                // ★ 降级语义（§4.2）：MS 不可用时写事务必须**明确失败**，而不是
                // 拿一个可能重复的时间戳继续跑。重复的 commit_ts 会让 MVCC 失去全序，
                // 而它是静默的——宁可让这一笔写入失败（可见、可重试），
                // 也不要发一个可能重复的号
                throw SqlException(
                    MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                    "元数据服务不可用，无法预留时间戳区间（已试过 ${cluster.addresses.joinToString(",")}）。" +
                        "已取到的时间戳区间仍可继续服务只读与写入，用尽后写事务会失败。" +
                        "请检查 MS 进程与网络。原因：${e.message}",
                )
            }
            return when (resp) {
                is MetaResponse.TimestampReserved -> TimestampWindow(resp.base, resp.through)
                is MetaResponse.Failure -> throw resp.toException()
                is MetaResponse.Error -> throw msError("MS 预留时间戳失败：${resp.message}")
                else -> throw msError("MS 返回了意外的响应：$resp")
            }
        }

        override suspend fun watermark(): Long = when (val r = call(MetaRequest.ReadWatermark)) {
            is MetaResponse.WatermarkValue -> r.watermark
            is MetaResponse.Failure -> throw r.toException()
            is MetaResponse.Error -> throw msError("MS 读水位失败：${r.message}")
            else -> throw msError("MS 返回了意外的响应：$r")
        }

        override fun describe() = "MS 集群(${cluster.addresses.joinToString(",")})"
    }

    // ── 租约（§4.3）──────────────────────────────────────────────

    /** MS 模式的租约：epoch 由 MS 的共识状态机给出，**永不重置**。 */
    private class MsLease(
        val shardId: ShardId,
        override val holder: String,
        override val epoch: Long,
        private val expiresAt: Long,
        /** 授予它的 MS 任期：任期变了这把租约就不再可信（见 [MetaServiceClient]）。 */
        val term: Long,
    ) : ShardLease {
        override fun isExpired(now: Long): Boolean = now >= expiresAt

        override fun remainingMillis(now: Long): Long = expiresAt - now

        override fun toString() = "MsLease($shardId, ${holder}, epoch=$epoch)"
    }

    private inner class MsLeaseProvider : LeaseProvider {

        override suspend fun acquire(shard: ShardId): ShardLease {
            val resp = call(MetaRequest.AcquireLease(shard.encode(), nodeId, leaseTtlMillis))
            return when (resp) {
                is MetaResponse.LeaseGranted -> MsLease(shard, nodeId, resp.epoch, resp.expiresAtMillis, resp.term)
                is MetaResponse.Failure -> throw resp.toException()
                is MetaResponse.Error -> throw msError("MS 授予租约失败：${resp.message}")
                else -> throw msError("MS 返回了意外的响应：$resp")
            }
        }

        override suspend fun renew(lease: ShardLease): ShardLease {
            val l = lease as? MsLease ?: throw msError("这不是 MS 模式发出的租约：$lease")
            val resp = call(MetaRequest.RenewLease(l.shardId.encode(), nodeId, l.epoch, leaseTtlMillis))
            return when (resp) {
                is MetaResponse.LeaseRenewed -> MsLease(l.shardId, nodeId, resp.epoch, resp.expiresAtMillis, resp.term)
                // 续期被拒 = 我们已经被罢免。**必须让调用方知道**（它会停止写入），
                // 而不是假装续期成功——那会让一个已被罢免的写者继续写
                is MetaResponse.Failure -> throw SqlException(
                    resp.code, "HY000",
                    "租约续期被拒绝，本节点已不是该分片的写者：${resp.message}",
                )
                is MetaResponse.Error -> throw msError("MS 续期失败：${resp.message}")
                else -> throw msError("MS 返回了意外的响应：$resp")
            }
        }

        override suspend fun release(lease: ShardLease) {
            val l = lease as? MsLease ?: return
            runCatching { call(MetaRequest.ReleaseLease(l.shardId.encode(), nodeId, l.epoch)) }
            // 释放失败不抛异常：租约会自然过期，而抛异常会让"已经提交成功的事务"报错
        }
    }

    // ── 锁（§4.4）────────────────────────────────────────────────

    /**
     * MS 模式的行锁。
     *
     * ## 本地要记任期：这是"锁会不会静默失效"的唯一防线
     *
     * 领导者一换，新领导者的锁表是空的（§4.4 不持久化）。如果客户端不知道这件事，
     * 它会继续以为自己持有某一行的排他锁，而另一个事务在新领导者上拿到了同一把锁——
     * 两个事务都基于"我锁住了这一行"做出决策，其中一个的更新会被另一个覆盖。
     *
     * 所以每个 handle 都记住**授予它的任期**，[holdsValidLocks] 用来在提交前自检。
     */
    private inner class MsLockTableClient : LockTable {

        private val terms = ConcurrentHashMap<String, Long>()

        /** 已失效的锁（任期变化导致），只用于诊断与测试断言。 */
        val invalidated = AtomicLong(0)

        override suspend fun lock(
            key: LockKey,
            mode: LockMode,
            owner: String,
            ttlMillis: Long,
            waitMillis: Long,
        ): LockHandle {
            val req = MetaRequest.Lock(key.tableId, key.pkHash, mode.name, owner, ttlMillis, waitMillis)
            val resp = try {
                call(req)
            } catch (e: MetaUnreachableException) {
                // 加锁**不能降级**：锁的全部意义就是有一个权威方来裁定先后。
                // 降级成"假设自己拿到了"会让两个事务同时以为锁在手里
                throw SqlException(
                    MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                    "元数据服务不可用，无法加锁（`SELECT ... FOR UPDATE` 需要它裁定先后）。" +
                        "请检查 MS 进程与网络。原因：${e.message}",
                )
            }
            return when (resp) {
                is MetaResponse.LockGranted -> {
                    terms[resp.token] = resp.term
                    object : LockHandle {
                        override val key = key
                        override val mode = mode
                        override val token = resp.token
                        override val expiresAtMillis = resp.expiresAtMillis
                    }
                }
                is MetaResponse.Failure -> throw resp.toException()
                is MetaResponse.Error -> throw msError("MS 加锁失败：${resp.message}")
                else -> throw msError("MS 返回了意外的响应：$resp")
            }
        }

        override suspend fun unlock(handle: LockHandle) {
            val granted = terms.remove(handle.token)
            runCatching { call(MetaRequest.Unlock(handle.token)) }
            if (granted != null && granted < maxSeenTerm.get()) {
                invalidated.incrementAndGet()
            }
        }

        /**
         * 这些锁现在还有效吗？（提交前的自检）
         *
         * 提交路径**必须**调它：锁在领导者更替时会被丢掉，而"以为还锁着"的提交
         * 等于静默的丢更新。失效时明确报 1213 让事务重试——重试是可见的代价，
         * 覆盖别人的更新是不可见的损坏。
         */
        fun holdsValidLocks(handles: Collection<LockHandle>): Boolean {
            if (handles.isEmpty()) return true
            val term = maxSeenTerm.get()
            val now = System.currentTimeMillis()
            return handles.all { h ->
                // 两件事都要看：**任期**（领导者换了锁表就空了）与 **TTL**
                // （持有者卡住超过 TTL，锁会被别人拿走——同一个任期内也发生）。
                // 只看任期会漏掉后者：那同样是"以为自己还锁着"，同样是静默的丢更新
                (terms[h.token] ?: Long.MAX_VALUE) >= term && h.expiresAtMillis > now
            }
        }

        /**
         * 提交前自检：**任期变了就认为锁已经失效**。
         *
         * 这是 MS 模式下唯一能防住"锁静默失效"的地方（§4.4 的锁不进共识日志）。
         * 走的是本地判断（比对见过的最大任期），**不产生 RPC**——提交路径上多一个
         * RTT 是不可接受的，而任期的变化会从任何一次 MS 响应里被观测到。
         */
        override suspend fun stillHeld(handles: Collection<LockHandle>): Boolean = holdsValidLocks(handles)

        override suspend fun describeLock(key: LockKey): String? = null
    }

    /** 提交前的锁自检入口（`CoordinationService` 的 MS 模式特有，事务层按需调用）。 */
    fun locksStillHeld(handles: Collection<LockHandle>): Boolean =
        (locks as MsLockTableClient).holdsValidLocks(handles)
}
