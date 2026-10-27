package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.Epoch
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.PreconditionFailedException
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.Precondition
import kotlinx.coroutines.delay

/**
 * 分片写者租约（设计文档 §4.3）。
 *
 * ## 为什么必须有它
 *
 * 这个系统的写者身份**不是节点属性，而是一份租约**（原则 P5）。没有租约时：
 *  - 两个 CN 可以同时打开同一个分片，各自以为自己是写者；
 *  - 它们会往**同一个 WAL 前缀**下写对象，谁的 manifest 条件写成功谁就"赢"，
 *    但输的那一方写下的 WAL 对象仍然躺在那里，等下一个恢复者把它们重放进去——
 *    **数据错乱，而且没有任何一方报错**；
 *  - `wal/e{epoch}/` 里的 epoch 永远等于 1，那道"旧写者天然被忽略"的防线
 *    从未真正生效过。
 *
 * ## 放在对象存储上，而不是一个常驻服务里
 *
 * 设计文档 §4.7 的 Q2 决策要求**两种协调模式都实现**。这里是
 * `ObjectStoreCoordination` 那一种：用 `If-None-Match` / `If-Match` 的 CAS
 * 在对象存储上做选举与授时，**零新增基础设施**。
 *
 * 这个选择不是权宜之计：本系统存在的理由就是「没有客户的时候不要付固定费用」，
 * 为一个低频数据库常驻三个 Raft 节点恰恰是它想消灭的成本。
 * 代价是租约操作有对象存储的 RTT（几十毫秒），而租约只在**分片打开**与
 * **周期续期**时用到，不在每次写入的关键路径上。
 *
 * ## epoch 永不重置（这条是正确性的关键）
 *
 * 租约对象**从不被删除**，释放只是把 holder 清空并让 TTL 到期。
 * 因为 epoch 一旦被回收重用，一个旧写者当年写在 `wal/e{3}/` 下的对象
 * 就会被新写者当成自己的日志重放进去——那是静默的数据损坏。
 * 宁可让对象多留一个字段，也不让 epoch 回退。
 */
class LeaseManager(
    private val store: ObjectStore,
    /** 本节点的标识。写进租约，用于诊断「谁占着」。 */
    val holder: String,
    /** 租约时长。必须**显著大于**分片恢复时间（设计文档 §6.8 估算是 3–5s）。 */
    val ttlMillis: Long = 15_000,
    /**
     * 本**实例**的标识（每个 `LeaseManager` 对象生成一个）。
     *
     * 与 [holder] 的区别是这两个问题的答案不同：
     *  - `holder` 回答「哪个节点」，部署时给定，可以重复（某些人真会把两个 CN 都叫 `cn-1`）；
     *  - `instance` 回答「同一个进程吗」，所以续期失败后可以直接判断「占有者还是我」。
     *
     * 这条字段**只用于本地判断**，不承担互斥责任：即使两个进程的 `holder` 相同，
     * 它们的 `instance` 也必定不同，于是"自己接自己的班"不会变成两个写者。
     */
    val instance: String = java.util.UUID.randomUUID().toString().substring(0, 8),
) : com.cloudea.yangbase.storage.LeaseProvider {

    /**
     * 一份租约。`epoch` 单调递增，永不重置。
     *
     * 实现 [com.cloudea.yangbase.storage.ShardLease] —— 存储层只认那个接口，
     * 不知道协调是通过对象存储 CAS 还是 Raft 做的（§4.7 的两种模式）。
     */
    data class Lease(
        val shard: ShardId,
        override val holder: String,
        override val epoch: Long,
        val expiresAtMillis: Long,
        /** CAS 凭据。续期与释放都要带上它。 */
        val etag: String,
        /** 取得它的那个进程实例（见 [LeaseManager.instance]）。旧对象里没有这个字段时为空串。 */
        val instance: String = "",
    ) : com.cloudea.yangbase.storage.ShardLease {
        override fun isExpired(now: Long): Boolean = now >= expiresAtMillis

        /** 距离过期还有多久。用于决定何时续期。 */
        override fun remainingMillis(now: Long): Long = expiresAtMillis - now
    }

    private fun keyOf(shard: ShardId) = ObjectKeys.shardLease(shard)

    /**
     * 租约的**只读快照**：谁持有、哪个 epoch、什么时候到期。
     *
     * 刻意与 [Lease] 分开：快照拿不到 CAS 凭据（etag），所以它**不能**用来续期/释放。
     * 把它做成独立类型，是为了让「查一眼」这件事在类型上就无法被误用成「拿去改」。
     */
    data class LeaseSnapshot(
        val shard: ShardId,
        val holder: String,
        val epoch: Long,
        val expiresAtMillis: Long,
        /** 持有它的进程实例；旧对象或已释放时为空串。 */
        val instance: String = "",
    ) {
        fun isExpired(now: Long = System.currentTimeMillis()): Boolean = now >= expiresAtMillis

        fun remainingMillis(now: Long = System.currentTimeMillis()): Long = expiresAtMillis - now
    }

    /**
     * 读取当前租约。返回 null 表示从未有人持有过。
     *
     * ⚠️ 已过期的租约**仍然返回**（带原来的 epoch）——调用方需要它的 epoch
     * 才能算出 next = old + 1。把它当成 null 会让 epoch 回到 1。
     */
    suspend fun current(shard: ShardId): Lease? {
        val key = keyOf(shard)
        val meta = try {
            store.head(key)
        } catch (_: ObjectNotFoundException) {
            return null
        }
        val bytes = try {
            store.get(key)
        } catch (_: ObjectNotFoundException) {
            return null   // head 与 get 之间被删了；当作不存在，调用方会重试
        }
        val snap = snapshotOf(shard, String(bytes, Charsets.UTF_8))
        return Lease(
            shard = shard,
            holder = snap.holder,
            epoch = snap.epoch,
            expiresAtMillis = snap.expiresAtMillis,
            etag = meta.etag.value,
            instance = snap.instance,
        )
    }

    /**
     * 只读地看一眼当前租约（运维查询用：`yb-cli shards --owner`）。
     *
     * 与 [current] 的区别是**只发 1 次 GET**：`current` 多一次 `head` 是为了拿 CAS 凭据，
     * 而对「这个分片现在归谁」这个问题，凭据没有意义。请求数在这个项目里是一等成本，
     * 一次查询 N 个分片时这一次之差就是一倍。
     *
     * 返回 null 表示没有租约对象：单 CN 部署（`SingleProcessLease` 根本不写对象）
     * 或者 MS 模式（租约在共识状态里，不在对象存储上）。
     */
    suspend fun peek(shard: ShardId): LeaseSnapshot? {
        val bytes = try {
            store.get(keyOf(shard))
        } catch (_: ObjectNotFoundException) {
            return null
        }
        return snapshotOf(shard, String(bytes, Charsets.UTF_8))
    }

    private fun snapshotOf(shard: ShardId, text: String) = LeaseSnapshot(
        shard = shard,
        holder = Regex("\"holder\"\\s*:\\s*\"([^\"]*)\"").find(text)?.groupValues?.get(1) ?: "",
        epoch = Regex("\"epoch\"\\s*:\\s*(\\d+)").find(text)?.groupValues?.get(1)?.toLongOrNull() ?: 0L,
        expiresAtMillis = Regex("\"expiresAt\"\\s*:\\s*(\\d+)").find(text)?.groupValues?.get(1)?.toLongOrNull() ?: 0L,
        instance = Regex("\"instance\"\\s*:\\s*\"([^\"]*)\"").find(text)?.groupValues?.get(1) ?: "",
    )

    /**
     * 【本进程发出的租约】吗？
     *
     * 两个条件缺一不可：`holder` 相同只说明“同一台节点”，`instance` 相同才说明
     * “同一个进程”。两个 CN 被误配成同一个 `--node-id` 时，`holder` 会相等而
     * `instance` 不同——这正是不能只凭 `holder` 就把租约认成自己的原因。
     */
    private fun isMine(it: Lease): Boolean = it.holder == holder && it.instance == instance

    /**
     * 取得分片的写者租约。
     *
     * 已被别人持有时**抛异常而不是等待**——调用方是一个正要打开分片的 CN，
     * 它应该立刻转成只读或去别处，而不是阻塞在这里。
     */
    override suspend fun acquire(shard: ShardId): com.cloudea.yangbase.storage.ShardLease =
        acquireInternal(shard, 16)

    private suspend fun acquireInternal(shard: ShardId, maxAttempts: Int): Lease {
        var attempts = 0
        while (true) {
            val cur = current(shard)
            if (cur != null && !cur.isExpired()) {
                // ★ 如果是**本进程自己**（同一 instance、同一 epoch）仍持有的那把：
                // 采纳它，而不是报"被别人持有"。
                //
                // 什么时候会走到这里：续租失败把 `leaseLost` 置上之后，下一次写会
                // 重新 acquire，而租约对象上写的持有者就是自己。旧代码在这里直接报
                // “分片 x 的写者租约被 'cn-a' 持有”——**被自己持有**，运维看到这句
                // 话无从下手，而且该分片会一直不可写直到 TTL 过期。
                //
                // ⚠️ 只认 instance，**不认 holder**：两个 CN 被误配成同一个
                // `--node-id` 时 holder 同样相等，那时必须继续报冲突——否则租约
                // 防的就是“两个写者”，而这里恰好会造出两个写者。
                if (isMine(cur)) return cur
                // ★ 错误码 1205（而不是 1105）：见 LsmStore.ensureWriter 的说明——
                // 「资源被别人占着、稍后重试」在这个系统里是**确定可重试**的语义，
                // 1205 让 JDBC 客户端把它映射成可重试的事务异常，1105 则是一个死胡同。
                throw SqlException(
                    MySqlErrors.ER_LOCK_WAIT_TIMEOUT, "HY000",
                    "分片 $shard 的写者租约被 '${cur.holder}' 持有（epoch ${cur.epoch}），" +
                        "还有 ${cur.remainingMillis()}ms 到期。同一分片任一时刻只能有一个写者。",
                )
            }
            val nextEpoch = (cur?.epoch ?: 0L) + 1
            val expires = System.currentTimeMillis() + ttlMillis
            val payload = """{"holder":"$holder","instance":"$instance","epoch":$nextEpoch,"expiresAt":$expires}""".toByteArray()
            val pre = if (cur == null) Precondition.MustNotExist else Precondition.MustMatch(
                com.cloudea.yangbase.objectstore.ETag(cur.etag)
            )
            try {
                val etag = store.put(keyOf(shard), payload, pre)
                Metrics.onLeaseAcquired()
                return Lease(shard, holder, nextEpoch, expires, etag.value, instance)
            } catch (_: ConcurrentModificationException) {
                // 有人抢先了，重读
                Metrics.onLeaseContended()
            } catch (_: PreconditionFailedException) {
                // 同上：对方持有了，或者刚好释放；重读一次会看清
                Metrics.onLeaseContended()
            }
            attempts++
            if (attempts >= maxAttempts) {
                Metrics.onLeaseAcquireFailed()
                // 同样是 1205：撞了一轮说明有别的写者正在抢这个分片，重试是对的答案
                throw SqlException(
                    MySqlErrors.ER_LOCK_WAIT_TIMEOUT, "HY000",
                    "无法取得分片 $shard 的租约：连续 $attempts 次与别人相撞",
                )
            }
            delay(1L shl minOf(attempts, 6))
        }
    }

    /**
     * 续期。
     *
     * **epoch 保持不变**——续期是同一个写者继续持有，不是换了人。
     * 换 epoch 会让这个写者自己刚写的 WAL 前缀立刻失效。
     */
    override suspend fun renew(lease: com.cloudea.yangbase.storage.ShardLease):
        com.cloudea.yangbase.storage.ShardLease {
        require(lease is Lease) { "LeaseManager 只处理自己发出的租约" }
        return renewInternal(lease)
    }

    private suspend fun renewInternal(lease: Lease): Lease {
        val expires = System.currentTimeMillis() + ttlMillis
        val payload = """{"holder":"$holder","instance":"$instance","epoch":${lease.epoch},"expiresAt":$expires}""".toByteArray()
        return try {
            val etag = store.put(
                keyOf(lease.shard), payload,
                Precondition.MustMatch(com.cloudea.yangbase.objectstore.ETag(lease.etag)),
            )
            lease.copy(expiresAtMillis = expires, etag = etag.value)
        } catch (_: ConcurrentModificationException) {
            Metrics.onLeaseLost()
            throw com.cloudea.yangbase.common.LeaseLostException(lease.shard, Epoch(lease.epoch))
        } catch (_: PreconditionFailedException) {
            // etag 变了 = 有人接管了这个分片（我们的租约已经过期被人抢走）
            Metrics.onLeaseLost()
            throw com.cloudea.yangbase.common.LeaseLostException(lease.shard, Epoch(lease.epoch))
        }
    }

    /**
     * 确认租约还在不在自己手上（见 [com.cloudea.yangbase.storage.LeaseProvider.confirm]）。
     *
     * 为什么这里必须区分 `holder` 与 `instance`：续期超时的典型情形是
     * **PUT 已经成功、只是响应丢了**——于是对象上的 holder 仍是我们、epoch 未变，
     * 但 etag 已经变了。旧代码接下来用旧 etag 续期 ⇒ 412 ⇒ 判作“被挤占”
     * ⇒ 一个合法的写者自己停写，且下一次 acquire 还会报“被自己持有”。
     * 靠 `instance` 就能把这两种情形分干净：换了人（instance 不同）= 确实丢了；
     * 同一个人（instance 相同）= 只是凭据过期，采纳新 etag 即可。
     */
    override suspend fun confirm(lease: com.cloudea.yangbase.storage.ShardLease):
        com.cloudea.yangbase.storage.ShardLease? {
        require(lease is Lease) { "LeaseManager 只处理自己发出的租约" }
        val cur = current(lease.shard) ?: return null
        if (!isMine(cur)) return null          // 换了人（或换了 epoch）
        if (cur.epoch != lease.epoch) return null
        if (cur.isExpired()) return null
        return cur                              // 仍是自己的：etag / 到期时间已刷新
    }

    /**
     * 主动释放：清空 holder 并让 TTL 立刻到期。
     *
     * **不删除对象**。删掉会让下一个写者的 epoch 从 1 重新开始，
     * 而这个分片在 `wal/e{3}/` 下可能还躺着旧写者的日志——
     * epoch 一旦被重用，那些日志就会被当成新写者的日志重放。
     */
    override suspend fun release(lease: com.cloudea.yangbase.storage.ShardLease) {
        require(lease is Lease) { "LeaseManager 只处理自己发出的租约" }
        releaseInternal(lease)
    }

    private suspend fun releaseInternal(lease: Lease) {
        val payload = """{"holder":"","instance":"","epoch":${lease.epoch},"expiresAt":0}""".toByteArray()
        runCatching {
            store.put(
                keyOf(lease.shard), payload,
                Precondition.MustMatch(com.cloudea.yangbase.objectstore.ETag(lease.etag)),
            )
        }
    }
}
