package com.cloudea.yangbase.ms

import kotlinx.serialization.Serializable

/**
 * Raft 状态机：MS 的**全部持久状态**（设计文档 §4.1）。
 *
 * ## 它为什么可以这么小
 *
 * §4.1 的关键设计点是「MS 只是 Catalog 的写串行化器与读缓存，真相源在对象存储上」。
 * 于是状态机里没有库表定义、没有索引、没有数据——只有三样**短期或派生**的状态：
 *
 * | 状态 | 性质 | 丢失后果 |
 * |---|---|---|
 * | [timestampWatermark] | 单调递增即可 | 跳号（可接受），代码里用 max 保证不回退 |
 * | [leases] | TTL 秒级 | 全部租约过期，写者重新竞选 |
 * | [catalogVersion] | 派生 | 从 `catalog/` 重建（只是缓存失效判据） |
 *
 * **锁不在里面**：§4.4 明确"锁表在 MS 内存中实现，**不持久化**"。
 * 理由是锁的每一次获取都落一次共识日志会把提交路径的成本抬高一个量级，
 * 而丢失锁的后果（持锁事务失败重试）比丢失数据轻得多。见 [LockTable] 的说明。
 *
 * ## 它必须是**确定性**的
 *
 * 同一个日志在任何节点上重放必须得到同一个状态，否则各副本会漂移，
 * 而漂移后"谁是领导者"就不再能决定正确性。所以 [apply] 里不许读时钟、
 * 不许用随机数、不许读外部状态——所有时间相关的值都由**日志项自己带进来**
 * （例如租约的 `expiresAt` 由领导者算好写进命令，而不是由每个节点各自算）。
 */
@Serializable
data class MetaState(
    val timestampWatermark: Long = 0,
    val leases: Map<String, LeaseState> = emptyMap(),
    val catalogVersion: Long = 0,
)

/** 一份分片租约（§4.3）。 */
@Serializable
data class LeaseState(
    val shard: String,
    val holder: String,
    /** **单调递增、永不重置**。它写进 WAL 的对象前缀，是 fencing 的第一道防线。 */
    val epoch: Long,
    val expiresAtMillis: Long,
)

/**
 * 状态机。**所有变更都经由 [apply]**，且 apply 的输入只有日志项与当前状态。
 */
class MetaStateMachine(private var state: MetaState = MetaState()) {

    /** 已应用的最大日志下标（快照的 `lastIncludedIndex` 与它对齐）。 */
    var appliedIndex: Long = 0
        private set

    fun snapshot(): MetaState = state

    fun restore(s: MetaState, index: Long) {
        state = s
        appliedIndex = index
    }

    fun watermark(): Long = state.timestampWatermark

    fun catalogVersion(): Long = state.catalogVersion

    fun leaseOf(shard: String): LeaseState? = state.leases[shard]

    fun leases(): Collection<LeaseState> = state.leases.values

    /**
     * 应用一条日志项，返回给客户端的响应（领导者用它回包）。
     *
     * 时间由**日志项携带**（[LogEntry.issuedAt]）：租约的到期时刻是领导者在生成
     * 命令时算好的，每个副本照着写即可（见 [MetaState] 的确定性说明）。
     */
    fun apply(entry: LogEntry): MetaResponse {
        appliedIndex = entry.index
        val req = entry.request ?: return MetaResponse.TimestampReserved(entry.term, state.timestampWatermark, state.timestampWatermark)
        return when (req) {
            is MetaRequest.ReserveTimestamp -> {
                // 水位只许往前：`max(当前水位, atLeast) + span`。
                // 用 max 而不是直接覆盖，是为了在"领导者换了、新领导者还没有把它自己的
                // 水位推上去"时也不回退——时间戳回退是静默的 MVCC 事故
                val base = maxOf(state.timestampWatermark, req.atLeast)
                val through = base + req.span
                state = state.copy(timestampWatermark = through)
                MetaResponse.TimestampReserved(entry.term, base, through)
            }

            is MetaRequest.AcquireLease -> {
                val prev = state.leases[req.shard]
                // ★ 必须先检查"这个分片是不是已经有活着的写者"（§4.3）。
                //
                // 少了这一步，第二个 CN 会**直接拿到租约并开始往同一个分片写**：
                // 两个写者往同一个 WAL 前缀下写对象，只有 manifest 的 CAS 拦得住，
                // 而输的那一方写下的日志仍会被下一个恢复者重放进去——静默的数据错乱。
                // 这个检查是整个租约机制存在的**全部理由**。
                //
                // 判断用的是 `entry.issuedAt`（领导者生成命令时的毫秒）而不是本地时钟：
                // 状态机必须是确定性的，各副本读各自的时钟会让租约表慢慢漂移
                if (prev != null && prev.expiresAtMillis > entry.issuedAt && prev.holder != req.holder) {
                    // 1105 → 1205：与对象存储模式的租约冲突保持**同一个错误码**。
                    // 两种协调模式对客户端应当是同一件事（“分片写权被别人占着，稍后重试”），
                    // 区别只在实现（CAS vs 共识），不应该泄露到错误码上——
                    // 否则同一个应用换一种部署就要改一次重试逻辑（issue #14）
                    MetaResponse.Failure(
                        entry.term, 1205,
                        "分片 ${req.shard} 的写者租约被 '${prev.holder}' 持有（epoch ${prev.epoch}），" +
                            "还有 ${prev.expiresAtMillis - entry.issuedAt}ms 到期。" +
                            "同一分片任一时刻只能有一个写者（§4.3）",
                    )
                } else {
                    // epoch **永不重置**：即使租约早已过期，也从它继续 +1。
                    // 复用 epoch 会让旧写者当年写在 `wal/e{3}/` 下的对象被新写者当成自己的日志重放
                    val epoch = (prev?.epoch ?: 0L) + 1
                    val expires = entry.issuedAt + req.ttlMillis
                    val st = LeaseState(req.shard, req.holder, epoch, expires)
                    state = state.copy(leases = state.leases + (req.shard to st))
                    MetaResponse.LeaseGranted(entry.term, epoch, expires)
                }
            }

            is MetaRequest.RenewLease -> {
                val prev = state.leases[req.shard]
                if (prev == null || prev.epoch != req.epoch || prev.holder != req.holder) {
                    // 续期一个已经不属于你的租约：**必须拒绝**。
                    // 接受它等于让一个已被罢免的写者以为自己还是写者
                    MetaResponse.Failure(entry.term, 1205, "租约已不属于 $req.holder（shard=${req.shard}, epoch=${req.epoch}）")
                } else {
                    val expires = entry.issuedAt + req.ttlMillis
                    state = state.copy(leases = state.leases + (req.shard to prev.copy(expiresAtMillis = expires)))
                    MetaResponse.LeaseRenewed(entry.term, prev.epoch, expires)
                }
            }

            is MetaRequest.ReleaseLease -> {
                val prev = state.leases[req.shard]
                if (prev != null && prev.epoch == req.epoch && prev.holder == req.holder) {
                    // 释放**不删条目**：epoch 要留着继续 +1（见 AcquireLease）。
                    // 把 expiresAt 设成 0 就是"立即过期"，效果与删除一样，但不丢 epoch
                    state = state.copy(leases = state.leases + (req.shard to prev.copy(expiresAtMillis = 0)))
                }
                MetaResponse.LeaseReleased(entry.term)
            }

            MetaRequest.BumpCatalogVersion -> {
                val next = state.catalogVersion + 1
                state = state.copy(catalogVersion = next)
                MetaResponse.CatalogVersionValue(entry.term, next)
            }

            MetaRequest.CatalogVersion -> MetaResponse.CatalogVersionValue(entry.term, state.catalogVersion)

            MetaRequest.ReadWatermark -> MetaResponse.WatermarkValue(entry.term, state.timestampWatermark)

            // 锁不进日志（§4.4），走到这里说明领导者把不该复制的命令复制了
            is MetaRequest.Lock, is MetaRequest.Unlock ->
                MetaResponse.Error(entry.term, "锁操作不应进入 Raft 日志（§4.4）")
        }
    }
}
