package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.ObjectStoreException
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.common.metrics.StoreOp
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.ThreadLocalRandom

/**
 * L1 适配层：**重试 / 退避 / 限流**（设计文档 §2.2 的层图）。
 *
 * ```
 * L1 适配层    ObjectStore SPI │ 能力探测 │ 重试/退避 │ 限流
 * ```
 *
 * ## 为什么这一层必须存在
 *
 * 对象存储的失败**不是异常事件，而是常态**：5xx 与 `SlowDown` 在厂商 SLA 里就是
 * 允许发生的，真实观测中它们在整个请求量里占了可观的比例（§14.1 引的实测里，
 * S3 PUT 的 **p99 是 400–600ms**——"慢"与"失败"之间只差一个超时阈值）。
 * 而本系统把每一次点查、每一次提交都压在一到几次对象存储请求上：
 * **没有重试层，一次瞬时抖动就是一次用户可见的查询失败**。
 *
 * ## 哪些失败该重试，哪些**绝不能**重试（§3.3.1 的错误码语义）
 *
 * | 情况 | 处置 | 理由 |
 * |---|---|---|
 * | 5xx / 429 / 408 / 超时 / 网络中断 | **重试**（指数退避 + 抖动） | 瞬时故障，重发可能成功 |
 * | `409 ConditionalRequestConflict` | **重试** | 条件的**结论未知**（有并发写在进行中），重发会得到明确结论：成功或 412 |
 * | `412 PreconditionFailed` | **绝不重试** | 条件**确定**不成立。重试只会浪费时间，还会把一次确定的 CAS 失败拖成长尾 |
 * | `404 Not Found` | **绝不重试** | 明确的业务语义：对象不存在 / CAS 目标已消失 |
 *
 * 分类的**知识放在后端**（[ObjectStoreException.retryable]，由 `S3ObjectStore` 看
 * HTTP 状态、`LocalFileObjectStore` 看 IO 异常填写；409 由
 * [ConcurrentModificationException] 这个类型表达）。这一层只知道**两类**东西：
 * 「可重试的存储故障」与「条件写冲突」——让装饰器去猜 message 里的状态码，
 * 是在错误的地方做判断，而且会在后端换用词时静默失效。
 *
 * ## ⚠️ 重试不会破坏 CAS / fencing
 *
 * 这是这一层最需要论证清楚的一点：
 *
 *  - **条件写的结论只由服务端给**。我们重发的是**同一个请求**（同样的 key、同样的
 *    `If-Match`/`If-None-Match`、同样的字节），条件写本身在服务端是原子的——
 *    只有当服务端**接受**了那次写，调用方才会看到成功。
 *  - 条件确定不成立时（etag 已变、对象已存在），重发得到的是 `412`，
 *    而 `412` 在这一层不重试，会原样抛给调用方 → 调用方看到的仍是「CAS 失败」。
 *  - 409 是**唯一**「结论未知」的情形：有并发条件写在途。重发它是安全的，
 *    因为它要么成功（我们那次其实落下了），要么 412（别人赢了）——两者都是确定的结论。
 *    这也是 §3.3.1 表格里「409：PutObject 可退避重试」的由来。
 *  - **fencing 不受影响**：旧写者被 fence 之后，它的 CAS 一定拿到 412（etag 已变），
 *    重试只会更快地拿到同一个 412，不会让它写进去。
 *
 * 换句话说：这一层消除的是**传输层的不确定性**，不改变条件的结论。
 *
 * ## 限流
 *
 * 并发信号量限制**在途请求数**（默认 64，<= 0 表示不限）。理由不是"友善"，而是
 * **降低尾延迟**：S3 对同一前缀有请求速率上限（约 3500 PUT / 5500 GET 每秒），
 * 而 §10.2 的并行预取天生会并发发出大量 GET——不设上限的话，突发流量换来的是一片
 * `503 SlowDown`，接着被重试层翻译成更长的尾延迟（每次重试都在退避上等待）。
 * 在上游把并发压住，比在下游重试更便宜。
 *
 * 信号量只圈住**单次尝试**：退避等待期间不占名额（等待中的请求并不是"在途请求"，
 * 占着名额会白白缩容，让一次抖动波及到本来健康的请求）。
 *
 * ## 与其他装饰器的关系
 *
 * 它必须**套在埋点（[InstrumentedObjectStore]）之外**——见 `YangBaseServer.start()`
 * 的接线说明：埋点统计的是"这个进程真实发出的请求数"，一次重试就是一次真实请求。
 */
class RetryingObjectStore(
    private val delegate: ObjectStore,
    /** 单次操作最多尝试几次（含首次）。 */
    private val maxAttempts: Int = 4,
    /** 退避基数；第 n 次失败后等待 `base * 2^(n-1)`，再乘一个抖动系数。 */
    private val baseDelayMillis: Long = 20,
    private val maxDelayMillis: Long = 1_000,
    /**
     * 单次操作的总时限。超过它就不再重试，把最后一次的错误抛给调用方。
     *
     * 必须有：没有它时，一次操作最坏会等 `maxAttempts × maxDelay`，而这个时间会
     * 叠加在**每一次**点查上，把尾延迟从百毫秒推到秒级——那时"重试"就从
     * 提高可用性的手段变成了制造超时的手段。
     */
    private val deadlineMillis: Long = 5_000,
    /** 在途请求上限。<= 0 表示不限流。 */
    private val maxConcurrentRequests: Int = 64,
    /**
     * **全局重试预算**（每秒允许的额外尝试次数）。<= 0 表示不限。
     *
     * ## 为什么需要它
     *
     * 重试是**乘法**放大的：这一层 `maxAttempts=4`，而调用方（自动号段、租约、
     * 时间戳窗口）各自还有 16 次重试——最坏情况一次逻辑操作产生 **64 次真实请求**，
     * 每一次都计费。而在后端抖动/限流时，请求数随并发**线性**放大：
     * 账单最贵的时刻正好是最不该放大请求的时刻。
     *
     * 信号的并发上限解决不了这个（它限的是"同时多少个"，不是"总共多少次"）。
     * 这里用令牌桶限制**重试速率**：超预算的重试直接放弃（把错误交给调用方，
     * 让上层决定是降级还是失败）——那比"用双倍请求去撞一个正在抖动的后端"更好。
     */
    private val retryBudgetPerSecond: Double = 50.0,
) : ObjectStore {

    override val caps: ObjectStoreCapabilities get() = delegate.caps

    init {
        require(maxAttempts >= 1) { "maxAttempts 至少为 1（首次尝试）: $maxAttempts" }
    }

    private val permits: Semaphore? =
        if (maxConcurrentRequests > 0) Semaphore(maxConcurrentRequests) else null

    /**
     * 令牌桶（简单的按秒补充）。
     *
     * 只对**重试**扣令牌，首次尝试永远放行——预算的作用是"抖动时不放大"，
     * 不是"正常流量也限流"。
     */
    // 桶**初始是满的**：预算管的是"持续速率"，不是"冷启动时一次都不许重试"。
    // 从 0 开始会让进程刚起来时的一次瞬时抖动直接失败——那不是限流，是故障
    private val retryTokens =
        java.util.concurrent.atomic.AtomicLong(retryBudgetPerSecond.toLong().coerceAtLeast(1))
    private val retryTokensAt = java.util.concurrent.atomic.AtomicLong(System.nanoTime())

    private fun takeRetryToken(): Boolean {
        if (retryBudgetPerSecond <= 0) return true
        val now = System.nanoTime()
        val last = retryTokensAt.getAndSet(now)
        val elapsed = (now - last).coerceAtLeast(0)
        val refill = (elapsed / 1e9 * retryBudgetPerSecond).toLong()
        if (refill > 0) {
            val cap = retryBudgetPerSecond.toLong().coerceAtLeast(1)
            retryTokens.updateAndGet { minOf(cap, it + refill) }
        }
        while (true) {
            val cur = retryTokens.get()
            if (cur <= 0) return false
            if (retryTokens.compareAndSet(cur, cur - 1)) return true
        }
    }

    /**
     * 重试外壳。
     *
     * [op] 只用于指标与错误信息，取值与 `yb_objectstore_requests_total` 的 `op` 标签一致。
     */
    private suspend fun <T> retrying(op: StoreOp, block: suspend () -> T): T {
        val started = System.nanoTime()
        var attempt = 1
        while (true) {
            try {
                // 限流只圈住这一次尝试，不圈住退避等待
                return if (permits == null) block() else permits.withPermit { block() }
            } catch (e: Throwable) {
                if (!isRetryable(e)) throw e

                if (attempt >= maxAttempts) throw exhausted(op, e, attempt, "已达尝试次数上限 $maxAttempts")
                // ★ 全局重试预算：后端抖动时**不再放大请求数**（见 retryBudgetPerSecond）。
                // 超预算就把错误交给调用方——上层可以降级（读路径回对象存储重取、
                // 写路径明确失败），而"用双倍请求去撞一个正在抖动的后端"只会更糟
                if (!takeRetryToken()) {
                    throw exhausted(op, e, attempt, "重试预算用尽（$retryBudgetPerSecond 次/秒）")
                }
                val waited = backoffMillis(attempt)
                if (elapsedMillis(started) + waited > deadlineMillis) {
                    throw exhausted(op, e, attempt, "总时限 ${deadlineMillis}ms 已到")
                }
                Metrics.onObjectStoreRetry(op, if (e is ConcurrentModificationException) "conflict" else "transient")
                delay(waited)
                attempt++
            }
        }
    }

    /**
     * 重试判据。
     *
     * 只有两个来源为真：后端**显式**标记的瞬时故障，以及 409 冲突（条件结论未知）。
     * 其余一切（412 / 404 / 能力不支持 / 数据损坏 / 取消）都原样抛出去——
     * 注意 `CancellationException` 也走这条路：把取消当成可重试会吞掉协程取消。
     */
    private fun isRetryable(e: Throwable): Boolean = when {
        e is ConcurrentModificationException -> true
        e is ObjectStoreException -> e.retryable
        else -> false
    }

    /**
     * 放弃时的抛出。
     *
     * **冲突要保持类型**：调用方（`LeaseManager`、`AutoInc`、`Tso`、`Manifest`）的
     * 冲突处理是「重读当前状态再 CAS」，它们 catch 的是
     * [ConcurrentModificationException] / `PreconditionFailedException`。
     * 把冲突包装成别的类型会让那些路径静默失效——重试层反而制造了新的失败模式。
     *
     * 瞬时的存储故障则包装成 [ObjectStoreException]，并刻意把 [ObjectStoreException.retryable]
     * 标成 `false`：重试预算在这一层已经用尽，再标成可重试会诱使上层再叠一层，
     * 而**重试预算在每一层各算一份，总尝试次数是各层之积**。原始异常作为 cause 保留，
     * 需要更长期策略的调用方仍能看到它的类型与 `retryable`。
     */
    private fun exhausted(op: StoreOp, cause: Throwable, attempts: Int, why: String): Throwable {
        Metrics.onObjectStoreRetryExhausted(op)
        val msg = "${cause.message}（${op.wire} 已尝试 $attempts 次：$why）"
        return when (cause) {
            is ConcurrentModificationException -> ConcurrentModificationException(msg, cause)
            else -> ObjectStoreException(msg, cause, retryable = false)
        }
    }

    /**
     * 指数退避 + **抖动**。
     *
     * 抖动不是锦上添花：多个 CN 在同一个瞬时故障后如果同时重试，会在恢复的那一刻
     * 再次把对方打下去（重试风暴）；分区裁剪/预取又天然会让同一个进程内并发发出
     * 一批同时失败、同时重试的请求。随机化把它们错开。
     *
     * 用[全抖动]（在 `[capped/2, capped]` 之间取值）而不是固定间隔：期望等待更短，
     * 同时保留了「每多失败一次就等更久」的退让性。
     */
    private fun backoffMillis(attempt: Int): Long {
        val exp = baseDelayMillis shl minOf(attempt - 1, 20) // 移位前先夹住，避免溢出成负数
        val capped = minOf(exp, maxDelayMillis)
        val half = capped / 2
        return half + ThreadLocalRandom.current().nextLong(half + 1)
    }

    private fun elapsedMillis(startNanos: Long): Long = (System.nanoTime() - startNanos) / 1_000_000

    // ── 读路径 ────────────────────────────────────────────────────

    override suspend fun get(key: String, range: ByteRange?): ByteArray =
        retrying(if (range == null) StoreOp.GET else StoreOp.GET_RANGE) { delegate.get(key, range) }

    override suspend fun head(key: String): ObjectMeta =
        retrying(StoreOp.HEAD) { delegate.head(key) }

    override suspend fun list(prefix: String, pageToken: String?, limit: Int): ListPage =
        retrying(StoreOp.LIST) { delegate.list(prefix, pageToken, limit) }

    /**
     * 计数口径与 [InstrumentedObjectStore] 保持一致：`exists` 就是一次 HEAD。
     *
     * 重试的**内容**是 `delegate.exists`（后端可以覆写成比 HEAD 更廉价的方式），
     * 因此重试的是那个廉价操作，标签仍记成 HEAD。
     */
    override suspend fun exists(key: String): Boolean =
        retrying(StoreOp.HEAD) { delegate.exists(key) }

    // ── 写路径 ────────────────────────────────────────────────────

    override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag =
        retrying(if (precondition == null) StoreOp.PUT else StoreOp.PUT_CONDITIONAL) {
            delegate.put(key, data, precondition)
        }

    override suspend fun delete(key: String): Boolean =
        retrying(StoreOp.DELETE) { delegate.delete(key) }

}
