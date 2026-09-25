package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.ShardId

/**
 * 分片写者租约（设计文档 §4.3）在存储层的**接口**。
 *
 * 存储层需要知道「我是不是这个分片当前的写者」，但**不需要知道协调是怎么做的**——
 * 那是对象存储 CAS 还是 Raft 元数据服务，是部署形态问题（§4.7 的两种模式）。
 * 接口定义在这里、实现放在 yb-txn，依赖方向因此保持单向。
 */
interface ShardLease {

    /** 租约的世代号。**单调递增、永不重置**，写进 WAL 的对象前缀。 */
    val epoch: Long

    /** 持有人，仅用于诊断。 */
    val holder: String

    fun isExpired(now: Long = System.currentTimeMillis()): Boolean

    fun remainingMillis(now: Long = System.currentTimeMillis()): Long
}

/** 租约的取得与续期。 */
interface LeaseProvider {

    /**
     * 取得分片的写者租约。
     *
     * 已被别人持有时**应当抛异常**而不是阻塞等待——调用方是一个正要打开分片的 CN，
     * 它应该立刻转成只读或换一个分片，而不是卡在这里。
     */
    suspend fun acquire(shard: ShardId): ShardLease

    /** 续期。epoch 不变——续期是同一个写者继续持有，不是换了人。 */
    suspend fun renew(lease: ShardLease): ShardLease

    /** 主动释放。实现必须保证**下一次取得的 epoch 仍然更大**。 */
    suspend fun release(lease: ShardLease)
}

/**
 * 无租约模式：**假设同一分片只有一个进程在写**。
 *
 * 这是当前默认，也是一个**必须显式承认的假设**：它对应「单 CN 部署」，
 * 在该前提下完全正确且零开销。一旦有第二个 CN 打开同一个分片，
 * fencing 就只剩 manifest 的 CAS 一道防线，两个写者会往同一个 WAL 前缀下写对象——
 * 输的那一方写下的日志仍会被下一个恢复者重放。
 *
 * 所以它**不是"没有协调"，而是"协调由部署方式保证"**。
 */
object SingleProcessLease : LeaseProvider {
    private val lease = object : ShardLease {
        override val epoch: Long = 1
        override val holder: String = "single-process"
        override fun isExpired(now: Long) = false
        override fun remainingMillis(now: Long) = Long.MAX_VALUE
    }

    override suspend fun acquire(shard: ShardId): ShardLease = lease
    override suspend fun renew(lease: ShardLease): ShardLease = lease
    override suspend fun release(lease: ShardLease) = Unit
}
