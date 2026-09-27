package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.CommitTs
import com.cloudea.yangbase.common.CorruptDataException
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.objectstore.ObjectStore
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * 全局时间戳分配器（设计文档 §4.2）。
 *
 * 时间戳编码为 64 位整数：
 * ```
 *  63        46 45                    18 17                    0
 *  ┌───────────┬───────────────────────┬───────────────────────┐
 *  │ 保留 (18) │  物理时间 ms (28 bit)  │  逻辑计数 (18 bit)     │
 *  └───────────┴───────────────────────┴───────────────────────┘
 * ```
 * 物理部分让时间戳天然可用于「时间旅行」的近似映射，逻辑部分处理同毫秒内的并发。
 */
interface Tso {
    /** 分配一个提交时间戳。**必须单调递增。** */
    suspend fun next(): CommitTs

    /** 当前水位（不推进）。 */
    suspend fun peek(): CommitTs

    /**
     * 把水位同步到权威副本。**进程启动后必须调用一次**。
     *
     * 不调用的话，重启后发出去的时间戳可能**小于**崩溃前已经发过的值——
     * 时间戳回退会让 MVCC 的可见性失去全序，而且是静默的（见 [WindowTso.recover]）。
     * 内存实现（[MonotonicTso]）没有持久状态，默认实现什么都不做。
     */
    suspend fun recover() {}

    fun describe(): String
}

/**
 * 单调时间戳分配器。
 *
 * 实现不变量：`next()` 返回的值**严格递增**，即使系统时钟回拨也不会回退。
 * 时钟回拨时自旋等待物理时间追上逻辑水位——这比返回一个更小的时间戳安全得多，
 * 后者会破坏 MVCC 的可见性序。
 */
class MonotonicTso(private val logicalBits: Int = 18) : Tso {

    private val mutex = Mutex()
    private var lastMillis: Long = 0
    private var counter: Long = 0

    private val maxCounter = (1L shl logicalBits) - 1

    override suspend fun next(): CommitTs = mutex.withLock {
        var now = System.currentTimeMillis()
        if (now < lastMillis) {
            // 时钟回拨：自旋到物理时间追平，绝不回退时间戳
            while (now < lastMillis) {
                Thread.sleep(1)
                now = System.currentTimeMillis()
            }
        }
        if (now == lastMillis) {
            counter++
            if (counter > maxCounter) {
                // 同毫秒内逻辑位耗尽：等到下一毫秒
                while (System.currentTimeMillis() <= lastMillis) Thread.sleep(0, 100_000)
                now = System.currentTimeMillis()
                counter = 0
            }
        } else {
            lastMillis = now
            counter = 0
        }
        CommitTs((now shl logicalBits) or counter)
    }

    override suspend fun peek(): CommitTs = CommitTs(lastMillis shl logicalBits or counter)

    override fun describe() = "MonotonicTso(本地内存)"

    companion object {
        fun physicalMillis(ts: Long, logicalBits: Int = 18): Long = ts ushr logicalBits
    }
}

/**
 * 把时间戳水位持久化到**对象存储**的 TSO —— 没有常驻 MS 时的那个 TSO（设计文档 §4.2）。
 *
 * 现在它只是 [WindowTso] + [ObjectStoreTimestampSource] 的组合：分配逻辑（实时
 * `commit_ts`、区间兜底、读时间戳下界）与 MS 模式**共用同一份实现**。
 * 这不是为了少写代码——"commit_ts 的高位必须是提交那一刻的毫秒"这类不变量
 * 一旦有两份实现，必然漂移，而漂移的表现是 `AS OF` 读到不属于那个时刻的数据库。
 *
 * 关于时间戳的构成、两阶段分配、以及区间为什么按毫秒算（而不是按个数），
 * 见 [WindowTso] 与 [TimestampSource] 的说明。
 */
class PersistentTso(
    store: ObjectStore,
    clusterId: String,
    batchMillis: Long = 1_000,
    logicalBits: Int = 18,
    /** 见 [ObjectStoreTimestampSource.allowNonAtomicCas]：后端没有 CAS 时的显式降级开关。 */
    allowNonAtomicCas: Boolean = false,
) : Tso {

    private val inner = WindowTso(
        ObjectStoreTimestampSource(store, clusterId, allowNonAtomicCas), logicalBits, batchMillis,
    )

    override suspend fun next(): CommitTs = inner.next()

    override suspend fun peek(): CommitTs = inner.peek()

    override suspend fun recover() = inner.recover()

    override fun describe() = inner.describe()
}

/** 只读事务的 start_ts 可预取（设计文档 §4.2）。 */
data class TimestampBatch(val base: Long, val count: Int)

object TsoUtil {
    fun readTsOf(ts: CommitTs): ReadTs = ReadTs(ts.value)
}
