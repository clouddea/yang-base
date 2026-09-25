package com.cloudea.yangbase.ms

import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.Precondition
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * **两个"只在真实对象存储上才出现"的共识缺陷的回归测试**。
 *
 * 两个缺陷都是协调模式基准（`:yb-bench:benchCoordination`）与
 * `S3BackedMsTest` 跑出来的：内存后端上全绿，因为它们都长在
 * 「一次 PUT 要花几十毫秒」这个时间窗里。所以这里**只给 `put` 加延迟**——
 * 不是模拟故障，是把对象存储的真实代价如实搬到内存里。
 *
 * | 用例 | 缺陷 | 修法 |
 * |---|---|---|
 * | [等待者必须与进日志同一个临界区] | 注册晚一步 → 集群健康却报「多数派未能确认日志 N」 | 注册挪进 `log.add` 的临界区 |
 * | [日志落盘之前状态机不许推进] | 自己那一票没等落盘 → 状态机（与读）跑到持久性前面 | `persistedIndex` 记账（见 `RaftNode.advanceCommit`） |
 * | [响应必须晚于本节点落盘] | 不是上面那个缺陷的回归测试，见该用例的说明 | `persistEntryRecorded` 是 await 的 |
 * | [落盘失败必须报给客户端] | ——（守的是错误路径本身：不能假装成功） | `persistEntryRecorded` 把异常交还调用方 |
 *
 * ## 写这两个回归测试时踩到的坑（值得记住）
 *
 * 第一版两个用例**都抓不到缺陷**，而且原因各不相同：
 *
 * 1. 「等待者」用例给所有节点**一样长**的延迟——可领导者自己那次 PUT 与"心跳推给
 *    跟随者"是同时开始的，于是同时结束，那个窗口永远撞不上。真实的交错是
 *    **领导者的 PUT 比一个跟随者的往返更慢**（它的写排在别的流量后面）。
 * 2. 「持久性」用例从**响应**入手——但写请求的响应本来就卡在那次 await 上。
 *    必须从**读**入手（读不进日志，因此不等任何 PUT）。
 *
 * 教训：回归测试写完要**把缺陷改回去看它变不变红**。这两个用例都是在回退验证里
 * 才改成现在这个形状的。
 */
class RaftDurabilityRegressionTest {

    private fun scope() = CoroutineScope(SupervisorJob())

    /**
     * 给日志对象的 `put` 加延迟，并记录它**完成**的时刻。
     *
     * 只作用于日志对象（`meta/ms/log/...`）：硬状态（选票/任期）的写入保持即时，
     * 否则集群连选举都做不完，测的就不是写入路径了。
     *
     * ## 为什么延迟要**分节点**给
     *
     * 这是这份测试里唯一需要动脑的地方。第一个版本给所有节点的 PUT 加同样长的延迟
     * （60ms）——结果它**抓不到缺陷**：领导者自己那次 PUT 和"心跳把它推给跟随者"
     * 是同时开始的，于是双方同时结束，提交位点恰好落在等待者登记之后，那个窗口
     * 永远撞不上。
     *
     * 真实的交错长这样：**领导者的那次 PUT 比一个跟随者的往返更慢**（它的写请求排在
     * 别的流量后面），于是跟随者先确认、心跳那一轮先把条目提交掉，而领导者手里的
     * PUT 还没回来。这正是当初在 S3 上观测到的形状。所以这里按 `nodeId` 分别延迟：
     * 领导者慢、跟随者快——把"谁先到"这件事**确定下来**，而不是碰运气。
     */
    private class SlowLogStore(
        private val inner: ObjectStore,
        /** 领导者节点 id；选举完成后由用例设置（之前不知道谁会当选）。 */
        @Volatile var leaderNodeId: String = "",
        /** 领导者自己那次日志 PUT 的延迟。 */
        @Volatile var leaderPutDelayMillis: Long = 0,
        /** 跟随者的日志 PUT 延迟。 */
        private val followerPutDelayMillis: Long = 0,
        /** true 时所有日志对象的写入都失败（硬状态照常写，集群仍能选出领导者）。 */
        private val failLogPuts: Boolean = false,
    ) : ObjectStore by inner {

        /** 最近一次日志对象 PUT **完成**的时刻（`System.nanoTime()`）。 */
        @Volatile
        var lastLogPutFinishedNanos: Long = 0

        override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag {
            val isLog = key.contains("/log/")
            if (isLog) {
                val isLeader = leaderNodeId.isNotEmpty() && key.contains("/log/$leaderNodeId/")
                val d = if (isLeader) leaderPutDelayMillis else followerPutDelayMillis
                if (d > 0) delay(d)
            }
            if (isLog && failLogPuts) throw java.io.IOException("对象存储写失败（本用例构造）")
            val etag = inner.put(key, data, precondition)
            if (isLog) lastLogPutFinishedNanos = System.nanoTime()
            return etag
        }
    }

    /**
     * ★ **落盘期间被心跳提交掉的请求，仍然必须拿到响应。**
     *
     * 缺陷的原形：`handle()` 原来在 `persistEntry`（一次 PUT）**之后**才把等待者登记进
     * `pending`。而心跳协程每 50ms 就会把这条日志推给跟随者并提交它 → 提交位点一推进就
     * 立刻应用（`applyCommittedLocked` 去 `pending` 里找等待者）——**那时它还没登记**。
     * 于是等待者挂在一个已经被应用过的下标上，永远等不到唤醒，最后报
     * 「多数派未能确认日志 N」：集群完全健康、日志也确实提交了，客户端却收到一个失败。
     *
     * 所以这里连着发 20 笔：**只要有一笔落在那个窗口里，旧实现就会报错**。
     * 修好之后这个用例与时间无关——等待者与 `log.add` 在同一个临界区，不存在窗口。
     */
    @Test
    fun `等待者必须与进日志同一个临界区`() = runBlocking {
        val store = SlowLogStore(
            MemoryObjectStore(),
            leaderPutDelayMillis = 200,      // 领导者那次 PUT：慢
            followerPutDelayMillis = 5,      // 跟随者的往返：快（于是心跳先确认完）
        )
        val cluster = InProcessMetaCluster.start(
            3, store, scope(),
            electionTimeoutMillis = 300L..600L, heartbeatMillis = 50,
        )
        try {
            val leader = cluster.awaitLeader(10_000)
            store.leaderNodeId = leader.nodeId
            val client = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-waiter")
            repeat(20) { i ->
                // 抛异常 = 客户端收到了那个假的"提交超时"（旧实现就是这个症状）
                val v = client.bumpCatalogVersion()
                assertTrue(v >= 1, "第 ${i + 1} 笔提交必须成功返回，而不是报'多数派未能确认'（v=$v）")
            }
        } finally {
            cluster.stopAll()
        }
    }

    /**
     * ★ **状态机不许跑到持久性前面：日志还没落到对象存储时，读不能看见它。**
     *
     * Raft 的「多数派已确认」里，领导者自己那一票必须建立在"我的副本已经在对象存储里"
     * 之上。单节点集群里自己是唯一多数派，所以旧实现里 `log.add`（一次内存操作）就够了
     * ——心跳每 50ms 一次 `advanceCommit`，于是**在 PUT 落地之前**条目就被提交并应用了。
     *
     * ## 为什么这里必须用**读**来观察
     *
     * 写请求自己的响应本来就晚于它那次 PUT（`persistEntryRecorded` 是 await 的），
     * 所以「响应早于落盘」这个角度**测不出**这个缺陷——第一版用例就是这么写的，
     * 把 `own` 改回无条件 1 之后它照样通过。
     *
     * 真正漏出去的是**读**：读请求不进日志，因此不等任何 PUT。状态机一旦提前推进，
     * 水位 / 租约 epoch / catalog 版本立刻对读可见，而它们在崩溃后会退回去
     * （重发已经发出去的水位区间是 MVCC 失去全序的那类错误）。
     *
     * 所以断言是：**在落盘完成之前，读到的版本号必须还是旧的。**
     */
    @Test
    fun `日志落盘之前状态机不许推进`() = runBlocking {
        // 领导者落盘 200ms；心跳 50ms 一次 → 旧实现里至少三轮心跳能抢先提交
        val store = SlowLogStore(MemoryObjectStore(), leaderPutDelayMillis = 200)
        val cluster = InProcessMetaCluster.start(
            1, store, scope(),
            electionTimeoutMillis = 300L..600L, heartbeatMillis = 50,
        )
        try {
            val leader = cluster.awaitLeader(10_000)
            store.leaderNodeId = leader.nodeId
            val client = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-durable")

            // 先写一笔并等它返回：这同时确认了"本任期的第一条日志已提交"（领导者就绪），
            // 之后的读请求不会再撞上"尚未就绪"而报错
            client.bumpCatalogVersion()
            val before = client.catalogVersion()

            // 在后台发起一笔写（它会花 200ms 落盘），然后趁它还没落完读一次
            val job = launch(Dispatchers.Default) { client.bumpCatalogVersion() }
            delay(120)                       // > 两次心跳（50/100ms），< 落盘（200ms）
            val during = client.catalogVersion()
            job.join()

            assertEquals(
                before, during,
                "日志还在落盘（200ms）时，读就已经看到新版本号了——" +
                    "说明'提交/应用'跑在了'落盘'前面。崩溃后这个版本会退回去。",
            )
        } finally {
            cluster.stopAll()
        }
    }

    /**
     * **响应必须晚于本节点落盘。**
     *
     * 注意：这条**不是**上面那个缺陷的回归测试——把 `own` 改回无条件 1，它照样通过，
     * 因为写请求的响应本来就卡在 `persistEntryRecorded` 那次 await 上（那是另一条路径
     * 的保证）。留在这里是因为它是一个真实的、值得钉住的性质：将来若有人把
     * `persistEntryRecorded` 挪到等待之后，这条会立刻变红。
     */
    @Test
    fun `响应必须晚于本节点落盘`() = runBlocking {
        val store = SlowLogStore(MemoryObjectStore(), leaderPutDelayMillis = 100)
        val cluster = InProcessMetaCluster.start(
            1, store, scope(),
            electionTimeoutMillis = 300L..600L, heartbeatMillis = 50,
        )
        try {
            val leader = cluster.awaitLeader(10_000)
            store.leaderNodeId = leader.nodeId
            val client = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-ack")
            store.lastLogPutFinishedNanos = 0
            val t0 = System.nanoTime()
            client.bumpCatalogVersion()
            val t1 = System.nanoTime()

            assertTrue(
                store.lastLogPutFinishedNanos >= t0,
                "这次调用必须真的写了一个日志对象（否则这个用例什么也没测到）",
            )
            assertTrue(
                t1 >= store.lastLogPutFinishedNanos,
                "响应必须晚于日志落盘：落盘完成于 ${store.lastLogPutFinishedNanos}，" +
                    "而响应在 $t1（早了 ${(store.lastLogPutFinishedNanos - t1) / 1_000_000}ms）",
            )
        } finally {
            cluster.stopAll()
        }
    }

    /**
     * ★ **日志落盘失败时，客户端必须收到那个失败**——不能等到超时，更不能成功。
     *
     * 这条守的是 `persistEntryRecorded` 的错误路径：条目已经进了内存日志，但没落到
     * 对象存储。此时若回一个成功，客户端就会认为写入发生了（它有可能是被后来的日志
     * 覆盖掉的半成品）；而若回一个"提交超时"，排查方向会被指到共识与网络上去。
     * 正确的做法是把**存储层的那个原因**交还给客户端。
     */
    @Test
    fun `落盘失败必须报给客户端`() = runBlocking {
        val store = SlowLogStore(MemoryObjectStore(), failLogPuts = true)
        val cluster = InProcessMetaCluster.start(1, store, scope(), electionTimeoutMillis = 300L..600L)
        try {
            cluster.awaitLeader(10_000)
            val client = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-failput")
            val e = assertFailsWith<Exception>("日志写不进去时，写请求必须明确失败") {
                client.bumpCatalogVersion()
            }
            assertTrue(
                e.message!!.contains("对象存储写失败"),
                "错误信息要指向真正的原因（落盘失败），而不是'多数派未能确认'：${e.message}",
            )
        } finally {
            cluster.stopAll()
        }
    }

    /** 单节点集群也必须能选出领导者（多数派 = 自己），否则上面两个用例都无从谈起。 */
    @Test
    fun `单节点集群在慢后端上仍能选出领导者`() = runBlocking {
        val store = SlowLogStore(MemoryObjectStore(), followerPutDelayMillis = 30)
        val cluster = InProcessMetaCluster.start(1, store, scope(), electionTimeoutMillis = 300L..600L)
        try {
            val leader = cluster.awaitLeader(10_000)
            assertEquals(com.cloudea.yangbase.ms.RaftNode.Role.LEADER, leader.raft.currentRole)
        } finally {
            cluster.stopAll()
        }
    }
}
