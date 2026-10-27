package com.cloudea.yangbase.ms

import com.cloudea.yangbase.objectstore.MemoryObjectStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Raft 本身的行为（设计文档 §4.6）：选举、复制、故障切换、重启恢复。
 *
 * ## 这些断言为什么必须存在
 *
 * 共识算法的缺陷几乎全是"偶发且静默"的：日志回滚错一次、提交位点推进早一格，
 * 症状都是"某次故障之后数据不对"，而不是某个请求报错。所以这里对着三件最容易
 * 写错的事各写一条断言：
 *
 * 1. **多数派**：少数派（1/3）活着的集群不能选出领导者，也不能提交——
 *    否则一个网络分区就会分裂出两个领导者，各自接受写入；
 * 2. **领导者更替不丢已提交的状态**：新领导者必须已经包含所有已提交的日志；
 * 3. **重启后状态不丢**：水位、租约、catalog 版本都从日志/快照恢复。
 */
class RaftTest {

    private fun scope() = CoroutineScope(SupervisorJob())

    /**
     * ★ **下台时必须当场失败未确认的写入**（issue #5 的真身）。
     *
     * 这个用例守的是一条诊断质量 + 可用性：领导者受理了一笔写入、还没复制给多数派，
     * 就收到了一个更高任期的 `AppendEntries`（最常见的一次换人）。旧实现在那条路径上
     * 只把 `role` 改成 FOLLOWER，**不唤醒等待者**——于是调用方挂满整个提交预算
     * （CI 上是 5000ms）之后才收到「多数派未能确认日志 N」，把人指向网络与共识，
     * 而真相是「换人了，这笔写入结果未知」。
     *
     * 提交预算在这里设成 30s：如果等待者真的靠超时兜底，本用例会超时失败，
     * 而不是"碰巧也通过"。
     */
    @Test
    fun `领导者下台时未确认的写入立刻失败而不是挂到提交超时`() = runBlocking<Unit> {
        val store = MemoryObjectStore()
        val s = scope()
        val cluster = InProcessMetaCluster.start(
            3, store, s, electionTimeoutMillis = 300L..600L, commitTimeoutMillis = 30_000,
        )
        try {
            val leader = cluster.awaitLeader()
            // 跟随者全下线 → 这笔写入凑不齐多数派，只能干等
            cluster.nodes.filter { it !== leader }.forEach { it.stop() }

            val call = async { leader.handle(MetaRequest.ReserveTimestamp(0, 1)) }
            // 给它时间走完"进日志 → 落盘 → 注册等待者"
            delay(500)

            // 更高任期的 AppendEntries：这就是换人时跟随者/新领导者会发来的东西
            leader.handleRaft(
                RaftMessage.AppendEntries(
                    term = 99, leaderId = "ms-9",
                    prevLogIndex = 0, prevLogTerm = 0, entries = emptyList(), leaderCommit = 0,
                ),
            )

            // 3s 足够宽松，而提交预算是 30s：拿不到就说明还在挂
            val resp = withTimeout(3_000) { call.await() }
            val err = assertIs<MetaResponse.Error>(resp)
            assertTrue(
                err.message.contains("领导者已变更"),
                "必须说清是换人导致的结果未知，而不是甩一句「多数派未能确认」：${err.message}",
            )
            assertTrue(
                err.message.contains("重试"),
                "这类请求（分配时间戳/号段/租约）重试是安全的，错误信息必须指路：${err.message}",
            )
        } finally {
            cluster.stopAll()
        }
    }

    /**
     * ★ 多数派才能选出领导者。
     *
     * 只启动 2 个节点的集群（quorum = 2）时必须能选出领导者；
     * 而把其中一个停掉之后（1/2 < 多数派）**不能再选出新的领导者**，
     * 已经在任的领导者也无法提交新的日志。
     */
    @Test
    fun `多数派才能选出领导者并提交`() = runBlocking<Unit> {
        val store = MemoryObjectStore()
        val s = scope()
        val cluster = InProcessMetaCluster.start(3, store, s, electionTimeoutMillis = 300L..600L)
        try {
            val leader = cluster.awaitLeader(5_000)
            // 写入一笔，确认它被提交
            val client = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-1")
            val first = client.bumpCatalogVersion()
            assertTrue(first >= 1)

            // 关掉两个节点：只剩 1/3，少数派
            val others = cluster.nodes.filter { it.address != leader.address }
            cluster.transport.unregister(others[0].address)
            cluster.transport.unregister(others[1].address)
            others.forEach { it.stop() }

            // 少数派里的这个领导者不能再提交（它连不上多数派）
            val e = runCatching { client.bumpCatalogVersion() }.exceptionOrNull()
            assertNotNull(e, "少数派中的领导者不该能提交新的日志（否则分区会分裂出两个领导者）")
        } finally {
            cluster.stopAll()
        }
    }

    /**
     * ★ 领导者更替之后，**已提交的状态必须还在**。
     *
     * 这是 Raft 最容易写错的一处：新领导者如果没等"本任期的日志提交"就开始服务，
     * 或者 matchIndex 推进得比实际复制位置快，就会丢已提交的写入。
     */
    @Test
    fun `领导者更替后已提交的状态仍在`() = runBlocking {
        val store = MemoryObjectStore()
        val s = scope()
        val cluster = InProcessMetaCluster.start(3, store, s, electionTimeoutMillis = 300L..600L)
        try {
            val leader1 = cluster.awaitLeader(5_000)
            val client = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-1")
            // 写三笔可观察的状态：时间戳区间、catalog 版本
            var maxTs = 0L
            repeat(3) { maxTs = client.tso.next().value }
            val v1 = client.bumpCatalogVersion()

            // 让领导者"失联"（从传输层摘掉，模拟它挂了/被分区）
            cluster.transport.unregister(leader1.address)
            leader1.stop()

            // 剩下的两个节点必须自己选出新领导者，并保留已提交的状态
            val leader2 = cluster.awaitLeader(10_000)
            assertTrue(leader2.address != leader1.address, "应当在剩下的节点里选出新领导者")
            assertTrue(leader2.raft.term > leader1.raft.term, "新领导者的任期必须更大")

            val v2 = client.catalogVersion()
            assertEquals(v1, v2, "已提交的 catalog 版本不能丢")
            // 时间戳**绝不回退**：新领导者必须从已提交的水位之后继续发号
            val next = client.tso.next().value
            assertTrue(next > maxTs, "领导者更替后时间戳回退了：$next <= $maxTs（会让 MVCC 失去全序）")
        } finally {
            cluster.stopAll()
        }
    }

    /**
     * ★ 重启后状态从日志与快照恢复。
     *
     * 一个 3 节点的集群全部重启（模拟"MS × 3 可整体重启"，§2.1）之后：
     * 水位、catalog 版本都必须在，且新发的时间戳不回退。
     */
    @Test
    fun `全部重启后状态从对象存储恢复`() = runBlocking {
        val store = MemoryObjectStore()
        val s1 = scope()
        var maxTs = 0L
        var version = 0L
        val addresses: List<String>

        val cluster1 = InProcessMetaCluster.start(3, store, s1, electionTimeoutMillis = 300L..600L)
        try {
            cluster1.awaitLeader(5_000)
            val client = MetaServiceClient(MetaCluster(cluster1.addresses), cluster1.transport, "cn-1")
            repeat(5) { maxTs = client.tso.next().value }
            version = client.bumpCatalogVersion()
            addresses = cluster1.addresses
        } finally {
            cluster1.stopAll()
        }

        // 全部重启：新的 RaftNode 实例，同一个对象存储
        val s2 = scope()
        val cluster2 = InProcessMetaCluster.start(3, store, s2, electionTimeoutMillis = 300L..600L)
        try {
            cluster2.awaitLeader(10_000)
            val client = MetaServiceClient(MetaCluster(addresses), cluster2.transport, "cn-1")
            client.tso.recover()
            val next = client.tso.next().value
            assertTrue(next > maxTs, "重启后时间戳回退了：$next <= $maxTs")
            assertEquals(version, client.catalogVersion(), "重启后 catalog 版本必须还在（它来自日志/快照）")
        } finally {
            cluster2.stopAll()
        }
    }

    /**
     * ★ 租约在领导者更替后仍然有效（它**进**共识日志）。
     *
     * 与锁形成对照：锁是领导者本地状态（换领导者就没了，§4.4），租约必须活下来
     * ——否则每次 MS 换领导，所有 CN 都会以为自己的写者身份没了，写入全线抖动。
     */
    @Test
    fun `租约在领导者更替后仍然有效`() = runBlocking {
        val store = MemoryObjectStore()
        val s = scope()
        val cluster = InProcessMetaCluster.start(3, store, s, electionTimeoutMillis = 300L..600L)
        try {
            val leader1 = cluster.awaitLeader(5_000)
            val client = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-writer")
            val shard = com.cloudea.yangbase.common.ShardId(
                com.cloudea.yangbase.common.TableId(42),
                com.cloudea.yangbase.common.PartitionId(0),
            )
            val lease = client.leases.acquire(shard)

            cluster.transport.unregister(leader1.address)
            leader1.stop()
            val leader2 = cluster.awaitLeader(10_000)
            assertTrue(leader2.address != leader1.address)

            // 续期必须仍然成功（租约在共识状态里，不随领导者消失）
            val renewed = client.leases.renew(lease)
            assertEquals(lease.epoch, renewed.epoch, "续期不换 epoch")
            assertTrue(renewed.remainingMillis() > 0)

            // 而另一个写者仍然抢不到
            val other = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-other")
            assertTrue(
                runCatching { other.leases.acquire(shard) }.isFailure,
                "已被持有的租约在领导者更替后仍不该被抢走",
            )
        } finally {
            cluster.stopAll()
        }
    }

    /** 单节点集群也能选自己（开发与嵌入式部署的形态）。 */
    @Test
    fun `单节点集群可以立刻选出领导者`() = runBlocking {
        val cluster = InProcessMetaCluster.start(1, MemoryObjectStore(), scope(), electionTimeoutMillis = 50L..100L)
        try {
            val leader = cluster.awaitLeader(5_000)
            val client = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-1")
            assertTrue(client.tso.next().value > 0)
            assertEquals(1L, client.bumpCatalogVersion())
            assertEquals("ms", client.kind)
        } finally {
            cluster.stopAll()
        }
    }
}
