package com.cloudea.yangbase.ms

import com.cloudea.yangbase.objectstore.ByteRange
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectMeta
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.ObjectStoreCapabilities
import com.cloudea.yangbase.objectstore.Precondition
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * 选举在**高延迟后端**上必须仍然收敛。
 *
 * ## 这个文件补的是什么
 *
 * 2026-10-26 在真实阿里云 OSS 上跑集成测试时，`MultiCnCoordinationTest` 的 5 个用例
 * 全部失败在同一个地方：`MS 集群没有选出领导者`，三个节点都是 `role=CANDIDATE
 * term=35 commit=0 log=0`。同样的代码跑内嵌 S3（同机、往返亚毫秒）全绿——所以它
 * 不是逻辑错误，而是**选举超时的随机化在高延迟下失效**：
 *
 * `electionLoop` 每 20ms 重新抽一次随机超时，于是实际等待时间收敛到区间的**下沿**
 * （每一次 tick 都是一次"这次够久了吗"的重新掷骰），三个同时启动的节点的抽签结果
 * 高度相关；再叠加"投票前要先把硬状态 PUT 到对象存储"的固定开销（跨洲 200ms+），
 * 每个节点都在同一时刻变成候选人、都先投自己、再互相拒绝——选票被反复瓜分，
 * 任期一路涨到几十，谁也当不上领导者。这正是类注释里警告过的"选举风暴"，
 * 只是它的触发条件不是"超时太小"，而是"超时没有被真正随机化"。
 *
 * ## 为什么用一个人造慢存储，而不是连真实 OSS
 *
 * 单元测试不该依赖云。这个桩把**唯一相关的变量**（每次对象存储操作几百毫秒的固定
 * 延迟）抽出来，于是本用例可以在任何机器上确定性地复现跨洲部署的选举行为。
 * 真实 OSS 的证据由 CI 的 `OSS conformance` workflow 提供，两者分工不同。
 */
class RaftHighLatencyElectionTest {

    /**
     * 给每次对象存储操作加上固定延迟的包装。
     *
     * 延迟加在**操作内部**而不是 RPC 上：MS 在持有互斥锁的情况下落盘硬状态与日志，
     * 这正是高延迟后端改变选举时序的地方。只慢 RPC 会放过真正的瓶颈。
     */
    private class SlowStore(
        private val delegate: ObjectStore = MemoryObjectStore(),
        private val latencyMillis: Long,
    ) : ObjectStore {
        override val caps: ObjectStoreCapabilities get() = delegate.caps

        override suspend fun get(key: String, range: ByteRange?): ByteArray {
            delay(latencyMillis); return delegate.get(key, range)
        }

        override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag {
            delay(latencyMillis); return delegate.put(key, data, precondition)
        }

        override suspend fun delete(key: String): Boolean {
            delay(latencyMillis); return delegate.delete(key)
        }

        override suspend fun list(prefix: String, pageToken: String?, limit: Int): com.cloudea.yangbase.objectstore.ListPage {
            delay(latencyMillis); return delegate.list(prefix, pageToken, limit)
        }

        override suspend fun head(key: String): ObjectMeta {
            delay(latencyMillis); return delegate.head(key)
        }
    }

    /**
     * 三个节点、每次对象存储操作 150ms（≈ 跨洲 RTT）、选举超时 1.5–3s（生产量级）。
     * 必须在这段时间里选出领导者。
     *
     * 用 `term` 做断言的一部分：失败时的诊断信息里任期是多少，直接告诉你
     * "是选举没收敛（任期一大截）"还是"集群根本没起来（任期还是 0）"。
     */
    @Test
    fun `高延迟后端上三个节点必须选出领导者`() = runBlocking {
        val store = SlowStore(latencyMillis = 400)
        val scope = CoroutineScope(SupervisorJob())
        val cluster = InProcessMetaCluster.start(
            count = 3,
            store = store,
            scope = scope,
            electionTimeoutMillis = 1_500L..3_000L,
            // 提交预算与选举无关，这里放宽：本用例测的是"选不选得出来"，
            // 不是"一次提交能不能在 1.5s 内完成"
            commitTimeoutMillis = 10_000,
        )
        try {
            val leader = cluster.awaitLeader(30_000)
            assertTrue(
                leader.raft.term <= 20,
                "领导者选出来了，但任期涨到 ${leader.raft.term}：说明选举一直在重试，" +
                    "超时的随机化没有起到去相关的作用（诊断：${cluster.nodes.map { it.describe() }}）",
            )
            // 选出来之后必须真的能用：一次提交要能在延迟后端上完成
            val client = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-1")
            assertTrue(client.bumpCatalogVersion() >= 1)
        } finally {
            cluster.stopAll()
        }
    }
}
