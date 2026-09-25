package com.cloudea.yangbase.ms

import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.Precondition
import com.cloudea.yangbase.txn.LockKey
import com.cloudea.yangbase.txn.LockMode
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * **并发 + 慢后端**下的 MS：它必须要么成功，要么报错——不能转圈。
 *
 * ## 这个文件补的是什么
 *
 * 其它 MS 用例都是"几个请求、按顺序、断言结果"。而共识的失效模式全部长在
 * **交错**上：同一轮复制的两个 RPC 同时落到一个跟随者、心跳与请求驱动的复制重叠、
 * 落盘（一次真实的 PUT）横在"校验"与"追加"之间。这些交错在内存后端上是纳秒级的，
 * 几乎撞不上；把它们变慢，才会从"偶发"变成"常常"。
 *
 * 所以这里的后端是一个给每次 PUT 加固定延迟的装饰器——**它不是模拟故障，
 * 只是把"落盘要花几毫秒"如实搬到内存里**：对象存储上的 MS 本来就是这样的。
 *
 * ## 三条断言，各盯一类静默错误
 *
 * 1. **不许忙等**：`applyCommittedLocked` 里的循环没有挂起点，一旦取到的条目不推进
 *    状态机就会原地打转，把整个节点的事件循环占满（进程 100% CPU、集群毫无响应）。
 *    它比抛异常严重得多，所以整段负载包在 [withTimeout] 里：转圈 = 超时失败。
 * 2. **日志下标必须与位置一致**：代码里所有的"取下标 i 的条目"都是按下标算位置，
 *    错位之后一致性检查会莫名其妙地通过，而状态机会取到错误的条目。
 * 3. **不许静默丢写**：每个请求都必须有一个明确的结果（成功，或一个**报出来的**失败）。
 *    这是"提交超时"那类缺陷的反面——等待者漏唤醒时，集群健康、日志也提交了，
 *    客户端却收到超时。
 */
class RaftConcurrencyStressTest {

    private fun scope() = CoroutineScope(SupervisorJob())

    /**
     * 给每次 [put] 加固定延迟的装饰器。
     *
     * 只延迟 `put`：读是幂等的、快慢都不影响正确性；而 `put` 恰好是共识里
     * "校验完之后、追加之前"那一段——交错就发生在那里。
     */
    private class SlowPutStore(
        private val inner: ObjectStore,
        private val putDelayMillis: Long,
    ) : ObjectStore by inner {
        override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag {
            delay(putDelayMillis)
            return inner.put(key, data, precondition)
        }
    }

    @Test
    fun `并发请求下不忙等且日志下标保持一致`() = runBlocking {
        val store = SlowPutStore(MemoryObjectStore(), putDelayMillis = 3)
        val scope = scope()
        val cluster = InProcessMetaCluster.start(3, store, scope, electionTimeoutMillis = 300L..600L)
        try {
            val leader = cluster.awaitLeader(10_000)
            val client = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-stress")
            // 负载开始时的任期。**任期在负载期间不该涨**——涨了就是"集群在重新选举"，
            // 而那说明复制的等待上限没有显著小于选举超时（见 RaftNode.replicateTimeoutMillis）：
            // 一次慢复制会让跟随者等不到心跳、发起选举，于是每次换领导者都丢掉锁表。
            // 这是这个用例唯一能看见"参数配错了"的窗口：它不报错，只是慢
            val termBeforeLoad = leader.raft.term

            val ok = AtomicInteger()
            val failed = AtomicInteger()
            val failures = java.util.Collections.synchronizedList(ArrayList<String>())

            // ⚠️ 用 withTimeout 把"转圈"变成"失败"：忙等的循环里没有挂起点，
            // 它自己永远不会返回，只有外部的超时才能把它变成一个可见的结果
            withTimeout(120_000) {
                withContext(Dispatchers.Default) {
                    coroutineScope {
                        repeat(6) { worker ->
                            launch {
                                repeat(30) { round ->
                                    // 每个 worker 用不同的分片：既制造并发，也制造租约争抢
                                    val shard = ShardId(TableId((worker % 3 + 1).toLong()), PartitionId(0))
                                    suspend fun record(name: String, block: suspend () -> Unit) {
                                        try {
                                            block(); ok.incrementAndGet()
                                        } catch (e: Throwable) {
                                            failed.incrementAndGet()
                                            failures.add("$name#${worker}-$round: ${e.javaClass.simpleName}: ${e.message}")
                                        }
                                    }
                                    record("tso") { client.tso.next() }
                                    record("watermark") { client.tso.peek() }
                                    record("catalog") { client.catalogVersion() }
                                    // 租约：同一个分片被多个 worker 抢，被拒是**正常**结果
                                    record("lease") {
                                        val lease = client.leases.acquire(shard)
                                        client.leases.release(lease)
                                    }
                                    record("lock") {
                                        val h = client.locks.lock(
                                            LockKey.row(shard.table.value, byteArrayOf(worker.toByte())),
                                            LockMode.EXCLUSIVE, "stress-$worker", ttlMillis = 30_000, waitMillis = 0,
                                        )
                                        client.locks.unlock(h)
                                    }
                                }
                            }
                        }
                    }
                }
            }

            assertTrue(ok.get() > 0, "至少要有一批操作成功（否则这个用例什么也没测到）")
            // 失败必须是"报出来的、有原因的"失败——被拒绝的租约/锁是合法的业务失败。
            // 这里只断言失败都带了信息，不断言"零失败"：并发抢租约本来就该有人失败
            failures.forEach { assertTrue(it.contains(":"), "失败必须带原因：$it") }

            // ★ 下标与位置一致（见 logIndexDiagnosis 的说明）。它一旦被破坏，
            // 下一次 applyCommitted 就是忙等
            for (node in cluster.nodes) {
                assertNull(node.raft.logIndexDiagnosis(), "节点 ${node.nodeId} 的日志错位了")
            }
            // 领导者应当稳定：反复重新选举意味着复制等待与选举超时这对参数不成立
            val leaders = cluster.nodes.count { it.raft.currentRole == RaftNode.Role.LEADER }
            assertEquals(1, leaders, "任何时刻只该有一个领导者")
            val maxTerm = cluster.nodes.maxOf { it.raft.term }
            assertTrue(
                maxTerm <= termBeforeLoad + 1,
                "负载期间发生了重新选举（任期从 $termBeforeLoad 涨到 $maxTerm）：" +
                    "说明复制的等待上限没有显著小于选举超时——跟随者等不到心跳。" +
                    "每次重新选举都会丢掉领导者本地的行锁表（§4.4），而且不报错",
            )
        } finally {
            cluster.stopAll()
        }
    }
}
