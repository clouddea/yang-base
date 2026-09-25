package com.cloudea.yangbase.ms

import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.txn.CoordinationService
import com.cloudea.yangbase.txn.LockKey
import com.cloudea.yangbase.txn.LockMode
import com.cloudea.yangbase.txn.ObjectStoreCoordination
import com.cloudea.yangbase.txn.SingleProcessCoordination
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * **协调能力的一致性测试套件**（设计文档 §4.7 的硬性要求）。
 *
 * > 「由于纯对象存储模式的租约边界情况极难穷尽，**两种模式必须共用同一套
 * > 并发/故障注入测试**（把 `CoordinationService` 作为可替换的实现注入）。
 * > 这是 Q2 决策带来的额外测试成本，必须在 M1 的验收中包含。」
 *
 * 所以这个文件里没有一行是"给 MS 写的"或"给对象存储写的"——**同一套断言对三种实现
 * 各跑一遍**（单进程 / 对象存储 CAS / MS 三节点 Raft）。任何一条在某个实现上失败，
 * 就是那个实现违反了 `CoordinationService` 的契约；契约本身写在 `yb-txn` 的
 * `Coordination.kt` 里。
 *
 * ## 为什么这套测试值得单独存在
 *
 * 两种模式的**失败方式完全不同**：对象存储模式错在 CAS 的竞态窗口，
 * MS 模式错在共识的边界（选举、任期、日志回滚）。而它们的**共同契约**只有三条：
 *
 * 1. 时间戳区间**互不重叠**（重叠 = 重复的 commit_ts = MVCC 失去全序）；
 * 2. 租约的 epoch **单调递增、永不重置**（重置 = 旧写者的 WAL 被当成新写者的日志重放）；
 * 3. 排他锁**真的互斥**（不互斥 = 静默的丢更新）。
 *
 * 这三条都是"违反之后不报错、只是数据慢慢坏掉"，所以必须逐条断言。
 */
class CoordinationConformanceTest {

    /**
     * 一套**共享后端**的协调实现。
     *
     * ⚠️ `open` 必须把多个客户端指向**同一个后端**（同一个对象存储 / 同一个 MS 集群）。
     * 第一版这里是"每次 open 都新建一个后端"，于是两个"客户端"活在两个互不相干的世界里
     * ——争用测试全部通过（因为根本没有争用），却什么也没验证。见各用例的注释。
     */
    private class Rig(
        val name: String,
        val open: (String) -> CoordinationService,
        val close: suspend () -> Unit,
    )

    private suspend fun rigs(): List<Rig> {
        val store = MemoryObjectStore()
        // 单进程模式：**只有一个协调者**（这正是"单 CN 部署"的含义）。
        // 两个"客户端"共用它，才能测出租约/锁在该模式下的真实语义
        val single = SingleProcessCoordination("cn-shared")
        val msStore = MemoryObjectStore()
        val scope = CoroutineScope(SupervisorJob())
        // 选举超时必须**显著大于**心跳间隔：50-90ms 与 50ms 的心跳一样紧，
        // 任何一次调度抖动都会触发重新选举，于是"领导者"在测试里不停地换——
        // 而锁是领导者本地状态（§4.4），换一次领导者锁就全没了。
        // 第一版就是这么写的，表现为"锁没有互斥"（其实是对面换了个人）
        val cluster = InProcessMetaCluster.start(3, msStore, scope, electionTimeoutMillis = 300L..600L)
        cluster.awaitLeader(5_000)
        return listOf(
            Rig("single", { single }, {}),
            Rig(
                "objectstore",
                { holder -> ObjectStoreCoordination(store, "conf", holder, leaseTtlMillis = 30_000) },
                {},
            ),
            Rig(
                "ms",
                { holder ->
                    MetaServiceClient(
                        cluster = MetaCluster(cluster.addresses),
                        transport = cluster.transport,
                        nodeId = holder,
                        leaseTtlMillis = 30_000,
                    )
                },
                { cluster.stopAll() },
            ),
        )
    }

    // ── 契约 1：时间戳 ────────────────────────────────────────────

    /**
     * ★ 两个客户端各自预留的区间**绝不重叠**，且全局单调。
     *
     * 重叠的后果是两笔不同的事务拿到同一个 `commit_ts`：MVCC 的可见性依赖
     * "时间戳的大小顺序就是提交顺序"，重复的时间戳会让这个序失去全序，
     * 症状是偶尔读到不该读到的版本——**不报错**。
     */
    @Test
    fun `时间戳区间互不重叠且单调递增`() = runBlocking {
        for (rig in rigs()) {
            val name = rig.name

            val a = rig.open("cn-a")
            val b = rig.open("cn-b")
            try {
                val fromA = ArrayList<Long>()
                val fromB = ArrayList<Long>()
                // 交替取号：两个客户端的区间如果有重叠，交错取样几乎必然抓到
                repeat(30) {
                    fromA.add(a.tso.next().value)
                    fromB.add(b.tso.next().value)
                }
                assertTrue(fromA.zipWithNext().all { (x, y) -> y > x }, "[$name] cn-a 的时间戳必须严格递增")
                assertTrue(fromB.zipWithNext().all { (x, y) -> y > x }, "[$name] cn-b 的时间戳必须严格递增")

                // 全局不重复：把两边所有号放在一起，必须没有重复
                val all = fromA + fromB
                assertEquals(all.size, all.toSet().size, "[$name] 两个 CN 发出的时间戳出现了重复")
            } finally {
                rig.close()
            }
        }
    }

    /**
     * ★ 重启后**不回退**：新进程的号必须大于旧进程已经发出过的所有号。
     *
     * 回退的后果同样是静默的：一个 `commit_ts` 小于既有数据的时间戳，
     * 会让新写入"插入到历史中间"，而 `AS OF` 与 GC 水位都会因此判错。
     */
    @Test
    fun `重启后时间戳不回退`() = runBlocking {
        for (rig in rigs()) {
            val name = rig.name

            val first = rig.open("cn-restart")
            var maxIssued = 0L
            try {
                repeat(10) { maxIssued = maxOf(maxIssued, first.tso.next().value) }
            } finally {
            }
            val second = rig.open("cn-restart")
            try {
                second.tso.recover()
                val next = second.tso.next().value
                assertTrue(next > maxIssued, "[$name] 重启后的第一个时间戳($next)必须大于重启前发出过的最大值($maxIssued)")
            } finally {
                rig.close()
            }
        }
    }

    /** `peek()` 不推进水位，且 `commit_ts` 必须严格大于已经发出的读时间戳。 */
    @Test
    fun `读时间戳不推进水位且提交时间戳严格更大`() = runBlocking {
        for (rig in rigs()) {
            val name = rig.name

            val c = rig.open("cn-peek")
            try {
                val p1 = c.tso.peek().value
                val p2 = c.tso.peek().value
                // peek 反映的是**当前毫秒**，所以两次取值可以不同（时间是会走的）；
                // 它不该做的是**消耗**一个时间戳。那是下面这条断言要证明的事
                assertTrue(p2 >= p1, "[$name] peek 的结果不该回退")
                val commit = c.tso.next().value
                assertTrue(commit > p1, "[$name] 提交时间戳必须严格大于已发出的读时间戳（否则读者会读到之后才提交的写入）")
            } finally {
                rig.close()
            }
        }
    }

    // ── 契约 2：租约 ──────────────────────────────────────────────

    /**
     * ★ epoch **单调递增、永不重置**——这是 fencing 的第一道防线。
     *
     * `wal/e{epoch}/` 的对象前缀靠它区分写者。epoch 一旦被回收重用，
     * 一个旧写者当年写的对象会被新写者当成自己的日志重放进去：静默的数据损坏。
     */
    @Test
    fun `租约的 epoch 永不重置`() = runBlocking {
        val shard = ShardId(TableId(7), PartitionId(0))
        for (rig in rigs()) {
            val name = rig.name
            // 单进程模式**没有租约**：`SingleProcessLease` 的 epoch 恒为 1，
            // 因为它的前提就是"只有一个写者"，不需要用 epoch 区分写者
            if (name == "single") continue

            val a = rig.open("cn-a")
            try {
                val l1 = a.leases.acquire(shard)
                a.leases.release(l1)
                val l2 = a.leases.acquire(shard)
                assertTrue(l2.epoch > l1.epoch, "[$name] 重新获取的 epoch(${l2.epoch}) 必须大于上一次(${l1.epoch})")
                a.leases.release(l2)
                val l3 = a.leases.acquire(shard)
                assertTrue(l3.epoch > l2.epoch, "[$name] epoch 必须一直递增")
            } finally {
                rig.close()
            }
        }
    }

    /** 同一个分片**不能**同时有两个写者。 */
    @Test
    fun `同一分片同时只能有一个写者`() = runBlocking {
        val shard = ShardId(TableId(9), PartitionId(1))
        for (rig in rigs()) {
            val name = rig.name

            // 单进程模式**故意**允许多个持有者（它是一份"由部署保证"的假设），
            // 所以这条契约不适用于它——这正是它必须显式声明自己的适用场景的原因
            if (name == "single") continue
            val a = rig.open("cn-a")
            val b = rig.open("cn-b")
            try {
                val la = a.leases.acquire(shard)
                // b 抢同一个分片：必须失败（而不是"等着"或"也拿到"）
                val e = assertFailsWith<Exception>("[$name] 第二个写者必须抢不到租约") {
                    withTimeout(3_000) { b.leases.acquire(shard) }
                }
                assertNotNull(e.message, "[$name] 抢租约失败时要给出说明")
                a.leases.release(la)
                val lb = b.leases.acquire(shard)
                assertTrue(lb.epoch > la.epoch, "[$name] 换写者之后 epoch 必须更大（fencing 依赖它）")
            } finally {
                rig.close()
            }
        }
    }

    /** 已被罢免的写者**续期必须被拒绝**——接受它等于让旧写者继续写。 */
    @Test
    fun `已被取代的租约续期被拒绝`() = runBlocking {
        val shard = ShardId(TableId(11), PartitionId(0))
        for (rig in rigs()) {
            val name = rig.name

            if (name == "single") continue
            val a = rig.open("cn-a")
            val b = rig.open("cn-b")
            try {
                val la = a.leases.acquire(shard)
                a.leases.release(la)
                val lb = b.leases.acquire(shard)
                val failed = runCatching { a.leases.renew(la) }.isFailure
                assertTrue(failed, "[$name] 已被取代的租约续期必须失败（否则两个写者会同时成立）")
                assertTrue(lb.epoch > la.epoch)
            } finally {
                rig.close()
            }
        }
    }

    // ── 契约 3：锁 ────────────────────────────────────────────────

    /** ★ 排他锁真的互斥：第二个持有者在等待超时后必须拿到 1205。 */
    @Test
    fun `排他锁互斥`() = runBlocking {
        for (rig in rigs()) {
            val name = rig.name

            // 单进程模式没有锁（它假设没有第二个进程），跳过
            if (name == "single") continue
            val a = rig.open("cn-a")
            val b = rig.open("cn-b")
            val key = LockKey.row(TableId(3).value, byteArrayOf(1, 2, 3))
            try {
                val h = a.locks.lock(key, LockMode.EXCLUSIVE, "tx-a", waitMillis = 0)
                val e = assertFailsWith<SqlException>("[$name] 第二个持有者必须等到 1205") {
                    b.locks.lock(key, LockMode.EXCLUSIVE, "tx-b", waitMillis = 200)
                }
                assertEquals(1205, e.errorCode, "[$name] 锁等待超时必须报 1205（与 innodb_lock_wait_timeout 的语义一致）")
                a.locks.unlock(h)
                // 释放之后别人应当能拿到
                val h2 = b.locks.lock(key, LockMode.EXCLUSIVE, "tx-b", waitMillis = 1_000)
                b.locks.unlock(h2)
            } finally {
                rig.close()
            }
        }
    }

    /** 共享锁可以共存，但会挡住排他锁。 */
    @Test
    fun `共享锁共存且挡住排他锁`() = runBlocking {
        for (rig in rigs()) {
            val name = rig.name

            if (name == "single") continue
            val a = rig.open("cn-a")
            val b = rig.open("cn-b")
            val c = rig.open("cn-c")
            val key = LockKey.row(TableId(4).value, byteArrayOf(9))
            try {
                val h1 = a.locks.lock(key, LockMode.SHARED, "tx-a", waitMillis = 0)
                val h2 = b.locks.lock(key, LockMode.SHARED, "tx-b", waitMillis = 200)
                val e = assertFailsWith<SqlException>("[$name] 共享锁持有期间排他锁必须等待") {
                    c.locks.lock(key, LockMode.EXCLUSIVE, "tx-c", waitMillis = 200)
                }
                assertEquals(1205, e.errorCode, "[$name] 共享锁持有期间排他锁必须等待")
                a.locks.unlock(h1)
                b.locks.unlock(h2)
            } finally {
                rig.close()
            }
        }
    }

    /** 锁会随 TTL 过期——这是持有者崩溃后唯一的兜底。 */
    @Test
    fun `锁会随 TTL 过期`() = runBlocking {
        for (rig in rigs()) {
            val name = rig.name

            if (name == "single") continue
            val a = rig.open("cn-a")
            val b = rig.open("cn-b")
            val key = LockKey.row(TableId(5).value, byteArrayOf(7))
            try {
                a.locks.lock(key, LockMode.EXCLUSIVE, "tx-a", ttlMillis = 150, waitMillis = 0)
                // a 崩了（不再续期、也不释放）：b 必须能在 TTL 之后拿到
                delay(200)
                val h = b.locks.lock(key, LockMode.EXCLUSIVE, "tx-b", ttlMillis = 5_000, waitMillis = 2_000)
                b.locks.unlock(h)
            } finally {
                rig.close()
            }
        }
    }

    // ── Catalog 版本（§4.5 的失效判据）────────────────────────────

    /** 版本号只增不减：它不回退，CN 的 schema 缓存才不会永久陈旧。 */
    @Test
    fun `catalog 版本单调递增`() = runBlocking {
        for (rig in rigs()) {
            val name = rig.name

            val c = rig.open("cn-a")
            try {
                val v0 = c.catalogVersion()
                val v1 = c.bumpCatalogVersion()
                val v2 = c.bumpCatalogVersion()
                assertTrue(v1 > v0, "[$name] bump 之后版本必须变大")
                assertTrue(v2 > v1, "[$name] 每次 DDL 都要推进版本")
                assertEquals(v2, c.catalogVersion(), "[$name] 读到的版本必须是最新的")
            } finally {
                rig.close()
            }
        }
    }

}
