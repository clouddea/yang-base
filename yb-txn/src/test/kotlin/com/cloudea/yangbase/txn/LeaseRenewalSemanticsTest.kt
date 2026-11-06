package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.Precondition
import kotlinx.coroutines.runBlocking
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 续租的两件“看不清”的事：**响应丢了** vs **真的被接管**。
 *
 * 这两件事在客户端看来是同一个现象（一次抛异常），但正确的处置完全相反：
 *
 *  - 响应丢了（PUT 已生效）：我们**仍是写者**，只是本地记的 etag 过期了。
 *    如果按“失败”处理，这个分片会在本节点上停写直到 TTL 过期，而且下一次
 *    acquire 还会报「被自己持有」——一个连运维都读不懂的错误。
 *  - 真的被接管：必须立刻停写，否则旧写者写下的对象会被下一个恢复者重放。
 *
 * 区分手段是租约对象里的 `instance`：`holder` 只能说明“同一台节点”
 * （`--node-id` 是部署时给的，可以重复），`instance` 才说明“同一个进程”。
 */
class LeaseRenewalSemanticsTest {

    private val shard = ShardId(TableId(2001), PartitionId(0))

    /**
     * 一个会在**PUT 生效之后**抛 IO 异常的存储：模拟「请求到达了、对象改了、响应丢了」。
     *
     * 这是最容易写错的那种失败——从调用方看是一次异常，而对象其实已经变了。
     */
    private class LostResponseStore(
        private val inner: ObjectStore,
        var loseNextPutResponse: Boolean = false,
    ) : ObjectStore by inner {
        override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag {
            val etag = inner.put(key, data, precondition)
            if (loseNextPutResponse) {
                loseNextPutResponse = false
                throw IOException("模拟：PUT 已生效，但响应在网络上丢了")
            }
            return etag
        }
    }

    @Test
    fun `响应丢了的续租不会被误判成被接管`() = runBlocking<Unit> {
        val store = LostResponseStore(MemoryObjectStore())
        val a = LeaseManager(store, holder = "cn-a", ttlMillis = 5_000)
        val lease = a.acquire(shard)

        // 续期：PUT 生效了，但调用方收到的是 IO 异常
        store.loseNextPutResponse = true
        assertFailsWith<IOException>("这一轮续期必须表现为失败（响应丢了）") { a.renew(lease) }

        // ★ 关键：confirm 必须认出“还是我”，并给出**刷新的凭据**
        val confirmed = assertNotNull(
            a.confirm(lease),
            "租约对象上的持有者仍是本实例，confirm 必须返回租约而不是 null——" +
                "否则一个合法的写者会把自己误杀（旧代码就是这样：停写 + 下次 acquire 报「被自己持有」）",
        )
        assertEquals(lease.epoch, confirmed.epoch, "确认不等于换人：epoch 必须不变")

        // 用刷新后的凭据续期必须成功（说明 etag 真的被采纳了，不是拿旧 etag 硬撞）
        val renewed = a.renew(confirmed)
        assertEquals(lease.epoch, renewed.epoch)
    }

    @Test
    fun `真的被接管时确认返回空`() = runBlocking<Unit> {
        val store = LostResponseStore(MemoryObjectStore())
        val ttl = 300L
        val a = LeaseManager(store, holder = "cn-a", ttlMillis = ttl)
        val lease = a.acquire(shard)

        // 让 a 的租约自然过期，b 接管（epoch +1）
        kotlinx.coroutines.delay(ttl + 50)
        val b = LeaseManager(store, holder = "cn-b", ttlMillis = ttl)
        val leaseB = b.acquire(shard)
        assertTrue(leaseB.epoch > lease.epoch, "接管必须拿到更大的 epoch")

        assertNull(a.confirm(lease), "换了人就必须认输：confirm 返回 null")
        // 用旧凭据续期也必须失败（etag 已变）
        assertFailsWith<com.cloudea.yangbase.common.LeaseLostException> { a.renew(lease) }
    }

    @Test
    fun `同一个 node-id 的两个进程不能互相认成自己`() = runBlocking<Unit> {
        val store = LostResponseStore(MemoryObjectStore())
        // 两个 CN 被误配成同一个 --node-id：holder 相同，instance 不同
        val a = LeaseManager(store, holder = "cn-1", ttlMillis = 60_000)
        val b = LeaseManager(store, holder = "cn-1", ttlMillis = 60_000)
        val leaseA = a.acquire(shard)

        val e = assertFailsWith<SqlException>(
            "同名的另一个进程必须被挡住——否则租约防的就是“两个写者”，而这里正好造出两个",
        ) { b.acquire(shard) }
        assertEquals(1205, e.errorCode, "分片写权冲突必须报 1205（可重试）")

        assertNull(b.confirm(leaseA), "另一个实例不能把别人的租约确认成自己的")
    }

    @Test
    fun `同一实例重新取租约会采纳自己而不是报冲突`() = runBlocking<Unit> {
        val store = LostResponseStore(MemoryObjectStore())
        val a = LeaseManager(store, holder = "cn-a", ttlMillis = 60_000)
        val first = a.acquire(shard) as LeaseManager.Lease

        // 续租失败后 leaseLost，下一次写会重新 acquire：对象上的持有者就是自己
        val again = a.acquire(shard) as LeaseManager.Lease
        assertEquals(first.epoch, again.epoch, "自己接自己的班不该把 epoch 推高（那会白扔一个 WAL 前缀）")
        assertEquals(first.instance, again.instance)
    }

    @Test
    fun `释放之后 holder 清空但 epoch 保留`() = runBlocking<Unit> {
        val store = MemoryObjectStore()
        val a = LeaseManager(store, holder = "cn-a", ttlMillis = 60_000)
        val lease = a.acquire(shard)
        a.release(lease)

        val snap = assertNotNull(a.peek(shard), "释放不删对象——epoch 必须留着继续 +1")
        assertEquals("", snap.holder)
        assertEquals(lease.epoch, snap.epoch, "epoch 永不重置：回收它会让旧写者的日志被当成新写者的")
        assertNull(a.confirm(lease), "已释放的租约不能再确认成自己的")
    }
}
