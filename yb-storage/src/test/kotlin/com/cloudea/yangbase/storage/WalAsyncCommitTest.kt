package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.Epoch
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.objectstore.ByteRange
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.ListPage
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectMeta
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.ObjectStoreCapabilities
import com.cloudea.yangbase.objectstore.Precondition
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * `commit_mode=async` 的语义边界测试（issue #13）。
 *
 * 这个模式最危险的失败形态不是「慢」，而是**静默**：ACK 了却从没落盘、
 * 或者反过来「说是 async 其实还在等 PUT」。所以这里断言的全是**可观测的差别**：
 *
 *  1. ASYNC 下 PUT 还没完成，`submit` 必须已经返回，且 `durable == false`；
 *  2. SYNC 下同样的场景必须**不返回**（对照组，证明第 1 条不是「根本没在写」）；
 *  3. 落盘失败要重试，**不能把已 ACK 的写入扔掉**；
 *  4. `onCommitted` 仍然按 WAL 序号顺序执行（行数增量靠这个序才算得对）；
 *  5. 关闭时在途的批次要排干。
 *
 * 注意这里**不能用 `runTest`**：它把 `delay`/`withTimeout` 换成虚拟时间，
 * 而本测试要断言的恰恰是「真实时间过去了多久之后还没返回」。
 */
class WalAsyncCommitTest {

    private val shard = ShardId(TableId(2001), PartitionId(0))
    private val epoch = Epoch(7)

    private fun batch(txId: String, key: String = "k1", ts: Long = 100L) =
        WalBatch(txId, ts, listOf(WalOp(key.toByteArray(), "v".toByteArray())))

    private fun scope() = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private fun walKey(seq: Long) = ObjectKeys.walBatch(shard, epoch, seq)

    @Test
    fun `async 下 PUT 未完成时提交已返回，且标为未落盘`() = runBlocking {
        val inner = MemoryObjectStore()
        val store = GatedStore(inner)
        val s = scope()
        val writer = WalWriter(store, shard, epoch, startSeq = 1, mode = CommitMode.ASYNC, scope = s)
        val job = writer.start()

        val r = withTimeout(5_000) { writer.submit(batch("t-async")) }

        assertFalse(r.durable, "ASYNC 下返回值必须是 durable=false：它答应的是 RPO>0，不是已落盘")
        // 同一个时刻：对象还不在。这就是「ACK 早于落盘」的定义
        assertFalse(inner.exists(walKey(r.walSeq)), "ACK 时 WAL 对象不该已经存在（否则就是同步等待了）")

        store.release()
        withTimeout(5_000) { while (!inner.exists(walKey(r.walSeq))) delay(10) }
        assertEquals(r.walSeq, writer.durableThroughSeq, "放行后必须真的落盘，且 durableThroughSeq 跟上")

        writer.close()
        job.cancel()
        s.cancel()
    }

    @Test
    fun `sync 下 PUT 未完成时提交不返回（对照组）`() = runBlocking {
        val inner = MemoryObjectStore()
        val store = GatedStore(inner)
        val s = scope()
        val writer = WalWriter(store, shard, epoch, startSeq = 1, mode = CommitMode.SYNC, scope = s)
        val job = writer.start()

        // 1 秒够长：如果 SYNC 也「提前 ACK」，这里一定拿得到结果
        val r = withTimeoutOrNull(1_000) { writer.submit(batch("t-sync")) }
        assertNull(r, "SYNC 的契约是「返回即已持久化」，PUT 没完成时绝不能返回")

        store.release()
        withTimeout(5_000) { while (writer.durableThroughSeq < 1) delay(10) }

        writer.close()
        job.cancel()
        s.cancel()
    }

    @Test
    fun `落盘失败会重试，不丢掉已 ACK 的写入`() = runBlocking {
        val inner = MemoryObjectStore()
        val store = GatedStore(inner).apply {
            failPuts = 2
            release()
        }
        val s = scope()
        val writer = WalWriter(store, shard, epoch, startSeq = 1, mode = CommitMode.ASYNC, scope = s)
        val job = writer.start()

        val r = withTimeout(5_000) { writer.submit(batch("t-retry")) }
        // 前两次 PUT 抛异常，第 3 次才成功：客户端早已拿到成功，服务端必须自己扛住
        withTimeout(10_000) { while (!inner.exists(walKey(r.walSeq))) delay(20) }

        assertEquals(3, store.putAttempts, "应当在失败后重试直到落盘（不是第一次失败就放弃）")

        writer.close()
        job.cancel()
        s.cancel()
    }

    @Test
    fun `async 下 onCommitted 仍按 WAL 序号顺序执行`() = runBlocking {
        val inner = MemoryObjectStore()
        val store = GatedStore(inner).apply { release() }
        val s = scope()
        val applied = java.util.Collections.synchronizedList(ArrayList<Long>())
        val writer = WalWriter(
            store, shard, epoch, startSeq = 1, mode = CommitMode.ASYNC, scope = s,
            onCommitted = { batches -> batches.forEach { applied.add(it.seq) } },
        )
        val job = writer.start()

        for (i in 1..5) withTimeout(5_000) { writer.submit(batch("t$i", key = "k$i", ts = 100L + i)) }
        withTimeout(5_000) { while (applied.size < 5) delay(10) }

        assertEquals(applied.sorted(), applied.toList(), "应用顺序必须是 WAL 提交序（行数增量依赖它）")

        writer.close()
        job.cancel()
        s.cancel()
    }

    @Test
    fun `关闭时会把在途的批次排干`() = runBlocking {
        val inner = MemoryObjectStore()
        val store = GatedStore(inner).apply { release() }
        val s = scope()
        val writer = WalWriter(store, shard, epoch, startSeq = 1, mode = CommitMode.ASYNC, scope = s)
        val job = writer.start()

        repeat(3) { withTimeout(5_000) { writer.submit(batch("t$it", key = "k$it")) } }
        writer.close()
        withTimeout(10_000) { job.join() }

        val keys = inner.listAll(ObjectKeys.walPrefix(shard, epoch))
        assertEquals(3, keys.size, "关闭必须等在途 PUT 收尾，否则这 3 笔就白丢在内存里了")

        s.cancel()
    }

    /** 用测试控制 `put` 何时返回 / 是否失败。其余操作直接代理给内存后端。 */
    private class GatedStore(
        private val delegate: MemoryObjectStore,
    ) : ObjectStore {
        private val gate = CompletableDeferred<Unit>()
        var failPuts = 0
        var putAttempts = 0

        fun release() = gate.complete(Unit)

        override val caps: ObjectStoreCapabilities get() = delegate.caps

        override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag {
            putAttempts++
            gate.await()
            if (failPuts > 0) {
                failPuts--
                throw IllegalStateException("模拟一次连接超时")
            }
            return delegate.put(key, data, precondition)
        }

        override suspend fun get(key: String, range: ByteRange?): ByteArray = delegate.get(key, range)

        override suspend fun delete(key: String): Boolean = delegate.delete(key)

        override suspend fun list(prefix: String, pageToken: String?, limit: Int): ListPage =
            delegate.list(prefix, pageToken, limit)

        override suspend fun head(key: String): ObjectMeta = delegate.head(key)

        override suspend fun exists(key: String): Boolean = delegate.exists(key)
    }
}
