package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Epoch
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.storage.Manifest
import com.cloudea.yangbase.storage.ManifestStore
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * **GC 的视野必须覆盖 WAL 的每一种对象形态。**
 *
 * WAL 前缀下有两种键：普通批次 `{seq}.log` 与**段对象** `s-{from}-{to}.log`
 * （`LsmStore.compactWalSegments` 把已 flush 的批次合并成段，见 §6.3.2）。
 *
 * 这里曾经是一个**不报错、只是慢慢变贵**的缺陷：GC 自己写了一份序号解析
 * （`substringAfterLast('/').substringBefore('.')`），它对普通对象成立，
 * 对段对象返回 null，而调用点是 `?: continue`——于是段对象**既不算活、也不算垃圾**，
 * 永远留在桶里。在一个以"消解固定存储成本"为存在理由的系统里，
 * 这正是最不该有的失效模式。
 *
 * 断言的方式是**直接检查标记结果**，而不是"跑一次 GC 没报错"——
 * 后者对这个缺陷完全无感。
 */
class GcWalCoverageTest {

    private val table = TableSchema(
        tableId = 7,
        databaseId = 1,
        name = "t",
        version = 1,
        columns = listOf(ColumnDef("id", ColumnType.BIGINT, nullable = false)),
        primaryKey = listOf("id"),
    )
    private val shard = ShardId(TableId(7), PartitionId(0))

    /** 安全窗口设成负数：所有对象都"足够老"，测试关心的不是时间而是**视野**。 */
    private fun gc(store: MemoryObjectStore) = GarbageCollector(store, safetyWindowMillis = -1_000)

    private suspend fun storeWithManifest(flushedThrough: Long): MemoryObjectStore {
        val store = MemoryObjectStore()
        val m = Manifest.empty(Epoch(1), "n1").copy(
            walEpoch = 1,
            walFlushedThroughSeq = flushedThrough,
        )
        ManifestStore(store).commit(shard, m, null)
        return store
    }

    @Test
    fun `已 flush 的段对象必须进入 GC 视野`() = runBlocking {
        val store = storeWithManifest(flushedThrough = 128)
        val seg = ObjectKeys.walSegment(shard, Epoch(1), 1, 128)
        store.put(seg, ByteArray(8))

        val r = gc(store).mark(table, shard)
        assertTrue(seg in r.garbage, "段对象应当被标记为可回收，实际 garbage=${r.garbage}")
    }

    @Test
    fun `尚未 flush 的段对象必须留下`() = runBlocking {
        val store = storeWithManifest(flushedThrough = 40)
        // 段覆盖 1..128，而 manifest 只 flush 到 40：它整体**不能**删
        val seg = ObjectKeys.walSegment(shard, Epoch(1), 1, 128)
        store.put(seg, ByteArray(8))

        val r = gc(store).mark(table, shard)
        assertTrue(seg !in r.garbage, "未 flush 的段绝不能删，实际 garbage=${r.garbage}")
    }

    @Test
    fun `普通 WAL 批次与段对象一视同仁`() = runBlocking {
        val store = storeWithManifest(flushedThrough = 128)
        val batch = ObjectKeys.walBatch(shard, Epoch(1), 50)
        val seg = ObjectKeys.walSegment(shard, Epoch(1), 1, 40)
        store.put(batch, ByteArray(8))
        store.put(seg, ByteArray(8))

        val r = gc(store).mark(table, shard)
        assertTrue(batch in r.garbage, "普通批次应当被回收")
        assertTrue(seg in r.garbage, "段对象应当被回收")
    }

    /**
     * `.p` 前缀是**未决 WAL**（写入过程中先落 pending 对象再改名，§6.3.4）。
     * 它可能属于正在进行的提交，因此解析不出序号时必须**保守地留下**，
     * 而不是"解析不出来就当垃圾"。
     */
    @Test
    fun `未决 WAL 对象必须保守保留`() = runBlocking {
        val store = storeWithManifest(flushedThrough = 128)
        val pending = ObjectKeys.walPrefix(shard, Epoch(1)) + ".p0000000000000000128-abc.log"
        store.put(pending, ByteArray(8))

        val r = gc(store).mark(table, shard)
        assertTrue(pending !in r.garbage && pending !in r.fencedWal, "未决对象不能被回收")
    }
}
