package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.CapabilityNotSupportedException
import com.cloudea.yangbase.objectstore.ByteRange
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.ListPage
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectMeta
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.ObjectStoreCapabilities
import com.cloudea.yangbase.objectstore.Precondition
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * **没有 ETag CAS 的后端上，时间戳水位怎么办**（阿里云 OSS 那一类）。
 *
 * 这是真实故障（2026-09-27）的回归用例：在 OSS 上启动 `--coord-mode single`，
 * 探测过了、服务也起来了，但**第一条语句**就炸在
 * `key=meta/tso/default.json` 上——因为 `single` 模式的 TSO 把水位持久化在对象存储上，
 * 续期时用 `If-Match`，而 OSS 没有 CAS。
 *
 * 这里把三件事分别钉住：
 *
 * 1. **默认必须拒绝**（P6：宁可拒绝服务，也不要悄悄失去互斥）——
 *    判据不能靠"`put` 会不会抛异常"来兜底：走弱化路径时压根不发 `If-Match`，
 *    于是没有任何一层会报错，表现就是"开关没开也照样降级"；
 * 2. **显式同意后可以降级**，且水位**只增不减**（降级的是原子性，不是单调性）；
 * 3. ★ **弱化路径必须能发现"另一个写者"**：回读校验失败时**拒绝发号**。
 *    这一条是降级能被接受的前提——它把"默默发出重复时间戳"（静默的 MVCC 事故）
 *    变成一次明确的失败。
 */
class NonAtomicTsoTest {

    /** 声明"没有 ETag CAS"的后端（OSS 预设）。 */
    private fun noCasStore() = MemoryObjectStore(ObjectStoreCapabilities.OSS)

    private val wmKey = "meta/tso/default.json"

    /** 模拟"另一个预留者在我们的 PUT 与回读之间插了一脚"。 */
    private class InterleavingWriter(
        private val delegate: ObjectStore,
        private val key: String,
        private val other: ByteArray,
    ) : ObjectStore {
        override val caps get() = delegate.caps
        override suspend fun get(k: String, range: ByteRange?) = delegate.get(k, range)
        override suspend fun put(k: String, data: ByteArray, precondition: Precondition?): ETag {
            val etag = delegate.put(k, data, precondition)
            // 只对水位对象插这一脚，避免影响其它键
            if (k == key) delegate.put(k, other, null)
            return etag
        }

        override suspend fun delete(k: String) = delegate.delete(k)
        override suspend fun list(prefix: String, pageToken: String?, limit: Int) =
            delegate.list(prefix, pageToken, limit)

        override suspend fun head(k: String): ObjectMeta = delegate.head(k)
    }

    @Test
    fun `没有 CAS 又没有显式同意时必须拒绝而不是悄悄降级`() = runBlocking {
        val store = noCasStore()
        // 水位对象已经存在 = 桶被用过（或上一次运行留下的）——这正是真实故障的触发条件
        store.put(wmKey, """{"watermark":1000}""".toByteArray())

        val e = assertFailsWith<CapabilityNotSupportedException> {
            PersistentTso(store, "default", batchMillis = 0, allowNonAtomicCas = false).next()
        }
        assertTrue(
            e.message!!.contains("i-accept-nonatomic-pointer-switch"),
            "拒绝信息要给出补救动作（开关名或改用 ms），实际：${e.message}",
        )
        assertEquals(
            """{"watermark":1000}""", String(store.get(wmKey)),
            "被拒绝时**不能**已经把水位写下去（那正是「悄悄降级」）",
        )
    }

    @Test
    fun `显式同意后水位可以续期且只增不减`() = runBlocking {
        val store = noCasStore()
        store.put(wmKey, """{"watermark":1000}""".toByteArray())

        val tso = PersistentTso(store, "default", batchMillis = 0, allowNonAtomicCas = true)
        val ts = tso.next().value

        val wm = Regex(""""watermark"\s*:\s*(\d+)""").find(String(store.get(wmKey)))!!.groupValues[1].toLong()
        assertTrue(wm > 1000, "水位只能往前推，实际 $wm")
        assertTrue(ts > 1000, "发出的时间戳必须大于历史水位，实际 $ts")
    }

    @Test
    fun `弱化路径必须能发现另一个写者并拒绝发号`() = runBlocking {
        val backing = noCasStore()
        backing.put(wmKey, """{"watermark":1000}""".toByteArray())
        // 我们写完之后，别人立刻把水位覆盖成他自己的值
        val store = InterleavingWriter(backing, wmKey, """{"watermark":999999}""".toByteArray())

        val e = assertFailsWith<Exception> {
            PersistentTso(store, "default", batchMillis = 0, allowNonAtomicCas = true).next()
        }
        // 关键：**不能**带着一个可能重叠的时间戳区间继续跑
        assertTrue(
            e.message!!.contains("另一个写者") || e.message!!.contains("覆盖"),
            "要报出「水位被别人覆盖」，而不是把这一次当成正常预留继续发号。实际：${e.message}",
        )
    }
}
