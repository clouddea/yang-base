package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.PreconditionFailedException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 对象存储抽象的行为测试。
 *
 * 这里最重要的是**条件写语义**——整套系统的正确性建立在它之上
 * （写者选举、manifest 指针切换、fencing）。条件写若被静默忽略，
 * 系统会在并发下损坏数据却不报任何错。
 */
class MemoryObjectStoreTest {

    private fun store() = MemoryObjectStore()

    @Test
    fun `读写往返`() = runBlocking {
        val s = store()
        s.put("k1", "hello".toByteArray())
        assertContentEquals("hello".toByteArray(), s.get("k1"))
    }

    @Test
    fun `不存在的对象抛 ObjectNotFound`() = runBlocking {
        assertFailsWith<ObjectNotFoundException> { store().get("nope") }
    }

    @Test
    fun `HEAD 返回元信息`() = runBlocking {
        val s = store()
        s.put("k", ByteArray(42))
        val m = s.head("k")
        assertEquals(42L, m.size)
        assertTrue(m.etag.value.isNotEmpty(), "ETag 不能为空")
    }

    @Test
    fun `覆盖写会改变 ETag`() = runBlocking {
        val s = store()
        val e1 = s.put("k", "a".toByteArray())
        val e2 = s.put("k", "b".toByteArray())
        assertTrue(e1 != e2, "覆盖写必须产生新的 ETag，否则 CAS 形同虚设")
    }

    // ── 条件写：整套系统的正确性基础 ───────────────────────────────

    @Test
    fun `MustNotExist 只在对象不存在时成功`() = runBlocking {
        val s = store()
        s.put("k", "first".toByteArray(), Precondition.MustNotExist)
        assertFailsWith<PreconditionFailedException> {
            s.put("k", "second".toByteArray(), Precondition.MustNotExist)
        }
        assertContentEquals("first".toByteArray(), s.get("k"), "失败的写入不得改变内容")
    }

    @Test
    fun `MustMatch 是真正的比较交换`() = runBlocking {
        val s = store()
        val e1 = s.put("k", "v1".toByteArray())

        // 用当前 ETag 交换成功
        val e2 = s.put("k", "v2".toByteArray(), Precondition.MustMatch(e1))
        assertTrue(e2 != e1)

        // 再用陈旧的 ETag 交换必须失败 —— 这就是 fencing
        assertFailsWith<ConcurrentModificationException> {
            s.put("k", "v3".toByteArray(), Precondition.MustMatch(e1))
        }
        assertContentEquals("v2".toByteArray(), s.get("k"))
    }

    @Test
    fun `MustMatch 对不存在的对象失败`() = runBlocking {
        assertFailsWith<ObjectNotFoundException> {
            store().put("k", "v".toByteArray(), Precondition.MustMatch(ETag("whatever")))
        }
    }

    @Test
    fun `并发 CAS 只有一个成功 - 写者选举的基础`() = runBlocking {
        val s = store()
        s.put("lease", "initial".toByteArray())
        val etag = s.head("lease").etag

        val results = coroutineScope {
            (1..16).map { i ->
                async {
                    try {
                        s.put("lease", "owner-$i".toByteArray(), Precondition.MustMatch(etag))
                        true
                    } catch (_: Exception) {
                        false
                    }
                }
            }.map { it.await() }
        }
        assertEquals(1, results.count { it }, "16 个并发 CAS 必须只有一个成功")
    }

    // ── LIST ──────────────────────────────────────────────────────

    @Test
    fun `LIST 按字典序返回`() = runBlocking {
        val s = store()
        for (k in listOf("d/10", "d/9", "d/1", "d/2")) s.put(k, ByteArray(0))
        val keys = s.listAll("d/")
        assertEquals(listOf("d/1", "d/10", "d/2", "d/9"), keys, "LIST 必须是字节字典序")
    }

    @Test
    fun `LIST 分页`() = runBlocking {
        val s = store()
        for (i in 1..25) s.put("p/${"%03d".format(i)}", ByteArray(0))
        var token: String? = null
        val collected = ArrayList<String>()
        do {
            val page = s.list("p/", token, limit = 7)
            collected.addAll(page.keys)
            token = page.nextToken
        } while (token != null)
        assertEquals(25, collected.size)
        assertEquals(collected.sorted(), collected)
    }

    @Test
    fun `LIST 前缀过滤`() = runBlocking {
        val s = store()
        s.put("a/1", ByteArray(0)); s.put("b/1", ByteArray(0))
        assertEquals(listOf("a/1"), s.listAll("a/"))
        assertEquals(2, s.listAll("").size)
    }

    // ── Range ─────────────────────────────────────────────────────

    @Test
    fun `Range 读取精确的字节区间`() = runBlocking {
        val s = store()
        val data = ByteArray(1000) { (it % 256).toByte() }
        s.put("k", data)
        val slice = s.get("k", ByteRange(100, 199))
        assertEquals(100, slice.size)
        for (i in 0 until 100) assertEquals(data[100 + i], slice[i])
    }

    @Test
    fun `Range 超出末尾时截断而不报错`() = runBlocking {
        val s = store()
        s.put("k", ByteArray(10))
        assertEquals(10, s.get("k", ByteRange(0, 999)).size)
        assertEquals(0, s.get("k", ByteRange(100, 200)).size)
    }

    @Test
    fun `非法的 Range 被拒绝`() {
        assertFailsWith<IllegalArgumentException> { ByteRange(10, 5) }
        assertFailsWith<IllegalArgumentException> { ByteRange(-1, 5) }
    }


    @Test
    fun `删除是幂等的`() = runBlocking {
        val s = store()
        s.put("k", ByteArray(0))
        assertTrue(s.delete("k"))
        assertFalse(s.delete("k"), "重复删除应返回 false 而不是报错")
    }

    @Test
    fun `exists 不抛异常`() = runBlocking {
        val s = store()
        s.put("k", ByteArray(0))
        assertTrue(s.exists("k"))
        assertFalse(s.exists("nope"))
    }
}

class CountingObjectStoreTest {

    @Test
    fun `计数各类操作`() = runBlocking {
        val c = CountingObjectStore(MemoryObjectStore())
        c.put("k", ByteArray(10))
        c.put("k2", ByteArray(5), Precondition.MustNotExist)
        c.get("k")
        c.get("k", ByteRange(0, 3))
        c.head("k")
        c.list("")

        assertEquals(1, c.count("put"))
        assertEquals(1, c.count("put_cond"))
        assertEquals(1, c.count("get"))
        assertEquals(1, c.count("get_range"))
        assertEquals(1, c.count("head"))
        assertEquals(1, c.count("list"))
        assertEquals(6, c.totalOps())
    }

    @Test
    fun `reset 清零`() = runBlocking {
        val c = CountingObjectStore(MemoryObjectStore())
        c.put("k", ByteArray(1))
        c.reset()
        assertEquals(0, c.totalOps())
    }

    @Test
    fun `failOnList 把热路径上的 LIST 变成硬失败`() = runBlocking {
        val c = CountingObjectStore(MemoryObjectStore())
        c.failOnList = true
        assertFailsWith<AssertionError> { c.list("") }
    }

    @Test
    fun `failOnPut 用于断言只读路径不产生写`() = runBlocking {
        val c = CountingObjectStore(MemoryObjectStore())
        c.failOnPut = true
        assertFailsWith<AssertionError> { c.put("k", ByteArray(0)) }
    }

    @Test
    fun `exists 走 head 而不是绕过计数`() = runBlocking {
        val backing = MemoryObjectStore()
        val c = CountingObjectStore(backing)
        backing.put("k", ByteArray(0))
        c.reset()
        c.exists("k")
        assertEquals(1, c.count("head"), "exists 必须被计数，否则预算断言会失真")
    }

    @Test
    fun `report 人类可读`() = runBlocking {
        val c = CountingObjectStore(MemoryObjectStore())
        c.put("k", ByteArray(3))
        assertTrue(c.report().contains("put=1"))
    }
}

class ObjectKeysTest {

    @Test
    fun `序号零填充 - 保证字典序等于时间序`() {
        // 不做零填充的话 "10" 会排在 "9" 之前，恢复时日志会乱序
        val a = ObjectKeys.walBatch(com.cloudea.yangbase.common.ShardId(
            com.cloudea.yangbase.common.TableId(1), com.cloudea.yangbase.common.PartitionId(0)
        ), com.cloudea.yangbase.common.Epoch(1), 9)
        val b = ObjectKeys.walBatch(com.cloudea.yangbase.common.ShardId(
            com.cloudea.yangbase.common.TableId(1), com.cloudea.yangbase.common.PartitionId(0)
        ), com.cloudea.yangbase.common.Epoch(1), 10)
        assertTrue(a < b, "字典序必须等于序号序：$a 应小于 $b")
    }

    @Test
    fun `WAL 键包含 epoch - fencing 的第一道防线`() {
        val shard = com.cloudea.yangbase.common.ShardId(
            com.cloudea.yangbase.common.TableId(1), com.cloudea.yangbase.common.PartitionId(0)
        )
        val e1 = ObjectKeys.walBatch(shard, com.cloudea.yangbase.common.Epoch(1), 5)
        val e2 = ObjectKeys.walBatch(shard, com.cloudea.yangbase.common.Epoch(2), 5)
        assertTrue(e1 != e2)
        assertTrue(e1.contains("e1"), "epoch 必须在键里：$e1")
        assertTrue(e2.contains("e2"))
    }

    @Test
    fun `索引与数据分离成两个对象 - 点查性能的前提`() {
        val shard = com.cloudea.yangbase.common.ShardId(
            com.cloudea.yangbase.common.TableId(1), com.cloudea.yangbase.common.PartitionId(0)
        )
        val fid = com.cloudea.yangbase.common.FileId("abc")
        val dat = ObjectKeys.sstData(shard, 0, fid)
        val idx = ObjectKeys.sstIndex(shard, 0, fid)
        assertTrue(dat.endsWith(".dat"))
        assertTrue(idx.endsWith(".idx"))
        assertTrue(dat != idx, "数据块与索引必须是不同的对象，否则点查要下载整个文件")
    }

    @Test
    fun `超宽序号被拒绝而不是静默截断`() {
        assertFailsWith<IllegalArgumentException> { ObjectKeys.pad(9999999999999L, 12) }
    }
}
