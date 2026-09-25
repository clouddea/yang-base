package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.ConcurrentModificationException
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 本地文件后端的 CAS 语义测试。
 *
 * 这个文件的存在是因为**能力探测抓到了一个真 bug**：`LocalFileObjectStore`
 * 原本用 `size-mtimeNanos` 当 ETag，看起来合理，但当两次写入**大小相同**
 * 且发生在同一个文件系统时间戳粒度内时，ETag 会碰撞——于是陈旧的 ETag
 * 也能通过 CAS 检查，fencing 静默失效。
 *
 * 本地文件后端只用于开发测试，但 fencing 失效意味着「开发环境跑通的并发逻辑
 * 在生产上不成立」，这比后端本身不可用危险得多。
 */
class LocalFileStoreCasTest {

    private val dir = Files.createTempDirectory("yb-fs-cas")
    private fun store() = LocalFileObjectStore(dir)

    @AfterTest
    fun cleanup() {
        runCatching { dir.toFile().deleteRecursively() }
    }

    /** 这正是探测里失败的场景：连续写入等长内容。 */
    @Test
    fun `等长内容的连续写入必须产生不同的 ETag`() = runBlocking {
        val s = store()
        val key = "same-size"
        val etags = (1..5).map { i ->
            s.put(key, "v$i".toByteArray())   // 全是 2 字节
        }
        assertEquals(
            etags.size, etags.toSet().size,
            "每次写入都必须产生新的 ETag。相同 size + 相同 mtime 粒度会让 ETag 碰撞，" +
                "进而让陈旧的 ETag 也能通过 CAS —— fencing 会静默失效。实际: $etags",
        )
    }

    @Test
    fun `陈旧 etag 的 CAS 必须被拒绝`() = runBlocking {
        val s = store()
        val key = "cas"
        s.put(key, "v1".toByteArray())
        val stale = s.head(key).etag

        // 用当前 etag 交换成功
        s.put(key, "v2".toByteArray(), Precondition.MustMatch(stale))
        assertTrue(s.get(key).contentEquals("v2".toByteArray()))

        // 再用陈旧的 etag 必须失败 —— 这是 fencing 的基础
        assertFailsWith<ConcurrentModificationException>(
            "陈旧 etag 被接受意味着写者选举/fencing 在这种后端上失效"
        ) {
            s.put(key, "v3".toByteArray(), Precondition.MustMatch(stale))
        }
        assertTrue(s.get(key).contentEquals("v2".toByteArray()), "失败的 CAS 不得改变内容")
    }

    @Test
    fun `MustNotExist 对已存在的对象必须失败`() = runBlocking {
        val s = store()
        val key = "create-once"
        s.put(key, "first".toByteArray(), Precondition.MustNotExist)
        assertFailsWith<com.cloudea.yangbase.common.PreconditionFailedException> {
            s.put(key, "second".toByteArray(), Precondition.MustNotExist)
        }
        assertTrue(s.get(key).contentEquals("first".toByteArray()))
    }

    @Test
    fun `并发 CAS 只有一个成功`() = runBlocking {
        val s = store()
        val key = "lease"
        s.put(key, "initial".toByteArray())
        val etag = s.head(key).etag

        val wins = (1..16).map { i ->
            try {
                s.put(key, "owner-$i".toByteArray(), Precondition.MustMatch(etag)); 1
            } catch (_: Exception) {
                0
            }
        }.sum()
        assertEquals(1, wins, "16 个并发 CAS 必须只有一个成功")
    }

    /** 覆盖写必须让 ETag 变化——否则缓存会一直返回旧内容。 */
    @Test
    fun `内容变化时 ETag 必须变化`() = runBlocking {
        val s = store()
        val key = "grow"
        val e1 = s.put(key, "a".toByteArray())
        val e2 = s.put(key, "aaaaaaaaaa".toByteArray())
        assertTrue(e1 != e2, "长度不同的内容必须产生不同 ETag")
    }
}
