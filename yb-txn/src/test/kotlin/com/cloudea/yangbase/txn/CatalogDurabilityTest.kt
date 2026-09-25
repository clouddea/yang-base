package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Catalog 的持久化语义：**删掉的东西不能在重载之后回来**。
 *
 * `DROP TABLE` 在这个系统里是"写墓碑 + 保留 schema 与 CURRENT 指针"——
 * 保留是为了 30 天内能 O(1) 恢复（§12.3）。于是 `load()` **必须**看墓碑：
 * 不看的话，一次重启、或者一次目录重载（多 CN 下的常规动作），
 * 被删的表就会连同数据一起回来，而使用者以为自己删掉了。
 */
class CatalogDurabilityTest {

    private fun catalog(store: MemoryObjectStore) = Catalog(store)

    @Test
    fun `删表之后重载目录不会把表带回来`() = runBlocking {
        val store = MemoryObjectStore()
        val c = catalog(store)
        val db = c.createDatabase("d", ifNotExists = true)
        c.createTable(
            db.databaseId, "t", listOf(ColumnDef("id", ColumnType.BIGINT, false)), listOf("id"),
        )
        assertTrue(c.findTable(db.databaseId, "t") != null)

        c.dropTable(db.databaseId, "t")
        assertNull(c.findTable(db.databaseId, "t"), "删掉之后内存里就该没有了")

        // 重载（等价于重启一个 CN，或多 CN 下另一个 CN 首次加载目录）
        val reloaded = catalog(store)
        reloaded.load()
        assertNull(
            reloaded.findTable(db.databaseId, "t"),
            "重载之后表又回来了——墓碑没被检查（DROP 故意保留了 schema 与 CURRENT 指针）",
        )
    }

    /** 恢复（30 天内的 `RESTORE`）之后表要能正常回来。 */
    @Test
    fun `恢复之后的表在重载时仍然存在`() = runBlocking {
        val store = MemoryObjectStore()
        val c = catalog(store)
        val db = c.createDatabase("d2", ifNotExists = true)
        c.createTable(db.databaseId, "t2", listOf(ColumnDef("id", ColumnType.BIGINT, false)), listOf("id"))
        c.dropTable(db.databaseId, "t2")
        c.restoreTable(db.databaseId, "t2")

        val reloaded = catalog(store)
        reloaded.load()
        assertEquals(
            "t2",
            reloaded.findTable(db.databaseId, "t2")?.name,
            "恢复过的表必须能重载出来（恢复要清掉墓碑）",
        )
    }
}
