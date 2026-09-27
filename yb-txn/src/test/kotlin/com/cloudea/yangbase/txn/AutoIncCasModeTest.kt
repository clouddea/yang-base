package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.CapabilityNotSupportedException
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStoreCapabilities
import com.cloudea.yangbase.storage.SingleProcessLease
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * **自增段水位的降级只在单写者部署下成立**（2026-09-27，评审发现）。
 *
 * 起因是这样一条路径：`--i-accept-nonatomic-pointer-switch` 是**部署级**开关
 * （OSS 上必须开，因为 manifest 指针切换没有 CAS），它被一路传到了
 * `AutoIncAllocator`。但在 `ms` 模式下，自增领段的互斥**完全依赖**这一次 CAS——
 * 它没有走 MS（实现是 §4.7 的"对象存储协调"那一档）。于是两个 CN 会：
 *
 * ```
 * CN-A 读水位 1000 ─┐
 * CN-B 读水位 1000 ─┴─→ 各自写 2000 ─→ 各自回读 ─→ 都读到 2000 ✓（两边写的是同一个值）
 *                                     ↓
 *                        各自发出 1001 → 重复主键 → 后写的行静默覆盖先写的
 * ```
 *
 * 也就是说：**回读校验在这里天然失效**（它只能发现"值不一样"），
 * 而降级把原来的"明确失败"变成了"静默重复发号"。所以判据不能是"用户开了开关"，
 * 必须是"**部署上确实只有一个写者**"。
 *
 * 这两条用例一起看才有意义：第一条守"ms 下不许降级"，第二条守"single 下仍然可以"
 * ——只加前者会把单 CN + OSS 这个已经跑通的组合一并打死。
 */
class AutoIncCasModeTest {

    private fun noCasStore() = MemoryObjectStore(ObjectStoreCapabilities.OSS)

    /** 只借 `kind` 一个字段的协调实现：**不自增不走它**，它只用来表达"这是多 CN 部署"。 */
    private class CoordinationOf(override val kind: String) : CoordinationService {
        override val tso: Tso = MonotonicTso()
        override val leases = SingleProcessLease
        override val locks: LockTable = LocalLockTable()
        override suspend fun catalogVersion(): Long = 0
        override suspend fun bumpCatalogVersion(): Long = 0
    }

    private fun engine(mode: String, allowNonAtomic: Boolean): DatabaseEngine =
        DatabaseEngine(
            noCasStore(),
            CoroutineScope(SupervisorJob() + Dispatchers.Default),
            nodeId = "t",
            coordination = CoordinationOf(mode),
            allowNonAtomicPointerSwitch = allowNonAtomic,
        )

    /** 让领段走 `If-Match` 那条路：水位对象**已经存在**（重启过 / 桶被用过）。 */
    private suspend fun seedWatermark(engine: DatabaseEngine, tableId: Long) {
        engine.store.put(ObjectKeys.autoIncWatermark(TableId(tableId)), "1000".toByteArray())
    }

    @Test
    fun `ms 模式下即使开了开关也不许降级领段`() = runBlocking {
        val engine = engine("ms", allowNonAtomic = true)
        seedWatermark(engine, 1001)

        val e = assertFailsWith<CapabilityNotSupportedException> {
            engine.autoIncrement.nextId(1001) { 0L }
        }
        assertTrue(
            e.message!!.contains("多个 CN") || e.message!!.contains("coord-mode ms"),
            "拒绝信息必须说清**为什么这个开关在这里不适用**（多 CN），" +
                "否则使用者只会反复检查自己有没有传开关。实际：${e.message}",
        )
    }

    @Test
    fun `single 模式下同样的后端可以降级领段`() = runBlocking {
        val engine = engine("single", allowNonAtomic = true)
        seedWatermark(engine, 1002)

        // 单写者 ⇒ 这个降级是成立的（水位从 1000 往前推）
        val id = engine.autoIncrement.nextId(1002) { 0L }
        assertTrue(id > 1000, "领到的 id 必须大于原水位，实际 $id")
        assertEquals("single", engine.coord.kind)
    }

    @Test
    fun `没开开关时两种模式都拒绝`() = runBlocking {
        for (mode in listOf("single", "ms")) {
            val engine = engine(mode, allowNonAtomic = false)
            seedWatermark(engine, 1003)
            assertFailsWith<CapabilityNotSupportedException>("$mode 下没开开关不得降级") {
                engine.autoIncrement.nextId(1003) { 0L }
            }
        }
    }
}
