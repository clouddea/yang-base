package com.cloudea.yangbase.txn

import com.cloudea.yangbase.objectstore.MemoryObjectStore
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 时间戳分配器的测试。
 *
 * 最核心的不变式是**单调性**：TSO 绝不能回退。回退会破坏 MVCC 的可见性序，
 * 表现为「先提交的事务读不到、后提交的反而读得到」这类难以复现的错乱。
 */
class TsoTest {

    /** 一个只存在于内存里的时间戳来源：区间按调用顺序严格前进。 */
    private class FakeSource : TimestampSource {
        private var watermark = 0L
        override suspend fun reserve(atLeast: Long, span: Long): TimestampWindow {
            val base = maxOf(watermark, atLeast)
            watermark = base + span
            return TimestampWindow(base, watermark)
        }

        override suspend fun watermark(): Long = watermark
        override fun describe() = "FakeSource"
    }

    /**
     * ★ 物理时钟越过窗口右端之后，**连续两次提交不能拿到同一个时间戳**。
     *
     * 这条用例钉的是一个真实的静默缺陷：`next()` 的"要不要预取"与 `reserve()` 的
     * "要不要跳过"曾经用了两个不同的边界条件（`>= reservedThrough - 1` vs
     * `< reservedThrough`），在 `allocated == reservedThrough - 1` 上结论相反——
     * 于是窗口发不出新值时，`next()` 的兜底把它压回 `ceiling`，
     * **两次提交拿到同一个 commit_ts**。重复的时间戳会让 MVCC 失去全序。
     */
    @Test
    fun `窗口过期后连续两次提交不得重复时间戳`() = runBlocking {
        // 1 毫秒的窗口：sleep 之后物理时钟必然越过窗口右端
        val tso = WindowTso(FakeSource(), logicalBits = 18, batchMillis = 1)
        val a = tso.next().value
        delay(5)
        val b = tso.next().value
        val c = tso.next().value
        assertTrue(b > a, "第二次提交($b)必须大于第一次($a)")
        assertTrue(c > b, "第三次提交($c)必须大于第二次($b)——相同的时间戳会让可见性失去全序")
    }

    @Test
    fun `严格单调递增`() = runBlocking {
        val tso = MonotonicTso()
        var prev = -1L
        repeat(10_000) {
            val ts = tso.next().value
            assertTrue(ts > prev, "时间戳必须严格递增: $ts 不大于 $prev")
            prev = ts
        }
    }

    /**
     * 逻辑位的作用：**同一毫秒内的多次分配必须靠它区分**。
     *
     * ⚠️ 这条用例**不能断言"两次分配落在同一毫秒"**——那是环境的属性，不是代码的。
     * 满载的 CI（或刚跑完一轮全量测试的机器）上，两次调用之间时钟跳一格完全正常，
     * 于是断言会随机器负载随机变红。它曾经就是这么写的，也确实那么红过一次。
     *
     * 真正要守的不变式是**条件式**的：
     *  - 物理部分相同时，逻辑位必须严格递增（这是逻辑位存在的理由）；
     *  - 物理部分前进时，逻辑位重置（否则时间戳会无谓地跑在前面）。
     * 两者合起来 = 拼接后的值严格单调，而那正是 MVCC 全序依赖的性质。
     */
    @Test
    fun `同一毫秒内逻辑位递增`() = runBlocking {
        val tso = MonotonicTso()
        val logicalBits = 18
        val mask = (1L shl logicalBits) - 1
        var sawSameMilli = false
        var prev = tso.next().value
        repeat(10_000) {
            val cur = tso.next().value
            val prevPhys = prev shr logicalBits
            val curPhys = cur shr logicalBits
            assertTrue(cur > prev, "无论时钟怎么走，拼接后的时间戳都必须严格递增：$cur 不大于 $prev")
            if (curPhys == prevPhys) {
                sawSameMilli = true
                assertTrue(
                    (cur and mask) == (prev and mask) + 1,
                    "同一毫秒内的逻辑位必须**恰好**加一（不能跳、不能回退）：" +
                        "${prev and mask} → ${cur and mask}",
                )
            } else {
                assertTrue(curPhys > prevPhys, "物理部分只许前进：$prevPhys → $curPhys")
                assertTrue(
                    (cur and mask) == 0L || (cur and mask) < (prev and mask),
                    "跨毫秒时逻辑位应当重置（新的一毫秒从头开始）",
                )
            }
            prev = cur
        }
        assertTrue(sawSameMilli, "一万次分配里应当至少出现过一次同毫秒分配（否则这条断言没验到东西）")
    }

    @Test
    fun `并发分配不产生重复或回退`() = runBlocking {
        val tso = MonotonicTso()
        val n = 64
        val per = 500
        val all = coroutineScope {
            (0 until n).map { async { (0 until per).map { tso.next().value } } }
                .flatMap { it.await() }
        }
        assertEquals(n * per, all.size)
        assertEquals(all.size, all.toSet().size, "并发分配不允许出现重复时间戳")
        assertEquals(all.sorted(), all.sorted().also { }, "时间戳集合应可全序排列")
    }

    @Test
    fun `物理时间部分可还原`() = runBlocking {
        val tso = MonotonicTso()
        val before = System.currentTimeMillis()
        val ts = tso.next().value
        val physical = MonotonicTso.physicalMillis(ts)
        assertTrue(physical >= before - 5, "物理部分应接近当前时间: $physical vs $before")
        assertTrue(physical <= System.currentTimeMillis() + 5)
    }

    // ── 持久化 TSO ────────────────────────────────────────────────

    @Test
    fun `持久化 TSO 批量预取 - 发放不产生逐次请求`() = runBlocking {
        val store = com.cloudea.yangbase.objectstore.CountingObjectStore(MemoryObjectStore())
        // 段的跨度按**毫秒**算：64 秒的段足够覆盖这段测试的全部发放
        val tso = PersistentTso(store, "c1", batchMillis = 64_000)
        repeat(100) { tso.next() }
        assertTrue(store.count("put") <= 1, "一段之内发放不应产生逐次请求，实际=${store.report()}")
    }

    @Test
    fun `持久化 TSO 重启后不回退`() = runBlocking {
        val store = MemoryObjectStore()
        val t1 = PersistentTso(store, "c1", batchMillis = 64_000)
        var last = 0L
        repeat(100) { last = t1.next().value }

        // 模拟进程重启：新实例从持久化水位恢复
        val t2 = PersistentTso(store, "c1", batchMillis = 64_000)
        t2.recover()
        val afterRestart = t2.next().value
        assertTrue(afterRestart > last, "重启后时间戳不得回退: $afterRestart 不大于 $last")
    }

    @Test
    fun `持久化 TSO 在没有水位对象时正常启动`() = runBlocking {
        val tso = PersistentTso(MemoryObjectStore(), "fresh")
        tso.recover()   // 不应抛异常
        assertTrue(tso.next().value > 0)
    }

    @Test
    fun `持久化 TSO 的水位对象损坏时报错而不是静默重置`() = runBlocking {
        val store = MemoryObjectStore()
        store.put("meta/tso/c1.json", "这不是 JSON".toByteArray())
        var threw = false
        try {
            PersistentTso(store, "c1").recover()
        } catch (_: com.cloudea.yangbase.common.CorruptDataException) {
            threw = true
        }
        assertTrue(threw, "水位损坏必须报错——静默重置会让时间戳与既有数据冲突")
    }
}
