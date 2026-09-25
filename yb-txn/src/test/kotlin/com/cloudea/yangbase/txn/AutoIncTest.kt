package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.Precondition
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 自增分段分配（§11.4）的测试。
 *
 * 这一组测试盯的不是「发号是否连续」——不连续是**明确接受的设计后果**。
 * 盯的是两条会静默损坏数据的不变式：
 *
 * 1. **同一个 id 绝不能被发出两次**（重复主键 = 后写的行覆盖先写的，无声无息）；
 * 2. **进程重启后不能从头开始发号**（否则新行会覆盖恢复回来的老行）。
 *
 * 旧实现两条都违反：计数器在内存里、按会话各存一份、重启即归零。
 */
class AutoIncTest {

    private val tableId = 42L
    private val noSeed: suspend () -> Long = { 0L }

    @Test
    fun `连续取号从 1 开始且单调`() = runBlocking {
        val a = AutoIncAllocator(MemoryObjectStore(), segmentSize = 4)
        assertEquals(1L, a.nextId(tableId, noSeed))
        assertEquals(2L, a.nextId(tableId, noSeed))
        assertEquals(3L, a.nextId(tableId, noSeed))
        assertEquals(4L, a.nextId(tableId, noSeed))
        // 跨段：5 之后水位对象已经是 4，新段是 (4, 8]
        assertEquals(5L, a.nextId(tableId, noSeed))
        assertEquals(6L, a.nextId(tableId, noSeed))
    }

    /**
     * ★ 核心用例：**换一个分配器实例**（模拟 CN 重启 / 另一个 CN）接着发号，
     * 绝不能重复已经发出去的号。
     *
     * 这正是旧实现出错的地方：内存计数器归零、再去扫表，扫到的最大值可能
     * 比已经发出但尚未提交（或已回滚）的号更小，于是同一个 id 被发第二次。
     */
    @Test
    fun `重启后不会重发已经用过的 id`() = runBlocking {
        val store = MemoryObjectStore()
        val first = AutoIncAllocator(store, segmentSize = 100)
        val issued = (1..50).map { first.nextId(tableId, noSeed) }

        // CN 重启：分配器全新，但对象存储上的水位还在
        val second = AutoIncAllocator(store, segmentSize = 100)
        val after = (1..50).map { second.nextId(tableId, noSeed) }

        assertEquals(emptySet(), issued.toSet() intersect after.toSet(), "重启后发出了重复的 id")
        assertTrue(after.min() > issued.max(), "重启后必须接着往后发：${after.min()} vs ${issued.max()}")
    }

    /**
     * ★ 两个分配器交替取号（模拟两个 CN 同时写同一张表）必须**无重叠**。
     *
     * 这是分段分配存在的全部理由：CAS 保证一个号段只可能被一个 CN 领到。
     */
    @Test
    fun `两个节点拿到的号段不会重叠`() = runBlocking {
        val store = MemoryObjectStore()
        val a = AutoIncAllocator(store, segmentSize = 8)
        val b = AutoIncAllocator(store, segmentSize = 8)

        val fromA = (1..40).map { a.nextId(tableId, noSeed) }
        val fromB = (1..40).map { b.nextId(tableId, noSeed) }

        assertEquals(emptySet(), fromA.toSet() intersect fromB.toSet(), "两个节点发出了同一个 id")
        assertEquals(80, (fromA + fromB).toSet().size, "80 个号应当两两不同")
    }

    /** 并发取号（同一进程里多个连接）也不能重复。 */
    @Test
    fun `并发取号不重复`() = runBlocking {
        val a = AutoIncAllocator(MemoryObjectStore(), segmentSize = 1000)
        val ids = coroutineScope {
            (1..3000).map { async { a.nextId(tableId, noSeed) } }.awaitAll()
        }
        assertEquals(3000, ids.toSet().size, "并发取号出现了重复")
    }

    /**
     * 水位对象不存在时，用 [AutoIncAllocator.nextId] 的 seed 兜底。
     *
     * 升级自旧版本、以及从备份/时间点恢复回来的表都属于「表里已经有数据，
     * 但水位对象还不存在」。不兜这一下就会从 1 重新发号，覆盖已有行。
     */
    @Test
    fun `从未分配过的表从已有最大值起步`() = runBlocking {
        val a = AutoIncAllocator(MemoryObjectStore(), segmentSize = 100)
        // 表里已经有 id 到 900 的行
        assertEquals(901L, a.nextId(tableId) { 900L })
    }

    /** seed 只在第一次算：它要扫全表，不能每次 CAS 失败都重扫一遍。 */
    @Test
    fun `seed 只被调用一次`() = runBlocking {
        val a = AutoIncAllocator(MemoryObjectStore(), segmentSize = 2)
        var calls = 0
        val seed: suspend () -> Long = { calls++; 0L }
        repeat(10) { a.nextId(tableId, seed) }
        assertEquals(1, calls, "seed 扫全表，只应在建水位对象时执行一次")
    }

    @Test
    fun `显式插入大值之后自增接着它往后发`() = runBlocking {
        val a = AutoIncAllocator(MemoryObjectStore(), segmentSize = 4)
        a.observeExplicit(tableId, 100_000L, noSeed)
        assertEquals(100_001L, a.nextId(tableId, noSeed))
    }

    /**
     * 显式值落在**已经领到手的段内**时，本地游标也必须被推过去。
     *
     * 只推水位对象是不够的：段里的号还没发出去，下一次自增会照发不误，
     * 于是刚插进去的那一行被覆盖。
     */
    @Test
    fun `显式插入落在本地段内也会推游标`() = runBlocking {
        val a = AutoIncAllocator(MemoryObjectStore(), segmentSize = 100)
        assertEquals(1L, a.nextId(tableId, noSeed))      // 领下 (0,100]，下一个是 2
        a.observeExplicit(tableId, 7L, noSeed)           // 7 在段内
        assertEquals(8L, a.nextId(tableId, noSeed), "段内游标没有被推过 7")
    }

    /**
     * 显式插入一个**较小**的值不应该把水位降下来。
     *
     * 水位单调是这套机制的安全底线：降下来就会重发已经用过的号。
     */
    @Test
    fun `显式插入小值不会降低水位`() = runBlocking {
        val a = AutoIncAllocator(MemoryObjectStore(), segmentSize = 10)
        repeat(25) { a.nextId(tableId, noSeed) }         // 水位已到 30
        a.observeExplicit(tableId, 3L, noSeed)
        assertTrue(a.nextId(tableId, noSeed) > 25L, "水位被显式的小值拉低了")
    }

    /**
     * CAS 相撞必须重试并成功，而不是把异常抛给调用方。
     *
     * 用装饰器让**第一次** put 无条件失败，模拟「另一个 CN 抢先写了一次水位」。
     */
    @Test
    fun `水位的 CAS 相撞会重试`() = runBlocking {
        val delegate = MemoryObjectStore()
        var injected = false
        val flaky = object : ObjectStore by delegate {
            override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag {
                // 只让「第一次建立水位」失败一次，之后放行
                if (!injected && precondition is Precondition.MustNotExist) {
                    injected = true
                    throw ConcurrentModificationException("注入的并发冲突")
                }
                return delegate.put(key, data, precondition)
            }
        }
        val a = AutoIncAllocator(flaky, segmentSize = 5)
        assertEquals(1L, a.nextId(tableId, noSeed), "CAS 相撞后应当重试成功")
        assertTrue(injected, "这台测试装置本身要生效")
    }

    /**
     * ★ IO 预算：1000 次取号只应产生**一次**对象存储写。
     *
     * 这条断言是分段分配的全部意义所在（原则 P4）。如果哪天有人把领段改回
     * 「每次取号读一次水位」，这里会立刻红——而不是等到压测时才发现
     * 每次 INSERT 都多了一次几十毫秒的往返。
     *
     * 注意计数口径：带条件的 PUT 记在 `put_cond` 下（那正是它要防的
     * 「无条件覆盖水位」写法）。
     */
    @Test
    fun `一次领段摊薄一千次插入`() = runBlocking {
        val counting = com.cloudea.yangbase.objectstore.CountingObjectStore(MemoryObjectStore())
        val a = AutoIncAllocator(counting, segmentSize = 1000)
        repeat(1000) { a.nextId(tableId, noSeed) }
        assertEquals(
            0L, counting.count("put"),
            "水位必须用条件写推进，无条件写会丢掉并发节点的整个号段",
        )
        assertEquals(1L, counting.count("put_cond"), "每 1000 次取号只应写 1 次水位对象")
        // 首次判断水位在不在只发一次 HEAD——但那是个 404，而夹具只统计
        // **成功返回**的调用，所以这里读数是 0 而不是 1。写在这里免得读者
        // 误以为「一次读都没有」。
        assertEquals(0L, counting.totalReads(), "水位不存在时的那次 HEAD 是 404，夹具不计")
    }

    @Test
    fun `invalidate 丢掉本地段`() = runBlocking {
        val store = MemoryObjectStore()
        val a = AutoIncAllocator(store, segmentSize = 1000)
        val before = a.nextId(tableId, noSeed)
        assertEquals(999L, a.remaining(tableId))
        a.invalidate(tableId)
        assertEquals(0L, a.remaining(tableId))
        // 丢段只是浪费几个号：接着发的号仍然更大
        assertTrue(a.nextId(tableId, noSeed) > before)
    }
}
