package com.cloudea.yangbase.storage

import kotlinx.coroutines.async
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicLong

/**
 * 内存块缓存（设计文档 §6.7 的 L1 层）。
 *
 * 关键性质：**所有缓存的都是不可变对象**，因此条目永不过期、永不失效——
 * 只需按容量淘汰。这比传统数据库的 buffer pool 简单得多，也没有脏页概念。
 *
 * 另外实现了**在途请求去重**（§6.7.1）：同一 key 的并发未命中只会产生一次
 * 对象存储 GET，其余协程 await 同一个结果。没有这一层，一次缓存未命中会被
 * 放大成 N 次 GET——在对象存储上这是致命的请求数与延迟放大。
 */
class BlockCache(private val maxBytes: Long = 512L * 1024 * 1024) {

    private class Entry(val data: ByteArray, val size: Long)

    private val mutex = Mutex()
    // accessOrder=true 实现 LRU
    private val lru = LinkedHashMap<String, Entry>(1024, 0.75f, true)

    /** 占用字节。`@Volatile` 是为了 [counters] 能在不拿锁的情况下安全读——长整型读非原子（JLS 17.7）。 */
    @Volatile
    private var used = 0L

    private val inFlight = java.util.concurrent.ConcurrentHashMap<String, kotlinx.coroutines.Deferred<ByteArray>>()
    private val scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.Default)

    val hits = AtomicLong()
    val misses = AtomicLong()
    /** 被在途去重「省下」的请求数——这个数字应当被监控，它直接等于省下的钱。 */
    val coalesced = AtomicLong()

    /**
     * 被容量挤出去的条目数。
     *
     * 它和命中率要一起看：命中率低而淘汰数高，说明**工作集比缓存大**，
     * 加内存能直接换来更少的对象存储请求；命中率低而淘汰数也低，
     * 说明访问本身就没有局部性，加内存没有用。
     */
    val evictions = AtomicLong()

    suspend fun get(key: String): ByteArray? = mutex.withLock {
        val e = lru[key]
        if (e == null) { misses.incrementAndGet(); null } else { hits.incrementAndGet(); e.data }
    }

    suspend fun put(key: String, data: ByteArray) {
        if (data.size.toLong() > maxBytes) return // 超大对象不进缓存，避免冲刷整个工作集
        mutex.withLock {
            val prev = lru.put(key, Entry(data, data.size.toLong()))
            used += data.size - (prev?.size ?: 0)
            evictIfNeeded()
        }
    }

    private fun evictIfNeeded() {
        val it = lru.entries.iterator()
        var evicted = 0L
        while (used > maxBytes && it.hasNext()) {
            val e = it.next()
            used -= e.value.size
            it.remove()
            evicted++
        }
        if (evicted > 0) evictions.addAndGet(evicted)
    }

    /** 二级缓存（本地 NVMe）。为 null 表示没有配（设计文档 §6.7）。 */
    @Volatile
    var secondLevel: DiskCache? = null

    /**
     * 带在途去重的加载：并发请求同一个 key 只会触发一次 [loader]。
     *
     * @param immutableKey 这个 key 的内容**是否永不再变**。只有为 true 时才会
     *   写进二级缓存——`manifest/CURRENT` 这类对象内容会变，按 key 缓存它会
     *   读到陈旧内容，那是**静默的错读**，比读不到危险得多。
     *   SST 的 `.idx` 与 `.dat` 块天然满足（文件名里带文件 id，永不复用）。
     */
    suspend fun getOrLoad(
        key: String,
        immutableKey: Boolean = false,
        loader: suspend () -> ByteArray,
    ): ByteArray {
        get(key)?.let { return it }

        val existing = inFlight[key]
        if (existing != null) {
            coalesced.incrementAndGet()
            return existing.await()
        }

        val deferred = scope.async(start = kotlinx.coroutines.CoroutineStart.LAZY) {
            try {
                // 内存未命中 → 先问二级缓存。命中的话**一次对象存储请求都不产生**，
                // 这正是它存在的全部意义
                if (immutableKey) {
                    secondLevel?.get(key)?.let { data ->
                        put(key, data)
                        return@async data
                    }
                }
                val data = loader()
                put(key, data)
                if (immutableKey) secondLevel?.put(key, data)
                data
            } finally {
                inFlight.remove(key)
            }
        }
        val prev = inFlight.putIfAbsent(key, deferred)
        if (prev != null) {
            coalesced.incrementAndGet()
            return prev.await()
        }
        deferred.start()
        return deferred.await()
    }

    suspend fun clear() {
        mutex.withLock { lru.clear(); used = 0 }
        secondLevel?.clear()
    }

    suspend fun stats(): CacheStats = mutex.withLock {
        CacheStats(hits.get(), misses.get(), coalesced.get(), used, lru.size, evictions.get())
    }

    /**
     * 只读内存计数，**不拿锁**。
     *
     * [stats] 要走 `mutex`，而指标抓取（`/metrics`）发生在协议线程上，
     * 不能在那里等一把可能被读路径持有的锁——那会把一次抓取变成一个
     * 可观测性引起的延迟尖峰。`entries` 需要遍历 LRU 表，因此不在这里提供，
     * 要精确值用 [stats]。
     */
    fun counters(): BlockCacheCounters =
        BlockCacheCounters(hits.get(), misses.get(), coalesced.get(), evictions.get(), used)

    fun hitRate(): Double {
        val h = hits.get(); val m = misses.get()
        return if (h + m == 0L) 0.0 else h.toDouble() / (h + m)
    }
}

/** [BlockCache.counters] 的结果：纯内存计数，抓取它不拿锁、不产生任何 IO。 */
data class BlockCacheCounters(
    val hits: Long,
    val misses: Long,
    val coalescedRequests: Long,
    val evictions: Long,
    val usedBytes: Long,
)

data class CacheStats(
    val hits: Long,
    val misses: Long,
    val coalescedRequests: Long,
    val bytesUsed: Long,
    val entries: Int,
    val evictions: Long = 0,
) {
    override fun toString(): String =
        "命中=$hits 未命中=$misses 合并请求=$coalescedRequests 淘汰=$evictions " +
            "占用=${bytesUsed / 1024}KB 条目=$entries"
}
