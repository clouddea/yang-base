package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.CapabilityNotSupportedException
import com.cloudea.yangbase.common.CorruptDataException
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.ObjectStoreException
import com.cloudea.yangbase.common.PreconditionFailedException
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.common.metrics.StoreOp

/**
 * 装饰器：把每一次对象存储调用折算成 §12.5 的指标。
 *
 * ## 为什么装饰在最外层
 *
 * 指标要回答的问题是「这个进程**真正**向对象存储发了几次请求、传了多少字节」。
 * 因此计数点必须在最靠近真实后端的那一层之上——在它之下（后端内部）统计不到
 * 上层的调用意图，在它之上（引擎内部）统计不到重试与缓存穿透后的真实次数。
 *
 * 与 [CountingObjectStore] 的关系：那个是**测试夹具**（断言 IO 预算，
 * 可以随时 `reset()`、可以注入故障），这个是**生产埋点**（长期累积、可被抓取）。
 * 两者刻意不合并——测试夹具需要能被清零和注入故障，生产埋点两者都不该有。
 *
 * ## 开销
 *
 * 每次调用：两次 `System.nanoTime()`、一次数组索引、一次 `LongAdder` 加法。
 * **不分配对象、不加锁、不产生任何对象存储请求**（原则 P4）——
 * 这一点由 `MetricsNoIoIntegrationTest` 用 `CountingObjectStore` 断言。
 * 异常路径多一次 `ConcurrentHashMap` 查询，那是冷路径。
 */
class InstrumentedObjectStore(private val delegate: ObjectStore) : ObjectStore {

    override val caps: ObjectStoreCapabilities get() = delegate.caps

    override suspend fun get(key: String, range: ByteRange?): ByteArray {
        val t0 = System.nanoTime()
        try {
            val data = delegate.get(key, range)
            Metrics.onStoreRequest(
                if (range == null) StoreOp.GET else StoreOp.GET_RANGE,
                bytesOut = data.size.toLong(),
                nanos = System.nanoTime() - t0,
            )
            return data
        } catch (t: Throwable) {
            recordError(t)
            throw t
        }
    }

    override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag {
        val t0 = System.nanoTime()
        try {
            val etag = delegate.put(key, data, precondition)
            Metrics.onStoreRequest(
                if (precondition == null) StoreOp.PUT else StoreOp.PUT_CONDITIONAL,
                bytesIn = data.size.toLong(),
                nanos = System.nanoTime() - t0,
            )
            return etag
        } catch (t: Throwable) {
            recordError(t)
            throw t
        }
    }

    override suspend fun delete(key: String): Boolean {
        val t0 = System.nanoTime()
        try {
            val r = delegate.delete(key)
            Metrics.onStoreRequest(StoreOp.DELETE, nanos = System.nanoTime() - t0)
            return r
        } catch (t: Throwable) {
            recordError(t)
            throw t
        }
    }

    override suspend fun deleteBatch(keys: Collection<String>): Int {
        val t0 = System.nanoTime()
        try {
            val n = delegate.deleteBatch(keys)
            // 延迟按一次调用记（那是用户等的时间），请求数由各后端自己的分页决定；
            // 需要请求数口径的断言请用 CountingObjectStore
            Metrics.onStoreRequest(StoreOp.DELETE_BATCH, nanos = System.nanoTime() - t0)
            return n
        } catch (t: Throwable) {
            recordError(t)
            throw t
        }
    }

    override suspend fun list(prefix: String, pageToken: String?, limit: Int): ListPage {
        val t0 = System.nanoTime()
        try {
            val page = delegate.list(prefix, pageToken, limit)
            Metrics.onStoreRequest(StoreOp.LIST, nanos = System.nanoTime() - t0)
            return page
        } catch (t: Throwable) {
            recordError(t)
            throw t
        }
    }

    override suspend fun head(key: String): ObjectMeta {
        val t0 = System.nanoTime()
        try {
            val m = delegate.head(key)
            Metrics.onStoreRequest(StoreOp.HEAD, nanos = System.nanoTime() - t0)
            return m
        } catch (t: Throwable) {
            recordError(t)
            throw t
        }
    }

    /**
     * 覆写默认实现（默认实现走 [head]，会被计成一次 HEAD）。
     *
     * 语义上 `exists` 就是一次 HEAD，因此保持「一次 HEAD」的计数不变，
     * 只是让它落到同一个方法里，避免两处各记一遍。
     */
    override suspend fun exists(key: String): Boolean = try {
        head(key); true
    } catch (_: ObjectNotFoundException) {
        false
    }

    /**
     * 错误分类。
     *
     * 用「确定的类型名」而不是异常消息——消息里带 key、etag 这些高基数的内容，
     * 作为标签会炸掉时序数据库。这里的分类刚好对应调用方**能做不同反应**的种类：
     * 条件不成立不该重试，并发冲突可以退避，其余是后端故障。
     */
    private fun recordError(t: Throwable) {
        val code = when (t) {
            is ObjectNotFoundException -> "not_found"
            is PreconditionFailedException -> "precondition_failed"
            is com.cloudea.yangbase.common.ConcurrentModificationException -> "conflict"
            is CapabilityNotSupportedException -> "capability"
            is CorruptDataException -> "corrupt"
            is ObjectStoreException -> "store_error"
            else -> t::class.simpleName ?: "unknown"
        }
        Metrics.onStoreError(code)
    }
}
