package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.PreconditionFailedException
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

/**
 * 纯内存后端。用于**快速单元测试**，不用于集成测试（后者必须走真实对象存储语义）。
 *
 * 它实现了完整的条件写语义，因此可以用来单测 fencing / CAS 这类逻辑，
 * 但它的「网络」是 0 延迟的——任何与延迟或请求数相关的结论都不能来自它。
 */
class MemoryObjectStore(
    override val caps: ObjectStoreCapabilities = ObjectStoreCapabilities(
        conditionalCreate = true,
        conditionalUpdate = true,
        strongReadAfterWrite = true,
        strongListConsistency = true,
        lowLatencyTier = true,
        lowLatencyTierSupportsConditionalWrite = true,
        conditionalWriteVerified = true,
        requestsAreBilled = false,
        serverSideCopy = true,
        eventNotification = false,
    ),
) : ObjectStore {

    private data class Entry(val data: ByteArray, val etag: ETag, val modified: Long)

    /** 用有序 Map，天然满足 LIST 的字典序语义。 */
    private val map = ConcurrentSkipListMap<String, Entry>()
    private val version = AtomicLong(0)
    private val epochMillis = AtomicLong(System.currentTimeMillis())

    /** 便于测试：模拟一个「已存在的对象」的时间戳推进。 */
    private fun nextModified(): Long = epochMillis.incrementAndGet()

    override suspend fun get(key: String, range: ByteRange?): ByteArray {
        val e = map[key] ?: throw ObjectNotFoundException(key)
        if (range == null) return e.data.copyOf()
        val start = range.start
        val end = minOf(range.endInclusive + 1, e.data.size.toLong())
        if (start >= e.data.size) return ByteArray(0)
        return e.data.copyOfRange(start.toInt(), end.toInt())
    }

    override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag {
        synchronized(this) {
            val existing = map[key]
            when (precondition) {
                null -> {}
                Precondition.MustNotExist -> if (existing != null) {
                    throw PreconditionFailedException("对象已存在: $key")
                }
                is Precondition.MustMatch -> {
                    if (existing == null) throw ObjectNotFoundException(key)
                    if (existing.etag != precondition.etag) {
                        throw ConcurrentModificationException(
                            "etag 不匹配: $key 期望=${precondition.etag} 实际=${existing.etag}"
                        )
                    }
                }
            }
            val etag = ETag("mem-${version.incrementAndGet()}")
            map[key] = Entry(data.copyOf(), etag, nextModified())
            return etag
        }
    }

    override suspend fun delete(key: String): Boolean = synchronized(this) {
        map.remove(key) != null
    }

    override suspend fun list(prefix: String, pageToken: String?, limit: Int): ListPage {
        val start = pageToken ?: prefix
        val keys = ArrayList<String>(limit)
        for (k in map.tailMap(start, true).keys) {
            if (!k.startsWith(prefix)) break
            if (pageToken != null && k == pageToken) continue
            keys.add(k)
            if (keys.size >= limit) break
        }
        val next = if (keys.size >= limit && map.tailMap(keys.last(), false).keys.any { it.startsWith(prefix) }) {
            keys.last()
        } else null
        // 尺寸本来就在内存里，顺手带出去（0 成本）
        val sizes = keys.associateWith { map[it]?.data?.size?.toLong() ?: 0L }
        return ListPage(keys, next, sizes)
    }

    override suspend fun head(key: String): ObjectMeta {
        val e = map[key] ?: throw ObjectNotFoundException(key)
        return ObjectMeta(key, e.data.size.toLong(), e.etag, e.modified)
    }

    /** 测试辅助：清空。 */
    fun clear() = synchronized(this) { map.clear() }

    /** 测试辅助：当前对象数，用于断言 GC 是否真的删掉了东西。 */
    fun size(): Int = map.size

    fun keys(): List<String> = map.keys.toList()
}
