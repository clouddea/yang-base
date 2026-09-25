package com.cloudea.yangbase.index.skip

import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.index.IndexKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.storage.BlockCache
import kotlinx.serialization.cbor.Cbor
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * 跳数索引文件的读写。
 *
 * **请求数账本（P4）**：
 *  - 写：每个 SST 文件 1 次 PUT（在 flush/compaction 之后、离线进行）；
 *  - 读：每个 SST 文件**至多 1 次 GET**，之后常驻内存（走 §6.7 的 [BlockCache]，
 *    并发未命中由在途去重合并成一次）；不存在的文件会被记进 `knownMissing`，
 *    避免每查询都白打一次 GET。
 *  - **minmax 剪枝不需要本文件**——它读的是必然已常驻的 `.idx`，0 次请求。
 *
 * 写法用普通 PUT 而非条件写：内容是 SST 文件的**确定性函数**，
 * 重复写产生完全相同的字节，因此不需要 CAS，也就绕开了「后端是否支持条件写」
 * 这个坑（P6 / §3.3.1）。
 */
class SkipIndexStore(
    private val store: ObjectStore,
    private val cache: BlockCache,
) {
    private val cbor = Cbor { ignoreUnknownKeys = true }
    private val log = LoggerFactory.getLogger(javaClass)

    /** 已知「没有对应 .skp」的 SST 文件，避免反复 GET 一个不存在的对象。 */
    private val knownMissing: MutableSet<String> = ConcurrentHashMap.newKeySet()

    /** 写出一个跳数索引文件，返回对象字节数。 */
    suspend fun write(def: SkipIndexDef, file: SkipIndexFile): Long {
        val key = IndexKeys.skipFile(def.indexId, file.sstFileId)
        val bytes = cbor.encodeToByteArray(SkipIndexFile.serializer(), file)
        store.put(key, bytes)
        cache.put(key, bytes)
        knownMissing.remove(key)
        return bytes.size.toLong()
    }

    /**
     * 读取某个 SST 文件的跳数索引；不存在（或类型不匹配）时返回 null。
     *
     * 类型/列不匹配时**当作不存在**而不是报错：那份摘要与当前索引定义无关，
     * 用它做剪枝会得出错误结论。
     */
    suspend fun load(def: SkipIndexDef, sstFileId: String): SkipIndexFile? {
        val key = IndexKeys.skipFile(def.indexId, sstFileId)
        if (key in knownMissing) return null
        val bytes = try {
            cache.getOrLoad(key) { store.get(key) }
        } catch (_: ObjectNotFoundException) {
            knownMissing.add(key)
            return null
        }
        val file = cbor.decodeFromByteArray(SkipIndexFile.serializer(), bytes)
        if (file.formatVersion != 1) {
            log.warn("跳数索引格式版本不支持: {} -> {}", key, file.formatVersion)
            return null
        }
        if (file.kind != def.kind.name) {
            log.warn("跳数索引类型不匹配，忽略: {} 期望={} 实际={}", key, def.kind, file.kind)
            return null
        }
        if (def.columnNames.isNotEmpty() &&
            file.columns.map { it.lowercase() } != def.columnNames.map { it.lowercase() }
        ) {
            log.warn("跳数索引列不匹配，忽略: {} 期望={} 实际={}", key, def.columnNames, file.columns)
            return null
        }
        return file
    }

    /** 清掉「不存在」的记忆，供测试重新测量请求数。 */
    fun forgetMissing() = knownMissing.clear()
}
