package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.stats.PartitionColumnStats
import com.cloudea.yangbase.common.stats.TableStats
import com.cloudea.yangbase.objectstore.ObjectKeys
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json
import java.util.concurrent.ConcurrentHashMap

/**
 * 统计信息的**对象存储读写 + 内存缓存**（设计文档 §10.3、§5.1）。
 *
 * ## 版本怎么定、怎么发现（这是整件事的关键设计）
 *
 * 统计对象名里带版本号（`stats/table/{tid}/partitions.v{n}.json`），版本必须单调、
 * 永不复用（§5.2 规则 3）。这里**复用表元数据自身的版本**：`ANALYZE TABLE`
 * 先 `Catalog.updateTable` 把 schema 版本 +1，再按新版本号写统计对象。好处：
 *  - **发现是 0 次请求**：版本号来自本地已常驻内存的 Catalog，不需要 LIST，
 *    也不需要额外读一个指针对象（原则 P4：规划路径的请求数必须是 0）；
 *  - **失效是自动的**：任何 DDL（加列、加索引）都会 +1，旧统计自然再也匹配不上，
 *    优化器退回硬编码优先级链——不会出现「拿旧统计规划新表」的静默错误；
 *  - **对象不可变**（原则 P2）：同一个版本号的对象只写一次，永不覆盖。
 *
 * ⚠️ 已知限制（多 CN）：版本发现依赖**本地** Catalog 的内存视图。另一个 CN 要等到
 * 重新加载 catalog 才会看到新版本（当前是单节点，元数据服务未实现，见 §4/§16.3）。
 *
 * ## 缓存
 *
 * `load` 的结果按 `(tableId, version)` 负缓存：没有统计的表**不会**每次规划都去
 * GET 一个不存在的对象（那会把「没有统计」变成每个查询一次额外的 404 请求）。
 * 版本一变缓存即失效。
 */
class TableStatsStore(private val engine: DatabaseEngine) {

    companion object {
        /** 每个引擎一份。与 [com.cloudea.yangbase.sql.FullTextSupport] 同样的理由：缓存必须共享。 */
        private val instances = ConcurrentHashMap<DatabaseEngine, TableStatsStore>()

        fun of(engine: DatabaseEngine): TableStatsStore = instances.computeIfAbsent(engine) { TableStatsStore(it) }
    }

    private val json = Json { prettyPrint = true; encodeDefaults = true; ignoreUnknownKeys = true }
    private val cache = ConcurrentHashMap<Long, CacheEntry>()
    private val loadMutex = Mutex()

    private class CacheEntry(val version: Long, val stats: TableStats?)

    /** `ANALYZE TABLE` 的产物摘要（SQL 层拿它组织结果集与报告体积）。 */
    data class AnalyzeResult(
        val tableName: String,
        val statsVersion: Long,
        val rowCount: Long,
        val flushedRows: Long,
        val partitionCount: Int,
        val columnCount: Int,
        val objectsWritten: Int,
        val bytesWritten: Long,
        /** 折进来的 `.idx` 文件数（**全部来自常驻内存**，不产生数据块读，§13.3）。 */
        val indexFiles: Int,
        val tableObjectKey: String,
        val columnObjectKeys: List<String>,
        val notes: List<String>,
        val stats: TableStats,
    )

    // ── 采集 ──────────────────────────────────────────────────────

    /**
     * `ANALYZE TABLE`：从各分片的 `.idx` 聚合统计并落盘。
     *
     * 请求数（原则 P4 要求每个功能都能回答这个问题）：
     *  - **读**：`.idx` 与 manifest 常驻内存 → 0 次 GET；只有被缓存淘汰的 `.idx`
     *    才会补读。**绝不读 `.dat`**，所以数据块 GET 恒为 0（§13.3）。
     *  - **写**：catalog 的 schema 版本对象 + CURRENT + byname 共 3 次 PUT
     *    （`Catalog.updateTable` 的既有行为），加上每分区 1 个 `colstats` 对象
     *    与 1 个表级 `partitions` 对象。
     *  - 这是一个**显式重操作**，允许昂贵（§13.3：「避免全表 ANALYZE」指的是别在
     *    后台自动跑，而不是别提供）。
     */
    suspend fun analyze(table: TableSchema): AnalyzeResult {
        // 1) 先分配新版本：统计对象名里的版本号与它一致，因此「写完统计」这件事
        //    本身就是可见性开关——旧版本读取者看不到新统计，也就不存在中间态。
        val bumped = engine.catalog.updateTable(table)
        val collector = StatsCollector(bumped)
        val partitions = ArrayList<PartitionColumnStats>()
        val summaries = ArrayList<com.cloudea.yangbase.common.stats.PartitionStats>()
        val columnKeys = ArrayList<String>()
        var objects = 0
        var bytes = 0L

        for (shard in engine.allShards(bumped)) {
            val pid = shard.partition.value
            val lsm = engine.shardStore(shard, bumped)
            val indexes = lsm.loadedIndexes()
            val exact = lsm.exactRowCount()
            val memEntries = lsm.stats().memtableEntries
            val name = bumped.partitions.getOrNull(pid.toInt())?.name ?: "p$pid"
            val collected = collector.collect(pid, name, indexes, exact, memEntries)
            summaries += collected.summary
            val obj = PartitionColumnStats(
                tableId = bumped.tableId,
                partitionId = pid,
                partitionName = name,
                statsVersion = bumped.version,
                rowCount = collected.summary.rowCount,
                flushedRows = collected.summary.flushedRows,
                fileCount = collected.summary.fileCount,
                blockCount = collected.summary.blockCount,
                columns = collected.columns,
                notes = collected.notes,
            )
            partitions += obj
            val key = ObjectKeys.columnStats(TableId(bumped.tableId), PartitionId(pid), bumped.version)
            val body = json.encodeToString(PartitionColumnStats.serializer(), obj).toByteArray()
            engine.store.put(key, body)
            columnKeys += key
            objects++
            bytes += body.size
        }

        val stats = TableStats(
            tableId = bumped.tableId,
            tableName = bumped.name,
            statsVersion = bumped.version,
            rowCount = collector.tableRowCount(),
            rowCountExact = collector.rowsExact(),
            collectedAtMs = System.currentTimeMillis(),
            analyzer = engine.nodeId,
            complete = collector.rowsExact(),
            partitions = summaries,
            columns = collector.mergedColumns(),
            notes = collector.notes(),
        )
        val tableKey = ObjectKeys.partitionStats(TableId(bumped.tableId), bumped.version)
        val tableBody = json.encodeToString(TableStats.serializer(), stats).toByteArray()
        engine.store.put(tableKey, tableBody)
        objects++
        bytes += tableBody.size

        // 刚采集的统计直接进缓存：同一个 CN 的下一次规划应当 0 次请求就用上它。
        cache[bumped.tableId] = CacheEntry(bumped.version, stats)

        return AnalyzeResult(
            tableName = bumped.name,
            statsVersion = bumped.version,
            rowCount = stats.rowCount,
            flushedRows = collector.tableFlushedRows(),
            partitionCount = collector.partitionCount(),
            columnCount = stats.columns.size,
            objectsWritten = objects,
            bytesWritten = bytes,
            indexFiles = partitions.sumOf { it.fileCount },
            tableObjectKey = tableKey,
            columnObjectKeys = columnKeys,
            notes = stats.notes,
            stats = stats,
        )
    }

    // ── 读取（规划路径）────────────────────────────────────────────

    /**
     * 取表的统计信息；没有（或版本对不上、或对象损坏）时返回 `null`。
     *
     * **规划路径 0 次请求**：第一次读这个表会 GET 一次（命中后不再），之后每次规划
     * 都走内存缓存。而「没有统计」是**负缓存**的——否则每个没有 ANALYZE 过的表的
     * 每次查询都要多发一次「取一个不存在的对象」的请求（原则 P4）。
     *
     * 这个方法**从不抛异常**：统计坏了不能把查询弄挂，只能让优化器退回兜底链
     * （设计文档 §10.2：没有统计时的优先级链是保底路径）。
     */
    suspend fun load(table: TableSchema): TableStats? {
        cache[table.tableId]?.let { if (it.version == table.version) return it.stats }
        return loadMutex.withLock {
            cache[table.tableId]?.let { if (it.version == table.version) return@withLock it.stats }
            val stats = read(table)
            cache[table.tableId] = CacheEntry(table.version, stats)
            stats
        }
    }

    private suspend fun read(table: TableSchema): TableStats? = try {
        val key = ObjectKeys.partitionStats(TableId(table.tableId), table.version)
        val text = String(engine.store.get(key), Charsets.UTF_8)
        json.decodeFromString(TableStats.serializer(), text).takeIf { it.tableId == table.tableId }
    } catch (_: ObjectNotFoundException) {
        null
    } catch (_: Throwable) {
        // 版本对不上、字段缺失、JSON 坏了 —— 一律当作「没有统计」。
        // 静默降级是刻意的：优化器有兜底链，而这里抛异常会把一个能正确执行的
        // 查询变成失败（代价模型只是「选哪条路」，不是「能不能算」）。
        null
    }

    /** 丢弃缓存（测试与 DDL 后使用）。 */
    fun invalidate(tableId: Long) {
        cache.remove(tableId)
    }

    fun invalidateAll() {
        cache.clear()
    }
}
