package com.cloudea.yangbase.index.skip

import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.storage.BlockEntry
import com.cloudea.yangbase.storage.SstHandle
import com.cloudea.yangbase.storage.SstReader

/** 扫描出的一行。 */
class ScannedRow(
    val userKey: ByteArray,
    val commitTs: Long,
    val values: List<Datum>,
)

/** 扫描统计。[pruneStats] 为 null 表示本次没有走剪枝（对照路径）。 */
data class ScanStats(
    val blocksTotal: Int,
    val blocksRead: Int,
    val rowsExamined: Int,
    val rowsReturned: Int,
    val pruneStats: PruneStats?,
) {
    override fun toString(): String =
        "块=$blocksTotal 实读=$blocksRead 行扫描=$rowsExamined 返回=$rowsReturned" +
            (pruneStats?.let { " | $it" } ?: "")
}

class ScanResult(val rows: List<ScannedRow>, val stats: ScanStats)

/** 扫描参数。 */
class ScanOptions(
    val predicates: List<SkipPredicate> = emptyList(),
    /** 可见性快照。默认全可见（`Long.MAX_VALUE`）。 */
    val readTs: ReadTs = ReadTs(Long.MAX_VALUE),
    val keyFrom: ByteArray? = null,
    val keyTo: ByteArray? = null,
    val limit: Int = Int.MAX_VALUE,
    /** 已加载的 `.skp` 及其覆盖的 schema 列下标；为 null 时退化为纯 minmax 剪枝。 */
    val skipIndex: SkipIndexFile? = null,
    val skipIndexColumns: List<Int>? = null,
    /** 关闭剪枝，走「全块读取 + 逐行过滤」的对照路径。 */
    val disablePruning: Boolean = false,
)

/**
 * 带跳数索引剪枝的 SST 扫描。
 *
 * 执行顺序（§8.5 的查询执行示意）：
 * ```
 *   1. 剪枝判定 —— 纯内存，0 次请求（用 .idx 的 min/max 与可选的 .skp）
 *   2. 只对存活块发起范围 GET
 *   3. 块内逐行解压 + 逐行复核谓词   ← 剪枝不能替代过滤，这一步不可省
 * ```
 *
 * 第 3 步是硬要求：跳数索引只能否定，不能肯定。任何「剪枝说可能含就直接返回行」
 * 的实现都会返回假阳性甚至漏行。
 *
 * ⚠️ 本类只做**单文件**扫描（与 [SstReader] 同级）。跨文件的版本归并与墓碑处理
 * 仍是 `LsmStore` / `MergingIterator` 的职责。
 */
class SkipIndexScanner(
    private val reader: SstReader,
    private val schema: TableSchema,
) {

    suspend fun scan(handle: SstHandle, options: ScanOptions = ScanOptions()): ScanResult {
        val rows = ArrayList<ScannedRow>()
        val stats = scanInto(handle, options) { rows.add(it); true }
        return ScanResult(rows, stats)
    }

    /**
     * 流式扫描。[sink] 返回 false 表示提前终止。
     */
    suspend fun scanInto(
        handle: SstHandle,
        options: ScanOptions,
        sink: suspend (ScannedRow) -> Boolean,
    ): ScanStats {
        validate(options.predicates)
        val idx = handle.index
        val pruneStats: PruneStats?
        val blocks: IntArray
        if (options.disablePruning) {
            pruneStats = null
            blocks = IntArray(idx.blocks.size) { it }
        } else {
            val r = BlockPruner(handle, schema, options.skipIndexColumns, options.skipIndex).prune(
                options.predicates, options.readTs, options.keyFrom, options.keyTo,
            )
            pruneStats = r.stats
            blocks = r.neededBlocks
        }

        var read = 0
        var examined = 0
        var returned = 0

        outer@ for (b in blocks) {
            val entries = reader.readBlock(handle, b)
            read++
            var i = 0
            while (i < entries.size) {
                val first = entries[i]
                // 同一 userKey 的版本在块内连续且按 ts 降序（SstWriter 的不变式）
                var vis: BlockEntry? = null
                var j = i
                while (j < entries.size &&
                    ByteArrayComparator.compare(entries[j].userKey, first.userKey) == 0
                ) {
                    if (entries[j].commitTs <= options.readTs.value) { vis = entries[j]; break }
                    j++
                }
                // 跳过该 key 的所有版本
                var j2 = i
                while (j2 < entries.size &&
                    ByteArrayComparator.compare(entries[j2].userKey, first.userKey) == 0
                ) j2++
                i = j2

                if (vis == null || vis.isTombstone) continue
                if (options.keyFrom != null &&
                    ByteArrayComparator.compare(first.userKey, options.keyFrom) < 0
                ) continue
                if (options.keyTo != null &&
                    ByteArrayComparator.compare(first.userKey, options.keyTo) > 0
                ) continue

                examined++
                val row = RowCodec.decode(vis.value!!, schema)
                // ★ 剪枝只能排除，行级复核不可省（§8.5）
                if (!options.predicates.allMatch(row)) continue
                if (!sink(ScannedRow(first.userKey, vis.commitTs, row))) break@outer
                returned++
                if (returned >= options.limit) break@outer
            }
        }

        return ScanStats(
            blocksTotal = idx.blocks.size,
            blocksRead = read,
            rowsExamined = examined,
            rowsReturned = returned,
            pruneStats = pruneStats,
        )
    }

    private fun validate(predicates: List<SkipPredicate>) {
        val n = schema.columns.size
        for (p in predicates) {
            for (c in p.columns) {
                require(c in 0 until n) { "谓词引用了不存在的列下标 $c（schema 只有 $n 列）: ${p.describe()}" }
            }
        }
    }
}
