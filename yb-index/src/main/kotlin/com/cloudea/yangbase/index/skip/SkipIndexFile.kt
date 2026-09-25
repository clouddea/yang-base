package com.cloudea.yangbase.index.skip

import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.IndexDef
import com.cloudea.yangbase.common.IndexId
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.storage.BloomData
import kotlinx.serialization.Serializable

/** [IndexKind] 中哪些属于跳数索引（§8.5）。 */
val IndexKind.isSkipIndex: Boolean
    get() = this == IndexKind.SKIP_MINMAX || this == IndexKind.SKIP_SET ||
        this == IndexKind.SKIP_BLOOM || this == IndexKind.SKIP_NGRAM_BLOOM

/**
 * 一个跳数索引文件（`.skp`），对应**一个 SST 文件**。
 *
 * 设计要点（§8.5 + P3）：
 *  - 它是 SST 文件的**确定性投影**：内容只是该文件各数据块的紧凑摘要，
 *    因此键由 `(index_id, sst_file_id)` 唯一确定，**不需要 manifest 指针**——
 *    它和 `.idx` 同构，多一层指针只会给查询热路径多加一次 GET；
 *  - 必须小到能整体常驻内存（「元数据小而热」是 P3 的原文）；
 *  - 不可变：SST 文件不可变 ⇒ 它的摘要也不可变（P2）。
 *
 * 与 SST `.idx` 里已有的 `BlockIndexEntry.colStats` 的关系（必须说清楚，否则重复建设）：
 *  - `min` / `max` **已经在 `.idx` 里**，而 `.idx` 在查询时本就常驻内存 ⇒
 *    minmax 剪枝是 **0 次额外 GET、0 字节额外传输**，永远优先用它；
 *  - 本文件补充的是 `.idx` 里没有的 `set` / `bloom` / `ngram`。
 *    这些是**可选**的：没有 `.skp` 时剪枝自动退化为 minmax。
 */
@Serializable
data class SkipIndexFile(
    val formatVersion: Int = 1,
    /** 构建它的索引类型（[IndexKind] 的名字），用于防止读错对象。 */
    val kind: String,
    val columns: List<String>,
    val sstFileId: String,
    /** 每 N 个数据块合一个索引项（§8.1 的 [IndexDef.granularity]）。 */
    val granularity: Int,
    val blockCount: Int,
    val entries: List<SkipEntry>,
    val fileRows: Long = 0,
) {
    fun estimateBytes(): Long = 160L + entries.sumOf { e ->
        e.columns.sumOf { c -> c?.estimatedBytes() ?: 0L }
    }

    /** 块 → 索引项下标。-1 表示该块没有摘要（一律不可剪枝）。 */
    fun blockToEntryMap(): IntArray {
        val out = IntArray(blockCount) { -1 }
        for ((ei, e) in entries.withIndex()) {
            val hi = minOf(e.blockTo, blockCount - 1)
            for (b in e.blockFrom..hi) if (b >= 0) out[b] = ei
        }
        return out
    }
}

/** 一个索引项：覆盖数据块 `[blockFrom, blockTo]`。 */
@Serializable
data class SkipEntry(
    val blockFrom: Int,
    val blockTo: Int,
    val rows: Long,
    /** 按 [SkipIndexFile.columns] 顺序；null 表示该列未收集（一律不可剪枝）。 */
    val columns: List<ColumnSummary?>,
)

/**
 * 一个索引项内某一列的摘要。
 *
 * **完整性标记（决定正确性，不是优化）**：
 *  - `values == null` 表示「未收集或不完整」，此时 set 剪枝必须放弃；
 *  - `values != null` 表示该索引项内**所有非 NULL 值都在里面**。
 *    超限时整体置 null，**绝不截断列表**——不完整的集合会让「一定不含」判定出错，
 *    那是静默漏行。
 */
@Serializable
data class ColumnSummary(
    val min: ByteArray? = null,
    val max: ByteArray? = null,
    val nulls: Long = 0,
    val values: List<ByteArray>? = null,
    val bloom: BloomData? = null,
    val ngramBloom: BloomData? = null,
    /**
     * 是否真的收集过 min/max。
     *
     * 必须显式记录：`min == null && max == null` 既是「全是 NULL」也是
     * 「这类索引压根不收集 min/max」（如 ngram）的形态。混用会得出
     * 「整块全 NULL」⇒ 剪掉 `IS NOT NULL` ⇒ **漏行**。
     */
    val minmaxCollected: Boolean = true,
) {
    /** 该索引项内**所有**行都是 NULL（`IS NOT NULL` 可以据此剪枝）。 */
    val allNull: Boolean get() = minmaxCollected && min == null && max == null && nulls > 0

    fun estimatedBytes(): Long =
        (min?.size ?: 0) + (max?.size ?: 0) + 24L +
            (values?.sumOf { it.size + 4 } ?: 0) +
            (bloom?.bits?.size ?: 0).toLong() + (ngramBloom?.bits?.size ?: 0).toLong()

    /** 值是否可能在这个索引项内。`values == null` 时保守返回 true。 */
    fun mightContainValue(encoded: ByteArray): Boolean {
        val vs = values ?: return true
        for (v in vs) if (v.contentEquals(encoded)) return true
        return false
    }

    /** 索引项内所有非 NULL 值是否都等于 `encoded`（用于 `!=` 剪枝）。 */
    fun allNonNullEqual(encoded: ByteArray): Boolean {
        val vs = values ?: return false
        return vs.size == 1 && vs[0].contentEquals(encoded)
    }
}

/** 一个跳数索引的定义（从 catalog 的 [IndexDef] 解析而来）。 */
data class SkipIndexDef(
    val indexId: IndexId,
    val kind: IndexKind,
    /** schema 列下标。 */
    val columns: List<Int>,
    /** 与 [columns] 一一对应的列名（用于读取时的自校验）。 */
    val columnNames: List<String> = emptyList(),
    val granularity: Int = 1,
) {
    init {
        require(kind.isSkipIndex) { "$kind 不是跳数索引" }
        require(columns.isNotEmpty()) { "跳数索引至少要有一列" }
        require(granularity >= 1) { "granularity 必须 >= 1" }
        require(columnNames.isEmpty() || columnNames.size == columns.size) {
            "columnNames 与 columns 长度不一致"
        }
    }

    companion object {
        fun of(indexId: IndexId, def: IndexDef, schema: TableSchema): SkipIndexDef =
            SkipIndexDef(
                indexId = indexId,
                kind = def.kind,
                columns = def.columns.map { schema.indexOf(it) },
                columnNames = def.columns,
                granularity = def.granularity.coerceAtLeast(1),
            )
    }
}

/**
 * 逐块累积跳数索引。
 *
 * 写路径的代价（§8.5「代价」）：每行每列一次编码 + 摘要更新。这是**写路径为读路径
 * 付的钱**，与 `SstWriter` 收集 `colStats` 同构。可行的话应当在 flush/compaction
 * 的行还在内存里时顺便构建，而不是事后回读整个 SST。
 *
 * 内存：峰值 ≈ 一个块的去重值集合 + 一个块的 n-gram 集合（block 级位图在
 * `endBlock()` 时就地构建并丢弃集合）。
 */
class SkipIndexBuilder(
    private val def: SkipIndexDef,
    private val schema: TableSchema,
    /** 每块预期去重键数 + 每键位数 ⇒ 所有块位图同尺寸（分组时要 OR 合并）。 */
    private val expectedKeysPerBlock: Int = 512,
    /** 每块预期 n-gram 数，用来定 ngram 位图大小。 */
    private val expectedGramsPerBlock: Int = 8192,
    private val bitsPerKey: Int = 10,
    /** set 索引每块去重值上限；超过则整块放弃收集（绝不截断）。 */
    private val maxSetDistinct: Int = 256,
    /** bloom 索引每块去重值上限；超过则整块放弃收集。 */
    private val maxBloomKeys: Int = 65536,
    private val ngramN: Int = 3,
) {
    private val wantSet = def.kind == IndexKind.SKIP_SET
    private val wantMinMax = def.kind != IndexKind.SKIP_NGRAM_BLOOM
    private val wantBloom = def.kind == IndexKind.SKIP_SET || def.kind == IndexKind.SKIP_BLOOM
    private val wantNgram = def.kind == IndexKind.SKIP_NGRAM_BLOOM

    private val cols = def.columns
    private val n = cols.size

    private val keyCap: Int = if (wantSet) maxSetDistinct else maxBloomKeys

    private val keyBloomBytes: Int =
        ((expectedKeysPerBlock.toLong() * bitsPerKey + 7) / 8).toInt().coerceAtLeast(8)
    private val gramBloomBytes: Int =
        ((expectedGramsPerBlock.toLong() * bitsPerKey + 7) / 8).toInt().coerceAtLeast(8)

    // ── 当前块 ────────────────────────────────────────────────────
    private var curBlock = -1
    private var curRows = 0L
    private var curMin = arrayOfNulls<ByteArray>(n)
    private var curMax = arrayOfNulls<ByteArray>(n)
    private val curNulls = LongArray(n)
    private var curKeys = arrayOfNulls<LinkedHashMap<String, ByteArray>>(n)
    private var curGrams = arrayOfNulls<LinkedHashSet<String>>(n)
    private var curOverflow = BooleanArray(n)

    private val perBlock = ArrayList<SkipEntry>()
    private var totalRows = 0L

    fun beginBlock(blockIdx: Int) {
        require(blockIdx == curBlock + 1 || curBlock == -1) {
            "块下标必须从 0 起严格递增：期望 ${curBlock + 1}，收到 $blockIdx"
        }
        curBlock = blockIdx
        curRows = 0
        curMin = arrayOfNulls(n)
        curMax = arrayOfNulls(n)
        curNulls.fill(0)
        curKeys = arrayOfNulls(n)
        curGrams = arrayOfNulls(n)
        curOverflow = BooleanArray(n)
    }

    /** 喂入一行（列数与 schema 一致）。 */
    fun observe(row: List<Datum>) {
        require(curBlock >= 0) { "必须先 beginBlock" }
        curRows++
        for (i in 0 until n) {
            val ci = cols[i]
            val d = row[ci]
            if (d is Datum.Null) { curNulls[i]++; continue }
            val type = schema.columns[ci].type
            val enc = KeyEncoder.encodeDatum(d, type)
            if (wantMinMax) {
                val mn = curMin[i]
                if (mn == null || ByteArrayComparator.compare(enc, mn) < 0) curMin[i] = enc
                val mx = curMax[i]
                if (mx == null || ByteArrayComparator.compare(enc, mx) > 0) curMax[i] = enc
            }
            if (wantBloom && !curOverflow[i]) {
                val m = curKeys[i] ?: LinkedHashMap<String, ByteArray>().also { curKeys[i] = it }
                val k = String(enc, Charsets.ISO_8859_1)
                if (!m.containsKey(k)) {
                    if (m.size >= keyCap) {
                        // 超限：整列放弃本次收集。**绝不截断**（不完整 ⇒ 可能漏行）
                        curKeys[i] = null
                        curOverflow[i] = true
                    } else m[k] = enc
                }
            }
            if (wantNgram && type.isString) {
                val set = curGrams[i] ?: LinkedHashSet<String>().also { curGrams[i] = it }
                addGrams(d.asString(), set)
            }
        }
    }

    /** 结束当前块。 */
    fun endBlock() {
        if (curBlock < 0) return
        val summaries = ArrayList<ColumnSummary?>(n)
        for (i in 0 until n) {
            val keys = curKeys[i]
            val values = keys?.values?.toList()
            val bloom = if (wantBloom && keys != null) {
                BloomData.build(values!!, bitsPerKey = bitsPerKey)
            } else null
            val ngram = if (wantNgram) curGrams[i]?.let { g ->
                if (g.isEmpty()) null
                else BloomData.build(g.map { it.toByteArray(Charsets.ISO_8859_1) }, bitsPerKey = bitsPerKey)
            } else null
            summaries.add(
                ColumnSummary(
                    min = curMin[i],
                    max = curMax[i],
                    nulls = curNulls[i],
                    values = values,
                    bloom = bloom,
                    ngramBloom = ngram,
                    minmaxCollected = wantMinMax,
                )
            )
        }
        perBlock.add(SkipEntry(curBlock, curBlock, curRows, summaries))
        totalRows += curRows
        curBlock = -1
    }

    private fun addGrams(s: String, out: LinkedHashSet<String>) {
        if (s.length < ngramN) return
        for (i in 0..s.length - ngramN) out.add(s.substring(i, i + ngramN))
    }

    /** 产出最终文件：把逐块摘要按 granularity 合成索引项。 */
    fun build(sstFileId: String): SkipIndexFile {
        if (curBlock >= 0) endBlock()
        val g = def.granularity
        val entries = ArrayList<SkipEntry>((perBlock.size + g - 1) / g)
        var i = 0
        while (i < perBlock.size) {
            val group = perBlock.subList(i, minOf(i + g, perBlock.size))
            entries.add(merge(group))
            i += g
        }
        return SkipIndexFile(
            kind = def.kind.name,
            columns = cols.map { schema.columns[it].name },
            sstFileId = sstFileId,
            granularity = g,
            blockCount = perBlock.size,
            entries = entries,
            fileRows = totalRows,
        )
    }

    /**
     * 合并一组块摘要。
     *
     * 三条**保守**规则（宁可不剪枝，绝不错剪）：
     *  - 任一块的 set 不完整 ⇒ 整组的 set 置 null；
     *  - 并集超过上限 ⇒ 置 null；
     *  - 位图同理（它就是从完整的键集合构建出来的）。
     *    ngram 位图不设上限并把每块 OR 起来：它只用于「一定不含」的判定，
     *    位更密只会提高假阳性率，不会漏。
     */
    private fun merge(group: List<SkipEntry>): SkipEntry {
        val colsOut = ArrayList<ColumnSummary?>(n)
        for (i in 0 until n) {
            val summaries = group.map { it.columns[i] }
            if (summaries.all { it == null }) { colsOut.add(null); continue }

            var min: ByteArray? = null
            var max: ByteArray? = null
            var nulls = 0L
            var minmaxCollected = false
            var valuesComplete = true
            val union = LinkedHashSet<String>()

            for (s in summaries) {
                if (s == null) { valuesComplete = false; continue }
                nulls += s.nulls
                if (s.minmaxCollected) {
                    minmaxCollected = true
                    if (s.min != null && (min == null || ByteArrayComparator.compare(s.min, min!!) < 0)) min = s.min
                    if (s.max != null && (max == null || ByteArrayComparator.compare(s.max, max!!) > 0)) max = s.max
                }
                val sv = s.values
                if (sv == null) valuesComplete = false
                else for (v in sv) union.add(String(v, Charsets.ISO_8859_1))
                if (union.size > keyCap) valuesComplete = false
            }

            val values = if (valuesComplete) union.map { it.toByteArray(Charsets.ISO_8859_1) } else null
            colsOut.add(
                ColumnSummary(
                    min = min,
                    max = max,
                    nulls = nulls,
                    values = values,
                    // 位图只能从**完整**键集合构建：不完整 ⇒ 假阴性 ⇒ 漏行
                    bloom = if (values != null) orBlooms(summaries.map { it?.bloom }) else null,
                    ngramBloom = orBlooms(summaries.map { it?.ngramBloom }),
                    minmaxCollected = minmaxCollected,
                )
            )
        }
        return SkipEntry(
            blockFrom = group.first().blockFrom,
            blockTo = group.last().blockTo,
            rows = group.sumOf { it.rows },
            columns = colsOut,
        )
    }

    /**
     * 位图的 OR 合并。**要求所有位图尺寸一致**——这正是位图大小由构造参数定死的原因。
     * 任何一个为 null（不完整）⇒ 整个结果 null（不完整的 OR 会漏掉某些键 ⇒ 假阴性）。
     */
    private fun orBlooms(blooms: List<BloomData?>): BloomData? {
        if (blooms.isEmpty() || blooms.any { it == null }) return null
        val first = blooms.first()!!
        val size = first.bits.size
        if (blooms.any { it!!.bits.size != size || it.numHashes != first.numHashes }) {
            // 尺寸不一致说明构建参数变过：放弃合并而不是猜
            return null
        }
        if (blooms.size == 1) return first
        val out = ByteArray(size)
        for (b in blooms) {
            val bits = b!!.bits
            for (j in 0 until size) out[j] = (out[j].toInt() or bits[j].toInt()).toByte()
        }
        return BloomData(out, first.numHashes, blooms.sumOf { it!!.numKeys })
    }
}
