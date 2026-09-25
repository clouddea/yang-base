package com.cloudea.yangbase.index.fulltext

/**
 * 单个（段, 词条）的倒排游标，带**块级**跳读与分数上界。
 *
 * 所有「下一块」的推进都基于已常驻内存的块索引（0 IO），只有真正取值时才读块
 * （每块 1 次范围 GET）。这正是 §8.6.2 让全文检索在对象存储上可行的关键。
 */
class TermCursor internal constructor(
    val segOrdinal: Int,
    val reader: SegmentReader,
    val term: String,
    val record: PostingsFormat.TermRecord,
    private val blockRecords: List<PostingsFormat.BlockRecord>,
    val idf: Double,
    private val params: Bm25Params,
    private val avgDocLength: Double,
) {
    private var blockIdx = -1
    private var block: PostingsFormat.BlockData? = null
    private var posInBlock = -1

    /** 当前段内 docId；-1 表示尚未定位或已耗尽。 */
    var docId: Int = -1
        private set
    var tf: Int = 0
        private set
    var dl: Int = 0
        private set

    /**
     * 当前文档里该词条的**词位**（升序，1-based 无关紧要，只要「相邻」可用）。
     *
     * 为 null 有两种情况，调用方必须区分：段没有记录词位（`hasPositions=false`），
     * 或者该文档恰好没有这个词条（不该发生——游标停在该文档就说明它命中了）。
     * 短语查询遇到前者必须**报错**而不是当成「不相邻」，否则会静默返回空结果。
     */
    var positions: IntArray? = null
        private set

    /** 全局文档键：`(段序号 << 32) | 段内 docId`。跨段 top-k 靠它建全序。 */
    val docKey: Long get() = if (docId < 0) Long.MAX_VALUE else (segOrdinal.toLong() shl 32) or docId.toLong()

    val exhausted: Boolean get() = docId < 0
    val df: Int get() = record.df
    val blockCount: Int get() = blockRecords.size

    /** 当前游标的精确 BM25 分数。 */
    fun score(): Double = params.score(tf, dl, idf, avgDocLength)

    /** 该词条在**本段**内的分数上界（用全局 (maxTf, minDl)）。 */
    fun segmentUpperBound(): Double =
        params.upperBound(record.maxTf, record.minDl, idf, avgDocLength)

    /**
     * 对「段 [seg]、段内 doc [doc]」这个候选文档，本游标可能贡献的分数上界。
     *
     * 只有当目标落在**本游标当前所在的块**内时才用块级上界；否则必须退回段级上界——
     * 游标后面还有别的块，用当前块的上界去界定「后面所有文档」是**错误的上界**，
     * 会让 WAND 剪掉本该进 top-k 的文档（静默漏结果）。
     */
    fun upperBoundAt(seg: Int, doc: Int): Double {
        if (seg != segOrdinal) return segmentUpperBound()
        val b = currentBlockRecord() ?: return segmentUpperBound()
        if (doc < 0 || b.lastDocId < doc) return segmentUpperBound()
        val at = params.upperBound(b.maxTf, b.minDl, idf, avgDocLength)
        return minOf(at, segmentUpperBound())
    }

    private fun currentBlockRecord(): PostingsFormat.BlockRecord? =
        if (blockIdx in blockRecords.indices) blockRecords[blockIdx] else null

    // ── 游标推进（可能触发一次范围 GET）───────────────────────────

    private suspend fun loadBlock(i: Int): Boolean {
        if (i >= blockRecords.size) {
            exhaust()
            return false
        }
        blockIdx = i
        block = reader.readBlock(record, blockRecords[i])
        posInBlock = -1
        return true
    }

    private fun exhaust() {
        blockIdx = blockRecords.size
        block = null
        posInBlock = -1
        docId = -1
        positions = null
    }

    /** 前进一个文档。游标耗尽后 `docId` 变回 -1。 */
    suspend fun advance() {
        val b = block
        if (b == null) {
            // 尚未定位（或上一个块已耗尽）：
            // blockIdx < 0 是「还没读过任何块」，否则是「下一块还没读」
            if (!loadBlock(if (blockIdx < 0) 0 else blockIdx + 1)) return
        } else if (posInBlock + 1 < b.size) {
            posInBlock++
            setFromBlock()
            return
        } else if (!loadBlock(blockIdx + 1)) {
            return
        }
        // 空块是坏数据（构建期不产生）；不要把它当成耗尽，跳过它继续找
        while (block != null && block!!.size == 0) {
            if (!loadBlock(blockIdx + 1)) return
        }
        posInBlock = 0
        setFromBlock()
    }

    private fun setFromBlock() {
        val b = block ?: run { docId = -1; positions = null; return }
        if (posInBlock < 0 || posInBlock >= b.size) { docId = -1; positions = null; return }
        docId = b.docIds[posInBlock]
        tf = b.tfs[posInBlock]
        dl = b.docLengths[posInBlock]
        positions = b.positions?.getOrNull(posInBlock)
    }

    /**
     * 把游标推进到全局位置上第一个 `(seg, docId) >= (targetSeg, targetDoc)` 的位置。
     *
     * 目标落在**更靠后的段**时，本游标剩余的所有文档在全局序里都在目标之前，
     * 因此直接耗尽——这是正确的（详见 [WandSearcher] 的剪枝说明）。
     */
    suspend fun seekTo(targetSeg: Int, targetDoc: Int) {
        if (targetSeg > segOrdinal) {
            exhaust()
            return
        }
        if (targetSeg < segOrdinal) {
            throw IllegalStateException("WAND 游标只能向前跳：seg=$segOrdinal 目标段=$targetSeg")
        }
        if (docId < 0 && blockIdx >= blockRecords.size) return   // 已耗尽
        if (docId >= targetDoc) return                            // 已经在目标之后（不该发生，但保守处理）

        // ① 先在已加载的块内二分（0 IO）
        val b = block
        if (b != null && posInBlock >= 0 && b.size > 0 && b.docIds[b.size - 1] >= targetDoc) {
            var lo = posInBlock + 1
            var hi = b.size - 1
            while (lo <= hi) {
                val mid = (lo + hi) ushr 1
                if (b.docIds[mid] < targetDoc) lo = mid + 1 else hi = mid - 1
            }
            if (lo < b.size) {
                posInBlock = lo
                setFromBlock()
                return
            }
        }
        // ② 跨块：块索引常驻，先二分定位到「最后一个块内 docId 仍 < target」的下一块
        val startFrom = if (blockIdx < 0) 0 else blockIdx + 1
        var target = blockRecords.size
        var lo = startFrom
        var hi = blockRecords.size - 1
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            if (blockRecords[mid].lastDocId < targetDoc) lo = mid + 1 else { target = mid; hi = mid - 1 }
        }
        if (target >= blockRecords.size) {
            exhaust()
            return
        }
        if (!loadBlock(target)) return
        val nb = block!!
        var j = 0
        while (j < nb.size && nb.docIds[j] < targetDoc) j++
        if (j >= nb.size) {
            // 块内没有（不该发生：块索引说本块 lastDocId >= target）
            advance()
        } else {
            posInBlock = j
            setFromBlock()
        }
    }

    override fun toString(): String = "Cursor(${reader.segId}:$term df=$df @$docId)"
}

/** 文档可见性过滤（MVCC 与删除都在这里汇合）。 */
interface DocFilter {

    /** 是否把该文档算作命中。 */
    suspend fun accepts(segOrdinal: Int, docId: Int): Boolean

    companion object {
        /** 全部可见（忽略 MVCC）。 */
        val ALL: DocFilter = object : DocFilter {
            override suspend fun accepts(segOrdinal: Int, docId: Int) = true
        }
    }
}

/** WAND 的执行统计。**请求数账本就靠它 + `CountingObjectStore` 一起看。** */
data class WandStats(
    val cursors: Int,
    val scoredCandidates: Long,
    /** 因「上界之和 ≤ 阈值」被整段剪掉、**没有读取任何块**的候选推进次数。 */
    val skippedDocs: Long,
    val results: Int,
) {
    override fun toString(): String =
        "游标=$cursors 打分候选=$scoredCandidates 跳过候选=$skippedDocs 命中=${results}"
}

class WandResult(val hits: List<TopKHeap.Entry>, val stats: WandStats)

/**
 * Block-Max WAND（设计文档 §8.6.2）。
 *
 * 算法（每轮）：
 * ```
 *  1. 按 docKey 升序排列还活着的游标
 *  2. 从小到大累加各游标的**分数上界**；累加值首次 ≥ 阈值 θ 的那个游标即枢轴 pivot
 *     （θ = top-k 堆满时堆顶的分数，堆没满时为 0）
 *  3. 若最小的 docKey 恰好等于 pivot 的 docKey → 对所有落在该 doc 的游标算精确 BM25、
 *     更新堆、把这些游标各推进一步
 *  4. 否则 → 把**最小 docKey 的游标**跳到 pivot 的 doc（跳过整个区间：
 *     区间内任何候选只可能被「枢轴之前」的游标命中，而它们上界之和 ≤ θ，进不了 top-k）
 *  5. 没有枢轴 ⇒ 剩余候选都不可能超过 θ ⇒ 结束
 * ```
 *
 * **精确性论证（必须原样成立，否则 top-k 会静默错）**：
 *  - 步骤 2 的上界对「任何 ≥ 该游标当前位置的文档」都成立：
 *    目标落在当前块内用块级 `(maxTf, minDl)` 算上界；落在更远处退回段级上界。
 *    BM25 分数随 tf 增、随 dl 减，所以这两个上界都是真上界。
 *  - 步骤 3 的精确打分覆盖了**所有**落在该 doc 的游标（它们必然排在最前面）。
 *  - 阈值用 `≥` 判定枢轴（不是论文里的 `>`）：分数并列时结果按 docKey 升序取，
 *    用 `>` 会把「分数恰好等于第 k 名但 docKey 更小」的文档剪掉——那就是漏结果。
 *  - 结果顺序为 `(分数降序, docKey 升序)`，与 [TopKHeap] 的淘汰方向严格一致。
 */
class WandSearcher(private val filter: DocFilter) {

    suspend fun search(cursors: List<TermCursor>, topK: Int): WandResult {
        require(topK > 0) { "topK 必须 > 0" }
        val heap = TopKHeap(topK)
        val live = cursors.toMutableList()
        for (c in live) c.advance()

        var scored = 0L
        var skipped = 0L

        while (true) {
            live.removeAll { it.exhausted }
            if (live.isEmpty()) break
            live.sortBy { it.docKey }

            val threshold = heap.threshold
            var acc = 0.0
            var pivotIdx = -1
            for (i in live.indices) {
                val candidate = live[i]
                acc += live[i].upperBoundAt(candidate.segOrdinal, candidate.docId)
                if (acc >= threshold) { pivotIdx = i; break }
            }
            if (pivotIdx < 0) {
                skipped += live.size
                break
            }

            val pivot = live[pivotIdx]
            val pivotSeg = pivot.segOrdinal
            val pivotDoc = pivot.docId

            if (live[0].segOrdinal == pivotSeg && live[0].docId == pivotDoc) {
                var score = 0.0
                var m = 0
                while (m < live.size && live[m].segOrdinal == pivotSeg && live[m].docId == pivotDoc) {
                    score += live[m].score()
                    m++
                }
                scored++
                if (filter.accepts(pivotSeg, pivotDoc)) {
                    val key = (pivotSeg.toLong() shl 32) or pivotDoc.toLong()
                    heap.offer(TopKHeap.Entry(key, pivotDoc, score))
                }
                for (j in 0 until m) live[j].advance()
            } else {
                skipped += 1
                live[0].seekTo(pivotSeg, pivotDoc)
            }
        }

        val hits = heap.toSortedList()
        return WandResult(hits, WandStats(cursors.size, scored, skipped, hits.size))
    }
}
