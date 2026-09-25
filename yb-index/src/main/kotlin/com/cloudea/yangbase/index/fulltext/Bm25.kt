package com.cloudea.yangbase.index.fulltext

import kotlin.math.ln

/**
 * BM25 参数与评分（设计文档 §8.6.2）。
 *
 * ```
 *   idf(t)  = ln(1 + (N - df + 0.5) / (df + 0.5))
 *   score   = idf(t) · tf·(k1+1) / ( tf + k1·(1 - b + b·dl/avgdl) )
 * ```
 *
 * ⚠️ **上界必须由「原料」算出，不能存算好的分数**：
 * 分数上界随 `tf` 单调增、随 `dl` 单调减，因此
 * `upperBound = 用 (块内最大 tf, 块内最小 dl) 代进同一个公式`
 * 才是真上界。若把分数直接存进索引，改了 `k1`/`b` 之后那个「上界」可能低于真实分数，
 * WAND 就会**静默漏掉本该进 top-k 的文档**。所以段里存的是 `(maxTf, minDl)`。
 */
data class Bm25Params(val k1: Double = 1.2, val b: Double = 0.75) {

    init {
        require(k1 >= 0.0) { "k1 必须 >= 0: $k1" }
        require(b in 0.0..1.0) { "b 必须在 [0,1]: $b" }
    }

    /** [docCount] 是参与本次检索的文档总数，[docFreq] 是含该词条的文档数（跨段求和）。 */
    fun idf(docFreq: Long, docCount: Long): Double {
        if (docCount <= 0) return 0.0
        val df = docFreq.coerceAtMost(docCount)
        return ln(1.0 + (docCount - df + 0.5) / (df + 0.5))
    }

    fun score(tf: Int, docLength: Int, idf: Double, avgDocLength: Double): Double {
        if (tf <= 0) return 0.0
        val avg = if (avgDocLength > 0.0) avgDocLength else 1.0
        val dl = docLength.coerceAtLeast(1)
        val denom = tf + k1 * (1.0 - b + b * dl / avg)
        return idf * (tf * (k1 + 1.0)) / denom
    }

    /**
     * 给定 `(最大 tf, 最小 dl)` 的分数上界。
     *
     * `dl` 按 1 兜底：含该词条的文档字段长度至少为 1，分母不会被压到 0。
     */
    fun upperBound(maxTf: Int, minDl: Int, idf: Double, avgDocLength: Double): Double {
        if (maxTf <= 0) return 0.0
        val avg = if (avgDocLength > 0.0) avgDocLength else 1.0
        val dl = minDl.coerceAtLeast(1)
        val denom = maxTf + k1 * (1.0 - b + b * dl / avg)
        if (denom <= 0.0) return Double.MAX_VALUE
        return idf * (maxTf * (k1 + 1.0)) / denom
    }
}

/** 一次检索的全局统计（跨段求和后算出，保证各段的分数可比）。 */
class SearchStats(
    val docCount: Long,
    val avgDocLength: Double,
)

/** 一个命中。 */
class Hit(
    val docKey: Long,
    val segId: String,
    val docId: Int,
    val pk: ByteArray,
    val commitTs: Long,
    val score: Double,
) {
    override fun toString(): String = "Hit(doc=$docId score=${"%.4f".format(score)} pk=${pk.size}B ts=$commitTs)"
}

/**
 * top-k 堆。堆顶是**当前第 k 名**（也就是最差的那个），
 * 于是剪枝阈值 = `堆满 ? 堆顶分数 : 0`。
 *
 * 淘汰规则：分数低者出局；分数相同时 **docId 大者出局**，
 * 因此最终结果在分数并列时按 docId 升序——这个顺序必须与 WAND 的剪枝方向一致
 * （见 [WandSearcher] 的说明），否则会漏掉并列项。
 */
class TopKHeap(val k: Int) {

    class Entry(val docKey: Long, val docId: Int, val score: Double)

    private val pq = java.util.PriorityQueue<Entry>(minOf(maxOf(1, k), 4096)) { a, b ->
        val c = a.score.compareTo(b.score)
        if (c != 0) c else -a.docKey.compareTo(b.docKey)   // 分数升序；同分时 docKey 降序（大者先出局）
    }

    val size: Int get() = pq.size
    val full: Boolean get() = pq.size >= k
    val threshold: Double get() = if (full) pq.peek().score else 0.0

    /** 尝试入堆。返回 true 表示确实进入了 top-k。 */
    fun offer(entry: Entry): Boolean {
        if (k <= 0) return false
        if (pq.size < k) { pq.add(entry); return true }
        val worst = pq.peek()
        val better = entry.score > worst.score ||
            (entry.score == worst.score && entry.docKey < worst.docKey)
        if (!better) return false
        pq.poll()
        pq.add(entry)
        return true
    }

    /** 按 (分数降序, docKey 升序) 返回。 */
    fun toSortedList(): List<Entry> = pq.sortedWith { a, b ->
        val c = b.score.compareTo(a.score)
        if (c != 0) c else a.docKey.compareTo(b.docKey)
    }
}
