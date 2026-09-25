package com.cloudea.yangbase.index.fulltext

/**
 * 已经解析成**具体词条**、并算好**全局 idf** 的检索项。
 *
 * idf 必须跨段算（`df` 对全部参与检索的段求和），否则各段分数不可比，
 * top-k 会偏向「df 小」的段而不是「更相关」的段。
 */
internal class TermRef(val term: String, val idf: Double)

/** 「一个段」在规划阶段的句柄。 */
internal class SegmentSpec(val ordinal: Int, val reader: SegmentReader) {
    val docCount: Int get() = reader.docCount
}

/**
 * 一个段上的执行计划。四组词条语义与 Lucene 的 BooleanQuery 一致：
 *  - [must]：**合取项列表**，每一项是一个析取（`term*` 展开后就是这一形态），
 *    候选必须满足所有项；
 *  - [phrases]：每项的词条必须**按词位相邻**出现（slop=0）；
 *  - [optional]：非空时要求至少命中一个（自然语言模式就是这一形态）；
 *  - [mustNot]：一个都不能命中。
 *
 * 游标按词条**缓存**（同一词条在整个段内只有一个游标）：
 * 既省掉重复的 FST 查找，也让「必需 + 短语」共享同一个倒排流。
 */
internal class SegmentGroup(
    val spec: SegmentSpec,
    val matchAll: Boolean,
    val must: List<List<TermRef>>,
    val phrases: List<List<TermRef>>,
    val optional: List<TermRef>,
    val mustNot: List<TermRef>,
    private val idf: Map<String, Double>,
    private val params: Bm25Params,
    private val avgDocLength: Double,
) {
    val ordinal: Int get() = spec.ordinal
    val reader: SegmentReader get() = spec.reader

    private val cursors = HashMap<String, TermCursor?>()

    /** 词条在本段的游标；本段没有该词条时返回 null（0 IO，只查常驻的 FST）。 */
    suspend fun cursor(term: String): TermCursor? = cursors.getOrPut(term) { open(term) }

    private suspend fun open(term: String): TermCursor? {
        val ordinal = reader.ordinalOf(term) ?: return null
        val table = reader.termTable()
        val rec = table.terms[ordinal]
        val start = reader.blockStart(rec)
        return TermCursor(
            segOrdinal = spec.ordinal,
            reader = reader,
            term = term,
            record = rec,
            blockRecords = table.blocks.subList(start, start + rec.blockCount),
            idf = idf[term] ?: 0.0,
            params = params,
            avgDocLength = avgDocLength,
        )
    }

    /**
     * 短语是否在该文档里按词位相邻出现（slop=0）。
     *
     * 段没有词位信息时必须**报错**：把它当成「不相邻」会让短语查询静默返回空集，
     * 那是比失败更糟的结果。
     */
    suspend fun phraseAdjacent(phrase: List<TermRef>, doc: Int): Boolean {
        if (phrase.isEmpty()) return true
        if (!reader.meta.hasPositions) {
            throw IllegalStateException(
                "段 ${reader.segId} 未记录词位（hasPositions=false），无法执行短语查询；" +
                    "请重建该索引，或把短语改成 AllOf"
            )
        }
        val lists = ArrayList<IntArray>(phrase.size)
        for (t in phrase) {
            val c = cursor(t.term) ?: return false
            if (c.docId != doc) {
                if (c.docId > doc) return false
                c.seekTo(ordinal, doc)
                if (c.docId != doc) return false
            }
            lists.add(c.positions ?: return false)
        }
        // 词位数组升序：p ∈ L0 且 p+1 ∈ L1 … 同时成立才算相邻
        for (p in lists[0]) {
            var ok = true
            for (k in 1 until lists.size) {
                if (lists[k].binarySearch(p + k) < 0) { ok = false; break }
            }
            if (ok) return true
        }
        return false
    }
}

/**
 * 「一个合取/析取子句的全部游标」按 docId 升序产出候选文档流。
 *
 * 产出 [doc] 时**游标仍停在命中 [doc] 的位置上**（不动），下一次 [advance] 才把
 * 停在 [doc] 上的游标一起推进。这条「先产出、后推进」的次序是**语义要求**，
 * 不是风格问题：调用方要用 `contains(cur, seg, doc)`（即 `cur.docId == doc`）判断
 * 「本子句是否命中该文档」、并用 `phraseAdjacent` 读游标上的词位。若在产出前就推进，
 * 调用方看到的游标已经越过了候选文档，于是**所有交集查询（`+词` / 短语 / `And`）
 * 静默返回空集**——不报错、只是查不到。
 *
 * 另外两条：首次 [advance] 必须先把游标定位（`docId == -1` 表示「尚未定位」，
 * 不是「已耗尽」）；同一个 docId 由多个词条命中时只产出一次（`lastYield` 去重）。
 */
internal class ClauseCursor(private val cursors: List<TermCursor>, private val segOrdinal: Int) {

    private val live = cursors.toMutableList()
    private var lastYield = -1
    private var started = false

    /** 当前候选 docId；-1 表示已耗尽。调用方需先 [advance] 一次。 */
    var doc: Int = -1
        private set

    val cursorCount: Int get() = live.size

    suspend fun advance() {
        if (!started) {
            started = true
            for (c in live) c.advance()
        }
        while (true) {
            live.removeAll { it.exhausted }
            if (live.isEmpty()) { doc = -1; return }
            var min = Int.MAX_VALUE
            for (c in live) if (c.docId < min) min = c.docId
            if (min != lastYield) {
                lastYield = min
                doc = min
                return
            }
            // 上一次已经产出过这个 docId：把停在它上面的游标全部推进，再看下一个候选
            for (c in live) if (c.docId == min) c.advance()
        }
    }

    override fun toString(): String = "ClauseCursor(seg=$segOrdinal 词条数=${cursors.size} @$doc)"
}

/** 规划结果。[isWandable] 为真时可以走（也才该走）block-max WAND。 */
internal class PlannedQuery(val segments: List<SegmentGroup>, val isWandable: Boolean)

/**
 * 查询规划：`TextQuery`（与字段无关的表达式）→ 每段的「具体词条 + 全局 idf」。
 *
 * 三件事：
 *  1. **前缀展开**：逐段查 FST（0 IO，`terms.fst` 常驻）。段之间的词条集合不同，
 *     展开必须分段做——这也顺便让「某个段没有该词条」变成 0 IO 的剪枝。
 *  2. **全局 idf**：`df` 对全部段求和后再算，保证跨段分数可比。
 *  3. **路径判定**：[PlannedQuery.isWandable] 只在「纯 OR」时成立；
 *     只要出现必须项/短语/排除项就退回交集扫描（见 `FullTextIndex.andSearch` 的说明）。
 */
internal class QueryPlanner(
    private val analyzer: Analyzer,
    private val params: Bm25Params,
    private val stats: SearchStats,
    private val maxPrefixExpansion: Int,
) {

    private class Leaf(
        val matchAll: Boolean,
        val required: List<List<String>>,
        val phrases: List<List<String>>,
        val optional: List<String>,
        val mustNot: List<String>,
    )

    suspend fun plan(query: TextQuery, specs: List<SegmentSpec>): PlannedQuery {
        val drafts = ArrayList<Pair<SegmentSpec, Leaf>>(specs.size)
        val df = HashMap<String, Int>()
        for (spec in specs) {
            val leaf = draft(query, spec)
            drafts.add(spec to leaf)
            for (t in allTerms(leaf)) df[t] = (df[t] ?: 0) + dfIn(spec, t)
        }
        val idf = HashMap<String, Double>(df.size * 2)
        for ((t, d) in df) idf[t] = params.idf(d.toLong(), stats.docCount)

        val groups = drafts.map { (spec, leaf) ->
            SegmentGroup(
                spec = spec,
                matchAll = leaf.matchAll,
                must = leaf.required.map { clause -> clause.map { TermRef(it, idf[it] ?: 0.0) } },
                phrases = leaf.phrases.map { p -> p.map { TermRef(it, idf[it] ?: 0.0) } },
                optional = leaf.optional.map { TermRef(it, idf[it] ?: 0.0) },
                mustNot = leaf.mustNot.map { TermRef(it, idf[it] ?: 0.0) },
                idf = idf,
                params = params,
                avgDocLength = stats.avgDocLength,
            )
        }
        val wandable = drafts.all { (_, d) ->
            !d.matchAll && d.required.isEmpty() && d.phrases.isEmpty() &&
                d.mustNot.isEmpty() && d.optional.isNotEmpty()
        }
        return PlannedQuery(groups, wandable)
    }

    // ── 表达式 → 词条 ───────────────────────────────────────────────

    private suspend fun draft(query: TextQuery, spec: SegmentSpec): Leaf = when (query) {
        is TextQuery.MatchAll -> Leaf(true, emptyList(), emptyList(), emptyList(), emptyList())

        is TextQuery.Boolean -> {
            val required = ArrayList<List<String>>()
            val phrases = ArrayList<List<String>>()
            val optional = ArrayList<String>()
            val not = ArrayList<String>()
            for (c in query.must) addConjunct(c, spec, required, phrases)
            for (c in query.should) addDisjunct(c, spec, optional)
            for (c in query.mustNot) addDisjunct(c, spec, not)
            Leaf(false, required, phrases, optional, not)
        }

        is TextQuery.Term -> Leaf(false, listOf(termsOf(query, spec)), emptyList(), emptyList(), emptyList())
        is TextQuery.AllOf ->
            // 全部词条都命中：拆成若干单元素合取项，驱动词条才有「所有候选都含它」的前提
            Leaf(false, termsOf(query, spec).distinct().map { listOf(it) }, emptyList(), emptyList(), emptyList())
        is TextQuery.Phrase -> Leaf(false, emptyList(), listOf(termsOf(query, spec)), emptyList(), emptyList())
        // AnyOf / Prefix 是纯析取 → optional，于是自然语言查询天然可走 WAND
        is TextQuery.AnyOf -> Leaf(false, emptyList(), emptyList(), termsOf(query, spec).distinct(), emptyList())
        is TextQuery.Prefix -> Leaf(false, emptyList(), emptyList(), termsOf(query, spec), emptyList())
    }

    /** 合取项：本项必须被满足。多词条时本项内部是**析取**。 */
    private suspend fun addConjunct(
        q: TextQuery,
        spec: SegmentSpec,
        required: MutableList<List<String>>,
        phrases: MutableList<List<String>>,
    ) {
        when (q) {
            is TextQuery.Term, is TextQuery.AnyOf, is TextQuery.AllOf ->
                required.add(termsOf(q, spec).distinct())
            is TextQuery.Prefix -> required.add(termsOf(q, spec))
            is TextQuery.Phrase -> phrases.add(termsOf(q, spec))
            // MatchAll 在 must 里是恒真约束：什么都不加
            is TextQuery.MatchAll -> Unit
            is TextQuery.Boolean -> throw IllegalArgumentException("Boolean 里不能再嵌 Boolean（构造时已禁止）")
        }
    }

    /** 析取项：命中其中任意一个即可。 */
    private suspend fun addDisjunct(q: TextQuery, spec: SegmentSpec, out: MutableList<String>) {
        out.addAll(termsOf(q, spec))
    }

    /** 叶子 → 具体词条。短语**保留重复词条**（`very very good` 需要三次命中 + 相邻）。 */
    private suspend fun termsOf(leaf: TextQuery, spec: SegmentSpec): List<String> = when (leaf) {
        is TextQuery.Term -> listOfNotNull(singleTerm(leaf.text))
        is TextQuery.AnyOf -> analyzer.analyze(leaf.text).map { it.term }
        is TextQuery.AllOf -> analyzer.analyze(leaf.text).map { it.term }
        is TextQuery.Phrase -> analyzer.analyze(leaf.text).map { it.term }
        is TextQuery.Prefix -> expandPrefix(leaf.text, spec)
        is TextQuery.MatchAll -> emptyList()
        is TextQuery.Boolean -> throw IllegalArgumentException("Boolean 已被分解，不会走到这里")
    }

    /**
     * 单词语义：文本必须**恰好**切成一个词。
     * 切出 0 个（停用词/过短）→ 该词条不存在，返回 null（查询结果为空，不是错误）。
     * 切出多个 → 报错（否则「Term('new york') 悄悄只查了 new」是最难查的一类 bug）。
     */
    private fun singleTerm(text: String): String? {
        val toks = analyzer.analyze(text)
        return when {
            toks.size == 1 -> toks[0].term
            toks.isEmpty() -> null
            else -> throw IllegalArgumentException(
                "Term 查询的文本必须恰好切成一个词：'$text' 切成了 " +
                    "${toks.size} 个（${toks.map { it.term }}）；请改用 AnyOf / AllOf / Phrase"
            )
        }
    }

    private suspend fun expandPrefix(raw: String, spec: SegmentSpec): List<String> {
        val key = prefixKey(raw)
        val (terms, truncated) = spec.reader.termsWithPrefix(key, maxPrefixExpansion)
        if (truncated) {
            throw IllegalStateException(
                "前缀 '$raw' 展开超过 $maxPrefixExpansion 个词条；请缩小前缀。" +
                    "（静默截断会让结果集看起来「就是这样」，因此这里直接失败）"
            )
        }
        return terms.map { it.first }
    }

    /** 前缀文本也要过分析器（`Data*` 要变成 `data*`）；分析器丢光了（单个 CJK 字）就按字面小写。 */
    private fun prefixKey(raw: String): String {
        val toks = analyzer.analyze(raw)
        return when {
            toks.size == 1 -> toks[0].term
            toks.isEmpty() -> raw.lowercase()
            else -> throw IllegalArgumentException(
                "前缀查询的文本必须切成一个前缀：'$raw' 切成了 ${toks.map { it.term }}"
            )
        }
    }

    /** 本段内该词条的文档频率；本段没有该词条就是 0（0 IO，只查 FST 与常驻的 term 记录）。 */
    private suspend fun dfIn(spec: SegmentSpec, term: String): Int {
        val ordinal = spec.reader.ordinalOf(term) ?: return 0
        return spec.reader.termTable().terms[ordinal].df
    }

    private fun allTerms(leaf: Leaf): Set<String> {
        val out = HashSet<String>()
        for (c in leaf.required) out.addAll(c)
        for (p in leaf.phrases) out.addAll(p)
        out.addAll(leaf.optional)
        out.addAll(leaf.mustNot)
        return out
    }
}
