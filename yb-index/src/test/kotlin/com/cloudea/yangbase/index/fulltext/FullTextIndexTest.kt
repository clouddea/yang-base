package com.cloudea.yangbase.index.fulltext

import com.cloudea.yangbase.common.CorruptDataException
import com.cloudea.yangbase.common.IndexId
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.index.IndexKeys
import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.ObjectStoreCapabilities
import com.cloudea.yangbase.storage.BlockCache
import kotlinx.coroutines.test.runTest
import java.nio.ByteBuffer
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

private const val FIELD = "body"
private val INDEX_ID = IndexId(9101)

private fun pkBytes(id: Long): ByteArray = ByteBuffer.allocate(8).putLong(id).array()

private fun pkOf(pk: ByteArray): Long = ByteBuffer.wrap(pk).long

/** 语料里的一行：`(主键, commit_ts, 文本)`。 */
private typealias Row = Triple<Long, Long, String>

/**
 * 段式倒排索引的正确性（设计文档 §8.6）。
 *
 * 断言的**方向**刻意选成这样：所有「打分 / top-k / 剪枝」的结论都拿一个
 * **暴力参照实现**（[Oracle]：同一份语料、同一个分析器、全量算 BM25）逐条对比，
 * 而不是只看几个样例。WAND 是近似会**静默漏结果**的那类组件（漏了不报错、
 * 只是「少了几行」），只有与暴力结果等价才算对。
 *
 * 请求数断言配合 `CountingObjectStore`，把 §14.2 的 IO 预算变成可执行的门槛。
 */
class FullTextIndexTest {

    // ── 夹具 ────────────────────────────────────────────────────────

    private class Fixture(
        val counting: CountingObjectStore,
        val cache: BlockCache = BlockCache(16L * 1024 * 1024),
        val analyzer: Analyzer = StandardAnalyzer(),
        val params: Bm25Params = Bm25Params(),
        val blockDocs: Int = SegmentBuilder.DEFAULT_BLOCK_DOCS,
        val maxPrefixExpansion: Int = 1024,
        val maxDocsPerSegment: Int = SegmentBuilder.DEFAULT_MAX_DOCS,
    ) {
        val index = FullTextIndex(
            store = counting,
            cache = cache,
            indexId = INDEX_ID,
            field = FIELD,
            analyzer = analyzer,
            params = params,
            blockDocs = blockDocs,
            maxPrefixExpansion = maxPrefixExpansion,
            maxDocsPerSegment = maxDocsPerSegment,
        )

        /** 一批文档 = 一个段（与调用方「攒够一批切一个段」的用法一致）。 */
        suspend fun indexBatch(rows: List<Row>): SegmentRef =
            index.indexDocuments(rows.map { IndexDoc(pkBytes(it.first), it.second, it.third) })

        suspend fun search(q: TextQuery, topK: Int = 10, readTs: Long = Long.MAX_VALUE): FullTextSearchResult =
            index.search(q, topK, ReadTs(readTs))

        suspend fun keys(q: TextQuery, topK: Int = 10, readTs: Long = Long.MAX_VALUE): List<Long> =
            search(q, topK, readTs).hits.map { pkOf(it.pk) }

        /**
         * 模拟**另一个进程 / 另一个 CN** 来读同一个索引：对象存储不变，
         * 内存里的一切（`BlockCache`、`FullTextIndex` 的 manifest 与段读取器缓存）全部作废。
         *
         * 冷启动的请求数只能这样测。同一个进程里刚建完段再检索，写者已经把段对象
         * 按**读取器的键**放进了 [BlockCache]（见 [SegmentBuilder.put]），
         * 那条路径的账是另一回事（也是 P4 要求对齐两套键的原因）。
         */
        fun asFreshReader(): FullTextIndex = FullTextIndex(
            store = counting,
            cache = BlockCache(16L * 1024 * 1024),
            indexId = INDEX_ID,
            field = FIELD,
            analyzer = analyzer,
            params = params,
            blockDocs = blockDocs,
            maxPrefixExpansion = maxPrefixExpansion,
            maxDocsPerSegment = maxDocsPerSegment,
        )
    }

    private fun fixture(
        analyzer: Analyzer = StandardAnalyzer(),
        params: Bm25Params = Bm25Params(),
        blockDocs: Int = SegmentBuilder.DEFAULT_BLOCK_DOCS,
        maxPrefixExpansion: Int = 1024,
        maxDocsPerSegment: Int = SegmentBuilder.DEFAULT_MAX_DOCS,
        store: ObjectStore = MemoryObjectStore(),
    ) = Fixture(
        counting = CountingObjectStore(store, recordKeys = true),
        analyzer = analyzer,
        params = params,
        blockDocs = blockDocs,
        maxPrefixExpansion = maxPrefixExpansion,
        maxDocsPerSegment = maxDocsPerSegment,
    )

    // ── 暴力参照实现 ─────────────────────────────────────────────────

    /**
     * 暴力参照：把每批文档当成一个段，与实现**用同一套规则**算分。
     *
     * 规则必须逐条对齐，否则比对的是两套语义：
     *  - `df` 对**可见段**求和、`N` = 可见段的文档数、`avgdl` 用可见段的 token 总数；
     *  - 段的可见性由 `minTs > read_ts` 整段裁掉（段内文档再按 `commit_ts <= read_ts` 逐条过滤）；
     *  - 分数 = 每段上「命中的去重词条」的 BM25 之和（游标按词条共享，所以是去重后的）。
     */
    private class Oracle(private val analyzer: Analyzer, private val params: Bm25Params) {

        private class D(
            val key: Long,
            val commitTs: Long,
            val dl: Int,
            val tf: Map<String, Int>,
            val pos: Map<String, IntArray>,
        )

        private class Seg(val docs: List<D>) {
            val minTs: Long = docs.minOf { it.commitTs }
            val maxTs: Long = docs.maxOf { it.commitTs }
            val tokens: Long = docs.sumOf { it.dl }.toLong()
        }

        private val segments = ArrayList<Seg>()

        fun batch(rows: List<Row>) {
            segments.add(
                Seg(
                    rows.map { (key, ts, text) ->
                        val toks = analyzer.analyze(text)
                        val tf = HashMap<String, Int>()
                        val pos = HashMap<String, MutableList<Int>>()
                        for (t in toks) {
                            tf[t.term] = (tf[t.term] ?: 0) + 1
                            pos.getOrPut(t.term) { ArrayList() }.add(t.position)
                        }
                        D(key, ts, toks.size, tf, pos.mapValues { it.value.toIntArray() })
                    }
                )
            )
        }

        private fun termsOf(text: String): List<String> = analyzer.analyze(text).map { it.term }

        private fun visible(readTs: Long): List<D> =
            segments.filter { it.minTs <= readTs }.flatMap { it.docs }.filter { it.commitTs <= readTs }

        private fun idf(vis: List<D>, terms: List<String>): Pair<Map<String, Double>, Double> {
            val n = vis.size.toLong()
            val avg = if (vis.isEmpty()) 0.0 else vis.sumOf { it.dl }.toDouble() / vis.size
            val map = terms.associateWith { t ->
                params.idf(vis.count { d -> (d.tf[t] ?: 0) > 0 }.toLong(), n)
            }
            return map to avg
        }

        private fun rank(
            vis: List<D>,
            scoreTerms: List<String>,
            topK: Int,
            require: (D) -> Boolean,
            forbid: List<String> = emptyList(),
        ): List<Pair<Long, Double>> {
            val (idf, avg) = idf(vis, scoreTerms)
            return vis
                .filter { d -> forbid.none { (d.tf[it] ?: 0) > 0 } && require(d) }
                .mapNotNull { d ->
                    val s = scoreTerms.distinct().sumOf { t ->
                        val f = d.tf[t] ?: 0
                        if (f == 0) 0.0 else params.score(f, d.dl, idf[t] ?: 0.0, avg)
                    }
                    if (s > 0.0) d.key to s else null
                }
                .sortedWith(compareByDescending<Pair<Long, Double>> { it.second }.thenBy { it.first })
                .take(topK)
        }

        /** 自然语言（OR）：命中任一词条即可，分数为命中词之和。 */
        fun rankAnyOf(text: String, topK: Int = Int.MAX_VALUE, readTs: Long = Long.MAX_VALUE, deleted: Set<Long> = emptySet()) =
            rank(visible(readTs).filter { it.key !in deleted }, termsOf(text), topK, require = { true })

        /** 全部词条必须命中。 */
        fun rankAllOf(
            text: String,
            topK: Int = Int.MAX_VALUE,
            readTs: Long = Long.MAX_VALUE,
            deleted: Set<Long> = emptySet(),
        ): List<Pair<Long, Double>> {
            val terms = termsOf(text).distinct()
            return rank(
                visible(readTs).filter { it.key !in deleted }, terms, topK,
                require = { d -> terms.all { (d.tf[it] ?: 0) > 0 } },
            )
        }

        /** 短语：词位必须严格相邻（slop=0）。 */
        fun rankPhrase(
            text: String,
            topK: Int = Int.MAX_VALUE,
            readTs: Long = Long.MAX_VALUE,
            deleted: Set<Long> = emptySet(),
        ): List<Pair<Long, Double>> {
            val terms = termsOf(text)
            return rank(
                visible(readTs).filter { it.key !in deleted }, terms, topK,
                require = { d -> adjacent(d, terms) },
            )
        }

        /** 布尔：must 全中、mustNot 一个都不中、should 至少中一个。 */
        fun rankBoolean(
            must: List<String>,
            mustNot: List<String> = emptyList(),
            should: List<String> = emptyList(),
            topK: Int = Int.MAX_VALUE,
            readTs: Long = Long.MAX_VALUE,
            deleted: Set<Long> = emptySet(),
        ): List<Pair<Long, Double>> {
            val mustTerms = must.flatMap { termsOf(it) }
            val notTerms = mustNot.flatMap { termsOf(it) }
            val shouldTerms = should.flatMap { termsOf(it) }
            val scoreTerms = (mustTerms + shouldTerms).distinct()
            return rank(
                visible(readTs).filter { it.key !in deleted }, scoreTerms, topK,
                require = { d ->
                    mustTerms.all { (d.tf[it] ?: 0) > 0 } &&
                        (shouldTerms.isEmpty() || shouldTerms.any { (d.tf[it] ?: 0) > 0 })
                },
                forbid = notTerms,
            )
        }

        private fun adjacent(d: D, terms: List<String>): Boolean {
            if (terms.isEmpty()) return false
            val lists = terms.map { d.pos[it] ?: return false }
            outer@ for (p in lists[0]) {
                for (k in 1 until lists.size) if (lists[k].binarySearch(p + k) < 0) continue@outer
                return true
            }
            return false
        }
    }

    /** 逐条比对 (主键, 分数)：分数用 1e-9 容差，顺序必须一致。 */
    private fun assertSameRanking(
        expected: List<Pair<Long, Double>>,
        actual: List<Hit>,
        what: String,
    ) {
        assertEquals(
            expected.map { it.first },
            actual.map { pkOf(it.pk) },
            "$what：命中集合/顺序不一致",
        )
        for ((i, e) in expected.withIndex()) {
            val a = actual[i].score
            assertTrue(
                kotlin.math.abs(e.second - a) <= 1e-9 * kotlin.math.max(1.0, kotlin.math.abs(e.second)),
                "$what：第 $i 条分数不一致 expected=${e.second} actual=$a",
            )
        }
    }

    // ── top-k 与暴力参照等价 ────────────────────────────────────────

    /** 语料：24 个词、每篇 2–8 个词（有重复，制造 tf > 1 与长度差异）。 */
    private fun corpus(count: Int, vocab: Int, seed: Int, perDoc: IntRange = 2..8): List<Row> {
        val rnd = Random(seed)
        return (1..count).map { id ->
            val n = perDoc.first + rnd.nextInt(perDoc.last - perDoc.first + 1)
            val text = (1..n).joinToString(" ") { "w" + rnd.nextInt(vocab) }
            Triple(id.toLong(), id.toLong(), text)
        }
    }

    @Test
    fun `OR 检索的 top-k 与暴力打分逐条一致`() = runTest {
        val rows = corpus(count = 60, vocab = 24, seed = 20260924)
        val fx = fixture()
        fx.indexBatch(rows)
        val oracle = Oracle(StandardAnalyzer(), Bm25Params()).apply { batch(rows) }

        val queries = listOf("w1", "w1 w2", "w3 w7 w11", "w0 w5 w9 w13 w17", "w23", "w1 w1 w1")
        for (q in queries) {
            for (k in listOf(1, 5, 1000)) {
                val res = fx.search(TextQuery.AnyOf(FIELD, q), topK = k)
                assertSameRanking(oracle.rankAnyOf(q, topK = k), res.hits, "AnyOf('$q') topK=$k")
            }
        }
    }

    @Test
    fun `同分文档按主键升序且不因 top-k 截断而改变`() = runTest {
        // 完全相同的文本 ⇒ 完全相同的分数 ⇒ 顺序只能由主键决定
        val rows = (1..12).map { Triple(it.toLong(), 1000L, "alpha beta") }
        val fx = fixture()
        fx.indexBatch(rows)
        val all = fx.keys(TextQuery.AnyOf(FIELD, "alpha"), topK = 12)
        assertEquals((1L..12L).toList(), all, "同分应按主键升序")
        assertEquals(listOf(1L, 2L, 3L), fx.keys(TextQuery.AnyOf(FIELD, "alpha"), topK = 3))
        assertEquals(listOf(1L, 2L, 3L), fx.keys(TextQuery.AnyOf(FIELD, "beta"), topK = 3))
    }

    @Test
    fun `WAND 在块级上界剪枝下应跳过绝大多数候选`() = runTest {
        // 1000 篇都含 common；rare 每 37 篇出现一次。topK=1 的答案是「既含 rare 又含 common」的文档，
        // 于是 common 的大部分倒排块都能被块级上界剪掉，不必读、也不必打分。
        val rows = (1..1000).map { id ->
            val text = if (id % 37 == 0) "common rare" else "common filler"
            Triple(id.toLong(), id.toLong(), text)
        }
        val fx = fixture()
        fx.indexBatch(rows)
        val oracle = Oracle(StandardAnalyzer(), Bm25Params()).apply { batch(rows) }

        val res = fx.search(TextQuery.AnyOf(FIELD, "common rare"), topK = 1)
        assertSameRanking(oracle.rankAnyOf("common rare", topK = 1), res.hits, "WAND topK=1")

        val total = 1000L
        assertTrue(
            res.stats.scoredCandidates < total / 4,
            "块级上界剪枝应显著减少打分候选：实际 ${res.stats.scoredCandidates}/$total",
        )
        assertEquals(2, res.stats.cursors, "两个词条 ⇒ 两个游标")
        // 剪枝后仍然必须与暴力结果一致（上面已比对），这里再确认大 top-k 也一样
        val big = fx.search(TextQuery.AnyOf(FIELD, "common rare"), topK = 100)
        assertSameRanking(oracle.rankAnyOf("common rare", topK = 100), big.hits, "WAND topK=100")
    }

    @Test
    fun `top-k 大于文档数时返回全部命中`() = runTest {
        val rows = corpus(count = 20, vocab = 8, seed = 7)
        val fx = fixture()
        fx.indexBatch(rows)
        val oracle = Oracle(StandardAnalyzer(), Bm25Params()).apply { batch(rows) }
        val res = fx.search(TextQuery.AnyOf(FIELD, "w1 w2"), topK = 1000)
        assertSameRanking(oracle.rankAnyOf("w1 w2"), res.hits, "topK 超限")
    }

    // ── 跨段 / 时间 / MVCC ──────────────────────────────────────────

    @Test
    fun `跨段的 df 与 avgdl 必须合并后打分`() = runTest {
        val a = listOf(Row(1, 1000, "alpha beta"), Row(2, 1000, "alpha"))
        val b = listOf(Row(3, 2000, "alpha alpha gamma"), Row(4, 2000, "gamma"))
        val fx = fixture()
        fx.indexBatch(a)
        fx.indexBatch(b)
        val oracle = Oracle(StandardAnalyzer(), Bm25Params()).apply { batch(a); batch(b) }

        val res = fx.search(TextQuery.AnyOf(FIELD, "alpha gamma"), topK = 10)
        assertSameRanking(oracle.rankAnyOf("alpha gamma", topK = 10), res.hits, "跨段打分")
        assertEquals(2, res.stats.segmentsTotal)
        // 两段的分数必须可比：仅在第 2 段出现的 gamma 不能因为「df 小」而拿到假高分
        assertEquals(setOf(1L, 2L, 3L, 4L), res.hits.map { pkOf(it.pk) }.toSet())
    }

    @Test
    fun `段级时间裁剪是 0 次数据读取`() = runTest {
        val rows = corpus(count = 40, vocab = 10, seed = 3).map { Row(it.first, 5000L, it.third) }
        val fx = fixture()
        fx.indexBatch(rows)
        fx.search(TextQuery.AnyOf(FIELD, "w1"))    // 预热：读 manifest / 段元信息
        fx.counting.reset()

        val res = fx.search(TextQuery.AnyOf(FIELD, "w1"), readTs = 4000)
        assertTrue(res.hits.isEmpty(), "minTs > read_ts 的段整体不可见")
        assertEquals(1, res.stats.segmentsPrunedByTs)
        assertEquals(0, res.stats.cursors)
        assertEquals(0L, fx.counting.count("get_range"), "整段被裁掉时不应读任何数据块")
        assertEquals(0L, fx.counting.count("get"), "整段被裁掉时连段内对象都不该读")
    }

    @Test
    fun `时间旅行 - 快照只看得到当时已提交的段`() = runTest {
        val old = listOf(Row(1, 1000, "shared marker"), Row(2, 1000, "shared other"))
        val new = listOf(Row(3, 2000, "shared marker"))
        val fx = fixture()
        fx.indexBatch(old)
        fx.indexBatch(new)
        val oracle = Oracle(StandardAnalyzer(), Bm25Params()).apply { batch(old); batch(new) }

        assertEquals(setOf(1L, 2L), fx.keys(TextQuery.AnyOf(FIELD, "shared"), readTs = 1500).toSet())
        assertEquals(listOf(1L), fx.keys(TextQuery.AnyOf(FIELD, "marker"), readTs = 1500), "marker 只在第 1 行里")
        assertEquals(listOf(1L, 3L), fx.keys(TextQuery.AnyOf(FIELD, "marker"), readTs = 2500).sorted())
        assertSameRanking(oracle.rankAnyOf("marker", readTs = 1500), fx.search(TextQuery.AnyOf(FIELD, "marker"), readTs = 1500).hits, "旧快照")
    }

    @Test
    fun `段跨越快照时逐文档校验 commit_ts`() = runTest {
        val rows = listOf(
            Row(1, 1000, "alpha"),
            Row(2, 2000, "alpha"),
            Row(3, 3000, "alpha"),
        )
        val fx = fixture()
        fx.indexBatch(rows)          // 一个段：minTs=1000, maxTs=3000
        val segId = fx.index.manifest()!!.segments.single().segId
        val doctsKey = IndexKeys.segmentFile(INDEX_ID, segId, IndexKeys.DOCTS_BIN)

        assertEquals(listOf(1L), fx.keys(TextQuery.AnyOf(FIELD, "alpha"), readTs = 1000))
        assertEquals(listOf(1L, 2L), fx.keys(TextQuery.AnyOf(FIELD, "alpha"), readTs = 2000))
        assertEquals(listOf(1L, 2L, 3L), fx.keys(TextQuery.AnyOf(FIELD, "alpha"), readTs = 3000))

        // 跨越快照的查询需要 docts.bin。这里必须换一个**冷读取器**来测：
        // 同一个进程里写者已经把段对象按读取器的键缓存过了，本地测不出读取器的行为。
        // 三次查询（两次真的跨越快照）合计只应读一次——解析后在读取器里常驻。
        val reader = fx.asFreshReader()
        suspend fun readerKeys(ts: Long): List<Long> =
            reader.search(TextQuery.AnyOf(FIELD, "alpha"), 10, ReadTs(ts)).hits.map { pkOf(it.pk) }
        assertEquals(listOf(1L), readerKeys(1000))
        assertEquals(listOf(1L, 2L), readerKeys(2000))
        assertEquals(listOf(1L, 2L, 3L), readerKeys(3000))
        val doctsGets = fx.counting.keysFor("get").filter { it.first == doctsKey }
        assertEquals(1, doctsGets.size, "docts.bin 至多读一次（解析后在读取器里常驻）")

        // 整段不可见（minTs > read_ts）：段级时间裁剪直接跳过该段 ⇒ 一次都不读
        val reader2 = fx.asFreshReader()
        fx.counting.reset()
        reader2.search(TextQuery.AnyOf(FIELD, "alpha"), 10, ReadTs(500))
        assertEquals(0, fx.counting.keysFor("get").count { it.first == doctsKey }, "整段不可见应 0 IO")

        // 整段可见：版本校验**不需要** docts，但命中行要给出精确的 commit_ts ⇒ 每段读一次，
        // 与命中数无关（3 个命中仍只读 1 次），解析后常驻（同段后续查询 0 次）。
        val reader3 = fx.asFreshReader()
        fx.counting.reset()
        val visible = reader3.search(TextQuery.AnyOf(FIELD, "alpha"), 10, ReadTs(Long.MAX_VALUE))
        assertEquals(3, visible.hits.size)
        assertEquals(
            1, fx.counting.keysFor("get").count { it.first == doctsKey },
            "commit_ts 是按段读（1 次），不是按命中读（3 次）",
        )
        fx.counting.reset()
        reader3.search(TextQuery.AnyOf(FIELD, "alpha"), 10, ReadTs(Long.MAX_VALUE))
        assertEquals(
            0, fx.counting.keysFor("get").count { it.first == doctsKey },
            "docts 解析后在读取器里常驻",
        )
    }

    @Test
    fun `删除按 commit_ts 精确生效 - read_ts 早于删除时旧版本仍可见`() = runTest {
        val rows = listOf(Row(1, 1000, "marker one"), Row(2, 1000, "marker two"), Row(3, 1000, "marker three"))
        val fx = fixture()
        fx.indexBatch(rows)
        assertEquals(listOf(1L, 2L, 3L), fx.keys(TextQuery.AnyOf(FIELD, "marker")))

        val deleted = fx.index.deleteDocuments(listOf(pkBytes(2)), commitTs = 3000)
        assertEquals(1, deleted, "应标记 1 条")

        assertEquals(listOf(1L, 2L, 3L), fx.keys(TextQuery.AnyOf(FIELD, "marker"), readTs = 2999), "read_ts < commit_ts 时删除尚未生效")
        assertEquals(listOf(1L, 3L), fx.keys(TextQuery.AnyOf(FIELD, "marker"), readTs = 3000), "commit_ts <= read_ts 即生效")
        assertEquals(listOf(1L, 3L), fx.keys(TextQuery.AnyOf(FIELD, "marker"), readTs = 5000))
        assertEquals(0, fx.keys(TextQuery.AnyOf(FIELD, "nonexistent")).size)
    }

    @Test
    fun `另一个写者提交的删除必须被读者看到`() = runTest {
        val store = MemoryObjectStore()
        val reader = FullTextIndex(store, BlockCache(), INDEX_ID, FIELD, StandardAnalyzer())
        val writer = FullTextIndex(store, BlockCache(), INDEX_ID, FIELD, StandardAnalyzer())
        writer.indexDocuments((1..3).map { IndexDoc(pkBytes(it.toLong()), 1000, "marker") })
        assertEquals(3, reader.search(TextQuery.AnyOf(FIELD, "marker")).hits.size)

        writer.deleteDocuments(listOf(pkBytes(2)), commitTs = 2000)
        // 读者的 manifest 缓存与「无删除」的删除集缓存都已过期：必须复核 CURRENT 的 etag
        assertEquals(
            2, reader.search(TextQuery.AnyOf(FIELD, "marker"), topK = 10).hits.size,
            "读者停在旧视图上会给出「已删除的行仍被检索到」的过期结果",
        )
    }

    @Test
    fun `覆盖写遮蔽旧版本 - 同一主键只留最新文本`() = runTest {
        val fx = fixture()
        fx.indexBatch(listOf(Row(1, 1000, "originalword here")))
        assertEquals(listOf(1L), fx.keys(TextQuery.AnyOf(FIELD, "originalword")))

        fx.indexBatch(listOf(Row(1, 2000, "replacementword here")))
        assertTrue(fx.keys(TextQuery.AnyOf(FIELD, "originalword")).isEmpty(), "旧版本必须被遮蔽")
        assertEquals(listOf(1L), fx.keys(TextQuery.AnyOf(FIELD, "replacementword")))
        assertEquals(1, fx.index.manifest()!!.deletesSeq.size, "只有承载旧版本的段需要删除集")

        // 时间旅行到覆盖写之前，旧版本仍然可见（段不可变 + 删除集带精确时间）
        assertEquals(listOf(1L), fx.keys(TextQuery.AnyOf(FIELD, "originalword"), readTs = 1500))
    }

    // ── 交集路径：AllOf / Phrase / Boolean ──────────────────────────

    @Test
    fun `AllOf 要求全部词条命中`() = runTest {
        val rows = listOf(
            Row(1, 1000, "alpha beta gamma"),
            Row(2, 1000, "alpha beta"),
            Row(3, 1000, "alpha"),
            Row(4, 1000, "gamma beta"),
        )
        val fx = fixture()
        fx.indexBatch(rows)
        val oracle = Oracle(StandardAnalyzer(), Bm25Params()).apply { batch(rows) }

        assertSameRanking(oracle.rankAllOf("alpha beta"), fx.search(TextQuery.AllOf(FIELD, "alpha beta"), topK = 10).hits, "AllOf 2 词")
        assertSameRanking(
            oracle.rankAllOf("alpha beta gamma"),
            fx.search(TextQuery.AllOf(FIELD, "alpha beta gamma"), topK = 10).hits,
            "AllOf 3 词",
        )
        assertEquals(listOf(1L, 2L), fx.keys(TextQuery.AllOf(FIELD, "alpha beta")).sorted())
        assertTrue(fx.keys(TextQuery.AllOf(FIELD, "alpha beta gamma")).sorted() == listOf(1L))
    }

    @Test
    fun `短语按词位相邻 - 词序不同与跨停用词都不命中`() = runTest {
        val rows = listOf(
            Row(1, 1000, "the quick brown fox"),
            Row(2, 1000, "brown quick fox"),
            Row(3, 1000, "quick the brown fox"),   // 停用词占词位 ⇒ 不相邻
            Row(4, 1000, "a quick brown bear"),    // 前导停用词不该影响
        )
        val fx = fixture()
        fx.indexBatch(rows)
        val oracle = Oracle(StandardAnalyzer(), Bm25Params()).apply { batch(rows) }

        val hits = fx.search(TextQuery.Phrase(FIELD, "quick brown"), topK = 10)
        assertSameRanking(oracle.rankPhrase("quick brown"), hits.hits, "短语 quick brown")
        assertEquals(listOf(1L, 4L), hits.hits.map { pkOf(it.pk) }.sorted(), "词序不同/跨停用词都不应命中")

        // 短语的两个词都命中但顺序相反：单看游标交集是 4 篇，短语语义只留 2 篇
        assertEquals(4, fx.keys(TextQuery.AllOf(FIELD, "quick brown")).size)
    }

    @Test
    fun `三段短语要求两次相邻`() = runTest {
        val rows = listOf(
            Row(1, 1000, "one two three"),
            Row(2, 1000, "one three two"),
            Row(3, 1000, "one two x three"),
        )
        val fx = fixture()
        fx.indexBatch(rows)
        val oracle = Oracle(StandardAnalyzer(), Bm25Params()).apply { batch(rows) }
        val hits = fx.search(TextQuery.Phrase(FIELD, "one two three"), topK = 10)
        assertSameRanking(oracle.rankPhrase("one two three"), hits.hits, "三段短语")
        assertEquals(listOf(1L), hits.hits.map { pkOf(it.pk) })
    }

    @Test
    fun `布尔模式的必须与排除`() = runTest {
        val rows = listOf(
            Row(1, 1000, "quick brown fox"),
            Row(2, 1000, "quick brown rabbit"),
            Row(3, 1000, "quick red fox"),
        )
        val fx = fixture()
        fx.indexBatch(rows)
        val oracle = Oracle(StandardAnalyzer(), Bm25Params()).apply { batch(rows) }

        val q = MatchAgainstParser.parse(FIELD, "+quick -rabbit", MatchMode.BOOLEAN)
        val hits = fx.search(q, topK = 10)
        assertSameRanking(
            oracle.rankBoolean(must = listOf("quick"), mustNot = listOf("rabbit")),
            hits.hits,
            "布尔 +必须 -排除",
        )
        assertEquals(listOf(1L, 3L), hits.hits.map { pkOf(it.pk) }.sorted(), "排除项必须真的排除")

        // 排除项把候选清空时必须是空集，而不是退化成「不排除」
        assertTrue(
            fx.keys(MatchAgainstParser.parse(FIELD, "+quick -fox -brown -red", MatchMode.BOOLEAN)).isEmpty(),
            "全部候选都被排除时应为空集",
        )
        // should + mustNot：命中任一 should 且不含排除词
        assertEquals(
            listOf(3L),
            fx.keys(MatchAgainstParser.parse(FIELD, "fox rabbit -brown", MatchMode.BOOLEAN)).sorted(),
            "第 1 行含 brown，应被排除",
        )
        // 纯排除（无 must/should）在解析阶段就应被拒绝
        assertFailsWith<IllegalArgumentException> { MatchAgainstParser.parse(FIELD, "-rabbit", MatchMode.BOOLEAN) }
    }

    @Test
    fun `布尔模式的短语查询`() = runTest {
        val rows = listOf(Row(1, 1000, "the quick brown fox"), Row(2, 1000, "brown quick fox"))
        val fx = fixture()
        fx.indexBatch(rows)
        val q = MatchAgainstParser.parse(FIELD, "\"quick brown\"", MatchMode.BOOLEAN)
        assertEquals(listOf(1L), fx.keys(q), "短语必须按词位相邻")
    }

    @Test
    fun `布尔模式的前缀查询`() = runTest {
        val rows = listOf(
            Row(1, 1000, "database systems"),
            Row(2, 1000, "datacenter power"),
            Row(3, 1000, "storage engine"),
            Row(4, 1000, "date range"),
        )
        val fx = fixture()
        fx.indexBatch(rows)
        val q = MatchAgainstParser.parse(FIELD, "data*", MatchMode.BOOLEAN)
        assertEquals(listOf(1L, 2L), fx.keys(q).sorted(), "前缀只应命中 data 开头，不应命中 date")
        assertSameRanking(
            Oracle(StandardAnalyzer(), Bm25Params()).apply { batch(rows) }.rankAnyOf("database datacenter"),
            fx.search(q, topK = 10).hits,
            "前缀展开后的打分",
        )
    }

    @Test
    fun `前缀展开超过上限时报错而不是静默截断`() = runTest {
        val rows = (1..12).map { Row(it.toLong(), 1000L, "pref" + ('a' + it - 1) + "x other") }
        val fx = fixture(maxPrefixExpansion = 4)
        fx.indexBatch(rows)
        val e = assertFailsWith<IllegalStateException> { fx.search(TextQuery.Prefix(FIELD, "pref")) }
        assertTrue(e.message!!.contains("展开"), "错误信息应说明前缀展开超限：${e.message}")

        // 上限之内正常
        assertEquals(1, fx.keys(TextQuery.Prefix(FIELD, "prefa")).size)
        assertTrue(fx.keys(TextQuery.Prefix(FIELD, "zzz")).isEmpty(), "展开为空的词条 → 空集，不是崩溃")
    }

    @Test
    fun `查询文本切不出词条时返回空集而不是崩溃`() = runTest {
        val fx = fixture()
        fx.indexBatch(listOf(Row(1, 1000, "alpha")))
        // 全部是停用词 / 前缀无展开 / 词条不存在：三条路径都必须给出「空集」这个正确答案
        assertTrue(fx.keys(TextQuery.AnyOf(FIELD, "the of and")).isEmpty())
        assertTrue(fx.keys(TextQuery.AnyOf(FIELD, "!!!")).isEmpty())
        assertTrue(fx.keys(TextQuery.Prefix(FIELD, "zzz")).isEmpty())
        assertTrue(fx.keys(TextQuery.Term(FIELD, "the")).isEmpty())
        // 段级裁剪计数应把这种情况记为「本段无词条」，而不是假装扫过
        assertEquals(1, fx.search(TextQuery.AnyOf(FIELD, "the")).stats.segmentsPrunedByTerms)
    }

    @Test
    fun `MatchAll 返回全部可见文档且分数为 0`() = runTest {
        val rows = listOf(Row(1, 1000, "alpha"), Row(2, 1000, "!!!"), Row(3, 2000, "beta"))
        val fx = fixture()
        fx.indexBatch(rows)
        val res = fx.search(TextQuery.MatchAll(FIELD), topK = 10)
        assertEquals(listOf(1L, 2L, 3L), res.hits.map { pkOf(it.pk) }, "MatchAll 应按段序/文档序返回全部")
        assertTrue(res.hits.all { it.score == 0.0 }, "MatchAll 不做打分")
        assertEquals(listOf(1L, 2L), fx.keys(TextQuery.MatchAll(FIELD), readTs = 1500))
    }

    @Test
    fun `没有词条的文档仍然可被 MatchAll 命中且不参与打分`() = runTest {
        val rows = listOf(Row(1, 1000, "!!!"), Row(2, 1000, "alpha"))
        val fx = fixture()
        fx.indexBatch(rows)
        assertEquals(listOf(1L, 2L), fx.keys(TextQuery.MatchAll(FIELD)))
        assertEquals(listOf(2L), fx.keys(TextQuery.AnyOf(FIELD, "alpha")))
        val stats = fx.index.stats()!!
        assertEquals(0.5, stats.avgFieldLength, 1e-9, "空文档的字段长度为 0，会拉低 avgdl")
    }

    // ── 参数校验 ────────────────────────────────────────────────────

    @Test
    fun `字段不一致与 topK 非法必须报错`() = runTest {
        val fx = fixture()
        fx.indexBatch(listOf(Row(1, 1000, "alpha")))
        assertFailsWith<IllegalArgumentException> { fx.search(TextQuery.AnyOf("title", "alpha")) }
        assertFailsWith<IllegalArgumentException> { fx.search(TextQuery.AnyOf(FIELD, "alpha"), topK = 0) }
    }

    @Test
    fun `Term 查询切成多个词时报错 - 静默只查一个词是最难查的 bug`() = runTest {
        val fx = fixture()
        fx.indexBatch(listOf(Row(1, 1000, "new york city")))
        val e = assertFailsWith<IllegalArgumentException> { fx.search(TextQuery.Term(FIELD, "new york")) }
        assertTrue(e.message!!.contains("AnyOf"), "错误信息应给出替代写法：${e.message}")
        assertEquals(listOf(1L), fx.keys(TextQuery.Term(FIELD, "york")))
    }

    @Test
    fun `分析器不一致的段必须被拒绝而不是静默返回空集`() = runTest {
        val store = MemoryObjectStore()
        val standard = FullTextIndex(store, BlockCache(), INDEX_ID, FIELD, StandardAnalyzer())
        standard.indexDocuments(listOf(IndexDoc(pkBytes(1), 1000, "alpha beta")))

        val ngram = FullTextIndex(store, BlockCache(), INDEX_ID, FIELD, ChineseNgramAnalyzer())
        assertFailsWith<CorruptDataException> { ngram.search(TextQuery.AnyOf(FIELD, "alpha")) }
        // 中文索引里查英文词条同样必须报错（两者永不相交，静默空集是错误答案）
        assertFailsWith<CorruptDataException> { ngram.search(TextQuery.AnyOf(FIELD, "阿尔法")) }
    }

    @Test
    fun `没有索引时检索返回空集而不是报错`() = runTest {
        val fx = fixture()
        val res = fx.search(TextQuery.AnyOf(FIELD, "alpha"))
        assertTrue(res.hits.isEmpty())
        assertEquals(0, res.stats.segmentsTotal)
        assertNull(fx.index.stats())
        assertEquals(0, fx.index.deleteDocuments(listOf(pkBytes(1)), 100), "没有索引时删除是幂等的空操作")
    }

    @Test
    fun `段没有词位时短语必须报错 - 把它当成不相邻会静默返回空集`() = runTest {
        val store = MemoryObjectStore()
        val cache = BlockCache(4L * 1024 * 1024)
        val analyzer = StandardAnalyzer()
        // 直接建一个 hasPositions=false 的段（省下块内词位字节的取舍，见 IndexKeys 的说明）
        val builder = SegmentBuilder(segId = "p0", field = FIELD, analyzerName = analyzer.name, hasPositions = false)
        builder.addDocument(pkBytes(1), 1000, "quick brown fox", analyzer)
        builder.addDocument(pkBytes(2), 1000, "quick brown rabbit", analyzer)
        val meta = builder.write(store, cache, INDEX_ID)
        val ref = SegmentRef(
            segId = meta.segId, docCount = meta.docCount, minTs = meta.minTs, maxTs = meta.maxTs,
            termCount = meta.termCount, totalTokens = meta.totalTokens,
            bytes = meta.files.values.sumOf { it.bytes }, field = meta.field, analyzer = meta.analyzer,
        )
        val index = FullTextIndex(store, cache, INDEX_ID, FIELD, analyzer)
        index.commitManifest(IndexManifest(indexId = INDEX_ID.value, seq = 1, schemaVersion = 0, segments = listOf(ref)), expectedEtag = null)

        // 没有词位不影响普通检索（这是该项取舍成立的前提）
        assertEquals(listOf(1L, 2L), index.search(TextQuery.AnyOf(FIELD, "quick")).hits.map { pkOf(it.pk) }.sorted())
        val e = assertFailsWith<IllegalStateException> { index.search(TextQuery.Phrase(FIELD, "quick brown")) }
        assertTrue(e.message!!.contains("词位"), "错误信息应说明段缺少词位：${e.message}")
    }

    // ── 段管理：合并 / 统计 / 删除对象 ──────────────────────────────

    @Test
    fun `合并前后检索结果逐条一致且保留词位`() = runTest {
        val batches = listOf(
            corpus(count = 30, vocab = 12, seed = 11),
            corpus(count = 25, vocab = 12, seed = 12).map { Row(it.first + 100, it.second + 1000, it.third) },
            corpus(count = 20, vocab = 12, seed = 13).map { Row(it.first + 200, it.second + 2000, it.third) },
        )
        val fx = fixture()
        for (b in batches) fx.indexBatch(b)
        assertEquals(3, fx.index.stats()!!.segments)

        val queries = listOf(TextQuery.AnyOf(FIELD, "w1 w2"), TextQuery.AllOf(FIELD, "w1 w2"), TextQuery.AnyOf(FIELD, "w3"))
        val before = queries.map { fx.search(it, topK = 50) }
        // 合并里必须带上一组短语查询：合并是 K 路归并（不重新分词），词位不能丢
        val phraseBefore = fx.search(TextQuery.Phrase(FIELD, "w1 w2"), topK = 50)

        val ref = fx.index.compact()
        assertNotNull(ref)
        assertEquals(1, fx.index.stats()!!.segments, "合并后只剩一个段")
        assertEquals(0, fx.index.stats()!!.deletesSeq.size, "没有删除记录时合并后不用挂删除集")
        assertEquals(4, fx.index.stats()!!.seq, "三次建段 + 一次合并")

        for ((i, q) in queries.withIndex()) {
            assertSameRanking(
                before[i].hits.map { pkOf(it.pk) to it.score },
                fx.search(q, topK = 50).hits,
                "合并后 $q",
            )
        }
        assertSameRanking(
            phraseBefore.hits.map { pkOf(it.pk) to it.score },
            fx.search(TextQuery.Phrase(FIELD, "w1 w2"), topK = 50).hits,
            "合并后的短语查询（词位必须保留）",
        )
        assertNull(fx.index.compact(), "只有一个段时合并是空操作")
    }

    @Test
    fun `合并必须在未丢弃时搬运删除记录 - 否则已删除的行会复活`() = runTest {
        // 12 篇分成两个段，删除第 3 与第 10 篇（commit_ts=3000）
        suspend fun seeded(): Fixture {
            val f = fixture()
            f.indexBatch((1..8).map { Row(it.toLong(), 1000L, "marker alpha$it") })
            f.indexBatch((9..12).map { Row(it.toLong(), 2000L, "marker beta$it") })
            assertEquals(2, f.index.deleteDocuments(listOf(pkBytes(3), pkBytes(10)), commitTs = 3000))
            return f
        }

        // ① 水位低于删除时间 ⇒ 文档保留，但**删除语义必须一起搬过去**（新段有新的 segId）
        val keep = seeded()
        assertNotNull(keep.index.compact(dropDeletedBefore = 2500, newSegId = "keep"))
        val st = keep.index.stats()!!
        assertEquals(12, st.docs, "水位之下的已删文档仍要保留（可能还有读者停在那儿）")
        assertEquals(1, st.deletesSeq.size, "未被物理丢弃的删除必须要搬运到新段上")
        val segs = keep.index.manifest()!!.segments
        assertEquals(1, segs.size, "合并后只剩新段")
        assertEquals("keep", segs.single().segId)
        assertTrue(keep.index.stats()!!.deletesSeq.containsKey("keep"), "删除集必须挂在新段上（旧段号已不在 manifest 里）")
        assertEquals(10, keep.keys(TextQuery.AnyOf(FIELD, "marker"), topK = 20, readTs = 4000).size, "搬运后删除在检索上仍然生效")
        assertEquals(
            12, keep.keys(TextQuery.AnyOf(FIELD, "marker"), topK = 20, readTs = 2500).size,
            "commit_ts 被原样保留 ⇒ 时间旅行语义不变",
        )

        // ② 水位高于删除时间 ⇒ 文档被真正丢掉，段体积随之下降
        val drop = seeded()
        val before = drop.index.stats()!!.bytes
        assertNotNull(drop.index.compact(dropDeletedBefore = 3000, newSegId = "drop"))
        val after = drop.index.stats()!!
        assertEquals(10, after.docs, "被删除的文档应被物理丢弃")
        assertEquals(0, after.deletesSeq.size, "文档都丢了，删除记录也没有必要再留")
        assertTrue(after.bytes < before, "丢掉文档后索引体积应变小：$before -> ${after.bytes}")
        assertEquals(10, drop.keys(TextQuery.AnyOf(FIELD, "marker"), topK = 20).size)
    }

    @Test
    fun `合并后旧段对象（含删除集）可以被显式回收`() = runTest {
        val fx = fixture()
        fx.indexBatch(listOf(Row(1, 1000, "alpha")))
        fx.indexBatch(listOf(Row(1, 2000, "beta"), Row(2, 2000, "gamma")))
        val oldSegId = fx.index.manifest()!!.segments.first().segId
        assertEquals(2, fx.index.manifest()!!.segments.size)
        val oldDeleteKeys = fx.counting.listAll(IndexKeys.deletesPrefix(INDEX_ID, oldSegId))
        assertEquals(1, oldDeleteKeys.size, "旧段上应有一条覆盖写的删除集")

        fx.index.compact()
        val live = fx.index.manifest()!!.segments.single().segId
        assertTrue(live != oldSegId)
        fx.index.dropSegmentObjects(oldSegId)
        for (key in IndexKeys.segmentFiles(INDEX_ID, oldSegId)) {
            assertTrue(!fx.counting.exists(key), "旧段对象应被删除: $key")
        }
        for (key in oldDeleteKeys) {
            assertTrue(!fx.counting.exists(key), "旧段的删除集对象也应被回收: $key")
        }
        assertTrue(fx.keys(TextQuery.AnyOf(FIELD, "alpha")).isEmpty(), "合并后旧版本仍被遮蔽")
        assertEquals(listOf(1L), fx.keys(TextQuery.AnyOf(FIELD, "beta")))
        assertEquals(listOf(2L), fx.keys(TextQuery.AnyOf(FIELD, "gamma")))
    }

    @Test
    fun `索引级统计来自 manifest`() = runTest {
        val rows = corpus(count = 40, vocab = 10, seed = 21)
        val fx = fixture()
        val ref = fx.indexBatch(rows)
        val st = fx.index.stats()!!
        assertEquals(1, st.seq)
        assertEquals(1, st.segments)
        assertEquals(40, st.docs)
        assertEquals(ref.termCount.toLong(), st.terms)
        val oracleAvg = rows.sumOf { StandardAnalyzer().analyze(it.third).size }.toDouble() / rows.size
        assertEquals(oracleAvg, st.avgFieldLength, 1e-9)
        assertEquals(ref.bytes, st.bytes)
        assertTrue(st.bytes > 0)

        fx.index.deleteDocuments(listOf(pkBytes(1)), 2000)
        assertEquals(2, fx.index.stats()!!.seq)
        assertTrue(fx.index.stats()!!.deletesSeq.isNotEmpty())
        // 删除不存在的主键不写 manifest
        assertEquals(0, fx.index.deleteDocuments(listOf(pkBytes(999)), 2000))
        assertEquals(2, fx.index.stats()!!.seq)
        assertEquals(0, fx.index.deleteDocuments(emptyList(), 2000))
    }

    @Test
    fun `索引文档的主键不能重复`() = runTest {
        val fx = fixture()
        assertFailsWith<IllegalArgumentException> {
            fx.index.indexDocuments(listOf(IndexDoc(pkBytes(1), 1000, "a"), IndexDoc(pkBytes(1), 1000, "b")))
        }
        assertFailsWith<IllegalArgumentException> { fx.index.indexDocuments(emptyList()) }
    }

    // ── 请求数账本（§14.2）──────────────────────────────────────────

    @Test
    fun `单段稳态检索的请求数远低于候选文档数`() = runTest {
        // 3000 篇 × 8 词 / 500 词汇 → 单词条的 df ≈ 48，倒排块 1 块（blockDocs=256）
        val rnd = Random(20260924)
        val rows = (1..3000).map { id ->
            Row(id.toLong(), 1000L, (1..8).joinToString(" ") { "v" + rnd.nextInt(500) })
        }
        val fx = fixture(blockDocs = 256)
        fx.indexBatch(rows)

        val q = TextQuery.AnyOf(FIELD, "v7")
        // ① 同进程的第一次检索：写者已经按**读取器的缓存键**缓存了段对象
        //    （SegmentBuilder.put），所以一次整对象 GET 都不该有——这正是两套键必须对齐的原因（P4）。
        val first = fx.search(q, topK = 100)
        val df = first.hits.size
        assertEquals(
            0L, fx.counting.count("get"),
            "写者缓存过的段对象不该再 GET：${fx.counting.keysFor("get")}",
        )
        assertTrue(fx.counting.count("get_range") >= 1, "倒排块仍需按范围读，写者不缓存整份 postings.bin")

        // ② 稳态：块、词典、docmap 全部常驻内存 → 0 次数据读取（只有 1 次 head 复核 manifest）
        fx.counting.reset()
        val second = fx.search(q, topK = 100)
        assertEquals(first.hits.map { pkOf(it.pk) }, second.hits.map { pkOf(it.pk) })
        assertEquals(0L, fx.counting.count("get"), "稳态不应再有整对象 GET")
        assertEquals(0L, fx.counting.count("get_range"), "稳态不应再读倒排块")
        assertEquals(1L, fx.counting.count("head"), "每次检索只复核一次 CURRENT 的 etag")
        assertEquals(1L, fx.counting.totalReads(), "除 head 外不应有读请求")
        assertTrue(df >= 20, "语料应让该词条有足够的候选：df=$df")

        // ③ 冷启动（另一个进程：fresh BlockCache + 空段读取器）：只读 manifest 链、
        //    每个段的常驻元数据（meta / terms.fst / postings.idx / docmap.bin / docmap.idx / docts.bin）
        //    与常数个倒排块，**与 topK 无关**。
        val cold = fixture(blockDocs = 256)
        cold.indexBatch(rows)
        val reader = cold.asFreshReader()
        cold.counting.reset()
        val onCold = reader.search(q, topK = 100)
        assertEquals(first.hits.map { pkOf(it.pk) }, onCold.hits.map { pkOf(it.pk) }, "冷读结果必须与稳态一致")
        // 指针对 1 + manifest 快照 1 + 单段 6 = 8 次 GET。段内 6 次就是 §8.6 的「元数据小而热」：
        // 词典 / 词条块表 / docmap（含主键） / docts（含命中行 commit_ts），此后全常驻。
        assertEquals(
            8L, cold.counting.count("get"),
            "冷启动整对象 GET 的账：${cold.counting.keysFor("get")}",
        )
        assertEquals(
            1L, cold.counting.count("get_range"),
            "3000 篇里查一个词条只应读常数个倒排块：${cold.counting.keysFor("get_range")}",
        )
        assertTrue(
            cold.counting.bytesOut("get_range") <= 8L * 1024,
            "倒排块读取的字节数应与命中数成正比而不是与段体积成正比，实际 ${cold.counting.bytesOut("get_range")} B",
        )
    }

    @Test
    fun `检索路径不产生 LIST 也不产生写`() = runTest {
        val fx = fixture()
        fx.indexBatch(listOf(Row(1, 1000, "quick brown fox"), Row(2, 1000, "quick brown rabbit")))
        fx.counting.reset()                 // 计时从「只读路径」开始
        fx.counting.failOnList = true
        fx.counting.failOnPut = true
        fx.search(TextQuery.AnyOf(FIELD, "quick"))
        fx.search(TextQuery.AllOf(FIELD, "quick brown"))
        fx.search(TextQuery.Phrase(FIELD, "quick brown"))
        fx.search(MatchAgainstParser.parse(FIELD, "+quick -rabbit", MatchMode.BOOLEAN))
        fx.search(TextQuery.Prefix(FIELD, "qu"))
        fx.search(TextQuery.MatchAll(FIELD))
        assertEquals(0L, fx.counting.count("put"))
        assertEquals(0L, fx.counting.count("put_cond"))
    }

    // ── P6：条件写能力的显式降级 ────────────────────────────────────

    @Test
    fun `后端不支持条件写时必须显式降级并报告`() = runTest {
        val noCas = MemoryObjectStore(
            caps = ObjectStoreCapabilities(
                conditionalCreate = false,
                conditionalUpdate = false,
                strongReadAfterWrite = true,
                strongListConsistency = true,
            )
        )
        val fx = fixture(store = noCas)
        fx.indexBatch(listOf(Row(1, 1000, "alpha")))
        assertTrue(fx.index.lastCommitDegraded, "降级状态必须能被调用方看到（P6）")
        assertEquals(listOf(1L), fx.keys(TextQuery.AnyOf(FIELD, "alpha")))

        val withCas = fixture()
        withCas.indexBatch(listOf(Row(1, 1000, "alpha")))
        assertTrue(!withCas.index.lastCommitDegraded, "支持条件写时不应降级")
    }

    @Test
    fun `陈旧的 etag 提交被拒绝 - fencing`() = runTest {
        val fx = fixture()
        fx.index.indexDocuments(listOf(IndexDoc(pkBytes(1), 1000, "alpha")))
        val mf = fx.index.manifest()!!
        val etag = fx.index.currentEtag()!!

        // 同一个 manifest 用当前 etag 提交两次：第二次必然陈旧
        fx.index.commitManifest(mf.copy(seq = mf.seq + 1), etag)
        assertFailsWith<IllegalStateException> { fx.index.commitManifest(mf.copy(seq = mf.seq + 2), etag) }
        // 被拒绝后缓存必须被丢弃，视图不能停在「自己以为成功了」的版本上
        assertEquals(mf.seq + 1, fx.index.manifest(force = true)!!.seq)
        assertFailsWith<IllegalStateException> { fx.index.commitManifest(mf.copy(seq = mf.seq + 3), ETag("bogus")) }
    }
}
