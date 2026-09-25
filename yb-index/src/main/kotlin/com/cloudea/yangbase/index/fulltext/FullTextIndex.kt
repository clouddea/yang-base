package com.cloudea.yangbase.index.fulltext

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.CorruptDataException
import com.cloudea.yangbase.common.IndexId
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.PreconditionFailedException
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.index.IndexKeys
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.Precondition
import com.cloudea.yangbase.storage.BlockCache
import kotlinx.serialization.cbor.Cbor
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/** 待索引的一篇文档。文本由调用方从数据面取好——索引器**不接触数据面**。 */
class IndexDoc(val pk: ByteArray, val commitTs: Long, val text: String)

/** 检索统计。请求数账本请配合 `CountingObjectStore` 一起看。 */
data class FullTextStats(
    val segmentsTotal: Int,
    /** 因段级时间裁剪（`minTs > read_ts`）而未触碰的段数：**0 IO**。 */
    val segmentsPrunedByTs: Int,
    /** 因「本段根本没有必需词条」而被整段跳过的段数：**0 IO**。 */
    val segmentsPrunedByTerms: Int,
    val cursors: Int,
    val scoredCandidates: Long,
) {
    override fun toString(): String =
        "段=$segmentsTotal(时间裁剪=$segmentsPrunedByTs 词条裁剪=$segmentsPrunedByTerms) " +
            "游标=$cursors 打分=$scoredCandidates"
}

class FullTextSearchResult(val hits: List<Hit>, val stats: FullTextStats)

/** manifest 提交结果。[conditional]=false 表示**已降级**为无条件写（见 [FullTextIndex.commitManifest]）。 */
class CommitOutcome(val etag: ETag, val conditional: Boolean)

/** 索引级统计（诊断 / `EXPLAIN` / 覆盖测试用）。 */
data class FullTextIndexStats(
    val seq: Long,
    val segments: Int,
    val docs: Long,
    val terms: Long,
    val bytes: Long,
    val avgFieldLength: Double,
    val deletesSeq: Map<String, Long>,
)

/**
 * 段式倒排索引（设计文档 §8.6）。
 *
 * 对象布局与不可变性：
 * ```
 * index/{id}/manifest/CURRENT     ← 唯一可变对象（指针）
 * index/{id}/manifest/M-….mf
 * index/{id}/seg/{segId}/…        ← 段内文件全部不可变
 * index/{id}/deletes/{segId}/{seq}.cbor
 * ```
 * 一切变更 = 写新段（或新删除集）+ 写新 manifest + 切 CURRENT 指针（P2）。
 * 段与删除集对象**永不原地修改**，因此读者拿到的 manifest 就是一个一致快照。
 *
 * ## 请求数账本（P4，对照 §14.2）
 * | 动作 | 请求数 |
 * |---|---|
 * | 读 CURRENT + manifest（已有缓存且未变） | 1 次 `head`；etag 未变则 **0 次 GET** |
 * | 段级时间裁剪（`minTs > read_ts`） | **0** |
 * | 词条定位 / 前缀展开 / 块级上界 | **0**（`terms.fst` + `postings.idx` 常驻） |
 * | 读一个倒排块 | 1 次范围 GET（只取该块的几十–几百字节） |
 * | 回表取主键 | 段小（≤ `docMapInlineMax`）：1 次 GET 覆盖全部命中；段大：每命中一条 1 次范围 GET |
 * | 段跨越 `read_ts` 时的版本校验 | 该段 `docts.bin` 1 次 GET |
 * | 命中行的 `commitTs` | 该段 `docts.bin` 1 次 GET（**每段一次**，与命中数无关；解析后常驻，同段后续检索 0）|
 *
 * 最后一行是刻意的取舍：整段可见时 MVCC 本不需要 `docts.bin`，但 [Hit] 要给出精确的
 * `commit_ts`，只有这个文件有。代价是**冷启动每段多 1 次 GET**（不是每次命中 1 次），
 * 换来的是「命中行的版本」这一信息不需要调用方再回数据面查一次。
 *
 * ⚠️ **写入路径的并发前提**：同一 `index_id` 任一时刻只有一个写者（与 §6.3 的写者租约同源）。
 * 后端支持条件写时 CURRENT 用 CAS；不支持时**显式降级**为无条件写，并把
 * `conditional=false` 返回给调用方（P6）——绝不假装自己有 CAS。
 */
class FullTextIndex(
    private val store: ObjectStore,
    private val cache: BlockCache,
    val indexId: IndexId,
    /** 本索引覆盖的字段名。查询里的字段必须与它一致。 */
    val field: String,
    /** 索引期与查询期**必须**是同一个分析器（名字写进段元信息并逐段校验）。 */
    val analyzer: Analyzer,
    val params: Bm25Params = Bm25Params(),
    private val writerEpoch: Long = 1,
    private val blockDocs: Int = SegmentBuilder.DEFAULT_BLOCK_DOCS,
    /** 前缀展开上限。超过就**报错**而不是静默截断（截断 = 静默少结果）。 */
    private val maxPrefixExpansion: Int = 1024,
    private val docMapInlineMax: Long = 4L * 1024 * 1024,
    private val maxDocsPerSegment: Int = SegmentBuilder.DEFAULT_MAX_DOCS,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val cbor = Cbor { ignoreUnknownKeys = true }

    private val currentKey = ObjectKeys.indexManifestCurrent(indexId)
    private val readers = ConcurrentHashMap<String, SegmentReader>()

    @Volatile private var cachedManifest: IndexManifest? = null
    @Volatile private var cachedEtag: ETag? = null

    /** 最近一次提交是否降级（后端不支持条件写）。 */
    @Volatile var lastCommitDegraded: Boolean = false
        private set

    // ── manifest ────────────────────────────────────────────────────

    /**
     * 读当前 manifest。
     *
     * 稳态成本：`head(CURRENT)` **1 次**（拿 etag 判断有没有变），变了才 GET 指针 + 快照。
     * 写路径与读路径共用同一份缓存。
     *
     * ⚠️ **不能只查缓存就返回**：同一进程内的写者会把缓存刷新，但另一个进程/节点
     * （或另一个 CN）的写者不会。只查缓存会让读者**永远**停在旧视图上，
     * 于是「已经改掉/删掉的行」继续被检索到——不报错、只给过期结果，
     * 正是 §8.6.5 要防的那类静默错误。所以每次读都用一个廉价的 `head` 复核 etag。
     */
    suspend fun manifest(force: Boolean = false): IndexManifest? {
        val meta = try {
            store.head(currentKey)
        } catch (_: ObjectNotFoundException) {
            cachedManifest = null
            cachedEtag = null
            return null
        }
        if (!force && cachedManifest != null && meta.etag == cachedEtag) return cachedManifest
        val ptr = cbor.decodeFromByteArray(IndexManifestPointer.serializer(), store.get(currentKey))
        val mf = cbor.decodeFromByteArray(IndexManifest.serializer(), store.get(ptr.path))
        if (mf.indexId != indexId.value) {
            throw CorruptDataException("manifest 的 indexId=${mf.indexId} 与请求的 ${indexId.value} 不一致")
        }
        cachedManifest = mf
        cachedEtag = meta.etag
        pruneReaders(mf)
        return mf
    }

    /** 当前 manifest 的 etag（写路径做 CAS 用）；没有索引时为 null。 */
    suspend fun currentEtag(): ETag? {
        manifest()
        return cachedEtag
    }

    /**
     * 切换 CURRENT 指针。
     *
     * 有 CAS 能力时用条件写（fencing 的第二道防线，§4.3）；没有时**显式降级**
     * 为无条件写并在返回里标明——正确性此时完全依赖「单写者」这个前提，
     * 调用方必须自己保证。这是 P6 要求暴露的信息，不是可以悄悄吞掉的细节。
     */
    suspend fun commitManifest(manifest: IndexManifest, expectedEtag: ETag?): CommitOutcome {
        val mfKey = ObjectKeys.indexManifestSnapshot(indexId, manifest.seq)
        store.put(mfKey, cbor.encodeToByteArray(IndexManifest.serializer(), manifest))

        val ptrBytes = cbor.encodeToByteArray(
            IndexManifestPointer.serializer(),
            IndexManifestPointer(manifest.seq, mfKey, writerEpoch),
        )
        val canCas = if (expectedEtag == null) store.caps.conditionalCreate else store.caps.conditionalUpdate
        if (!canCas) {
            lastCommitDegraded = true
            log.warn(
                "后端不支持条件写（create={} update={}）：index manifest 切换降级为无条件写，" +
                    "正确性依赖单写者前提，indexId={}",
                store.caps.conditionalCreate, store.caps.conditionalUpdate, indexId.value,
            )
            val etag = store.put(currentKey, ptrBytes)
            cachedManifest = manifest
            cachedEtag = etag
            return CommitOutcome(etag, conditional = false)
        }
        val pre = if (expectedEtag == null) Precondition.MustNotExist else Precondition.MustMatch(expectedEtag)
        val etag = try {
            store.put(currentKey, ptrBytes, pre)
        } catch (e: PreconditionFailedException) {
            // `MustNotExist` 不成立 = 指针被别人建出来了
            throwFencing(manifest, e)
        } catch (e: ConcurrentModificationException) {
            // `MustMatch` 不成立 = 指针被别人切走了（S3 上是 409）
            throwFencing(manifest, e)
        }
        lastCommitDegraded = false
        cachedManifest = manifest
        cachedEtag = etag
        return CommitOutcome(etag, conditional = true)
    }

    // ── 写入 ────────────────────────────────────────────────────────

    /**
     * 索引一批文档，并为一个全新的不可变段切换 manifest。
     *
     * 覆盖写（同一主键的新版本）按 §8.6.5 处理：新版本进新段，
     * 旧版本在**原段**里被删除集标记（段本身不变）。
     *
     * 请求数：建段 7 次 PUT +（每个受影响的旧段）读 docmap 1 次 + 写删除集 1 次 + manifest 2 次 PUT。
     */
    suspend fun indexDocuments(
        docs: List<IndexDoc>,
        schemaVersion: Long = 0,
        newSegId: String? = null,
    ): SegmentRef {
        require(docs.isNotEmpty()) { "没有要索引的文档" }
        val base = manifest(force = true)
        val seq = (base?.seq ?: 0L) + 1
        val segId = newSegId ?: ("g" + ObjectKeys.pad(seq))
        val pks = docs.map { it.pk }
        // ⚠️ 按**内容**判重：ByteArray 的 equals 是引用相等，直接 toSet() 会让这道
        // 检查永远通过；而同一批里的重复主键会同时进入新段，互相之间没有删除集遮蔽，
        // 于是检索会返回同一行的两个版本。
        require(pks.map { keyOfPk(it) }.toSet().size == pks.size) {
            "同一批文档里有重复主键；请先合并再索引"
        }

        val builder = SegmentBuilder(
            segId = segId,
            field = field,
            analyzerName = analyzer.name,
            blockDocs = blockDocs,
            params = params,
            maxDocs = maxDocsPerSegment,
        )
        for (d in docs) builder.addDocument(d.pk, d.commitTs, d.text, analyzer)
        val meta = builder.write(store, cache, indexId)
        val ref = refOf(meta)

        // 旧段里的同主键版本：标记删除（段不可变，旧版本靠删除集遮蔽）
        val upsertTs = docs.maxOf { it.commitTs }
        val deletesSeq = HashMap<String, Long>()
        var seqCounter = base?.deletesSeq?.values?.maxOrNull() ?: 0L
        for (segRef in base?.segments ?: emptyList()) {
            if (segRef.segId == segId) continue
            val reader = loadReader(segRef.segId)
            reader.useDeletes(base?.deletesSeq?.get(segRef.segId))
            val hits = ArrayList<Int>()
            for (pk in pks) reader.docIdOf(pk)?.let { hits.add(it) }
            if (hits.isEmpty()) continue
            hits.sort()
            seqCounter++
            writeDeletes(reader, hits, upsertTs, seqCounter)
            deletesSeq[segRef.segId] = seqCounter
        }
        base?.deletesSeq?.forEach { (k, v) -> deletesSeq.putIfAbsent(k, v) }

        val updated = IndexManifest(
            indexId = indexId.value,
            seq = seq,
            schemaVersion = schemaVersion,
            segments = (base?.segments ?: emptyList()) + ref,
            deletesSeq = deletesSeq,
            updatedAt = upsertTs,
        )
        commitManifest(updated, cachedEtag)
        return ref
    }

    /**
     * 删除一批主键（写删除集 + 切 manifest），返回被标记的 (段, 文档) 数。
     *
     * 删除记录的 `commitTs` 是**精确时间点**：`read_ts < commitTs` 的读者看不到这次删除，
     * 旧版本依然可见（§8.6.5，这就是「时间旅行 + 删除」能共存的原因）。
     */
    suspend fun deleteDocuments(keys: List<ByteArray>, commitTs: Long): Int {
        if (keys.isEmpty()) return 0
        val base = manifest(force = true) ?: return 0
        val deletesSeq = HashMap(base.deletesSeq)
        var seqCounter = base.deletesSeq.values.maxOrNull() ?: 0L
        var total = 0
        for (segRef in base.segments) {
            val reader = loadReader(segRef.segId)
            reader.useDeletes(base.deletesSeq[segRef.segId])
            val hits = ArrayList<Int>()
            for (pk in keys) reader.docIdOf(pk)?.let { hits.add(it) }
            if (hits.isEmpty()) continue
            hits.sort()
            seqCounter++
            writeDeletes(reader, hits, commitTs, seqCounter)
            deletesSeq[segRef.segId] = seqCounter
            total += hits.size
        }
        if (total == 0) return 0
        commitManifest(
            base.copy(seq = base.seq + 1, deletesSeq = deletesSeq, updatedAt = commitTs),
            cachedEtag,
        )
        return total
    }

    /**
     * 条件写失败 = fencing 生效，本写者已不是合法写者。
     *
     * 必须**同时**丢掉内存里的 manifest 缓存：留着它会让本写者继续基于过期视图
     * （段列表 / 删除集序号）写下一个 manifest，把并发写者的成果覆盖掉——
     * 那正是条件写要防的事情，丢掉缓存是这道防线的另一半。
     */
    private fun throwFencing(manifest: IndexManifest, cause: Throwable): Nothing {
        cachedManifest = null
        cachedEtag = null
        throw IllegalStateException(
            "index manifest 已被其他写者切换（indexId=${indexId.value} seq=${manifest.seq}）：" +
                "本写者的租约已失效，缓存与本地视图已丢弃", cause,
        )
    }

    private suspend fun writeDeletes(reader: SegmentReader, sorted: List<Int>, ts: Long, seq: Long) {
        val existing = reader.deleteEntries()
        val entries = ArrayList<DeleteEntry>(existing.size + 1)
        entries.addAll(existing)
        entries.add(DeleteEntry(ts, PostingsFormat.encodeDocIdSet(sorted.toIntArray())))
        writeDeleteSet(reader.segId, entries, seq)
    }

    /** 写一个新的删除集对象（新序号 = 新对象，P2）。写者紧接着可能读它，所以顺手进缓存。 */
    private suspend fun writeDeleteSet(segId: String, entries: List<DeleteEntry>, seq: Long) {
        val set = DeleteSet(segId = segId, entries = entries.sortedBy { it.commitTs })
        val key = IndexKeys.deletes(indexId, segId, seq)
        val bytes = cbor.encodeToByteArray(DeleteSet.serializer(), set)
        store.put(key, bytes)
        cache.put(SegmentReader.deleteCacheKey(indexId, segId, seq), bytes)
    }

    // ── 检索 ────────────────────────────────────────────────────────

    /**
     * 检索 top-k。
     *
     * 三段式：
     *  1. **段级裁剪**（0 IO）：`minTs > read_ts` 的段整体不可见；
     *  2. **规划**（0 IO）：前缀展开、词条定位、全局 idf、决定走 WAND 还是交集扫描；
     *  3. **取数**：只读真正需要的倒排块。
     */
    suspend fun search(
        query: TextQuery,
        topK: Int = 10,
        readTs: ReadTs = ReadTs(Long.MAX_VALUE),
    ): FullTextSearchResult {
        require(topK > 0) { "topK 必须 > 0" }
        require(query.field == field) {
            "查询字段 '${query.field}' 与索引字段 '$field' 不一致（一个索引只覆盖一个字段）"
        }
        val mf = manifest() ?: return FullTextSearchResult(emptyList(), EMPTY_STATS)
        val refs = mf.segments
        val visible = refs.filter { it.minTs <= readTs.value }
        if (visible.isEmpty()) {
            return FullTextSearchResult(emptyList(), FullTextStats(refs.size, refs.size, 0, 0, 0))
        }
        val specs = ArrayList<SegmentSpec>(visible.size)
        for ((ordinal, ref) in visible.withIndex()) {
            val reader = loadReader(ref.segId)
            if (reader.meta.analyzer != analyzer.name) {
                throw CorruptDataException(
                    "段 ${ref.segId} 用分析器 '${reader.meta.analyzer}'，本次查询用 '${analyzer.name}'；" +
                        "两者不一致会静默返回空结果，因此这里直接失败"
                )
            }
            reader.useDeletes(mf.deletesSeq[ref.segId])
            specs.add(SegmentSpec(ordinal, reader))
        }

        val docTotal = specs.sumOf { it.docCount }
        val stats = SearchStats(
            docCount = docTotal.toLong(),
            avgDocLength = specs.sumOf { it.reader.meta.totalTokens }.toDouble() / docTotal.coerceAtLeast(1),
        )
        val plan = QueryPlanner(analyzer, params, stats, maxPrefixExpansion).plan(query, specs)
        val filter = MvccFilter(specs, readTs.value)
        val accum = HitAccumulator(specs)

        val run = if (plan.isWandable) {
            wandSearch(plan, filter, accum, topK)
        } else {
            andSearch(plan, filter, accum, topK)
        }
        return FullTextSearchResult(
            accum.toHits(topK),
            FullTextStats(
                segmentsTotal = refs.size,
                segmentsPrunedByTs = refs.size - visible.size,
                segmentsPrunedByTerms = run.prunedByTerms,
                cursors = run.cursors,
                scoredCandidates = run.scored,
            ),
        )
    }

    /**
     * 纯 OR 路径：block-max WAND（§8.6.2）。
     *
     * 所有 `optional` 词条（跨段）汇成一池游标，由 [WandSearcher] 按上界剪枝：
     * 只有「可能进 top-k」的候选才会被读块与打分。
     */
    private suspend fun wandSearch(
        plan: PlannedQuery,
        filter: DocFilter,
        accum: HitAccumulator,
        topK: Int,
    ): RunStats {
        val cursors = ArrayList<TermCursor>()
        for (g in plan.segments) {
            for (t in g.optional) g.cursor(t.term)?.let { cursors.add(it) }
        }
        if (cursors.isEmpty()) return RunStats(0, 0, 0)
        val res = WandSearcher(filter).search(cursors, topK)
        for (e in res.hits) {
            accum.offer((e.docKey ushr 32).toInt(), e.docId, e.score)
        }
        return RunStats(0, res.stats.cursors, res.stats.scoredCandidates)
    }

    /**
     * 交集扫描路径：存在必须项 / 短语 / 排除项时走它。
     *
     * 为什么不套 WAND：WAND 回答的是「哪些文档**可能**进入 OR 的 top-k」，
     * 而这里的候选集是**交集**——用 OR 的 top-k 去近似交集会漏结果。
     * 代价与「驱动子句」的文档频率成正比（Lucene 同样如此）。
     *
     * 驱动子句取「每个候选都必须满足的子句」中文档频率之和最小者，它保证候选流是
     * **全部合法文档的超集**；某个必需子句在本段一个词条都查不到时（0 IO 的 FST 判定），
     * 本段整体跳过。
     */
    private suspend fun andSearch(
        plan: PlannedQuery,
        filter: DocFilter,
        accum: HitAccumulator,
        topK: Int,
    ): RunStats {
        var cursorsTotal = 0
        var scored = 0L
        var prunedByTerms = 0
        for (g in plan.segments) {
            if (g.matchAll) {
                for (d in 0 until g.reader.docCount) {
                    if (!filter.accepts(g.ordinal, d)) continue
                    scored++
                    accum.offer(g.ordinal, d, 0.0)
                }
                continue
            }
            // ① 各子句的游标；任一必需子句在本段无处可查 → 整段无结果
            val mandatory = ArrayList<Clause>()
            var unsatisfiable = false
            for (clause in g.must) {
                val cs = clause.mapNotNull { g.cursor(it.term) }
                if (cs.isEmpty()) { unsatisfiable = true; break }
                mandatory.add(Clause(cs))
            }
            if (!unsatisfiable) {
                for (p in g.phrases) {
                    val cs = p.mapNotNull { g.cursor(it.term) }
                    // 短语里缺任何一个词条 => 本段不可能有相邻命中
                    if (cs.size != p.size) { unsatisfiable = true; break }
                    mandatory.add(Clause(cs))
                }
            }
            if (unsatisfiable) { prunedByTerms++; continue }
            val optCursors = g.optional.mapNotNull { g.cursor(it.term) }
            if (g.optional.isNotEmpty() && optCursors.isEmpty()) { prunedByTerms++; continue }
            val notCursors = g.mustNot.mapNotNull { g.cursor(it.term) }
            val optionalClause = if (g.optional.isNotEmpty()) Clause(optCursors) else null
            // 全部词条都切没了（停用词 / 前缀展开为空）：本段没有候选。
            // 不提前返回的话下面 `optionalClause!!` 会直接抛 NPE——查询「本来就查不到」
            // 和「查询崩了」是两件事，前者是正确答案。
            if (mandatory.isEmpty() && optionalClause == null) { prunedByTerms++; continue }

            // ② 驱动子句：候选必须是「命中驱动子句」的超集，故取 df 之和最小者
            val driverClauses = if (mandatory.isNotEmpty()) mandatory else listOf(optionalClause!!)
            val driver = driverClauses.minByOrNull { it.dfSum() } ?: continue
            cursorsTotal += driver.cursors.size

            val iter = ClauseCursor(driver.cursors, g.ordinal)
            iter.advance()
            while (iter.doc >= 0) {
                val doc = iter.doc
                if (filter.accepts(g.ordinal, doc) &&
                    satisfies(g, mandatory, doc) &&
                    phraseOk(g, doc)
                ) {
                    val optHit = optionalClause == null || optCursors.any { contains(it, g.ordinal, doc) }
                    if (optHit && notCursors.none { contains(it, g.ordinal, doc) }) {
                        scored++
                        accum.offer(g.ordinal, doc, scoreOf(mandatory, optCursors, doc))
                    }
                }
                iter.advance()
            }
        }
        return RunStats(prunedByTerms, cursorsTotal, scored)
    }

    /** 每个必需子句都至少有一个词条命中该文档。 */
    private suspend fun satisfies(g: SegmentGroup, clauses: List<Clause>, doc: Int): Boolean {
        for (c in clauses) {
            var ok = false
            for (cur in c.cursors) {
                if (contains(cur, g.ordinal, doc)) { ok = true; break }
            }
            if (!ok) return false
        }
        return true
    }

    private suspend fun phraseOk(g: SegmentGroup, doc: Int): Boolean {
        for (p in g.phrases) if (!g.phraseAdjacent(p, doc)) return false
        return true
    }

    /** 命中文档的分数 = 所有停在它上面的游标的 BM25 之和（同一游标只算一次）。 */
    private fun scoreOf(mandatory: List<Clause>, optCursors: List<TermCursor>, doc: Int): Double {
        var score = 0.0
        val seen = HashSet<TermCursor>()
        for (c in mandatory) {
            for (cur in c.cursors) {
                if (cur.docId == doc && seen.add(cur)) score += cur.score()
            }
        }
        for (cur in optCursors) {
            if (cur.docId == doc && seen.add(cur)) score += cur.score()
        }
        return score
    }

    /** 游标是否已停在 [doc]；不在则向前跳到第一个 ≥ doc 的位置（游标只前进）。 */
    private suspend fun contains(c: TermCursor, seg: Int, doc: Int): Boolean {
        if (c.docId == doc) return true
        if (c.docId > doc) return false
        c.seekTo(seg, doc)
        return c.docId == doc
    }

    private class Clause(val cursors: List<TermCursor>) {
        fun dfSum(): Int = cursors.sumOf { it.df }
    }

    private class RunStats(val prunedByTerms: Int, val cursors: Int, val scored: Long)

    // ── 合并（§8.6.6）───────────────────────────────────────────────

    /**
     * 把所有段合并成一个新段（**K 路归并，不重新分词**）。
     *
     * 不重新分词意味着位置信息不会丢，代价也不会翻倍——这是设计文档强调的那一点。
     * 文档重编号后按词条归并即可，**完全不需要接触数据面**。
     *
     * [dropDeletedBefore] 为 null 时保留所有文档（含旧版本，最安全）；
     * 传入 GC 水位（通常是 `min(活跃读者 read_ts)`）时，删除时间早于水位的文档
     * 会在合并里被真正丢掉——这正是 §6.9 说的那次「巨大的 GC 机会」。
     *
     * ⚠️ **没被丢掉的删除必须被搬运到新段上**：删除集是按 `segId` 命名的，
     * 合并后旧段号从 manifest 里消失，若只把「段列表」换成新段而不搬运删除记录，
     * 那些**没有被物理丢掉的已删文档就会重新出现在检索结果里**（§8.6.5 明确要防的
     * 过期结果）。搬运时**保留每条删除原本的 `commit_ts`**，时间旅行语义因此不变。
     *
     * ⚠️ 旧段对象**不在这里删除**：可能还有读者持有旧 manifest。
     * 回收由 [dropSegmentObjects] 显式触发（时机由 GC 的可达性分析决定）。
     */
    suspend fun compact(dropDeletedBefore: Long? = null, newSegId: String? = null): SegmentRef? {
        val base = manifest(force = true) ?: return null
        if (base.segments.size < 2) return null
        val seq = base.seq + 1
        val segId = newSegId ?: ("c" + ObjectKeys.pad(seq))
        val sources = base.segments.map { loadReader(it.segId) }

        val out = SegmentBuilder(
            segId = segId,
            field = field,
            analyzerName = analyzer.name,
            blockDocs = blockDocs,
            params = params,
            maxDocs = maxDocsPerSegment,
        )
        // ① 文档重编号：oldSeg → (oldDoc → newDoc)，被丢掉的文档映射为 -1
        val maps = ArrayList<IntArray>(sources.size)
        val carried = ArrayList<DeleteEntry>()
        for (src in sources) {
            src.useDeletes(base.deletesSeq[src.segId])
            val versions = src.docVersions()
            val dropped = if (dropDeletedBefore == null) null else src.deletedDocs(dropDeletedBefore)
            val map = IntArray(src.docCount) { -1 }
            for (d in 0 until src.docCount) {
                if (dropped != null && dropped.isNotEmpty() && dropped.binarySearch(d) >= 0) continue
                map[d] = out.addDocumentKey(src.primaryKey(d), versions.ts[d], versions.dls[d])
            }
            // 没被物理丢掉的删除记录：映射到新段号后原样搬运（含原有的 commit_ts）
            for (e in src.deleteEntries()) {
                val kept = PostingsFormat.decodeDocIdSet(e.docs).map { map[it] }.filter { it >= 0 }.sorted()
                if (kept.isNotEmpty()) {
                    carried.add(DeleteEntry(e.commitTs, PostingsFormat.encodeDocIdSet(kept.toIntArray())))
                }
            }
            maps.add(map)
        }
        // ② 词条级 K 路归并：各段的词条序列都是字典序，反复取「当前最小词条」推进
        val fstCursors = sources.map { src ->
            val list = ArrayList<Pair<String, Int>>()
            src.fst().forEachTerm("") { t, o -> list.add(t to o); true }
            list.iterator()
        }
        val head = arrayOfNulls<Pair<String, Int>>(sources.size)
        for (i in sources.indices) head[i] = if (fstCursors[i].hasNext()) fstCursors[i].next() else null
        while (true) {
            var term: String? = null
            for (h in head) {
                if (h != null && (term == null || h.first < term)) term = h.first
            }
            if (term == null) break
            for (i in sources.indices) {
                val h = head[i] ?: continue
                if (h.first != term) continue
                val src = sources[i]
                val map = maps[i]
                val table = src.termTable()
                val rec = table.terms[h.second]
                val start = src.blockStart(rec)
                for (b in start until start + rec.blockCount) {
                    val block = src.readBlock(rec, table.blocks[b])
                    for (k in 0 until block.size) {
                        val newDoc = map[block.docIds[k]]
                        if (newDoc < 0) continue
                        out.addPostings(
                            term = term,
                            docId = newDoc,
                            tf = block.tfs[k],
                            dl = block.docLengths[k],
                            positions = block.positions?.getOrNull(k),
                        )
                    }
                }
                head[i] = if (fstCursors[i].hasNext()) fstCursors[i].next() else null
            }
        }
        val meta = out.write(store, cache, indexId)
        val ref = refOf(meta)
        val deletesSeq: Map<String, Long> = if (carried.isEmpty()) {
            emptyMap()
        } else {
            val s = (base.deletesSeq.values.maxOrNull() ?: 0L) + 1
            writeDeleteSet(segId, carried, s)
            mapOf(segId to s)
        }
        commitManifest(
            IndexManifest(
                indexId = indexId.value,
                seq = seq,
                schemaVersion = base.schemaVersion,
                segments = listOf(ref),
                deletesSeq = deletesSeq,
                updatedAt = meta.maxTs,
            ),
            cachedEtag,
        )
        return ref
    }

    /**
     * 删除一个段的全部对象（GC 用；调用前必须确保没有读者还需要它）。
     *
     * 删除集对象也属于这个段（键里带 `segId`），必须一并回收——否则它们会永远留在
     * 对象存储里无人引用（每次合并都会产生一批）。这里用 LIST 是**允许的**：
     * GC 不是热路径，而删除集的键里带序号，不列举就不知道该删哪些。
     */
    suspend fun dropSegmentObjects(segId: String) {
        for (key in IndexKeys.segmentFiles(indexId, segId)) store.delete(key)
        for (key in store.listAll(IndexKeys.deletesPrefix(indexId, segId))) store.delete(key)
        readers.remove(segId)
    }

    // ── 内部 ────────────────────────────────────────────────────────

    /** 已加载的段的读取器（需先 [manifest]）。 */
    fun readerFor(segId: String): SegmentReader =
        readers[segId] ?: throw IllegalStateException("段 $segId 的读取器尚未加载；请先调用 manifest()")

    private suspend fun loadReader(segId: String): SegmentReader {
        readers[segId]?.let { return it }
        val key = IndexKeys.segmentFile(indexId, segId, IndexKeys.SEG_META)
        val bytes = cache.getOrLoad(SegmentReader.cacheKey(indexId, segId, IndexKeys.SEG_META)) { store.get(key) }
        val meta = cbor.decodeFromByteArray(SegmentMeta.serializer(), bytes)
        val created = SegmentReader(store, cache, indexId, meta, docMapInlineMax)
        readers.putIfAbsent(segId, created)
        return readers[segId]!!
    }

    private fun pruneReaders(mf: IndexManifest) {
        val live = mf.segments.mapTo(HashSet()) { it.segId }
        readers.keys.retainAll(live)
    }

    private fun refOf(meta: SegmentMeta): SegmentRef = SegmentRef(
        segId = meta.segId,
        docCount = meta.docCount,
        minTs = meta.minTs,
        maxTs = meta.maxTs,
        termCount = meta.termCount,
        totalTokens = meta.totalTokens,
        bytes = meta.files.values.sumOf { it.bytes },
        field = meta.field,
        analyzer = meta.analyzer,
    )

    /** 索引级统计（全部来自 manifest，0 IO；已缓存时连 `head` 都省了）。 */
    suspend fun stats(): FullTextIndexStats? {
        val mf = manifest() ?: return null
        return FullTextIndexStats(
            seq = mf.seq,
            segments = mf.segments.size,
            docs = mf.docCount,
            terms = mf.segments.sumOf { it.termCount.toLong() },
            bytes = mf.segments.sumOf { it.bytes },
            avgFieldLength = if (mf.docCount == 0L) 0.0 else mf.totalTokens.toDouble() / mf.docCount,
            deletesSeq = mf.deletesSeq,
        )
    }

    /**
     * 段级时间裁剪 + 删除集 + **逐文档**版本校验。
     *
     * 三种情形：
     *  - `maxTs <= read_ts`：整段可见，只需查删除集（没有删除时 0 IO）；
     *  - `minTs > read_ts`：整段不可见（`search` 已提前裁掉，这里兜底）；
     *  - 跨越 `read_ts`：读该段 `docts.bin`（1 次 GET）逐文档比对 commit_ts。
     */
    private class MvccFilter(private val specs: List<SegmentSpec>, private val readTs: Long) : DocFilter {
        private val deletes = HashMap<Int, IntArray>()
        private val versions = HashMap<Int, PostingsFormat.DocVersions>()

        override suspend fun accepts(segOrdinal: Int, docId: Int): Boolean {
            val reader = specs[segOrdinal].reader
            if (reader.meta.isAfter(readTs)) return false
            val deleted = deleted(segOrdinal, reader)
            if (deleted.isNotEmpty() && deleted.binarySearch(docId) >= 0) return false
            if (reader.meta.isFullyVisibleAt(readTs)) return true
            val v = versions[segOrdinal] ?: reader.docVersions().also { versions[segOrdinal] = it }
            return v.ts[docId] <= readTs
        }

        private suspend fun deleted(segOrdinal: Int, reader: SegmentReader): IntArray =
            deletes.getOrPut(segOrdinal) { reader.deletedDocs(readTs) }
    }

    /** 命中累积：`docKey → 分数`，跨段可直接比大小。 */
    private class HitAccumulator(private val specs: List<SegmentSpec>) {
        private val best = HashMap<Long, Double>()

        fun offer(segOrdinal: Int, docId: Int, score: Double) {
            val key = (segOrdinal.toLong() shl 32) or docId.toLong()
            val old = best[key]
            if (old == null || score > old) best[key] = score
        }

        /** 按 (分数降序, 段序, docId) 取前 k，再逐条回表取主键。 */
        suspend fun toHits(topK: Int): List<Hit> {
            val ordered = best.entries
                .sortedWith(compareByDescending<Map.Entry<Long, Double>> { it.value }.thenBy { it.key })
                .take(topK)
            val out = ArrayList<Hit>(ordered.size)
            for (e in ordered) {
                val seg = (e.key ushr 32).toInt()
                val docId = (e.key and 0xFFFFFFFFL).toInt()
                val reader = specs[seg].reader
                out.add(
                    Hit(
                        docKey = e.key,
                        segId = reader.segId,
                        docId = docId,
                        pk = reader.primaryKey(docId),
                        commitTs = reader.commitTsOf(docId),
                        score = e.value,
                    )
                )
            }
            return out
        }
    }

    companion object {
        private val EMPTY_STATS = FullTextStats(0, 0, 0, 0, 0)

        /** 主键的按内容比较用键（逐字节无损；与 [SegmentReader.docIdOf] 用的是同一套）。 */
        private fun keyOfPk(pk: ByteArray): String = String(pk, Charsets.ISO_8859_1)
    }
}
