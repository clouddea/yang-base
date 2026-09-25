package com.cloudea.yangbase.index.fulltext

import com.cloudea.yangbase.common.Checksums
import com.cloudea.yangbase.common.IndexId
import com.cloudea.yangbase.index.IndexKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.storage.BlockCache
import java.io.ByteArrayOutputStream

/**
 * 段构建器（内存中构建 → 一次性写出不可变对象）。
 *
 * 两条构建路径：
 *  - **从文本建索引**：[addDocument] → 分析器切词 → 归并成倒排；
 *  - **从已有段合并**：[addDocumentKey] + [addPostings] → K 路归并
 *    （§8.6.6：合并时做真正的多路归并 term 与 docid，**不重新分词**，
 *    因此位置信息不会丢，代价也不翻倍）。
 *
 * ⚠️ **构建期是全内存的**：倒排表在写之前整份留在堆里。设计文档的段大小目标
 * 是 8–64 MiB，对应几百 MB 的构建内存；对更大的段需要改成外部排序 + 流式写。
 * 这是当前实现明确的限制（见交付报告），不是"以后再优化"的乐观假设。
 */
class SegmentBuilder(
    private val segId: String,
    private val field: String,
    /** 分析器标识，写进段元信息供检索期自校验。合并路径传 null 表示沿用来源段。 */
    private val analyzerName: String?,
    private val blockDocs: Int = DEFAULT_BLOCK_DOCS,
    private val hasPositions: Boolean = true,
    private val params: Bm25Params = Bm25Params(),
    /** 段内文档数上限。docmap.idx 常驻内存的开销是 4 B/文档，上限就是内存上限。 */
    private val maxDocs: Int = DEFAULT_MAX_DOCS,
) {
    init {
        require(blockDocs >= 1) { "blockDocs 必须 >= 1" }
        require(segId.isNotEmpty()) { "segId 不能为空" }
    }

    private class TermPostings {
        val docIds = IntVec()
        val tfs = IntVec()
        val dls = IntVec()
        /** 与 docIds 等长；元素为 null 表示该文档无词位。 */
        val positions = ArrayList<IntArray?>()
    }

    private val docsPk = ArrayList<ByteArray>()
    private val docsTs = ArrayList<Long>()
    private val docLengths = ArrayList<Int>()
    private val postings = HashMap<String, TermPostings>()
    private var totalTokens = 0L
    private var minTs = Long.MAX_VALUE
    private var maxTs = Long.MIN_VALUE

    val docCount: Int get() = docsPk.size
    val termCount: Int get() = postings.size
    val bufferedTokens: Long get() = totalTokens

    /** 粗略的内存占用估计，供调用方决定何时切段。 */
    fun estimatedBytes(): Long {
        var n = 0L
        for (tp in postings.values) {
            n += tp.docIds.size * (4 + 4 + 4)
            for (p in tp.positions) n += (p?.size ?: 0) * 4
            n += 48
        }
        return n
    }

    // ── 路径一：从文本建索引 ────────────────────────────────────────

    /**
     * 加入一篇文档。[text] 由调用方先取好（索引器不接触数据面）。
     * 返回 docId（段内密集递增）。
     */
    fun addDocument(pk: ByteArray, commitTs: Long, text: String, analyzer: Analyzer): Int {
        val tokens = analyzer.analyze(text)
        val dl = tokens.size
        val docId = addDocumentKey(pk, commitTs, dl)
        if (tokens.isEmpty()) return docId

        // tokens 已按词位升序，同词条的位置天然有序
        val counts = HashMap<String, Int>()
        for (t in tokens) counts[t.term] = (counts[t.term] ?: 0) + 1
        val posBuf = HashMap<String, IntVec>()
        for (t in tokens) posBuf.getOrPut(t.term) { IntVec() }.add(t.position)
        for ((term, c) in counts) {
            addPostings(
                term = term,
                docId = docId,
                tf = c,
                dl = dl,
                positions = if (hasPositions) posBuf[term]!!.toIntArray() else null,
            )
        }
        return docId
    }

    // ── 路径二：从已有段归并 ────────────────────────────────────────

    /** 登记一个文档（docId 由调用顺序决定）。[docLength] 是该文档的字段长度（无词条时为 0）。 */
    fun addDocumentKey(pk: ByteArray, commitTs: Long, docLength: Int): Int {
        require(docsPk.size < maxDocs) {
            "段 $segId 的文档数达到上限 $maxDocs；请切新段（docmap.idx 是每文档 4 字节的常驻元数据）"
        }
        require(commitTs >= 0) { "commitTs 必须非负: $commitTs" }
        require(docLength >= 0) { "字段长度必须非负: $docLength" }
        val docId = docsPk.size
        docsPk.add(pk)
        docsTs.add(commitTs)
        docLengths.add(docLength)
        totalTokens += docLength
        if (commitTs < minTs) minTs = commitTs
        if (commitTs > maxTs) maxTs = commitTs
        return docId
    }

    /**
     * 追加一条倒排项。合并路径专用：docId 必须按**严格升序**逐文档调用，
     * 且同一文档的 dl 必须与其它词条一致（它来自同一个源文档）。
     */
    fun addPostings(term: String, docId: Int, tf: Int, dl: Int, positions: IntArray?) {
        require(term.isNotEmpty()) { "词条不能为空" }
        require(tf >= 1) { "tf 必须 >= 1: tf=$tf term=$term" }
        require(dl >= 1) { "字段长度必须 >= 1: dl=$dl term=$term" }
        require(docId in 0 until docsPk.size) { "addPostings 前必须先 addDocumentKey: docId=$docId" }
        val tp = postings.getOrPut(term) { TermPostings() }
        if (tp.docIds.size > 0) {
            val prev = tp.docIds.last()
            require(docId > prev) { "同一词条的 docId 必须严格升序: term=$term prev=$prev now=$docId" }
        }
        tp.docIds.add(docId)
        tp.tfs.add(tf)
        tp.dls.add(dl)
        if (hasPositions) {
            val ps = positions ?: IntArray(0)
            for (i in 1 until ps.size) require(ps[i] > ps[i - 1]) { "同一文档内词位必须严格升序: $term" }
            require(ps.size == tf) { "词位数 ${ps.size} 与 tf $tf 不符: $term" }
            tp.positions.add(ps)
        }
    }

    // ── 写出 ────────────────────────────────────────────────────────

    /**
     * 写出段的全部对象。**对象总大小 = 段体积**，调用方负责用 manifest 指针切换
     * 让它生效（[FullTextIndex] 负责）。
     *
     * 请求数：每个段 6 次 PUT（postings.bin / postings.idx / terms.fst /
     * docmap.bin / docmap.idx / docts.bin / meta.cbor 共 7 个对象，其中 postings.bin 通常最大）。
     */
    suspend fun write(store: ObjectStore, cache: BlockCache, indexId: IndexId): SegmentMeta {
        require(docsPk.isNotEmpty()) { "段 $segId 没有任何文档" }
        require(analyzerName != null) { "合并路径必须显式给出分析器标识" }
        val files = LinkedHashMap<String, FileInfo>()

        val sortedTerms = postings.keys.sorted()
        val termRecords = ArrayList<PostingsFormat.TermRecord>(sortedTerms.size)
        val blockRecords = ArrayList<PostingsFormat.BlockRecord>()

        val postingsOut = ByteArrayOutputStream(1 shl 16)
        for ((ordinal, term) in sortedTerms.withIndex()) {
            val tp = postings.getValue(term)
            val offset = postingsOut.size()
            var maxTf = 0
            var minDl = Int.MAX_VALUE
            var totalTf = 0
            var blockCount = 0
            var i = 0
            while (i < tp.docIds.size) {
                val end = minOf(i + blockDocs, tp.docIds.size)
                val n = end - i
                val docIds = IntArray(n)
                val tfs = IntArray(n)
                val dls = IntArray(n)
                val poss = if (hasPositions) arrayOfNulls<IntArray>(n) else null
                var blkMaxTf = 0
                var blkMinDl = Int.MAX_VALUE
                for (k in 0 until n) {
                    docIds[k] = tp.docIds[i + k]
                    tfs[k] = tp.tfs[i + k]
                    dls[k] = tp.dls[i + k]
                    if (hasPositions) poss!![k] = tp.positions[i + k]
                    if (tfs[k] > blkMaxTf) blkMaxTf = tfs[k]
                    if (dls[k] < blkMinDl) blkMinDl = dls[k]
                    totalTf += tfs[k]
                    if (tfs[k] > maxTf) maxTf = tfs[k]
                    if (dls[k] < minDl) minDl = dls[k]
                }
                val bytes = PostingsFormat.encodeBlock(docIds, tfs, dls, poss)
                postingsOut.write(bytes)
                blockRecords.add(
                    PostingsFormat.BlockRecord(
                        lastDocId = docIds[n - 1],
                        maxTf = blkMaxTf,
                        minDl = blkMinDl,
                        offset = postingsOut.size() - offset - bytes.size,
                        bytes = bytes.size,
                    )
                )
                blockCount++
                i = end
            }
            termRecords.add(
                PostingsFormat.TermRecord(
                    ordinal = ordinal,
                    df = tp.docIds.size,
                    blockCount = blockCount,
                    totalTf = totalTf,
                    maxTf = maxTf,
                    minDl = if (minDl == Int.MAX_VALUE) 1 else minDl,
                    postingsOffset = offset,
                    postingsBytes = postingsOut.size() - offset,
                )
            )
        }

        // postings.bin 可能很大：不进缓存（读路径按块范围读，缓存整个文件既浪费又永不命中）
        put(store, cache, indexId, files, IndexKeys.POSTINGS_BIN, postingsOut.toByteArray(), cacheIt = false)

        val postingsIdx = PostingsFormat.encodePostingsIdx(termRecords, blockRecords)
        put(store, cache, indexId, files, IndexKeys.POSTINGS_IDX, postingsIdx)

        val fstBuilder = FstBuilder()
        for ((ordinal, term) in sortedTerms.withIndex()) fstBuilder.add(term, ordinal)
        put(store, cache, indexId, files, IndexKeys.TERMS_FST, fstBuilder.build().toBytes())

        // docmap：docId → 主键；索引文件带哨兵（末条 = 文件长度），便于按记录范围读
        val docmapOut = ByteArrayOutputStream(1 shl 12)
        val offsets = IntArray(docsPk.size + 1)
        for (i in docsPk.indices) {
            offsets[i] = docmapOut.size()
            docmapOut.write(PostingsFormat.encodeDocmapRecord(docsPk[i]))
        }
        offsets[docsPk.size] = docmapOut.size()
        put(store, cache, indexId, files, IndexKeys.DOCMAP_BIN, docmapOut.toByteArray())
        put(store, cache, indexId, files, IndexKeys.DOCMAP_IDX, PostingsFormat.encodeDocmapIdx(offsets))

        put(
            store, cache, indexId, files, IndexKeys.DOCTS_BIN,
            PostingsFormat.encodeDocTs(docsTs.toLongArray(), docLengths.toIntArray(), minTs),
        )

        val meta = SegmentMeta(
            segId = segId,
            indexId = indexId.value,
            field = field,
            analyzer = analyzerName,
            docCount = docsPk.size,
            minTs = minTs,
            maxTs = maxTs,
            termCount = sortedTerms.size,
            totalTokens = totalTokens,
            blockDocs = blockDocs,
            hasPositions = hasPositions && sortedTerms.isNotEmpty(),
            files = files,
            k1 = params.k1,
            b = params.b,
        )
        val metaBytes = cbor().encodeToByteArray(SegmentMeta.serializer(), meta)
        val metaKey = IndexKeys.segmentFile(indexId, segId, IndexKeys.SEG_META)
        store.put(metaKey, metaBytes)
        cache.put(SegmentReader.cacheKey(indexId, segId, IndexKeys.SEG_META), metaBytes)
        return meta
    }

    /**
     * 写一个段内对象并登记它的体积/校验和。
     *
     * 写完后按**读取器的缓存键**放进 [BlockCache]（[SegmentReader.cacheKey]）：
     * 同一个进程紧接着检索这个新段是最常见的情形，键不一致会让那几次 GET 白跑（P4）。
     * `postings.bin` 例外（`cacheIt=false`）——它按块范围读，整份缓存永不命中。
     */
    private suspend fun put(
        store: ObjectStore,
        cache: BlockCache,
        indexId: IndexId,
        files: MutableMap<String, FileInfo>,
        name: String,
        bytes: ByteArray,
        cacheIt: Boolean = true,
    ) {
        val key = IndexKeys.segmentFile(indexId, segId, name)
        store.put(key, bytes)
        if (cacheIt) cache.put(SegmentReader.cacheKey(indexId, segId, name), bytes)
        files[name] = FileInfo(bytes.size.toLong(), Checksums.crc32c(bytes))
    }

    companion object {
        const val DEFAULT_BLOCK_DOCS = 128
        /** docmap.idx 常驻内存 4 B/文档；2M 文档 = 8 MB，是 P3 的合理上限。 */
        const val DEFAULT_MAX_DOCS = 1 shl 21

        private fun cbor() = kotlinx.serialization.cbor.Cbor { ignoreUnknownKeys = true }
    }
}

/** 极简可增长 int 数组（热路径避免装箱）。 */
internal class IntVec(initial: Int = 4) {
    private var data = IntArray(initial)
    var size: Int = 0
        private set

    fun add(v: Int) {
        if (size == data.size) data = data.copyOf(data.size * 2)
        data[size++] = v
    }

    fun last(): Int = data[size - 1]

    operator fun get(i: Int): Int = data[i]

    fun toIntArray(): IntArray = data.copyOf(size)
}
