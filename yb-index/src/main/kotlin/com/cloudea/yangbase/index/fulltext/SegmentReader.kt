package com.cloudea.yangbase.index.fulltext

import com.cloudea.yangbase.common.CorruptDataException
import com.cloudea.yangbase.common.IndexId
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.index.IndexKeys
import com.cloudea.yangbase.objectstore.ByteRange
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.storage.BlockCache
import kotlinx.serialization.cbor.Cbor

/**
 * 一个倒排段的读取器。
 *
 * **内存常驻的部分（P3：小而热的元数据）**：`terms.fst`（词典）、`postings.idx`
 * （词条记录 + 块记录）、`docmap.idx`（每文档偏移）。这三者加载一次后
 * 在 [SegmentReader] 的生命周期内不再产生任何请求——[FullTextIndex] 会按段缓存
 * 读取器，因此**稳态查询的前缀展开、词条定位、块级剪枝全是 0 IO**。
 *
 * **按需读取的部分**：`postings.bin` 的块（每块 1 次范围 GET，走 [BlockCache]
 * 的在途去重）、`docmap.bin`（小则整读，大则按记录范围读）、`docts.bin`
 * （只在段跨越 read_ts 时读）、删除集（有删除时才读）。
 */
class SegmentReader(
    private val store: ObjectStore,
    private val cache: BlockCache,
    val indexId: IndexId,
    val meta: SegmentMeta,
    /** `docmap.bin` 超过该体积就不整体读入，改成按记录范围读。 */
    private val docMapInlineMax: Long = 4L * 1024 * 1024,
) {
    private val cbor = Cbor { ignoreUnknownKeys = true }

    val segId: String get() = meta.segId
    val docCount: Int get() = meta.docCount

    private val fstKey = IndexKeys.segmentFile(indexId, segId, IndexKeys.TERMS_FST)
    private val postingsBinKey = IndexKeys.segmentFile(indexId, segId, IndexKeys.POSTINGS_BIN)
    private val postingsIdxKey = IndexKeys.segmentFile(indexId, segId, IndexKeys.POSTINGS_IDX)
    private val docmapBinKey = IndexKeys.segmentFile(indexId, segId, IndexKeys.DOCMAP_BIN)
    private val docmapIdxKey = IndexKeys.segmentFile(indexId, segId, IndexKeys.DOCMAP_IDX)
    private val doctsKey = IndexKeys.segmentFile(indexId, segId, IndexKeys.DOCTS_BIN)

    @Volatile private var fstObj: Fst? = null
    @Volatile private var termTableObj: PostingsFormat.TermTable? = null
    @Volatile private var blockStartsObj: IntArray? = null
    @Volatile private var pkIndexObj: HashMap<String, Int>? = null
    @Volatile private var docOffsetsObj: IntArray? = null
    @Volatile private var docMapInlineObj: ByteArray? = null
    @Volatile private var docMapInlineLoaded: Boolean = false
    @Volatile private var docVersionsObj: PostingsFormat.DocVersions? = null
    @Volatile private var deleteEntriesObj: List<DeleteEntry>? = null
    @Volatile private var deleteEntriesLoaded: Boolean = false

    // ── 常驻内存的元数据 ────────────────────────────────────────────

    suspend fun fst(): Fst {
        fstObj?.let { return it }
        val bytes = cache.getOrLoad(cacheKey(indexId, segId, IndexKeys.TERMS_FST)) { store.get(fstKey) }
        val f = Fst.fromBytes(bytes)
        fstObj = f
        return f
    }

    suspend fun termTable(): PostingsFormat.TermTable {
        termTableObj?.let { return it }
        val bytes = cache.getOrLoad(cacheKey(indexId, segId, IndexKeys.POSTINGS_IDX)) { store.get(postingsIdxKey) }
        val t = PostingsFormat.decodePostingsIdx(bytes)
        if (t.terms.size != meta.termCount) {
            throw CorruptDataException(
                "段 $segId 的 postings.idx 词条数 ${t.terms.size} 与元信息 ${meta.termCount} 不一致"
            )
        }
        // 块记录按词条分组存放，只有一个前缀和才能把它切回「每个词条自己的块」
        val starts = IntArray(t.terms.size + 1)
        for (i in t.terms.indices) starts[i + 1] = starts[i] + t.terms[i].blockCount
        if (starts[t.terms.size] != t.blocks.size) {
            throw CorruptDataException(
                "段 $segId 的 postings.idx 块记录数 ${t.blocks.size} 与各词条 blockCount 之和 " +
                    "${starts[t.terms.size]} 不一致"
            )
        }
        blockStartsObj = starts
        termTableObj = t
        return t
    }

    /**
     * 某词条在块记录数组里的起点 `[start, start + rec.blockCount)`（0 IO，常驻）。
     * 调用前必须已经加载过 [termTable]。
     */
    suspend fun blockStart(rec: PostingsFormat.TermRecord): Int {
        val starts = blockStartsObj ?: run { termTable(); blockStartsObj!! }
        if (rec.ordinal < 0 || rec.ordinal + 1 >= starts.size) {
            throw CorruptDataException("段 $segId: 词条 ordinal ${rec.ordinal} 越界")
        }
        return starts[rec.ordinal]
    }

    /** 词条 → ordinal（0 IO：只查常驻的 FST）。不存在返回 null。 */
    suspend fun ordinalOf(term: String): Int? = fst().get(term)

    /** 前缀枚举（0 IO）。[limit] 用尽时返回 false 表示还有更多。 */
    suspend fun termsWithPrefix(prefix: String, limit: Int): Pair<List<Pair<String, Int>>, Boolean> {
        val out = ArrayList<Pair<String, Int>>()
        var truncated = false
        fst().forEachTerm(prefix) { t, o ->
            if (out.size >= limit) { truncated = true; false } else { out.add(t to o); true }
        }
        return out to truncated
    }

    // ── 倒排块 ──────────────────────────────────────────────────────

    /**
     * 读取一个倒排块。**这是全文检索唯一的数据读取路径**：
     * 一次范围 GET，只取该块的几十到几百字节（块越大越省请求，越小越省带宽）。
     */
    suspend fun readBlock(t: PostingsFormat.TermRecord, b: PostingsFormat.BlockRecord): PostingsFormat.BlockData {
        val off = t.postingsOffset.toLong() + b.offset
        val raw = cache.getOrLoad(blockCacheKey(indexId, segId, off)) {
            store.get(postingsBinKey, ByteRange.ofLength(off, b.bytes))
        }
        return PostingsFormat.decodeBlock(raw, meta.hasPositions, "段 $segId postings 块")
    }

    // ── 文档映射与版本 ──────────────────────────────────────────────

    private suspend fun docMapInline(): ByteArray? {
        if (docMapInlineLoaded) return docMapInlineObj
        val info = meta.files[IndexKeys.DOCMAP_BIN]
        if (info == null || info.bytes > docMapInlineMax) {
            docMapInlineLoaded = true
            return null
        }
        val bytes = cache.getOrLoad(cacheKey(indexId, segId, IndexKeys.DOCMAP_BIN)) { store.get(docmapBinKey) }
        docMapInlineObj = bytes
        docMapInlineLoaded = true
        return bytes
    }

    suspend fun docOffsets(): IntArray {
        docOffsetsObj?.let { return it }
        val bytes = cache.getOrLoad(cacheKey(indexId, segId, IndexKeys.DOCMAP_IDX)) { store.get(docmapIdxKey) }
        val o = PostingsFormat.decodeDocmapIdx(bytes)
        if (o.size != meta.docCount + 1) {
            throw CorruptDataException(
                "段 $segId 的 docmap.idx 条目数 ${o.size} 与 docCount=${meta.docCount} 不一致"
            )
        }
        docOffsetsObj = o
        return o
    }

    /** 读取某文档对应的主键。小段整读（1 次 GET 覆盖全部命中），大段按记录范围读。 */
    suspend fun primaryKey(docId: Int): ByteArray {
        require(docId >= 0 && docId < meta.docCount) { "docId 越界: $docId / ${meta.docCount}" }
        val offsets = docOffsets()
        val off = offsets[docId]
        val end = offsets[docId + 1]
        docMapInline()?.let { inline ->
            return PostingsFormat.decodeDocmapRecord(inline, off, end)
        }
        val raw = cache.getOrLoad("dmap:$docmapBinKey#$off") {
            store.get(docmapBinKey, ByteRange(off.toLong(), end - 1L))
        }
        return PostingsFormat.decodeDocmapRecord(raw, 0, raw.size)
    }

    /**
     * 主键 → 段内 docId；不存在返回 null。
     *
     * **只在写入路径需要**（覆盖写要标记旧版本、删除要定位文档），检索路径从不调用它，
     * 所以它的内存开销不污染查询（P3）。
     *
     * 代价：整读一次 `docmap.bin`（1 次 GET，之后走 [BlockCache]）+ 一张哈希表。
     * 表用 ISO-8859-1 字符串做键（逐字节无损、哈希按内容），每条约 pk 长度 + 40 B
     * 固定开销——2M 文档的大段最坏约 80 MB。**删除密集的场景应靠合并段（§8.6.6），
     * 而不是在这个表上反复点删**。
     */
    suspend fun docIdOf(pk: ByteArray): Int? {
        pkIndexObj?.let { return it[keyOf(pk)] }
        val bytes = cache.getOrLoad(cacheKey(indexId, segId, IndexKeys.DOCMAP_BIN)) { store.get(docmapBinKey) }
        val offsets = docOffsets()
        val map = HashMap<String, Int>((meta.docCount * 4 / 3) + 1)
        for (d in 0 until meta.docCount) {
            val record = PostingsFormat.decodeDocmapRecord(bytes, offsets[d], offsets[d + 1])
            map[String(record, Charsets.ISO_8859_1)] = d
        }
        pkIndexObj = map
        return map[keyOf(pk)]
    }

    private fun keyOf(pk: ByteArray): String = String(pk, Charsets.ISO_8859_1)

    /** 每文档的 commit_ts 与字段长度（整文件读；只在段跨越 read_ts 或合并段时需要）。 */
    suspend fun docVersions(): PostingsFormat.DocVersions {        docVersionsObj?.let { return it }
        val bytes = cache.getOrLoad(cacheKey(indexId, segId, IndexKeys.DOCTS_BIN)) { store.get(doctsKey) }
        val dv = PostingsFormat.decodeDocTs(bytes)
        if (dv.size != meta.docCount) {
            throw CorruptDataException("段 $segId 的 docts 条数 ${dv.size} 与 docCount=${meta.docCount} 不一致")
        }
        docVersionsObj = dv
        return dv
    }

    suspend fun commitTsOf(docId: Int): Long = docVersions().ts[docId]

    // ── 删除集 ──────────────────────────────────────────────────────

    /** 该段当前生效的删除集序号；manifest 里没有就是「从未删除过」（0 IO）。 */
    private var deleteSeq: Long? = null

    /**
     * 切换到 manifest 里登记的删除集序号。
     *
     * ⚠️ 序号**变化时必须丢掉已解析的删除集**。段是不可变的，但删除集不是：
     * 新增删除 = 写一个**新序号**的对象（P2）。不失效就会让读取器永远停在
     * 首次看到的那份删除集上，结果是「已经删掉的行仍然被检索到」——
     * 不报错、不降级，只是给出过期答案（§8.6.5 明确要避免的那类错误）。
     */
    fun useDeletes(seq: Long?) {
        if (seq == deleteSeq) return
        deleteSeq = seq
        deleteEntriesObj = null
        deleteEntriesLoaded = false
    }

    suspend fun deleteEntries(): List<DeleteEntry> {
        if (deleteEntriesLoaded) return deleteEntriesObj ?: emptyList()
        val seq = deleteSeq
        if (seq == null) {
            deleteEntriesLoaded = true
            deleteEntriesObj = emptyList()
            return emptyList()
        }
        val key = IndexKeys.deletes(indexId, segId, seq)
        val bytes = try {
            cache.getOrLoad(deleteCacheKey(indexId, segId, seq)) { store.get(key) }
        } catch (_: ObjectNotFoundException) {
            throw CorruptDataException("段 $segId 的删除集缺失: $key（manifest 指向的序号不存在）")
        }
        val set = cbor.decodeFromByteArray(DeleteSet.serializer(), bytes)
        val list = set.entries.sortedBy { it.commitTs }
        deleteEntriesObj = list
        deleteEntriesLoaded = true
        return list
    }

    /** `read_ts` 时点已生效的删除文档（升序）。 */
    suspend fun deletedDocs(readTs: Long): IntArray {
        val entries = deleteEntries()
        val sets = entries.filter { it.commitTs <= readTs }.map { PostingsFormat.decodeDocIdSet(it.docs) }
        if (sets.isEmpty()) return EMPTY_INT
        if (sets.size == 1) return sets[0]
        val all = java.util.TreeSet<Int>()
        for (s in sets) for (d in s) all.add(d)
        return all.toIntArray()
    }

    /** 清掉全部解析结果（段被替换/删除时调用）。 */
    fun invalidate() {
        fstObj = null; termTableObj = null; blockStartsObj = null; pkIndexObj = null
        docOffsetsObj = null
        docMapInlineObj = null; docMapInlineLoaded = false
        docVersionsObj = null; deleteEntriesObj = null; deleteEntriesLoaded = false
    }

    override fun toString(): String = "Segment($segId docs=${meta.docCount} terms=${meta.termCount})"

    companion object {
        private val EMPTY_INT = IntArray(0)

        /**
         * 段内对象在 [BlockCache] 里的键。
         *
         * **写入方必须用同一套键**（[SegmentBuilder] 就是这么做的）：否则写者刚缓存过的
         * 对象，读者还要再 GET 一次——同一个进程里刚建完段就检索是最常见的情形，
         * 那 5 次多出来的请求正好落在热路径上（P4）。
         * 唯一**刻意不进缓存**的是 `postings.bin`：它按块范围读，缓存整个文件既浪费又永不命中。
         */
        internal fun cacheKey(index: IndexId, segId: String, fileName: String): String {
            val prefix = when (fileName) {
                IndexKeys.TERMS_FST -> "fst"
                IndexKeys.POSTINGS_IDX -> "pidx"
                IndexKeys.DOCMAP_BIN -> "dmap"
                IndexKeys.DOCMAP_IDX -> "dmapidx"
                IndexKeys.DOCTS_BIN -> "dts"
                IndexKeys.SEG_META -> "meta"
                else -> throw IllegalArgumentException("该文件按块读取，不进整对象缓存: $fileName")
            }
            return "$prefix:${IndexKeys.segmentFile(index, segId, fileName)}"
        }

        /** 一个倒排块在缓存里的键（块不可变，偏移即身份）。 */
        internal fun blockCacheKey(index: IndexId, segId: String, offset: Long): String =
            IndexKeys.segmentFile(index, segId, IndexKeys.POSTINGS_BIN) + "#" + offset

        /** 删除集在缓存里的键。 */
        internal fun deleteCacheKey(index: IndexId, segId: String, seq: Long): String =
            "del:" + IndexKeys.deletes(index, segId, seq)
    }
}
