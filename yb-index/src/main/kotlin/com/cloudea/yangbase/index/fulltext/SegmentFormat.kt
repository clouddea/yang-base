package com.cloudea.yangbase.index.fulltext

import com.cloudea.yangbase.common.BigEndian
import com.cloudea.yangbase.common.Checksums
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.Varint
import kotlinx.serialization.Serializable
import java.io.ByteArrayOutputStream

// ── 元信息（CBOR：加载一次后常驻内存，可读性优先）─────────────────────

/** 段内一个文件的清单项。用于一致性校验与体积统计。 */
@Serializable
data class FileInfo(
    val bytes: Long,
    val crc: Int,
    val records: Int = 0,
)

/** 一个**不可变**的倒排段（§8.6.1）。 */
@Serializable
data class SegmentMeta(
    val formatVersion: Int = FORMAT_VERSION,
    val segId: String,
    val indexId: Long,
    /** 被索引的字段名（一个段只索引一个字段）。 */
    val field: String,
    /** 分析器标识。**检索时逐段校验**，防止索引期/查询期分析器不一致导致静默空结果。 */
    val analyzer: String,
    val docCount: Int,
    /** 段内文档 commit_ts 的区间（段级剪枝的依据）。 */
    val minTs: Long,
    val maxTs: Long,
    val termCount: Int,
    /** 全部文档的字段长度之和（BM25 的 avgdl 分子）。 */
    val totalTokens: Long,
    /** postings 每块的文档数。 */
    val blockDocs: Int,
    /** 是否记录了词位（短语查询需要）。 */
    val hasPositions: Boolean,
    val files: Map<String, FileInfo>,
    /** 构建时的 BM25 参数快照（仅诊断用；检索按当前参数重算，不读它）。 */
    val k1: Double,
    val b: Double,
    val createdAt: Long = 0,
) {
    val avgFieldLength: Double get() = if (docCount == 0) 0.0 else totalTokens.toDouble() / docCount

    /** 段是否整体落在 [ts] 之后（不可见）。 */
    fun isAfter(ts: Long): Boolean = minTs > ts

    /** 段是否整体不晚于 [ts]（内部所有版本都可见，只受 deletes 影响）。 */
    fun isFullyVisibleAt(ts: Long): Boolean = maxTs <= ts

    /** 是否跨越 [ts]：需要逐文档校验 commit_ts。 */
    fun straddles(ts: Long): Boolean = !isAfter(ts) && !isFullyVisibleAt(ts)

    companion object {
        const val FORMAT_VERSION = 1
    }
}

/** manifest 里对一个段的引用（只含检索时**不需要额外 IO** 就能用的统计量）。 */
@Serializable
data class SegmentRef(
    val segId: String,
    val docCount: Int,
    val minTs: Long,
    val maxTs: Long,
    val termCount: Int,
    val totalTokens: Long,
    val bytes: Long,
    val field: String,
    val analyzer: String,
)

/** 索引的当前状态。指向一组段 + 各自的删除集序号。 */
@Serializable
data class IndexManifest(
    val formatVersion: Int = 1,
    val indexId: Long,
    val seq: Long,
    /** 建索引时的表结构版本；不匹配说明索引需要重建。 */
    val schemaVersion: Long,
    val segments: List<SegmentRef>,
    /** 每个段当前生效的删除集序号（§8.6.5）。 */
    val deletesSeq: Map<String, Long> = emptyMap(),
    val updatedAt: Long = 0,
) {
    val docCount: Long get() = segments.sumOf { it.docCount.toLong() }
    val totalTokens: Long get() = segments.sumOf { it.totalTokens }
}

/** `manifest/CURRENT` 的内容。刻意极小。 */
@Serializable
data class IndexManifestPointer(
    val seq: Long,
    val path: String,
    /** 写者代际，用于 fencing（与 §6.3 的写者租约同源）。 */
    val writerEpoch: Long,
)

/**
 * 一批删除。
 *
 * **一条记录的生效时间是一个精确的 commit_ts，不是区间。** 这是刻意的：
 * 区间只有在「读者 `read_ts >= tsTo`」时才能整体生效，而读者可能落在区间内部；
 * 那时需要逐文档的删除时间才能判定，而本结构不携带它。用单 ts 记录，
 * 判定就退化成一次简单的 `commitTs <= readTs` 比较——**精确且不可能漏判**。
 * （设计文档 §8.6.5 画的 `(ts: 1000..1050)` 区间是同一语义的压缩表达，
 * 压缩的前提是每个区间内所有删除的 commit_ts 都 ≤ 任何会读它的读者，无法保证。）
 */
@Serializable
data class DeleteSet(
    val formatVersion: Int = 1,
    val segId: String,
    /** 按 commitTs 升序。 */
    val entries: List<DeleteEntry>,
)

@Serializable
data class DeleteEntry(
    val commitTs: Long,
    /** `DocIdSet` 编码（升序 docId 的 delta varint 序列）。 */
    val docs: ByteArray,
) {
    override fun equals(other: Any?): Boolean =
        other is DeleteEntry && commitTs == other.commitTs && docs.contentEquals(other.docs)

    override fun hashCode(): Int = 31 * commitTs.hashCode() + docs.contentHashCode()
}

/** docId → 主键 + 版本。检索命中后用它回表。 */
class DocRef(val pk: ByteArray, val commitTs: Long) {
    override fun toString(): String = "DocRef(${pk.size}B, ts=$commitTs)"
}

// ── 二进制布局（热路径：手写二进制，格式要精确可控）───────────────────

/**
 * 各文件的字节布局。
 *
 * ```
 * postings.idx
 *   magic "YBPI" | version(Int) | termCount(Int) | blockCount(Int) | crc32c(Int)
 *   term 记录 × termCount（32 B，按 ordinal 升序）
 *   block 记录 × blockCount（20 B，按 term 分组、组内按 docid 升序）
 *
 * postings.bin（按块范围读）
 *   block := firstDocId:varint | (docDelta:varint tf:varint dl:varint)*
 *            [ posCount:varint (posDelta:varint)* ]* | crc32c(Int)
 *
 * docmap.idx
 *   magic "YBDM" | version(Int) | docCount(Int) | crc32c(Int) | offset:Int × docCount
 * docmap.bin
 *   doc 记录 × docCount := pkLen:varint pk 字节
 * docts.bin
 *   magic "YBTS" | version(Int) | docCount(Int) | minTs(Long) |
 *   zigzag-delta varint × docCount（commit_ts） | varint × docCount（字段长度 dl） | crc32c(Int)
 * ```
 *
 * 每块末尾带 CRC32C：块是**按范围**读的，整文件校验和帮不上忙，
 * 只有块内校验和才能在部分读时发现损坏（与 SST 的做法一致）。
 */
object PostingsFormat {

    val POSTINGS_IDX_MAGIC = byteArrayOf('Y'.code.toByte(), 'B'.code.toByte(), 'P'.code.toByte(), 'I'.code.toByte())
    val DOCMAP_IDX_MAGIC = byteArrayOf('Y'.code.toByte(), 'B'.code.toByte(), 'D'.code.toByte(), 'M'.code.toByte())
    val DOCTS_MAGIC = byteArrayOf('Y'.code.toByte(), 'B'.code.toByte(), 'T'.code.toByte(), 'S'.code.toByte())
    const val VERSION = 1

    const val TERM_RECORD_BYTES = 32
    const val BLOCK_RECORD_BYTES = 20

    /**
     * term 记录。`ordinal` 便于自校验（FST 的输出就是它）。
     *
     * field 语义：`df` 文档频率；`totalTf` 该词条在段内的 tf 总和；
     * `maxTf` / `minDl` 是**全局**上界用的（BM25 的分数上界随 tf 增、随 dl 减，
     * 所以 `(maxTf, minDl)` 给出该词条在段内的分数上界）。
     */
    class TermRecord(
        val ordinal: Int,
        val df: Int,
        val blockCount: Int,
        val totalTf: Int,
        val maxTf: Int,
        val minDl: Int,
        /** 该词条 postings 在 postings.bin 中的起始偏移。 */
        val postingsOffset: Int,
        val postingsBytes: Int,
    ) {
        fun writeTo(out: ByteArrayOutputStream) {
            BigEndian.writeInt(out, ordinal)
            BigEndian.writeInt(out, df)
            BigEndian.writeInt(out, blockCount)
            BigEndian.writeInt(out, totalTf)
            BigEndian.writeInt(out, maxTf)
            BigEndian.writeInt(out, minDl)
            BigEndian.writeInt(out, postingsOffset)
            BigEndian.writeInt(out, postingsBytes)
        }

        companion object {
            fun readFrom(buf: ByteArray, off: Int) = TermRecord(
                ordinal = BigEndian.readInt(buf, off),
                df = BigEndian.readInt(buf, off + 4),
                blockCount = BigEndian.readInt(buf, off + 8),
                totalTf = BigEndian.readInt(buf, off + 12),
                maxTf = BigEndian.readInt(buf, off + 16),
                minDl = BigEndian.readInt(buf, off + 20),
                postingsOffset = BigEndian.readInt(buf, off + 24),
                postingsBytes = BigEndian.readInt(buf, off + 28),
            )
        }
    }

    /**
     * 块记录。对应设计文档 §8.6.2 的 `{offset, length, first_docid, last_docid, max_score_upper_bound}`：
     * 上界不直接存分数，而是存算它需要的 `(maxTf, minDl)`——分数上界随 BM25 参数变化，
     * 存原料才能让参数改动后依然正确（存分数的话，换参数就会变成**不安全的上界**）。
     */
    class BlockRecord(
        val lastDocId: Int,
        val maxTf: Int,
        val minDl: Int,
        val offset: Int,
        val bytes: Int,
    ) {
        fun writeTo(out: ByteArrayOutputStream) {
            BigEndian.writeInt(out, lastDocId)
            BigEndian.writeInt(out, maxTf)
            BigEndian.writeInt(out, minDl)
            BigEndian.writeInt(out, offset)
            BigEndian.writeInt(out, bytes)
        }

        companion object {
            fun readFrom(buf: ByteArray, off: Int) = BlockRecord(
                lastDocId = BigEndian.readInt(buf, off),
                maxTf = BigEndian.readInt(buf, off + 4),
                minDl = BigEndian.readInt(buf, off + 8),
                offset = BigEndian.readInt(buf, off + 12),
                bytes = BigEndian.readInt(buf, off + 16),
            )
        }
    }

    // ── postings.idx ────────────────────────────────────────────────

    fun encodePostingsIdx(terms: List<TermRecord>, blocks: List<BlockRecord>): ByteArray {
        val out = ByteArrayOutputStream(HEADER + terms.size * TERM_RECORD_BYTES + blocks.size * BLOCK_RECORD_BYTES + 4)
        out.write(POSTINGS_IDX_MAGIC)
        BigEndian.writeInt(out, VERSION)
        BigEndian.writeInt(out, terms.size)
        BigEndian.writeInt(out, blocks.size)
        for (t in terms) t.writeTo(out)
        for (b in blocks) b.writeTo(out)
        return withTrailingCrc(out)
    }

    class TermTable(
        val terms: List<TermRecord>,
        val blocks: List<BlockRecord>,
    ) {
        fun term(ordinal: Int): TermRecord = terms[ordinal]
    }

    fun decodePostingsIdx(bytes: ByteArray): TermTable {
        requireMagic(bytes, POSTINGS_IDX_MAGIC, "postings.idx")
        val version = BigEndian.readInt(bytes, 4)
        require(version == VERSION) { "postings.idx 版本不支持: $version" }
        val termCount = BigEndian.readInt(bytes, 8)
        val blockCount = BigEndian.readInt(bytes, 12)
        val need = HEADER + termCount * TERM_RECORD_BYTES + blockCount * BLOCK_RECORD_BYTES + 4
        require(need == bytes.size) {
            "postings.idx 长度与头部不符: 头部声称 $need 字节，实际 ${bytes.size}"
        }
        verifyTrailingCrc(bytes, "postings.idx")
        var p = HEADER
        val terms = ArrayList<TermRecord>(termCount)
        for (i in 0 until termCount) {
            val t = TermRecord.readFrom(bytes, p)
            require(t.ordinal == i) { "postings.idx term 记录乱序: 第 $i 条 ordinal=${t.ordinal}" }
            terms.add(t); p += TERM_RECORD_BYTES
        }
        val blocks = ArrayList<BlockRecord>(blockCount)
        for (i in 0 until blockCount) {
            blocks.add(BlockRecord.readFrom(bytes, p)); p += BLOCK_RECORD_BYTES
        }
        return TermTable(terms, blocks)
    }

    // ── postings.bin 的块 ───────────────────────────────────────────

    /**
     * 一块的倒排数据。
     *
     * `positions[i]` 为 null 表示该块未记录词位（`hasPositions=false`）。
     * 位置是**段内文档的字段内词位序号**，升序。
     */
    class BlockData(
        val docIds: IntArray,
        val tfs: IntArray,
        val docLengths: IntArray,
        val positions: Array<IntArray?>?,
    ) {
        val size: Int get() = docIds.size
    }

    /**
     * 编码一块。[positions] 与 [docIds] 等长；元素为 null 表示该文档没有词位信息。
     */
    fun encodeBlock(
        docIds: IntArray,
        tfs: IntArray,
        docLengths: IntArray,
        positions: Array<IntArray?>?,
    ): ByteArray {
        require(docIds.size == tfs.size && docIds.size == docLengths.size) { "块内数组长度不一致" }
        val out = ByteArrayOutputStream(16 * docIds.size)
        var prev = 0
        for (i in docIds.indices) {
            // 块内第一个 docId 是绝对值，其余是 delta（delta+varint 是设计文档 §8.6.1 的要求）
            Varint.writeUnsigned(out, if (i == 0) docIds[i] else docIds[i] - prev)
            prev = docIds[i]
            Varint.writeUnsigned(out, tfs[i])
            Varint.writeUnsigned(out, docLengths[i])
            val pos = positions?.get(i)
            if (positions != null) {
                if (pos == null) {
                    Varint.writeUnsigned(out, 0)
                } else {
                    Varint.writeUnsigned(out, pos.size)
                    var pv = 0
                    for (k in pos.indices) {
                        Varint.writeUnsigned(out, if (k == 0) pos[k] else pos[k] - pv)
                        pv = pos[k]
                    }
                }
            }
        }
        return withTrailingCrc(out)
    }

    /** 解码一块。[hasPositions] 必须与写入时一致（它由段元信息决定）。 */
    fun decodeBlock(
        bytes: ByteArray,
        hasPositions: Boolean,
        blockDesc: String,
    ): BlockData {
        if (bytes.size < 5) throw com.cloudea.yangbase.common.CorruptDataException("$blockDesc: 块太小 (${bytes.size} B)")
        verifyTrailingCrc(bytes, blockDesc)
        val body = bytes.size - 4
        val docIds = ArrayList<Int>(64)
        val tfs = ArrayList<Int>(64)
        val dls = ArrayList<Int>(64)
        val poss = if (hasPositions) ArrayList<IntArray?>(64) else null
        var p = 0
        var prev = 0
        while (p < body) {
            val (d, n1) = Varint.readUnsigned(bytes, p); p += n1
            val docId = if (docIds.isEmpty()) d else prev + d
            if (docIds.isNotEmpty() && docId <= prev) {
                throw com.cloudea.yangbase.common.CorruptDataException(
                    "$blockDesc: 块内 docId 未严格递增 (前=$prev, 现=$docId)"
                )
            }
            prev = docId
            val (tf, n2) = Varint.readUnsigned(bytes, p); p += n2
            val (dl, n3) = Varint.readUnsigned(bytes, p); p += n3
            docIds.add(docId); tfs.add(tf); dls.add(dl)
            if (poss != null) {
                val (pc, n4) = Varint.readUnsigned(bytes, p); p += n4
                if (pc == 0) {
                    poss.add(null)
                } else {
                    val arr = IntArray(pc)
                    var pv = 0
                    for (k in 0 until pc) {
                        val (pv2, n5) = Varint.readUnsigned(bytes, p); p += n5
                        arr[k] = if (k == 0) pv2 else pv + pv2
                        pv = arr[k]
                    }
                    poss.add(arr)
                }
            }
        }
        if (p != body) throw com.cloudea.yangbase.common.CorruptDataException(
            "$blockDesc: 块数据在边界外结束 (p=$p, body=$body)"
        )
        return BlockData(docIds.toIntArray(), tfs.toIntArray(), dls.toIntArray(), poss?.toTypedArray())
    }

    // ── docmap ──────────────────────────────────────────────────────

    fun encodeDocmapIdx(offsets: IntArray): ByteArray {
        val out = ByteArrayOutputStream(16 + offsets.size * 4)
        out.write(DOCMAP_IDX_MAGIC)
        BigEndian.writeInt(out, VERSION)
        BigEndian.writeInt(out, offsets.size)
        for (o in offsets) BigEndian.writeInt(out, o)
        return withTrailingCrc(out)
    }

    fun decodeDocmapIdx(bytes: ByteArray): IntArray {
        requireMagic(bytes, DOCMAP_IDX_MAGIC, "docmap.idx")
        val version = BigEndian.readInt(bytes, 4)
        require(version == VERSION) { "docmap.idx 版本不支持: $version" }
        val n = BigEndian.readInt(bytes, 8)
        require(12 + n * 4 + 4 == bytes.size) { "docmap.idx 长度与头部不符 (n=$n, size=${bytes.size})" }
        verifyTrailingCrc(bytes, "docmap.idx")
        val out = IntArray(n)
        var p = 12
        for (i in 0 until n) { out[i] = BigEndian.readInt(bytes, p); p += 4 }
        return out
    }

    fun encodeDocmapRecord(pk: ByteArray): ByteArray {
        val out = ByteArrayOutputStream(pk.size + 5)
        Varint.writeUnsigned(out, pk.size)
        out.write(pk)
        return out.toByteArray()
    }

    fun decodeDocmapRecord(bytes: ByteArray, off: Int, end: Int): ByteArray {
        val (len, n) = Varint.readUnsigned(bytes, off)
        if (off + n + len > end) throw com.cloudea.yangbase.common.CorruptDataException("docmap.bin: 记录越界")
        return bytes.copyOfRange(off + n, off + n + len)
    }

    // ── docts（每文档的 commit_ts 与字段长度）───────────────────────

    /** 每文档的版本信息。字段长度也在这里，合并段时才不必靠 postings 反推（会漏掉无词条的文档）。 */
    class DocVersions(val ts: LongArray, val dls: IntArray) {
        val size: Int get() = ts.size
    }

    fun encodeDocTs(ts: LongArray, dls: IntArray, minTs: Long): ByteArray {
        require(ts.size == dls.size) { "commit_ts 与 dl 数量不一致: ${ts.size} vs ${dls.size}" }
        val out = ByteArrayOutputStream(16 + ts.size * 6)
        out.write(DOCTS_MAGIC)
        BigEndian.writeInt(out, VERSION)
        BigEndian.writeInt(out, ts.size)
        BigEndian.writeLong(out, minTs)
        var prev = minTs
        for (t in ts) {
            RowCodec.writeZigZag(out, t - prev)
            prev = t
        }
        for (d in dls) Varint.writeUnsigned(out, d)
        return withTrailingCrc(out)
    }

    fun decodeDocTs(bytes: ByteArray): DocVersions {
        requireMagic(bytes, DOCTS_MAGIC, "docts.bin")
        val version = BigEndian.readInt(bytes, 4)
        require(version == VERSION) { "docts.bin 版本不支持: $version" }
        val n = BigEndian.readInt(bytes, 8)
        val minTs = BigEndian.readLong(bytes, 12)
        verifyTrailingCrc(bytes, "docts.bin")
        val body = bytes.size - 4
        val ts = LongArray(n)
        var p = 20
        var prev = minTs
        for (i in 0 until n) {
            val (d, sz) = RowCodec.readZigZag(bytes, p)
            p += sz
            ts[i] = prev + d
            prev = ts[i]
        }
        val dls = IntArray(n)
        for (i in 0 until n) {
            val (d, sz) = Varint.readUnsigned(bytes, p)
            p += sz
            dls[i] = d
        }
        if (p != body) throw com.cloudea.yangbase.common.CorruptDataException(
            "docts.bin: 数据在边界外结束 (p=$p, body=$body)"
        )
        return DocVersions(ts, dls)
    }

    // ── 删除集 ──────────────────────────────────────────────────────

    /** 升序 docId 的 delta varint 编码。 */
    fun encodeDocIdSet(docs: IntArray): ByteArray {
        val out = ByteArrayOutputStream(docs.size * 2 + 4)
        var prev = 0
        for ((i, d) in docs.withIndex()) {
            require(i == 0 || d > docs[i - 1]) { "docId 集合必须严格升序: $d" }
            Varint.writeUnsigned(out, if (i == 0) d else d - prev)
            prev = d
        }
        return out.toByteArray()
    }

    fun decodeDocIdSet(bytes: ByteArray): IntArray {
        val out = ArrayList<Int>(bytes.size)
        var p = 0
        var prev = 0
        while (p < bytes.size) {
            val (d, n) = Varint.readUnsigned(bytes, p)
            p += n
            val docId = if (out.isEmpty()) d else prev + d
            prev = docId
            out.add(docId)
        }
        return out.toIntArray()
    }

    // ── 工具 ────────────────────────────────────────────────────────

    private const val HEADER = 16

    private fun withTrailingCrc(out: ByteArrayOutputStream): ByteArray {
        val body = out.toByteArray()
        val full = ByteArray(body.size + 4)
        body.copyInto(full)
        val crc = Checksums.crc32c(body)
        for (k in 0 until 4) {
            full[body.size + k] = ((crc ushr ((3 - k) * 8)) and 0xFF).toByte()
        }
        return full
    }

    private fun requireMagic(bytes: ByteArray, magic: ByteArray, what: String) {
        require(bytes.size >= magic.size + 4) { "$what: 对象太小 (${bytes.size} B)" }
        for (i in magic.indices) {
            require(bytes[i] == magic[i]) { "$what: 魔数不匹配" }
        }
    }

    private fun verifyTrailingCrc(bytes: ByteArray, what: String) {
        if (bytes.size < 4) throw com.cloudea.yangbase.common.CorruptDataException("$what: 缺少校验和")
        val expect = BigEndian.readInt(bytes, bytes.size - 4)
        val actual = Checksums.crc32c(bytes, 0, bytes.size - 4)
        if (expect != actual) throw com.cloudea.yangbase.common.CorruptDataException(
            "$what: 校验和不匹配 (期望=${"%08x".format(expect)} 实际=${"%08x".format(actual)})"
        )
    }
}
