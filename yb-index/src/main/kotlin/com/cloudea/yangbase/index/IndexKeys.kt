package com.cloudea.yangbase.index

import com.cloudea.yangbase.common.IndexId
import com.cloudea.yangbase.objectstore.ObjectKeys

/**
 * 索引对象的键。
 *
 * **拼接规则（这是本文件存在的唯一理由）**：
 *  - 所有前缀都来自 [ObjectKeys]（`indexRoot` / `indexSegment` / `indexManifest*`），
 *    绝不重新拼 `index/{id}/` 这类结构；
 *  - 所有**序号**分量都必须过 [ObjectKeys.pad]（零填充是正确性要求，不是风格问题——
 *    LIST 按字节字典序，`10` 会排在 `9` 前）；
 *  - 本文件只做两件事：把常量文件名接到 [ObjectKeys] 给出的目录前缀上，
 *    以及对序号调用 `ObjectKeys.pad`。
 *
 * 段内文件名是**常量**（不带序号），所以不需要零填充；删除位图与 manifest 带序号，
 * 走 `ObjectKeys.pad`。
 */
object IndexKeys {

    // ── 倒排段内的文件（§8.6.1）──────────────────────────────────────
    //
    // 对 §8.6.1 布局的**三处**偏离，都在此显式记录（偏离的理由都是「请求数」，
    // 见 P4：分离文件会把 1 次 GET 变成 2 次，除非两者的读取粒度不同）：
    //  · `norms.bin` 未单独成文件 —— 字段长度 dl 内联在 postings 块里（每文档 1 个 varint）。
    //    norms 的读取粒度与 postings 完全一致（都是「命中一个文档就要读」），分离只会多一次 GET。
    //  · `positions.bin` / `positions.idx` 未实现 —— 词位同样内联在 postings 块的尾部。
    //    理由相同：短语校验只在「该词条已命中该文档」时发生，此时块已经读进来了。
    //    代价是**不用词位的查询也会把这些字节读进来**；缓解手段是建段时
    //    `hasPositions=false`（段元信息里显式记录，检索期据此解释块布局）。
    //  · `terms.blk` 未实现 —— 段被限制在 8–64 MiB，FST 本体（< 10 MB）整体常驻内存。

    const val SEG_META = "meta.cbor"
    const val TERMS_FST = "terms.fst"
    const val POSTINGS_BIN = "postings.bin"
    const val POSTINGS_IDX = "postings.idx"
    const val DOCMAP_BIN = "docmap.bin"
    const val DOCMAP_IDX = "docmap.idx"
    /** 每文档的 commit_ts 与字段长度（密集、变长）。只有跨越 read_ts 的段才需要读它。 */
    const val DOCTS_BIN = "docts.bin"

    /** 段内一个文件的完整键。 */
    fun segmentFile(index: IndexId, segId: String, fileName: String): String =
        ObjectKeys.indexSegment(index, segId) + fileName

    /** 段内所有文件的键（含本实现不写出的可选文件），供 GC 的可达性分析与测试使用。 */
    fun segmentFiles(index: IndexId, segId: String): List<String> = listOf(
        SEG_META, TERMS_FST, POSTINGS_BIN, POSTINGS_IDX, DOCMAP_BIN, DOCMAP_IDX, DOCTS_BIN,
    ).map { segmentFile(index, segId, it) }

    // ── 跳数索引（§8.5）─────────────────────────────────────────────
    //
    // 一个 SST 文件对应一个不可变的 `.skp` 对象。**刻意不引入 manifest 指针**：
    // 它是「单个 SST 文件的确定性投影」，键由 (index_id, sst_file_id) 唯一确定，
    // 与 `.idx` 同构。多一层指针只会给查询热路径加一次 GET。

    fun skipFile(index: IndexId, sstFileId: String): String =
        ObjectKeys.indexRoot(index) + "skip/" + sstFileId + ".skp"

    fun skipPrefix(index: IndexId): String = ObjectKeys.indexRoot(index) + "skip/"

    // ── 倒排索引的删除位图（§8.6.5）─────────────────────────────────

    fun deletes(index: IndexId, segId: String, seq: Long): String =
        ObjectKeys.indexRoot(index) + "deletes/" + segId + "/" + ObjectKeys.pad(seq) + ".cbor"

    fun deletesPrefix(index: IndexId, segId: String): String =
        ObjectKeys.indexRoot(index) + "deletes/" + segId + "/"
}
