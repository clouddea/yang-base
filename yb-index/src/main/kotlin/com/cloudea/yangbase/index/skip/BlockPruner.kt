package com.cloudea.yangbase.index.skip

import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.OrderPreservingCodec
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.storage.SstHandle

/** 一次剪枝的统计。**这是 §12.5 里 `yb_objects_skipped_by_skip_index` 的数据源。** */
data class PruneStats(
    val totalBlocks: Int,
    val prunedByKey: Int,
    val prunedByTs: Int,
    val prunedByPredicate: Int,
    val needed: Int,
) {
    val pruned: Int get() = totalBlocks - needed

    override fun toString(): String =
        "块总数=$totalBlocks 剪掉: 键范围=$prunedByKey 版本=$prunedByTs 谓词=$prunedByPredicate " +
            "→ 需读=$needed（省 ${if (totalBlocks == 0) 0 else pruned * 100 / totalBlocks}%）"
}

/** 剪枝结果。 */
class PruneResult(val neededBlocks: IntArray, val stats: PruneStats)

/**
 * 数据块级剪枝（设计文档 §8.5）。
 *
 * **全部判定都在内存里完成，0 次对象存储请求**：
 *  - `min` / `max` / `nullCount` 来自 SST 的 `.idx`（查询时本就常驻内存）；
 *  - `set` / `bloom` / `ngram` 来自可选的 `.skp`（每个文件至多 1 次 GET，之后常驻）。
 *
 * 三条剪枝依据，按代价从低到高：
 *  1. **键范围**：块的键区间与 `[keyFrom, keyTo]` 不相交 → 跳过（块首键有序，可提前终止）；
 *  2. **版本**：`block.minTs > readTs` ⇒ 整块都在快照之后 ⇒ 无可见行 → 跳过；
 *  3. **谓词**：合取中任一子谓词「一定不含」→ 跳过。
 *
 * ⚠️ 判定方向永远保守：只有能证明「一定不含」才返回可跳过。返回 false（可能含）
 * 而实际不含，只是多读一个块；反过来就是静默漏行。
 */
class BlockPruner(
    private val handle: SstHandle,
    private val schema: TableSchema,
    /** `.skp` 覆盖的列（schema 下标）。为 null 表示不使用 `.skp`。 */
    private val skpColumns: List<Int>? = null,
    private val skp: SkipIndexFile? = null,
) {
    private val idx = handle.index
    private val entryOf: IntArray? = skp?.blockToEntryMap()

    /** 谓词的 schema 列下标 → `.skp` 内的列下标。 */
    private fun skpColumnIndex(column: Int): Int = skpColumns?.indexOf(column) ?: -1

    // ── 主入口 ──────────────────────────────────────────────────────

    fun prune(
        predicates: List<SkipPredicate>,
        readTs: ReadTs,
        keyFrom: ByteArray? = null,
        keyTo: ByteArray? = null,
    ): PruneResult {
        val blocks = idx.blocks
        val out = ArrayList<Int>(blocks.size)
        var byKey = 0
        var byTs = 0
        var byPred = 0

        for (b in blocks.indices) {
            val blk = blocks[b]
            // ① 键范围：本块的首键已越过上界 ⇒ 后续所有块也越过（块首键升序）
            if (keyTo != null && ByteArrayComparator.compare(blk.firstKey, keyTo) > 0) {
                byKey += blocks.size - b
                break
            }
            // 下界：用**下一块的首键**作为本块键的上界（块索引里没有 lastKey，
            // 这是等价且不需要额外元数据的做法）
            if (keyFrom != null && b + 1 < blocks.size &&
                ByteArrayComparator.compare(blocks[b + 1].firstKey, keyFrom) <= 0
            ) {
                byKey++; continue
            }
            // ② 版本：整块都新于快照 ⇒ 无可见行
            if (blk.minTs > readTs.value) { byTs++; continue }
            // ③ 谓词
            if (predicates.any { canSkipBlock(b, it) }) { byPred++; continue }
            out.add(b)
        }
        val needed = out.toIntArray()
        return PruneResult(
            needed,
            PruneStats(blocks.size, byKey, byTs, byPred, needed.size),
        )
    }

    // ── 谓词判定 ────────────────────────────────────────────────────

    /**
     * 该块是否**一定不含**满足 `p` 的行。
     *
     * `And`：任一子句可证明不含 ⇒ 整块不含。`Or`：所有子句都可证明不含 ⇒ 整块不含。
     * 两个方向都不能反——反了就是漏行。
     */
    fun canSkipBlock(blockIdx: Int, p: SkipPredicate): Boolean = when (p) {
        is SkipPredicate.And -> p.children.any { canSkipBlock(blockIdx, it) }
        is SkipPredicate.Or -> p.children.all { canSkipBlock(blockIdx, it) }
        else -> canSkipLeaf(blockIdx, p)
    }

    private fun canSkipLeaf(blockIdx: Int, p: SkipPredicate): Boolean = when (p) {
        is SkipPredicate.Equals -> canSkipEquals(blockIdx, p.column, encode(p.column, p.value))
        is SkipPredicate.InList ->
            p.values.all { canSkipEquals(blockIdx, p.column, encode(p.column, it)) }
        is SkipPredicate.NotEquals -> canSkipNotEquals(blockIdx, p.column, encode(p.column, p.value))
        is SkipPredicate.Range -> canSkipRange(blockIdx, p)
        is SkipPredicate.IsNull -> canSkipIsNull(blockIdx, p)
        is SkipPredicate.Like -> canSkipLike(blockIdx, p)
        is SkipPredicate.And, is SkipPredicate.Or -> canSkipBlock(blockIdx, p)
    }

    private fun encode(column: Int, v: Datum): ByteArray =
        KeyEncoder.encodeDatum(v, schema.columns[column].type)

    /** `.idx` 的块级列统计（可能缺失：老文件或列越界）。 */
    private fun blockStat(blockIdx: Int, column: Int) =
        idx.blocks[blockIdx].colStats.getOrNull(column)

    /** `.skp` 覆盖该块的那一项里该列的摘要。 */
    private fun summary(blockIdx: Int, column: Int): ColumnSummary? {
        val file = skp ?: return null
        val map = entryOf ?: return null
        if (blockIdx >= map.size) return null
        val ei = map[blockIdx]
        if (ei < 0) return null
        val ci = skpColumnIndex(column)
        if (ci < 0) return null
        return file.entries[ei].columns.getOrNull(ci)
    }

    private fun canSkipEquals(blockIdx: Int, column: Int, enc: ByteArray): Boolean {
        // ① .idx 的精确值计数（低基数列）：直接查值→计数
        blockStat(blockIdx, column)?.let { st ->
            if (st.valueCounts != null && st.countOf(enc) == 0L) return true
            if (st.min != null && st.max != null) {
                if (ByteArrayComparator.compare(enc, st.min!!) < 0) return true
                if (ByteArrayComparator.compare(enc, st.max!!) > 0) return true
            }
            // min/max 都为空 ⇒ 该块内没有非 NULL 值 ⇒ 等值不可能成立
            // （注意：这与「列统计未收集」不同——colStats 一定为每列都产出条目）
            if (st.min == null && st.max == null) return true
        }
        // ② .skp 的 set / bloom
        summary(blockIdx, column)?.let { s ->
            if (s.allNull) return true
            if (!s.mightContainValue(enc)) return true
            s.bloom?.let { if (!it.mightContain(enc)) return true }
        }
        return false
    }

    private fun canSkipNotEquals(blockIdx: Int, column: Int, enc: ByteArray): Boolean {
        blockStat(blockIdx, column)?.let { st ->
            val mn = st.min ?: return@let
            val mx = st.max ?: return@let
            if (st.valueCounts != null) {
                // 值计数完整时更精确：块内只有一个不同的非 NULL 值且等于 enc
                if (st.valueCounts!!.size == 1 && st.countOf(enc) > 0) return true
            } else if (ByteArrayComparator.compare(mn, mx) == 0 &&
                ByteArrayComparator.compare(mn, enc) == 0
            ) return true
        }
        summary(blockIdx, column)?.let { s ->
            if (s.allNonNullEqual(enc)) return true
        }
        return false
    }

    private fun canSkipRange(blockIdx: Int, p: SkipPredicate.Range): Boolean {
        val st = blockStat(blockIdx, p.column)
        if (st != null) {
            // 全 NULL 的块：任何比较都不成立
            if (st.min == null && st.max == null) return true
            val mn = st.min
            val mx = st.max
            if (mn != null && mx != null) {
                val lo = p.lower
                if (lo != null) {
                    val c = ByteArrayComparator.compare(mx, encode(p.column, lo.value))
                    if (if (lo.inclusive) c < 0 else c <= 0) return true
                }
                val hi = p.upper
                if (hi != null) {
                    val c = ByteArrayComparator.compare(mn, encode(p.column, hi.value))
                    if (if (hi.inclusive) c > 0 else c >= 0) return true
                }
            }
        }
        // 退化路径：只有 .skp 的 minmax（例如 SST 的 .idx 未收集列统计）
        summary(blockIdx, p.column)?.let { s ->
            if (!s.minmaxCollected) return@let
            if (s.allNull) return true
            val mn = s.min ?: return@let
            val mx = s.max ?: return@let
            val lo = p.lower
            if (lo != null) {
                val c = ByteArrayComparator.compare(mx, encode(p.column, lo.value))
                if (if (lo.inclusive) c < 0 else c <= 0) return true
            }
            val hi = p.upper
            if (hi != null) {
                val c = ByteArrayComparator.compare(mn, encode(p.column, hi.value))
                if (if (hi.inclusive) c > 0 else c >= 0) return true
            }
        }
        return false
    }

    private fun canSkipIsNull(blockIdx: Int, p: SkipPredicate.IsNull): Boolean {
        val st = blockStat(blockIdx, p.column)
        if (st != null) {
            if (!p.negated && st.nullCount == 0L) return true
            if (p.negated && st.min == null && st.max == null) return true
        }
        summary(blockIdx, p.column)?.let { s ->
            if (!p.negated && s.nulls == 0L && s.minmaxCollected) return true
            if (p.negated && s.allNull) return true
        }
        return false
    }

    /**
     * `LIKE` 的剪枝。两条路：
     *  1. **字面量前缀** `L%`：`L` 的所有取值都落在 `[F, succ(F))`（F = L 的保序编码去掉终结符），
     *     与块的 `[min, max]` 无交集即可跳过。
     *  2. **子串** `%x%`：min/max **无法**剪枝（子串关系不蕴含任何序关系：
     *     `"aab" ⊆ "zzz"` 但 `"aab" < "zzz"`；`"zzz" ⊆ "aazzz"` 而 `"zzz" > "aazzz"`）。
     *     只能靠 ngram 布隆与完整集合。
     */
    private fun canSkipLike(blockIdx: Int, p: SkipPredicate.Like): Boolean {
        val st = blockStat(blockIdx, p.column)
        val s = summary(blockIdx, p.column)
        val type = schema.columns[p.column].type

        val leading = p.leadingLiteral
        if (leading != null && (st != null || s != null)) {
            val enc = KeyEncoder.encodeDatum(Datum.Str(leading), type)
            // 去掉保序编码的 00 00 终结符，得到「L 的所有取值都以 F 为前缀」
            val f = if (enc.size >= 2) enc.copyOf(enc.size - 2) else ByteArray(0)
            if (f.isNotEmpty()) {
                val succ = prefixSuccessor(f)
                st?.let { if (it.min != null && it.max != null) {
                    if (ByteArrayComparator.compare(it.max!!, f) < 0) return true
                    if (succ != null && ByteArrayComparator.compare(it.min!!, succ) >= 0) return true
                } }
                s?.let { c ->
                    if (c.minmaxCollected) {
                        if (c.min != null && c.max != null) {
                            if (ByteArrayComparator.compare(c.max!!, f) < 0) return true
                            if (succ != null && ByteArrayComparator.compare(c.min!!, succ) >= 0) return true
                        } else if (c.allNull) return true
                    }
                }
                // 完整集合：块内没有任何值以 L 开头
                s?.values?.let { vs ->
                    if (vs.none { startsWithEncoded(it, f) }) return true
                }
            }
        }

        // 子串：每个字面量片段的所有 n-gram 都必须在 ngram 布隆里
        s?.ngramBloom?.let { bloom ->
            for (seg in p.literals) {
                for (g in grams(seg)) {
                    if (!bloom.mightContain(g.toByteArray(Charsets.ISO_8859_1))) return true
                }
            }
        }
        // 完整集合：没有任何值包含任一字面量片段 ⇒ 一定不含
        s?.values?.let { vs ->
            if (p.literals.isNotEmpty()) {
                val decoded = vs.map { decodeStringOrNull(it) ?: "" }
                if (p.literals.none { lit -> decoded.any { it.contains(lit) } }) return true
            }
        }
        return false
    }

    private fun startsWithEncoded(encodedValue: ByteArray, prefix: ByteArray): Boolean {
        if (encodedValue.size < prefix.size) return false
        return ByteArrayComparator.compare(encodedValue, 0, prefix.size, prefix, 0, prefix.size) == 0
    }

    private fun decodeStringOrNull(encoded: ByteArray): String? = try {
        OrderPreservingCodec.decodeString(encoded).first
    } catch (_: Exception) {
        null
    }

    private fun grams(text: String): List<String> {
        if (text.length < NGRAM_N) return emptyList()
        val out = ArrayList<String>(text.length)
        for (i in 0..text.length - NGRAM_N) out.add(text.substring(i, i + NGRAM_N))
        return out
    }

    companion object {
        /** 与 [SkipIndexBuilder] 的默认 ngramN 一致。 */
        const val NGRAM_N = 3

        /**
         * 字节串前缀区间的**最小上界**：所有以 [prefix] 为前缀的字节串都 `< succ`。
         *
         * 做法是把最后一个非 `0xFF` 字节加一、砍掉其后的部分。
         * 全 `0xFF` 时不存在有限上界，返回 null（调用方只用下界判定）。
         */
        fun prefixSuccessor(prefix: ByteArray): ByteArray? {
            var i = prefix.size - 1
            while (i >= 0 && prefix[i] == 0xFF.toByte()) i--
            if (i < 0) return null
            val out = prefix.copyOf(i + 1)
            out[i] = ((out[i].toInt() and 0xFF) + 1).toByte()
            return out
        }

    }
}
