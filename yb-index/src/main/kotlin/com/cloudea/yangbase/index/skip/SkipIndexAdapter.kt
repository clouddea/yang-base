package com.cloudea.yangbase.index.skip

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.IndexDef
import com.cloudea.yangbase.common.IndexId
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.storage.BlockCache
import com.cloudea.yangbase.storage.BlockSkipper
import com.cloudea.yangbase.storage.ColumnRange
import com.cloudea.yangbase.storage.SstBlockObserver
import com.cloudea.yangbase.storage.SstHandle

/**
 * 把跳数索引接到存储层的两个 SPI 上（设计文档 §8.5）。
 *
 * 存储层在 `yb-storage` 里定义了 [SstBlockObserver]（写半边）和
 * [BlockSkipper]（读半边），这里给出它们的实现。依赖方向因此保持单向
 * （`yb-index → yb-storage`），而跳数索引又能落在**真正的读写路径**上。
 *
 * ## 这一点为什么重要
 *
 * 在接线之前，跳数索引构件是齐的、单元测试是绿的，但它**从来没有被写过、
 * 也从来没有被用过**——`SkipIndexBuilder` 没人调用，`BlockPruner` 没人问。
 * 那种状态比没有实现更糟：它有测试、有文档、看起来是完成的，
 * 却对系统行为零影响。
 */
class SkipIndexBlockObserver(
    private val store: ObjectStore,
    private val cache: BlockCache,
    private val def: SkipIndexDef,
    private val schema: TableSchema,
) : SstBlockObserver {

    private val builder = SkipIndexBuilder(def, schema)
    private var started = false

    override fun beginBlock(blockIdx: Int) {
        builder.beginBlock(blockIdx)
        started = true
    }

    override fun observe(row: List<Datum>) = builder.observe(row)

    override fun endBlock() = builder.endBlock()

    override suspend fun persist(sstFileId: String, level: Int): Int {
        if (!started) return 0
        SkipIndexStore(store, cache).write(def, builder.build(sstFileId))
        return 1
    }

    override val isEmpty: Boolean get() = !started
}

/**
 * 用跳数索引做块级剪枝。
 *
 * **委托给 [BlockPruner] 而不是自己重写判定**：那里已经处理好了
 * 「`values == null` 表示收集不完整，必须放弃剪枝」「`And` 任一子句可剪则整块可剪、
 * `Or` 要全部可剪」这些容易搞反的语义。重写一遍只会得到第二份会漂移的实现。
 */
class SkipIndexBlockSkipper(
    private val store: ObjectStore,
    private val cache: BlockCache,
    private val indexes: List<IndexDef>,
    private val schema: TableSchema,
    /** 本次扫描可下推的列约束。为空时一次剪枝都不会发生。 */
    ranges: List<ColumnRange>,
) : BlockSkipper {

    private val predicates: List<SkipPredicate> = ranges.mapNotNull { toPredicate(it) }

    /** sstFileId → (列下标, 该文件的 `.skp`)。首次用到时加载，每文件至多 1 次 GET。 */
    private val loaded = HashMap<String, List<Pair<SkipIndexDef, SkipIndexFile?>>>()
    private val indexStore = SkipIndexStore(store, cache)

    override suspend fun mightMatchBlock(handle: SstHandle, blockIdx: Int): Boolean {
        if (predicates.isEmpty()) return true

        // ① 先用 `.idx` 里已有的块级列统计判一遍——**0 次 GET、0 字节传输**。
        //    它覆盖 min/max 与值计数，能挡掉大部分块。`.skp` 只是补充。
        val noSkp = BlockPruner(handle, schema, skpColumns = null, skp = null)
        if (predicates.any { noSkp.canSkipBlock(blockIdx, it) }) return false

        // ② `.idx` 判不了时才去问 `.skp`（set / bloom / ngram 才有的信息）
        val files = loaded.getOrPut(handle.fileId.value) {
            indexes.map { def ->
                val sid = SkipIndexDef.of(IndexId(def.id), def, schema)
                sid to runCatching { indexStore.load(sid, handle.fileId.value) }.getOrNull()
            }
        }
        for ((sid, file) in files) {
            if (file == null) continue                       // 没有 → 问不出结论 → 不剪
            val cols = sid.columns
            if (cols.none { c -> predicates.any { it.columns.contains(c) } }) continue
            val pruner = BlockPruner(handle, schema, skpColumns = cols, skp = file)
            if (predicates.any { pruner.canSkipBlock(blockIdx, it) }) return false
        }
        return true
    }

    /**
     * 把存储层的区间表示翻成剪枝谓词。
     *
     * 等值走 [SkipPredicate.Equals]（能用上 set / bloom / 值计数），
     * 其余走 [SkipPredicate.Range]（只能用 min/max）。
     * **两者都只允许放宽**：谓词比真实约束更宽只会少剪几个块，更窄就是漏行。
     */
    private fun toPredicate(r: ColumnRange): SkipPredicate? {
        r.likePattern?.let { return SkipPredicate.Like(r.column, it) }
        val eq = r.equality
        if (eq != null && eq !is Datum.Null) return SkipPredicate.Equals(r.column, eq)
        val lo = r.low?.takeIf { it !is Datum.Null }?.let { Bound(it, r.lowInclusive) }
        val hi = r.high?.takeIf { it !is Datum.Null }?.let { Bound(it, r.highInclusive) }
        if (lo == null && hi == null) return null
        return SkipPredicate.Range(r.column, lo, hi)
    }

    companion object {
        /** 表上有哪些跳数索引。 */
        fun skipIndexesOf(table: TableSchema): List<IndexDef> =
            // 只看 ACTIVE：BUILDING 的索引统计不完整，用它剪枝会**剪掉有数据的块**
            table.activeIndexes.filter { it.kind.isSkipIndex }
    }
}
