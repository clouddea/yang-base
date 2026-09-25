package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.IndexDef
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.stats.ColumnStats
import com.cloudea.yangbase.common.stats.TableStats

/**
 * 访问路径的类型。`explainName` 就是 `EXPLAIN` 结果里 `type` 列的取值。
 *
 * ⭐ `METADATA_COUNT` 必须能出现在这里（设计文档 §8.9.6）：它是用户**唯一**
 * 能验证「`COUNT(*)` 走了 0 次对象存储请求的那条路」的手段。让计数路径在
 * EXPLAIN 里显示成一个普通扫描，等于把这个优化藏起来——它错了也没人会发现。
 */
enum class AccessType(val explainName: String) {
    /** 主键点查（全部主键列等值）。 */
    CONST("const"),

    /** 二级索引（等值前缀）。索引项里存的是整行，因此不需要回表。 */
    REF("ref"),

    /** 主键上的范围/前缀扫描。 */
    RANGE("range"),

    /** 全表扫描（并行预取）。 */
    ALL("ALL"),

    /** 元数据计数：0 次对象存储请求（§8.9）。 */
    METADATA_COUNT("METADATA_COUNT"),

    /** 倒排索引。 */
    FULLTEXT("FULLTEXT"),

    /** 无 FROM 的常量查询。 */
    NO_TABLE("NULL"),
    // ⚠️ 声明顺序即「打平容差内越具体越优先」的顺序（见 choose）。
}

/**
 * 一条访问路径的估算结果。
 *
 * `gets` 是**预计对象存储请求数**——这是本项目代价模型的主成本项（§10.2 R1）：
 * 延迟主要由「要发几次请求、串行几轮」决定，而不是 CPU。字节数只作为次要指标。
 */
data class PathPlan(
    val type: AccessType,
    /** 用到的索引名（`PRIMARY` 表示主键）。 */
    val key: String? = null,
    val keyColumns: List<String> = emptyList(),
    /** EXPLAIN 的 `rows`：预计**取出/检查**的行数。 */
    val rows: Long,
    /** EXPLAIN 的 `filtered`：这些行里能通过完整 WHERE 的比例（0..1）。 */
    val filtered: Double,
    /** 预计对象存储请求数（已按 R4 的并行度折算）。 */
    val gets: Double,
    val possibleKeys: List<String> = emptyList(),
    val extra: List<String> = emptyList(),
) {
    /** EXPLAIN 的 `filtered` 列（百分比形式，MySQL 的形状）。 */
    val filteredPercent: Double get() = (filtered * 100.0).coerceIn(0.0, 100.0)
    /** 估算的最终输出行数。 */
    val outputRows: Long get() = Math.round(rows * filtered)
}

/**
 * 一个可用于等值查找的二级索引候选。
 *
 * [boundColumns] 是索引列前缀上被等值绑定的列数：`INDEX (a, b)` 上
 * `WHERE a = 1` 是 1，`WHERE a = 1 AND b = 2` 是 2。绑定得越多越有选择性——
 * 这是「列数少优先」之外真正决定代价的量。
 */
data class IndexCandidate(
    val index: IndexDef,
    val value: Datum,
    val boundColumns: Int,
    val selectivity: Double,
)

/**
 * 规划器的全部输入事实。
 *
 * 这些事实由执行器在**零 IO**（统计来自内存缓存、主键约束来自纯语法分析）的
 * 前提下算好，因此规划本身不产生任何对象存储请求（原则 P4）。
 * `metadataCountRows` 非 null 表示元数据计数**确实可用**并且已经给出精确行数
 * ——它由执行器真正调用一次元数据路径得到（0 次 GET），因此 EXPLAIN 里的
 * `rows` 与执行时的结果不可能不一致。
 */
class PlannerInput(
    val table: TableSchema,
    val stats: TableStats?,
    /** 行数：有统计时取统计值，否则取兜底值。 */
    val rowCount: Long,
    /** 扫描路径会碰到的分片数（已考虑分区裁剪）。 */
    val shards: Int,
    /**
     * 表的全部分片数。
     *
     * 主键范围扫描目前**不做分区裁剪**（`scanKeyRange` 只看键区间），
     * 所以它的代价必须按全部分片算——否则优化器会高估这条路的收益，
     * 在一个 100 分区的表上选出一条要读 100 个分片的路径。
     */
    val totalShards: Int = shards,
    val where: Expr?,
    /** 主键全部列等值。 */
    val constPk: Boolean,
    /** 主键范围的选择率；null 表示没有可用的键区间。 */
    val rangeSelectivity: Double?,
    val rangeColumns: List<String> = emptyList(),
    val indexCandidates: List<IndexCandidate> = emptyList(),
    /** 元数据计数可用时的**精确**行数（§8.9.6 的 `rows`）。 */
    val metadataCountRows: Long? = null,
    /** 元数据计数不可用的原因——EXPLAIN 要如实写出来，而不是假装它能用。 */
    val metadataCountBlocked: String? = null,
    val fullText: Boolean = false,
    val fullTextSelectivity: Double = ColumnStats.DEFAULT_EQ_SELECTIVITY,
    val orderBy: Boolean = false,
    /** 扫描路径上是否下推了块级剪枝（跳数索引）。 */
    val blockPruning: Boolean = false,
    /**
     * 这条查询做了**分组**（`GROUP BY` / `HAVING` / 聚合）。
     *
     * 执行器为此在内存里建了一张分组表，因此 `EXPLAIN` 如实写 `Using temporary`
     * ——与 MySQL 对这个词的含义一致。不写的话，使用者会以为计划里没有额外的
     * 内存开销与一趟完整扫描。
     */
    val grouped: Boolean = false,
    /**
     * 连接查询的各来源（表名或别名），按书写顺序。
     *
     * 非空表示这是一条连接查询：`EXPLAIN` 要**每个来源输出一行**（MySQL 的形状），
     * 而不是挤在一行里——挤在一行会让使用者以为只读了一张表。
     * 同时连接查询不走索引选择与分区裁剪（见 [Executor.selectJoined]），
     * 所以这些行的 `type` 一律是 `ALL`，必须如实反映。
     */
    val joinSources: List<String> = emptyList(),
) {
    val rowsPerBlock: Double
        get() = stats?.rowsPerBlock?.takeIf { it > 0.0 } ?: TableStats.FALLBACK_ROWS_PER_BLOCK

    /** 完整 WHERE 的保留比例。 */
    val whereSelectivity: Double get() = CostModel.selectivity(where, table, stats)
}

/**
 * 基于代价的访问路径选择（设计文档 §10.2 的 R1–R6）。
 *
 * ## 为什么主成本项是「请求数」而不是 CPU（R1）
 *
 * 对象存储每次请求有几十毫秒的固定成本且高度可并行，而本地过滤一行的成本
 * 是纳秒级。在 1 万倍以上的量级差面前，任何把 CPU 放进代价函数的做法都会
 * 得出「全表扫描比点查便宜」这种荒谬结论。因此代价函数只有一项：
 * **预计要发出多少次对象存储请求**（字节数只在同请求数时作为次级判据）。
 *
 * ## 规则到实现的对应
 *
 * | 规则 | 内容 | 在这里怎么体现 |
 * |---|---|---|
 * | R1 | 少请求 ≫ 少字节 | 所有代价都用 `gets` 表示 |
 * | R2 | 优先覆盖索引 | 索引项存整行 ⇒ [AccessType.REF] 不需要回表，Extra 写 `Using index` |
 * | R3 | 谓词顺序按跳数索引友好 | 块级剪枝下推（[PlannerInput.blockPruning]），Extra 如实标注 |
 * | R4 | 并行便宜 | 块数除以 [PARALLELISM]（每个分片的第一块算满） |
 * | R5 | 流式 | 扫描不物化中间结果；LIMIT 暂未下推（见报告） |
 * | R6 | 计数走元数据（**硬规则**，不比代价） | [AccessType.METADATA_COUNT] 一旦可用就直接胜出 |
 */
object CostModel {

    /**
     * R4 的并行度：块级预取的并发数。
     *
     * 每个分片的**第一块**按 1 次串行请求计（它决定了这次扫描的第一个字节何时
     * 到达），其余块按并行折算。这样 `1 行 → 1 次请求`、`8 块 → 1.875 次`，
     * 数量级与真实的行为一致，也不会出现「大表扫描比点查便宜」这种失真。
     */
    const val PARALLELISM = 8.0

    /** 估算的打平容差：两条路径相差不到半次请求时，认为它们一样便宜。 */
    const val TIE_EPSILON = 0.5

    /** 完全没有信息时的兜底选择率（与 MySQL 的经验默认值接近）。 */
    const val DEFAULT_OTHER_SELECTIVITY = 0.5
    const val DEFAULT_LIKE_SELECTIVITY = 0.1

    // ── 代价函数 ──────────────────────────────────────────────────

    /** `rows` 行要读多少个数据块。 */
    fun blocks(rows: Long, rowsPerBlock: Double): Double =
        if (rows <= 0L) 0.0 else Math.ceil(rows / rowsPerBlock.coerceAtLeast(1.0))

    /**
     * 扫描 `rows` 行、跨 [shards] 个分片的请求数（R1 + R4）。
     * 每个分片的第一块算满（串行），其余按 [PARALLELISM] 折算。
     */
    fun scanGets(rows: Long, rowsPerBlock: Double, shards: Int): Double {
        if (rows <= 0L) return 0.0
        val s = shards.coerceAtLeast(1)
        val total = blocks(rows, rowsPerBlock)
        val serial = minOf(total, s.toDouble())
        return serial + (total - serial) / PARALLELISM
    }

    /** 点查：稳态 1 次 GET（设计文档 §14.2 的验收标准）。 */
    const val POINT_GETS = 1.0

    /** 元数据计数：0 次 GET（§8.9 的全部意义所在）。 */
    const val METADATA_COUNT_GETS = 0.0

    /** 二级索引查找：索引分片自身的入口 + 命中项所在块的读取。 */
    fun indexGets(matches: Long, rowsPerBlock: Double): Double =
        1.0 + scanGets(matches, rowsPerBlock, 1)

    /** 全文检索：段读取 + 命中块的读取。 */
    fun fullTextGets(hits: Long, rowsPerBlock: Double, shards: Int): Double =
        1.0 + scanGets(hits, rowsPerBlock, shards)

    // ── 选择率估算 ────────────────────────────────────────────────

    /**
     * 谓词的保留比例。`AND` 按独立假设相乘、`OR` 取并集。
     *
     * 没有统计时的默认值是刻意保守的（等值 0.1、范围 0.333）：估得**偏高**会
     * 让优化器倾向于扫描（安全但慢），估得偏低会让它错误地选择索引（快但可能
     * 慢在实际读了太多块）。宁可偏向扫描。
     */
    fun selectivity(where: Expr?, table: TableSchema, stats: TableStats?): Double {
        if (where == null) return 1.0
        return when (where) {
            is Literal -> if (Eval.truthy(where.value)) 1.0 else 0.0
            is BinaryOp -> when (where.op.uppercase()) {
                "AND" -> (selectivity(where.left, table, stats) * selectivity(where.right, table, stats))
                    .coerceIn(0.0, 1.0)
                "OR" -> {
                    val a = selectivity(where.left, table, stats)
                    val b = selectivity(where.right, table, stats)
                    (1.0 - (1.0 - a) * (1.0 - b)).coerceIn(0.0, 1.0)
                }
                "=", "<=>" -> equalitySelectivity(where, table, stats)
                "<>", "!=" -> (1.0 - equalitySelectivity(where, table, stats)).coerceIn(0.0, 1.0)
                "<", "<=", ">", ">=" -> rangeSelectivity(where, table, stats)
                else -> DEFAULT_OTHER_SELECTIVITY
            }
            is UnaryOp -> if (where.op.equals("NOT", true)) {
                (1.0 - selectivity(where.expr, table, stats)).coerceIn(0.0, 1.0)
            } else DEFAULT_OTHER_SELECTIVITY
            is IsNull -> {
                val col = columnStatsOf(where.expr, table, stats)
                val f = col?.nullFraction ?: 0.0
                if (where.negated) 1.0 - f else f
            }
            is Between -> {
                val col = columnStatsOf(where.expr, table, stats)
                val lo = constOf(where.low, table, emptyMap())?.let { numericOf(it) }
                val hi = constOf(where.high, table, emptyMap())?.let { numericOf(it) }
                if (col == null || lo == null || hi == null) DEFAULT_RANGE
                else col.rangeSelectivity(lo, hi).let { if (where.negated) 1.0 - it else it }
            }
            is InList -> {
                val col = columnStatsOf(where.expr, table, stats)
                val k = where.values.size
                val each = col?.equalitySelectivity() ?: ColumnStats.DEFAULT_EQ_SELECTIVITY
                val s = minOf(1.0, k * each)
                if (where.negated) 1.0 - s else s
            }
            is Like -> if (where.negated) 1.0 - DEFAULT_LIKE_SELECTIVITY else DEFAULT_LIKE_SELECTIVITY
            is MatchAgainst -> ColumnStats.DEFAULT_EQ_SELECTIVITY
            else -> DEFAULT_OTHER_SELECTIVITY
        }
    }

    private val DEFAULT_RANGE = ColumnStats.DEFAULT_RANGE_SELECTIVITY

    private fun equalitySelectivity(e: BinaryOp, table: TableSchema, stats: TableStats?): Double {
        val col = columnStatsOf(e.left, table, stats) ?: columnStatsOf(e.right, table, stats)
            ?: return ColumnStats.DEFAULT_EQ_SELECTIVITY
        // 常量侧必须是可求值的字面量；列与列比较时用默认值
        val other = if (columnStatsOf(e.left, table, stats) != null) e.right else e.left
        if (other is ColumnRef) return DEFAULT_OTHER_SELECTIVITY
        return col.equalitySelectivity()
    }

    private fun rangeSelectivity(e: BinaryOp, table: TableSchema, stats: TableStats?): Double {
        val colLeft = columnStatsOf(e.left, table, stats)
        val col = colLeft ?: columnStatsOf(e.right, table, stats) ?: return DEFAULT_RANGE
        val constExpr = if (colLeft != null) e.right else e.left
        val v = constOf(constExpr, table, emptyMap())?.let { numericOf(it) } ?: return DEFAULT_RANGE
        // `常量 <op> 列` 要翻转：`100 > age` 等价于 `age < 100`
        val op = if (colLeft != null) e.op else when (e.op) {
            "<" -> ">"; "<=" -> ">="; ">" -> "<"; else -> "<="
        }
        // ⚠️ 补集必须取**同一侧**的累积分布：`x > v` = 1 − P(x ≤ v)。
        // 写成 `1 − P(x ≥ v)` 得到的是 P(x < v)——上界谓词会被算成下界谓词。
        // 分布对称时两者的数字碰巧接近（这也是它容易潜伏的原因），偏斜分布上
        // 会完全反掉：`age > 60` 在 97% 都大于 60 的表上会被估成 3%，
        // 于是优化器放弃本该使用的索引/范围路径去全表扫描。
        return when (op) {
            "<", "<=" -> col.rangeSelectivity(null, v)
            else -> (1.0 - col.rangeSelectivity(null, v)).coerceIn(0.0, 1.0)
        }
    }

    private fun columnStatsOf(e: Expr, table: TableSchema, stats: TableStats?): ColumnStats? {
        val name = (e as? ColumnRef)?.name ?: return null
        table.indexOfOrNull(name) ?: return null
        return stats?.column(name)
    }

    /** 常量求值；非常量或 NULL 返回 null。 */
    fun constOf(e: Expr, table: TableSchema, vars: Map<String, Datum>): Datum? = try {
        val d = Eval.eval(e, EvalContext(table, null, vars))
        if (d is Datum.Null) null else d
    } catch (_: Exception) {
        null
    }

    /** 数值形式（范围估算用）。时间戳按毫秒、定点数按未缩放整数——都是单调刻度。 */
    fun numericOf(d: Datum): Double? = when (d) {
        is Datum.I64 -> d.v.toDouble()
        is Datum.F64 -> d.v
        is Datum.Bool -> if (d.v) 1.0 else 0.0
        is Datum.Ts -> d.millis.toDouble()
        is Datum.Dec -> d.unscaled.toDouble()
        else -> null
    }

    // ── 约束提取 ──────────────────────────────────────────────────

    /** 摊平 `a AND b AND c`。刻意不穿透 OR——OR 之下没有可用的合取信息。 */
    fun conjuncts(e: Expr?): List<Expr> = when (e) {
        null -> emptyList()
        is BinaryOp -> if (e.op.equals("AND", ignoreCase = true)) {
            conjuncts(e.left) + conjuncts(e.right)
        } else listOf(e)
        else -> listOf(e)
    }

    /**
     * 找出所有「索引首列 = 常量」的候选（索引前缀上的等值约束）。
     *
     * 这是本次优化器相对旧实现**唯一会改变执行计划**的地方：旧实现只认
     * `WHERE indexed_col = 常量` 这一种整条 WHERE 就是一个等值的形状，
     * `WHERE indexed_col = 5 AND other = 3` 会整个退化成全表扫描。
     * 从 AND 合取项里挑一个等值谓词是**结果等价**的——索引路径拿到行以后
     * 仍然会用完整 WHERE 过滤一遍（见 `Executor.indexLookup`）。
     */
    fun indexCandidates(
        table: TableSchema,
        where: Expr?,
        vars: Map<String, Datum>,
        stats: TableStats?,
    ): List<IndexCandidate> {
        val out = ArrayList<IndexCandidate>()
        val cs = conjuncts(where)
        // 只看 ACTIVE：BUILDING 的索引还没回填完，选它取数会**静默少返回行**
        for (idx in table.activeIndexes) {
            if (idx.kind != IndexKind.ORDERED && idx.kind != IndexKind.HASH) continue
            val eqs = HashMap<String, Datum>()
            for (c in cs) {
                val (col, value) = equalityParts(c, table, vars) ?: continue
                eqs.putIfAbsent(col.lowercase(), value)
            }
            if (eqs.isEmpty()) continue
            var bound = 0
            for (c in idx.columns) {
                if (eqs.containsKey(c.lowercase())) bound++ else break
            }
            if (bound == 0) continue
            val first = eqs[idx.columns[0].lowercase()] ?: continue
            out.add(
                IndexCandidate(
                    index = idx,
                    value = first,
                    boundColumns = bound,
                    selectivity = stats?.column(idx.columns[0])?.equalitySelectivity()
                        ?: ColumnStats.DEFAULT_EQ_SELECTIVITY,
                )
            )
        }
        return out.sortedWith(
            compareByDescending<IndexCandidate> { it.boundColumns }
                .thenByDescending { it.index.unique }
                .thenBy { it.index.columns.size }
        )
    }

    /**
     * 主键键区间的选择率。
     *
     * 复合主键上键是**逐列拼接**的，所以选择率是「前缀各列等值选择率之积 ×
     * 停止列上的范围选择率」。把前缀各列当成独立事件是这个模型的既有近似
     * （与 §10.3 的直方图/ndv 一致），估出来的是量级，不是精确值。
     */
    fun keyRangeSelectivity(
        range: KeyRange,
        table: TableSchema,
        stats: TableStats?,
        rowCount: Long,
    ): Double {
        val pkIdx = table.primaryKeyIndexes
        if (pkIdx.isEmpty()) return 1.0
        if (range.exactPk != null) return if (rowCount > 0L) 1.0 / rowCount else 1.0
        var sel = 1.0
        for ((k, colIdx) in pkIdx.withIndex()) {
            if (k >= range.prefix.size) break
            sel *= stats?.column(table.columns[colIdx].name)?.equalitySelectivity()
                ?: ColumnStats.DEFAULT_EQ_SELECTIVITY
        }
        val stop = range.prefix.size
        if (stop < pkIdx.size && (range.low != null || range.high != null)) {
            val cs = stats?.column(table.columns[pkIdx[stop]].name)
            val lo = range.low?.let { numericOf(it) }
            val hi = range.high?.let { numericOf(it) }
            sel *= if (cs != null && (lo != null || hi != null)) {
                cs.rangeSelectivity(lo, hi)
            } else {
                ColumnStats.DEFAULT_RANGE_SELECTIVITY
            }
        }
        return sel.coerceIn(0.0, 1.0)
    }

    /** `列 = 常量`（或反向）的分解；两侧都是列、或是 OR 之下时返回 null。 */
    fun equalityParts(e: Expr, table: TableSchema, vars: Map<String, Datum>): Pair<String, Datum>? {
        val b = e as? BinaryOp ?: return null
        if (b.op != "=" && b.op != "<=>") return null
        val (colRef, valExpr) = when {
            b.left is ColumnRef && b.right !is ColumnRef -> b.left to b.right
            b.right is ColumnRef && b.left !is ColumnRef -> b.right to b.left
            else -> return null
        }
        table.indexOfOrNull(colRef.name) ?: return null
        val v = constOf(valExpr, table, vars) ?: return null
        return colRef.name to v
    }

    // ── 路径选择 ──────────────────────────────────────────────────

    /**
     * 选出最便宜的访问路径。
     *
     * ⭐ **R6 是硬规则，不是代价比较**：只要元数据计数可用（`COUNT(*)` 这类），
     * 它就直接胜出。理由是这条路的代价是 **0 次请求**，而任何扫描至少 1 次；
     * 把 0 放进比较里虽然也会赢，但把它写成显式的硬规则可以防止将来有人
     * 「调参」把代价函数改到让它输——那会静默退化成扫描，而用户拿 COUNT
     * 做分页与对账（§8.9.7）。
     *
     * 兜底：**没有统计信息时**，比较退化成旧的硬编码优先级链
     * （精确主键 → 键范围 → 二级索引 → 全表扫描），未经 ANALYZE 的表
     * 的行为与优化器引入之前**完全一致**，不会退化。
     */
    fun choose(input: PlannerInput): PathPlan {
        if (input.fullText) return fullTextPlan(input)
        if (input.metadataCountRows != null) return metadataCountPlan(input)

        // ⭐ 没有统计信息 ⇒ **不做代价比较**，退回硬编码优先级链。
        //
        // 这不只是保守，而是「不退化」的硬要求：没有统计时所有选择率都是拍出来的
        // 默认值（等值 0.1、范围 0.333），拿它们比价会把 `WHERE indexed = 5` 在小表上
        // 判成「全表扫描更便宜」——索引明明可用却不用。未经 ANALYZE 的表必须与
        // 优化器引入之前**行为一致**（设计文档 §10.2 的兜底链）。
        if (input.stats == null) return fallbackPlan(input)

        val candidates = ArrayList<PathPlan>(4)
        if (input.constPk) candidates.add(constPlan(input))
        checkRangePlan(input)?.let { candidates.add(it) }
        checkIndexPlan(input)?.let { candidates.add(it) }
        candidates.add(scanPlan(input))

        // 打平容差内按「越具体越好」排序：点查 > 索引 > 键范围 > 全表。
        val best = candidates.minByOrNull { it.gets } ?: scanPlan(input)
        val tie = candidates.filter { it.gets <= best.gets + TIE_EPSILON }
        return tie.firstOrNull { it.type.ordinal < best.type.ordinal } ?: best
    }

    /**
     * 硬编码优先级链：**精确主键 → 键范围 → 二级索引 → 全表扫描**。
     *
     * 顺序与优化器引入之前完全一致；`rows`/`filtered`/`gets` 仍然给出来，
     * 但它们是用默认选择率拍出来的，`EXPLAIN` 会同时标注「本表没有统计」。
     */
    fun fallbackPlan(input: PlannerInput): PathPlan = when {
        input.constPk -> constPlan(input)
        input.rangeSelectivity != null -> checkRangePlan(input) ?: scanPlan(input)
        input.indexCandidates.isNotEmpty() -> checkIndexPlan(input) ?: scanPlan(input)
        else -> scanPlan(input)
    }

    private fun constPlan(input: PlannerInput): PathPlan = PathPlan(
        type = AccessType.CONST,
        key = "PRIMARY",
        keyColumns = input.table.primaryKey,
        rows = 1L,
        // 点查找返回 1 行；residual 给出「这一行还要再过掉多少其它谓词」（EXPLAIN 的 filtered）
        filtered = residual(input, 1.0 / maxOf(1L, input.rowCount).toDouble()),
        gets = POINT_GETS,
        possibleKeys = listOf("PRIMARY"),
        extra = buildList {
            add("Using primary key point lookup")
            if (input.where != null) add("Using where")
        },
    )

    private fun checkRangePlan(input: PlannerInput): PathPlan? {
        val sel = input.rangeSelectivity ?: return null
        val rows = maxOf(1L, Math.round(input.rowCount * sel))
        // 前缀等值本身就确定了连续区间；选择率为 0 时不该用这条路
        if (input.rowCount > 0L && sel <= 0.0) return null
        val gets = scanGets(rows, input.rowsPerBlock, input.totalShards)
        return PathPlan(
            type = AccessType.RANGE,
            key = "PRIMARY",
            keyColumns = input.rangeColumns.ifEmpty { input.table.primaryKey },
            rows = rows,
            filtered = residual(input, sel),
            gets = gets,
            possibleKeys = listOf("PRIMARY"),
            extra = buildList {
                add("Using primary key range")
                if (input.where != null) add("Using where")
            },
        )
    }

    private fun checkIndexPlan(input: PlannerInput): PathPlan? {
        val c = input.indexCandidates.firstOrNull() ?: return null
        val rows = maxOf(1L, Math.round(input.rowCount * c.selectivity))
        val gets = indexGets(rows, input.rowsPerBlock)
        return PathPlan(
            type = AccessType.REF,
            key = c.index.name,
            keyColumns = c.index.columns.take(c.boundColumns),
            rows = rows,
            filtered = residual(input, c.selectivity),
            gets = gets,
            possibleKeys = input.indexCandidates.map { it.index.name }.distinct(),
            extra = buildList {
                // 索引项里存的是**整行**（见 IndexCodec），所以命中即得到数据，
                // 不需要回表。"Using index" 在这里是真的，不是抄 MySQL 的措辞。
                add("Using index")
                if (input.where != null) add("Using where")
            },
        )
    }

    private fun scanPlan(input: PlannerInput): PathPlan {
        val rows = input.rowCount
        val gets = scanGets(rows, input.rowsPerBlock, input.shards)
        return PathPlan(
            type = AccessType.ALL,
            key = null,
            rows = rows,
            filtered = input.whereSelectivity,
            gets = gets,
            possibleKeys = emptyList(),
            extra = buildList {
                if (input.blockPruning) add("Using skip index (block-level pruning)")
                if (input.where != null) add("Using where")
            },
        )
    }

    private fun metadataCountPlan(input: PlannerInput): PathPlan = PathPlan(
        type = AccessType.METADATA_COUNT,
        key = null,
        rows = input.metadataCountRows ?: 0L,
        filtered = 1.0,
        gets = METADATA_COUNT_GETS,
        possibleKeys = emptyList(),
        extra = buildList {
            // §8.9.6 要求 EXPLAIN 里能看出这条路 —— 文案与设计文档一致
            add("Using metadata count (manifest, exact, 0 object reads)")
            if (input.where != null) add("Using where")
        },
    )

    private fun fullTextPlan(input: PlannerInput): PathPlan {
        val hits = maxOf(1L, Math.round(input.rowCount * input.fullTextSelectivity))
        return PathPlan(
            type = AccessType.FULLTEXT,
            key = "FULLTEXT",
            rows = hits,
            filtered = residual(input, input.fullTextSelectivity),
            gets = fullTextGets(hits, input.rowsPerBlock, input.shards),
            extra = listOf("Using inverted index (BM25)"),
        )
    }

    /** 扫描路径取出 `pathSelectivity` 比例的行之后，还要再过掉多少（EXPLAIN 的 filtered）。 */
    private fun residual(input: PlannerInput, pathSelectivity: Double): Double {
        if (pathSelectivity <= 0.0) return 1.0
        return (input.whereSelectivity / pathSelectivity).coerceIn(0.0, 1.0)
    }

    /**
     * 收尾提示（`Extra` 的通用部分）。与路径无关的那些。
     *
     * `Using filesort` 是**如实报告**：执行器在内存里对结果排序（`finishSelect`），
     * 没有利用主键序，也没有谓词/LIMIT 下推到排序之前。
     *
     * ⭐ `metadataCountBlocked` 也在这里，而**不是**放在计数路径自己的 extra 里：
     * 那条路的 extra 只在它被选中时才出现，而「为什么没选中它」恰恰是没选中时才需要
     * 回答的问题。`COUNT(*)` 本该 0 次 GET 却退化成扫描时，EXPLAIN 必须能看出原因
     * （§8.9.6）——否则使用者会以为这条优化根本不适用，而不是它退化掉了。
     */
    fun trailingExtras(input: PlannerInput): List<String> = buildList {
        if (input.orderBy) add("Using filesort")
        if (input.grouped) add("Using temporary (group by / aggregate)")
        if (input.metadataCountRows == null) input.metadataCountBlocked?.let { add(it) }
        if (input.stats == null) {
            add("no statistics: hard-coded fallback path order (run ANALYZE TABLE for cost-based planning)")
        }
    }
}
