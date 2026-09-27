package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.storage.ColumnRange

/**
 * 从 WHERE 里抽取可以下推到**块级**的列约束（设计文档 §8.5）。
 *
 * 这是跳数索引省下 IO 的入口：约束越具体，能被 minmax / set / bloom 判否的块越多，
 * 而被判否的块**一次 GET 都不产生**。
 *
 * ## 只允许放宽，绝不允许收紧
 *
 * 抽出来的约束**可能比原 WHERE 更宽**——这没关系，真正的过滤由 WHERE 兜底。
 * 反过来（抽得更窄）会让本该读的块被跳过，那是**静默漏行**。
 * 所以这里只认最朴素的形式：`列 op 常量`、`列 BETWEEN a AND b`、`列 IN (...)`。
 * `列 + 0 op 常量` 这种包装一律不认——一个被函数包过的列与列本身不同序，
 * 认了就会裁错。
 */
object PruneRanges {

    /** 一次最多下推几条约束。再多的话索引自身的加载成本会超过剪枝收益。 */
    private const val MAX_RANGES = 8

    fun extract(where: Expr?, table: TableSchema): List<ColumnRange> {
        if (where == null) return emptyList()
        val out = ArrayList<ColumnRange>()
        collect(where, table, out)
        return out.take(MAX_RANGES)
    }

    private fun collect(e: Expr, table: TableSchema, out: MutableList<ColumnRange>) {
        when (e) {
            // 合取：两侧都能各自下推（AND 的语义就是两个独立约束）
            is BinaryOp -> when (e.op.uppercase()) {
                "AND" -> { collect(e.left, table, out); collect(e.right, table, out) }
                // OR 不能分别下推：`a=1 OR b=2` 下推成两个约束会把两个都当成必须满足
                "=" -> eq(e, table, out)
                "<", "<=", ">", ">=" -> cmp(e, table, out)
                else -> {}
            }
            is Between -> if (!e.negated) between(e, table, out)
            is InList -> if (!e.negated) inList(e, table, out)
            is Like -> if (!e.negated) {
                val col = column(e.expr, table)
                val pattern = (e.pattern as? Literal)?.value as? Datum.Str
                if (col != null && col.second.isString && pattern != null) {
                    add(out, col.first, ColumnRange(column = col.first, likePattern = pattern.v))
                }
            }
            else -> {}
        }
    }

    private fun columnName(e: Expr): String? = (e as? ColumnRef)?.name

    /**
     * 取字面量的值，并**按列类型**解释它。
     *
     * ⚠️ 这一步不能省：过滤（[Eval.evalBinary]）会按列类型把字面量转过去，
     * 剪枝若不转，就会拿"字符串编码"的值去和"整数编码"的块边界比大小——
     * 比不过，于是**整块被跳过、行静默消失**。同一条 `WHERE ts = '5'`
     * 在扫描路径上能查出 1 行、在带跳数索引的路径上返回 0 行，两边都不报错。
     */
    private fun lit(e: Expr, type: ColumnType?): Datum? =
        (e as? Literal)?.value?.takeIf { it !is Datum.Null }?.let { v -> type?.let { v.coerceTo(it) } ?: v }

    /** 列在表里的下标与类型。 */
    private fun column(e: Expr, table: TableSchema): Pair<Int, ColumnType>? {
        val name = (e as? ColumnRef)?.name ?: return null
        val i = table.indexOfOrNull(name) ?: return null
        return i to table.columns[i].type
    }

    private fun add(out: MutableList<ColumnRange>, colIdx: Int, r: ColumnRange) {
        if (out.any { it.column == colIdx }) return   // 同一列只取第一条，避免自相矛盾
        out.add(r)
    }

    private fun eq(e: BinaryOp, table: TableSchema, out: MutableList<ColumnRange>) {
        val (colExpr, valExpr) = when {
            e.left is ColumnRef && e.right is Literal -> e.left to e.right
            e.right is ColumnRef && e.left is Literal -> e.right to e.left
            else -> return
        }
        val (i, type) = column(colExpr, table) ?: return
        val v = lit(valExpr, type) ?: return
        add(out, i, ColumnRange(column = i, equality = v))
    }

    private fun cmp(e: BinaryOp, table: TableSchema, out: MutableList<ColumnRange>) {
        val colLeft = e.left is ColumnRef && e.right is Literal
        val colRight = e.right is ColumnRef && e.left is Literal
        if (!colLeft && !colRight) return
        val colExpr = if (colLeft) e.left else e.right
        val valExpr = if (colLeft) e.right else e.left
        val (i, type) = column(colExpr, table) ?: return
        val v = lit(valExpr, type) ?: return
        // 列在右边时比较符要翻转——`100 > age` 等价于 `age < 100`。
        // 不翻转就会把结果集整个翻过来，那是最难发现的一类错误。
        val op = if (colLeft) e.op else when (e.op) {
            "<" -> ">"; "<=" -> ">="; ">" -> "<"; else -> "<="
        }
        add(
            out, i,
            when (op) {
                "<" -> ColumnRange(column = i, high = v, highInclusive = false)
                "<=" -> ColumnRange(column = i, high = v, highInclusive = true)
                ">" -> ColumnRange(column = i, low = v, lowInclusive = false)
                else -> ColumnRange(column = i, low = v, lowInclusive = true)
            },
        )
    }

    private fun between(e: Between, table: TableSchema, out: MutableList<ColumnRange>) {
        val (i, type) = column(e.expr, table) ?: return
        val lo = lit(e.low, type) ?: return
        val hi = lit(e.high, type) ?: return
        add(out, i, ColumnRange(column = i, low = lo, lowInclusive = true, high = hi, highInclusive = true))
    }

    private fun inList(e: InList, table: TableSchema, out: MutableList<ColumnRange>) {
        val (i, type) = column(e.expr, table) ?: return
        val vs = e.values.map { lit(it, type) }
        if (vs.any { it == null }) return
        // `IN` 可以下推成 min/max 的外包络：块内所有值都在 [min(vs), max(vs)] 之外
        // 就一定不含任何一个。这是**放宽**（外包络覆盖了中间那些不想要的值），
        // 但方向正确——放宽只少剪几个块，不会漏行。
        val vals = vs.filterNotNull()
        val lo = vals.minWith { a, b -> a.compareTo(b) }
        val hi = vals.maxWith { a, b -> a.compareTo(b) }
        add(out, i, ColumnRange(column = i, low = lo, lowInclusive = true, high = hi, highInclusive = true))
    }
}
