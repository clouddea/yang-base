package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.txn.Partitioning

/**
 * 分区裁剪（设计文档 §9.2 的 L1 层）。
 *
 * 裁剪的全部意义是**不产生 IO**：被排除的分区连 manifest 都不会读。
 * 它是三层裁剪（分区 → 文件 → 块）里最粗也最便宜的一层——
 * **纯内存计算，零对象存储请求**。
 *
 * 这一层只认**分区列上的常量谓词**。这不是能力缺失，而是分区裁剪的固有边界：
 * 分区边界是静态的元数据，只有把谓词折叠成常量集合才谈得上「排除整个分区」。
 */
object PartitionPrune {

    /**
     * 从 WHERE 中抽出分区列上的约束。
     *
     * 返回 null 表示推不出可用约束（调用方应扫描全部分区）。
     * 返回值里的约束**可能比原 WHERE 更宽**——裁剪只允许放宽，
     * 绝不允许收紧，否则会漏掉本该返回的行。真正的过滤仍由 WHERE 兜底。
     */
    fun constraintFor(table: TableSchema, where: Expr?): Partitioning.Constraint? {
        if (where == null) return null
        if (Partitioning.hasHashColumn(table)) {
            val values = hashValues(where, table) ?: return null
            return Partitioning.Constraint(values.map { Partitioning.Interval(it, true, it, true) })
        }
        if (!table.isEnumeratedPartition) return null
        val col = table.partitionColumn ?: return null
        val intervals = collect(where, col) ?: return null
        if (intervals.isEmpty()) return null

        // ⚠️ 必须把字面量转成**分区列的类型**再比较。
        // 分区边界是按列类型求值的（`'2026-04-01'` → Ts），而 WHERE 里的字面量
        // 词法上只是字符串。不转换就会走跨类型比较 → 永不相等 → **整片分区被错误裁掉**，
        // 行静默消失。裁剪裁错比不裁剪严重得多，所以这一步是正确性要求而非优化。
        val type = table.columns[table.indexOf(col)].type
        return Partitioning.Constraint(
            intervals.map { Partitioning.Interval(it.low?.coerceTo(type), it.lowInclusive, it.high?.coerceTo(type), it.highInclusive) }
        )
    }

    // HASH 只需离散值。AND 取任一已知约束（放宽），OR 必须两侧都有约束；
    // 不在类型转换前做区间求交，避免 42 与 '42' 比较出空集而漏行。
    private fun hashValues(e: Expr, table: TableSchema): List<Datum>? {
        val col = table.partitionColumn ?: return null
        val type = table.column(col).type
        fun value(expr: Expr) = lit(expr)?.coerceTo(type)
        return when (e) {
            is BinaryOp -> when (e.op.uppercase()) {
                "AND" -> hashValues(e.left, table) ?: hashValues(e.right, table)
                "OR" -> {
                    val left = hashValues(e.left, table) ?: return null
                    val right = hashValues(e.right, table) ?: return null
                    left + right
                }
                "=", "<=>" -> {
                    val v = when {
                        refersTo(e.left, col) -> value(e.right)
                        refersTo(e.right, col) -> value(e.left)
                        else -> null
                    }
                    v?.let { listOf(it) }
                }
                else -> null
            }
            is InList -> if (!e.negated && refersTo(e.expr, col)) {
                val vs = e.values.map { value(it) }
                if (vs.any { it == null }) null else vs.filterNotNull()
            } else null
            else -> null
        }
    }

    /**
     * 返回 null = 这个表达式给不出关于分区列的**任何**信息。
     * 返回空列表 = 表达式恒假（例如 `col IN ()`）。
     */
    private fun collect(e: Expr, col: String): List<Partitioning.Interval>? = when (e) {
        is BinaryOp -> when (e.op.uppercase()) {
            "AND" -> {
                val l = collect(e.left, col)
                val r = collect(e.right, col)
                when {
                    l == null -> r
                    r == null -> l
                    // 两侧都有约束 → 求交
                    else -> intersect(l, r)
                }
            }
            // OR 只有在两侧都给出约束时才安全，否则只能放弃裁剪
            "OR" -> {
                val l = collect(e.left, col)
                val r = collect(e.right, col)
                if (l == null || r == null) null else l + r
            }
            "=", "<=>" -> equality(e, col)
            "<", "<=", ">", ">=" -> comparison(e, col)
            else -> null
        }
        is Between -> {
            if (e.negated) null
            else if (!refersTo(e.expr, col)) null
            else {
                val lo = lit(e.low) ?: return null
                val hi = lit(e.high) ?: return null
                listOf(Partitioning.Interval(lo, true, hi, true))
            }
        }
        is InList -> {
            if (e.negated || !refersTo(e.expr, col)) null
            else {
                val vs = e.values.map { lit(it) }
                if (vs.any { it == null }) null else vs.map { Partitioning.Interval(it, true, it, true) }
            }
        }
        else -> null
    }

    /**
     * `col <op> 常量` 与 `常量 <op> col` 两种写法都要认——
     * 反写的方向要翻转比较符，否则 `100 > age` 会被当成 `age > 100`。
     */
    private fun comparison(e: BinaryOp, col: String): List<Partitioning.Interval>? {
        val colOnLeft = refersTo(e.left, col) && lit(e.right) != null
        val colOnRight = refersTo(e.right, col) && lit(e.left) != null
        if (!colOnLeft && !colOnRight) return null

        val v = (if (colOnLeft) lit(e.right) else lit(e.left)) ?: return null
        return if (colOnLeft) {
            // col < v  → 上界；col > v → 下界
            when (e.op) {
                "<" -> listOf(Partitioning.Interval(null, false, v, false))
                "<=" -> listOf(Partitioning.Interval(null, false, v, true))
                ">" -> listOf(Partitioning.Interval(v, false, null, false))
                else -> listOf(Partitioning.Interval(v, true, null, false))
            }
        } else {
            // v < col 等价于 col > v：比较符要翻转
            when (e.op) {
                "<" -> listOf(Partitioning.Interval(v, false, null, false))
                "<=" -> listOf(Partitioning.Interval(v, true, null, false))
                ">" -> listOf(Partitioning.Interval(null, false, v, false))
                else -> listOf(Partitioning.Interval(null, false, v, true))
            }
        }
    }

    private fun equality(e: BinaryOp, col: String): List<Partitioning.Interval>? {
        val v = when {
            refersTo(e.left, col) -> lit(e.right)
            refersTo(e.right, col) -> lit(e.left)
            else -> null
        } ?: return null
        return listOf(Partitioning.Interval(v, true, v, true))
    }

    /**
     * 表达式是否**就是**这一列。
     *
     * 刻意不认 `col + 0` 这类包装：分区路由用的是原始值，
     * 一个被函数包过的列未必与分区边界同序，认了会裁错。
     */
    private fun refersTo(e: Expr, col: String): Boolean = e is ColumnRef && e.name.equals(col, ignoreCase = true)

    private fun lit(e: Expr): Datum? = (e as? Literal)?.value?.takeIf { it !is Datum.Null }

    /**
     * 两组区间的交集。
     *
     * 逐个配对求交即可——`col > 10 AND col < 100` 这种合取在解析后
     * 两侧各是一个区间，配对后仍是一个区间；只有跨 OR 的并集配对才可能变多。
     */
    private fun intersect(
        a: List<Partitioning.Interval>,
        b: List<Partitioning.Interval>,
    ): List<Partitioning.Interval> {
        val out = ArrayList<Partitioning.Interval>()
        for (x in a) for (y in b) intersectOne(x, y)?.let { out.add(it) }
        return out
    }

    private fun intersectOne(x: Partitioning.Interval, y: Partitioning.Interval): Partitioning.Interval? {
        // 下界取更紧的那个
        val (lo, loInc) = tighterLow(x.low, x.lowInclusive, y.low, y.lowInclusive) ?: return null
        val (hi, hiInc) = tighterHigh(x.high, x.highInclusive, y.high, y.highInclusive) ?: return null
        if (lo != null && hi != null) {
            val c = lo.compareTo(hi)
            if (c > 0 || (c == 0 && !(loInc && hiInc))) return null
        }
        return Partitioning.Interval(lo, loInc, hi, hiInc)
    }

    private fun tighterLow(
        a: Datum?, aInc: Boolean, b: Datum?, bInc: Boolean,
    ): Pair<Datum?, Boolean>? {
        if (a == null) return b to bInc
        if (b == null) return a to aInc
        val c = a.compareTo(b)
        return when {
            c > 0 -> a to aInc
            c < 0 -> b to bInc
            else -> a to (aInc && bInc)
        }
    }

    private fun tighterHigh(
        a: Datum?, aInc: Boolean, b: Datum?, bInc: Boolean,
    ): Pair<Datum?, Boolean>? {
        if (a == null) return b to bInc
        if (b == null) return a to aInc
        val c = a.compareTo(b)
        return when {
            c < 0 -> a to aInc
            c > 0 -> b to bInc
            else -> a to (aInc && bInc)
        }
    }
}
