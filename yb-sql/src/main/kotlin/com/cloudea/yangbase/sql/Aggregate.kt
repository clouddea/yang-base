package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema

/**
 * 分组与聚合（`GROUP BY` / `HAVING` / `SUM` / `AVG` / `MIN` / `MAX` / `GROUP_CONCAT`）。
 *
 * ## 为什么单独一个文件
 *
 * 聚合不是"多几个函数"：它把查询从**逐行**变成**分组**，于是三件事同时变了——
 * 求值顺序（WHERE 在分组前、HAVING 在分组后）、求值上下文（聚合值不在任何一行里）、
 * 以及合法性规则（ONLY_FULL_GROUP_BY）。这三件事都必须成体系地做对，
 * 散在 `Executor` 的各处只会得到一份"看起来能跑、边界全错"的实现。
 *
 * ## 求值顺序（SQL 的语义骨架，不能颠倒）
 *
 * ```
 *   FROM → WHERE → GROUP BY → 聚合 → HAVING → SELECT → ORDER BY → LIMIT
 * ```
 *
 * 最容易搞错的两处：
 *  - **`WHERE` 里不能有聚合**。它在分组之前求值，那时根本没有「一组」。
 *    静默当成 NULL 会让整个查询返回空集而不报错——所以这里是硬报错（1111）。
 *  - **`HAVING` 在分组之后**，它可以引用聚合，但**不能**把 WHERE 的活干了：
 *    `WHERE` 少写一个条件、把它写进 `HAVING`，结果相同但代价差一个数量级的行数
 *    （那属于性能问题，语义上仍然正确）。
 *
 * ## ONLY_FULL_GROUP_BY：本项目选了"报错"而不是"随便挑一行"
 *
 * MySQL 5.7 起默认开启 `ONLY_FULL_GROUP_BY`：`SELECT city, name ... GROUP BY city`
 * 会**报错**。关掉它的旧行为是"每组随便挑一行"——那是**不确定**的结果：
 * 同一查询在不同时刻返回不同的 name，而 SQL 层看起来一切正常。
 *
 * 本系统的 `sql_mode` 如实回报 `ONLY_FULL_GROUP_BY`，所以这里必须真的强制它：
 * 报了却不做，等于用一个"看起来更兼容"的假象换一个静默的不确定结果，
 * 那正是这个项目最不能接受的一类缺陷。
 */
object Aggregates {

    /**
     * 一行在一个组里的表示：**代表行 + 本组的聚合结果**。
     *
     * 代表行用来求值分组键与普通列（同组的这些值必然相同），
     * 聚合结果供投影 / `HAVING` / `ORDER BY` 查表。
     * 不把聚合结果塞进"行"里，是因为行是 schema 对齐的定长结构，
     * 而聚合值的数量与位置取决于查询文本——硬塞会把两者都搞脏。
     */
    class GroupCell(val values: List<Datum>, val aggs: Map<String, Datum>)

    /** 不分组时的公共空表：避免每行都建一个 Map。 */
    private val NO_AGGS = emptyMap<String, Datum>()

    /** 把普通行包成"整表一组"的形式（有聚合但没 `GROUP BY`）。 */
    fun singleGroup(rows: List<List<Datum>>, computed: Map<String, Datum>): List<GroupCell> =
        if (rows.isEmpty()) {
            // SQL 语义：无 GROUP BY 的聚合**总是**返回一行。
            // `SELECT COUNT(*) FROM t` 在空表上返回 0 行是错的；`SUM(v)` 返回 NULL 才对。
            listOf(GroupCell(EMPTY_ROW, computed))
        } else {
            listOf(GroupCell(rows[0], computed))
        }

    private val EMPTY_ROW: List<Datum> = emptyList()

    /** 把每行包成 [GroupCell]（不含聚合的分组查询：只有分组键，没有聚合）。 */
    fun wrap(rows: List<List<Datum>>): List<GroupCell> = rows.map { GroupCell(it, NO_AGGS) }

    /**
     * 按 `GROUP BY` 表达式分组。
     *
     * 键是各分组表达式的值组成的列表——用 `Datum` 相等性做键，因此
     * **NULL 会归到同一组**（SQL 的语义：NULL 之间"相等"只发生在分组这一处），
     * 而 `1` 与 `"1"` 是两组（值语义不同）。
     */
    fun group(
        table: TableSchema,
        rows: List<List<Datum>>,
        groupBy: List<Expr>,
        vars: Map<String, Datum>,
    ): List<Pair<List<Datum>, List<List<Datum>>>> {
        val out = LinkedHashMap<List<Datum>, MutableList<List<Datum>>>()
        for (row in rows) {
            val ctx = EvalContext(table, row, vars)
            val key = groupBy.map { Eval.eval(it, ctx) }
            out.getOrPut(key) { ArrayList() }.add(row)
        }
        return out.map { it.key to it.value }
    }

    /**
     * 收集查询里出现的**全部**聚合表达式（投影 + `HAVING` + `ORDER BY`），按规范键去重。
     *
     * 必须把三处都收进来：`SELECT city FROM t GROUP BY city HAVING COUNT(*) > 1`
     * 里的 `COUNT(*)` 不在投影里，但照样要算。只收投影的话，`HAVING` 会查不到值
     * 而抛「Invalid use of group function」——一个看起来像用户写错了、实际是执行器漏算的错。
     */
    fun collect(stmt: SelectStmt): List<Expr> {
        val seen = LinkedHashMap<String, Expr>()
        fun walk(e: Expr?) {
            if (e == null) return
            if (e is CountCall || e is AggCall) {
                seen.putIfAbsent(aggregateKey(e), e)
                // 聚合的**参数**里不允许再嵌聚合（`SUM(COUNT(*))` 是 1111），
                // 但仍然走一遍——嵌套的情况由下面的 validate 报错，这里只负责收集
                walk(e.let { if (it is CountCall) it.arg else (it as AggCall).arg })
                return
            }
            childrenOf(e).forEach { walk(it) }
        }
        stmt.projections.forEach { if (it is SelectItem.Field) walk(it.expr) }
        walk(stmt.having)
        stmt.orderBy.forEach { walk(it.expr) }
        return seen.values.toList()
    }

    /**
     * 计算一个聚合在一组行上的结果。
     *
     * 各函数的 SQL 语义（**NULL 的处理是最容易错的地方**）：
     * - `COUNT(x)` 数**非 NULL** 的 x；`COUNT(*)` 数行数。
     * - `SUM` / `AVG` / `MIN` / `MAX` **忽略 NULL**；一组全 NULL（或空）时返回 NULL，
     *   而 `COUNT` 此时返回 0——这两个"空"的含义不同，不能统一。
     * - `GROUP_CONCAT` 忽略 NULL，用 `,` 连接；全 NULL 时返回 NULL。
     */
    fun compute(
        agg: Expr,
        rows: List<List<Datum>>,
        table: TableSchema,
        vars: Map<String, Datum>,
        /**
         * 每行**所属那一组**已经算好的聚合值，与 [rows] 一一对应。
         *
         * 只有窗口聚合会用到：`SUM(SUM(v)) OVER (ORDER BY x)` 里的内层 `SUM(v)`
         * 是"分组聚合"，它在逐行求值时得从这张表里查（见 [EvalContext.aggregates]）。
         * 传 null 表示这批行上没有聚合可查——那时内层出现聚合就是真错了。
         */
        aggsPerRow: List<Map<String, Datum>>? = null,
        /**
         * **本语句**的时刻。聚合是逐行求值的，若不显式传下去，
         * 每一行都会拿到"构造这一刻"——`COUNT(DISTINCT NOW())` 于是可能大于 1，
         * 而 `NOW()` 在 MySQL 里是语句级常量。
         */
        nowMillis: Long = System.currentTimeMillis(),
    ): Datum {
        val distinct = when (agg) {
            is CountCall -> agg.distinct
            is AggCall -> agg.distinct
            else -> false
        }
        val arg = when (agg) {
            is CountCall -> agg.arg
            is AggCall -> agg.arg
            else -> null
        }
        val star = agg is CountCall && agg.arg == null

        val values = ArrayList<Datum>(rows.size)
        for ((idx, row) in rows.withIndex()) {
            val v = if (star) Datum.I64(1)
            else Eval.eval(
                arg!!,
                EvalContext(table, row, vars, aggsPerRow?.getOrNull(idx) ?: emptyMap(), nowMillis),
            )
            if (v is Datum.Null) continue          // 聚合一律忽略 NULL
            values.add(v)
        }
        val input = if (distinct) values.distinct() else values

        return when {
            agg is CountCall -> Datum.I64(input.size.toLong())
            agg is AggCall -> when (agg.name) {
                "SUM" -> sumOf(input)
                "AVG" -> if (input.isEmpty()) Datum.Null
                else Datum.F64(input.sumOf { numeric(it) } / input.size)
                "MIN" -> input.minOrNull() ?: Datum.Null
                "MAX" -> input.maxOrNull() ?: Datum.Null
                "GROUP_CONCAT" ->
                    if (input.isEmpty()) Datum.Null
                    else Datum.Str(input.joinToString(",") { it.asString() })
                else -> throw SqlException(
                    MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", "不支持的聚合函数 ${agg.name}",
                )
            }
            else -> throw IllegalStateException("compute() 只处理聚合表达式")
        }
    }

    /**
     * `SUM`。
     *
     * 整数用 `Math.addExact` 累加，**溢出时明确报错**而不是回绕。
     * 回绕会给出一个看起来正常、实际完全错误的数字（比如正数变负数），
     * 而使用者拿它做对账——那是最难被发现的一类错误。
     * MySQL 的 `SUM(int)` 返回 `DECIMAL` 因而不会溢出，本系统没有那个类型，
     * 所以选择"报错"这条同样诚实的路。
     */
    private fun sumOf(values: List<Datum>): Datum {
        if (values.isEmpty()) return Datum.Null
        val allInt = values.all { it is Datum.I64 }
        return if (allInt) {
            var acc = 0L
            for (v in values) {
                try {
                    acc = Math.addExact(acc, (v as Datum.I64).v)
                } catch (_: ArithmeticException) {
                    throw SqlException(
                        MySqlErrors.ER_DATA_OUT_OF_RANGE, "22003",
                        "SUM 的结果超出 BIGINT 范围（已累加到 $acc）。" +
                            "本系统没有 DECIMAL 累加类型，溢出时明确报错而不是回绕——" +
                            "回绕会给出一个看起来很正常的错误数字",
                    )
                }
            }
            Datum.I64(acc)
        } else {
            Datum.F64(values.sumOf { numeric(it) })
        }
    }

    /**
     * 取数值。非数值类型（字符串、时间）按 MySQL 的宽松转换处理成 0——但这里宁可报错。
     *
     * `internal` 而不是 private：[Window] 的增量累加器必须用**同一个**数值解释，
     * 两份实现各自演化的话，同一个 `SUM` 在两条路径上会给出不同的值。
     */
    internal fun numeric(d: Datum): Double = when (d) {
        is Datum.I64 -> d.v.toDouble()
        is Datum.F64 -> d.v
        is Datum.Bool -> if (d.v) 1.0 else 0.0
        is Datum.Dec -> d.unscaled.toDouble() / Math.pow(10.0, d.scale.toDouble())
        else -> throw SqlException(
            MySqlErrors.ER_WRONG_ARGUMENTS, "HY000",
            "聚合函数需要数值参数，收到 '$d'（类型 ${d::class.simpleName}）。" +
                "本系统不做隐式字符串→数字转换：那种转换在 MySQL 里会把非数字串静默当 0",
        )
    }

    /**
     * `ONLY_FULL_GROUP_BY` 检查 + 嵌套聚合检查。
     *
     * 只在「这条查询确实是分组查询」时执行：有 `GROUP BY`、或有 `HAVING`、
     * 或任何位置出现聚合。不分组也不聚合的普通查询一行都不检查——那是绝大多数查询，
     * 不该为这个规则付代价。
     */
    fun validate(
        table: TableSchema,
        stmt: SelectStmt,
        /**
         * `SELECT` 列表的别名表。检查前必须先把 `HAVING` / `ORDER BY` 里的别名
         * 解析成原表达式——否则 `HAVING n > 1`（n = `COUNT(*)`）会被当成
         * "选了一个非分组列 n" 而误报 1055。
         */
        aliases: Map<String, Expr> = emptyMap(),
    ) {
        // ★ `WHERE` 里出现聚合要在**这里**就拦住，而且要在下面那个"是不是分组查询"的
        // 提前返回**之前**：`SELECT id FROM t WHERE SUM(v) > 1` 的投影里没有聚合、
        // 也没有 GROUP BY，早期返回会让它一路走到求值阶段——
        // 那时才报错（或者更糟：静默当成 NULL 把每一行都过滤掉）。
        // WHERE 在分组**之前**求值，那时根本没有"一组"，这是用法错误而非数据问题
        if (stmt.where != null && containsAggregate(stmt.where)) {
            throw SqlException(
                MySqlErrors.ER_INVALID_GROUP_FUNC_USE, "HY000",
                "Invalid use of group function: WHERE 里不能用聚合。WHERE 在分组**之前**" +
                    "求值（求值顺序是 FROM → WHERE → GROUP BY → 聚合 → HAVING → SELECT → ORDER BY），" +
                    "按组的条件请写在 HAVING 里",
            )
        }

        val hasAgg = stmt.projections.any { it is SelectItem.Field && containsAggregate(it.expr) } ||
            (stmt.having?.let { containsAggregate(it) } == true) ||
            stmt.orderBy.any { containsAggregate(it.expr) }
        if (stmt.groupBy.isEmpty() && stmt.having == null && !hasAgg) return

        // 聚合不能嵌套：`SUM(COUNT(*))` 没有意义（一组的 COUNT 是单值）
        stmt.projections.forEach { if (it is SelectItem.Field) rejectNested(it.expr) }
        stmt.having?.let { rejectNested(it) }
        stmt.orderBy.forEach { rejectNested(it.expr) }

        // `GROUP BY` 里不能有聚合（1111）
        stmt.groupBy.forEach {
            if (containsAggregate(it)) {
                throw SqlException(
                    MySqlErrors.ER_INVALID_GROUP_FUNC_USE, "HY000",
                    "Invalid use of group function: GROUP BY 里不能出现聚合 $it" +
                        "（分组发生在聚合之前）",
                )
            }
        }

        val keys = stmt.groupBy.map { expressionKey(it) }.toSet()
        val grouped = stmt.groupBy.isNotEmpty()

        fun check(e: Expr?, origin: String) {
            if (e == null) return
            when {
                e is CountCall || e is AggCall -> return      // 聚合本身合法
                e is ColumnRef -> {
                    if (expressionKey(e) in keys) return
                    val code = if (grouped) MySqlErrors.ER_WRONG_FIELD_WITH_GROUP
                    else MySqlErrors.ER_MIX_OF_GROUP_FUNC_AND_FIELDS
                    throw SqlException(
                        code, "42000",
                        if (grouped) {
                            "$origin 里的非聚合列 '${e.name}' 不在 GROUP BY 中。" +
                                "本系统强制 ONLY_FULL_GROUP_BY（sql_mode 里如实回报了它）：" +
                                "否则同一查询在不同时刻可能返回**不同的行**，" +
                                "而 SQL 层看起来一切正常。请把 '${e.name}' 加进 GROUP BY，或用聚合函数包起来"
                        } else {
                            "$origin 里的列 '${e.name}' 没有出现在聚合函数中，" +
                                "但查询里有聚合且没有 GROUP BY。请用聚合函数包起来，或补上 GROUP BY"
                        },
                    )
                }
                else -> childrenOf(e).forEach { check(it, origin) }
            }
        }

        for ((i, item) in stmt.projections.withIndex()) {
            val where = "SELECT 列表第 ${i + 1} 项"
            when (item) {
                is SelectItem.All ->
                    // `SELECT *` 在分组查询里等价于列出所有列，于是每一列都要过检查。
                    // 不展开的话 `SELECT * FROM t GROUP BY city` 会**悄悄**通过，
                    // 然后每组的 name/age 取的是"随便哪一行"的值
                    table.columns.forEach { check(ColumnRef(it.name), where) }
                is SelectItem.Field -> check(item.expr, where)
            }
        }
        check(stmt.having?.let { resolveAliases(it, aliases) }, "HAVING")
        stmt.orderBy.forEach { check(resolveAliases(it.expr, aliases), "ORDER BY") }
    }

    /** 嵌套聚合（`SUM(COUNT(*))`）：MySQL 报 1111。 */
    private fun rejectNested(e: Expr) {
        val inner = when (e) {
            is CountCall -> e.arg
            is AggCall -> e.arg
            else -> null
        }
        if (inner != null && containsAggregate(inner)) {
            throw SqlException(
                MySqlErrors.ER_INVALID_GROUP_FUNC_USE, "HY000",
                "Invalid use of group function: 聚合不能嵌套（$e）。" +
                    "一组的聚合结果是单值，再对它聚合没有意义",
            )
        }
        childrenOf(e).forEach { rejectNested(it) }
    }
}
