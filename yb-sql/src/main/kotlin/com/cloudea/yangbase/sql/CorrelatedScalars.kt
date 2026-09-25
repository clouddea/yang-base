package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema

/**
 * **标量位置的相关子查询**：`SELECT (SELECT … WHERE i.k = o.k) FROM o`。
 *
 * ## 与谓词位置的区别：它给出一个**值**，不是一个"是不是"
 *
 * [CorrelatedSubqueries] 处理的是 `EXISTS` / `IN`——那两种只要"有没有匹配的行"，
 * 于是能改写成半连接/反连接，一行不落地就得到答案。标量位置要的是**值本身**：
 * `SELECT name, (SELECT MAX(amount) FROM orders o WHERE o.user_id = u.id) FROM users u`
 * 里的子查询在每一行上给一个值（可能为 NULL），它还要参与投影、比较、排序。
 *
 * ## 做法：内层按关联键扫**一次**，建一张"键 → 值"的表
 *
 * ```
 *   SELECT (SELECT MAX(amount) FROM orders o WHERE o.user_id = u.id) FROM users u
 *   ⇒ 内层一次扫描 → { 10: 100, 11: 200, 12: NULL }，外层每行按键查表
 * ```
 *
 * 这是"每外层行跑一次子查询"与"无法支持"之间的第三条路，而且它是**精确**的，
 * 不是近似：关联条件是等值时，内层的结果只由键决定，与外层的其它列无关。
 *
 * 于是代价是「内层**一次**全表扫描 + 外层行数次哈希查表」，而不是
 * 「外层行数 × 内层扫一次」——后者在对象存储上会把 IO 打爆（见设计文档 §7.5）。
 *
 * ## 为什么必须有"至多一行"的检查
 *
 * 标量子查询的语义是"**一个**值"。内层按关联键分组后：
 *
 * - 投影里有聚合 → 每组**一行**，天然满足（空组也给值，如 `COUNT(*)` = 0）；
 * - 不然 → 每组必须**恰好一行**；两行就报 1242（`Subquery returns more than 1 row`），
 *   与 MySQL 一致。不检查的话只能"取第一行"，而那是一个**看起来正常**的错答案：
 *   同一个查询换一次扫描顺序就返回另一个值，而使用者不会想到去怀疑它。
 */
object CorrelatedScalars {

    /** 一条待求值的标量相关子查询。 */
    class Spec(
        val node: ScalarSubquery,
        /** 关联等值对：`(外层表达式, 内层表达式)`，顺序即键的顺序。 */
        val keys: List<Pair<Expr, Expr>>,
        /** 内层列的类型（能确定时），用来把外层键转成同一类型再查表。 */
        val keyTypes: List<ColumnType?>,
        val innerTable: TableSchema,
        val innerQualifier: String,
        val inner: SelectStmt,
        /** 内层自己的谓词（不含关联条件）。扫描时按它筛，再看键。 */
        val innerFilter: List<Expr>,
        /** 只引用外层的谓词：外层行不满足时，这个子查询的结果是 NULL（内层一行都不匹配）。 */
        val outerGuards: List<Expr>,
        /** 内层投影里出现的聚合（没有 GROUP BY 时整组一行）。 */
        val aggregates: List<Expr>,
    ) {
        override fun toString() = "标量相关子查询(${keys.joinToString { "${it.first} = ${it.second}" }})"
    }

    /**
     * 从语句里抽出所有标量位置的相关子查询（投影 / WHERE / HAVING / ORDER BY 都找）。
     *
     * [outerQualifiers] 是**外层表的全部可用名字**（表名 + 别名），不是一个名字：
     * `FROM users u` 之后子查询里写 `users.id` 是完全合法的写法，只认 "u" 的话
     * 那个条件会被当成"内层自己的条件"，然后 `id` 退化成**内层表的 id**——
     * 子查询照常返回一批错值，而使用者看不出任何异常。
     */
    fun extract(
        stmt: SelectStmt,
        outerQualifiers: Set<String>,
        resolveInner: (String) -> TableSchema?,
    ): List<Spec> {
        val found = LinkedHashMap<String, Spec>()
        fun walk(e: Expr?) {
            if (e == null) return
            if (e is ScalarSubquery) {
                val spec = toSpec(e, outerQualifiers, resolveInner) ?: return
                found.putIfAbsent(scalarKey(e), spec)
                return
            }
            // 子查询**内部**的表达式不用再找：嵌套的相关标量子查询是另一个量级的
            // 工作（要递归地按行驱动），这一版明确不支持
            if (e is ExistsSubquery || e is InSubquery) return
            childrenOf(e).forEach { walk(it) }
        }
        stmt.projections.forEach { if (it is SelectItem.Field) walk(it.expr) }
        walk(stmt.where)
        walk(stmt.having)
        stmt.orderBy.forEach { walk(it.expr) }
        stmt.groupBy.forEach { walk(it) }
        return found.values.toList()
    }

    private fun toSpec(
        node: ScalarSubquery,
        outerQualifiers: Set<String>,
        resolveInner: (String) -> TableSchema?,
    ): Spec? {
        val sel = node.query as? SelectStmt ?: return null
        val innerName = sel.table ?: return null
        val inner = resolveInner(innerName) ?: return null
        val innerQualifier = (sel.tableAlias ?: innerName.substringAfterLast('.')).lowercase()
        if (sel.joins.isNotEmpty() || sel.groupBy.isNotEmpty() || sel.having != null || sel.asOf != null) {
            throw notSupported(
                "标量相关子查询的内层暂不支持 JOIN / GROUP BY / HAVING / AS OF：$node。" +
                    "内层请用单个表加 WHERE 条件（需要分组请把聚合写进投影）",
            )
        }
        val innerWhere = sel.where ?: return null
        // 内层作用域里的名字：表名与别名都认（写别名时 MySQL 只认别名，宽松一点没有坏处）
        val innerQualifiers = setOf(innerQualifier, innerName.lowercase(), innerName.substringAfterLast('.').lowercase())

        val keys = ArrayList<Pair<Expr, Expr>>()
        val keyTypes = ArrayList<ColumnType?>()
        val innerFilter = ArrayList<Expr>()
        val outerGuards = ArrayList<Expr>()
        for (c in conjunctsOf(innerWhere)) {
            checkScope(c, innerQualifiers, outerQualifiers, node)
            val refsOuter = refsOuter(c, outerQualifiers)
            if (!refsOuter) {
                innerFilter.add(c)
                continue
            }
            val eq = c as? BinaryOp
            if (eq == null || eq.op != "=") {
                throw notSupported(
                    "标量相关子查询的关联条件必须是等值（`内层列 = 外层.列`），" +
                        "实际是 $c。非等值关联只能按行驱动子查询，代价是外层行数 × 内层扫描，故明确拒绝",
                )
            }
            val lOuter = refsOuter(eq.left, outerQualifiers)
            val rOuter = refsOuter(eq.right, outerQualifiers)
            when {
                lOuter && !rOuter -> { keys.add(eq.left to eq.right); keyTypes.add(columnTypeOf(eq.right, inner)) }
                rOuter && !lOuter -> { keys.add(eq.right to eq.left); keyTypes.add(columnTypeOf(eq.left, inner)) }
                // 两边都引用外层：既不是关联条件，也没法在内层求值
                else -> outerGuards.add(c)
            }
        }
        if (keys.isEmpty()) {
            // 相关、但没有等值关联键 → 内层的结果不由任何键决定，只能按行驱动
            throw notSupported(
                "标量相关子查询里没有找到等值关联条件（`内层列 = 外层.列`）：$node。" +
                    "本系统把内层按关联键建成一张表再查，没有键就没有可查的东西",
            )
        }
        val projection = (sel.projections.singleOrNull() as? SelectItem.Field)?.expr
            ?: throw SqlException(
                MySqlErrors.ER_SUBQUERY_NO_1_ROW, "21000",
                "标量子查询只能返回一列（$node）",
            )
        if (containsWindow(projection)) {
            throw notSupported("标量相关子查询的投影里暂不支持窗口函数：$node")
        }
        if (containsSubquery(projection)) {
            throw notSupported("标量相关子查询的投影里暂不支持嵌套子查询：$node")
        }
        val aggregates = ArrayList<Expr>()
        fun collectAgg(e: Expr?) {
            if (e == null) return
            if (e is CountCall || e is AggCall) { aggregates.add(e); return }
            childrenOf(e).forEach { collectAgg(it) }
        }
        collectAgg(projection)
        return Spec(
            node = node,
            keys = keys,
            keyTypes = keyTypes,
            innerTable = inner,
            innerQualifier = innerQualifier,
            inner = sel,
            innerFilter = innerFilter,
            outerGuards = outerGuards,
            aggregates = aggregates,
        )
    }

    /**
     * 逐外层行算出子查询的值。
     *
     * @param outerRows 外层行（已按访问路径取好）
     * @param scanInner 取内层表的全部行；由执行器注入（它才知道怎么读、读哪个时间戳）
     * @return 与外层行等长；第 i 项是"第 i 行上这些子查询的值"，键是 [scalarKey]
     */
    suspend fun evaluate(
        specs: List<Spec>,
        outerRows: List<List<Datum>>,
        outerSchema: TableSchema,
        vars: Map<String, Datum>,
        scanInner: suspend (TableSchema) -> List<List<Datum>>,
    ): List<Map<String, Datum>> {
        val out = List(outerRows.size) { HashMap<String, Datum>(maxOf(1, specs.size)) }
        for (spec in specs) {
            val key = scalarKey(spec.node)
            val innerRows = scanInner(spec.innerTable)
            // 先按内层自己的谓词筛（不涉及外层），再按键分组——
            // 顺序反了会把"内层不满足自身条件的行"也算进键里
            val filtered = innerRows.filter { row ->
                val ctx = EvalContext(spec.innerTable, row, vars)
                spec.innerFilter.all { Eval.truthy(Eval.eval(it, ctx)) }
            }
            // 键 → 该组的行。用 LinkedHashMap 保持稳定顺序，便于定位"多行"的那一组
            val groups = LinkedHashMap<List<Datum>, MutableList<List<Datum>>>()
            for (row in filtered) {
                val ctx = EvalContext(spec.innerTable, row, vars)
                val k = spec.keys.map { Eval.eval(it.second, ctx) }
                groups.getOrPut(k) { ArrayList() }.add(row)
            }
            val table = HashMap<List<Datum>, Datum>(groups.size * 2)
            for ((k, rows) in groups) table[k] = valueOf(spec, rows, vars)

            for (i in outerRows.indices) {
                val ctx = EvalContext(outerSchema, outerRows[i], vars)
                if (spec.outerGuards.any { !Eval.truthy(Eval.eval(it, ctx)) }) {
                    // 只引用外层的条件为假 ⇒ 内层**一行都不匹配**，而不是"子查询失败"。
                    // 这两者在有聚合时结果不同：`(SELECT COUNT(*) … AND o.flag = 1)`
                    // 在 flag ≠ 1 时是 **0**，不是 NULL——把它当 NULL 会给出一个
                    // 看起来正常、却与 MySQL 不同的值（NULL 与 0 在求和/计数里差别很大）
                    out[i][key] = emptyGroupValue(spec, vars, outerSchema, outerRows[i])
                    continue
                }
                val k = spec.keys.mapIndexed { n, (outerExpr, _) ->
                    // 按**内层列的类型**解释外层的键值：不转换的话，
                    // `VARCHAR` 与 `INT` 这类跨类型比较会永远查不到（键对不上），
                    // 于是每行都静默返回 NULL
                    val v = Eval.eval(outerExpr, ctx)
                    spec.keyTypes[n]?.let { v.coerceTo(it) } ?: v
                }
                out[i][key] = table[k] ?: emptyGroupValue(spec, vars, outerSchema, outerRows[i])
            }
        }
        return out
    }

    /**
     * 一个键对应的值。
     *
     * 有聚合时：整组归约成一行（**空组也要出值**——`COUNT(*)` 给 0、`SUM` 给 NULL，
     * 这与不带 `GROUP BY` 的聚合语义一致，也是"查不到就 NULL"这条直觉的例外）。
     * 没有聚合时：按 `DISTINCT`/`ORDER BY`/`LIMIT` 整理之后必须只剩一行。
     */
    private fun valueOf(spec: Spec, rows: List<List<Datum>>, vars: Map<String, Datum>): Datum {
        if (spec.aggregates.isNotEmpty()) {
            val projection = (spec.inner.projections.single() as SelectItem.Field).expr
            val computed = spec.aggregates.associate { aggregateKey(it) to Aggregates.compute(it, rows, spec.innerTable, vars) }
            return Eval.eval(projection, EvalContext(spec.innerTable, rows.firstOrNull(), vars, computed))
        }
        var picked = rows
        if (spec.inner.distinct) {
            // `SELECT DISTINCT x` 下两行同值只算一行——这正是它比"两行报错"更宽松的地方
            val projection = (spec.inner.projections.single() as SelectItem.Field).expr
            val seen = HashSet<Datum>()
            picked = rows.filter { row ->
                seen.add(Eval.eval(projection, EvalContext(spec.innerTable, row, vars)))
            }
        }
        if (spec.inner.orderBy.isNotEmpty()) {
            picked = picked.sortedWith { a, b ->
                for (oi in spec.inner.orderBy) {
                    val av = Eval.eval(oi.expr, EvalContext(spec.innerTable, a, vars))
                    val bv = Eval.eval(oi.expr, EvalContext(spec.innerTable, b, vars))
                    val c = av.compareTo(bv)
                    if (c != 0) return@sortedWith if (oi.ascending) c else -c
                }
                0
            }
        }
        val offset = spec.inner.offset ?: 0
        val limit = spec.inner.limit
        if (offset > 0) picked = picked.drop(offset)
        if (limit != null) picked = picked.take(limit)
        if (picked.size > 1) {
            throw SqlException(
                MySqlErrors.ER_SUBQUERY_NO_1_ROW, "21000",
                "Subquery returns more than 1 row：标量相关子查询按关联键" +
                    "（${spec.keys.joinToString { "${it.second}" }}）分组后有 ${picked.size} 行，" +
                    "而标量位置只能有一个值。请加上聚合（`MAX` / `SUM` …）或 `LIMIT 1`——" +
                    "本系统不做「取第一行」，那会给出一个看起来正常、却取决于扫描顺序的答案",
            )
        }
        val projection = (spec.inner.projections.single() as SelectItem.Field).expr
        val row = picked.firstOrNull() ?: return Datum.Null
        return Eval.eval(projection, EvalContext(spec.innerTable, row, vars))
    }

    /** 键在内层一行都没有：有聚合时仍要算一次"空集上的聚合"，否则是 NULL。 */
    private fun emptyGroupValue(
        spec: Spec,
        vars: Map<String, Datum>,
        outerSchema: TableSchema,
        outerRow: List<Datum>,
    ): Datum {
        if (spec.aggregates.isEmpty()) return Datum.Null
        val projection = (spec.inner.projections.single() as SelectItem.Field).expr
        val computed = spec.aggregates.associate {
            aggregateKey(it) to Aggregates.compute(it, emptyList(), spec.innerTable, vars)
        }
        // 空组没有内层行可引用：投影里的裸列没有值可取（`SELECT x` 配 `COUNT(*)` 这种写法
        // 在空组上本来就无意义）。给一行 NULL 让它求值，聚合本身照常出值
        val placeholders = List(spec.innerTable.columns.size) { Datum.Null }
        return Eval.eval(projection, EvalContext(spec.innerTable, placeholders, vars, computed))
    }

    // ── 小工具 ────────────────────────────────────────────────────

    private fun conjunctsOf(e: Expr): List<Expr> = when {
        e is BinaryOp && e.op.uppercase() == "AND" -> conjunctsOf(e.left) + conjunctsOf(e.right)
        else -> listOf(e)
    }

    private fun refsOuter(e: Expr, outerQualifiers: Set<String>): Boolean {
        if (e is ColumnRef) return e.table?.lowercase() in outerQualifiers
        // 子查询内部的引用不算：那是它自己的作用域
        if (e is ScalarSubquery || e is ExistsSubquery || e is InSubquery) return false
        return childrenOf(e).any { refsOuter(it, outerQualifiers) }
    }

    /**
     * 限定名必须落在**内层或外层**的作用域里。
     *
     * 少了这一步，一个写错的限定名（`FROM users u` 却写 `user.id`）会退化成
     * "内层的裸列 id"——`Eval` 找不到限定名时会退回裸名，于是子查询照常执行，
     * 返回一批与它看起来无关的值。这就是 [com.cloudea.yangbase.sql.Executor]
     * 里 `validateQualifiers` 存在的同一个理由，只是子查询这一层它管不到。
     */
    private fun checkScope(e: Expr, innerQualifiers: Set<String>, outerQualifiers: Set<String>, node: ScalarSubquery) {
        if (e is ColumnRef && e.table != null) {
            val q = e.table.lowercase()
            if (q !in innerQualifiers && q !in outerQualifiers) {
                throw SqlException(
                    MySqlErrors.ER_BAD_TABLE_ERROR, "42S02",
                    "Unknown table '$q'（在标量相关子查询 $node 里）。" +
                        "子查询里只能引用内层表（${innerQualifiers.joinToString("/")}）" +
                        "与外层表（${outerQualifiers.joinToString("/")}）",
                )
            }
            return
        }
        if (e is ScalarSubquery || e is ExistsSubquery || e is InSubquery) return
        childrenOf(e).forEach { checkScope(it, innerQualifiers, outerQualifiers, node) }
    }

    private fun containsSubquery(e: Expr): Boolean = when (e) {
        is ScalarSubquery, is ExistsSubquery, is InSubquery -> true
        else -> childrenOf(e).any { containsSubquery(it) }
    }

    fun containsWindow(e: Expr): Boolean = when (e) {
        is WindowCall -> true
        else -> childrenOf(e).any { containsWindow(it) }
    }

    private fun columnTypeOf(e: Expr, schema: TableSchema): ColumnType? {
        val name = (e as? ColumnRef)?.name ?: return null
        val i = schema.indexOfOrNull(name) ?: return null
        return schema.columns[i].type
    }

    private fun notSupported(msg: String) =
        SqlException(MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", msg)
}

/** 一个标量相关子查询的值在"预计算值表"里的键（按 SQL 原文，同一个子查询只算一次）。 */
fun scalarKey(e: ScalarSubquery): String = "SUBQUERY:" + e.text

/** 这个表达式里有没有标量相关子查询（执行器用它决定要不要多走一遍预计算）。 */
fun containsScalarSubquery(e: Expr?): Boolean = when (e) {
    null -> false
    is ScalarSubquery -> true
    is ExistsSubquery, is InSubquery -> false
    else -> childrenOf(e).any { containsScalarSubquery(it) }
}
