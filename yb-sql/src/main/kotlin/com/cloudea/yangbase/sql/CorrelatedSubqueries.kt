package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema

/**
 * **相关子查询**的求值：改写成半连接 / 反连接。
 *
 * ## 相关子查询为什么不能"提前算一次"
 *
 * `WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)` 里的 `u.id`
 * 引用了**外层的当前行**，所以子查询的结果**逐行不同**。[Subqueries] 那套
 * 「取数前折算成字面量」的做法在这里根本不成立。
 *
 * ## 解法：它不是"每行跑一次子查询"，而是一次连接
 *
 * ```
 *   WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
 *   ≡  users SEMI JOIN orders ON o.user_id = u.id
 * ```
 *
 * 这不是近似或妥协，而是**同一个集合的另一种写法**——关系代数里 `EXISTS`
 * 的定义就是投影后的半连接。于是只需**一次**内表扫描（不是每行一次），
 * 等值关联直接建哈希表；外层的投影/排序/分组仍然跑在**外层表自己的 schema** 上
 * （半连接不增加列，`SELECT u.*` 不会多出 `o.` 的列）。
 *
 * ## 为什么不逐行驱动
 *
 * 「外层每行执行一次子查询」实现起来更简单、覆盖的形状也更全，但代价是
 * `外层行数 × 一次内表扫描`：一千行的外层表就是一千次内表扫描。
 * 这里宁可对不能改写的形状**明确报错**，也不给一个能跑但会打爆 IO 的实现——
 * 使用者会以为"慢"是数据量的问题，而真正的原因是计划形状。
 *
 * | 子查询 | 重写成 |
 * |---|---|
 * | `EXISTS (…)` / `IN (…)` | **半连接**：有一行匹配就输出外层行**一次** |
 * | `NOT EXISTS (…)` | **反连接**（`AntiSemantics.EXISTS`） |
 * | `NOT IN (…)` | **反连接**（`AntiSemantics.NOT_IN`，**NULL 语义与 NOT EXISTS 不同**） |
 *
 * 关联条件必须是**等值**（多个等值用 `AND` 连起来即复合键）。
 * 非等值关联（`b.x > a.x`）会报错并说明原因——那只能靠双层循环。
 */
object CorrelatedSubqueries {

    /** 一条待重写的相关子查询谓词。 */
    class Spec(
        /** 它在 `WHERE` 里的原始节点，用于从剩余谓词里摘掉它。 */
        val node: Expr,
        val innerTable: TableSchema,
        val innerQualifier: String,
        /** 内层自己的 `WHERE`（已去掉关联条件，且列引用已按内层限定名补全）。 */
        val innerWhere: Expr?,
        /** 关联条件：`(外层限定列, 内层列名)`。 */
        val correlations: List<Pair<ColumnRef, String>>,
        val negated: Boolean,
        /**
         * `IN` / `NOT IN` 的"被比较"那一对列：`(内层列, 外层列)`。
         *
         * `EXISTS` 没有它——`EXISTS` 只问"有没有匹配的行"，而 `IN` 还要求
         * **被比较的值相等**：`u.name IN (SELECT o.tag …)` 与
         * `EXISTS (… WHERE o.tag = u.name)` 才是同一件事。
         * 漏掉这一条会把它当成 `EXISTS`：**匹配条件变松了**，
         * 于是返回一批本不该匹配的行——不报错，只是行数多了。
         *
         * 两侧**都必须是普通的列引用**。它们会被接到连接的 `ON` 上，
         * 而反连接的 `NOT IN` NULL 规则要靠"哪一对等值键是被比较的那一对"来定位：
         * 表达式的写法虽然也能参与匹配，却让定位失准——NULL 规则会作用在关联列上，
         * 于是结果多几行或少几行，而查询照常返回。
         */
        val compared: Pair<ColumnRef, ColumnRef>? = null,
    )

    /** 从 `WHERE` 的合取项里抽出所有相关子查询谓词。 */
    fun extract(
        where: Expr?,
        outerQualifier: String,
        resolveInner: (String) -> TableSchema?,
    ): List<Spec> {
        if (where == null) return emptyList()
        val out = ArrayList<Spec>()
        for (c in conjunctsOf(where)) {
            toSpec(c, outerQualifier, resolveInner)?.let { out.add(it) }
        }
        return out
    }

    private fun conjunctsOf(e: Expr): List<Expr> = when {
        e is BinaryOp && e.op == "AND" -> conjunctsOf(e.left) + conjunctsOf(e.right)
        else -> listOf(e)
    }

    private fun toSpec(
        rawNode: Expr,
        outerQualifier: String,
        resolveInner: (String) -> TableSchema?,
    ): Spec? {
        // `NOT EXISTS (…)` 会被解析成 `UnaryOp(NOT, ExistsSubquery)`——
        // 因为 `NOT` 在语法上是**前缀一元运算符**，而 `x NOT IN (…)` 里的 NOT 是
        // 比较级的后缀写法。两种形态都要认，否则 `NOT EXISTS` 会掉进
        // "子查询没能被提前求值"那条路（一个看起来像执行器缺陷的错）
        val node = rawNode
        val query: Statement
        val negated: Boolean
        when {
            rawNode is ExistsSubquery -> { query = rawNode.query; negated = rawNode.negated }
            rawNode is InSubquery -> { query = rawNode.query; negated = rawNode.negated }
            rawNode is UnaryOp && rawNode.op == "NOT" -> when (val inner = rawNode.expr) {
                is ExistsSubquery -> { query = inner.query; negated = !inner.negated }
                is InSubquery -> { query = inner.query; negated = !inner.negated }
                else -> return null
            }
            else -> return null
        }
        val sel = query as? SelectStmt ?: return null
        // 带聚合/分组/连接的子查询不在这一版覆盖里：它们的"匹配"语义要复杂得多
        if (sel.projectionHasAggregate() || sel.groupBy.isNotEmpty() || sel.having != null) return null
        if (sel.joins.isNotEmpty() || sel.asOf != null || sel.orderBy.isNotEmpty()) return null
        val innerName = sel.table ?: return null
        val inner = resolveInner(innerName) ?: return null
        val innerQualifier = (sel.tableAlias ?: innerName.substringAfterLast('.')).lowercase()

        val correlations = ArrayList<Pair<ColumnRef, String>>()
        val rest = ArrayList<Expr>()
        for (c in conjunctsOf(sel.where ?: return null)) {
            val pair = asCorrelation(c, outerQualifier, innerQualifier, inner)
            if (pair != null) correlations.add(pair) else rest.add(c)
        }
        if (correlations.isEmpty()) return null    // 不是相关子查询：交给 Subqueries 那条路

        // `IN (SELECT p …)` 还要加上 `p = 左侧表达式` 这一条等值。
        // 子查询必须只投影一列（多列的话 IN 的语义本来就不成立）
        val compared = if (node is InSubquery || (node is UnaryOp && (node.expr as? InSubquery) != null)) {
            val inNode = if (node is InSubquery) node else (node as UnaryOp).expr as InSubquery
            val outerExpr = inNode.expr
            val innerExpr = (sel.projections.singleOrNull() as? SelectItem.Field)?.expr
                ?: throw SqlException(
                    MySqlErrors.ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, "21000",
                    "IN 的子查询只能返回一列",
                )
            asComparedColumns(outerExpr, innerExpr, outerQualifier, innerQualifier)
        } else {
            null
        }
        return Spec(
            node = node,
            innerTable = inner,
            innerQualifier = innerQualifier,
            compared = compared,
            // 内层自己的条件里的列引用要补上内层限定名：连接 schema 里两张表的
            // 同名列（典型是 `id`）**故意不登记裸名**，不补的话会解析不到
            innerWhere = rest.reduceOrNull { a, b -> BinaryOp("AND", a, b) }
                ?.let { qualify(it, innerQualifier, inner) },
            correlations = correlations,
            negated = negated,
        )
    }

    /**
     * `外层限定列 = 内层列` 这种等值。返回 `(外层列, 内层列名)`。
     *
     * ⚠️ 内层那一侧**可以带限定名**，而且现实里通常就是带的：
     * `o.user_id = u.id` 才是最自然的写法。只认"内层不带限定名"的话，
     * 满大街的相关子查询都会掉进"形状不支持"那条路——而使用者写的是最标准的 SQL。
     * 所以两种写法都要认：内层带自己的限定名、或者不带（且能在内层里找到这一列）。
     */
    private fun asCorrelation(
        c: Expr,
        outerQualifier: String,
        innerQualifier: String,
        inner: TableSchema,
    ): Pair<ColumnRef, String>? {
        val eq = c as? BinaryOp ?: return null
        if (eq.op != "=") return null
        val l = eq.left as? ColumnRef ?: return null
        val r = eq.right as? ColumnRef ?: return null

        fun isOuter(c: ColumnRef) = c.table.equals(outerQualifier, true)
        fun isInner(c: ColumnRef) = when {
            c.table == null -> inner.has(c.name)
            else -> c.table.equals(innerQualifier, true) && inner.has(c.name)
        }
        // 返回 `(外层列, 内层列名)`
        return when {
            isOuter(r) && isInner(l) -> r to l.name
            isOuter(l) && isInner(r) -> l to r.name
            else -> null
        }
    }

    private fun TableSchema.has(col: String) = indexOfOrNull(col) != null

    /**
     * 把 `IN` 左侧的表达式渲染成一段 **SQL 文本**，稍后由 `Parser` 重新解析。
     *
     * 为什么绕一圈用文本：`IN` 左侧可以是任意表达式（`u.a + 1`、函数调用…），
     * 而我要把它接到连接的 `ON` 上——那里需要一个 `Expr`。用文本往返一次
     * 复用现成的解析器，比手写一个"表达式 → 连接 schema"的改写器可靠得多
     * （那种改写器要为每种节点都写一份，而且还得把列引用按外层表补限定名）。
     */
    /**
     * `IN` 两侧必须都是普通的列引用（见 [Spec.compared]）。
     *
     * 限定名一律补全：连接 schema 里两张表的同名列**故意不登记裸名**，
     * 不补的话 `ON` 里的裸名要么解析不到（报一个与真正原因无关的错），
     * 要么解析到**另一张表**的同名列上——那样匹配条件就悄悄换了一个意思。
     */
    private fun asComparedColumns(
        outerExpr: Expr,
        innerExpr: Expr,
        outerQualifier: String,
        innerQualifier: String,
    ): Pair<ColumnRef, ColumnRef> {
        val outer = outerExpr as? ColumnRef
        val inner = innerExpr as? ColumnRef
        if (outer == null || inner == null) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                "相关子查询的 IN / NOT IN 两侧必须是普通的列（" +
                    "左侧是 ${outerExpr::class.simpleName}、右侧是 ${innerExpr::class.simpleName}）。" +
                    "`NOT IN` 的 NULL 规则需要精确定位「被比较的是哪一列」——" +
                    "表达式写法会让它错位，于是结果多几行或少几行且不报错。" +
                    "可以先把表达式算成一列（用别名或视图）再比较",
            )
        }
        return ColumnRef(inner.name, inner.table ?: innerQualifier) to
            ColumnRef(outer.name, outer.table ?: outerQualifier)
    }

    private fun SelectStmt.projectionHasAggregate(): Boolean =
        projections.any { it is SelectItem.Field && containsAggregate(it.expr) }

    /**
     * 把表达式里**未限定**的列引用补上 [qualifier]。
     *
     * 连接 schema 里两张表都有的列名（`id` 是最典型的）**故意不登记裸名**，
     * 于是未限定的引用会解析失败。补上限定名之后既能解析、又不会取错一边。
     */
    fun qualify(e: Expr, qualifier: String, schema: TableSchema): Expr = when {
        e is ColumnRef && e.table == null && schema.indexOfOrNull(e.name) != null ->
            ColumnRef(e.name, qualifier)
        e is BinaryOp -> BinaryOp(e.op, qualify(e.left, qualifier, schema), qualify(e.right, qualifier, schema))
        e is UnaryOp -> UnaryOp(e.op, qualify(e.expr, qualifier, schema))
        e is InList -> InList(
            qualify(e.expr, qualifier, schema), e.values.map { qualify(it, qualifier, schema) }, e.negated,
        )
        e is Between -> Between(
            qualify(e.expr, qualifier, schema), qualify(e.low, qualifier, schema),
            qualify(e.high, qualifier, schema), e.negated,
        )
        e is IsNull -> IsNull(qualify(e.expr, qualifier, schema), e.negated)
        e is Like -> Like(qualify(e.expr, qualifier, schema), qualify(e.pattern, qualifier, schema), e.negated)
        e is FuncCall -> FuncCall(e.name, e.args.map { qualify(it, qualifier, schema) }, e.distinct)
        else -> e
    }

    /**
     * 把一条 [Spec] 变成连接子句。
     *
     * 关联条件与内层自己的条件都写进 `ON`——对半/反连接来说它们是同一件事：
     * `ON` 决定「什么算匹配」。把内层条件放进外层 `WHERE` 就错了：
     * 那变成"匹配之后再过滤"，反连接的结果会完全不同。
     */
    fun toJoinClause(spec: Spec): JoinClause {
        // 被比较值相等这一条放在**最后**：JoinEngine 的 `NOT IN` NULL 规则约定
        // "等值键里最后一个是被比较值那一对"（`Join.kt` 里 `keys.last()`），
        // 顺序反了规则就会作用在关联列上。两侧都是列引用这一点由
        // [asComparedColumns] 保证——表达式会被 `equiKeys` 丢掉，位置也就不对了
        var on: Expr = BinaryOp(
            "=",
            ColumnRef(spec.correlations[0].second, spec.innerQualifier),
            ColumnRef(spec.correlations[0].first.name, spec.correlations[0].first.table),
        )
        for ((outerCol, innerCol) in spec.correlations.drop(1)) {
            on = BinaryOp(
                "AND", on,
                BinaryOp("=", ColumnRef(innerCol, spec.innerQualifier), ColumnRef(outerCol.name, outerCol.table)),
            )
        }
        spec.compared?.let { (innerCol, outerCol) ->
            on = BinaryOp("AND", on, BinaryOp("=", innerCol, outerCol))
        }
        spec.innerWhere?.let { on = BinaryOp("AND", on, it) }
        return JoinClause(
            table = spec.innerTable.name,
            alias = spec.innerQualifier,
            type = if (spec.negated) JoinType.ANTI else JoinType.SEMI,
            on = on,
        )
    }

    /** `NOT IN` 与 `NOT EXISTS` 的反连接 NULL 语义不同（见 [AntiSemantics]）。 */
    fun antiSemanticsOf(node: Expr): AntiSemantics {
        // `NOT (x IN …)` 会被解析成 `UnaryOp(NOT, InSubquery)`，而 `x NOT IN …`
        // 是 `InSubquery(negated = true)`——两种写法要认成同一件事。
        // 认漏了的话 NULL 规则会按 `NOT EXISTS` 走，`NOT IN` 于是多返回一批行
        val inner = if (node is UnaryOp && node.op == "NOT") node.expr else node
        return if (inner is InSubquery) AntiSemantics.NOT_IN else AntiSemantics.EXISTS
    }

    /**
     * 把相关子查询谓词从 `WHERE` 里摘掉（它们已经变成连接了）。
     *
     * 不摘的话 `finishSelect` 还会去求值那个节点，而它在逐行上下文里求不出来，
     * 会抛「子查询没能被提前求值」。只摘**整个合取项**，不动同层 `AND` 里的其它条件。
     */
    fun removeSpecs(where: Expr?, specs: List<Spec>): Expr? {
        if (where == null || specs.isEmpty()) return where
        val toRemove = specs.map { it.node }
        fun strip(e: Expr): Expr? = when {
            toRemove.any { it === e } -> null
            e is BinaryOp && e.op == "AND" -> when (val l = strip(e.left)) {
                null -> strip(e.right)
                else -> when (val r = strip(e.right)) {
                    null -> l
                    else -> BinaryOp("AND", l, r)
                }
            }
            else -> e
        }
        return strip(where)
    }

    /**
     * 子查询是相关的、但**关联条件不是等值**时，明确报错。
     *
     * 报错而不是退化成双层循环：那种计划的代价是"外层行数 × 内层行数"，
     * 在对象存储上会把 IO 打爆，而使用者只会看到"查询很慢"。
     */
    fun rejectNonEqui(where: Expr?, outerQualifier: String, resolveInner: (String) -> TableSchema?) {
        for (c in conjunctsOf(where ?: return)) {
            val sub = when {
                c is ExistsSubquery -> c.query
                c is InSubquery -> c.query
                c is UnaryOp && c.op == "NOT" -> when (val i = c.expr) {
                    is ExistsSubquery -> i.query
                    is InSubquery -> i.query
                    else -> null
                }
                else -> null
            } ?: continue
            val sel = sub as? SelectStmt ?: continue
            if (sel.table == null || sel.where == null) continue
            if (resolveInner(sel.table) == null) continue
            // 子查询里引用了外层、但没有任何等值关联 → 只能双层循环
            val refsOuter = containsOuterRef(sel.where, outerQualifier)
            if (!refsOuter) continue
            val inner = resolveInner(sel.table) ?: continue
            val innerQualifier = (sel.tableAlias ?: sel.table.substringAfterLast('.')).lowercase()
            val hasEqui = conjunctsOf(sel.where)
                .any { asCorrelation(it, outerQualifier, innerQualifier, inner) != null }
            if (!hasEqui) {
                throw SqlException(
                    MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                    "暂不支持**非等值**的相关子查询（$c）：本系统把相关子查询改写成哈希连接，" +
                        "而这要求关联条件是等值（`内层列 = 外层列`）。" +
                        "非等值关联只能靠双层循环，代价是外层行数 × 内层行数——" +
                        "那会把对象存储的 IO 打爆，所以这里明确报错而不是给一个能跑的计划",
                )
            }
        }
    }

    private fun containsOuterRef(e: Expr, outerQualifier: String): Boolean {
        if (e is ColumnRef) return e.table.equals(outerQualifier, true)
        return childrenOf(e).any { containsOuterRef(it, outerQualifier) }
    }
}
