package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException

/**
 * 子查询的**提前求值**（非相关子查询）。
 *
 * ## 为什么是"改写"而不是"在求值器里支持子查询"
 *
 * 子查询的结果是**一整个集合**，它与当前行无关（非相关子查询），
 * 而逐行求值器（[Eval]）看到的世界只有一行。把子查询塞进求值器意味着
 * 给每个 `eval` 调用都可能挂上一次对象存储访问——求值器会从纯函数变成
 * 带 IO 的东西，而它被用在排序、投影、谓词三处的内层循环里。
 *
 * 所以这里在**取数之前**把子查询算完，并把表达式树改写掉：
 *
 * ```
 *   id IN (SELECT user_id FROM orders)      →  id IN (1, 2, 3)
 *   id = (SELECT MAX(id) FROM t)            →  id = 7
 *   EXISTS (SELECT 1 FROM t WHERE ...)      →  true
 * ```
 *
 * 改写之后逐行求值器看到的只是普通的 `InList` 与 `Literal`，
 * **一行代码都不用改**，而且"子查询只执行一次"这件事是结构上保证的，
 * 不依赖谁记得去缓存它。
 *
 * ## 只支持**非相关**子查询，相关子查询明确报错
 *
 * `WHERE EXISTS (SELECT 1 FROM b WHERE b.x = a.x)` 里的 `a.x` 引用了外层行，
 * 它的结果**逐行不同**，上面那套"求值一次"的做法根本不成立。
 * 实现它需要按行驱动子查询（或者改写成一趟连接），那是另一个量级的工作。
 *
 * 这里的选择是**明确报错**：相关子查询在本系统里会报一个"暂不支持"，
 * 而不是静默按非相关的方式算出一个错误的结果——后者会返回错误的行集，
 * 而使用者完全看不出来。
 */
object Subqueries {

    /**
     * 把语句里所有子查询折算成字面量。
     *
     * [run] 用来执行一个子查询并拿到结果集——由执行器注入，
     * 因为"怎么执行一条 SELECT"是执行器的事（要读时间戳、要走 session）。
     * 这样这个文件不依赖 `Executor`，两个方向的依赖都不会出现。
     */
    suspend fun rewrite(stmt: Statement, run: suspend (Statement) -> QueryRows): Statement =
        when (stmt) {
            is SelectStmt -> stmt.copy(
                projections = stmt.projections.map { item ->
                    if (item is SelectItem.Field) item.copy(expr = rewriteExpr(item.expr, run))
                    else item
                },
                where = stmt.where?.let { rewriteExpr(it, run) },
                joins = stmt.joins.map { it.copy(on = it.on?.let { e -> rewriteExpr(e, run) }) },
                groupBy = stmt.groupBy.map { rewriteExpr(it, run) },
                having = stmt.having?.let { rewriteExpr(it, run) },
                orderBy = stmt.orderBy.map { it.copy(expr = rewriteExpr(it.expr, run)) },
            )
            is UnionStmt -> stmt.copy(
                left = rewrite(stmt.left, run),
                right = rewrite(stmt.right, run),
            )
            is UpdateStmt -> stmt.copy(
                assignments = stmt.assignments.map { (c, e) -> c to rewriteExpr(e, run) },
                where = stmt.where?.let { rewriteExpr(it, run) },
            )
            is DeleteStmt -> stmt.copy(where = stmt.where?.let { rewriteExpr(it, run) })
            is InsertStmt -> stmt.copy(
                rows = stmt.rows.map { r -> r.map { rewriteExpr(it, run) } },
            )
            else -> stmt
        }

    /** 子查询的执行结果：列名 + 行。 */
    class QueryRows(val columns: List<String>, val rows: List<List<Datum>>)

    /**
     * 子查询是否**引用了外层**的列（即相关子查询）。
     *
     * 判断不需要元数据：子查询自己的表名/别名是已知的，凡是用了一个**别的**限定名
     * 的列引用，就一定是外层的（或写错了）。
     *
     * 为什么要在这里判：这一层是"把子查询算成字面量"，而相关子查询**算不了**
     * （结果逐行不同）。不判的话它会先被执行一次、报出一个"Unknown table"，
     * 然后在 [executor] 里被翻译成"相关子查询不支持"——那条路能走通，
     * 但它发生在**半连接改写之前**，于是改写的路径永远轮不到。
     */
    private fun looksCorrelated(query: Statement): Boolean {
        val sel = query as? SelectStmt ?: return false
        val own = setOfNotNull(
            sel.tableAlias?.lowercase(),
            sel.table?.substringAfterLast('.')?.lowercase(),
            sel.table?.lowercase(),
        )
        var found = false
        fun walk(e: Expr?) {
            if (e == null || found) return
            if (e is ColumnRef) {
                if (e.table != null && e.table.lowercase() !in own) found = true
                return
            }
            childrenOf(e).forEach { walk(it) }
        }
        sel.where?.let { walk(it) }
        sel.projections.forEach { if (it is SelectItem.Field) walk(it.expr) }
        return found
    }

    private suspend fun rewriteExpr(e: Expr, run: suspend (Statement) -> QueryRows): Expr {
        // 先看自己是不是子查询，再递归到子节点
        return when (e) {
            is ScalarSubquery -> {
                if (looksCorrelated(e.query)) return e     // 交给外层（见 looksCorrelated）
                val rs = executor(e.query, run, e)
                if (rs.rows.size > 1) {
                    throw SqlException(
                        MySqlErrors.ER_SUBQUERY_NO_1_ROW, "21000",
                        "Subquery returns more than 1 row（标量子查询至多返回一行，实际 ${rs.rows.size} 行）",
                    )
                }
                val v = rs.rows.firstOrNull()?.firstOrNull() ?: Datum.Null
                // 标量子查询的结果必须是**单个值**：多列在解析阶段就该被拦住，
                // 这里再挡一道，因为"取第一列"会让一个写错的查询静默得到部分结果
                if (rs.columns.size > 1) {
                    throw SqlException(
                        MySqlErrors.ER_SUBQUERY_NO_1_ROW, "21000",
                        "标量子查询只能返回一列，实际返回 ${rs.columns.size} 列",
                    )
                }
                Literal(v)
            }
            is InSubquery -> {
                if (looksCorrelated(e.query)) return e     // 由 CorrelatedSubqueries 改写成半连接
                val rs = executor(e.query, run, e)
                if (rs.columns.size != 1) {
                    throw SqlException(
                        MySqlErrors.ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, "21000",
                        "IN 的子查询只能返回一列，实际返回 ${rs.columns.size} 列",
                    )
                }
                // 折成普通的 IN 列表：三值逻辑（含 NULL 时 NOT IN 为未知）
                // 由 [InList] 的求值负责，这里不做任何简化
                InList(
                    rewriteExpr(e.expr, run),
                    rs.rows.map { Literal(it[0]) },
                    e.negated,
                )
            }
            is ExistsSubquery -> {
                if (looksCorrelated(e.query)) return e     // 由 CorrelatedSubqueries 改写成半/反连接
                val rs = executor(e.query, run, e)
                val v = rs.rows.isNotEmpty()
                Literal(Datum.Bool(if (e.negated) !v else v))
            }
            else -> mapChildren(e) { rewriteExpr(it, run) }
        }
    }

    /**
     * 执行子查询，并把"相关子查询"翻译成一句人话。
     *
     * 相关子查询的表现是：子查询里引用了外层表的列，于是它自己执行时会报
     * 「Unknown column」。直接把这个错误抛给使用者毫无帮助——他写的是完全合法的 SQL。
     * 所以这里换成一条说明"本系统暂不支持相关子查询"的错误，并指出那个列名。
     */
    private suspend fun executor(
        query: Statement,
        run: suspend (Statement) -> QueryRows,
        node: Expr,
    ): QueryRows = try {
        run(query)
    } catch (e: SqlException) {
        // 1054（未知列）与 1053（未知表/别名）都可能来自"引用了外层的列"：
        // 前者是裸列名，后者是 `u.id` 这种限定名。两种都要翻译成人话
        if (e.errorCode == MySqlErrors.ER_BAD_FIELD_ERROR || e.errorCode == MySqlErrors.ER_BAD_TABLE_ERROR) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                "暂不支持**相关**子查询（$node）：子查询里引用了外层的列（${e.message}）。" +
                    "相关子查询的结果逐行不同，无法提前算一次；" +
                    "本系统宁可明确报错，也不按非相关的方式算出一个错误的行集。" +
                    "可以改写成 JOIN。",
            )
        }
        throw e
    }

    /** 挂起版的 `List.map`。stdlib 的 `map` 接受的是非挂起函数类型。 */
    private suspend fun mapEach(list: List<Expr>, f: suspend (Expr) -> Expr): List<Expr> {
        val out = ArrayList<Expr>(list.size)
        for (e in list) out.add(f(e))
        return out
    }

    /**
     * 重建表达式树，只替换子节点。
     *
     * 接受 suspend 的函数类型：子节点本身可能是子查询，而求值子查询要挂起
     * （它要访问对象存储）。写成普通函数类型的话，这个递归就没法调用了。
     */
    private suspend fun mapChildren(e: Expr, f: suspend (Expr) -> Expr): Expr = when (e) {
        // 注意：这里不能用 `let` / `map`——它们要的是**非挂起**的函数类型，
        // 而 f 是挂起的（子节点可能是子查询，求值它要访问对象存储）
        is CountCall -> CountCall(if (e.arg != null) f(e.arg) else null, e.distinct)
        is AggCall -> AggCall(e.name, f(e.arg), e.distinct)
        is FuncCall -> FuncCall(e.name, mapEach(e.args, f), e.distinct)
        is BinaryOp -> BinaryOp(e.op, f(e.left), f(e.right))
        is UnaryOp -> UnaryOp(e.op, f(e.expr))
        is InList -> InList(f(e.expr), mapEach(e.values, f), e.negated)
        is Between -> Between(f(e.expr), f(e.low), f(e.high), e.negated)
        is IsNull -> IsNull(f(e.expr), e.negated)
        is Like -> Like(f(e.expr), f(e.pattern), e.negated)
        else -> e
    }
}
