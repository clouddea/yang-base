package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema
import kotlin.math.abs

/**
 * 表达式求值上下文。
 * @param schema 行所属的表；无 FROM 的常量查询为 null
 * @param row 当前行；求常量表达式时为 null
 * @param vars 系统变量与用户变量。键统一小写并去掉 `@@session.` / `@@global.` 前缀
 */
class EvalContext(
    val schema: TableSchema?,
    val row: List<Datum>?,
    val vars: Map<String, Datum> = emptyMap(),
    /**
     * 本组已算出的聚合结果（`聚合表达式的规范键 → 值`）。
     *
     * 聚合**不能**在逐行求值里算出来：它是"一整组行的归约"，不在任何一行里。
     * 所以执行器先分组、把每个聚合在本组上的结果算好放进这里，
     * 再让投影 / `HAVING` / `ORDER BY` 按普通的逐行方式求值——
     * 遇到聚合节点就来这里查表。这样投影、排序、过滤三条路径**共用同一套求值器**，
     * 不需要为聚合各写一份。
     */
    val aggregates: Map<String, Datum> = emptyMap(),
    /**
     * **本语句**开始执行的时刻（毫秒）。
     *
     * `NOW()` / `CURDATE()` 在 MySQL 里是**语句级常量**：同一条语句里的每一行看到的时间
     * 必须相同。曾经它逐行读 `System.currentTimeMillis()`——一个扫一百万行的查询里，
     * 第一行与最后一行的 `NOW()` 会差出整条语句的执行时间，而
     * `WHERE ts > NOW()` 这类写法因此变得不可复现。
     *
     * 默认值让老的构造点（34 处）不用改：它们拿到的是"构造这一刻"，语义不变。
     * 执行器应该显式传语句开始时间。
     */
    val nowMillis: Long = System.currentTimeMillis(),
)

/**
 * 聚合表达式的**规范键**：同一个聚合在不同位置写法相同时必须命中同一个键。
 *
 * 用它做 `Map` 的键，是为了让「投影里的 `SUM(v)`」与「`HAVING` 里的 `SUM(v)`」
 * 与「`ORDER BY` 里的 `SUM(v)`」共享**同一次**计算——否则同一个聚合会被算三遍，
 * 而三遍之间只要有任何差异（比如 `DISTINCT` 的处理），结果就会自相矛盾。
 */
fun aggregateKey(e: Expr): String = when (e) {
    is CountCall -> "COUNT(${e.arg?.let { expressionKey(it) } ?: "*"})${if (e.distinct) "#D" else ""}"
    is AggCall -> "${e.name}(${expressionKey(e.arg)})${if (e.distinct) "#D" else ""}"
    else -> expressionKey(e)
}

/**
 * 表达式树的结构化文本，用于做键（聚合去重、GROUP BY 键比对）。
 *
 * 是 `internal` 而不是 private：`ONLY_FULL_GROUP_BY` 的检查要用它比对
 * 「这个列是不是就是某个分组键」，两处必须用**同一个**渲染，
 * 否则 `GROUP BY a+1` 与 `SELECT a+1` 会被判成不相等而误报。
 */
internal fun expressionKey(e: Expr): String = when (e) {
    is ColumnRef -> e.table?.let { "$it.${e.name}" } ?: e.name
    is Literal -> e.value.toString()
    is CountCall, is AggCall -> aggregateKey(e)
    is Star -> "*"
    else -> e.toString()
}

/**
 * `SELECT` 列表的别名表（小写别名 → 表达式）。
 *
 * MySQL 的规则：`HAVING` / `ORDER BY` 里的名字**先按别名解析**，再按表的列解析。
 * 不解析的话，`SELECT SUM(v) AS total ... ORDER BY total` 会报"没有 total 这一列"——
 * 而这是最常见的写法之一。
 */
fun aliasMapOf(stmt: SelectStmt): Map<String, Expr> {
    val out = HashMap<String, Expr>()
    for (item in stmt.projections) {
        if (item is SelectItem.Field && item.alias != null) {
            out.putIfAbsent(item.alias.lowercase(), item.expr)
        }
    }
    return out
}

/**
 * 按别名表替换表达式里的列引用（**递归进子树**）。
 *
 * 递归是必须的：`HAVING total > 100` 里的别名躲在 `BinaryOp` 里，
 * 只替换顶层的话解析器拿到的仍是 `ColumnRef("total")`，于是去表里找一列叫 total 的。
 *
 * 只替换一层（别名指向别名不追），也不做自引用替换：那两种写法极少见，
 * 而递归替换会带来 `SELECT x AS x` 这种自引用的死循环。
 */
fun resolveAliases(e: Expr, aliases: Map<String, Expr>): Expr = when {
    e is ColumnRef && e.table == null -> aliases[e.name.lowercase()]?.takeIf { it !== e } ?: e
    e is BinaryOp -> BinaryOp(e.op, resolveAliases(e.left, aliases), resolveAliases(e.right, aliases))
    e is UnaryOp -> UnaryOp(e.op, resolveAliases(e.expr, aliases))
    e is InList -> InList(resolveAliases(e.expr, aliases), e.values.map { resolveAliases(it, aliases) }, e.negated)
    e is Between -> Between(
        resolveAliases(e.expr, aliases), resolveAliases(e.low, aliases),
        resolveAliases(e.high, aliases), e.negated,
    )
    e is IsNull -> IsNull(resolveAliases(e.expr, aliases), e.negated)
    e is Like -> Like(resolveAliases(e.expr, aliases), resolveAliases(e.pattern, aliases), e.negated)
    e is FuncCall -> FuncCall(e.name, e.args.map { resolveAliases(it, aliases) }, e.distinct)
    e is CastTo -> CastTo(resolveAliases(e.expr, aliases), e.typeName)
    e is CaseExpr -> CaseExpr(
        e.operand?.let { resolveAliases(it, aliases) },
        e.whens.map { WhenClause(resolveAliases(it.condition, aliases), resolveAliases(it.result, aliases)) },
        e.elseExpr?.let { resolveAliases(it, aliases) },
    )
    else -> e
}

/** 表达式树里是否含聚合（含 [CountCall]）。 */
fun containsAggregate(e: Expr): Boolean = when (e) {
    is CountCall, is AggCall -> true
    else -> childrenOf(e).any { containsAggregate(it) }
}

/**
 * 表达式树的直接子节点。
 *
 * 单独写出来是因为「遍历」在这套代码里有三处用途（判聚合、检查 ONLY_FULL_GROUP_BY、
 * 找可下推的谓词），而 `Expr` 是密封接口、节点类型还会继续增加——
 * 把结构知识集中在一个 `when` 里，新增节点时只会有**一处**编译期报错提醒。
 */
fun childrenOf(e: Expr): List<Expr> = when (e) {
    is CountCall -> listOfNotNull(e.arg)
    is AggCall -> listOf(e.arg)
    is FuncCall -> e.args
    // 窗口调用的所有子表达式都要走遍：参数里可以有聚合（`SUM(SUM(v)) OVER (…)`），
    // 分区键与排序键里可以有列引用。漏掉它们，"这段表达式里有没有聚合"会判错，
    // 于是 `SELECT ROW_NUMBER() OVER (ORDER BY SUM(v)) FROM t GROUP BY …`
    // 不会被当成分组查询——聚合就没人算了
    is WindowCall -> e.args + e.partitionBy + e.orderBy.map { it.expr }
    is BinaryOp -> listOf(e.left, e.right)
    is UnaryOp -> listOf(e.expr)
    is InList -> listOf(e.expr) + e.values
    is Between -> listOf(e.expr, e.low, e.high)
    is IsNull -> listOf(e.expr)
    is Like -> listOf(e.expr, e.pattern)
    // CASE 的每个子表达式都要走遍：条件与结果里都可以有聚合、窗口、子查询。
    // （`else -> emptyList()` 让"漏掉一个新节点"变成静默的——所以新增节点时
    //  必须**主动**来这个地方加一行，编译器不会提醒）
    is CastTo -> listOf(e.expr)
    is CaseExpr -> listOfNotNull(e.operand) +
        e.whens.flatMap { listOf(it.condition, it.result) } +
        listOfNotNull(e.elseExpr)
    else -> emptyList()
}

/** 语义分析阶段的类型错误与运行时错误都从这里抛。 */
object Eval {

    private val NOW_FN = setOf("NOW", "CURRENT_TIMESTAMP")

    /**
     * 聚合出现在不能求值的位置（典型是 `WHERE SUM(v) > 1`）。
     *
     * MySQL 的 1111 `ER_INVALID_GROUP_FUNC_USE`。**必须报错**：WHERE 在分组**之前**
     * 求值，那里根本没有"一组"可言；静默当成 NULL 会让整个查询返回空集。
     */
    private fun invalidAggregateUse(e: Expr) = SqlException(
        MySqlErrors.ER_INVALID_GROUP_FUNC_USE, "HY000",
        "Invalid use of group function: $e 只能出现在 SELECT 列表、HAVING 或 ORDER BY 中" +
            "（WHERE 在分组之前求值，那里没有「组」可用）",
    )

    /** 窗口函数取不到值：要么写在了不能写的位置，要么执行器漏算了。 */
    private fun misplacedWindow(e: Expr) = SqlException(
        MySqlErrors.ER_INVALID_GROUP_FUNC_USE, "HY000",
        "窗口函数 $e 在这里求不出值。窗口函数只能出现在 SELECT 列表或 ORDER BY 中" +
            "（它在 HAVING 之后才求值，写在 WHERE / HAVING / GROUP BY 里时还没有值）。" +
            "若它确实写在 SELECT 列表里，请把 SQL 报告为缺陷",
    )

    fun eval(expr: Expr, ctx: EvalContext): Datum = when (expr) {
        is Literal -> expr.value
        is ColumnRef -> {
            val s = ctx.schema ?: throw SqlException(
                MySqlErrors.ER_BAD_FIELD_ERROR, "42S22", "无法在此处引用列 '${expr.name}'"
            )
            // 连接查询里列名是带限定前缀的（`u.id`），而重名的裸列名**故意不登记**。
            // 所以**先按限定名找**，找不到再退回裸名——反过来的话
            // `SELECT u.id ... JOIN` 会去查裸名 `id`，而它正好是"歧义列"，
            // 于是报出一个看起来像"没有这一列"的错（实际是能找到的）
            val qualified = expr.table?.let { s.indexOfOrNull("$it.${expr.name}") }
            val idx = qualified ?: s.indexOf(expr.name)
            ctx.row?.getOrNull(idx) ?: Datum.Null
        }
        // 聚合：从上下文里取本组已经算好的结果。
        // 取不到 = 这个位置根本没有分组上下文（例如写在 WHERE 里），必须报错——
        // 静默返回 NULL 会让 WHERE 把每一行都过滤掉，查询返回空集而不报任何错
        is CountCall -> ctx.aggregates[aggregateKey(expr)] ?: throw invalidAggregateUse(expr)
        is AggCall -> ctx.aggregates[aggregateKey(expr)] ?: throw invalidAggregateUse(expr)
        // 窗口函数与聚合一样：值不在**任何一行**里（它要看好几行），由执行器先算好、
        // 放进同一张表。取不到 = 这个位置没有窗口上下文（写在 WHERE/HAVING 里），
        // 或者执行器漏算了——两种都必须报错：静默给 NULL 会让 WHERE 恒假、返回空集
        is WindowCall -> ctx.aggregates[windowKey(expr)] ?: throw misplacedWindow(expr)
        is UnaryOp -> when (expr.op) {
            // NOT unknown = unknown（三值逻辑）
            "NOT" -> eval(expr.expr, ctx).let { if (it is Datum.Null) Datum.Null else Datum.Bool(!truthy(it)) }
            "-" -> negate(eval(expr.expr, ctx))
            else -> throw SqlException(MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", "不支持的一元运算符 ${expr.op}")
        }
        is BinaryOp -> evalBinary(expr, ctx)
        is IsNull -> Datum.Bool((eval(expr.expr, ctx) is Datum.Null) != expr.negated)
        // `IN` 的三值逻辑：`x IN (1, NULL)` 在 x=2 时是 **UNKNOWN**，不是假。
        //
        // ⚠️ 这不是学究式的精确：它决定了 `NOT IN` 的结果。
        // `x NOT IN (SELECT y ...)` 里只要子查询含一个 NULL，SQL 规定
        // "找不到 x 时结果是未知"——于是 `WHERE x NOT IN (...)` **一行都不返回**。
        // 把未知当成假会让 `NOT IN` 返回一堆本该被排除的行，
        // 而这是最经典的一类"查询看起来能跑、结果悄悄变多"的缺陷。
        is InList -> {
            val v = eval(expr.expr, ctx)
            // 与 [evalBinary] 同一条规则：**按列的类型**解释字面量。
            // 少了这一步，`WHERE id IN ('1','2')` 会因为"字符串永不等于数字"
            // 而一行都匹配不到——不报错，只是没有结果
            val colType = columnTypeOf(expr.expr, ctx)
            var unknown = v is Datum.Null
            var hit = false
            for (item in expr.values) {
                var iv = eval(item, ctx)
                if (colType != null) iv = iv.coerceTo(colType)
                if (v is Datum.Null || iv is Datum.Null) { unknown = true; continue }
                if (eq(v, iv)) { hit = true; break }
            }
            when {
                hit -> Datum.Bool(!expr.negated)
                unknown -> Datum.Null              // 未知：NOT IN 也就不是真
                else -> Datum.Bool(expr.negated)
            }
        }
        is Between -> {
            val v = eval(expr.expr, ctx)
            var lo = eval(expr.low, ctx)
            var hi = eval(expr.high, ctx)
            // 与 `=` 一样按列类型解释边界：`WHERE dt BETWEEN '…' AND '…'` 里的
            // 字面量是字符串，而 dt 是 DATETIME——不转换的话两端都比不过，
            // 查询静默返回空集（`NOT BETWEEN` 反过来返回**全部**行）
            columnTypeOf(expr.expr, ctx)?.let { t -> lo = lo.coerceTo(t); hi = hi.coerceTo(t) }
            // 三值逻辑：任一端为 NULL 时结果是 UNKNOWN（不是假）
            if (v is Datum.Null || lo is Datum.Null || hi is Datum.Null) {
                Datum.Null
            } else {
                val inRange = v.compareTo(lo) >= 0 && v.compareTo(hi) <= 0
                Datum.Bool(inRange != expr.negated)
            }
        }
        // `LIKE` 同理：NULL 参与时是 UNKNOWN。当假处理会让 `NOT LIKE 'b%'`
        // **多返回**那些值为 NULL 的行（它们本该被过滤掉）
        is Like -> {
            val v = eval(expr.expr, ctx)
            val p = eval(expr.pattern, ctx)
            if (v is Datum.Null || p is Datum.Null) Datum.Null
            else Datum.Bool(likeMatch(v, p) != expr.negated)
        }
        // 全文谓词**不能**逐行求值：BM25 打分依赖文档频率、平均文档长度这些
        // 全语料统计量，它们不在任何一行里。走到这里说明执行器没有把它当成
        // 访问路径处理——那是规划错误，必须报出来而不是静默返回 false
        // （静默 false = 每一行都被过滤掉 = 查询返回空集却不报任何错）。
        // 子查询必须**先被 [Subqueries] 折算成字面量**才能求值：
        // 它的结果是一整个集合，不在任何一行里。走到这里说明执行器漏了那一步，
        // 必须报错——静默返回 NULL 会让谓词恒假，查询返回空集却不报错
        // 标量位置的**相关**子查询：值由执行器按关联键预先算好（见 [CorrelatedScalars]），
        // 与聚合、窗口函数一样从预计算值表里查。查不到说明执行器漏了那一步——
        // 静默给 NULL 会让整个表达式变成 NULL，而查询照常返回
        is ScalarSubquery -> ctx.aggregates[scalarKey(expr)] ?: throw SqlException(
            MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
            "标量相关子查询没能被预先求值（$expr）。请连同 SQL 一起报告为缺陷",
        )
        is InSubquery, is ExistsSubquery -> throw SqlException(
            MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
            "暂不支持这种形状的子查询（$expr）。" +
                "**谓词位置**的相关子查询要求条件能改写成半/反连接——即关联条件必须是" +
                "`AND` 合取里的等值（`内层列 = 外层列`）：`OR` 两侧、非等值关联都不行" +
                "（`OR` 可以把两个条件拆成 `UNION`）。" +
                "**标量位置**的相关子查询同样需要等值关联条件（`(SELECT … WHERE b.k = a.k)`）。" +
                "子查询在非相关时必须能提前算成一个值——走到这里说明它引用了外层行",
        )
        is MatchAgainst -> throw SqlException(
            MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
            "MATCH ... AGAINST 不能作为逐行谓词求值；它需要先在倒排索引上取候选集" +
                "（请确认表上建了对应的全文索引）",
        )
        // CASE：按顺序找第一个"条件为真"的分支。
        // ⚠️ 判定用 [truthy] 而不是"非 NULL"：unknown 不是真，见 [CaseExpr] 的说明
        is CaseExpr -> {
            val operandValue: Datum? = expr.operand?.let { eval(it, ctx) }
            // ⚠️ ELSE 与 THEN **按需求值**（短路）。无条件先算 ELSE 会让
            // `CASE WHEN 1=1 THEN 'ok' ELSE CAST('x' AS SIGNED) END` 报 22007——
            // 一个语义上不该被求值的分支把整条语句搞失败，
            // 而 `IF(cond, 好值, 危险表达式)` 这种"用短路保护坏值"的写法也完全失效
            var result: Datum = Datum.Null
            var matched = false
            for (w in expr.whens) {
                val hit = if (expr.operand == null) {
                    val c = eval(w.condition, ctx)
                    c !is Datum.Null && truthy(c)
                } else {
                    // 简单式：`CASE x WHEN v` 等价于 `x = v`（含 3VL：NULL 永不匹配）
                    val v = eval(w.condition, ctx)
                    // `null` 与 `Datum.Null` 都要排掉：简单式的操作数缺席时
                    // （理论上不会走到这里）也应当落到 ELSE
                    operandValue != null && operandValue !is Datum.Null &&
                        v !is Datum.Null && eq(operandValue, v)
                }
                if (hit) { result = eval(w.result, ctx); matched = true; break }
            }
            if (!matched) result = expr.elseExpr?.let { eval(it, ctx) } ?: Datum.Null
            result
        }
        is CastTo -> Builtins.castTo(eval(expr.expr, ctx), expr.typeName)
        is FuncCall -> evalFunction(expr, ctx)
        is VarRef -> {
            // `@x` 是**用户变量**（会话里存的值），`@@x` 是系统变量。
            // 两者在 MySQL 里是两个命名空间：`@version` 与 `@@version` 互不影响，
            // 所以这里按前缀分流，而不是一起 normalize 掉再查同一张表
            if (expr.name.startsWith("@") && !expr.name.startsWith("@@")) {
                ctx.vars["@" + expr.name.removePrefix("@").lowercase()] ?: Datum.Null
            } else {
                val k = normalizeVar(expr.name)
                // 未知变量返回 NULL 而不是报错：客户端在连接阶段会探测大量
                // 互不相同的变量，逐个报错会让任何驱动都连不上
                ctx.vars[k] ?: Datum.Null
            }
        }
        Star -> throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "'*' 不能出现在这里")
    }

    /** `NULLIF` 之类的函数需要的"SQL 相等"语义（含跨类型转换），与 `=` 运算符同一份实现。 */
    internal fun eqPublic(a: Datum, b: Datum): Boolean = eq(a, b)

    private fun evalFunction(f: FuncCall, ctx: EvalContext): Datum {
        // 函数名在解析期已大写（Parser.kt 的 `name.uppercase()`），这里再兜一次容错
        val name = f.name.uppercase()
        // 一元负号等前缀由解析器处理，这里只处理真正的函数调用
        if (f.distinct) {
            // `f(DISTINCT x)` 只对聚合有意义；标量函数上它是写法错误，
            // 静默按非 DISTINCT 算等于**忽略用户写下的关键字**（MySQL 报语法错）
            throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000",
                "DISTINCT 不能用在标量函数 $name 里（它只对聚合函数有意义）",
            )
        }
        val args = f.args.map { eval(it, ctx) }
        // ★ 内置函数库：字符串 / 数值 / 日期 / 控制流 / 类型转换。
        // 参数个数与 NULL 语义由注册表统一执行（见 Builtins）
        Builtins.call(name, args, ctx)?.let { return it }
        // JSON 函数：参数个数与语义都在 [JsonFunctions] 里，这里只做分发与参数检查
        if (f.name.uppercase() in JsonFunctions.NAMES) {
            fun need(n: Int) {
                if (args.size != n) throw SqlException(
                    MySqlErrors.ER_WRONG_ARGUMENTS, "HY000",
                    "${f.name} 需要 $n 个参数，实际 ${args.size} 个",
                )
            }
            // 路径语法错必须报错（1103），不能静默返回 NULL——
            // 拼错的路径会让"所有行都没这个字段"，而使用者看不出来
            return try {
                when (f.name.uppercase()) {
                    "JSON_EXTRACT" -> {
                        need(2)
                        // 多路径时返回数组（MySQL 的语义），单路径直接返回该值
                        if (args.size == 2) JsonFunctions.extract(args[0], args[1])
                        else JsonFunctions.extractMany(args[0], args.drop(1))
                    }
                    "JSON_UNQUOTE" -> { need(1); JsonFunctions.unquote(args[0]) }
                    "JSON_VALID" -> { need(1); JsonFunctions.isValid(args[0]) }
                    "JSON_TYPE" -> { need(1); JsonFunctions.typeOf(args[0]) }
                    "JSON_LENGTH" -> { need(1); JsonFunctions.lengthOf(args[0]) }
                    "JSON_KEYS" -> { need(1); JsonFunctions.keysOf(args[0]) }
                    else -> throw SqlException(
                        MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", "不支持的 JSON 函数 ${f.name}",
                    )
                }
            } catch (e: JsonPathError) {
                throw SqlException(
                    MySqlErrors.ER_INVALID_JSON_PATH, "42000",
                    "Invalid JSON path expression: ${e.message}",
                )
            }
        }
        // 剩下的是**会话相关**与**环境相关**的函数：它们的值不在参数里，
        // 而在连接/会话状态里（`DATABASE()`、`VERSION()`、`LAST_INSERT_ID()`）。
        // 这类函数不适合放进 `Builtins` 的纯函数注册表——那里只处理"输入决定输出"的函数
        return when (name) {
            "DATABASE", "SCHEMA" -> ctx.vars["database"] ?: Datum.Null
            "VERSION" -> Datum.Str(ServerInfo.VERSION)
            "USER", "CURRENT_USER", "SESSION_USER" -> ctx.vars["current_user"] ?: Datum.Str("root@%")
            "CONNECTION_ID" -> ctx.vars["connection_id"] ?: Datum.Null
            "LAST_INSERT_ID" -> ctx.vars["last_insert_id"] ?: Datum.I64(0)
            "ROW_COUNT" -> ctx.vars["row_count"] ?: Datum.I64(0)
            "FOUND_ROWS" -> ctx.vars["found_rows"] ?: Datum.I64(0)
            else -> throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", "不支持的函数 $name",
            )
        }
    }

    private fun evalBinary(b: BinaryOp, ctx: EvalContext): Datum {
        // AND / OR 走**三值逻辑**（NULL 是"未知"，不是"假"）。
        // 与"两个值当布尔与或"的差别只在 NULL 参与时，但那一处决定了
        // `SELECT NULL AND true` 是 NULL 还是 false——而 WHERE 里两者都表现为"不通过"，
        // 所以这里按语义正确写，让 SELECT 出来的值也诚实
        when (b.op.uppercase()) {
            "AND" -> {
                val l = eval(b.left, ctx)
                if (l is Datum.Null) {
                    // false AND unknown = false；unknown AND unknown/true = unknown
                    val r = eval(b.right, ctx)
                    return if (r !is Datum.Null && !truthy(r)) Datum.Bool(false) else Datum.Null
                }
                if (!truthy(l)) return Datum.Bool(false)
                val r = eval(b.right, ctx)
                return if (r is Datum.Null) Datum.Null else Datum.Bool(truthy(r))
            }
            "OR" -> {
                val l = eval(b.left, ctx)
                if (l !is Datum.Null && truthy(l)) return Datum.Bool(true)
                val r = eval(b.right, ctx)
                if (r !is Datum.Null && truthy(r)) return Datum.Bool(true)
                // true OR unknown = true（上面已返回）；这里只剩 false/unknown 的组合
                return if (l is Datum.Null || r is Datum.Null) Datum.Null else Datum.Bool(false)
            }
        }
        var l = eval(b.left, ctx)
        var r = eval(b.right, ctx)

        // 列与字面量比较时，按**列的类型**解释字面量。
        // 没有这一步，`WHERE dt = '2026-01-01 10:00:00'` 会走跨类型比较而永不相等，
        // 查询静默返回空集——不报错，只是没有结果，属于最难排查的一类缺陷。
        if (isComparison(b.op)) {
            val lt = columnTypeOf(b.left, ctx)
            val rt = columnTypeOf(b.right, ctx)
            when {
                lt != null && rt == null -> r = r.coerceTo(lt)
                rt != null && lt == null -> l = l.coerceTo(rt)
            }
        }
        // 字符串与数值比较：把**看起来是数字**的字符串按数值解释。
        //
        // 没有这一步，`Datum.compareTo` 会走「字符串排在所有数值之后」的类型序，
        // 于是 `j ->> '$.age' > 25` 对**每一行**都为真（`'30'` 也"大于" 25），
        // 过滤条件静默失效——查询不报错，只是把整张表返回了。
        //
        // JSON 是这条路最常见的入口：`->>` 按 MySQL 的语义给的是**文本**
        // （`JSON_UNQUOTE` 的结果），数字在它那里也是 `'30'` 而不是 30。
        //
        // 与 MySQL 的差异：MySQL 把非数字字符串转成 0（`'abc' = 0` 为真），
        // 这里只在**整个字符串都是合法数字**时才转换，其它情况保持原样。
        // 「垃圾值当 0」会让拼错的过滤条件匹配到一堆本不该匹配的行，
        // 而那种错误同样是静默的。
        if (isComparison(b.op)) {
            val asNum = (l as? Datum.Str)?.v?.let { textAsNumber(it) }
            val bsNum = (r as? Datum.Str)?.v?.let { textAsNumber(it) }
            if (asNum != null && isNumericValue(r)) l = asNum
            else if (bsNum != null && isNumericValue(l)) r = bsNum
        }
        // ★ 三值逻辑：与 NULL 的任何比较都是 **UNKNOWN**（`Datum.Null`），不是假。
        //
        // ⚠️ 这里曾经直接返回布尔值，而 `Datum.compareTo` 把 NULL 排在最前
        // （那是 `ORDER BY` 需要的语义）——于是 `user_id < 10` 对 user_id IS NULL
        // 的行**返回真**，那些行被错误地返回给查询。MySQL 永远不会这样：
        // 与 NULL 的比较既不真也不假。这是"不报错、只多返回行"的一类缺陷，
        // 是靠连接的外键测试（`o.user_id < u.id` 多匹配了三行）才露出来的。
        if (isComparison(b.op) && b.op != "<=>" && (l is Datum.Null || r is Datum.Null)) {
            return Datum.Null
        }
        // `->` / `->>`：JSON 取值（`->>` 额外去掉引号）。
        // 两个运算符的区别是**类型**：`->` 给 JSON（字符串带引号），`->>` 给字符串。
        // 混同会让 `WHERE j ->> '$.a' = 'x'` 永远匹配不上（拿到的是 `"x"`）
        if (b.op == "->" || b.op == "->>") {
            return try {
                if (b.op == "->") JsonFunctions.extract(l, r) else JsonFunctions.extractText(l, r)
            } catch (e: JsonPathError) {
                throw SqlException(
                    MySqlErrors.ER_INVALID_JSON_PATH, "42000",
                    "Invalid JSON path expression: ${e.message}",
                )
            }
        }
        return when (b.op) {
            "=", "<=>" -> {
                if (b.op == "<=>" && l is Datum.Null && r is Datum.Null) Datum.Bool(true)
                else Datum.Bool(eq(l, r))
            }
            "<>" -> Datum.Bool(!eq(l, r))
            "<" -> Datum.Bool(l.compareTo(r) < 0)
            "<=" -> Datum.Bool(l.compareTo(r) <= 0)
            ">" -> Datum.Bool(l.compareTo(r) > 0)
            ">=" -> Datum.Bool(l.compareTo(r) >= 0)
            "+", "-", "*", "/", "%" -> arith(b.op, l, r)
            else -> throw SqlException(MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", "不支持的运算符 ${b.op}")
        }
    }

    private fun isComparison(op: String) =
        op == "=" || op == "<=>" || op == "<>" || op == "<" || op == "<=" || op == ">" || op == ">="

    private fun isNumericValue(d: Datum) = d is Datum.I64 || d is Datum.F64

    /** 整个字符串都是合法数字时给出数值形式，否则 null（见 [evalBinary] 里的取舍说明）。 */
    private fun textAsNumber(text: String): Datum? {
        if (text.isBlank()) return null
        text.toLongOrNull()?.let { return Datum.I64(it) }
        text.toDoubleOrNull()?.let { return Datum.F64(it) }
        return null
    }

    /** 表达式若直接引用一个已知类型的列，返回它的列类型。 */
    private fun columnTypeOf(e: Expr, ctx: EvalContext): ColumnType? {
        val name = (e as? ColumnRef)?.name ?: return null
        val t = ctx.schema ?: return null
        val i = t.indexOfOrNull(name) ?: return null
        return t.columns[i].type
    }

    /**
     * 二元算术。`+` `-` `*` 在整数上**溢出会报错**（见下面的说明）；
     * `/` 一律按浮点算——与 MySQL 一致（`SELECT 5/2` 给 2.5 而不是 2），
     * 因此整数除法不存在溢出问题。
     */
    private fun arith(op: String, l: Datum, r: Datum): Datum {
        if (l is Datum.Null || r is Datum.Null) return Datum.Null
        // 字符串与时间统一按数值参与运算（与 MySQL 的隐式转换近似）
        return when {
            l is Datum.F64 || r is Datum.F64 || op == "/" -> {
                val a = l.asDouble(); val b = r.asDouble()
                when (op) {
                    "+" -> Datum.F64(a + b); "-" -> Datum.F64(a - b); "*" -> Datum.F64(a * b)
                    "/" -> if (b == 0.0) Datum.Null else Datum.F64(a / b)
                    "%" -> if (b == 0.0) Datum.Null else Datum.F64(a % b)
                    else -> throw SqlException(MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", "运算符 $op")
                }
            }
            // ★ 两个操作数都是定点数（同标度）：走**定点运算**，不要掉进下面的整数分支。
            //
            // 这里曾经只有 `+`/`-` 是对的，`*` 与 `%` 都执行
            // `l.asLong() * r.asLong() / 1` —— 既把小数据截断成整数，
            // 又让**取模算成了乘积**：`SELECT 10.00 % 3.00` 得到 30（而不是 1.00）。
            // 金额与比例上的静默错值，正是本项目最不能接受的一类。
            l is Datum.Dec && r is Datum.Dec && l.scale == r.scale -> {
                val scale = l.scale
                fun pow() = Datum.pow10Long(scale)
                when (op) {
                    "+" -> Datum.Dec(Math.addExact(l.unscaled, r.unscaled), scale)
                    "-" -> Datum.Dec(Math.subtractExact(l.unscaled, r.unscaled), scale)
                    // 乘法：结果标度是两者之和，这里再按原标度舍回（MySQL 会给出 s1+s2 位）
                    "*" -> {
                        val product = java.math.BigDecimal.valueOf(l.unscaled)
                            .multiply(java.math.BigDecimal.valueOf(r.unscaled))
                            .movePointLeft(scale)                       // 还原真实值
                            .setScale(scale, java.math.RoundingMode.HALF_UP)
                        Datum.Dec(product.movePointRight(scale).longValueExact(), scale)
                    }
                    "%" -> if (r.unscaled == 0L) Datum.Null else Datum.Dec(l.unscaled % r.unscaled, scale)
                    "/" -> {
                        if (r.unscaled == 0L) Datum.Null
                        else {
                            val q = java.math.BigDecimal.valueOf(l.unscaled)
                                .divide(java.math.BigDecimal.valueOf(r.unscaled), scale, java.math.RoundingMode.HALF_UP)
                            Datum.Dec(q.movePointRight(scale).longValueExact(), scale)
                        }
                    }
                    else -> throw SqlException(MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", "运算符 $op")
                }
            }
            // 两个都是定点但**标度不同**：对齐到较大标度再算（同样是定点，不掉浮点）
            l is Datum.Dec && r is Datum.Dec -> {
                val scale = maxOf(l.scale, r.scale)
                val lu = l.unscaled * Datum.pow10Long(scale - l.scale)
                val ru = r.unscaled * Datum.pow10Long(scale - r.scale)
                arith(op, Datum.Dec(lu, scale), Datum.Dec(ru, scale))
            }
            else -> {
                val a = l.asLong(); val b = r.asLong()
                // ★ 整数运算**溢出必须报错**，不能回绕。
                //
                // `9223372036854775807 + 1` 回绕成 `-9223372036854775808`：不报错，
                // 只是数字变成了一个看起来正常、符号却完全相反的负数，而使用者
                // 拿它做对账与计数。本系统对 `SUM` 已经就是这个立场
                // （见 [Aggregates.sumOf]），标量运算必须一致——同一个加法，
                // 聚合时报错、单独写就回绕，是一处没有理由的差异
                when (op) {
                    "+" -> Datum.I64(exactI64(a, b) { x, y -> Math.addExact(x, y) })
                    "-" -> Datum.I64(exactI64(a, b) { x, y -> Math.subtractExact(x, y) })
                    "*" -> Datum.I64(exactI64(a, b) { x, y -> Math.multiplyExact(x, y) })
                    // `/` 走不到这里：它在上面按浮点算（MySQL 的 `/` 也不做整除），
                    // 所以整数除法**不存在**溢出问题（`MIN / -1` 在双精度里是精确的）
                    "/" -> if (b == 0L) Datum.Null else Datum.I64(a / b)
                    "%" -> if (b == 0L) Datum.Null else Datum.I64(a % b)
                    else -> throw SqlException(MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", "运算符 $op")
                }
            }
        }
    }

    /** 整数运算的溢出检查（见 [arith] 里的说明）。 */
    private inline fun exactI64(a: Long, b: Long, op: (Long, Long) -> Long): Long = try {
        op(a, b)
    } catch (_: ArithmeticException) {
        throw overflowI64(a, b)
    }

    private fun overflowI64(a: Long, b: Long) = SqlException(
        MySqlErrors.ER_DATA_OUT_OF_RANGE, "22003",
        "整数运算溢出（BIGINT 范围）：$a 与 $b 的运算结果超出 64 位。" +
            "本系统不回绕——回绕会给出一个符号都反了的「正常」数字。" +
            "需要更大范围请把列改成 DECIMAL，或先转成浮点",
    )

    private fun negate(d: Datum): Datum = when (d) {
        // `-(-9223372036854775808)` 在 64 位里没有对应的正数（与 `/ -1` 同一处边界）
        is Datum.I64 -> if (d.v == Long.MIN_VALUE) throw overflowI64(d.v, -1L) else Datum.I64(-d.v)
        is Datum.F64 -> Datum.F64(-d.v)
        is Datum.Dec -> Datum.Dec(-d.unscaled, d.scale)
        is Datum.Null -> Datum.Null
        else -> Datum.I64(-d.asLong())
    }

    /** SQL 三值逻辑：NULL 参与比较得到 false（用于 WHERE）。 */
    /**
     * 布尔上下文里"这个值算不算真"。
     *
     * ⚠️ **字符串按 MySQL 的规则转成数字**：取前导数字部分，非数字串当 0。
     * 曾经的写法是 `isNotEmpty() && v != "0"`，于是 `'0.0'` / `'false'` / `'abc'`
     * 全都是**真**——而同一个值在 `WHERE '0.0' = 0` 里是**真**、在 `WHERE '0.0'` 里
     * 却是假（MySQL 两边都是假）。同一份数据在 `=` 与布尔上下文里判决不同，
     * 最典型的入口是 `WHERE json_col->>'$.flag'`（JSON 里的 `false`/`0`）
     */
    fun truthy(d: Datum): Boolean = when (d) {
        is Datum.Null -> false
        is Datum.Bool -> d.v
        is Datum.I64 -> d.v != 0L
        is Datum.F64 -> d.v != 0.0
        is Datum.Dec -> d.unscaled != 0L
        is Datum.Str -> leadingNumber(d.v) != 0.0
        is Datum.Ts -> d.millis != 0L
        is Datum.Bin -> d.v.isNotEmpty()
    }

    /** MySQL 的字符串→数字转换：取最长数字前缀，没有前缀就是 0（不报错）。 */
    private fun leadingNumber(text: String): Double {
        val t = text.trimStart()
        var end = 0
        if (end < t.length && (t[end] == '+' || t[end] == '-')) end++
        var seenDigit = false
        while (end < t.length && (t[end].isDigit() || t[end] == '.')) {
            if (t[end].isDigit()) seenDigit = true
            end++
        }
        if (!seenDigit) return 0.0
        // 指数部分
        if (end < t.length && (t[end] == 'e' || t[end] == 'E')) {
            var e = end + 1
            if (e < t.length && (t[e] == '+' || t[e] == '-')) e++
            var digits = 0
            while (e < t.length && t[e].isDigit()) { e++; digits++ }
            if (digits > 0) end = e
        }
        return t.substring(0, end).toDoubleOrNull() ?: 0.0
    }

    /** 变量名归一化：小写、去掉作用域前缀与 `@`。 */
    fun normalizeVar(name: String): String = name
        .lowercase()
        .removePrefix("@@")
        .removePrefix("session.")
        .removePrefix("global.")
        .removePrefix("@")

    /** 等值比较：NULL 与任何值（含 NULL）都不相等。 */
    fun eq(a: Datum, b: Datum): Boolean {
        if (a is Datum.Null || b is Datum.Null) return false
        if (a is Datum.Str && b is Datum.Str) return a.v == b.v
        return a.compareTo(b) == 0
    }

    /** SQL LIKE 的通配匹配。`%` 匹配任意长度，`_` 匹配单字符。 */
    fun likeMatch(value: Datum, pattern: Datum): Boolean {
        if (value is Datum.Null || pattern is Datum.Null) return false
        return com.cloudea.yangbase.index.skip.LikeMatcher.matches(value.asString(), pattern.asString())
    }

    /**
     * 尝试把 `列 = 常量` 或 `列 BETWEEN a AND b` 提取成主键上的范围。
     *
     * 这是**减少对象存储请求数**最重要的一步（设计文档 §10.2 R1）：
     * 把全表扫描变成一次点查或一次范围扫描。
     */
    fun extractPkRange(where: Expr?, schema: TableSchema): PkRange? {
        val pkCol = schema.primaryKey.firstOrNull() ?: return null
        val pkIdx = schema.indexOfOrNull(pkCol) ?: return null
        if (schema.primaryKey.size != 1) return null   // 复合主键的裁剪暂不实现

        fun asPkColumn(e: Expr): Boolean = e is ColumnRef && e.name.equals(pkCol, ignoreCase = true)

        return when (where) {
            is BinaryOp -> when (where.op) {
                "=" -> {
                    val (colSide, valSide) = when {
                        asPkColumn(where.left) -> where.left to where.right
                        asPkColumn(where.right) -> where.right to where.left
                        else -> return null
                    }
                    val v = tryEvalConst(valSide, schema)
                    if (v == null || v is Datum.Null) null else PkRange.eq(v, pkIdx)
                }
                ">", ">=", "<", "<=" -> {
                    val rising = when {
                        asPkColumn(where.left) -> {
                            val v = tryEvalConst(where.right, schema) ?: return null
                            when (where.op) { ">" -> PkRange.low(v, true); ">=" -> PkRange.low(v, false); "<" -> PkRange.high(v, true); else -> PkRange.high(v, false) }
                        }
                        asPkColumn(where.right) -> {
                            val v = tryEvalConst(where.left, schema) ?: return null
                            when (where.op) { "<" -> PkRange.low(v, true); "<=" -> PkRange.low(v, false); ">" -> PkRange.high(v, true); else -> PkRange.high(v, false) }
                        }
                        else -> null
                    }
                    rising
                }
                "AND" -> {
                    val a = extractPkRange(where.left, schema)
                    val b = extractPkRange(where.right, schema)
                    when {
                        a == null -> b
                        b == null -> a
                        else -> a.merge(b)
                    }
                }
                else -> null
            }
            is Between -> {
                if (!asPkColumn(where.expr)) return null
                val lo = tryEvalConst(where.low, schema) ?: return null
                val hi = tryEvalConst(where.high, schema) ?: return null
                PkRange.between(lo, hi, pkIdx)
            }
            else -> null
        }
    }

    /**
     * 从 WHERE 中提取**复合主键**上的键区间。
     *
     * 返回 null 表示推不出可用约束（调用方回退到全表扫描）。
     * 提取是**保守**的：宁可少约束一列，也不能多约束——多约束会漏行。
     */
    fun extractKeyRange(where: Expr?, schema: TableSchema): KeyRange? {
        val pkIdx = schema.primaryKeyIndexes
        if (pkIdx.isEmpty()) return null

        // 把 AND 树摊平成合取项，逐列收集等值/范围约束
        val eqs = HashMap<Int, Datum>()
        val lows = HashMap<Int, Pair<Datum, Boolean>>()
        val highs = HashMap<Int, Pair<Datum, Boolean>>()
        for (c in conjuncts(where)) {
            val ref = c as? BinaryOp ?: continue
            val op = ref.op.uppercase()
            for ((pos, colIdx) in pkIdx.withIndex()) {
                val colName = schema.columns[colIdx].name
                if (op !in COMPARISON_OPS) continue
                val hit = if (op == "=") {
                    equalityValue(ref, colName, schema)?.let { it to "=" }
                } else {
                    comparisonValue(ref, colName, op, schema)
                } ?: continue
                val v = hit.first
                if (hit.second == "=") {
                    eqs.putIfAbsent(colIdx, v)
                    continue
                }
                if (hit.second == "<" || hit.second == "<=") {
                    val cur = highs[colIdx]
                    val excl = hit.second == "<"
                    if (cur == null || tighterHigh(v, excl, cur.first, cur.second)) highs[colIdx] = v to excl
                } else {
                    val cur = lows[colIdx]
                    val excl = hit.second == ">"
                    if (cur == null || tighterLow(v, excl, cur.first, cur.second)) lows[colIdx] = v to excl
                }
            }
        }

        // 从最左列开始吃掉等值前缀
        val prefix = ArrayList<Datum>()
        for (colIdx in pkIdx) {
            val v = eqs[colIdx] ?: break
            prefix.add(v)
        }

        if (prefix.size == pkIdx.size) return KeyRange(prefix, exactPk = prefix.toList())

        // 停止列上还可能有范围约束
        val stop = pkIdx[prefix.size]
        val lo = lows[stop]
        val hi = highs[stop]
        if (prefix.isEmpty() && lo == null && hi == null) return null
        return KeyRange(
            prefix = prefix,
            low = lo?.first, lowExclusive = lo?.second ?: false,
            high = hi?.first, highExclusive = hi?.second ?: false,
        )
    }

    private val COMPARISON_OPS = setOf("=", "<", "<=", ">", ">=")

    private fun tighterHigh(v: Datum, excl: Boolean, curV: Datum, curExcl: Boolean): Boolean {
        val c = v.compareTo(curV)
        return c < 0 || (c == 0 && excl && !curExcl)
    }

    private fun tighterLow(v: Datum, excl: Boolean, curV: Datum, curExcl: Boolean): Boolean {
        val c = v.compareTo(curV)
        return c > 0 || (c == 0 && excl && !curExcl)
    }

    /** 摊平 `a AND b AND c`。刻意**不**穿透 OR——OR 之下没有可用的合取信息。 */
    private fun conjuncts(e: Expr?): List<Expr> = when (e) {
        null -> emptyList()
        is BinaryOp -> if (e.op.equals("AND", ignoreCase = true)) conjuncts(e.left) + conjuncts(e.right) else listOf(e)
        else -> listOf(e)
    }

    private fun equalityValue(e: BinaryOp, col: String, schema: TableSchema): Datum? {
        val v = when {
            e.left is ColumnRef && e.left.name.equals(col, true) -> tryEvalConst(e.right, schema)
            e.right is ColumnRef && e.right.name.equals(col, true) -> tryEvalConst(e.left, schema)
            else -> null
        } ?: return null
        return v.takeIf { it !is Datum.Null }
    }

    /**
     * `col <op> 常量` 与 `常量 <op> col`。
     *
     * 反写时比较符要翻转，否则 `100 > age` 会被当成 `age > 100`——
     * 那会把结果集整个翻过来，属于会静默给出错误答案的缺陷。
     */
    private fun comparisonValue(e: BinaryOp, col: String, op: String, schema: TableSchema): Pair<Datum, String>? {
        val colLeft = e.left is ColumnRef && e.left.name.equals(col, true) &&
            tryEvalConst(e.right, schema) != null
        val colRight = e.right is ColumnRef && e.right.name.equals(col, true) &&
            tryEvalConst(e.left, schema) != null
        if (!colLeft && !colRight) return null
        val v = (if (colLeft) tryEvalConst(e.right, schema) else tryEvalConst(e.left, schema)) ?: return null
        if (v is Datum.Null) return null
        // `常量 <op> 列` 要翻转比较符：`100 > age` 等价于 `age < 100`。
        // 不翻转就是把结果集取反——会静默给出错误答案。
        val effOp = if (colLeft) op else when (op) {
            "<" -> ">"; "<=" -> ">="; ">" -> "<"; else -> "<="
        }
        return v to effOp
    }

    private fun tryEvalConst(e: Expr, schema: TableSchema): Datum? = try {
        eval(e, EvalContext(schema, null))
    } catch (_: Exception) {
        null
    }
}

/** 主键上的范围约束。null 边界表示无界。 */
data class PkRange(
    val low: Datum? = null,
    val lowExclusive: Boolean = false,
    val high: Datum? = null,
    val highExclusive: Boolean = false,
    /** 若已知是等值查询，直接给出该值。 */
    val exact: Datum? = null,
    val columnIndex: Int = 0,
) {
    fun merge(other: PkRange): PkRange {
        if (exact != null) return this
        if (other.exact != null) return other
        var lo = low; var loEx = lowExclusive
        if (other.low != null && (lo == null || other.low.compareTo(lo) > 0 ||
                (other.low.compareTo(lo) == 0 && other.lowExclusive))) {
            lo = other.low; loEx = other.lowExclusive
        }
        var hi = high; var hiEx = highExclusive
        if (other.high != null && (hi == null || other.high.compareTo(hi) < 0 ||
                (other.high.compareTo(hi) == 0 && other.highExclusive))) {
            hi = other.high; hiEx = other.highExclusive
        }
        return PkRange(lo, loEx, hi, hiEx, null, columnIndex)
    }

    val isUnbounded: Boolean get() = low == null && high == null

    companion object {
        fun eq(v: Datum, col: Int) = PkRange(v, false, v, false, v, col)
        fun low(v: Datum, exclusive: Boolean, col: Int = 0) = PkRange(low = v, lowExclusive = exclusive, columnIndex = col)
        fun high(v: Datum, exclusive: Boolean, col: Int = 0) = PkRange(high = v, highExclusive = exclusive, columnIndex = col)
        fun between(lo: Datum, hi: Datum, col: Int) = PkRange(lo, false, hi, false, null, col)
    }
}

/**
 * 主键上的一个**键区间**。是 [PkRange] 的推广。
 *
 * [PkRange] 只能表达「单列主键上的范围」，于是 `if (primaryKey.size != 1) return null`
 * ——复合主键表的 `WHERE id = 1` 直接退化成**全表扫描**。在对象存储上那是
 * 1 次 GET 与上百次 GET 的差别，也是设计文档 §10.2 R1 最想消除的那一类开销。
 *
 * 复合主键的键是**逐列拼接**的，因此只能从最左列开始约束：
 * `(a, b, c)` 上可以用 `a = 1`、`a = 1 AND b > 2`，但不能只用 `b = 2`。
 * 这跟 B+ 树的最左前缀规则是同一条，不是实现取舍。
 */
data class KeyRange(
    /** 前 k 个主键列的等值约束，按主键顺序。k 可以为 0。 */
    val prefix: List<Datum>,
    /** 紧随 prefix 之后那一列的下界。 */
    val low: Datum? = null,
    val lowExclusive: Boolean = false,
    /** 紧随 prefix 之后那一列的上界。 */
    val high: Datum? = null,
    val highExclusive: Boolean = false,
    /** 全部主键列都被等值覆盖 → 可以退化成一次点查。 */
    val exactPk: List<Datum>? = null,
) {
    /** 能否用来缩小读取范围。仅有前缀也算——前缀本身即一段连续区间。 */
    val isUsable: Boolean get() = exactPk != null || prefix.isNotEmpty()
}
