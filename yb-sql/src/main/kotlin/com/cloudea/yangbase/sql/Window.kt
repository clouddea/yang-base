package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema

/**
 * **窗口函数**的求值（`ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)`）。
 *
 * ## 它在求值顺序里的位置
 *
 * ```
 *   FROM → WHERE → GROUP BY → 聚合 → HAVING → ★窗口函数★ → SELECT → DISTINCT → ORDER BY → LIMIT
 * ```
 *
 * 三件事直接由这个位置决定，而且每一件弄错都会给出**看起来正常**的结果：
 *
 * 1. 窗口函数看到的是 `WHERE` / `HAVING` **过滤之后**的行——不是表里的原始行。
 *    放在过滤之前的话，被 `HAVING` 筛掉的组会继续参与排名与求和。
 * 2. 有 `GROUP BY` 时，窗口函数作用在**分组后的行**上：`SUM(SUM(v)) OVER (ORDER BY x)`
 *    先按组求和、再在这些"组和"上开窗。所以窗口的入参可以是一个聚合。
 * 3. 窗口函数**在 `ORDER BY` 之前**算完，于是 `ORDER BY ROW_NUMBER() OVER (…)`
 *    是合法的（它是"给这一行起的名字"，不是聚合）。
 *
 * ## 与聚合的关键区别：不是"一组一行"，是"一行一行地看一个窗口"
 *
 * `SUM(v) OVER (ORDER BY k)` **不改变行数**——每一行都出一个值，只是每一行看到的
 * 窗口不同。把窗口函数实现成聚合的别名，会让 `SELECT ROW_NUMBER() OVER () FROM t`
 * 只返回一行（整表被归约掉了），而且不报错。
 *
 * ## 计算代价
 *
 * 窗口帧有两种求值方式，选择由**帧的形状**决定，而不是由"哪个更快"决定：
 *
 * | 帧 | 做法 | 代价 |
 * |---|---|---|
 * | 起点是 `UNBOUNDED PRECEDING`（含默认帧） | **增量累加**：分区内一路加下去 | 每分区 O(n) |
 * | 其它（滑动的窗，如 `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`） | 每个位置**重算**整帧 | 每位置 O(帧长) |
 *
 * 第一类覆盖了绝大多数真实用法（累计求和、排名、分位）。第二类是滑动窗：它必须
 * 支持"移出窗口"，而 `MIN` / `MAX` / `COUNT(DISTINCT)` 没有常数时间的删除，
 * 与其实现一个有条件的近似，不如老老实实重算。
 *
 * 两条路径的结果**必须逐值相同**——同一个帧的两种写法（默认帧 vs 显式写出
 * `ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW`，后者会被夹到同一个帧）
 * 会走两条不同的代码路径，答案不同就意味着"哪个对"取决于写法。
 * 这个不变量由 `SqlSurfaceTest.增量与重算两条路径结果一致` 逐值比对守着。
 */
object Window {

    /** 窗口形态的聚合函数。它们在帧上归约，语义与不带 `OVER` 时**完全一致**。 */
    val AGGREGATES = setOf("COUNT", "SUM", "AVG", "MIN", "MAX")

    /** 排名函数。它们**不看帧**，只看分区内的位置。 */
    val RANKING = setOf("ROW_NUMBER", "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST", "NTILE")

    /** 取值函数：从帧里（或相对位置的另一行）取一个值。 */
    val VALUES = setOf("LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE")

    val NAMES = AGGREGATES + RANKING + VALUES

    /**
     * **只有**窗口形态才有意义的函数名：没有 `OVER` 就是语法错误。
     *
     * 与 [NAMES] 的区别在于聚合（`COUNT` / `SUM` / …）：它们不带 `OVER` 时是
     * 普通聚合，完全合法——所以不能拿 [NAMES] 当这个名单用。
     */
    val WINDOW_ONLY = RANKING + VALUES

    /**
     * 没有 `ORDER BY` 就没有意义的函数。
     *
     * `RANK()` 问的是"按这个顺序我排第几"——没有顺序，这个问题本身不成立。
     * MySQL 会在这种写法上报 3587。放过去的话，只能拿扫描顺序当顺序：
     * 结果**看起来**是对的（数字都在合理范围内），但换一次执行计划就变，
     * 而使用者不会想到"这个排名是随机的"。`LAG` / `LEAD` 同理（"上一行"没有定义）。
     */
    private val NEEDS_ORDER = setOf(
        "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST", "LAG", "LEAD", "NTILE",
    )

    /** 收集表达式树里的全部窗口调用（按规范化键去重，保持出现顺序）。 */
    fun collect(stmt: SelectStmt): List<WindowCall> {
        val seen = LinkedHashMap<String, WindowCall>()
        fun walk(e: Expr?) {
            if (e == null) return
            if (e is WindowCall) {
                seen.putIfAbsent(windowKey(e), e)
                // 参数里可以有聚合（`SUM(SUM(v)) OVER (…)`），继续往下走
            }
            childrenOf(e).forEach { walk(it) }
        }
        stmt.projections.forEach { if (it is SelectItem.Field) walk(it.expr) }
        walk(stmt.having)
        stmt.orderBy.forEach { walk(it.expr) }
        return seen.values.toList()
    }

    /**
     * 算出每一行的窗口值。
     *
     * @param rows 行本身，顺序即最终输出顺序
     * @param aggsPerRow 每行所属那**一组**已经算好的聚合值（分组查询才有），
     *        窗口的入参若是聚合就从这里取——见 `SUM(SUM(v)) OVER (…)`
     * @return 与 [rows] 等长；第 i 项是"第 i 行的窗口键 → 值"
     */
    fun compute(
        rows: List<List<Datum>>,
        aggsPerRow: List<Map<String, Datum>>?,
        schema: TableSchema,
        vars: Map<String, Datum>,
        calls: List<WindowCall>,
    ): List<Map<String, Datum>> {
        val out = List(rows.size) { HashMap<String, Datum>(maxOf(1, calls.size)) }
        if (calls.isEmpty() || rows.isEmpty()) return out
        for (call in calls) {
            validate(call)
            val key = windowKey(call)
            for (members in partition(rows, aggsPerRow, schema, vars, call)) {
                val sorted = orderMembers(rows, aggsPerRow, schema, vars, call, members)
                // 回调给的是**原始行号**，不是"分区内的位置"：
                // 这里再按位置查一次 `sorted` 就错了（那是把排列又作用了一遍），
                // 结果是一组值被写到别的行上——排序看起来正常，数字全是错位的
                evaluate(call, sorted, rows, aggsPerRow, schema, vars) { row, value ->
                    out[row][key] = value
                }
            }
        }
        return out
    }

    /**
     * 窗口函数**不能**出现在 `WHERE` / `HAVING` / `GROUP BY` 里。
     *
     * 它们的求值时机在 `HAVING` 之后，所以写在那些位置根本无值可取。
     * 必须报错而不是当 NULL：`WHERE ROW_NUMBER() OVER (…) = 1` 若静默恒假，
     * 查询返回空集，看不出任何异常。
     */
    fun rejectMisplaced(stmt: SelectStmt) {
        fun check(e: Expr?, where: String) {
            if (e == null) return
            if (containsWindow(e)) {
                throw SqlException(
                    MySqlErrors.ER_INVALID_GROUP_FUNC_USE, "HY000",
                    "窗口函数不能出现在 $where 里。窗口函数在 HAVING **之后**才求值" +
                        "（求值顺序：FROM → WHERE → GROUP BY → 聚合 → HAVING → 窗口函数 → SELECT → ORDER BY），" +
                        "写到 $where 里时它还没有值。要在结果上筛选，请把窗口函数放进子查询或 CTE",
                )
            }
        }
        check(stmt.where, "WHERE")
        check(stmt.having, "HAVING")
        stmt.groupBy.forEach { check(it, "GROUP BY") }
        stmt.joins.forEach { check(it.on, "JOIN ON") }
    }

    fun containsWindow(e: Expr): Boolean = when (e) {
        is WindowCall -> true
        else -> childrenOf(e).any { containsWindow(it) }
    }

    // ── 分区与排序 ────────────────────────────────────────────────

    /** 按 `PARTITION BY` 切成若干组；没有 `PARTITION BY` 时整批行就是一组。 */
    private fun partition(
        rows: List<List<Datum>>,
        aggsPerRow: List<Map<String, Datum>>?,
        schema: TableSchema,
        vars: Map<String, Datum>,
        call: WindowCall,
    ): Collection<List<Int>> {
        if (call.partitionBy.isEmpty()) return listOf(rows.indices.toList())
        val groups = LinkedHashMap<List<Datum>, MutableList<Int>>()
        for (i in rows.indices) {
            val key = call.partitionBy.map { Eval.eval(it, ctx(rows, aggsPerRow, schema, vars, i)) }
            groups.getOrPut(key) { ArrayList() }.add(i)
        }
        return groups.values
    }

    /**
     * 分区内按窗口自己的 `ORDER BY` 排序，返回**行号**列表。
     *
     * 排序是**稳定**的（Kotlin 的 `sortedWith` 保证）：键相同的行保持原来的相对顺序。
     * 这一点有用：`ROW_NUMBER() OVER (ORDER BY k)` 在 `k` 有重复时，序号会与
     * 扫描顺序一致，而不是随排序实现而变化。
     */
    private fun orderMembers(
        rows: List<List<Datum>>,
        aggsPerRow: List<Map<String, Datum>>?,
        schema: TableSchema,
        vars: Map<String, Datum>,
        call: WindowCall,
        members: List<Int>,
    ): List<Int> {
        if (call.orderBy.isEmpty()) return members
        fun compare(a: Int, b: Int): Int {
            for (oi in call.orderBy) {
                val av = Eval.eval(oi.expr, ctx(rows, aggsPerRow, schema, vars, a))
                val bv = Eval.eval(oi.expr, ctx(rows, aggsPerRow, schema, vars, b))
                val c = av.compareTo(bv)
                if (c != 0) return if (oi.ascending) c else -c
            }
            return 0
        }
        return members.sortedWith(Comparator { a, b -> compare(a, b) })
    }

    private fun ctx(
        rows: List<List<Datum>>,
        aggsPerRow: List<Map<String, Datum>>?,
        schema: TableSchema,
        vars: Map<String, Datum>,
        i: Int,
    ) = EvalContext(schema, rows[i], vars, aggsPerRow?.getOrNull(i) ?: emptyMap())

    // ── 求值 ──────────────────────────────────────────────────────

    private fun evaluate(
        call: WindowCall,
        sorted: List<Int>,
        rows: List<List<Datum>>,
        aggsPerRow: List<Map<String, Datum>>?,
        schema: TableSchema,
        vars: Map<String, Datum>,
        out: (Int, Datum) -> Unit,
    ) {
        val n = sorted.size
        fun ctxAt(pos: Int) = ctx(rows, aggsPerRow, schema, vars, sorted[pos])
        // 同键（peer）分组：`RANK` 与 `RANGE` 帧都按它算
        val peerStart = IntArray(n)
        val peerEnd = IntArray(n)
        run {
            var s = 0
            while (s < n) {
                var e = s
                while (e + 1 < n && sameOrderKeys(call, ctxAt(e), ctxAt(e + 1))) e++
                for (p in s..e) { peerStart[p] = s; peerEnd[p] = e }
                s = e + 1
            }
        }
        // 帧的上下界（含两端，已夹到分区范围内）。空帧用 lo > hi 表示。
        // 刻意不用 IntRange：`IntRange.EMPTY` 与 `.first` / `.last` 在类型推断上
        // 会绕一圈，而这里只是两个整数，直接写更清楚
        fun frameLo(p: Int): Int {
            val f = call.frame ?: return 0
            val rangeMode = f.unit == FrameUnit.RANGE
            val lo = when (val b = f.start) {
                is WindowBound.UnboundedPreceding -> 0
                is WindowBound.CurrentRow -> if (rangeMode) peerStart[p] else p
                is WindowBound.Preceding -> p - b.n.toInt()
                is WindowBound.Following -> p + b.n.toInt()
                is WindowBound.UnboundedFollowing -> n - 1
            }
            return maxOf(lo, 0)
        }

        fun frameHi(p: Int): Int {
            val f = call.frame
            val hi = if (f == null) {
                // 默认帧：有 ORDER BY 时是 RANGE UNBOUNDED PRECEDING → CURRENT ROW
                // （含**所有同键的行**，这正是"同一天的两行都算进累计"的由来）；
                // 没有 ORDER BY 时是整个分区
                if (call.orderBy.isEmpty()) n - 1 else peerEnd[p]
            } else {
                val rangeMode = f.unit == FrameUnit.RANGE
                when (val b = f.end) {
                    is WindowBound.UnboundedFollowing -> n - 1
                    is WindowBound.CurrentRow -> if (rangeMode) peerEnd[p] else p
                    is WindowBound.Following -> p + b.n.toInt()
                    is WindowBound.Preceding -> p - b.n.toInt()
                    is WindowBound.UnboundedPreceding -> 0
                }
            }
            return minOf(hi, n - 1)
        }

        when (call.name) {
            // ── 排名：只看位置，不看帧 ──────────────────────────────
            "ROW_NUMBER" -> for (p in 0 until n) out(sorted[p], Datum.I64((p + 1).toLong()))

            "RANK" -> for (p in 0 until n) out(sorted[p], Datum.I64((peerStart[p] + 1).toLong()))

            "DENSE_RANK" -> {
                var dense = 0
                var p = 0
                while (p < n) {
                    dense++
                    val end = peerEnd[p]
                    for (q in p..end) out(sorted[q], Datum.I64(dense.toLong()))
                    p = end + 1
                }
            }

            // 分位：MySQL 的定义是 (rank - 1) / (行数 - 1)，只有一行时定义为 0
            "PERCENT_RANK" -> for (p in 0 until n) {
                out(sorted[p], Datum.F64(if (n == 1) 0.0 else peerStart[p].toDouble() / (n - 1)))
            }

            // 累积分布：(排名 ≤ 当前行的行数) / 总行数，因此最后一行的值恒为 1
            "CUME_DIST" -> for (p in 0 until n) {
                out(sorted[p], Datum.F64((peerEnd[p] + 1).toDouble() / n))
            }

            // 分桶：先给前 (n mod k) 个桶各多放一行，其余均分
            "NTILE" -> {
                val k = call.args.singleOrNull()?.let { intArg(it, "NTILE", vars) } ?: 0L
                if (k < 1) throw badArgs("NTILE 的参数必须是正整数，收到 $k")
                val bucketSize = n / k
                val bigger = (n % k).toInt()          // 多一行的桶个数
                for (p in 0 until n) {
                    val bucket =
                        if (bucketSize == 0L) (p + 1).toLong()          // 桶比行还多：每行一个桶
                        else {
                            val cutoff = (bucketSize + 1) * bigger
                            if (p < cutoff) p / (bucketSize + 1) + 1
                            else (p - cutoff) / bucketSize + bigger + 1
                        }
                    out(sorted[p], Datum.I64(bucket))
                }
            }

            // ── 取值：从另一行取 ────────────────────────────────────
            "LAG", "LEAD" -> {
                val expr = call.args[0]
                val offset = call.args.getOrNull(1)?.let { intArg(it, call.name, vars) } ?: 1L
                val default = call.args.getOrNull(2)
                if (offset < 0) throw badArgs("${call.name} 的偏移量不能是负数：$offset")
                for (p in 0 until n) {
                    val target = if (call.name == "LAG") p - offset else p + offset
                    out(
                        sorted[p],
                        if (target < 0 || target >= n) {
                            // 越界给默认值；没写默认值时是 NULL
                            default?.let { Eval.eval(it, ctxAt(p)) } ?: Datum.Null
                        } else {
                            Eval.eval(expr, ctxAt(target.toInt()))
                        },
                    )
                }
            }

            // 帧的第一个 / 最后一个 / 第 n 个值
            "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE" -> {
                val expr = call.args[0]
                val nth = if (call.name == "NTH_VALUE") {
                    val v = call.args.getOrNull(1)?.let { intArg(it, "NTH_VALUE", vars) } ?: 0L
                    if (v < 1) throw badArgs("NTH_VALUE 的序号从 1 开始，收到 $v")
                    v
                } else 1L
                for (p in 0 until n) {
                    val lo = frameLo(p)
                    val hi = frameHi(p)
                    val pos = when (call.name) {
                        "FIRST_VALUE" -> lo
                        "NTH_VALUE" -> (lo + nth.toInt() - 1).takeIf { it <= hi } ?: -1
                        // ⚠️ LAST_VALUE 取的是**帧的末尾**，而默认帧的末尾就是当前行
                        // （或它的同键行）。所以 `LAST_VALUE(x) OVER (ORDER BY k)`
                        // 给的是"到目前为止最后一个值"，不是"整个分区的最后一个值"——
                        // 想要后者必须显式写 `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
                        else -> hi
                    }
                    // 空帧（lo > hi）里没有任何值可取：SQL 规定给 NULL
                    out(sorted[p], if (lo > hi || pos < 0) Datum.Null else Eval.eval(expr, ctxAt(pos)))
                }
            }

            // ── 聚合：在帧上归约（与不带 OVER 的语义完全一致）──────────
            else -> {
                val agg = asAggregate(call)
                val prefixFrame = call.frame?.start == WindowBound.UnboundedPreceding ||
                    (call.frame == null)     // 默认帧的起点恒为 UNBOUNDED PRECEDING
                if (prefixFrame && call.name != "GROUP_CONCAT") {
                    // ★ 增量：帧的左端固定为 0，右端随位置单调不减 ⇒ 每行只需"加进新来的行"
                    val acc = Accumulator(agg, schema, vars)
                    var added = 0
                    for (p in 0 until n) {
                        val hi = frameHi(p)
                        while (added <= hi) {
                            acc.add(rows[sorted[added]], aggsPerRow?.getOrNull(sorted[added]))
                            added++
                        }
                        out(sorted[p], acc.value())
                    }
                } else {
                    for (p in 0 until n) {
                        val lo = frameLo(p)
                        val hi = frameHi(p)
                        if (lo > hi) {
                            // 空帧：COUNT 给 0，其余给 NULL（与 Aggregates.compute 一致）
                            out(sorted[p], Aggregates.compute(agg, emptyList(), schema, vars, null))
                            continue
                        }
                        val window = (lo..hi).toList()
                        val frameRows = window.map { rows[sorted[it]] }
                        val frameAggs = window.map { aggsPerRow?.getOrNull(sorted[it]) ?: emptyMap() }
                        out(sorted[p], Aggregates.compute(agg, frameRows, schema, vars, frameAggs))
                    }
                }
            }
        }
    }

    private fun sameOrderKeys(call: WindowCall, a: EvalContext, b: EvalContext): Boolean {
        for (oi in call.orderBy) {
            if (Eval.eval(oi.expr, a).compareTo(Eval.eval(oi.expr, b)) != 0) return false
        }
        return true
    }

    /** 把窗口调用还原成"同一个聚合函数"的表达式，好复用 [Aggregates.compute]。 */
    private fun asAggregate(call: WindowCall): Expr = when (call.name) {
        "COUNT" -> CountCall(call.args.firstOrNull()?.takeIf { it !is Star }, call.distinct)
        else -> AggCall(call.name, call.args.single(), call.distinct)
    }

    /**
     * **增量累加器**：帧的左端固定为 0 时，一路把行加进来，随时给出当前值。
     *
     * 它必须与 [Aggregates.compute] 在同一个帧上**给出同一个值**——两条路径只是
     * 算法不同（一个是折叠、一个是重算），语义必须完全一致。
     * 这个不变量由 `SqlSurfaceTest.增量与重算两条路径结果一致` 逐值比对守着。
     *
     * 容易写错的两处，都在这里按"与 compute 一致"写：
     * - `SUM` 的类型：`compute` 看的是**整个**帧里有没有非整数。增量版本不能
     *   "先按整数加、遇到小数再改"，因为整数加法可能已经溢出了——
     *   所以两条累加**同时**进行，最后再按"有没有见过非整数"选一条。
     * - 空帧：`COUNT` 给 0，其余给 NULL（与 `compute` 一致，见那里的说明）。
     */
    private class Accumulator(
        private val agg: Expr,
        private val schema: TableSchema,
        private val vars: Map<String, Datum>,
    ) {
        private val star = agg is CountCall && agg.arg == null
        private val arg = when (agg) {
            is CountCall -> agg.arg
            is AggCall -> agg.arg
            else -> null
        }
        private val distinct = when (agg) {
            is CountCall -> agg.distinct
            is AggCall -> agg.distinct
            else -> false
        }
        private val seen: HashSet<Datum>? = if (distinct) HashSet() else null

        private var rows = 0L          // 帧里的行数（COUNT(*) 用）
        private var nonNull = 0L       // 非 NULL 值的个数（COUNT(x) / AVG 用）
        private var intAcc = 0L
        private var overflowed = false
        private var doubleAcc = 0.0
        private var sawNonInt = false
        private var minVal: Datum? = null
        private var maxVal: Datum? = null

        fun add(row: List<Datum>, aggs: Map<String, Datum>?) {
            rows++
            if (star) return
            val v = Eval.eval(arg!!, EvalContext(schema, row, vars, aggs ?: emptyMap()))
            if (v is Datum.Null) return
            if (seen != null && !seen.add(v)) return    // DISTINCT：同一个值只算一次
            nonNull++
            when (agg) {
                is AggCall -> when (agg.name) {
                    "SUM" -> {
                        if (v is Datum.I64 && !sawNonInt) {
                            try {
                                intAcc = Math.addExact(intAcc, v.v)
                            } catch (_: ArithmeticException) {
                                overflowed = true
                            }
                        } else {
                            sawNonInt = true
                        }
                        doubleAcc += numericOf(v)
                    }
                    "AVG" -> doubleAcc += numericOf(v)
                    "MIN" -> if (minVal == null || v.compareTo(minVal!!) < 0) minVal = v
                    "MAX" -> if (maxVal == null || v.compareTo(maxVal!!) > 0) maxVal = v
                    else -> throw SqlException(
                        MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                        "窗口函数暂不支持 ${agg.name}（支持：${NAMES.sorted().joinToString(", ")}）",
                    )
                }
                else -> Unit
            }
        }

        fun value(): Datum = when (agg) {
            is CountCall -> Datum.I64(if (star) rows else nonNull)
            is AggCall -> when (agg.name) {
                "SUM" -> when {
                    nonNull == 0L -> Datum.Null
                    overflowed && !sawNonInt -> throw SqlException(
                        MySqlErrors.ER_DATA_OUT_OF_RANGE, "22003",
                        "SUM 的结果超出 BIGINT 范围。本系统没有 DECIMAL 累加类型，" +
                            "溢出时明确报错而不是回绕——回绕会给出一个看起来很正常的错误数字",
                    )
                    sawNonInt -> Datum.F64(doubleAcc)
                    else -> Datum.I64(intAcc)
                }
                "AVG" -> if (nonNull == 0L) Datum.Null else Datum.F64(doubleAcc / nonNull)
                "MIN" -> minVal ?: Datum.Null
                "MAX" -> maxVal ?: Datum.Null
                else -> throw IllegalStateException("不该走到这里：${agg.name}")
            }
            else -> throw IllegalStateException("不该走到这里")
        }

        private fun numericOf(d: Datum): Double = Aggregates.numeric(d)
    }

    // ── 校验 ──────────────────────────────────────────────────────

    private fun validate(call: WindowCall) {
        if (call.name !in NAMES) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                "不支持的窗口函数 ${call.name}。支持的有：${NAMES.sorted().joinToString(", ")}",
            )
        }
        if (call.name in NEEDS_ORDER && call.orderBy.isEmpty()) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                "${call.name}() 需要窗口里有 ORDER BY：`${call.name}(...) OVER (ORDER BY ...)`。" +
                    "没有顺序时它问的「第几行 / 上一行」本身没有定义——" +
                    "按扫描顺序给出答案看似正常，换一个执行计划就变了",
            )
        }
        val want = when (call.name) {
            "ROW_NUMBER", "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST" -> 0
            "NTILE", "FIRST_VALUE", "LAST_VALUE" -> 1
            "NTH_VALUE" -> 2
            "LAG", "LEAD" -> -1     // 1..3 个参数
            else -> 1               // SUM/AVG/MIN/MAX
        }
        val n = call.args.size
        val ok = when (want) {
            -1 -> n in 1..3
            else -> n == want
        }
        if (!ok) {
            throw badArgs(
                "${call.name}() 的参数个数不对（给了 $n 个）。" +
                    when (call.name) {
                        "LAG", "LEAD" -> "应为 LAG(表达式[, 偏移量[, 默认值]])"
                        "NTH_VALUE" -> "应为 NTH_VALUE(表达式, 序号)"
                        else -> "应为 ${call.name}(${if (want == 0) "" else "表达式"})"
                    },
            )
        }
    }

    private fun intArg(e: Expr, fn: String, vars: Map<String, Datum>): Long {
        val v = Eval.eval(e, EvalContext(null, null, vars))
        return (v as? Datum.I64)?.v
            ?: throw badArgs("$fn 的这个参数必须是整数字面量，收到 $v")
    }

    private fun badArgs(msg: String) =
        SqlException(MySqlErrors.ER_WRONG_ARGUMENTS, "HY000", msg)
}

/** 窗口表达式的规范化键（= [WindowCall.toString]），同一个窗口只算一次。 */
fun windowKey(e: WindowCall): String = e.toString()
