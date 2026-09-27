package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema

/**
 * 连接（`JOIN` / `LEFT JOIN` / `RIGHT JOIN` / `CROSS JOIN` / 逗号连接）。
 *
 * ## 为什么连接要单独一层
 *
 * 其余所有查询都是**在一个 schema 的行上**求值：列按下标取，`EvalContext` 拿着
 * 一张 `TableSchema` 就够。连接把这件事变了——一行由**多个表的列拼起来**，
 * `a.id` 与 `b.id` 是两个不同的位置，而未限定的 `id` 可能**有歧义**。
 *
 * 最省事也最危险的做法是"把两边的列名拼在一起、重名的后者覆盖前者"：
 * 那会让 `SELECT id FROM a JOIN b ON a.id=b.aid` **静默**取到 `b.id`。
 * 那是一个完全合法、结果却错的查询——所以这里把"歧义"当成**错误**（1052）报出来。
 *
 * ## 做法
 *
 * 合成一张**连接 schema**（列名带限定前缀 `a.id`，并额外登记"唯一的裸列名 → 同一列"），
 * 于是连接之后的全部环节（`WHERE` / `GROUP BY` / 聚合 / `ORDER BY` / 投影）
 * 都在这张 schema 上按普通单表查询处理——**不需要第二套求值器**。
 *
 * ## 为什么是哈希连接而不是双层循环
 *
 * 双层循环的连接代价是左右行数之积：两张一万行的表 = 一亿次比较与一亿个中间行分配，
 * 而那在"读多写少的小应用"里也足够把一次查询拖到不可用。等值连接占实际连接的绝大多数，
 * 所以这里对等值键建哈希表；**非等值条件仍然回退到双层循环**——
 * 那条路慢，但它正确，而且 `EXPLAIN` 会如实说用的是哪一种。
 */
object JoinEngine {

    /** 一个数据来源。[offset] 是它在合成行里的起始下标。 */
    class Source(
        val name: String,
        /** `FROM t AS x` 里的 `x`；没有别名时等于裸表名。 */
        val qualifier: String,
        val schema: TableSchema,
        val offset: Int,
    ) {
        val width: Int get() = schema.columns.size
    }

    /** 一次连接查询的"形状"：合成 schema + 各来源的列区间。 */
    class Joined(
        val schema: TableSchema,
        val sources: List<Source>,
    ) {
        /**
         * 最近一步连接实际用的策略：`hash` 或 `nested`。
         *
         * 记下来是为了让 `EXPLAIN` **说实话**（这些连接里有没有用上哈希），
         * 以及让测试能钉住它——等值连接退化成双层循环在数据量小的时候
         * 结果完全一样，只有把"用了哪种策略"暴露出来才测得到。
         */
        @Volatile
        var lastStrategy: String = "nested"
            internal set

        /** 合成行里某个来源的列区间（左闭右开，用 `until`）。 */
        fun sliceOf(s: Source): IntRange = s.offset until (s.offset + s.width)

        /** 真实列数 = 各来源宽度之和（合成 schema 里还多了一份"裸名映射"，不是列）。 */
        val realWidth: Int get() = sources.sumOf { it.width }

        /** 某个来源是第几个（0 = 首个表）。 */
        fun indexOfSource(s: Source): Int = sources.indexOf(s)
    }

    /**
     * 合成连接 schema。
     *
     * 列名带限定前缀；**唯一的**裸列名通过 [TableSchema.extraNames] 指向同一列
     * （而不是再加一份列定义——那样两处会拿到不同的下标，其中一处永远是 NULL）。
     */
    fun schemaOf(
        primary: TableSchema,
        primaryQualifier: String,
        joins: List<Pair<JoinClause, TableSchema>>,
    ): Joined {
        val cols = ArrayList<ColumnDef>()
        val sources = ArrayList<Source>()
        val bareCount = HashMap<String, Int>()
        val all = listOf(primaryQualifier to primary) +
            joins.map { (j, t) -> (j.alias ?: bareName(j.table)) to t }

        for ((qualifier, t) in all) {
            sources.add(Source(t.name, qualifier, t, cols.size))
            for (c in t.columns) {
                cols.add(c.copy(name = "${qualifier.lowercase()}.${c.name.lowercase()}"))
                val bare = c.name.lowercase()
                bareCount[bare] = (bareCount[bare] ?: 0) + 1
            }
        }
        // 唯一定义的裸列名 → 该列的**真实下标**。重名的不登记，
        // 于是它们只会走 1052（由 validateRefs 先一步报出），不会悄悄取到某一边
        val extra = HashMap<String, Int>()
        for ((i, c) in cols.withIndex()) {
            val bare = c.name.substringAfter('.')
            if (bareCount[bare] == 1) extra[bare] = i
        }
        val merged = TableSchema(
            tableId = primary.tableId,
            databaseId = primary.databaseId,
            name = primary.name,
            version = primary.version,
            columns = cols,
            primaryKey = primary.primaryKey,
            extraNames = extra,
        )
        return Joined(merged, sources)
    }

    /**
     * 执行连接。
     *
     * 顺序：`ON` → 外连接补 NULL → 之后才是 `WHERE`。
     * 这个顺序不能颠倒：`ON` 决定"哪一对算匹配"，`WHERE` 决定"匹配完的行要不要"。
     * 混在一起的话外连接会退化成内连接（未匹配的行被 `WHERE` 滤掉了），
     * 而结果看起来完全正常。
     */
    fun join(
        joined: Joined,
        leftRows: List<List<Datum>>,
        rightRows: List<List<List<Datum>>>,
        joinTypes: List<JoinType>,
        onPredicates: List<Expr?>,
        vars: Map<String, Datum>,
        /** 每一步反连接的 NULL 语义（`NOT EXISTS` 与 `NOT IN` 不同，见 [AntiSemantics]）。 */
        antiSemantics: List<AntiSemantics> = emptyList(),
    ): List<List<Datum>> {
        var acc = leftRows
        for (i in rightRows.indices) {
            val right = rightRows[i]
            acc = joinOne(
                joined, i, acc, right, joinTypes[i], onPredicates[i], vars,
                antiSemantics.getOrNull(i) ?: AntiSemantics.EXISTS,
            )
        }
        return acc
    }

    private fun joinOne(
        joined: Joined,
        step: Int,
        left: List<List<Datum>>,
        right: List<List<Datum>>,
        type: JoinType,
        on: Expr?,
        vars: Map<String, Datum>,
        antiSemantics: AntiSemantics = AntiSemantics.EXISTS,
    ): List<List<Datum>> {
        val rightSource = joined.sources[step + 1]
        val leftWidth = rightSource.offset
        val out = ArrayList<List<Datum>>()
        val matchedRight = HashSet<Int>()

        val keys = if (on == null) emptyList() else equiKeys(joined, step, on)
        val useHash = keys.isNotEmpty()
        joined.lastStrategy = if (useHash) "hash" else "nested"

        // `NOT IN` 的 NULL 规则需要知道"被比较的是哪一列"，而它**不是**关联列：
        // `u.name NOT IN (SELECT o.tag FROM orders o WHERE o.user_id = u.id)` 里
        // 关联列是 user_id、被比较的是 tag。约定：等值键里**最后一个是**被比较值的那一对，
        // 前面的都是关联键（由 [CorrelatedSubqueries.toJoinClause] 按这个顺序拼出来）。
        val needNotIn = type == JoinType.ANTI && antiSemantics == AntiSemantics.NOT_IN
        val corrKeyCount = if (needNotIn) (keys.size - 1).coerceAtLeast(0) else 0
        val comparedInner: ((List<Datum>) -> Datum)? =
            if (needNotIn && keys.isNotEmpty()) keys.last().second else null
        // 每一相关组里"被比较的值有没有 NULL"——**按组**算一次，不是每行扫一遍内表。
        // 同时要知道**这个组存不存在**：组不存在 = 内层是空集，
        // 而 `x NOT IN (空集)` 恒为**真**（没有值等于 x，也没有 NULL 让它未知）。
        // 少了这一条，外层值为 NULL 且内层没有匹配行时会被判成"未知"而丢掉这一行——
        // 少返回行的错误，而且只在"值为 NULL 且恰好没有关联行"时出现
        val nullByCorrGroup: Map<List<Datum>, Boolean> = if (needNotIn && comparedInner != null) {
            val corrKeys = keys.take(corrKeyCount)
            right.groupBy { r -> corrKeys.map { k -> k.second(r) } }
                .mapValues { (_, rows) -> rows.any { comparedInner(it) is Datum.Null } }
        } else {
            emptyMap()
        }

        // 右表按连接键建哈希。**键里有 NULL 的行不参与匹配**：
        // SQL 的 `=` 在 NULL 上既不为真也不为假，把 NULL 当普通键值会让两行 NULL
        // 互相匹配上——那是外连接最容易出的错，且结果看起来很正常
        val index: HashMap<List<Datum>, MutableList<Int>>? = if (useHash) {
            val m = HashMap<List<Datum>, MutableList<Int>>()
            for ((ri, r) in right.withIndex()) {
                val k = keys.map { it.second(r) }
                if (k.any { it is Datum.Null }) continue
                m.getOrPut(k) { ArrayList() }.add(ri)
            }
            m
        } else null

        for (l in left) {
            var matched = false
            /** `NOT IN` 用：本行匹配到的那些右行里，比较列是否为 NULL。 */
            var matchedNull = false
            fun emit(r: List<Datum>, ri: Int) {
                val combined = l + r
                // 哈希键只是**候选集的筛选**：完整的 ON 仍然要求值一次，
                // 因为 ON 里可能还有等值之外的额外条件（`ON a.k=b.k AND a.x>b.y`）
                if (on == null || Eval.truthy(Eval.eval(on, EvalContext(joined.schema, combined, vars)))) {
                    // 半/反连接**不输出拼接行**：它们只关心"这一左行有没有匹配"，
                    // 输出的是左行本身（由下面的分支补）。在这里顺手 add 的话，
                    // SEMI 会既输出拼接行又输出左行——行数与列数都会翻倍
                    if (!type.isSemiOrAnti) out.add(combined)
                    matched = true
                    matchedRight.add(ri)
                }
            }
            if (index != null) {
                val k = keys.map { it.first(l) }
                if (needNotIn && comparedInner != null) {
                    // ★ `NOT IN` 的三值规则（三处，缺一处就会多返回一批行）：
                    //   ① 外层的被比较值是 NULL → `NULL IN (...)` 是未知 → 不成立
                    //   ② 本**相关组**里出现过 NULL → 找不到时也是未知 → 不成立
                    //   ③ 组内找到了相等的值 → `IN` 为真 → `NOT IN` 不成立
                    val outerValue = k.lastOrNull()
                    val corrKey = k.take(corrKeyCount)
                    // 内层这个相关组**存在**吗？不存在就是空集，`NOT IN (空集)` 为真
                    val groupExists = nullByCorrGroup.containsKey(corrKey)
                    if (groupExists && (outerValue is Datum.Null || nullByCorrGroup[corrKey] == true)) {
                        matchedNull = true
                    }
                    if (outerValue !is Datum.Null) index[k]?.forEach { ri -> emit(right[ri], ri) }
                } else if (k.none { it is Datum.Null }) {
                    index[k]?.forEach { ri -> emit(right[ri], ri) }
                }
            } else {
                for ((ri, r) in right.withIndex()) emit(r, ri)
            }
            // 半/反连接**只输出左行**（一次），右表只用来判断"有没有匹配"
            if (type.isSemiOrAnti) {
                if (type == JoinType.SEMI) {
                    if (matched) out.add(l)
                } else {
                    // 反连接：没有任何匹配时才输出。
                    // `NOT IN` 还要额外看"匹配行里的比较值有没有 NULL"——
                    // 有的话整个子查询是"未知"，一行都不能返回（见 AntiSemantics）
                    // 反连接：没有匹配时才输出；`NOT IN` 的"未知"由 matchedNull 表示
                    val blockedByNull = antiSemantics == AntiSemantics.NOT_IN && matchedNull
                    if (!matched && !blockedByNull) out.add(l)
                }
                continue
            }
            if (!matched && type == JoinType.LEFT) out.add(l + List(rightSource.width) { Datum.Null })
        }
        if (type == JoinType.RIGHT) {
            for ((ri, r) in right.withIndex()) {
                if (ri !in matchedRight) out.add(List(leftWidth) { Datum.Null } + r)
            }
        }
        return out
    }

    /**
     * 从 `ON` 里抽出等值连接键：`(左侧取值, 右侧取值)` 的列表。
     *
     * 只认"一侧完全属于左边、另一侧完全属于右边"的等值（复合键就是多个这样的等式
     * 由 `AND` 连起来）。带函数的、跨边的（`a.x = b.y + 1`）一律不认——
     * 那些回退到双层循环，结果一样正确，只是慢。
     */
    private fun equiKeys(joined: Joined, step: Int, on: Expr): List<Pair<(List<Datum>) -> Datum, (List<Datum>) -> Datum>> {
        val out = ArrayList<Pair<(List<Datum>) -> Datum, (List<Datum>) -> Datum>>()
        val rightSource = joined.sources[step + 1]
        val rightRange = joined.sliceOf(rightSource)

        /**
         * 列引用 → 合成行里的下标。
         *
         * ⚠️ **必须先按限定名找**（`u.id`），找不到才退回裸名。
         * 两张表都有 `id` 时裸名是"歧义列"、**故意没有登记**，直接用裸名会解析失败——
         * 而解析失败在这里的表现是"这一对等值被悄悄跳过"，于是哈希表是空的、
         * 连接退化成双层循环、`NOT IN` 的三值规则也整套失效（实测踩到：
         * 用例返回了全部三行，看起来像规则写错了，实际是键根本没建起来）。
         */
        fun colIndex(c: ColumnRef): Int? =
            c.table?.let { joined.schema.indexOfOrNull("$it.${c.name}") }
                ?: joined.schema.indexOfOrNull(c.name)

        fun sideOf(e: Expr): Int? {
            // 0 = 左侧（已累积的部分），1 = 右侧（本步新加入的表）
            val idx = (e as? ColumnRef)?.let { colIndex(it) } ?: return null
            return if (idx in rightRange) 1 else 0
        }

        for (c in conjunctsOf(on)) {
            val eq = c as? BinaryOp ?: continue
            if (eq.op != "=") continue
            val l = sideOf(eq.left)
            val r = sideOf(eq.right)
            if (l == null || r == null || l == r) continue
            // 两侧都必须是"单个列"：组合表达式不能直接建哈希键
            val leftExpr = if (l == 0) eq.left else eq.right
            val rightExpr = if (l == 0) eq.right else eq.left
            if (leftExpr !is ColumnRef || rightExpr !is ColumnRef) continue
            val li = colIndex(leftExpr) ?: continue
            val ri = colIndex(rightExpr) ?: continue
            // ⚠️ 两个取值器作用在**不同的行**上，索引必须各自按行的局部坐标算：
            //   - 左侧取值器作用在「已累积的连接行」上，而它的布局与合成 schema 的
            //     前缀一致 → 直接用 li；
            //   - 右侧取值器作用在**内表自己的行**上（宽度 = 内表列数）→ 必须减掉
            //     它在合成行里的起始偏移。
            //
            // 不减偏移的后果不是"结果错"而是"**什么都匹配不上**"：右表的行没有那么多列，
            // `getOrNull(ri)` 全是 NULL，于是哈希桶里一条记录都没有、连接退化成
            // 空结果（实测踩到：内连接返回 0 行）。而在此之前，因为 `sideOf` 解析不了
            // 歧义列名、整对键被跳过，这条路径**从来没有真正跑过**——
            // 所有连接其实一直是双层循环，正确但慢。
            val rightLocal = ri - rightSource.offset
            require(rightLocal >= 0) { "连接键的内层列下标越界（$ri < ${rightSource.offset}）" }
            out.add(
                { row: List<Datum> -> row.getOrNull(li) ?: Datum.Null } to
                    { row: List<Datum> -> row.getOrNull(rightLocal) ?: Datum.Null },
            )
        }
        return out
    }

    /**
     * 只做**策略判定**（建不建得起等值哈希键），不取数。
     *
     * 给 `EXPLAIN` 用：它要如实写"这一步用的是哈希还是双层循环"。
     * 不真的算一遍键，那一列就只能靠猜——而猜出来的计划说明等于没有。
     */
    fun planStrategy(joined: Joined, step: Int, on: Expr?) {
        joined.lastStrategy = if (on != null && equiKeys(joined, step, on).isNotEmpty()) "hash" else "nested"
    }

    /** `AND` 合取项。 */
    private fun conjunctsOf(e: Expr): List<Expr> = when {
        e is BinaryOp && e.op == "AND" -> conjunctsOf(e.left) + conjunctsOf(e.right)
        else -> listOf(e)
    }

    /**
     * 列引用的**语义检查**：歧义列报 1052、未知列报 1054、未知表报 1053。
     *
     * 为什么必须显式做：合成 schema 里重名的裸列名是**故意不登记**的，
     * 于是 `indexOf` 对它们只会说"Unknown column"。而真正的问题是"这一列有歧义"——
     * 两者的排查方向完全不同，报错必须说清楚是哪一种。
     */
    fun validateRefs(joined: Joined, stmt: SelectStmt) {
        val bareCount = HashMap<String, Int>()
        for (s in joined.sources) {
            for (c in s.schema.columns) {
                val k = c.name.lowercase()
                bareCount[k] = (bareCount[k] ?: 0) + 1
            }
        }
        val qualifiers = joined.sources.map { it.qualifier.lowercase() }.toSet()

        fun check(e: Expr?, where: String) {
            if (e == null) return
            if (e is ColumnRef) {
                val q = e.table?.lowercase()
                if (q != null) {
                    val s = joined.sources.firstOrNull { it.qualifier.lowercase() == q }
                        ?: throw SqlException(
                            MySqlErrors.ER_BAD_TABLE_ERROR, "42S02",
                            "Unknown table '$q' in $where（可用的表别名：" +
                                joined.sources.joinToString(", ") { it.qualifier } + "）",
                        )
                    if (s.schema.columns.none { it.name.equals(e.name, ignoreCase = true) }) {
                        throw SqlException(
                            MySqlErrors.ER_BAD_FIELD_ERROR, "42S22",
                            "Unknown column '${e.name}' in $where（表 '${s.qualifier}' 里没有这一列）",
                        )
                    }
                    return
                }
                when (bareCount[e.name.lowercase()] ?: 0) {
                    0 -> throw SqlException(
                        MySqlErrors.ER_BAD_FIELD_ERROR, "42S22",
                        "Unknown column '${e.name}' in $where",
                    )
                    1 -> return
                    else -> throw SqlException(
                        MySqlErrors.ER_NON_UNIQ_ERROR, "23000",
                        "Column '${e.name}' in $where is ambiguous：多张表里都有这一列，" +
                            "请写成 表名.列名（可用的表别名：" +
                            joined.sources.joinToString(", ") { it.qualifier } + "）",
                    )
                }
            }
            childrenOf(e).forEach { check(it, where) }
        }

        stmt.projections.forEach { if (it is SelectItem.Field) check(it.expr, "field list") }
        check(stmt.where, "where clause")
        stmt.joins.forEach { check(it.on, "on clause") }
        val aliases = aliasMapOf(stmt)
        stmt.groupBy.forEach { check(resolveAliases(it, aliases), "group statement") }
        stmt.having?.let { check(resolveAliases(it, aliases), "having clause") }
        stmt.orderBy.forEach { check(resolveAliases(it.expr, aliases), "order clause") }
    }

    /**
     * `SELECT *` 的输出列名。
     *
     * 合成 schema 里的名字带限定前缀（`a.id`），那是**内部表示**：
     * 直接发给客户端的话，`rs.getString("id")` 会全部失败——而 MySQL 返回裸列名。
     * 两个来源重名时 MySQL 允许结果集里出现两个同名列定义，这里照做
     * （客户端按序号取即可）。
     */
    fun outputNames(joined: Joined): List<String> =
        joined.sources.flatMap { s -> s.schema.columns.map { it.name } }

    private fun bareName(qualified: String): String = qualified.substringAfterLast('.')
}
