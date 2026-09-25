package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 连接的**纯逻辑**测试（不需要引擎）。
 *
 * 连接有三类错误是"不报错、只给错结果"的，这组用例主要就盯它们：
 *
 * 1. **歧义列**：`SELECT id FROM a JOIN b ON ...` 里两张表都有 `id`，
 *    随手取一边会得到一个完全合法、结果却错的查询。
 * 2. **外连接补 NULL**：漏掉"未匹配的行也要输出"，外连接就退化成了内连接——
 *    少几行，而 SQL 层看起来正常。
 * 3. **NULL 键**：SQL 的 `=` 在 NULL 上既不为真也不为假，把 NULL 当普通键值
 *    会让两行 NULL 互相匹配上。
 */
class JoinTest {

    private val orders = TableSchema(
        tableId = 1, databaseId = 1, name = "orders", version = 1,
        columns = listOf(
            ColumnDef("id", ColumnType.BIGINT),
            ColumnDef("user_id", ColumnType.BIGINT),
            ColumnDef("amount", ColumnType.BIGINT),
        ),
        primaryKey = listOf("id"),
    )

    private val users = TableSchema(
        tableId = 2, databaseId = 1, name = "users", version = 1,
        columns = listOf(
            ColumnDef("id", ColumnType.BIGINT),
            ColumnDef("name", ColumnType.VARCHAR),
        ),
        primaryKey = listOf("id"),
    )

    // 两侧都有的列名是 `id` —— 正是歧义用例要的
    private fun joined(vararg joins: Pair<JoinClause, TableSchema>) =
        JoinEngine.schemaOf(orders, "o", joins.toList())

    /** 用**真正的解析器**解析出一个 ON 表达式：测试里不另造一条解析入口。 */
    private fun onExpr(expr: String): Expr =
        (Parser.parse("SELECT 1 FROM a JOIN b ON $expr") as SelectStmt).joins[0].on!!

    private fun onJoin(type: JoinType = JoinType.INNER, on: String = "o.user_id = u.id") =
        joined(JoinClause("users", "u", type, onExpr(on)) to users)

    /** 订单：3 号订单的 user_id 是 NULL（谁都匹配不上），4 号订单的用户不存在。 */
    private val orderRows = listOf(
        listOf(Datum.I64(1), Datum.I64(10), Datum.I64(100)),
        listOf(Datum.I64(2), Datum.I64(11), Datum.I64(200)),
        listOf(Datum.I64(3), Datum.Null, Datum.I64(300)),
        listOf(Datum.I64(4), Datum.I64(99), Datum.I64(400)),
    )
    private val userRows = listOf(
        listOf(Datum.I64(10), Datum.Str("alice")),
        listOf(Datum.I64(11), Datum.Str("bob")),
        listOf(Datum.I64(12), Datum.Str("carol")),
    )

    private fun run(
        j: JoinEngine.Joined,
        type: JoinType,
        on: Expr?,
        left: List<List<Datum>> = orderRows,
        right: List<List<Datum>> = userRows,
    ): List<List<Datum>> = JoinEngine.join(j, left, listOf(right), listOf(type), listOf(on), emptyMap())

    // ── 合成 schema ────────────────────────────────────────────────

    @Test
    fun `限定列名各自独立`() {
        val j = onJoin()
        val oid = j.schema.indexOf("o.id")
        val uid = j.schema.indexOf("u.id")
        assertTrue(oid != uid, "两张表的 id 必须是两个不同的位置")
        assertEquals(0, oid)
        assertEquals(3, uid)
    }

    @Test
    fun `唯一的裸列名指向同一列而不是新列`() {
        val j = onJoin()
        // `amount` 只在 orders 里有 → 可用；而且必须与 `o.amount` 是**同一个下标**
        assertEquals(j.schema.indexOf("o.amount"), j.schema.indexOf("amount"))
        assertEquals(2, j.schema.indexOf("amount"))
        // `name` 只在 users 里
        assertEquals(j.schema.indexOf("u.name"), j.schema.indexOf("name"))
    }

    @Test
    fun `重名的裸列名不登记`() {
        val j = onJoin()
        // `id` 两张表都有 → 裸名不可用。登记了的话就会悄悄命中其中一个
        assertNull(j.schema.indexOfOrNull("id"), "歧义列不该出现在 schema 里")
        assertFailsWith<SqlException> { j.schema.indexOf("id") }
    }

    @Test
    fun `真实列宽是各来源之和`() {
        val j = onJoin()
        assertEquals(5, j.realWidth, "3 列 + 2 列")
    }

    // ── 引用检查 ──────────────────────────────────────────────────

    @Test
    fun `歧义列报 1052`() {
        val e = assertFailsWith<SqlException> {
            JoinEngine.validateRefs(
                onJoin(),
                Parser.parse("SELECT id FROM orders o JOIN users u ON o.user_id = u.id") as SelectStmt,
            )
        }
        assertEquals(MySqlErrors.ER_NON_UNIQ_ERROR, e.errorCode)
        assertTrue(e.message!!.contains("ambiguous"), "应明确说是歧义：${e.message}")
    }

    @Test
    fun `限定写法就不歧义`() {
        JoinEngine.validateRefs(
            onJoin(),
            Parser.parse("SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id") as SelectStmt,
        )
    }

    @Test
    fun `未知列报 1054`() {
        val e = assertFailsWith<SqlException> {
            JoinEngine.validateRefs(
                onJoin(),
                Parser.parse("SELECT o.nope FROM orders o JOIN users u ON o.user_id = u.id") as SelectStmt,
            )
        }
        assertEquals(MySqlErrors.ER_BAD_FIELD_ERROR, e.errorCode)
    }

    @Test
    fun `未知表别名报 1053`() {
        val e = assertFailsWith<SqlException> {
            JoinEngine.validateRefs(
                onJoin(),
                Parser.parse("SELECT x.id FROM orders o JOIN users u ON o.user_id = u.id") as SelectStmt,
            )
        }
        assertEquals(MySqlErrors.ER_BAD_TABLE_ERROR, e.errorCode)
    }

    // ── 连接语义 ──────────────────────────────────────────────────

    @Test
    fun `内连接只保留两边都匹配的行`() {
        val j = onJoin()
        val out = run(j, JoinType.INNER, onExpr("o.user_id = u.id"))
        assertEquals(2, out.size, "只有订单 1、2 能匹配上用户")
        assertEquals(Datum.Str("alice"), out[0][4])
        assertEquals(Datum.Str("bob"), out[1][4])
    }

    @Test
    fun `NULL 键不匹配任何行`() {
        val j = onJoin()
        val out = run(j, JoinType.INNER, onExpr("o.user_id = u.id"))
        assertTrue(out.none { it[0] == Datum.I64(3) }, "user_id 为 NULL 的订单不能匹配上任何用户")
    }

    @Test
    fun `左连接保留未匹配的左行并补 NULL`() {
        val j = onJoin()
        val out = run(j, JoinType.LEFT, onExpr("o.user_id = u.id"))
        assertEquals(4, out.size, "左表四行都要在")
        // 订单 3（NULL 键）与订单 4（用户不存在）的用户列必须是 NULL
        val o3 = out.first { it[0] == Datum.I64(3) }
        assertEquals(Datum.Null, o3[3], "未匹配时右表的列补 NULL")
        assertEquals(Datum.Null, o3[4])
        val o4 = out.first { it[0] == Datum.I64(4) }
        assertEquals(Datum.Null, o4[4])
    }

    @Test
    fun `右连接保留未匹配的右行并在左侧补 NULL`() {
        val j = onJoin()
        val out = run(j, JoinType.RIGHT, onExpr("o.user_id = u.id"))
        assertEquals(3, out.size, "用户 12（carol）没有任何订单，也要出现")
        val carol = out.first { it[4] == Datum.Str("carol") }
        assertEquals(Datum.Null, carol[0], "未匹配时左表的列补 NULL")
        assertEquals(Datum.Null, carol[2])
    }

    @Test
    fun `笛卡尔积`() {
        val j = onJoin()
        val out = run(j, JoinType.CROSS, null)
        assertEquals(12, out.size, "4 × 3")
    }

    /** `ON` 里除等值之外还有额外条件时，候选集之外的行仍要被过滤掉。 */
    @Test
    fun `ON 的残余条件仍然生效`() {
        val j = onJoin()
        val on = onExpr("o.user_id = u.id AND o.amount > 150")
        val out = run(j, JoinType.INNER, on)
        assertEquals(1, out.size, "只有订单 2（200 > 150）满足")
        assertEquals(Datum.I64(2), out[0][0])
    }

    /** 复合连接键：两个等值条件都要参与哈希。 */
    @Test
    fun `复合连接键`() {
        val j = joined(JoinClause("users", "u", JoinType.INNER, null) to users)
        val on = onExpr("o.user_id = u.id AND o.id = u.id")
        val out = run(j, JoinType.INNER, on)
        // 只有 id 同时等于 user_id 且等于 users.id 的订单能匹配 → 没有这种数据
        assertEquals(0, out.size)
    }

    @Test
    fun `非等值连接回退到双层循环但结果正确`() {
        val j = onJoin()
        val out = run(j, JoinType.INNER, onExpr("o.user_id < u.id"))
        // 订单1(10) < 11,12；订单2(11) < 12；订单4(99) < 无
        assertEquals(3, out.size)
    }

    @Test
    fun `left join 时右表为空仍输出左表所有行`() {
        val j = onJoin()
        val out = run(j, JoinType.LEFT, onExpr("o.user_id = u.id"), right = emptyList())
        assertEquals(4, out.size, "右表空时左连接必须输出左表每一行（补 NULL），这是它与内连接的唯一差别")
        assertTrue(out.all { it[3] == Datum.Null })
    }

    // ── 半 / 反连接（相关子查询的落地形态）──────────────────────

    private fun semiJoin(type: JoinType, on: String) =
        joined(JoinClause("users", "u", type, onExpr(on)) to users)

    /**
     * ★ 半连接**只输出左行一次**——这是 `EXISTS` 与内连接的分界。
     *
     * 用内连接实现 `EXISTS`，一个外层行匹配多个内层行时会输出多行：
     * 行数翻倍、`COUNT(*)` 给出一个看起来很合理的数字，且不报错。
     */
    @Test
    fun `半连接只输出左行一次`() {
        val j = semiJoin(JoinType.SEMI, "o.user_id = u.id")
        // 右表两份都匹配订单 1（user_id=10）
        val out = JoinEngine.join(
            j, orderRows,
            listOf(listOf(listOf(Datum.I64(10), Datum.Str("alice")), listOf(Datum.I64(10), Datum.Str("alice2")))),
            listOf(JoinType.SEMI), listOf(onExpr("o.user_id = u.id")), emptyMap(),
        )
        assertEquals(1, out.size, "两个匹配行也只是一行")
        assertEquals(3, out[0].size, "输出的是**左行本身**（3 列），不是拼接行")
    }

    /**
     * ★ 等值连接**必须真的用上哈希**。
     *
     * 这条断言是在修掉一个真缺陷之后补上的：那一版里连接键的右侧取值器用了
     * 合成行的下标、却作用在内表自己的行上，于是**哈希桶永远是空的**——
     * 所有连接都退化成了双层循环。数据量小的时候结果完全一样，
     * 只有把"用了哪种策略"暴露出来才测得到（`EXPLAIN` 也因此能说实话）。
     */
    @Test
    fun `等值连接走哈希 而非等值回退双层循环`() {
        val equi = onJoin()
        val rows = run(equi, JoinType.INNER, onExpr("o.user_id = u.id"))
        assertEquals("hash", equi.lastStrategy, "等值连接必须建哈希")
        assertEquals(2, rows.size)

        val nonEqui = onJoin()
        run(nonEqui, JoinType.INNER, onExpr("o.user_id < u.id"))
        assertEquals("nested", nonEqui.lastStrategy, "非等值只能双层循环")
    }

    @Test
    fun `反连接输出没有匹配的左行`() {
        val out = run(semiJoin(JoinType.ANTI, "o.user_id = u.id"), JoinType.ANTI, onExpr("o.user_id = u.id"))
        assertEquals(listOf(3L, 4L), out.map { (it[0] as Datum.I64).v }.sorted(), "订单 3、4 没有匹配的用户")
        assertEquals(3, out[0].size, "只输出左行")
    }

    /**
     * ★ `NOT IN` 的 NULL 规则：**组内**出现 NULL 时该组不可返回，
     * 但外层值为 NULL 时也不可返回。三处规则缺一个都会多返回行。
     */
    /**
     * ★ `NOT IN` 的三值规则（**组内**出现 NULL 才算未知）。
     *
     * 这里把真实 SQL 的形状原样摆出来：
     * ```
     *   SELECT u.name FROM users u
     *    WHERE u.id NOT IN (SELECT o.amount FROM orders o WHERE o.user_id = u.id)
     * ```
     * 外层是 `users`、内层是 `orders`、关联键是 `o.user_id = u.id`、
     * **被比较值**是 `o.amount = u.id`。
     *
     * 约定：等值键里**最后一条**是"被比较值"那一对，前面的是关联键——
     * `CorrelatedSubqueries.toJoinClause` 就是按这个顺序拼出来的。
     */
    @Test
    fun `NOT IN 的三值规则`() {
        val inner = listOf(
            listOf(Datum.I64(1), Datum.I64(10), Datum.I64(100)),   // alice 那组：amount=100
            listOf(Datum.I64(8), Datum.I64(10), Datum.Null),       // alice 那组还有一个 NULL
            listOf(Datum.I64(2), Datum.I64(11), Datum.I64(200)),   // bob 那组：amount=200
        )
        val outer = listOf(
            listOf(Datum.I64(10), Datum.Str("alice")),
            listOf(Datum.I64(11), Datum.Str("bob")),
            listOf(Datum.I64(12), Datum.Str("carol")),             // 没有任何订单 → 空组
        )
        val j = JoinEngine.schemaOf(
            users, "u",
            listOf(JoinClause("orders", "o", JoinType.ANTI, null) to orders),
        )
        val on = onExpr("o.user_id = u.id AND o.amount = u.id")
        val out = JoinEngine.join(
            j, outer, listOf(inner), listOf(JoinType.ANTI), listOf(on), emptyMap(),
            listOf(AntiSemantics.NOT_IN),
        )
        assertEquals(
            listOf("bob", "carol"),
            out.map { (it[1] as Datum.Str).v },
            "alice 那组含 NULL → 未知；bob 那组只有 {200} → 11 不在其中 → 返回；" +
                "carol 那组是空集 → `NOT IN ()` 为真 → 返回",
        )
    }

    // ── 解析 ──────────────────────────────────────────────────────

    @Test
    fun `解析 JOIN 与别名`() {
        val s = Parser.parse("SELECT o.id FROM orders o JOIN users u ON o.user_id = u.id") as SelectStmt
        assertEquals(1, s.joins.size)
        assertEquals("users", s.joins[0].table)
        assertEquals("u", s.joins[0].alias)
        assertEquals(JoinType.INNER, s.joins[0].type)
        assertEquals("o", s.tableAlias)
    }

    @Test
    fun `解析各种连接类型`() {
        fun type(sql: String) = (Parser.parse(sql) as SelectStmt).joins[0].type
        assertEquals(JoinType.INNER, type("SELECT 1 FROM a JOIN b ON a.x = b.y"))
        assertEquals(JoinType.INNER, type("SELECT 1 FROM a INNER JOIN b ON a.x = b.y"))
        assertEquals(JoinType.LEFT, type("SELECT 1 FROM a LEFT JOIN b ON a.x = b.y"))
        assertEquals(JoinType.LEFT, type("SELECT 1 FROM a LEFT OUTER JOIN b ON a.x = b.y"))
        assertEquals(JoinType.RIGHT, type("SELECT 1 FROM a RIGHT JOIN b ON a.x = b.y"))
        assertEquals(JoinType.CROSS, type("SELECT 1 FROM a CROSS JOIN b"))
    }

    @Test
    fun `逗号连接等价于笛卡尔积`() {
        val s = Parser.parse("SELECT 1 FROM a, b") as SelectStmt
        assertEquals(1, s.joins.size)
        assertEquals(JoinType.CROSS, s.joins[0].type)
    }

    @Test
    fun `链式连接`() {
        val s = Parser.parse(
            "SELECT 1 FROM a JOIN b ON a.x = b.x JOIN c ON b.y = c.y",
        ) as SelectStmt
        assertEquals(2, s.joins.size)
    }

    @Test
    fun `非 CROSS 连接缺 ON 是语法错误`() {
        // 少写 ON 的 LEFT JOIN 会变成巨大的笛卡尔积，且结果看起来"有数据"
        assertFailsWith<SqlException> { Parser.parse("SELECT 1 FROM a LEFT JOIN b") }
        assertFailsWith<SqlException> { Parser.parse("SELECT 1 FROM a JOIN b") }
        // CROSS JOIN 本来就没有 ON
        Parser.parse("SELECT 1 FROM a CROSS JOIN b")
    }

    @Test
    fun `USING 与 NATURAL 明确报错而不是静默降级`() {
        val e1 = assertFailsWith<SqlException> { Parser.parse("SELECT 1 FROM a JOIN b USING (id)") }
        assertTrue(e1.message!!.contains("USING"), "${e1.message}")
        assertFailsWith<SqlException> { Parser.parse("SELECT 1 FROM a NATURAL JOIN b") }
    }

    @Test
    fun `别名不会吃掉后续子句的关键字`() {
        // `FROM t WHERE ...` 把 WHERE 当别名的话，报出来的错会与真正的问题毫无关系
        val s = Parser.parse("SELECT id FROM t WHERE id > 1") as SelectStmt
        assertNull(s.tableAlias)
        assertTrue(s.where != null)
    }

    @Test
    fun `SELECT 星号的输出名去掉限定前缀`() {
        val j = onJoin()
        assertEquals(
            listOf("id", "user_id", "amount", "id", "name"),
            JoinEngine.outputNames(j),
            "MySQL 返回的是裸列名；带前缀的内部名字不该泄漏给客户端",
        )
    }
}
