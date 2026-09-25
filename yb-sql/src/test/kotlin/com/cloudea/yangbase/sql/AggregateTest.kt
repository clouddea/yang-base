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
import kotlin.test.assertTrue

/**
 * 分组与聚合的**纯逻辑**测试（不需要引擎、不需要对象存储）。
 *
 * 这些用例盯的是 SQL 语义里最容易出错的几处，它们的共同点是
 * **错了也不会报错**：`SUM` 把 NULL 当 0、`COUNT(x)` 数了 NULL、
 * 空表上聚合返回 0 行、`HAVING` 在分组之前求值……
 * 每一条都会给出一个"看起来正常"的数字，而使用者拿它做对账。
 *
 * 端到端的行为（投影、`ORDER BY`、别名、JDBC）另有一组用例，
 * 那需要真起服务端——这里只把语义本身钉死。
 */
class AggregateTest {

    private val schema = TableSchema(
        tableId = 1, databaseId = 1, name = "t", version = 1,
        columns = listOf(
            ColumnDef("id", ColumnType.BIGINT),
            ColumnDef("city", ColumnType.VARCHAR),
            ColumnDef("amount", ColumnType.BIGINT),
            ColumnDef("score", ColumnType.DOUBLE),
        ),
        primaryKey = listOf("id"),
    )

    /** 三行数据 + 一行 amount 为 NULL（聚合的 NULL 语义全靠它）。 */
    private val rows: List<List<Datum>> = listOf(
        listOf(Datum.I64(1), Datum.Str("bj"), Datum.I64(10), Datum.F64(1.5)),
        listOf(Datum.I64(2), Datum.Str("bj"), Datum.I64(20), Datum.F64(2.5)),
        listOf(Datum.I64(3), Datum.Str("sh"), Datum.I64(30), Datum.F64(3.5)),
        listOf(Datum.I64(4), Datum.Str("sh"), Datum.Null, Datum.F64(4.5)),
    )

    private fun stmt(sql: String) = Parser.parse(sql) as SelectStmt

    /** 解析出一个聚合表达式（从 SELECT 列表第一项里取）。 */
    private fun aggOf(sql: String): Expr =
        (stmt(sql).projections[0] as SelectItem.Field).expr

    private fun compute(sql: String, data: List<List<Datum>> = rows): Datum =
        Aggregates.compute(aggOf(sql), data, schema, emptyMap())

    // ── 各聚合函数的基本语义 ───────────────────────────────────────

    @Test
    fun `SUM 忽略 NULL`() {
        // 10 + 20 + 30 = 60，NULL 那一行不参与（当成 0 也是 60，所以再用一组区分）
        assertEquals(Datum.I64(60), compute("SELECT SUM(amount) FROM t"))
        val onlyNull = listOf(listOf(Datum.I64(1), Datum.Str("x"), Datum.Null, Datum.F64(0.0)))
        assertEquals(Datum.Null, compute("SELECT SUM(amount) FROM t", onlyNull), "全 NULL 的 SUM 是 NULL，不是 0")
    }

    @Test
    fun `SUM 溢出时报错而不是回绕`() {
        val big = listOf(
            listOf(Datum.I64(1), Datum.Str("x"), Datum.I64(Long.MAX_VALUE), Datum.F64(0.0)),
            listOf(Datum.I64(2), Datum.Str("x"), Datum.I64(1), Datum.F64(0.0)),
        )
        val e = assertFailsWith<SqlException> { compute("SELECT SUM(amount) FROM t", big) }
        assertTrue(e.message!!.contains("BIGINT"), "应当说明是范围问题：${e.message}")
    }

    @Test
    fun `AVG 忽略 NULL 且按非空个数平均`() {
        // (10+20+30)/3 = 20；把 NULL 当 0 会得到 15——那是个安静的错答案
        assertEquals(Datum.F64(20.0), compute("SELECT AVG(amount) FROM t"))
    }

    @Test
    fun `MIN 与 MAX 忽略 NULL`() {
        assertEquals(Datum.I64(10), compute("SELECT MIN(amount) FROM t"))
        assertEquals(Datum.I64(30), compute("SELECT MAX(amount) FROM t"))
        val onlyNull = listOf(listOf(Datum.I64(1), Datum.Str("x"), Datum.Null, Datum.F64(0.0)))
        assertEquals(Datum.Null, compute("SELECT MIN(amount) FROM t", onlyNull))
    }

    @Test
    fun `COUNT 星号数行数 而 COUNT 列只数非空`() {
        assertEquals(Datum.I64(4), compute("SELECT COUNT(*) FROM t"))
        assertEquals(Datum.I64(3), compute("SELECT COUNT(amount) FROM t"), "COUNT(列) 数的是非 NULL 值")
    }

    @Test
    fun `DISTINCT 去重`() {
        assertEquals(Datum.I64(2), compute("SELECT COUNT(DISTINCT city) FROM t"))
        assertEquals(Datum.I64(60), compute("SELECT SUM(DISTINCT amount) FROM t"))
    }

    @Test
    fun `GROUP_CONCAT 用逗号连接并忽略 NULL`() {
        assertEquals(Datum.Str("bj,bj,sh,sh"), compute("SELECT GROUP_CONCAT(city) FROM t"))
        assertEquals(Datum.Str("bj,sh"), compute("SELECT GROUP_CONCAT(DISTINCT city) FROM t"))
    }

    @Test
    fun `对字符串求和必须报错而不是当成 0`() {
        // MySQL 会把 'abc' 隐式转成 0 然后给出一个和；本系统明确拒绝——
        // 那种"看起来有结果"的数字最难被发现
        val e = assertFailsWith<SqlException> { compute("SELECT SUM(city) FROM t") }
        assertEquals(MySqlErrors.ER_WRONG_ARGUMENTS, e.errorCode)
    }

    // ── 分组 ──────────────────────────────────────────────────────

    @Test
    fun `分组按顺序稳定 - 同一份数据每次得到同样的组序`() {
        val groups = Aggregates.group(schema, rows, listOf(ColumnRef("city")), emptyMap())
        assertEquals(listOf(Datum.Str("bj"), Datum.Str("sh")), groups.map { it.first[0] })
        assertEquals(listOf(2, 2), groups.map { it.second.size })
    }

    @Test
    fun `NULL 归到同一组`() {
        val nullCity = listOf(
            listOf(Datum.I64(1), Datum.Null, Datum.I64(1), Datum.F64(0.0)),
            listOf(Datum.I64(2), Datum.Null, Datum.I64(2), Datum.F64(0.0)),
            listOf(Datum.I64(3), Datum.Str("bj"), Datum.I64(3), Datum.F64(0.0)),
        )
        val groups = Aggregates.group(schema, nullCity, listOf(ColumnRef("city")), emptyMap())
        assertEquals(2, groups.size, "两行 NULL 应当是同组")
        assertEquals(2, groups[0].second.size)
    }

    @Test
    fun `多列分组`() {
        val groups = Aggregates.group(
            schema, rows, listOf(ColumnRef("city"), ColumnRef("amount")), emptyMap(),
        )
        assertEquals(4, groups.size, "四行两两不同，应当四组")
    }

    @Test
    fun `空表上不分组聚合仍然返回一行`() {
        val cell = Aggregates.singleGroup(emptyList(), mapOf("COUNT(*)" to Datum.I64(0)))
        assertEquals(1, cell.size, "无 GROUP BY 的聚合必须返回一行，否则 COUNT(*) 查空表会得到 0 行而不是 0")
    }

    // ── 合法性：ONLY_FULL_GROUP_BY 与聚合的位置 ────────────────────

    @Test
    fun `有 GROUP BY 时选非分组列报 1055`() {
        val s = stmt("SELECT city, amount FROM t GROUP BY city")
        val e = assertFailsWith<SqlException> { Aggregates.validate(schema, s) }
        assertEquals(MySqlErrors.ER_WRONG_FIELD_WITH_GROUP, e.errorCode)
        assertTrue(e.message!!.contains("ONLY_FULL_GROUP_BY"), "应指出是这条规则：${e.message}")
    }

    @Test
    fun `无 GROUP BY 而混用聚合与普通列报 1140`() {
        val s = stmt("SELECT city, SUM(amount) FROM t")
        val e = assertFailsWith<SqlException> { Aggregates.validate(schema, s) }
        assertEquals(MySqlErrors.ER_MIX_OF_GROUP_FUNC_AND_FIELDS, e.errorCode)
    }

    @Test
    fun `分组键上的普通列是合法的`() {
        // 不报错即通过
        Aggregates.validate(schema, stmt("SELECT city, SUM(amount) FROM t GROUP BY city"))
        Aggregates.validate(schema, stmt("SELECT city FROM t GROUP BY city"))
    }

    @Test
    fun `SELECT 星号在分组查询里也要过检查`() {
        // 不检查的话 `SELECT * FROM t GROUP BY city` 会通过，
        // 然后 amount/score 取的是"每组随便哪一行"的值——不确定的结果
        val e = assertFailsWith<SqlException> {
            Aggregates.validate(schema, stmt("SELECT * FROM t GROUP BY city"))
        }
        assertEquals(MySqlErrors.ER_WRONG_FIELD_WITH_GROUP, e.errorCode)
    }

    @Test
    fun `WHERE 里的聚合报 1111`() {
        val s = stmt("SELECT id FROM t WHERE SUM(amount) > 1")
        assertFailsWith<SqlException> { Aggregates.validate(schema, s) }
        // 真正报错的是求值：没有分组上下文时聚合取不到值
        val e = assertFailsWith<SqlException> {
            Eval.eval(s.where!!, EvalContext(schema, rows[0]))
        }
        assertEquals(MySqlErrors.ER_INVALID_GROUP_FUNC_USE, e.errorCode)
    }

    @Test
    fun `GROUP BY 里出现聚合报 1111`() {
        val s = stmt("SELECT id FROM t GROUP BY SUM(amount)")
        val e = assertFailsWith<SqlException> { Aggregates.validate(schema, s) }
        assertEquals(MySqlErrors.ER_INVALID_GROUP_FUNC_USE, e.errorCode)
    }

    @Test
    fun `嵌套聚合报 1111`() {
        val s = stmt("SELECT SUM(COUNT(*)) FROM t")
        val e = assertFailsWith<SqlException> { Aggregates.validate(schema, s) }
        assertEquals(MySqlErrors.ER_INVALID_GROUP_FUNC_USE, e.errorCode)
    }

    @Test
    fun `HAVING 里可以引用不在投影中的聚合`() {
        // 收集必须覆盖 HAVING：只收投影的话这个聚合会被漏算，
        // 报出来的错看起来像用户写错了（实际是执行器漏算）
        val s = stmt("SELECT city FROM t GROUP BY city HAVING COUNT(*) > 1")
        val collected = Aggregates.collect(s).map { aggregateKey(it) }
        assertTrue(collected.any { it.startsWith("COUNT") }, "HAVING 里的 COUNT 必须被收集：$collected")
        Aggregates.validate(schema, s)     // 不报错即通过
    }

    @Test
    fun `ORDER BY 里的聚合也会被收集`() {
        val s = stmt("SELECT city FROM t GROUP BY city ORDER BY SUM(amount) DESC")
        assertTrue(Aggregates.collect(s).any { it is AggCall && it.name == "SUM" })
        Aggregates.validate(schema, s)
    }

    @Test
    fun `同一个聚合在不同位置只收集一次`() {
        val s = stmt("SELECT SUM(amount) FROM t GROUP BY city HAVING SUM(amount) > 1 ORDER BY SUM(amount)")
        assertEquals(1, Aggregates.collect(s).size, "同一个聚合算三遍会浪费，还可能因为写法差异给出不一致的值")
    }

    // ── 解析 ──────────────────────────────────────────────────────

    @Test
    fun `解析出 GROUP BY 与 HAVING`() {
        val s = stmt("SELECT city, COUNT(*) FROM t GROUP BY city HAVING COUNT(*) > 1")
        assertEquals(1, s.groupBy.size)
        assertTrue(s.having != null)
        assertEquals("city", (s.groupBy[0] as ColumnRef).name)
    }

    @Test
    fun `多列 GROUP BY`() {
        val s = stmt("SELECT city, amount FROM t GROUP BY city, amount")
        assertEquals(2, s.groupBy.size)
    }

    @Test
    fun `聚合函数解析成 AggCall 而不是普通函数`() {
        assertTrue(aggOf("SELECT SUM(amount) FROM t") is AggCall)
        assertTrue(aggOf("SELECT COUNT(*) FROM t") is CountCall)
        assertEquals("GROUP_CONCAT", (aggOf("SELECT GROUP_CONCAT(city) FROM t") as AggCall).name)
    }

    @Test
    fun `GROUP BY 写在 HAVING 之后是语法错误`() {
        // 宽容地接受任意顺序会让 HAVING 按错误的语义执行
        assertFailsWith<SqlException> {
            Parser.parse("SELECT city FROM t HAVING COUNT(*) > 1 GROUP BY city")
        }
    }
}
