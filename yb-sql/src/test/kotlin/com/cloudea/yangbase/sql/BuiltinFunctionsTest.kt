package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * **内置标量函数库的语义**（纯逻辑，不需要引擎）。
 *
 * ## 这些断言为什么长这样
 *
 * 标量函数的"错"几乎都是**静默**的：结果是个合法的值，只是不是 MySQL 会给的那个。
 * `LENGTH('中')` 返回 1 还是 3、`ROUND(-2.5)` 是 -2 还是 -3、`CONCAT('a', NULL)`
 * 是 `NULL` 还是 `'a'`——没有一条会报错，而使用者拿它们做校验、分页、对账。
 *
 * 所以这里按**类别**钉三类性质：
 * 1. **NULL 传播**：MySQL 里绝大多数函数遇到 NULL 就返回 NULL（少数特例另列）；
 * 2. **边界与符号**：负数位置、越界、除零、空串——这些是各实现最容易分叉的地方；
 * 3. **参数个数**：写错个数必须是"SQL 写错了"（1210），而不是数据库自己的异常（1105）。
 */
class BuiltinFunctionsTest {

    /** 求一个常量表达式（`SELECT <expr>` 里的那个表达式）。 */
    private fun eval(sql: String): Datum {
        val stmt = Parser.parse("SELECT $sql") as SelectStmt
        val expr = (stmt.projections[0] as SelectItem.Field).expr
        return Eval.eval(expr, EvalContext(null, null))
    }

    private fun s(v: String) = Datum.Str(v)
    private fun i(v: Long) = Datum.I64(v)

    // ── NULL 传播 ─────────────────────────────────────────────────

    @Test
    fun `字符串函数遇到 NULL 返回 NULL`() {
        // 曾经 UPPER(NULL) 返回 ''、LENGTH(NULL) 返回 0、CONCAT('a',NULL) 返回 'a'
        // ——都是"不报错但结果不同"的那一类
        assertEquals(Datum.Null, eval("UPPER(NULL)"))
        assertEquals(Datum.Null, eval("LOWER(NULL)"))
        assertEquals(Datum.Null, eval("LENGTH(NULL)"))
        assertEquals(Datum.Null, eval("CHAR_LENGTH(NULL)"))
        assertEquals(Datum.Null, eval("CONCAT('a', NULL)"))
        assertEquals(Datum.Null, eval("CONCAT(NULL, NULL)"))
        assertEquals(Datum.Null, eval("SUBSTRING(NULL, 1)"))
        assertEquals(Datum.Null, eval("TRIM(NULL)"))
        assertEquals(Datum.Null, eval("REPLACE(NULL, 'a', 'b')"))
        assertEquals(Datum.Null, eval("LPAD(NULL, 3, 'x')"))
    }

    @Test
    fun `数值与日期函数遇到 NULL 返回 NULL`() {
        assertEquals(Datum.Null, eval("ABS(NULL)"))
        assertEquals(Datum.Null, eval("ROUND(NULL)"))
        assertEquals(Datum.Null, eval("ROUND(NULL, 2)"))
        assertEquals(Datum.Null, eval("CEIL(NULL)"))
        assertEquals(Datum.Null, eval("MOD(NULL, 3)"))
        assertEquals(Datum.Null, eval("YEAR(NULL)"))
        assertEquals(Datum.Null, eval("DATE_FORMAT(NULL, '%Y')"))
    }

    @Test
    fun `在 NULL 上做别的事的函数`() {
        // 这几个是刻意的特例：它们在 NULL 上有明确定义的另一种行为
        assertEquals(i(2), eval("IFNULL(NULL, 2)"))
        assertEquals(s("a"), eval("IFNULL('a', 'b')"))
        assertEquals(s("x"), eval("COALESCE(NULL, NULL, 'x')"))
        assertEquals(Datum.Null, eval("COALESCE(NULL, NULL)"))
        assertEquals(Datum.Null, eval("NULLIF(1, 1)"))
        assertEquals(i(1), eval("NULLIF(1, 2)"))
        assertEquals(Datum.Bool(true), eval("ISNULL(NULL)"))
        assertEquals(Datum.Bool(false), eval("ISNULL(1)"))
        // CONCAT_WS 跳过 NULL，但分隔符为 NULL 时整体为 NULL
        assertEquals(s("a-b"), eval("CONCAT_WS('-', 'a', NULL, 'b')"))
        assertEquals(Datum.Null, eval("CONCAT_WS(NULL, 'a')"))
    }

    @Test
    fun `控制流的三值逻辑`() {
        assertEquals(i(1), eval("IF(TRUE, 1, 2)"))
        assertEquals(i(2), eval("IF(FALSE, 1, 2)"))
        // 条件为 NULL → 走 ELSE（unknown 不是真）
        assertEquals(i(2), eval("IF(NULL, 1, 2)"))
        assertEquals(i(10), eval("CASE WHEN NULL THEN 1 ELSE 10 END"))
        // NULL 的比较永远是 unknown：`CASE NULL WHEN NULL` 不匹配
        assertEquals(s("b"), eval("CASE NULL WHEN NULL THEN 'a' ELSE 'b' END"))
    }

    // ── CASE ─────────────────────────────────────────────────────

    @Test
    fun `CASE 的搜索式与简单式`() {
        assertEquals(s("small"), eval("CASE WHEN 1 > 2 THEN 'big' ELSE 'small' END"))
        assertEquals(s("big"), eval("CASE WHEN 3 > 2 THEN 'big' ELSE 'small' END"))
        // 简单式
        assertEquals(s("one"), eval("CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END"))
        // 没有匹配且没有 ELSE → NULL（不是空串）
        assertEquals(Datum.Null, eval("CASE 9 WHEN 1 THEN 'one' END"))
        // 多分支按顺序匹配
        assertEquals(s("b"), eval("CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN 'c' END"))
    }

    // ── 字符串 ────────────────────────────────────────────────────

    @Test
    fun `LENGTH 是字节数 CHAR_LENGTH 是字符数`() {
        // utf8mb4 下一个汉字 3 字节。曾经两者都返回字符数——
        // 而 `LENGTH()` 常被用来判断"字段是否超长"，差 3 倍是致命的
        assertEquals(i(3), eval("LENGTH('中')"))
        assertEquals(i(1), eval("CHAR_LENGTH('中')"))
        assertEquals(i(6), eval("LENGTH('中文')"))
        assertEquals(i(2), eval("CHAR_LENGTH('中文')"))
        assertEquals(i(3), eval("LENGTH('abc')"))
        assertEquals(i(0), eval("LENGTH('')"))
    }

    @Test
    fun `SUBSTRING 的负数位置与越界`() {
        assertEquals(s("bcd"), eval("SUBSTRING('abcdef', 2, 3)"))
        assertEquals(s("bcdef"), eval("SUBSTRING('abcdef', 2)"))
        // 负数从末尾数（MySQL 语义）
        assertEquals(s("ef"), eval("SUBSTRING('abcdef', -2)"))
        assertEquals(s("ef"), eval("SUBSTRING('abcdef', -2, 2)"))
        // pos=0 是空串（不是从 1 开始）
        assertEquals(s(""), eval("SUBSTRING('abcdef', 0)"))
        // 超出长度 → 空串，不报错
        assertEquals(s(""), eval("SUBSTRING('abc', 10)"))
        assertEquals(s("c"), eval("SUBSTRING('abc', 3, 100)"))
        assertEquals(s("bcd"), eval("SUBSTR('abcdef', 2, 3)"))
    }

    @Test
    fun `LEFT RIGHT LPAD RPAD 的边界`() {
        assertEquals(s("ab"), eval("LEFT('abcdef', 2)"))
        assertEquals(s("ef"), eval("RIGHT('abcdef', 2)"))
        assertEquals(s("abcdef"), eval("LEFT('abcdef', 100)"))
        assertEquals(s(""), eval("LEFT('abcdef', 0)"))
        assertEquals(s("xxabc"), eval("LPAD('abc', 5, 'x')"))
        assertEquals(s("abcxx"), eval("RPAD('abc', 5, 'x')"))
        // 目标更长时按需重复填充串
        assertEquals(s("xyxabc"), eval("LPAD('abc', 6, 'xy')"))
        // 已经够长 → 截断到目标长度（MySQL 语义）
        assertEquals(s("ab"), eval("LPAD('abcdef', 2, 'x')"))
        assertEquals(s(""), eval("LPAD('abc', 0, 'x')"))
    }

    @Test
    fun `INSTR LOCATE REPLACE SUBSTRING_INDEX`() {
        assertEquals(i(3), eval("INSTR('abcdef', 'cd')"))
        assertEquals(i(0), eval("INSTR('abcdef', 'zz')"))
        assertEquals(i(3), eval("LOCATE('cd', 'abcdef')"))
        // 空串在任何位置都"找得到"，位置是 1（MySQL 语义）
        assertEquals(i(1), eval("LOCATE('', 'abcdef')"))
        assertEquals(i(1), eval("INSTR('abcdef', '')"))
        assertEquals(i(0), eval("LOCATE('cd', 'abcdef', 4)"))
        assertEquals(s("axc"), eval("REPLACE('abc', 'b', 'x')"))
        // 空搜索串返回原串（MySQL 语义，不是无限插入）
        assertEquals(s("abc"), eval("REPLACE('abc', '', 'x')"))
        assertEquals(s("a.b"), eval("SUBSTRING_INDEX('a.b.c', '.', 2)"))
        assertEquals(s("b.c"), eval("SUBSTRING_INDEX('a.b.c', '.', -2)"))
        assertEquals(s("a.b.c"), eval("SUBSTRING_INDEX('a.b.c', '.', 9)"))
    }

    @Test
    fun `TRIM 的三种语法`() {
        assertEquals(s("ab"), eval("TRIM('  ab  ')"))
        assertEquals(s("ab  "), eval("LTRIM('  ab  ')"))
        assertEquals(s("  ab"), eval("RTRIM('  ab  ')"))
        assertEquals(s("ab"), eval("TRIM('xxabxx', 'x')"))
        // 语法形式：TRIM(BOTH|LEADING|TRAILING 'x' FROM s)
        assertEquals(s("ab"), eval("TRIM(BOTH 'x' FROM 'xxabxx')"))
        assertEquals(s("abxx"), eval("TRIM(LEADING 'x' FROM 'xxabxx')"))
        assertEquals(s("xxab"), eval("TRIM(TRAILING 'x' FROM 'xxabxx')"))
    }

    @Test
    fun `HEX 与 UNHEX 往返`() {
        assertEquals(s("616263"), eval("HEX('abc')"))
        assertEquals(s("ABC"), eval("UNHEX('414243')").let { (it as Datum.Bin).let { _ -> Datum.Str(String(it.v)) } })
        // 奇数长度不是合法的十六进制 → NULL（MySQL 也是 NULL）
        assertEquals(Datum.Null, eval("UNHEX('abc')"))
        assertEquals(s("FF"), eval("HEX(255)"))
    }

    // ── 数值 ─────────────────────────────────────────────────────

    @Test
    fun `ROUND 是四舍五入远离零 不是银行家舍入`() {
        // Kotlin 的 `roundToLong` 会把 -2.5 变成 -2、2.5 变成 2（银行家舍入），
        // MySQL 是"远离零"。这是最容易写错也最难发现的一处
        assertEquals(Datum.F64(3.0), eval("ROUND(2.5)"))
        assertEquals(Datum.F64(-3.0), eval("ROUND(-2.5)"))
        assertEquals(Datum.F64(1.13), eval("ROUND(1.125, 2)"))
        assertEquals(Datum.F64(-1.13), eval("ROUND(-1.125, 2)"))
        // 注意：本系统的数字字面量只有整数与浮点两种（没有 DECIMAL 字面量），
        // 所以 `12.345` 是 F64，ROUND 走浮点路径。列类型是 DECIMAL 时，
        // 写入路径（Executor.coerce）会按列的标度转成精确的定点数
        assertEquals(Datum.F64(12.35), eval("ROUND(12.345, 2)"))
    }

    @Test
    fun `TRUNCATE 向零截断`() {
        assertEquals(Datum.F64(1.12), eval("TRUNCATE(1.129, 2)"))
        assertEquals(Datum.F64(-1.12), eval("TRUNCATE(-1.129, 2)"))
        assertEquals(Datum.I64(1), eval("TRUNCATE(19, -1)"))
    }

    @Test
    fun `除零与负数开方返回 NULL 而不是报错`() {
        // MySQL 里 `x/0` 与 `MOD(x,0)` 是 NULL（严格模式下也只是 warning），
        // 不是错误。"NULL 表示无意义"这件事必须与 MySQL 一致，
        // 否则应用里的 `IS NULL` 分支永远走不到
        assertEquals(Datum.Null, eval("MOD(1, 0)"))
        assertEquals(Datum.Null, eval("SQRT(-1)"))
        assertEquals(Datum.Null, eval("LN(0)"))
        assertEquals(Datum.Null, eval("LOG(-1)"))
        assertEquals(Datum.Null, eval("ASIN(2)"))
    }

    @Test
    fun `CEIL FLOOR SIGN 在负数上的方向`() {
        assertEquals(Datum.F64(-2.0), eval("FLOOR(-1.5)"))
        assertEquals(Datum.F64(-1.0), eval("CEIL(-1.5)"))
        assertEquals(Datum.F64(2.0), eval("CEIL(1.5)"))
        assertEquals(i(-1), eval("SIGN(-5)"))
        assertEquals(i(0), eval("SIGN(0)"))
        assertEquals(i(1), eval("SIGN(5)"))
    }

    @Test
    fun `ABS 在最小值上必须报错而不是回绕`() {
        // `abs(Long.MIN_VALUE)` 在 Kotlin 里会**静默返回负数**（回绕），
        // 与本项目"整数溢出报 1690"的立场冲突
        assertEquals(i(5), eval("ABS(-5)"))
        val e = assertFailsWith<SqlException> { eval("ABS(-9223372036854775808)") }
        assertEquals(MySqlErrors.ER_DATA_OUT_OF_RANGE, e.errorCode)
    }

    @Test
    fun `GREATEST 与 LEAST`() {
        assertEquals(i(3), eval("GREATEST(1, 3, 2)"))
        assertEquals(i(1), eval("LEAST(1, 3, 2)"))
        // 任一为 NULL 则结果为 NULL（MySQL 语义）
        assertEquals(Datum.Null, eval("GREATEST(1, NULL, 3)"))
    }

    // ── 日期时间 ─────────────────────────────────────────────────

    @Test
    fun `日期函数按 UTC 解释且往返稳定`() {
        // 时间戳字面量按 UTC（见 Datum.parseDateTime），格式化也必须按 UTC——
        // 两边不一致时 `DATE_FORMAT(d, '%H')` 与写进去的小时会对不上
        assertEquals(i(2026), eval("YEAR('2026-02-01 10:00:00')"))
        assertEquals(i(2), eval("MONTH('2026-02-01 10:00:00')"))
        assertEquals(i(1), eval("DAY('2026-02-01 10:00:00')"))
        assertEquals(i(10), eval("HOUR('2026-02-01 10:00:00')"))
        assertEquals(i(0), eval("MINUTE('2026-02-01 10:00:00')"))
        // DATE() 返回的是**日期值**（本系统里与 DATETIME 同构，都是 Datum.Ts）。
        // 想看字符串请用 DATE_FORMAT 或 CAST(... AS CHAR)
        assertEquals(Datum.Ts(Datum.parseDateTime("2026-02-01")!!), eval("DATE('2026-02-01 10:00:00')"))
    }

    @Test
    fun `DATE_FORMAT 覆盖日常格式符`() {
        assertEquals(s("2026-02-01"), eval("DATE_FORMAT('2026-02-01 10:00:00', '%Y-%m-%d')"))
        assertEquals(s("10:00:00"), eval("DATE_FORMAT('2026-02-01 10:00:00', '%H:%i:%s')"))
        assertEquals(s("Feb 1 2026"), eval("DATE_FORMAT('2026-02-01 10:00:00', '%b %e %Y')"))
        // 不认识的格式符原样输出（MySQL 同）——中文格式串必须能过
        assertEquals(s("2026年02月"), eval("DATE_FORMAT('2026-02-01 10:00:00', '%Y年%m月')"))
    }

    @Test
    fun `日期算术与差值`() {
        assertEquals(s("2026-02-02"), eval("DATE_FORMAT(DATE_ADD('2026-02-01', INTERVAL 1 DAY), '%Y-%m-%d')"))
        assertEquals(s("2026-01-31"), eval("DATE_FORMAT(DATE_SUB('2026-02-01', INTERVAL 1 DAY), '%Y-%m-%d')"))
        assertEquals(s("2027-02-01"), eval("DATE_FORMAT(DATE_ADD('2026-02-01', INTERVAL 1 YEAR), '%Y-%m-%d')"))
        assertEquals(i(1), eval("DATEDIFF('2026-02-02', '2026-02-01')"))
        assertEquals(i(-1), eval("DATEDIFF('2026-02-01', '2026-02-02')"))
        assertEquals(i(24), eval("TIMESTAMPDIFF(HOUR, '2026-02-01 00:00:00', '2026-02-02 00:00:00')"))
        assertEquals(i(1), eval("TIMESTAMPDIFF(MONTH, '2026-02-01', '2026-03-01')"))
    }

    @Test
    fun `月份加减的月末行为`() {
        // 1 月 31 日加一个月：MySQL 给 2 月 28/29 日（截到月末），不是 3 月 3 日
        assertEquals(s("2026-02-28"), eval("DATE_FORMAT(DATE_ADD('2026-01-31', INTERVAL 1 MONTH), '%Y-%m-%d')"))
        assertEquals(s("2026-02-28"), eval("DATE_FORMAT(LAST_DAY('2026-02-10'), '%Y-%m-%d')"))
    }

    @Test
    fun `UNIX_TIMESTAMP 与 FROM_UNIXTIME 往返`() {
        assertEquals(i(0), eval("UNIX_TIMESTAMP('1970-01-01 00:00:00')"))
        // FROM_UNIXTIME(无格式参数) 返回时间值；带格式参数才返回字符串
        assertEquals(Datum.Ts(0), eval("FROM_UNIXTIME(0)"))
        assertEquals(s("1970-01-01 00:00:00"), eval("FROM_UNIXTIME(0, '%Y-%m-%d %H:%i:%s')"))
        assertEquals(
            Datum.Ts(Datum.parseDateTime("2026-02-01 10:00:00")!!),
            eval("FROM_UNIXTIME(UNIX_TIMESTAMP('2026-02-01 10:00:00'))"),
        )
    }

    @Test
    fun `非法时间字符串报错而不是静默当 0`() {
        val e = assertFailsWith<SqlException> { eval("YEAR('not-a-date')") }
        assertEquals(MySqlErrors.ER_TRUNCATED_WRONG_VALUE, e.errorCode)
    }

    // ── 类型转换 ─────────────────────────────────────────────────

    @Test
    fun `CAST 的常见目标类型`() {
        assertEquals(i(12), eval("CAST('12' AS SIGNED)"))
        assertEquals(Datum.F64(12.5), eval("CAST('12.5' AS DOUBLE)"))
        assertEquals(s("12"), eval("CAST(12 AS CHAR)"))
        // CAST AS DATE 得到的是日期值（本系统与 DATETIME 同构：都是 Datum.Ts）
        assertEquals(Datum.Ts(Datum.parseDateTime("2026-02-01")!!), eval("CAST('2026-02-01 10:00:00' AS DATE)"))
        assertEquals(Datum.Null, eval("CAST(NULL AS SIGNED)"))
    }

    @Test
    fun `CAST 转换失败报错而不是给 0`() {
        // MySQL 宽松模式给 0 + warning；本系统没有 warning 通道，
        // 给 0 就是静默的错误答案（严格模式下的 MySQL 同样是报错）
        val e = assertFailsWith<SqlException> { eval("CAST('abc' AS SIGNED)") }
        assertEquals(MySqlErrors.ER_TRUNCATED_WRONG_VALUE, e.errorCode)
        assertFailsWith<SqlException> { eval("CAST(-1 AS UNSIGNED)") }
    }

    // ── 参数个数 ─────────────────────────────────────────────────

    @Test
    fun `参数个数不对报 1210 而不是内部异常`() {
        // 曾经 `UPPER()` 会抛 IndexOutOfBoundsException，被协议层兜成 1105
        // 并把栈打到 stderr——用户看到的是"数据库自己有缺陷"
        for (sql in listOf("UPPER()", "IFNULL(1)", "LENGTH()", "ROUND(1,2,3)", "SUBSTRING('a')", "ABS()")) {
            val e = assertFailsWith<SqlException>("`$sql` 应当报参数个数错") { eval(sql) }
            assertEquals(MySqlErrors.ER_WRONG_ARGUMENTS, e.errorCode, "错误码应当是 1210：$sql")
            assertTrue(e.message!!.contains("参数"), "文案要说清是参数问题：${e.message}")
        }
    }

    @Test
    fun `DISTINCT 用在标量函数上是要报错的`() {
        // `UPPER(DISTINCT name)` 曾经被静默当成 `UPPER(name)`——忽略用户写下的关键字
        val e = assertFailsWith<SqlException> { eval("UPPER(DISTINCT 'a')") }
        assertEquals(MySqlErrors.ER_PARSE_ERROR, e.errorCode)
    }

    @Test
    fun `不认识的函数仍然报 1235`() {
        val e = assertFailsWith<SqlException> { eval("NO_SUCH_FUNCTION(1)") }
        assertEquals(MySqlErrors.ER_NOT_SUPPORTED_YET, e.errorCode)
    }
}
