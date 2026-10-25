package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.SqlException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

/**
 * **定点数算术的精度**（纯逻辑，不需要引擎）。
 *
 * 这些用例守护的是一类**静默错值**：语句成功、结果是合法数字，只是算错了。
 * 它在金额与比例上最危险，所以每条断言都写死“MySQL 会给出的那个数”，
 * 而不是只比较“差不多”。
 *
 * 历史缺陷：
 * - `DECIMAL 列 * 2` 落进整数分支 → `12.34 * 2` 得到 `24`；
 * - `DECIMAL * DECIMAL` 少除一次 10^scale → `12.34 * 2.00` 得到 `2468.00`；
 * - `DECIMAL + 2` 同样被截断成 `14`。
 */
class DecimalArithmeticTest {

    private fun eval(sql: String): Datum {
        val stmt = Parser.parse("SELECT $sql") as SelectStmt
        val expr = (stmt.projections[0] as SelectItem.Field).expr
        return Eval.eval(expr, EvalContext(null, null))
    }

    // ── 列（Dec）× 整数字面量：最常见的写法 ──────────────────────

    @Test
    fun `定点数与整数运算保留小数`() {
        assertEquals(Datum.Dec(2468, 2), eval("CAST(12.34 AS DECIMAL(10,2)) * 2"))
        assertEquals(Datum.Dec(2468, 2), eval("2 * CAST(12.34 AS DECIMAL(10,2))"))
        assertEquals(Datum.Dec(1434, 2), eval("CAST(12.34 AS DECIMAL(10,2)) + 2"))
        assertEquals(Datum.Dec(1034, 2), eval("CAST(12.34 AS DECIMAL(10,2)) - 2"))
        assertEquals(Datum.Dec(34, 2), eval("CAST(12.34 AS DECIMAL(10,2)) % 3"))
        assertEquals(Datum.Dec(100, 2), eval("CAST(0.50 AS DECIMAL(10,2)) * 2"))
    }

    // ── 定点数 × 定点数：结果标度 = s1 + s2（MySQL 语义）──────────

    @Test
    fun `两个定点数相乘时标度相加`() {
        // 12.34 (s=2) * 2.00 (s=2) = 24.6800 (s=4)
        assertEquals(
            Datum.Dec(246800, 4),
            eval("CAST(12.34 AS DECIMAL(10,2)) * CAST(2 AS DECIMAL(10,2))"),
        )
        // 2.50 (s=2) * 3.00 (s=2) = 7.5000 (s=4)
        assertEquals(
            Datum.Dec(75000, 4),
            eval("CAST(2.50 AS DECIMAL(10,2)) * CAST(3.00 AS DECIMAL(10,2))"),
        )
    }

    @Test
    fun `定点数取模的符号随被除数`() {
        // MySQL: MOD(-12.34, 3) = -0.34
        assertEquals(Datum.Dec(-34, 2), eval("CAST(-12.34 AS DECIMAL(10,2)) % 3"))
        // 右操作数为 0 → NULL，而不是异常
        assertEquals(Datum.Null, eval("CAST(12.34 AS DECIMAL(10,2)) % 0"))
        assertEquals(
            Datum.Dec(100, 2),
            eval("CAST(10.00 AS DECIMAL(10,2)) % CAST(3.00 AS DECIMAL(10,2))"),
        )
    }

    // ── 其它数值类型 / 字符串的隐式转换 ──────────────────────────

    @Test
    fun `数值字符串按定点数参与运算`() {
        assertEquals(Datum.Dec(1334, 2), eval("'12.34' + CAST(1 AS DECIMAL(10,2))"))
        assertFailsWith<SqlException> { eval("CAST(12.34 AS DECIMAL(10,2)) + 'abc'") }
    }

    @Test
    fun `整数与浮点路径保持原语义`() {
        assertEquals(Datum.I64(5), eval("2 + 3"))
        assertEquals(Datum.I64(6), eval("2 * 3"))
        assertEquals(Datum.F64(3.5), eval("7 / 2"))
        // 没有定点数参与时，整数溢出仍然报错（不回绕）
        assertFailsWith<SqlException> { eval("9223372036854775807 + 1") }
    }
}
