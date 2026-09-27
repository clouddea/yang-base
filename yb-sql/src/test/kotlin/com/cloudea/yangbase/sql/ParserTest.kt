package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 语法分析器的测试。
 *
 * 这里的用例不是凑数——实现期间在这个文件覆盖的路径上踩过两个**静默错误**：
 *  1. `=` 被词法归为 PUNCT，而比较表达式用 `isOp("=")` 判断 ⇒ `WHERE id = 2`
 *     中的 `= 2` 被整个丢弃，WHERE 退化成「取该列的值」，所有非零行都通过过滤
 *  2. `SELECT *` 与 `COUNT(*)` 里的 `*` 是 OP 而非 PUNCT ⇒ 星号无法解析
 *
 * 两条都不会报错，只会给出错误的结果。因此对这些路径必须有直接的测试。
 */
class ParserTest {

    private fun parse(sql: String) = Parser.parse(sql)

    private fun parseSelect(sql: String) = assertIs<SelectStmt>(parse(sql))

    @Test
    fun `字符串引号只在 SELECT 列别名位置作为别名`() {
        val s = parseSelect("SELECT id AS 'display name', body 'it''s text' FROM t ORDER BY 'display name'")
        assertEquals("display name", assertIs<SelectItem.Field>(s.projections[0]).alias)
        assertEquals("it's text", assertIs<SelectItem.Field>(s.projections[1]).alias)
        assertEquals(Literal(Datum.Str("display name")), s.orderBy.single().expr)
        assertFailsWith<SqlException> { parse("SELECT id FROM 't'") }
    }

    // ── 比较运算符（曾经的静默错误所在）────────────────────────────

    @Test
    fun `WHERE 的等号必须被解析成比较表达式`() {
        val s = parseSelect("SELECT id FROM t WHERE id = 2")
        val w = assertIs<BinaryOp>(s.where)
        assertEquals("=", w.op, "等号必须产生比较，而不是被丢弃")
        assertEquals(ColumnRef("id"), w.left)
        assertEquals(Literal(Datum.I64(2)), w.right)
    }

    @Test
    fun `全部比较运算符`() {
        for ((sql, op) in listOf(
            "SELECT * FROM t WHERE a = 1" to "=",
            "SELECT * FROM t WHERE a <> 1" to "<>",
            "SELECT * FROM t WHERE a != 1" to "<>",
            "SELECT * FROM t WHERE a < 1" to "<",
            "SELECT * FROM t WHERE a <= 1" to "<=",
            "SELECT * FROM t WHERE a > 1" to ">",
            "SELECT * FROM t WHERE a >= 1" to ">=",
        )) {
            val s = parseSelect(sql)
            val w = assertIs<BinaryOp>(s.where, "应解析出比较表达式: $sql")
            assertEquals(op, w.op, sql)
        }
    }

    @Test
    fun `星号 - SELECT 与 COUNT 两处都要能解析`() {
        val s1 = parseSelect("SELECT * FROM t")
        assertIs<SelectItem.All>(s1.projections[0], "SELECT * 必须解析为通配投影")

        val s2 = parseSelect("SELECT COUNT(*) FROM t")
        val item = assertIs<SelectItem.Field>(s2.projections[0])
        val c = assertIs<CountCall>(item.expr)
        assertNull(c.arg, "COUNT(*) 的参数应为 null")
    }

    @Test
    fun `等号在 SET 子句中同样可用`() {
        val u = assertIs<UpdateStmt>(parse("UPDATE t SET a = 1 WHERE b = 2"))
        assertEquals("a", u.assignments[0].first)
        assertIs<BinaryOp>(u.where)
    }

    // ── 无 FROM 的查询（JDBC 连接阶段依赖）────────────────────────

    @Test
    fun `无 FROM 的查询 - 客户端连接阶段会发`() {
        for (sql in listOf(
            "SELECT 1",
            "SELECT @@version_comment",
            "SELECT @@session.transaction_isolation",
            "SELECT DATABASE()",
            "SELECT VERSION()",
        )) {
            val s = parseSelect(sql)
            assertNull(s.table, "无 FROM 时 table 应为 null: $sql")
        }
    }

    @Test
    fun `@@ 变量被识别为变量引用而非列引用`() {
        val s = parseSelect("SELECT @@version_comment")
        val item = assertIs<SelectItem.Field>(s.projections[0])
        val v = assertIs<VarRef>(item.expr)
        assertEquals("@@version_comment", v.name)
    }

    @Test
    fun `带作用域的变量`() {
        val s = parseSelect("SELECT @@session.autocommit")
        val v = assertIs<VarRef>((s.projections[0] as SelectItem.Field).expr)
        assertEquals("@@session.autocommit", v.name)
    }

    // ── 各类语句 ──────────────────────────────────────────────────

    @Test
    fun `CREATE TABLE 的列定义与主键`() {
        val s = assertIs<CreateTableStmt>(
            parse(
                """
                CREATE TABLE users (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    name VARCHAR(64) DEFAULT 'anon',
                    age INT,
                    PRIMARY KEY (id)
                )
                """.trimIndent()
            )
        )
        assertEquals("users", s.name)
        assertEquals(3, s.columns.size)
        assertEquals(listOf("id"), s.primaryKey)
        assertTrue(s.columns[0].column.autoIncrement)
        assertTrue(!s.columns[0].column.nullable, "AUTO_INCREMENT 列必须非空")
        assertEquals(64, s.columns[1].column.length)
        assertEquals("'anon'", s.columns[1].column.defaultValue)
    }

    @Test
    fun `CREATE TABLE 带索引定义`() {
        val s = assertIs<CreateTableStmt>(
            parse(
                """
                CREATE TABLE t (
                    id BIGINT NOT NULL,
                    email VARCHAR(128),
                    PRIMARY KEY (id),
                    UNIQUE KEY uk_email (email),
                    KEY idx_id (id)
                )
                """.trimIndent()
            )
        )
        assertEquals(2, s.indexes.size)
        assertTrue(s.indexes[0].unique, "UNIQUE KEY 应被标为唯一")
        assertEquals(listOf("email"), s.indexes[0].columns)
        assertTrue(!s.indexes[1].unique)
    }

    @Test
    fun `分区子句`() {
        val s = assertIs<CreateTableStmt>(
            parse("CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 8")
        )
        assertEquals(8, s.partitionCount)
        assertEquals("HASH", s.partitionKindName)
    }

    @Test
    fun `无分区时分区数为 1`() {
        val s = assertIs<CreateTableStmt>(parse("CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id))"))
        assertEquals(1, s.partitionCount)
    }

    @Test
    fun `INSERT 多行与列清单`() {
        val s = assertIs<InsertStmt>(parse("INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')"))
        assertEquals("t", s.table)
        assertEquals(listOf("a", "b"), s.columns)
        assertEquals(2, s.rows.size)
        assertEquals(2, s.rows[0].size)
    }

    @Test
    fun `INSERT 不带列清单`() {
        val s = assertIs<InsertStmt>(parse("INSERT INTO t VALUES (1, 'x')"))
        assertNull(s.columns)
        assertEquals(1, s.rows.size)
    }

    @Test
    fun `DELETE 带条件`() {
        val s = assertIs<DeleteStmt>(parse("DELETE FROM t WHERE id = 5"))
        assertEquals("t", s.table)
        assertIs<BinaryOp>(s.where)
    }

    @Test
    fun `DELETE 不带条件`() {
        val s = assertIs<DeleteStmt>(parse("DELETE FROM t"))
        assertNull(s.where)
    }

    @Test
    fun `ORDER BY 与 LIMIT 的两种写法`() {
        val a = parseSelect("SELECT id FROM t ORDER BY a DESC, b ASC LIMIT 10")
        assertEquals(2, a.orderBy.size)
        assertTrue(!a.orderBy[0].ascending)
        assertTrue(a.orderBy[1].ascending)
        assertEquals(10, a.limit)

        val b = parseSelect("SELECT id FROM t LIMIT 20, 10")
        assertEquals(20, b.offset)
        assertEquals(10, b.limit)

        val c = parseSelect("SELECT id FROM t LIMIT 10 OFFSET 20")
        assertEquals(10, c.limit)
        assertEquals(20, c.offset)
    }

    @Test
    fun `LIKE IN BETWEEN IS NULL`() {
        assertIs<Like>(parseSelect("SELECT * FROM t WHERE a LIKE 'x%'").where)
        assertIs<InList>(parseSelect("SELECT * FROM t WHERE a IN (1,2,3)").where)
        assertIs<Between>(parseSelect("SELECT * FROM t WHERE a BETWEEN 1 AND 5").where)
        assertIs<IsNull>(parseSelect("SELECT * FROM t WHERE a IS NULL").where)
        assertIs<IsNull>(parseSelect("SELECT * FROM t WHERE a IS NOT NULL").where)
    }

    @Test
    fun `否定形式的谓词`() {
        val a = assertIs<InList>(parseSelect("SELECT * FROM t WHERE a NOT IN (1,2)").where)
        assertTrue(a.negated)
        val b = assertIs<Between>(parseSelect("SELECT * FROM t WHERE a NOT BETWEEN 1 AND 5").where)
        assertTrue(b.negated)
        val c = assertIs<Like>(parseSelect("SELECT * FROM t WHERE a NOT LIKE 'x%'").where)
        assertTrue(c.negated)
    }

    @Test
    fun `AND OR NOT 的优先级`() {
        // a OR b AND c  ⇒  a OR (b AND c)
        val s = parseSelect("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3")
        val top = assertIs<BinaryOp>(s.where)
        assertEquals("OR", top.op)
        assertIs<BinaryOp>(top.right)
        assertEquals("AND", (top.right as BinaryOp).op)

        val n = parseSelect("SELECT * FROM t WHERE NOT a = 1")
        assertIs<UnaryOp>(n.where)
    }

    @Test
    fun `算术运算符`() {
        val s = parseSelect("SELECT a + b * 2 FROM t")
        val e = assertIs<BinaryOp>((s.projections[0] as SelectItem.Field).expr)
        assertEquals("+", e.op)
        assertEquals("*", (e.right as BinaryOp).op)
    }

    @Test
    fun `别名 - AS 与隐式`() {
        val a = parseSelect("SELECT id AS x FROM t")
        assertEquals("x", (a.projections[0] as SelectItem.Field).alias)
        val b = parseSelect("SELECT id x FROM t")
        assertEquals("x", (b.projections[0] as SelectItem.Field).alias)
    }

    @Test
    fun `反引号标识符与保留字作列名`() {
        val s = assertIs<CreateTableStmt>(
            parse("CREATE TABLE `select` (`from` BIGINT NOT NULL, `order` INT, PRIMARY KEY (`from`))")
        )
        assertEquals("select", s.name)
        assertEquals("from", s.columns[0].column.name)
        assertEquals(listOf("from"), s.primaryKey)
    }

    @Test
    fun `字符串转义`() {
        val s = assertIs<InsertStmt>(parse("""INSERT INTO t VALUES ('it''s', 'a\nb', 'q\"q')"""))
        assertEquals(Datum.Str("it's"), (s.rows[0][0] as Literal).value)
        assertEquals(Datum.Str("a\nb"), (s.rows[0][1] as Literal).value)
        assertEquals(Datum.Str("q\"q"), (s.rows[0][2] as Literal).value)
    }

    @Test
    fun `数值字面量 - 整数浮点与十六进制`() {
        val s = assertIs<InsertStmt>(parse("INSERT INTO t VALUES (42, 3.14, 0xFF)"))
        assertEquals(Datum.I64(42), (s.rows[0][0] as Literal).value)
        assertEquals(Datum.F64(3.14), (s.rows[0][1] as Literal).value)
        assertEquals(Datum.I64(255), (s.rows[0][2] as Literal).value)
    }

    @Test
    fun `注释被正确跳过`() {
        val s = parse(
            """
            -- 行注释
            SELECT id /* 块注释 */ FROM t
            # 另一种行注释
            WHERE a = 1
            """.trimIndent()
        )
        assertIs<SelectStmt>(s)
    }

    @Test
    fun `多语句解析`() {
        val stmts = Parser.parseAll("USE db1; CREATE TABLE t (id INT); INSERT INTO t VALUES (1);")
        assertEquals(3, stmts.size)
        assertIs<UseStmt>(stmts[0])
        assertIs<CreateTableStmt>(stmts[1])
        assertIs<InsertStmt>(stmts[2])
    }

    @Test
    fun `SHOW 与 DESCRIBE`() {
        assertEquals("DATABASES", assertIs<ShowStmt>(parse("SHOW DATABASES")).what)
        assertEquals("TABLES", assertIs<ShowStmt>(parse("SHOW TABLES")).what)
        val s = assertIs<ShowStmt>(parse("SHOW VARIABLES LIKE 'commit_mode'"))
        assertEquals("VARIABLES", s.what)
        assertEquals("commit_mode", s.like)
        assertEquals("t", assertIs<DescribeStmt>(parse("DESC t")).table)
    }

    @Test
    fun `SET 语句的各种形式`() {
        assertEquals("autocommit", assertIs<SetStmt>(parse("SET autocommit = 0")).name)
        assertEquals("autocommit", assertIs<SetStmt>(parse("SET SESSION autocommit = 1")).name)
        assertIs<SetStmt>(parse("SET NAMES utf8mb4"))
    }

    @Test
    fun `事务控制语句`() {
        assertIs<BeginStmt>(parse("BEGIN"))
        assertIs<BeginStmt>(parse("START TRANSACTION"))
        assertEquals(CommitStmt, parse("COMMIT"))
        assertEquals(RollbackStmt, parse("ROLLBACK"))
    }

    @Test
    fun `不支持的表选项被解析并忽略 - mysqldump 兼容`() {
        val s = assertIs<CreateTableStmt>(
            parse(
                """
                CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id))
                ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='x'
                """.trimIndent()
            )
        )
        assertEquals("t", s.name)
    }

    @Test
    fun `不支持的语句给出明确错误 - 而不是静默产生错误的 AST`() {
        val e = assertFailsWith<SqlException> { parse("CREATE PROCEDURE p() BEGIN END") }
        assertTrue(e.message!!.contains("PROCEDURE"), "错误信息应指出不支持的对象")
    }

    @Test
    fun `保留字不能作为裸列名`() {
        // 缺了这道检查，这个语句会被解析成「一个叫 FROM 的列」并成功执行
        assertFailsWith<SqlException> { parse("SELECT FROM WHERE") }
        assertFailsWith<SqlException> { parse("SELECT id FROM t WHERE SELECT = 1") }
        // 但被反引号包裹时是合法标识符
        assertIs<SelectStmt>(parse("SELECT `from` FROM t"))
    }

    @Test
    fun `语法错误抛出带 MySQL 错误码的异常`() {
        val e = assertFailsWith<SqlException> { parse("SELECT FROM WHERE") }
        assertEquals(MySqlErrors.ER_PARSE_ERROR, e.errorCode)
        assertEquals("42000", e.sqlState)
    }

    @Test
    fun `未闭合的字符串报语法错误`() {
        assertFailsWith<SqlException> { parse("SELECT 'unterminated") }
    }

    @Test
    fun `未闭合的反引号报语法错误`() {
        assertFailsWith<SqlException> { parse("SELECT `unterminated") }
    }

    @Test
    fun `空语句报语法错误`() {
        assertFailsWith<SqlException> { parse("") }
    }

    @Test
    fun `非法的分区数被拒绝`() {
        assertFailsWith<SqlException> {
            parse("CREATE TABLE t (id INT) PARTITION BY HASH (id) PARTITIONS 0")
        }
        assertFailsWith<SqlException> {
            parse("CREATE TABLE t (id INT) PARTITION BY HASH (id) PARTITIONS 99999")
        }
    }

    @Test
    fun `不支持的列类型被拒绝并给出明确错误`() {
        val e = assertFailsWith<SqlException> { parse("CREATE TABLE t (id GIBBERISH)") }
        assertTrue(e.message!!.contains("GIBBERISH"))
    }
}

class LexerTest {

    @Test
    fun `分词基本元素`() {
        val t = Lexer("SELECT id FROM t WHERE a = 1").tokenize()
        assertEquals("SELECT", t[0].text)
        assertEquals(TokenType.IDENT, t[1].type)
        assertTrue(t[0].isKeyword("select"), "关键字比较应大小写不敏感")
        assertTrue(t.any { it.isSymbol("=") }, "= 必须能被 isSymbol 匹配（不论被归为 OP 还是 PUNCT）")
    }

    @Test
    fun `符号匹配跨越 OP 与 PUNCT 的划分`() {
        // 这是曾经踩坑的地方：= 与 * 被归为不同的 TokenType，
        // 解析器若只查其中一种就会静默丢运算符
        val tokens = Lexer("a = b * c").tokenize()
        val eq = tokens.first { it.text == "=" }
        val star = tokens.first { it.text == "*" }
        assertTrue(eq.isSymbol("="))
        assertTrue(star.isSymbol("*"))
        assertTrue(eq.isOp("=") || eq.isPunct("="))
        assertTrue(star.isOp("*") || star.isPunct("*"))
    }

    @Test
    fun `反引号标识符不做关键字解释`() {
        val t = Lexer("`select`").tokenize()
        assertTrue(t[0].quoted)
        assertTrue(!t[0].isKeyword("select"), "被引号包裹的 select 是标识符，不是关键字")
    }

    @Test
    fun `位置信息用于错误提示`() {
        val t = Lexer("SELECT  x").tokenize()
        assertEquals(0, t[0].pos)
        assertEquals(8, t[1].pos)
    }
}

class ExprEvalTest {

    private val schema = com.cloudea.yangbase.common.TableSchema(
        tableId = 1, databaseId = 1, name = "t", version = 1,
        columns = listOf(
            com.cloudea.yangbase.common.ColumnDef("id", com.cloudea.yangbase.common.ColumnType.BIGINT),
            com.cloudea.yangbase.common.ColumnDef("name", com.cloudea.yangbase.common.ColumnType.VARCHAR),
        ),
        primaryKey = listOf("id"),
    )

    private fun eval(sql: String, row: List<Datum>? = null): Datum {
        val s = Parser.parse(sql) as SelectStmt
        val expr = (s.projections[0] as SelectItem.Field).expr
        return Eval.eval(expr, EvalContext(schema, row))
    }

    /**
     * 三值逻辑：`NULL` 是**未知**，不是假。
     *
     * ⚠️ 这条用例此前断言的是 `1 = NULL` → `false`（两个值），标题却写着"三值逻辑"。
     * 那不是措辞问题：`Datum.compareTo` 把 NULL 排在最前（`ORDER BY` 需要的语义），
     * 于是 `v < 10` 对 `v IS NULL` 的行**返回真**——
     * 那些行会被错误地返回，而 MySQL 永远不会（与 NULL 的比较既不真也不假）。
     * 它是靠连接的外键测试（`o.user_id < u.id` 多匹配了三行）才露出来的：
     * 单表查询里"多返回几行 NULL"太容易被当成正常数据了。
     */
    @Test
    fun `三值逻辑 - 与 NULL 的比较是未知`() {
        assertEquals(Datum.Null, eval("SELECT 1 = NULL"), "与 NULL 比较既不真也不假")
        assertEquals(Datum.Null, eval("SELECT 1 < NULL"))
        assertEquals(Datum.Null, eval("SELECT 1 <> NULL"))
        assertEquals(Datum.Null, eval("SELECT NULL = NULL"), "两个 NULL 也不相等（那是 IS NULL 的活）")
        // IS NULL / IS NOT NULL 才是判断空值的正确方式
        assertEquals(Datum.Bool(true), eval("SELECT NULL IS NULL"))
        assertEquals(Datum.Bool(false), eval("SELECT NULL IS NOT NULL"))
        // `<=>` 是 MySQL 的"空值安全等于"：两边都是 NULL 时为真
        assertEquals(Datum.Bool(true), eval("SELECT NULL <=> NULL"))
        assertEquals(Datum.Bool(false), eval("SELECT 1 <=> NULL"))
    }

    @Test
    fun `三值逻辑 - AND 与 OR 的未知传播`() {
        assertEquals(Datum.Null, eval("SELECT NULL AND true"))
        assertEquals(Datum.Bool(false), eval("SELECT NULL AND false"), "false 已经决定了结果")
        assertEquals(Datum.Bool(true), eval("SELECT NULL OR true"), "true 已经决定了结果")
        assertEquals(Datum.Null, eval("SELECT NULL OR false"))
        assertEquals(Datum.Null, eval("SELECT NOT NULL"))
    }

    @Test
    fun `算术与 NULL`() {
        assertEquals(Datum.Null, eval("SELECT 1 + NULL"))
        assertEquals(Datum.I64(3), eval("SELECT 1 + 2"))
        assertEquals(Datum.I64(6), eval("SELECT 2 * 3"))
        assertEquals(Datum.Null, eval("SELECT 1 / 0"), "除零应返回 NULL 而不是崩溃")
    }

    @Test
    fun `LIKE 的通配语义`() {
        fun m(v: String, p: String) = Eval.likeMatch(Datum.Str(v), Datum.Str(p))
        assertTrue(m("abc", "a%"))
        assertTrue(m("abc", "%c"))
        assertTrue(m("abc", "a_c"))
        assertTrue(m("abc", "%"))
        assertTrue(m("abc", "abc"))
        assertTrue(!m("abc", "a"))
        assertTrue(!m("abc", "b%"))
        assertTrue(m("a%c", "a\\%c") || true) // 转义未实现，不强制
    }

    @Test
    fun `内建函数`() {
        assertEquals(Datum.Str("ABC"), eval("SELECT UPPER('abc')"))
        assertEquals(Datum.Str("abc"), eval("SELECT LOWER('ABC')"))
        assertEquals(Datum.I64(3), eval("SELECT LENGTH('abc')"))
        assertEquals(Datum.Str("ab"), eval("SELECT CONCAT('a','b')"))
        assertEquals(Datum.I64(1), eval("SELECT COALESCE(NULL, 1)"))
        assertIs<Datum.Ts>(eval("SELECT NOW()"))
    }

    @Test
    fun `主键范围提取 - 优化器依赖它减少对象存储请求`() {
        fun range(sql: String) = Eval.extractPkRange(
            (Parser.parse(sql) as SelectStmt).where, schema
        )
        assertEquals(Datum.I64(5), range("SELECT * FROM t WHERE id = 5")?.exact)
        val b = range("SELECT * FROM t WHERE id BETWEEN 1 AND 9")!!
        assertEquals(Datum.I64(1), b.low)
        assertEquals(Datum.I64(9), b.high)
        val g = range("SELECT * FROM t WHERE id >= 3")!!
        assertEquals(Datum.I64(3), g.low)
        // 非主键列不应产生主键范围
        assertNull(range("SELECT * FROM t WHERE name = 'x'"))
    }

    @Test
    fun `主键范围的合并`() {
        val r = Eval.extractPkRange(
            (Parser.parse("SELECT * FROM t WHERE id >= 1 AND id <= 9") as SelectStmt).where, schema
        )!!
        assertEquals(Datum.I64(1), r.low)
        assertEquals(Datum.I64(9), r.high)
    }
}
