package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException

/**
 * MySQL 子集的递归下降语法分析器。
 *
 * 刻意手写而非用 ANTLR：本项目支持的语法是一个**明确的子集**，
 * 手写实现没有构建期代码生成、错误信息更可控，且调试成本低得多。
 * 若日后要覆盖完整 MySQL 方言，再换 ANTLR 更合适（设计文档 §10.1 的原方案）。
 */
class Parser(private val tokens: List<Token>) {

    private var i = 0

    private fun peek(): Token = tokens[i]
    private fun next(): Token = tokens[i++]
    private fun atEnd() = peek().type == TokenType.EOF

    private fun acceptKeyword(k: String): Boolean {
        if (peek().isKeyword(k)) { i++; return true }
        return false
    }

    private fun expectKeyword(k: String) {
        if (!acceptKeyword(k)) fail("期望关键字 $k，实际是 ${peek()}")
    }

    private fun acceptPunct(p: String): Boolean {
        if (peek().isPunct(p)) { i++; return true }
        return false
    }

    private fun expectPunct(p: String) {
        if (!acceptPunct(p)) fail("期望 '$p'，实际是 ${peek()}")
    }

    private fun fail(msg: String): Nothing =
        throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "SQL 语法错误: $msg")

    private fun ident(): String {
        if (peek().type != TokenType.IDENT) fail("期望标识符，实际是 ${peek()}")
        return next().text
    }

    /**
     * 可能是 `db.table` 形式的表名，返回**完整的点分名**。
     *
     * 不能只保留点号后半段：那样 `information_schema.tables` 会退化成
     * `tables`，于是系统库查询会被当成用户表去查找（然后报「表不存在」）。
     * 拆分交给执行器——只有它知道哪些前缀是系统库。
     */
    private fun qualifiedName(): String {
        val first = ident()
        if (acceptPunct(".")) return "$first.${ident()}"
        return first
    }

    // ── 入口 ──────────────────────────────────────────────────────

    fun parseStatement(): Statement {
        val stmt = parseStatementInner()
        // 允许结尾有一个分号
        acceptPunct(";")
        // ★ 残留的 token 必须**报错**，不能静默丢弃。
        //
        // 这条检查拦下的是一整类最危险的缺陷：`SELECT ... GROUP BY x`、
        // `... HAVING`、`... FOR UPDATE`、`... INTO OUTFILE`、`... UNION ...`
        // 这些子句解析器都不支持，但如果不报错，语句会**照常执行并返回一个
        // 看起来正常的结果**——只是结果没做分组、没加锁、没导出。
        // 使用者拿到的是一个静默错误的答案，而不是一个「不支持」的提示。
        //
        // 宁可在这里失败，也不要在下游给出错误的行。
        if (!atEnd()) {
            val rest = tokens.drop(i).takeWhile { it.type != TokenType.EOF }
                .joinToString(" ") { it.text }
            fail("不支持或位置错误的子句: '$rest'（语句在它之前就已结束）。" +
                "本系统明确拒绝而不是忽略不支持的语法——静默忽略会返回错误的结果。")
        }
        return stmt
    }

    private fun parseStatementInner(): Statement {
        if (atEnd()) fail("语句为空")
        return when {
            // ⚠️ 必须是 parseUnion：`SELECT … UNION SELECT …` 也是一种 SELECT，
            // 直接走 parseSelect 会把 UNION 当成"语句结束后多出来的子句"报错
            peek().isKeyword("SELECT") -> parseUnion()
            // EXPLAIN / ANALYZE 按**首关键字**判别（与其它语句互不冲突）。
            // 这里刻意不支持 `DESC SELECT ...` 这种别名：它要和 `DESCRIBE 表名`
            // 靠前瞻区分，而 `DESCRIBE t` 被客户端用得多得多。
            peek().isKeyword("EXPLAIN") -> parseExplain()
            peek().isKeyword("ANALYZE") -> parseAnalyze()
            peek().isKeyword("INSERT") || peek().isKeyword("REPLACE") -> parseInsert()
            peek().isKeyword("UPDATE") -> parseUpdate()
            peek().isKeyword("DELETE") -> parseDelete()
            peek().isKeyword("CREATE") -> parseCreate()
            peek().isKeyword("DROP") -> parseDrop()
            peek().isKeyword("ALTER") -> parseAlter()
            peek().isKeyword("TRUNCATE") -> parseTruncate()
            peek().isKeyword("SHOW") -> parseShow()
            peek().isKeyword("DESC") || peek().isKeyword("DESCRIBE") -> {
                next(); DescribeStmt(qualifiedName())
            }
            peek().isKeyword("USE") -> { next(); UseStmt(ident()) }
            peek().isKeyword("SET") -> parseSet()
            peek().isKeyword("BEGIN") || peek().isKeyword("START") -> {
                next()
                acceptKeyword("TRANSACTION")
                val ro = acceptKeyword("READ") && (acceptKeyword("ONLY") || true)
                BeginStmt(ro)
            }
            peek().isKeyword("COMMIT") -> { next(); CommitStmt }
            peek().isKeyword("ROLLBACK") -> { next(); RollbackStmt }
            // mysqldump / 客户端的固定开场：**接受但什么也不做**。
            // 拒绝它们会让 mysqldump 完全不可用；而本系统的写者身份是分片租约、
            // 读是快照读，没有"锁表"能提供的额外保证（见 compatibility.md）
            peek().isKeyword("LOCK") || peek().isKeyword("UNLOCK") || peek().isKeyword("FLUSH") -> {
                while (!atEnd() && !peek().isPunct(";")) next()
                NoOpStmt
            }
            else -> {
                val raw = tokens.drop(i).takeWhile { it.type != TokenType.EOF }.joinToString(" ") { it.text }
                i = tokens.size - 1
                UnsupportedStmt(raw)
            }
        }
    }

    /**
     * `EXPLAIN [EXTENDED] <语句>`（设计文档 §8.9.6）。
     *
     * `EXPLAIN ANALYZE` 会**真的执行**语句并附上运行时统计（MySQL 8 的语义）。
     * 本系统明确拒绝它：把一个执行语义伪装成规划语义，是使用者最难预料的一类意外。
     */
    private fun parseExplain(): Statement {
        expectKeyword("EXPLAIN")
        acceptKeyword("EXTENDED")
        if (peek().isKeyword("ANALYZE")) {
            fail("EXPLAIN ANALYZE 会真的执行该语句；本系统只支持「只规划不执行」的 EXPLAIN")
        }
        return ExplainStmt(parseStatementInner())
    }

    /**
     * `ANALYZE [LOCAL | NO_WRITE_TO_BINLOG] TABLE t [, t2 ...]`（设计文档 §10.3）。
     *
     * `LOCAL` 是 MySQL 的方言（表示不写 binlog）；这里接受并忽略它，因为
     * 本系统没有 binlog——但**必须接受**：mysqldump 与图形客户端都会发它。
     */
    private fun parseAnalyze(): Statement {
        expectKeyword("ANALYZE")
        acceptKeyword("LOCAL")
        acceptKeyword("NO_WRITE_TO_BINLOG")
        expectKeyword("TABLE")
        val tables = arrayListOf(qualifiedName())
        while (acceptPunct(",")) tables.add(qualifiedName())
        return AnalyzeStmt(tables)
    }

    // ── CREATE ────────────────────────────────────────────────────

    private fun parseCreate(): Statement {
        expectKeyword("CREATE")
        // 支持 CREATE UNIQUE INDEX
        val unique = acceptKeyword("UNIQUE")
        val fulltext = acceptKeyword("FULLTEXT")
        return when {
            acceptKeyword("DATABASE") || acceptKeyword("SCHEMA") -> {
                val ifNotExists = parseIfNotExists()
                val name = ident()
                parseTableOptionsIgnored()
                CreateDatabaseStmt(name, ifNotExists)
            }
            acceptKeyword("TABLE") -> parseCreateTable()
            // CREATE [OR REPLACE] VIEW v AS SELECT … —— 视图体以**原文**存进
            // CreateViewStmt.sql（AST 不是可序列化的，而视图定义要写进 catalog
            // 并跨版本读得出来，所以存文本；见 CreateViewStmt 的说明）
            acceptKeyword("OR") -> {
                expectKeyword("REPLACE")
                expectKeyword("VIEW")
                parseCreateView(orReplace = true)
            }
            acceptKeyword("VIEW") -> parseCreateView(orReplace = false)
            acceptKeyword("INDEX") || acceptKeyword("KEY") -> parseCreateIndex(unique, fulltext)
            else -> fail("不支持的 CREATE 对象: ${peek()}")
        }
    }

    /**
     * `CREATE [OR REPLACE] VIEW name [(cols)] AS <SELECT>`。
     *
     * 视图体用**原文**保存（不是 AST）：它要写进 catalog、跨版本读得出来，
     * 而 AST 没有序列化形式。拼回用的是每个 token 的 `raw`（含引号与反引号），
     * 不是 `text`——后者对字符串字面量已经去掉了引号，拼回去会变成两个 token，
     * 症状是"视图建好了，一查就报语法错误"。
     *
     * 显式列名清单（`VIEW v (a,b) AS`）明确拒绝：它的语义是给输出列改名，
     * 而本系统的视图不重写内层 SELECT，收下就等于**丢掉用户写的列名**。
     */
    private fun parseCreateView(orReplace: Boolean): Statement {
        val name = qualifiedName()
        if (peek().isPunct("(")) {
            fail(
                "暂不支持 `CREATE VIEW v (列名, ...) AS ...`（显式列名清单）。" +
                    "请在 SELECT 里用别名指定列名",
            )
        }
        expectKeyword("AS")
        val start = i
        // 视图体可以是 SELECT，也可以是 UNION 的 SELECT
        parseUnion()
        val sql = tokens.subList(start, i).joinToString(" ") { it.raw }
        return CreateViewStmt(name, orReplace, sql)
    }

    private fun parseIfNotExists(): Boolean {
        if (acceptKeyword("IF")) {
            expectKeyword("NOT")
            expectKeyword("EXISTS")
            return true
        }
        return false
    }

    private fun parseIfExists(): Boolean {
        if (acceptKeyword("IF")) {
            expectKeyword("EXISTS")
            return true
        }
        return false
    }

    private fun parseCreateTable(): Statement {
        val ifNotExists = parseIfNotExists()
        val name = qualifiedName()
        expectPunct("(")

        val columns = ArrayList<ColumnAst>()
        val indexes = ArrayList<IndexAst>()
        var primaryKey: List<String> = emptyList()
        var autoIdx = 0

        while (true) {
            if (peek().isPunct(")")) break
            when {
                peek().isKeyword("PRIMARY") -> {
                    next(); expectKeyword("KEY")
                    acceptKeyword("USING")
                    if (peek().type == TokenType.IDENT && !peek().isPunct("(")) {
                        // 可选的索引类型名，如 BTREE
                        if (!peek().isPunct("(")) next()
                    }
                    expectPunct("(")
                    primaryKey = parseIdentList()
                    expectPunct(")")
                }
                peek().isKeyword("KEY") || peek().isKeyword("INDEX") -> {
                    next()
                    // `KEY name (col) USING MINMAX`：跳数索引与普通二级索引**必须区分开**——
                    // 前者不维护任何键空间，只收集块级统计；当成二级索引会既浪费写入
                    // 又得不到任何剪枝收益
                    val idxName = if (!peek().isPunct("(")) ident() else "idx_${autoIdx++}"
                    parseIndexTypeNameIfPresent()
                    expectPunct("(")
                    val cols = parseIdentList()
                    expectPunct(")")
                    indexes.add(IndexAst(idxName, cols, unique = false, kind = parseIndexKind()))
                }
                peek().isKeyword("UNIQUE") -> {
                    next()
                    acceptKeyword("KEY"); acceptKeyword("INDEX")
                    val idxName = if (!peek().isPunct("(")) ident() else "uniq_${autoIdx++}"
                    parseIndexTypeNameIfPresent()
                    expectPunct("(")
                    val cols = parseIdentList()
                    expectPunct(")")
                    indexes.add(IndexAst(idxName, cols, unique = true, kind = parseIndexKind()))
                }
                peek().isKeyword("CONSTRAINT") -> {
                    // 外键等约束：解析但忽略（设计文档 §11.3 明确列为不支持级联）
                    while (!peek().isPunct(",") && !peek().isPunct(")") && !atEnd()) next()
                }
                else -> columns.add(parseColumnDef())
            }
            if (!acceptPunct(",")) break
        }
        expectPunct(")")

        var partitionCount = 1
        var partitionKind = "NONE"
        var partitionColumn: String? = null
        var partitionExpr: String? = null
        var partitionDefs: List<PartitionAst> = emptyList()

        if (acceptKeyword("PARTITION")) {
            expectKeyword("BY")
            partitionKind = ident().uppercase()

            // 分区表达式：`BY RANGE (created_at)` / `BY RANGE (TO_DAYS(created_at))`
            //              / `BY HASH (user_id)` / `BY KEY (a, b)`
            if (acceptPunct("(")) {
                val cols = ArrayList<String>()
                var expr: String? = null
                do {
                    val t = peek()
                    if (t.type == TokenType.IDENT && !t.quoted) {
                        // 可能是 `TO_DAYS(col)` 这样的函数调用，也可能只是列名
                        next()
                        if (peek().isPunct("(")) {
                            next()
                            val colName = ident()
                            expectPunct(")")
                            expr = "${t.text.uppercase()}($colName)"
                            cols.add(colName)
                        } else {
                            cols.add(t.text)
                        }
                    } else {
                        fail("非法的主键表达式: $t")
                    }
                } while (acceptPunct(","))
                expectPunct(")")
                partitionColumn = cols.firstOrNull()
                partitionExpr = expr
            }

            if (partitionKind == "HASH" || partitionKind == "KEY") {
                if (acceptKeyword("PARTITIONS")) {
                    val t = peek()
                    if (t.type != TokenType.NUMBER) fail("PARTITIONS 后应为数字")
                    next()
                    partitionCount = t.text.toIntOrNull() ?: fail("非法的分区数: ${t.text}")
                    if (partitionCount <= 0 || partitionCount > 8192) fail("分区数超出范围: $partitionCount")
                }
            } else if (partitionKind == "RANGE" || partitionKind == "LIST") {
                partitionDefs = parsePartitionDefs()
            }

            // 分区之后可能还跟着建表选项，交给统一的忽略逻辑
        }
        parseTableOptionsIgnored()

        // 列级 `PRIMARY KEY` 与表级 `PRIMARY KEY (…)` 是**同一个东西的两种写法**。
        // 两者同时出现按 MySQL 是错误（multiple primary key defined），
        // 这里也报错——静默挑一个会让另一处的语义悄悄消失
        val colLevelPk = columns.filter { it.primaryKey }.map { it.column.name }
        val effectivePk = when {
            colLevelPk.isEmpty() -> primaryKey
            primaryKey.isEmpty() -> colLevelPk
            else -> throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000",
                "表 '$name' 定义了多个主键：列级的 ${colLevelPk} 与表级的 $primaryKey。" +
                    "MySQL 也不允许（multiple primary key defined）",
            )
        }
        return CreateTableStmt(
            name, columns, effectivePk, indexes, ifNotExists,
            partitionCount, partitionKind, partitionColumn, partitionDefs, partitionExpr,
        )
    }

    /**
     * `( PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN MAXVALUE )`
     *
     * 边界表达式保留**原文**（例如 `TO_DAYS('2026-04-01')`）——求值需要列类型，
     * 那是建表时才有的信息。
     */
    private fun parsePartitionDefs(): List<PartitionAst> {
        val out = ArrayList<PartitionAst>()
        expectPunct("(")
        while (!peek().isPunct(")") && !atEnd()) {
            expectKeyword("PARTITION")
            val pname = ident()
            expectKeyword("VALUES")
            when {
                acceptKeyword("LESS") -> {
                    expectKeyword("THAN")
                    // `VALUES LESS THAN MAXVALUE` 是**不带括号**的，与带括号的
                    // `VALUES LESS THAN (100)` 并存——这是 MySQL 的写法，不是笔误
                    if (peek().isKeyword("MAXVALUE")) {
                        next()
                        out.add(PartitionAst(pname, upperBound = com.cloudea.yangbase.common.MAXVALUE))
                    } else {
                        expectPunct("(")
                        val bound = readBoundExpr()
                        expectPunct(")")
                        out.add(PartitionAst(pname, upperBound = bound))
                    }
                }
                acceptKeyword("IN") -> {
                    expectPunct("(")
                    val vals = ArrayList<String>()
                    do {
                        vals.add(readBoundExpr())
                    } while (acceptPunct(","))
                    expectPunct(")")
                    out.add(PartitionAst(pname, values = vals))
                }
                else -> fail("分区 '${pname}' 需要 VALUES LESS THAN 或 VALUES IN")
            }
            if (!acceptPunct(",")) break
        }
        expectPunct(")")
        return out
    }

    /**
     * 读一个分区边界表达式，返回其**规范化后的文本**。
     *
     * 支持三种形态（设计文档 §9.1 列出的写法）：
     *  - `MAXVALUE`
     *  - 字面量：数字或字符串
     *  - `TO_DAYS('2026-04-01')` / `YEAR('2026-01-01')`
     */
    private fun readBoundExpr(): String {
        val t = peek()
        if (t.isKeyword("MAXVALUE")) { next(); return com.cloudea.yangbase.common.MAXVALUE }
        if (t.type == TokenType.NUMBER) { next(); return t.text }
        if (t.type == TokenType.STRING) { next(); return "'" + t.text + "'" }
        if (t.type == TokenType.IDENT) {
            // TO_DAYS(...) / YEAR(...)：把整段调用原样收集起来，交给建表时求值
            val fn = next().text.uppercase()
            if (fn != "TO_DAYS" && fn != "YEAR") fail("不支持的分区边界函数: $fn（支持 TO_DAYS / YEAR）")
            expectPunct("(")
            val arg = readBoundExpr()
            expectPunct(")")
            return "$fn($arg)"
        }
        fail("无法解析的分区边界: ${t.text}")
    }

    private fun parseTableOptionsIgnored() {
        // ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='...' 等一律解析并忽略，
        // 因为 mysqldump 的输出里必然带这些
        while (!atEnd() && !peek().isPunct(";") && !peek().isPunct("(")) {
            if (peek().isKeyword("AS") || peek().isKeyword("SELECT")) break
            next()
        }
    }

    private fun parseColumnDef(): ColumnAst {
        val name = ident()
        val typeName = ident().uppercase()
        var length: Int? = null
        var precision: Int? = null
        var scale: Int? = null
        /** ENUM / SET 的成员表。 */
        var members: List<String> = emptyList()

        if (acceptPunct("(")) {
            if (typeName == "ENUM" || typeName == "SET") {
                // 成员是字符串列表：`ENUM('a','b')`
                val list = ArrayList<String>()
                while (true) {
                    val t = next()
                    if (t.type != TokenType.STRING) {
                        fail("$typeName 的成员必须是字符串字面量，收到 '${t.text}'")
                    }
                    list.add(t.text)
                    if (acceptPunct(",")) continue
                    break
                }
                expectPunct(")")
                if (list.isEmpty()) fail("$typeName 至少要有一个成员")
                members = list
                length = list.maxOf { it.length }
            } else {
                val first = next()
                if (first.type != TokenType.NUMBER) fail("列长度应为数字")
                if (acceptPunct(",")) {
                    precision = first.text.toIntOrNull()
                    val second = next()
                    if (second.type != TokenType.NUMBER) fail("列精度应为数字")
                    scale = second.text.toIntOrNull()
                } else {
                    length = first.text.toIntOrNull()
                }
                expectPunct(")")
            }
        }

        var nullable = true
        var autoInc = false
        var default: String? = null
        /**
         * 列级 `PRIMARY KEY`。
         *
         * ⚠️ 必须**带出去**：`CREATE TABLE t (id INT PRIMARY KEY, …)` 是最常见的写法，
         * 而它曾经只被当成 `NOT NULL`——表因此以**空主键**建成，
         * `KeyEncoder.encodeKey` 对空列清单返回 `ByteArray(0)`，
         * 于是**每一行都用同一个 key 写入**：LSM 的同 key 覆盖语义下，
         * 这张表永远只能存**最后一行**，且不报任何错。
         */
        var colPrimaryKey = false

        loop@ while (!atEnd()) {
            when {
                peek().isKeyword("NOT") -> { next(); expectKeyword("NULL"); nullable = false }
                peek().isKeyword("NULL") -> { next(); nullable = true }
                peek().isKeyword("AUTO_INCREMENT") -> { next(); autoInc = true }
                peek().isKeyword("DEFAULT") -> { next(); default = parseDefaultLiteral() }
                peek().isKeyword("PRIMARY") -> { next(); expectKeyword("KEY"); nullable = false; colPrimaryKey = true }
                peek().isKeyword("UNIQUE") -> { next(); acceptKeyword("KEY") }
                peek().isKeyword("COMMENT") -> { next(); if (peek().type == TokenType.STRING) next() }
                peek().isKeyword("UNSIGNED") || peek().isKeyword("ZEROFILL") -> next()
                peek().isKeyword("COLLATE") -> { next(); next() }
                peek().isKeyword("CHARACTER") -> { next(); acceptKeyword("SET"); next() }
                peek().isKeyword("ON") -> { next(); acceptKeyword("UPDATE"); next(); if (acceptPunct("(")) { while (!peek().isPunct(")") && !atEnd()) next(); expectPunct(")") } }
                else -> break@loop
            }
        }

        val type = mapType(typeName, length) ?: throw SqlException(
            MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", "暂不支持的列类型: $typeName"
        )
        val col = ColumnDef(
            name = name,
            type = type,
            nullable = if (autoInc) false else nullable,
            length = if (type == ColumnType.VARCHAR) (length ?: 255) else length,
            precision = precision,
            scale = scale,
            defaultValue = default,
            autoIncrement = autoInc,
            enumValues = members,
        )
        // ENUM/SET 的声明顺序**就是**它的语义序，而本系统按字符串比较
        // （见 ColumnType.ENUM 的说明）。两者不一致时，`ORDER BY` 与范围比较的
        // 结果会与 MySQL 不同——这种差异必须在建表时就**主动警告**，
        // 而不是等使用者发现排序不对（§7.4 的「主动警告」要求）。
        if (members.isNotEmpty() && members != members.sorted()) {
            System.err.println(
                "[yb] 警告: 列 '$name' 的 $typeName 声明顺序不是字典序（$members）。" +
                    "本系统的 ENUM/SET 比较与排序按**字符串**，MySQL 按**声明序号**，" +
                    "两者的 ORDER BY / 范围比较结果会不同。若依赖 MySQL 的序，" +
                    "请把声明改成字典序。"
            )
        }
        // ★ 列级 `PRIMARY KEY` **必须带出去**——上面那段注释解释了为什么（空主键 ⇒
        // 每一行用同一个 key ⇒ 整张表只留最后一行）。曾经这里写的是 `ColumnAst(col)`：
        // 标志位被算出来又丢掉，DDL 照常成功，数据静默丢失。
        return ColumnAst(col, colPrimaryKey)
    }

    private fun parseDefaultLiteral(): String? {
        val t = peek()
        return when {
            t.type == TokenType.STRING -> { next(); "'${t.text.replace("\\", "\\\\").replace("'", "''")}'" }
            t.type == TokenType.NUMBER -> { next(); t.text }
            t.isKeyword("NULL") -> { next(); null }
            t.isKeyword("CURRENT_TIMESTAMP") -> {
                next()
                if (acceptPunct("(")) { while (!peek().isPunct(")") && !atEnd()) next(); expectPunct(")") }
                "CURRENT_TIMESTAMP"
            }
            else -> { next(); t.text }
        }
    }

    private fun mapType(name: String, len: Int?): ColumnType? = when (name) {
        "TINYINT" -> ColumnType.TINYINT
        "SMALLINT", "MEDIUMINT" -> ColumnType.SMALLINT
        "INT", "INTEGER" -> ColumnType.INT
        "BIGINT" -> ColumnType.BIGINT
        "FLOAT" -> ColumnType.FLOAT
        "DOUBLE", "REAL" -> ColumnType.DOUBLE
        "DECIMAL", "NUMERIC" -> ColumnType.DECIMAL
        "BOOL", "BOOLEAN" -> ColumnType.BOOLEAN
        "VARCHAR", "CHAR" -> ColumnType.VARCHAR
        "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT" -> ColumnType.TEXT
        "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY" -> ColumnType.BLOB
        "DATE" -> ColumnType.DATE
        "DATETIME" -> ColumnType.DATETIME
        "TIMESTAMP" -> ColumnType.TIMESTAMP
        "YEAR" -> ColumnType.YEAR
        "ENUM" -> ColumnType.ENUM
        "SET" -> ColumnType.SET
        "JSON" -> ColumnType.JSON
        else -> null
    }

    private fun parseCreateIndex(unique: Boolean, fulltext: Boolean = false): Statement {
        val name = ident()
        expectKeyword("ON")
        val table = qualifiedName()
        expectPunct("(")
        val cols = parseIdentList()
        expectPunct(")")
        var kind = if (fulltext) IndexKind.FULLTEXT else IndexKind.ORDERED
        if (acceptKeyword("USING")) {
            kind = when (ident().uppercase()) {
                "HASH" -> IndexKind.HASH
                "BTREE" -> IndexKind.ORDERED
                "MINMAX", "MIN_MAX" -> IndexKind.SKIP_MINMAX
                "SET" -> IndexKind.SKIP_SET
                "BLOOM" -> IndexKind.SKIP_BLOOM
                "NGRAM", "NGRAM_BLOOM" -> IndexKind.SKIP_NGRAM_BLOOM
                else -> IndexKind.ORDERED
            }
        }
        // WITH PARSER <name>：全文索引的分词器。索引期与查询期必须一致，
        // 因此它随 schema 持久化，而不是查询时现取。
        var parser: String? = null
        if (acceptKeyword("WITH")) {
            expectKeyword("PARSER")
            parser = ident().lowercase()
        }
        // INCLUDE (...) 子句用于 covering index
        return CreateIndexStmt(name, table, cols, unique, kind, parser)
    }

    /**
     * `MATCH (col) AGAINST ('text' [IN BOOLEAN MODE] [WITH QUERY EXPANSION])`。
     *
     * `WITH QUERY EXPANSION` 解析但忽略——它是**召回策略**，不改匹配结果集的语义边界，
     * 静默接受比报错更贴近使用者的预期（MySQL 客户端工具会带这个子句）。
     */
    private fun parseMatchAgainst(): Expr {
        expectKeyword("MATCH")
        expectPunct("(")
        val cols = parseIdentList()
        expectPunct(")")
        expectKeyword("AGAINST")
        expectPunct("(")
        val qt = next()
        if (qt.type != TokenType.STRING) fail("AGAINST 需要一个字符串，实际是 $qt")
        var booleanMode = false
        if (acceptKeyword("IN")) {
            expectKeyword("BOOLEAN")
            expectKeyword("MODE")
            booleanMode = true
        }
        if (acceptKeyword("WITH")) {
            expectKeyword("QUERY")
            expectKeyword("EXPANSION")
        }
        expectPunct(")")
        return MatchAgainst(cols, qt.text, booleanMode)
    }

    // ── ALTER TABLE ───────────────────────────────────────────────

    /**
     * `ALTER TABLE t <action>[, <action>]...`
     *
     * 支持的动作见 [AlterAction]。不认识的子句**明确报错**——
     * 悄悄忽略一个 `ADD COLUMN` 会让使用者以为列加上了。
     */
    private fun parseAlter(): Statement {
        expectKeyword("ALTER")
        if (acceptKeyword("DATABASE") || acceptKeyword("SCHEMA")) {
            // ALTER DATABASE 只影响字符集等元信息，本系统不需要——吃掉并接受
            while (!atEnd() && !peek().isPunct(";")) next()
            return UnsupportedStmt("ALTER DATABASE")
        }
        expectKeyword("TABLE")
        val table = qualifiedName()
        val actions = ArrayList<AlterAction>()
        while (true) {
            actions.add(parseAlterAction())
            if (!acceptPunct(",")) break
        }
        if (actions.isEmpty()) fail("ALTER TABLE 至少要有一个动作")
        return AlterTableStmt(table, actions)
    }

    private fun parseAlterAction(): AlterAction {
        val t = peek()
        return when {
            t.isKeyword("ADD") -> {
                next()
                when {
                    peek().isKeyword("COLUMN") -> { next(); parseAddColumn() }
                    peek().isKeyword("PARTITION") -> { next(); AddPartitionAction(parsePartitionDefs()) }
                    peek().isKeyword("INDEX") || peek().isKeyword("KEY") -> {
                        next()
                        val name = ident()
                        parseIndexTypeNameIfPresent()
                        expectPunct("(")
                        val cols = parseIdentList()
                        expectPunct(")")
                        AddIndexAction(IndexAst(name, cols, unique = false, kind = parseIndexKind()))
                    }
                    peek().isKeyword("UNIQUE") -> {
                        next()
                        acceptKeyword("INDEX"); acceptKeyword("KEY")
                        val name = ident()
                        parseIndexTypeNameIfPresent()
                        expectPunct("(")
                        val cols = parseIdentList()
                        expectPunct(")")
                        AddIndexAction(IndexAst(name, cols, unique = true, kind = parseIndexKind()))
                    }
                    // `ADD col_name type` —— COLUMN 关键字可省
                    else -> parseAddColumn()
                }
            }
            t.isKeyword("DROP") -> {
                next()
                when {
                    peek().isKeyword("COLUMN") -> { next(); DropColumn(ident()) }
                    peek().isKeyword("PARTITION") -> {
                        next()
                        val names = ArrayList<String>()
                        do { names.add(ident()) } while (acceptPunct(","))
                        DropPartitionAction(names)
                    }
                    peek().isKeyword("INDEX") || peek().isKeyword("KEY") -> { next(); DropIndexAction(ident()) }
                    peek().isKeyword("PRIMARY") -> {
                        next(); expectKeyword("KEY")
                        fail("本系统不支持删除主键：主键是数据在 LSM 里的物理排序键，删除它等于重建整张表")
                    }
                    else -> DropColumn(ident())
                }
            }
            t.isKeyword("TRUNCATE") -> {
                next()
                acceptKeyword("PARTITION")
                val names = ArrayList<String>()
                do { names.add(ident()) } while (acceptPunct(","))
                TruncatePartitionAction(names)
            }
            t.isKeyword("REORGANIZE") -> {
                next()
                expectKeyword("PARTITION")
                val from = ArrayList<String>()
                do { from.add(ident()) } while (acceptPunct(","))
                expectKeyword("INTO")
                ReorganizePartitionAction(from, parsePartitionDefs())
            }
            t.isKeyword("RENAME") -> {
                next()
                acceptKeyword("TO"); acceptKeyword("AS")
                RenameTable(qualifiedName())
            }
            // `ALTER TABLE t ENABLE|DISABLE KEYS` —— **接受为无操作**。
            //
            // 它是 MyISAM 时代"批量导入期间先别维护索引"的提示，MySQL 在 InnoDB 上
            // 本身就是空操作。而 `mysqldump` 的每一张表都会发这两句
            // （`/*!40000 ALTER TABLE t DISABLE KEYS */`，版本注释里的内容会被执行），
            // 于是它成了**任何导入都会撞上**的第一道墙。
            //
            // 忽略它是安全的：本系统的索引维护与写入是原子的，没有"先关掉再重建"
            // 这个阶段——也就是说，这个提示在我们这里**没有可忽略的语义**，
            // 不像 `ORDER BY` 那样忽略会让结果不同
            t.isKeyword("ENABLE") || t.isKeyword("DISABLE") -> {
                next()
                expectKeyword("KEYS")
                NoOpAlterAction
            }
            else -> fail("暂不支持的 ALTER TABLE 动作: ${t.text}")
        }
    }

    /** `ALTER TABLE t ADD COLUMN c INT [NULL|NOT NULL] [DEFAULT v] [FIRST | AFTER col]` */
    private fun parseAddColumn(): AddColumn {
        val col = parseColumnDef().column
        var after: String? = null
        var first = false
        if (acceptKeyword("FIRST")) first = true
        else if (acceptKeyword("AFTER")) after = ident()
        return AddColumn(col, after, first)
    }

    /** `TRUNCATE [TABLE] t` —— 清空整表。 */
    private fun parseTruncate(): Statement {
        expectKeyword("TRUNCATE")
        acceptKeyword("TABLE")
        return AlterTableStmt(qualifiedName(), listOf(TruncateWholeTable))
    }

    /** 吃掉 `KEY name USING BTREE (cols)` 里 col 列表**之前**的类型名。 */
    private fun parseIndexTypeNameIfPresent() {
        if (peek().isKeyword("USING")) {
            next()
            if (!peek().isPunct("(")) next()
        }
    }

    /** 吃掉 col 列表**之后**的 `USING {HASH|BTREE}`，返回索引类型。 */
    /**
     * `USING BTREE | HASH | MINMAX | SET | BLOOM | NGRAM`。
     *
     * 跳数索引（§8.5）不是用来查行的，而是用来**跳过数据块**的——
     * 它不返回任何结果，只回答「这个块可能含有所需的值吗」。
     * 因此它有自己的一族类型名，与 BTREE/HASH 平级。
     */
    private fun parseIndexKind(): IndexKind {
        if (acceptKeyword("USING")) {
            val t = next()
            return when (t.text.uppercase()) {
                "HASH" -> IndexKind.HASH
                "MINMAX", "MIN_MAX" -> IndexKind.SKIP_MINMAX
                "SET" -> IndexKind.SKIP_SET
                "BLOOM" -> IndexKind.SKIP_BLOOM
                "NGRAM", "NGRAM_BLOOM" -> IndexKind.SKIP_NGRAM_BLOOM
                else -> IndexKind.ORDERED
            }
        }
        return IndexKind.ORDERED
    }

    private fun parseIdentList(): List<String> {
        val out = ArrayList<String>()
        while (true) {
            val n = ident()
            // 允许 col(10) 这样的前缀长度
            if (acceptPunct("(")) { next(); expectPunct(")") }
            acceptKeyword("ASC"); acceptKeyword("DESC")
            out.add(n)
            if (!acceptPunct(",")) break
        }
        return out
    }

    private fun parseDrop(): Statement {
        expectKeyword("DROP")
        return when {
            acceptKeyword("DATABASE") || acceptKeyword("SCHEMA") -> {
                val ifExists = parseIfExists()
                DropDatabaseStmt(ident(), ifExists)
            }
            acceptKeyword("TABLE") -> {
                val ifExists = parseIfExists()
                val names = ArrayList<String>()
                names.add(qualifiedName())
                // `DROP TABLE a, b`：**每个名字都要删**。曾经这里把后面的名字
                // 吃掉丢掉，于是"删两张表"只删了一张还返回成功（见 DropTableStmt）
                while (acceptPunct(",")) names.add(qualifiedName())
                DropTableStmt(names, ifExists)
            }
            acceptKeyword("VIEW") -> {
                val ifExists = parseIfExists()
                val names = ArrayList<String>()
                names.add(qualifiedName())
                while (acceptPunct(",")) names.add(qualifiedName())
                DropViewStmt(names, ifExists)
            }
            acceptKeyword("INDEX") -> {
                // ⚠️ `DROP INDEX i ON t` **不是**删表。
                //
                // 这里曾经产出 `DropTableStmt(t, true)`（注释写"以重建表代替"），
                // 而执行器会真的 `catalog.dropTable`——**一条 DROP INDEX 静默丢掉整张表**，
                // 还返回 affectedRows=1。安全路径一直存在（`ALTER TABLE t DROP INDEX i`），
                // 两者共用同一段实现即可
                val indexName = ident()
                expectKeyword("ON")
                val table = qualifiedName()
                AlterTableStmt(table, listOf(DropIndexAction(indexName)))
            }
            else -> fail("不支持的 DROP 对象")
        }
    }

    // ── INSERT ────────────────────────────────────────────────────

    private fun parseInsert(): Statement {
        // 三种动词只差"撞到已存在的行时怎么办"（见 InsertMode）：
        // INSERT 报 1062、IGNORE 跳过、REPLACE 先删后插
        val verb = peek().text.uppercase()
        next()
        val mode = when {
            verb == "REPLACE" -> InsertMode.REPLACE
            acceptKeyword("IGNORE") -> InsertMode.IGNORE
            else -> InsertMode.INSERT
        }
        acceptKeyword("INTO")
        val table = qualifiedName()

        var columns: List<String>? = null
        if (peek().isPunct("(") && !peekAheadKeyword("SELECT")) {
            next()
            columns = parseIdentList()
            expectPunct(")")
        }
        // `INSERT INTO t (a,b) SELECT …`：来源是查询而不是 VALUES 列表
        if (peek().isKeyword("SELECT")) {
            return InsertSelectStmt(table, columns, parseSelect(), mode)
        }
        expectKeyword("VALUES")
        val rows = ArrayList<List<Expr>>()
        while (true) {
            expectPunct("(")
            val values = ArrayList<Expr>()
            while (true) {
                values.add(parseExpr())
                if (!acceptPunct(",")) break
            }
            expectPunct(")")
            rows.add(values)
            if (!acceptPunct(",")) break
        }
        // ON DUPLICATE KEY UPDATE col = expr, ...
        val onDup = ArrayList<Pair<String, Expr>>()
        if (peek().isKeyword("ON")) {
            next()
            expectKeyword("DUPLICATE")
            expectKeyword("KEY")
            expectKeyword("UPDATE")
            while (true) {
                val col = ident()
                expectPunct("=")
                onDup.add(col to parseExpr())
                if (!acceptPunct(",")) break
            }
        }
        if (mode == InsertMode.REPLACE && onDup.isNotEmpty()) {
            fail("REPLACE INTO 不能带 ON DUPLICATE KEY UPDATE（前者的语义就是「先删后插」）")
        }
        return InsertStmt(table, columns, rows, mode, onDup)
    }

    private fun peekAheadKeyword(k: String) = tokens.getOrNull(i + 1)?.isKeyword(k) == true

    // ── SELECT ────────────────────────────────────────────────────

    /**
     * `AS OF TIMESTAMP '2026-01-01 10:00:00'` 或 `AS OF TSO 123456789`。
     *
     * 这是本系统在 SQL 上的扩展（MySQL 本身没有时间旅行）。
     * 保持 `AS OF` 这个词形是因为它在 Oracle / TiDB / Snowflake 里
     * 已经是一个稳定的直觉，使用者不需要重新学。
     */
    private fun parseAsOf(): AsOf? {
        if (!acceptKeyword("AS")) return null
        expectKeyword("OF")
        val kind = ident().uppercase()
        if (kind != "TIMESTAMP" && kind != "TSO") fail("AS OF 只支持 TIMESTAMP 或 TSO，收到 $kind")
        val t = peek()
        val text = when {
            t.type == TokenType.STRING || t.type == TokenType.NUMBER -> { next(); t.text }
            else -> fail("AS OF $kind 需要一个时间值，实际是 $t")
        }
        return AsOf(kind, text)
    }

    /**
     * `SELECT ... [UNION [ALL] SELECT ...]*`。
     *
     * 左结合折叠成一棵 [UnionStmt] 树；MySQL 的优先级是 `UNION` 低于一切其它子句，
     * 所以每侧都是一个完整的 `SELECT`。
     */
    private fun parseUnion(): Statement {
        // 先试探性解析第一个分支；只有确认后面跟着 UNION 时才按"并集分支"处理
        val first = parseSelect()
        if (!peek().isKeyword("UNION")) return first

        var branches = listOf(first)
        // ★ `ALL` 是**每条 UNION 边**各自的属性，不是整个语句的属性。
        //
        // 这里曾经用**一个** `all` 变量，循环里被反复覆盖，最后给每条边都用同一个值：
        // `A UNION B UNION ALL C` 会把两条边都当成 ALL（重复行多出来），
        // `A UNION ALL B UNION C` 则把两条边都当成 DISTINCT（重复行被误删）。
        // 两者都不报错，只是行数不对——而"行数不对"正是使用者最难发现的那类差异
        val allFlags = ArrayList<Boolean>()
        while (peek().isKeyword("UNION")) {
            next()
            allFlags.add(
                when {
                    acceptKeyword("ALL") -> true
                    acceptKeyword("DISTINCT") -> false
                    else -> false
                },
            )
            branches = branches + parseSelect(stopBeforeOrderBy = true)
        }
        // 并集级的 ORDER BY / LIMIT：位置在最后一个分支之后，语义作用于**整个并集**
        val orderBy = ArrayList<OrderItem>()
        if (acceptKeyword("ORDER")) {
            expectKeyword("BY")
            while (true) {
                val e = parseExpr()
                val asc = if (acceptKeyword("DESC")) false else { acceptKeyword("ASC"); true }
                orderBy.add(OrderItem(e, asc))
                if (!acceptPunct(",")) break
            }
        }
        var limit: Int? = null
        var offset: Int? = null
        if (acceptKeyword("LIMIT")) {
            val n1 = next().text.toIntOrNull() ?: fail("LIMIT 应为整数")
            if (acceptPunct(",")) {
                offset = n1
                limit = next().text.toIntOrNull() ?: fail("LIMIT 应为整数")
            } else {
                limit = n1
                if (acceptKeyword("OFFSET")) offset = next().text.toIntOrNull() ?: fail("OFFSET 应为整数")
            }
        }
        // NOTE：第一个分支可能已经带了 ORDER BY / LIMIT（它是在我们知道后面有 UNION 之前
        // 解析的）。那种写法在 MySQL 里其实是**语法错误**（子句要写在最后），
        // 所以这里明确报错，而不是把它的排序悄悄丢掉
        val firstSelect = first as? SelectStmt
        if (firstSelect != null &&
            (firstSelect.orderBy.isNotEmpty() || firstSelect.limit != null || firstSelect.offset != null)
        ) {
            fail(
                "UNION 的 ORDER BY / LIMIT 必须写在**最后一个分支之后**（作用于整个并集）。" +
                    "写在第一个分支里在 MySQL 里也是语法错误",
            )
        }
        branches.drop(1).forEach { b ->
            if (b !is SelectStmt || b.orderBy.isNotEmpty() || b.limit != null) {
                fail("UNION 的分支内部不能带 ORDER BY / LIMIT")
            }
        }
        var stmt: Statement = branches[0]
        branches.drop(1).forEachIndexed { i, b -> stmt = UnionStmt(stmt, b, allFlags[i]) }
        return if (orderBy.isEmpty() && limit == null && offset == null) stmt
        else (stmt as UnionStmt).copy(orderBy = orderBy, limit = limit, offset = offset)
    }

    private fun parseSelect(stopBeforeOrderBy: Boolean = false): Statement {
        expectKeyword("SELECT")
        // DISTINCT 是语义，不是提示：丢掉它查询照常返回，只是**重复行还在**。
        // 这里曾经直接 `acceptKeyword("DISTINCT")` 把结果扔掉
        val distinct = acceptKeyword("DISTINCT")
        acceptKeyword("ALL")     // `SELECT ALL` 是默认行为，接受即可
        acceptKeyword("DISTINCTROW")   // `DISTINCT` 的别名

        // ★ **SELECT 的查询修饰词**（MySQL 的调度/缓存提示）。
        //
        // 它们只影响"怎么算"，不影响"算出什么"，所以接受并忽略是安全的。
        // 但**必须显式接受**：`mysqldump` 导数据时发的是
        // `SELECT /*!40001 SQL_NO_CACHE */ * FROM t`——版本注释里的内容会被执行
        // （见版本注释的处理），于是 `SQL_NO_CACHE` 会落到投影位置上，
        // 被当成一个叫 SQL_NO_CACHE 的列名，报出来的错是
        // "保留字 'FROM' 不能作为列名"，与真正的原因（少认一个修饰词）隔得很远。
        //
        // `SQL_CALC_FOUND_ROWS` 是个例外：它**确实**改语义（让 FOUND_ROWS() 返回
        // 未分页前的行数）。本系统没有 FOUND_ROWS()，调用它会明确报错——
        // 于是这条路径不会静默给出错误的行数，只是提示词本身被忽略了
        while (true) {
            when {
                acceptKeyword("SQL_NO_CACHE") -> {}
                acceptKeyword("SQL_CACHE") -> {}
                acceptKeyword("SQL_CALC_FOUND_ROWS") -> {}
                acceptKeyword("HIGH_PRIORITY") -> {}
                acceptKeyword("STRAIGHT_JOIN") -> {}
                else -> break
            }
        }
        val items = ArrayList<SelectItem>()
        while (true) {
            if (peek().isSymbol("*")) { next(); items.add(SelectItem.All) }
            else {
                val e = parseExpr()
                var alias: String? = null
                // MySQL 允许 SELECT 列别名使用字符串引号（Workbench 的元数据查询会用到）。
                // 仅在别名位置接受，表名/列引用仍按标识符解析。
                if (acceptKeyword("AS")) alias = if (peek().type == TokenType.STRING) next().text else ident()
                else if (peek().type == TokenType.STRING) alias = next().text
                else if (peek().type == TokenType.IDENT && !isReservedClause(peek().text)) alias = ident()
                items.add(SelectItem.Field(e, alias))
            }
            if (!acceptPunct(",")) break
        }

        // FROM 是可选的：`SELECT 1`、`SELECT @@version` 都是合法 MySQL 语句，
        // 且客户端在连接建立阶段就会发它们。
        var table: String? = null
        var tableAlias: String? = null
        var asOf: AsOf? = null
        val joins = ArrayList<JoinClause>()
        if (acceptKeyword("FROM")) {
            table = qualifiedName()
            tableAlias = parseTableAlias()
            // AS OF 紧跟在表名之后：`SELECT ... FROM t AS OF TSO 123 WHERE ...`
            asOf = parseAsOf()
            // 逗号连接：`FROM a, b` 等价于 `CROSS JOIN`
            while (acceptPunct(",")) {
                val t2 = qualifiedName()
                joins.add(JoinClause(t2, parseTableAlias(), JoinType.CROSS, null))
            }
            // 显式 JOIN。允许链式：`FROM a JOIN b ON .. JOIN c ON ..`
            while (true) {
                val type = when {
                    peek().isKeyword("JOIN") -> JoinType.INNER
                    peek().isKeyword("INNER") -> { next(); JoinType.INNER }
                    peek().isKeyword("LEFT") -> { next(); acceptKeyword("OUTER"); JoinType.LEFT }
                    peek().isKeyword("RIGHT") -> { next(); acceptKeyword("OUTER"); JoinType.RIGHT }
                    peek().isKeyword("CROSS") -> { next(); JoinType.CROSS }
                    else -> break
                }
                expectKeyword("JOIN")
                val t2 = qualifiedName()
                val alias2 = parseTableAlias()
                var on: Expr? = null
                if (acceptKeyword("ON")) {
                    on = parseExpr()
                } else if (peek().isKeyword("USING") || peek().isKeyword("NATURAL")) {
                    // 不静默忽略：USING/NATURAL 改变的是"连接列怎么找"，
                    // 忽略它们会把外连接静默降级成笛卡尔积（结果行数爆炸且不报错）
                    fail("暂不支持 USING / NATURAL JOIN；请改用 ON 显式写出连接条件")
                } else if (type != JoinType.CROSS) {
                    // 没有 ON 的外连接在 MySQL 里是语法错误；
                    // 这里也拒绝——`a LEFT JOIN b` 少写 ON 会得到一个巨大的笛卡尔积
                    fail("${type} JOIN 需要 ON 条件（笛卡尔积请显式写 CROSS JOIN）")
                }
                joins.add(JoinClause(t2, alias2, type, on))
            }
        }

        // ★ **无 FROM 时后面照样可以带全部子句**——这里曾经直接 return，注释还写着
        // "MySQL 语义上不合法"。那是错的：MySQL 8.0 实测 `SELECT 1 LIMIT 1`、
        // `SELECT 1 ORDER BY 1`、`SELECT 1 WHERE 1=1`、`SELECT 1 HAVING 1=1` 全部合法。
        //
        // 代价不是"少支持一个语法"，而是**标准的 `mysql` 命令行客户端根本连不上**：
        // 它在每次连接建立时都会发 `select @@version_comment limit 1`，
        // 我们的解析器报 1064，客户端随后把连接丢掉——症状是
        // `ERROR 2013 Lost connection`，与真正的原因（解析器）隔了十万八千里。
        var where: Expr? = null
        if (acceptKeyword("WHERE")) where = parseExpr()

        // MySQL 的子句顺序：WHERE → GROUP BY → HAVING → ORDER BY → LIMIT。
        // 顺序写死是刻意的：`HAVING` 出现在 `GROUP BY` 之前是语法错误，
        // 而"宽容地接受任意顺序"会让 `HAVING` 悄悄按错误的语义执行
        val groupBy = ArrayList<Expr>()
        if (acceptKeyword("GROUP")) {
            expectKeyword("BY")
            while (true) {
                groupBy.add(parseExpr())
                if (!acceptPunct(",")) break
            }
        }
        var having: Expr? = null
        if (acceptKeyword("HAVING")) having = parseExpr()

        val orderBy = ArrayList<OrderItem>()
        // ⚠️ 在 `UNION` 链里的分支**不解析** ORDER BY / LIMIT：
        // 那两个子句在 MySQL 里作用于**整个并集**，而它们在语法上紧跟在最后一个分支后面。
        // 让分支把它们吃掉，就会出现"只排右边那一半然后拼接"——
        // 排序错了但结果看起来完全正常。见 [parseUnion] 与 [UnionStmt]。
        if (stopBeforeOrderBy) {
            return SelectStmt(
                items, table, where, emptyList(), null, null, asOf,
                groupBy = groupBy, having = having, joins = joins, tableAlias = tableAlias,
                // `UNION` 的分支自带集合语义：`UNION` 是去重的，
                // 所以分支上的 DISTINCT 不再额外做事（`UNION ALL` 也不会因此去重）
                distinct = distinct,
            )
        }
        if (acceptKeyword("ORDER")) {
            expectKeyword("BY")
            while (true) {
                val e = parseExpr()
                val asc = if (acceptKeyword("DESC")) false else { acceptKeyword("ASC"); true }
                orderBy.add(OrderItem(e, asc))
                if (!acceptPunct(",")) break
            }
        }

        var limit: Int? = null
        var offset: Int? = null
        if (acceptKeyword("LIMIT")) {
            val first = next()
            val n1 = first.text.toIntOrNull() ?: fail("LIMIT 应为整数")
            if (acceptPunct(",")) {
                offset = n1
                val t = next(); limit = t.text.toIntOrNull() ?: fail("LIMIT 应为整数")
            } else {
                limit = n1
                if (acceptKeyword("OFFSET")) {
                    val t = next(); offset = t.text.toIntOrNull() ?: fail("OFFSET 应为整数")
                }
            }
        }

        // 行锁：`FOR UPDATE` / `LOCK IN SHARE MODE`（设计文档 §4.4、§7.3）
        val lockMode = when {
            peek().isKeyword("FOR") && tokens.getOrNull(i + 1)?.isKeyword("UPDATE") == true -> {
                next(); next()
                // `FOR UPDATE NOWAIT` / `SKIP LOCKED` / `OF t` 都会改变语义，
                // 静默忽略等于给了一个"看起来加锁了、其实没有"的事务
                rejectLockModifiers()
                com.cloudea.yangbase.txn.LockMode.EXCLUSIVE
            }
            peek().isKeyword("LOCK") && tokens.getOrNull(i + 1)?.isKeyword("IN") == true -> {
                next(); next()
                expectKeyword("SHARE"); expectKeyword("MODE")
                rejectLockModifiers()
                com.cloudea.yangbase.txn.LockMode.SHARED
            }
            else -> null
        }

        return SelectStmt(
            items, table, where, orderBy, limit, offset, asOf,
            groupBy = groupBy, having = having, joins = joins, tableAlias = tableAlias,
            distinct = distinct,
            lockMode = lockMode,
        )
    }

    /** `FOR UPDATE` 后面的修饰符本系统不支持——必须报错而不是忽略。 */
    private fun rejectLockModifiers() {
        val t = peek()
        if (t.type == TokenType.IDENT && t.text.uppercase() in setOf("NOWAIT", "SKIP", "OF")) {
            fail("暂不支持 FOR UPDATE 的修饰符 '${t.text}'（NOWAIT / SKIP LOCKED / OF 都会改变加锁语义）；请去掉它")
        }
    }

    /**
     * 表别名：`t AS x` 或 `t x`。
     *
     * 无 `AS` 的形式必须靠关键字名单挡一下——否则 `FROM t WHERE ...` 会把
     * `WHERE` 当成别名吃掉，然后整个查询按"表叫 WHERE"去解析，
     * 报出来的错与真正的问题毫无关系。
     */
    private fun parseTableAlias(): String? {
        // ⚠️ `AS OF` 是时间旅行子句，不是别名。少了这一条，
        // `FROM t AS OF TIMESTAMP '...'` 会把 **OF 当成表别名**吃掉，
        // 然后 `AS OF` 的解析什么也看不到——报出来的错是"不支持或位置错误的子句:
        // 'TIMESTAMP ...'"，与真正的原因（别名解析）差了十万八千里
        if (peek().isKeyword("AS") && tokens.getOrNull(i + 1)?.isKeyword("OF") == true) return null
        if (acceptKeyword("AS")) return ident()
        val t = peek()
        if (t.type == TokenType.IDENT && !isTableClauseKeyword(t.text)) return ident()
        return null
    }

    /**
     * 子句关键字：出现在一个名字之后时，说明"名字已经结束、子句开始了"，
     * 因此不能被当作隐式别名吃掉。
     *
     * ⚠️ `LOCK`：`SELECT ... FROM t LOCK IN SHARE MODE` 少了它，
     * `LOCK` 会被当成**表别名**吃掉，`IN SHARE MODE` 变成语法错误。
     * ⚠️ `UNION` / `EXCEPT` / `INTERSECT`：`FROM users UNION SELECT ...`
     * 少了它，`UNION` 会被当成表别名吃掉。
     *
     * 两者的共同症状都是：报错位置指向后面很远的地方，而原因在别名解析。
     */
    private val CLAUSE_KEYWORDS = setOf(
        "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "AS", "FOR", "INTO",
        "JOIN", "INNER", "LEFT", "RIGHT", "CROSS", "OUTER", "ON", "USING", "NATURAL",
        "UNION", "EXCEPT", "INTERSECT", "LOCK",
        // 这些是**表达式内部**的保留词：`SELECT 1 AND 2`、`SELECT x IS NULL`
        // 里它们跟在列名之后，同样不能被当成别名
        "AND", "OR", "NOT", "XOR", "IS", "IN", "LIKE", "BETWEEN", "DIV", "MOD",
        "WHEN", "THEN", "ELSE", "END", "ASC", "DESC", "SEPARATOR",
    )

    private fun isTableClauseKeyword(s: String) = CLAUSE_KEYWORDS.any { it.equals(s, ignoreCase = true) }

    /**
     * 选择列表里**不能当隐式别名**的词。
     *
     * 必须与 [isTableClauseKeyword] 对齐：两个解析位置（列表项之后、表名之后）
     * 面对的是同一批子句词。曾经这里只有 9 个词、而表别名那份有 24 个，
     * 于是 `SELECT 10 UNION SELECT '10'` 把 **UNION 当成了 `10` 的别名**——
     * 语句在那里就"结束"了，真正的报错落在第二个 SELECT 上
     * （"不支持或位置错误的子句"），与原因隔了十万八千里。
     *
     * 无 FROM 的 `SELECT 1 UNION SELECT 2` 因此一直是坏的，而有 FROM 的
     * `SELECT a FROM t UNION ...` 是好的——正是这个不对称让它躲过了测试。
     */
    private fun isReservedClause(s: String) = CLAUSE_KEYWORDS.any { it.equals(s, ignoreCase = true) }

    // ── UPDATE / DELETE ───────────────────────────────────────────

    private fun parseUpdate(): Statement {
        expectKeyword("UPDATE")
        val table = qualifiedName()
        expectKeyword("SET")
        val assigns = ArrayList<Pair<String, Expr>>()
        while (true) {
            val col = ident()
            if (!peek().isSymbol("=")) fail("SET 子句期望 '='")
            next()
            assigns.add(col to parseExpr())
            if (!acceptPunct(",")) break
        }
        var where: Expr? = null
        if (acceptKeyword("WHERE")) where = parseExpr()
        return UpdateStmt(table, assigns, where)
    }

    private fun parseDelete(): Statement {
        expectKeyword("DELETE")
        acceptKeyword("FROM")
        val table = qualifiedName()
        var where: Expr? = null
        if (acceptKeyword("WHERE")) where = parseExpr()
        return DeleteStmt(table, where)
    }

    // ── SHOW / SET ────────────────────────────────────────────────

    /**
     * `SHOW <what> [FROM|IN <table>] [LIKE '...']`。
     *
     * 支持两段式与三段式：`SHOW CREATE TABLE t`、`SHOW INDEX FROM t`、
     * `SHOW COLUMNS FROM t FROM db`、`SHOW FULL TABLES`。
     * 解析不出来的部分仍然照吃，但会**记下对象名**——把它丢掉会让
     * `SHOW CREATE TABLE` 退化成「查到了但内容为空」的静默失败。
     */
    private fun parseShow(): Statement {
        expectKeyword("SHOW")
        // SHOW FULL TABLES / SHOW GLOBAL STATUS 之类的修饰词。
        // ⚠️ `FULL` 对 `SHOW TABLES` 是**有语义的**：它会多出一列 `Table_type`
        // （`BASE TABLE` / `VIEW`），而 mysqldump 正是靠这一列区分表与视图；
        // 少了它，客户端读到的 row[1] 是 NULL —— 实测会让 mysqldump **段错误**
        val full = acceptKeyword("FULL")
        acceptKeyword("GLOBAL")
        acceptKeyword("SESSION")
        var what = ident().uppercase()

        // ★ `SHOW TABLE STATUS [FROM db] [LIKE 'x']` 与 `SHOW TABLES` / `SHOW STATUS`
        // 都是不同的语句，必须在这里分开——否则它会被当成一个叫 "TABLE" 的语句
        // 落到"暂不支持"那一档。它是 **mysqldump 逐表取元数据**用的
        // （`show table status like 't'`），少了它任何导出工具都在第一张表上停住
        if (what == "TABLE" && peek().isKeyword("STATUS")) {
            next()
            what = "TABLE_STATUS"
        }

        var name: String? = null
        // `SHOW CREATE TABLE t` / `SHOW INDEX FROM t` / `SHOW COLUMNS FROM t`
        if (acceptKeyword("FROM") || acceptKeyword("IN")) {
            if (peek().type == TokenType.IDENT) name = qualifiedName()
        } else if (what == "CREATE" || what == "INDEX" || what == "KEYS" || what == "COLUMNS" || what == "FIELDS") {
            // `SHOW CREATE TABLE t`：对象类型词在中间
            if (peek().type == TokenType.IDENT) {
                next()   // TABLE / INDEX / KEYS / COLUMNS / FIELDS
                if (peek().type == TokenType.IDENT) name = qualifiedName()
            }
        }

        var like: String? = null
        if (acceptKeyword("LIKE")) {
            val t = next()
            if (t.type != TokenType.STRING) fail("LIKE 后应为字符串")
            like = t.text
        }
        if (acceptKeyword("WHERE")) {
            // `SHOW ... WHERE ...`（information_schema 风格的过滤）：暂不支持，
            // 明确吃掉并忽略——它只影响过滤，不影响结果集的形状
            while (!atEnd() && !peek().isPunct(";")) next()
        }
        // 其余附加词
        while (!atEnd() && !peek().isPunct(";")) next()
        return ShowStmt(what, like, name, full)
    }

    private fun parseSet(): Statement {
        expectKeyword("SET")
        acceptKeyword("SESSION"); acceptKeyword("GLOBAL"); acceptKeyword("LOCAL")
        // SET NAMES utf8mb4 / SET autocommit=1 等
        val name = next().text
        // `SET [SESSION|GLOBAL] TRANSACTION ISOLATION LEVEL <level>`
        // （以及 `SET TRANSACTION READ ONLY`）：这是**语法形式**而不是 `变量=值`。
        // 连接池与 JDBC 的 transactionIsolation 配置都会在连接建立时发它，
        // 所以必须解析得出来——否则客户端连不上（或更糟：静默按别的级别跑）
        if (name.equals("TRANSACTION", true)) {
            if (acceptKeyword("ISOLATION")) {
                expectKeyword("LEVEL")
                // `READ COMMITTED` / `READ UNCOMMITTED` / `REPEATABLE READ` 是**两个词**。
                // 只取第一个词会把四档里的三档统统变成 "READ"，于是
                // `@@yangbase_isolation_degraded` 对不同的请求回报同一个值
                val level = if (atEnd() || peek().isPunct(";")) "REPEATABLE-READ"
                else {
                    val first = next().text.uppercase()
                    val second = if (first == "READ" || first == "REPEATABLE") {
                        if (!atEnd() && !peek().isPunct(";")) next().text.uppercase() else ""
                    } else ""
                    if (second.isEmpty()) first else "$first-$second"
                }
                while (!atEnd() && !peek().isPunct(";")) next()
                return SetStmt("transaction_isolation", Literal(Datum.Str(level)))
            }
            // `SET TRANSACTION READ ONLY` / `READ WRITE`：本系统没有只读事务的
            // 强制实现，但收下它（同样是连接初始化 SQL 的常见内容）
            while (!atEnd() && !peek().isPunct(";")) next()
            return SetStmt("transaction_read_only", Literal(Datum.Str("accepted")))
        }
        // `:=` 与 `=` 都是赋值（MySQL 两种都收）。必须先在这里认出来，
        // 否则 `SET @c := 7` 会落进下面"无等号"那一支（`SET NAMES utf8mb4` 的形态），
        // 把 `:=` 本身当成值——现象是 `SELECT @c` 返回字符串 ":="（真发生过）
        val assignOp = peek().isSymbol("=") || (peek().type == TokenType.OP && peek().text == ":=")
        if (!assignOp) {
            // ★ 无 `=` 的形式（`SET NAMES utf8mb4`、`SET NAMES utf8mb4 COLLATE ...`）
            // **必须把值读出来**。此前这里是一句「读到分号为止」然后塞一个空字符串，
            // 于是 `SET NAMES gbk` 与 `SET NAMES utf8mb4` 在服务端看起来一模一样——
            // 都是空值。字符集正好是那种「值被丢掉也不会报错、只会乱码」的设置
            // （§11.1.1 陷阱 T1）。
            val value = if (atEnd() || peek().isPunct(";") || peek().isPunct(")")) {
                // `SET NAMES` 不带值：MySQL 的写法是 SET NAMES DEFAULT
                Literal(Datum.Str("default"))
            } else {
                Literal(Datum.Str(next().text))
            }
            // `COLLATE x` 只是进一步限定排序规则；字符集本身已经拿到，跳过即可
            if (peek().isKeyword("COLLATE")) { next(); if (!atEnd()) next() }
            while (!atEnd() && !peek().isPunct(";")) next()
            return SetStmt(name, value)
        }
        // `:=` 与 `=` 等价（MySQL 两种都收）；词法器把 `:=` 切成了单独的 OP
        if (peek().type == TokenType.OP && peek().text == ":=") next()
        else if (!acceptPunct("=")) fail("SET 期望 '=' 或 ':='，实际是 ${peek()}")
        // ★ 系统变量的值是**裸标识符**时按字符串处理：`SET character_set_client = utf8mb4`、
        // `SET sql_mode = STRICT_TRANS_TABLES`、`SET autocommit = ON` 都是这么写的
        // （mysqldump 的产物里成片出现）。当成列引用去求值只会得到 NULL，
        // 随后在别处以"Unknown character set: ''"这种完全指错方向的方式报出来。
        // 带括号的（`now()` 之类）仍然是表达式
        val v = if (peek().type == TokenType.IDENT &&
            !peek().text.startsWith("@") &&
            tokens.getOrNull(i + 1)?.isSymbol("(") != true
        ) {
            Literal(Datum.Str(next().text))
        } else {
            parseExpr()
        }
        while (!atEnd() && !peek().isPunct(";")) next()
        return SetStmt(name, v)
    }

    // ── 表达式（优先级从低到高）────────────────────────────────────

    fun parseExpr(): Expr = parseOr()

    private fun parseOr(): Expr {
        var left = parseAnd()
        while (peek().isKeyword("OR") || peek().isOp("||")) {
            next(); left = BinaryOp("OR", left, parseAnd())
        }
        return left
    }

    private fun parseAnd(): Expr {
        var left = parseNot()
        while (peek().isKeyword("AND") || peek().isOp("&&")) {
            next(); left = BinaryOp("AND", left, parseNot())
        }
        return left
    }

    private fun parseNot(): Expr {
        if (acceptKeyword("NOT")) return UnaryOp("NOT", parseNot())
        return parseComparison()
    }

    private fun parseComparison(): Expr {
        val left = parseAdditive()
        val t = peek()
        when {
            // ⚠️ `<=>` 是 MySQL 的**空值安全等于**，与 `=` 不是一回事：
            // `NULL <=> NULL` 为真、`1 <=> NULL` 为假，而 `=` 在 NULL 上全是"未知"。
            // 此前这里把 `<=>` 直接降级成 `=`，于是 `NULL <=> NULL` 返回 NULL——
            // 一个写法正确、语义被悄悄换掉的查询
            t.isSymbol("=") -> { next(); return BinaryOp("=", left, parseAdditive()) }
            t.isOp("<=>") -> { next(); return BinaryOp("<=>", left, parseAdditive()) }
            t.isOp("<>") || t.isOp("!=") -> { next(); return BinaryOp("<>", left, parseAdditive()) }
            t.isOp("<") -> { next(); return BinaryOp("<", left, parseAdditive()) }
            t.isOp("<=") -> { next(); return BinaryOp("<=", left, parseAdditive()) }
            t.isOp(">") -> { next(); return BinaryOp(">", left, parseAdditive()) }
            t.isOp(">=") -> { next(); return BinaryOp(">=", left, parseAdditive()) }
            t.isKeyword("IS") -> {
                next()
                val neg = acceptKeyword("NOT")
                if (acceptKeyword("NULL")) return IsNull(left, neg)
                fail("只支持 IS [NOT] NULL")
            }
            t.isKeyword("IN") -> {
                next(); expectPunct("(")
                if (peek().isKeyword("SELECT")) {
                    val sub = parseSelect()
                    expectPunct(")")
                    return InSubquery(left, sub, negated = false)
                }
                val vs = ArrayList<Expr>()
                while (true) { vs.add(parseExpr()); if (!acceptPunct(",")) break }
                expectPunct(")")
                return InList(left, vs, negated = false)
            }
            t.isKeyword("BETWEEN") -> {
                next()
                val low = parseAdditive()
                expectKeyword("AND")
                val high = parseAdditive()
                return Between(left, low, high, negated = false)
            }
            t.isKeyword("LIKE") -> { next(); return Like(left, parseAdditive(), negated = false) }
            t.isKeyword("NOT") -> {
                // NOT IN / NOT BETWEEN / NOT LIKE
                if (tokens.getOrNull(i + 1)?.isKeyword("IN") == true) {
                    next(); next(); expectPunct("(")
                    if (peek().isKeyword("SELECT")) {
                        val sub = parseSelect()
                        expectPunct(")")
                        return InSubquery(left, sub, negated = true)
                    }
                    val vs = ArrayList<Expr>()
                    while (true) { vs.add(parseExpr()); if (!acceptPunct(",")) break }
                    expectPunct(")")
                    return InList(left, vs, negated = true)
                }
                if (tokens.getOrNull(i + 1)?.isKeyword("BETWEEN") == true) {
                    next(); next()
                    val low = parseAdditive()
                    expectKeyword("AND")
                    val high = parseAdditive()
                    return Between(left, low, high, negated = true)
                }
                if (tokens.getOrNull(i + 1)?.isKeyword("LIKE") == true) {
                    next(); next()
                    return Like(left, parseAdditive(), negated = true)
                }
                if (tokens.getOrNull(i + 1)?.isKeyword("EXISTS") == true &&
                    tokens.getOrNull(i + 2)?.isPunct("(") == true
                ) {
                    next(); next(); next()
                    val sub = parseSelect()
                    expectPunct(")")
                    return ExistsSubquery(sub, negated = true)
                }
            }
        }
        return left
    }

    private fun parseAdditive(): Expr {
        var left = parseMultiplicative()
        while (peek().isOp("+") || peek().isOp("-")) {
            val op = next().text
            left = BinaryOp(op, left, parseMultiplicative())
        }
        // `->` / `->>` 的优先级**高于比较**（MySQL 也是如此）：
        // `WHERE j -> '$.a' = 1` 必须解析成 `(j -> '$.a') = 1`。
        // 放在比较那一层的循环里会得到 `j -> ('$.a' = 1)`——一个不报错、
        // 只是永远算错的解析结果
        while (peek().isOp("->") || peek().isOp("->>")) {
            val op = next().text
            left = BinaryOp(op, left, parseMultiplicative())
        }
        return left
    }

    private fun parseMultiplicative(): Expr {
        var left = parseUnary()
        while (peek().isOp("*") || peek().isOp("/") || peek().isOp("%")) {
            val op = next().text
            left = BinaryOp(op, left, parseUnary())
        }
        return left
    }

    private fun parseUnary(): Expr {
        if (peek().isOp("-")) {
            next()
            // 负号紧跟一个**纯整数**时，按带符号的文本来解析。
            //
            // 必须这么做的原因只有一个，但它足够硬：`-9223372036854775808` 是 i64 的
            // 最小值，而它的**无符号部分** 2^63 已经超出 i64——先解析正数再取负，
            // 中间那一步只能退化成浮点，于是这个合法的最小值会被静默地表示成
            // 一个丢了精度的双精度数
            val t = peek()
            if (t.type == TokenType.NUMBER && t.text.none { it == '.' || it == 'e' || it == 'E' } &&
                !t.text.startsWith("0x") && !t.text.startsWith("0X")
            ) {
                ("-" + t.text).toLongOrNull()?.let {
                    next()
                    return Literal(Datum.I64(it))
                }
            }
            val operand = parseUnary()
            // 数字字面量的负号在**解析期**就折进去。
            //
            // 两个理由，都不是风格问题：
            // ① `-9223372036854775808`（BIGINT 的最小值）若是"先取 2^63 再取负"，
            //    那 2^63 的字面量本身就超出 i64 范围，只能退化成浮点——于是这个
            //    合法的最小值会被静默地按双精度表示（丢精度）。
            // ② 折进去之后它才是一个 `Literal`，剪枝（[PruneRanges]）才认得出
            //    `WHERE x > -1` 的下界，否则那条约束根本不参与块级剪枝。
            if (operand is Literal) {
                return when (val v = operand.value) {
                    is Datum.I64 -> if (v.v == Long.MIN_VALUE) fail("整数字面量超出 BIGINT 范围") else Literal(Datum.I64(-v.v))
                    // 2^63 已经写成浮点了（见 numberLiteral 的拒绝范围），这里只是保持数值正确
                    is Datum.F64 -> Literal(Datum.F64(-v.v))
                    is Datum.Dec -> Literal(Datum.Dec(-v.unscaled, v.scale))
                    else -> UnaryOp("-", operand)
                }
            }
            return UnaryOp("-", operand)
        }
        if (peek().isOp("+")) { next(); return parseUnary() }
        return parsePrimary()
    }

    private fun parsePrimary(): Expr {
        val t = peek()
        // MATCH (col, ...) AGAINST ('...' [IN BOOLEAN MODE])
        if (t.isKeyword("MATCH")) return parseMatchAgainst()
        return when {
            t.type == TokenType.NUMBER -> { next(); numberLiteral(t.text) }
            t.type == TokenType.STRING -> { next(); Literal(Datum.Str(t.text)) }
            t.isPunct("(") -> {
                next()
                // 括号里是 SELECT = 标量子查询：`WHERE id = (SELECT MAX(id) FROM t)`
                if (peek().isKeyword("SELECT")) {
                    val start = i          // 指向 SELECT；用它取原文做"预计算值表"的键
                    val sub = parseSelect()
                    expectPunct(")")
                    val text = tokens.subList(start, i - 1).joinToString(" ") { it.raw }
                    return ScalarSubquery(sub, text)
                }
                val e = parseExpr()
                expectPunct(")")
                e
            }
            t.type == TokenType.IDENT -> {
                val name = t.text
                // 聚合函数
                if (name.equals("COUNT", true) && tokens.getOrNull(i + 1)?.isPunct("(") == true) {
                    next(); next()
                    val distinct = acceptKeyword("DISTINCT")
                    if (peek().isSymbol("*")) {
                        next(); expectPunct(")")
                        return maybeWindow(CountCall(null, distinct))
                    }
                    val a = parseExpr()
                    expectPunct(")")
                    return maybeWindow(CountCall(a, distinct))
                }
                // 其余聚合：SUM / AVG / MIN / MAX / GROUP_CONCAT。
                // 它们与 COUNT 分开解析是因为 COUNT 有独立的元数据路径（见 CountCall）。
                if (name.uppercase() in setOf("SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT") &&
                    tokens.getOrNull(i + 1)?.isPunct("(") == true
                ) {
                    next(); next()
                    val distinct = acceptKeyword("DISTINCT")
                    val a = parseExpr()
                    expectPunct(")")
                    return maybeWindow(AggCall(name.uppercase(), a, distinct))
                }
                if (name.equals("EXISTS", true) && tokens.getOrNull(i + 1)?.isPunct("(") == true) {
                    next(); next()
                    val sub = parseSelect()
                    expectPunct(")")
                    return ExistsSubquery(sub, negated = false)
                }
                // ★ CASE [operand] WHEN c THEN r … [ELSE e] END
                //
                // 必须在通用函数分支与保留字检查**之前**处理：`CASE` 在保留字表里，
                // 走到最后那一步会报"保留字 'CASE' 不能作为列名"——能拦住，但文案
                // 完全没提"这个语法还不支持"，用户会以为是自己写错了
                if (name.equals("CASE", true) && !t.quoted) return parseCase()
                if (name.equals("CAST", true) && tokens.getOrNull(i + 1)?.isPunct("(") == true) {
                    return parseCast()
                }
                if (name.equals("NULL", true)) { next(); return Literal(Datum.Null) }
                if (name.equals("TRUE", true)) { next(); return Literal(Datum.Bool(true)) }
                if (name.equals("FALSE", true)) { next(); return Literal(Datum.Bool(false)) }
                if (name.equals("CURRENT_TIMESTAMP", true) || name.equals("NOW", true)) {
                    next()
                    if (acceptPunct("(")) { if (acceptPunct(")")) { } else { while (!peek().isPunct(")") && !atEnd()) next(); expectPunct(")") } }
                    return FuncCall("NOW", emptyList())
                }
                // 普通函数调用
                if (tokens.getOrNull(i + 1)?.isPunct("(") == true) {
                    next(); next()
                    // `TRIM([BOTH|LEADING|TRAILING] 'x' FROM s)` 是**语法**不是函数调用：
                    // 它的参数用 FROM 分隔，当普通参数解析会报"期望 ')'"，
                    // 而用户（尤其做数据清洗时）写的就是这个形式
                    if (name.equals("TRIM", true) &&
                        (peek().isKeyword("BOTH") || peek().isKeyword("LEADING") || peek().isKeyword("TRAILING"))
                    ) {
                        val side = next().text.uppercase()
                        val what = parseExpr()
                        expectKeyword("FROM")
                        val target = parseExpr()
                        expectPunct(")")
                        // 折成三参调用：TRIM_SIDE(值, 要去掉的字符, 方向)
                        return FuncCall("TRIM_SIDE", listOf(target, what, Literal(Datum.Str(side))))
                    }
                    val distinct = acceptKeyword("DISTINCT")
                    val args = ArrayList<Expr>()
                    if (!peek().isPunct(")")) {
                        while (true) {
                            // `DATE_ADD(d, INTERVAL 1 DAY)`：INTERVAL 也是语法，
                            // 折成两个参数（数值 + 单位字面量）交给三参的 DATE_ADD
                            when {
                                peek().isKeyword("INTERVAL") -> {
                                    next()
                                    args.add(parseExpr())
                                    val unit = next().text.uppercase()
                                    args.add(Literal(Datum.Str(unit)))
                                }
                                // `TIMESTAMPDIFF(HOUR, a, b)` 里的单位是**关键字**不是列名。
                                // 当普通标识符解析会变成 `ColumnRef("HOUR")`，
                                // 求值时报"无法在此处引用列 'HOUR'"——错得让人摸不着头脑
                                name.uppercase() in UNIT_FIRST_FUNCTIONS && args.isEmpty() -> {
                                    val unit = next().text.uppercase()
                                    if (unit !in TIME_UNITS) {
                                        fail(
                                            "'$unit' 不是合法的时间单位。" +
                                                "支持 MICROSECOND/SECOND/MINUTE/HOUR/DAY/WEEK/MONTH/QUARTER/YEAR",
                                        )
                                    }
                                    args.add(Literal(Datum.Str(unit)))
                                }
                                else -> args.add(parseExpr())
                            }
                            if (!acceptPunct(",")) break
                        }
                    }
                    expectPunct(")")
                    return maybeWindow(FuncCall(name.uppercase(), args, distinct))
                }
                // 系统变量 / 用户变量：@@version、@@session.autocommit、@user_var
                if (name.startsWith("@")) {
                    next()
                    var full = name
                    // @@session.x / @@global.x / @@x.y —— 把限定部分一并吃掉
                    if (acceptPunct(".")) full += "." + next().text
                    if (acceptPunct(".")) full += "." + next().text
                    return VarRef(full)
                }
                // 走到这里说明它既不是函数调用也不是字面量，只能当列名用。
                // 此时保留字必须报错：`SELECT FROM WHERE` 若被解析成
                // 「一个叫 FROM 的列」就会静默通过，比报错危险得多。
                if (!t.quoted && RESERVED.contains(name.uppercase())) {
                    fail("保留字 '$name' 不能作为列名")
                }
                next()
                if (acceptPunct(".")) {
                    val col = next().text
                    return ColumnRef(col, name)
                }
                return ColumnRef(name)
            }
            else -> fail("无法解析的表达式，遇到 ${t}")
        }
    }

    /**
     * `函数(...) OVER ( 分区 / 排序 / 帧 )`。
     *
     * ## 没有 `OVER` 时窗口函数必须报错
     *
     * `SELECT ROW_NUMBER() FROM t` 在 MySQL 里是语法错误。放过去的话它会走到
     * 普通函数的求值路径——那里没有这个名字（`Eval.evalFunction` 找不到就报错，
     * 或者更糟：被当成某个同名用户函数）——总之错误信息会指向一个无关的地方。
     * 这里直接说清楚"它是窗口函数，要写 OVER"。
     */
    /**
     * `CASE [operand] WHEN … THEN … [ELSE …] END`。
     *
     * 与 MySQL 对齐：简单式（`CASE x WHEN 1 THEN …`，等价于 `x = 1`）与搜索式
     * （`CASE WHEN x > 1 THEN …`）共用一套语法；`ELSE` 可省（没有匹配时结果是 NULL）；
     * 分支可以有很多个。
     */
    private fun parseCase(): Expr {
        expectKeyword("CASE")
        // 简单式 vs 搜索式的判别：紧跟 CASE 的是 WHEN 就是搜索式
        val operand = if (peek().isKeyword("WHEN")) null else parseExpr()
        val whens = ArrayList<WhenClause>()
        while (acceptKeyword("WHEN")) {
            val cond = parseExpr()
            expectKeyword("THEN")
            whens.add(WhenClause(cond, parseExpr()))
        }
        if (whens.isEmpty()) fail("CASE 至少要有一个 WHEN 分支")
        val elseExpr = if (acceptKeyword("ELSE")) parseExpr() else null
        expectKeyword("END")
        return CaseExpr(operand, whens, elseExpr)
    }

    /**
     * `CAST(expr AS type[(p[,s])])`。
     *
     * 保留成一个独立节点而不是拼成函数名：类型名**不是表达式**，
     * `CAST(x AS DECIMAL(10,2))` 里的 `(10,2)` 是类型的一部分，
     * 拼字符串会在参数个数与函数名两处都出错。
     */
    private fun parseCast(): Expr {
        expectKeyword("CAST")
        expectPunct("(")
        val v = parseExpr()
        expectKeyword("AS")
        val type = parseCastTypeName()
        expectPunct(")")
        return CastTo(v, type)
    }

    /** `CAST` / `CONVERT` 里的目标类型名（含 `SIGNED INTEGER`、`DECIMAL(10,2)` 这类）。 */
    private fun parseCastTypeName(): String {
        val sb = StringBuilder(next().text.uppercase())
        // 二词类型：SIGNED INTEGER / DOUBLE PRECISION / CHARACTER VARYING
        if (sb.toString() in setOf("SIGNED", "UNSIGNED", "DOUBLE", "CHARACTER") &&
            peek().text.uppercase() in setOf("INTEGER", "INT", "PRECISION", "VARYING")
        ) {
            sb.append(' ').append(next().text.uppercase())
        }
        // 长度与精度：CHAR(10)、DECIMAL(10,2)
        if (acceptPunct("(")) {
            val parts = ArrayList<String>()
            while (!peek().isPunct(")") && !atEnd()) {
                parts.add(next().text)
                if (!acceptPunct(",")) break
            }
            expectPunct(")")
            sb.append('(').append(parts.joinToString(",")).append(')')
        }
        return sb.toString()
    }

    /** 第一个参数是**时间单位关键字**的函数。 */
    private val UNIT_FIRST_FUNCTIONS = setOf("TIMESTAMPDIFF", "TIMESTAMPADD", "EXTRACT")

    private val TIME_UNITS = setOf(
        "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK",
        "MONTH", "QUARTER", "YEAR",
    )

    private fun maybeWindow(base: Expr): Expr {
        if (!peek().isKeyword("OVER")) {
            val name = baseCallParts(base).first
            if (name in Window.WINDOW_ONLY) {
                fail("$name 是窗口函数，必须写成 $name(...) OVER (...)")
            }
            return base
        }
        next()
        // 命名窗口（`WINDOW w AS (...) ... OVER w`）不在这里：看到 OVER 后面不是 "(" 时，
        // 直接说清楚是"命名窗口"而不是报一个"期望 ( "——后者会让人以为是括号写错了
        if (!peek().isPunct("(")) {
            fail("暂不支持命名窗口（`WINDOW w AS (...)` 与 `OVER w`）；请把窗口定义直接写在 OVER ( ... ) 里")
        }
        next()
        val partitionBy = ArrayList<Expr>()
        val orderBy = ArrayList<OrderItem>()
        if (acceptKeyword("PARTITION")) {
            expectKeyword("BY")
            while (true) {
                partitionBy.add(parseExpr())
                if (!acceptPunct(",")) break
            }
        }
        if (acceptKeyword("ORDER")) {
            expectKeyword("BY")
            while (true) {
                val e = parseExpr()
                val asc = if (acceptKeyword("DESC")) false else { acceptKeyword("ASC"); true }
                orderBy.add(OrderItem(e, asc))
                if (!acceptPunct(",")) break
            }
        }
        val unit = when {
            acceptKeyword("ROWS") -> FrameUnit.ROWS
            acceptKeyword("RANGE") -> FrameUnit.RANGE
            acceptKeyword("GROUPS") -> fail("暂不支持 GROUPS 窗口帧；请改用 ROWS")
            else -> null
        }
        val frame = if (unit != null) parseWindowFrame(unit) else null
        expectPunct(")")
        val (fnName, args, distinct) = baseCallParts(base)
        return WindowCall(fnName, args, distinct, partitionBy, orderBy, frame)
    }

    /** 把一个"普通调用"拆成 `(函数名, 参数, 是否 DISTINCT)`，供窗口调用复用。 */
    private fun baseCallParts(base: Expr): Triple<String, List<Expr>, Boolean> = when (base) {
        is CountCall -> Triple("COUNT", if (base.arg == null) listOf(Star) else listOf(base.arg), base.distinct)
        is AggCall -> Triple(base.name.uppercase(), listOf(base.arg), base.distinct)
        is FuncCall -> Triple(base.name.uppercase(), base.args, base.distinct)
        else -> Triple((base as? ColumnRef)?.name?.uppercase() ?: "", listOf(base), false)
    }

    private fun parseWindowFrame(unit: FrameUnit): WindowFrame {
        var start: WindowBound
        var end: WindowBound
        if (acceptKeyword("BETWEEN")) {
            start = parseWindowBound()
            expectKeyword("AND")
            end = parseWindowBound()
        } else {
            // 单边界形式（`ROWS UNBOUNDED PRECEDING`）等价于 BETWEEN 它 AND CURRENT ROW
            start = parseWindowBound()
            end = WindowBound.CurrentRow
        }
        // RANGE 的 `n PRECEDING/FOLLOWING` 要按**排序键的值**做区间运算
        // （`RANGE 3 PRECEDING` 是"键 >= 当前键 - 3"），不是数行数。
        // 把它当 ROWS 处理会给出一批**看起来正常**的错行，所以明确拒绝
        if (unit == FrameUnit.RANGE) {
            for ((b, which) in listOf(start to "起点", end to "终点")) {
                if (b is WindowBound.Preceding || b is WindowBound.Following) {
                    fail(
                        "暂不支持 RANGE 帧的 $which $b（它要按排序键的值做区间运算，不是数行数）；" +
                            "需要按行数就用 ROWS",
                    )
                }
            }
        }
        return WindowFrame(unit, start, end)
    }

    private fun parseWindowBound(): WindowBound {
        if (acceptKeyword("UNBOUNDED")) {
            return when {
                acceptKeyword("PRECEDING") -> WindowBound.UnboundedPreceding
                acceptKeyword("FOLLOWING") -> WindowBound.UnboundedFollowing
                else -> fail("UNBOUNDED 后面应当跟 PRECEDING 或 FOLLOWING")
            }
        }
        if (acceptKeyword("CURRENT")) {
            expectKeyword("ROW")
            return WindowBound.CurrentRow
        }
        val tok = peek()
        val n = tok.text.toLongOrNull() ?: fail("窗口帧的边界应当是行数或 UNBOUNDED / CURRENT ROW，实际是 ${tok}")
        next()
        if (n < 0) fail("窗口帧的行数不能是负数：$n")
        return when {
            acceptKeyword("PRECEDING") -> WindowBound.Preceding(n)
            acceptKeyword("FOLLOWING") -> WindowBound.Following(n)
            else -> fail("窗口帧的行数后面应当跟 PRECEDING 或 FOLLOWING")
        }
    }

    private fun numberLiteral(text: String): Expr {
        if (text.startsWith("0x") || text.startsWith("0X")) {
            return Literal(Datum.I64(text.substring(2).toLong(16)))
        }
        return if (text.contains('.') || text.contains('e') || text.contains('E')) {
            Literal(Datum.F64(text.toDouble()))
        } else {
            // 纯整数字面量超出 i64 时**明确报错**，不退化成双精度：
            // 退化的表现是「9223372036854775809」被静默当成 9223372036854775808.0，
            // 数字只差 1，看起来毫无异常。本系统没有 DECIMAL 字面量，
            // 所以只能报错——而不是给一个精度已经丢了的值
            val l = text.toLongOrNull() ?: throw SqlException(
                MySqlErrors.ER_DATA_OUT_OF_RANGE, "22003",
                "整数字面量超出 BIGINT 范围：$text（本系统没有 DECIMAL 字面量）。" +
                    "把它写小一点，或加小数点显式当浮点用",
            )
            Literal(Datum.I64(l))
        }
    }

    /**
     * 不能作为裸列名的保留字。
     *
     * 缺了这道检查，`SELECT FROM WHERE` 会被解析成「一个叫 FROM 的列」——
     * 语法错误被静默吞掉，比报错危险得多。
     */
    private val RESERVED = setOf(
        "SELECT", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "BY", "LIMIT", "OFFSET",
        "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "DROP", "ALTER",
        "TABLE", "DATABASE", "INDEX", "PRIMARY", "KEY", "UNIQUE", "AND", "OR", "NOT",
        "IN", "IS", "NULL", "BETWEEN", "LIKE", "EXISTS", "JOIN", "LEFT", "RIGHT", "INNER",
        "OUTER", "ON", "UNION", "AS", "ASC", "DESC", "CASE", "WHEN", "THEN", "ELSE", "END",
        "DISTINCT", "TRANSACTION", "COMMIT", "ROLLBACK", "BEGIN", "SHOW", "USE",
    )

    companion object {

        /**
         * 只解析一个表达式（不构成完整语句）。
         *
         * 给内部改写用：连接条件在内部是以**文本**形式拼出来的（见
         * [CorrelatedSubqueries.outerExprRender]），这里把它交回给同一个解析器，
         * 而不是另写一个"表达式改写器"——那种改写器要为每种节点都写一份。
         */
        fun parseExprText(text: String): Expr = Parser(Lexer(text).tokenize()).let { p ->
            val e = p.parseExpr()
            if (!p.atEnd()) p.fail("表达式后面还有多余内容: $text")
            e
        }

        /** 解析单条语句。 */
        fun parse(sql: String): Statement = Parser(Lexer(sql).tokenize()).parseStatement()

        /** 解析多条语句（以分号分隔）。 */
        fun parseAll(sql: String): List<Statement> {
            val all = Lexer(sql).tokenize()
            val out = ArrayList<Statement>()
            var start = 0
            for (idx in all.indices) {
                if (all[idx].isPunct(";") || all[idx].type == TokenType.EOF) {
                    if (idx > start) {
                        val sub = all.subList(start, idx) + Token(TokenType.EOF, "", all[idx].pos)
                        out.add(Parser(sub).parseStatement())
                    }
                    start = idx + 1
                }
            }
            return out
        }
    }
}
