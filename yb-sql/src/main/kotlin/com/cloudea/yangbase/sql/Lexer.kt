package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException

enum class TokenType { IDENT, STRING, NUMBER, OP, PUNCT, EOF }

data class Token(
    val type: TokenType,
    /** 原始文本。标识符已去引号。 */
    val text: String,
    val pos: Int,
    /** 被反引号或引号包裹过——不做关键字解释。 */
    val quoted: Boolean = false,
    /**
     * **源码里的原样片段**（含引号、含反引号）。
     *
     * 与 [text] 的区别在字符串字面量上最要紧：`'a b'` 的 `text` 是 `a b`（去掉了引号），
     * 把它拼回 SQL 会变成两个 token。视图定义、内部生成的连接条件都要把
     * token 拼回**可再解析的 SQL**，拼错的表现是"视图建好了、一查就报语法错误"。
     */
    val raw: String = text,
) {
    fun isKeyword(k: String) = type == TokenType.IDENT && !quoted && text.equals(k, ignoreCase = true)
    fun isPunct(p: String) = type == TokenType.PUNCT && text == p
    fun isOp(o: String) = type == TokenType.OP && text.equals(o, ignoreCase = true)

    /**
     * 符号匹配，**不区分该片段被词法归为 OP 还是 PUNCT**。
     *
     * `=` 与 `*` 在词法上归入哪一类是任意的（`*` 既是乘号又是 `SELECT *`），
     * 解析器依赖这个区分就会静默出错。这里踩过一次真实的坑：`=` 被归为 PUNCT，
     * 而比较表达式用 `isOp("=")` 判断，导致 `WHERE id = 2` 中的 `= 2` 被整个丢弃，
     * WHERE 退化成「取该列的值」——所有非零行都通过过滤。
     */
    fun isSymbol(s: String) = text == s && (type == TokenType.OP || type == TokenType.PUNCT)

    override fun toString() = "${type}('$text')"
}

/**
 * MySQL 子集的分词器。
 *
 * 支持：反引号标识符、单/双引号字符串（含转义）、十进制与十六进制字面量、
 * 行注释与 C 风格块注释、多字符运算符。
 */
class Lexer(private val src: String) {


    private var pos = 0
    private val tokens = ArrayList<Token>()

    /**
     * 版本注释的**结尾位置**；-1 表示当前不在版本注释里。
     *
     * 版本注释形如「斜杠 + 星号 + 感叹号 + 版本号 + 内容 + 星号 + 斜杠」。
     * 它的内容要**照常进入词法流**（MySQL 会执行它，mysqldump 靠它输出条件 DDL），
     * 但结尾那个结束符不能当语法。记下位置、推进到那里时跳过即可——
     * 不需要改写源码，于是诊断信息里的位置仍然指向原始文本。
     */
    private var versionCommentEnd = -1

    fun tokenize(): List<Token> {
        while (pos < src.length) {
            skipWhitespaceAndComments()
            if (pos >= src.length) break
            val c = src[pos]
            when {
                c == '`' -> tokens.add(readQuotedIdent('`'))
                c == '\'' -> tokens.add(readString('\'', '"'))
                c == '"' -> tokens.add(readString('"', '"'))
                c.isDigit() || (c == '.' && pos + 1 < src.length && src[pos + 1].isDigit()) -> tokens.add(readNumber())
                c.isLetter() || c == '_' || c == '$' || c == '@' -> tokens.add(readIdent())
                else -> tokens.add(readOperatorOrPunct())
            }
        }
        tokens.add(Token(TokenType.EOF, "", pos))
        return tokens
    }

    private fun skipWhitespaceAndComments() {
        while (pos < src.length) {
            // 版本注释的收尾：内容已经当普通 SQL 解析过了，把它跳过去
            if (pos == versionCommentEnd) {
                pos += 2
                versionCommentEnd = -1
                continue
            }
            val c = src[pos]
            when {
                c.isWhitespace() -> pos++
                c == '-' && pos + 1 < src.length && src[pos + 1] == '-' &&
                    (pos + 2 >= src.length || src[pos + 2].isWhitespace()) -> {
                    while (pos < src.length && src[pos] != '\n') pos++
                }
                c == '#' -> { while (pos < src.length && src[pos] != '\n') pos++ }
                c == '/' && pos + 1 < src.length && src[pos + 1] == '*' -> {
                    pos += 2
                    // ★ `/*!40101 … */` 是**可执行的**版本注释：MySQL 会在版本号
                    // 不大于自己时执行其中的内容。mysqldump 靠它输出
                    // `/*!40101 SET @saved_cs_client = @@character_set_client */`
                    // 与 `/*!50003 CREATE TRIGGER … */` 这类条件语句。
                    //
                    // 当普通注释丢掉它们，等于把那些语句**静默跳过**——用户拿
                    // mysqldump 的产物导入本系统，会得到一份"看起来成功、
                    // 实际少了东西"的库。所以这里剥掉 `!版本号` 前缀与结尾的 `*/`，
                    // 让里面的内容**照常进入词法流**
                    if (pos < src.length && src[pos] == '!') {
                        pos++                                   // 跳过 '!'
                        while (pos < src.length && src[pos].isDigit()) pos++   // 版本号
                        // 记下结尾位置，内容照常解析（见 versionCommentEnd）
                        var end = pos
                        while (end + 1 < src.length && !(src[end] == '*' && src[end + 1] == '/')) end++
                        versionCommentEnd = if (end + 1 < src.length) end else -1
                    } else {
                        while (pos + 1 < src.length && !(src[pos] == '*' && src[pos + 1] == '/')) pos++
                        pos = minOf(pos + 2, src.length)
                    }
                }
                else -> return
            }
        }
    }

    private fun readQuotedIdent(quote: Char): Token {
        val start = pos
        pos++ // 跳过开引号
        val sb = StringBuilder()
        while (pos < src.length) {
            val c = src[pos]
            if (c == quote) {
                // 连写两个引号表示一个引号字符
                if (pos + 1 < src.length && src[pos + 1] == quote) { sb.append(quote); pos += 2; continue }
                pos++
                return Token(TokenType.IDENT, sb.toString(), start, quoted = true)
            }
            sb.append(c); pos++
        }
        throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "未闭合的反引号标识符 (位置 $start)")
    }

    private fun readString(quote: Char, altQuote: Char): Token {
        val start = pos
        pos++
        val sb = StringBuilder()
        while (pos < src.length) {
            // 版本注释的收尾：内容已经当普通 SQL 解析过了，把它跳过去
            if (pos == versionCommentEnd) {
                pos += 2
                versionCommentEnd = -1
                continue
            }
            val c = src[pos]
            when {
                c == quote -> {
                    if (pos + 1 < src.length && src[pos + 1] == quote) { sb.append(quote); pos += 2; continue }
                    pos++
                    // 原文含引号：拼回 SQL 时必须保留，否则 ``'a b'`` 会变成两个 token
                    return Token(TokenType.STRING, sb.toString(), start, raw = src.substring(start, pos))
                }
                // MySQL 的反斜杠转义
                c == '\\' && pos + 1 < src.length -> {
                    val e = src[pos + 1]
                    sb.append(
                        when (e) {
                            'n' -> '\n'; 't' -> '\t'; 'r' -> '\r'; '0' -> '\u0000'
                            '\\' -> '\\'; '\'' -> '\''; '"' -> '"'; '%' -> '%'; '_' -> '_'
                            else -> e
                        }
                    )
                    pos += 2
                }
                // 双写引号是 SQL 标准的转义
                c == altQuote && altQuote != quote -> { sb.append(c); pos++ }
                else -> { sb.append(c); pos++ }
            }
        }
        throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "未闭合的字符串 (位置 $start)")
    }

    private fun readNumber(): Token {
        val start = pos
        var isHex = false
        if (pos + 1 < src.length && src[pos] == '0' && (src[pos + 1] == 'x' || src[pos + 1] == 'X')) {
            isHex = true; pos += 2
            while (pos < src.length && (src[pos].isDigit() || src[pos] in 'a'..'f' || src[pos] in 'A'..'F')) pos++
            return Token(TokenType.NUMBER, src.substring(start, pos), start)
        }
        while (pos < src.length && src[pos].isDigit()) pos++
        if (pos < src.length && src[pos] == '.') { pos++; while (pos < src.length && src[pos].isDigit()) pos++ }
        if (pos < src.length && (src[pos] == 'e' || src[pos] == 'E')) {
            val save = pos
            pos++
            if (pos < src.length && (src[pos] == '+' || src[pos] == '-')) pos++
            if (pos < src.length && src[pos].isDigit()) { while (pos < src.length && src[pos].isDigit()) pos++ }
            else pos = save
        }
        return Token(TokenType.NUMBER, src.substring(start, pos), start)
    }

    private fun readIdent(): Token {
        val start = pos
        if (src[pos] == '@') {
            pos++
            if (pos < src.length && src[pos] == '@') pos++
        }
        while (pos < src.length && (src[pos].isLetterOrDigit() || src[pos] == '_' || src[pos] == '$')) pos++
        return Token(TokenType.IDENT, src.substring(start, pos), start)
    }

    private fun readOperatorOrPunct(): Token {
        val start = pos
        val c = src[pos]
        if (c in "(),;.=" ) {
            pos++
            return Token(if (c == ',') TokenType.PUNCT else TokenType.PUNCT, c.toString(), start)
        }
        // `:=` —— MySQL 的赋值运算符（`SET @a := 1`）。切成两个 PUNCT 由解析器合并，
        // 这样 `:` 本身也不必成为一个合法字符（它在 SQL 里没有别的用途，
        // 放行它只会让真正的语法错误晚一点被发现）
        if (c == ':') {
            if (pos + 1 < src.length && src[pos + 1] == '=') {
                pos += 2
                return Token(TokenType.OP, ":=", start)
            }
            throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "无法识别的字符 ':' (位置 $start)")
        }
        if (c in "+-*/%") {
            // JSON 取值运算符：`->`（给 JSON）与 `->>`（给字符串）。
            // 必须先认三字符的 `->>`，否则 `j ->> '$.a'` 会被切成 `->` + `>`
            val three = if (pos + 2 < src.length) src.substring(pos, pos + 3) else ""
            val two = if (pos + 1 < src.length) src.substring(pos, pos + 2) else ""
            if (three == "->>") { pos += 3; return Token(TokenType.OP, "->>", start) }
            if (two == "->") { pos += 2; return Token(TokenType.OP, "->", start) }
            pos++
            return Token(TokenType.OP, c.toString(), start)
        }
        if (c in "<>!|&") {
            // 尽量吃掉多字符运算符
            val two = if (pos + 1 < src.length) src.substring(pos, pos + 2) else ""
            val three = if (pos + 2 < src.length) src.substring(pos, pos + 3) else ""
            when {
                three == "<=>" -> { pos += 3; return Token(TokenType.OP, "<=>", start) }
                two in setOf("<=", ">=", "<>", "!=", "||", "&&", "<<", ">>") -> {
                    pos += 2; return Token(TokenType.OP, two, start)
                }
                c == '<' || c == '>' -> { pos++; return Token(TokenType.OP, c.toString(), start) }
            }
        }
        throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "无法识别的字符 '$c' (位置 $start)")
    }
}
