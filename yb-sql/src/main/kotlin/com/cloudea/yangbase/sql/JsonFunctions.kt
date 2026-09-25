package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.Datum
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * `JSON_EXTRACT` / `->` / `->>` 等 JSON 函数。
 *
 * ## 只支持一条 MySQL 路径语法，且**只读**
 *
 * 路径语法是 MySQL 的子集：`$` 开头，后面跟 `.键`、`[下标]`、`[*]`。
 * 不支持通配与递归下降（`**`）、也不支持 `JSON_SET` 之类的**写**函数——
 * 本系统的 JSON 列按文本存（见 [com.cloudea.yangbase.common.ColumnType.JSON]），
 * 改 JSON 就是改整列，那更适合直接用 `UPDATE ... SET j = '...'`。
 *
 * ## `->` 与 `->>` 的区别是**类型**，不是格式
 *
 * | 运算符 | 返回 | 用途 |
 * |---|---|---|
 * | `j -> '$.a'` | JSON（带引号的字符串、带括号的数组/对象） | 继续套 JSON 函数 |
 * | `j ->> '$.a'` | **字符串**（去掉 JSON 的引号） | 直接当值用 |
 *
 * 实现上两者共用一条取值路径，差别只在最后一步是否 `JSON_UNQUOTE`。
 * 混为一谈的表现是：`WHERE j ->> '$.name' = 'alice'` 查不到
 * （因为拿到的其实是 `"alice"`，带引号），而 `j -> '$.name' = '"alice"'` 才成立——
 * 一个不报错、只是永远匹配不上的查询。
 *
 * ## 找不到路径时返回 NULL，不是报错
 *
 * MySQL 的语义：路径不存在 → `NULL`（`JSON_EXTRACT` 不报错）。
 * 这与"路径语法写错了"不同：语法错要报错（1103 / `ER_INVALID_JSON_PATH`），
 * 否则一个拼错的路径会静默变成"所有行都返回 NULL"。
 */
object JsonFunctions {

    private val json = Json { ignoreUnknownKeys = true }

    /** 这些函数名由 [Eval.evalFunction] 分发过来。 */
    val NAMES = setOf(
        "JSON_EXTRACT", "JSON_UNQUOTE", "JSON_VALID", "JSON_TYPE", "JSON_LENGTH", "JSON_KEYS",
    )

    /** 一个 JSON 表达式的取值入口。返回 null 表示"值是 SQL NULL 或路径不存在"。 */
    fun extract(source: Datum, path: Datum): Datum {
        if (source is Datum.Null || path is Datum.Null) return Datum.Null
        val root = parse(source.asString()) ?: return Datum.Null
        val node = navigate(root, path.asString()) ?: return Datum.Null
        return Datum.Str(renderJson(node))
    }

    /** `->>`：取值之后去掉 JSON 的引号（字符串就是字符串本身）。 */
    fun extractText(source: Datum, path: Datum): Datum {
        val v = extract(source, path)
        if (v is Datum.Null) return v
        return Datum.Str(unquote((v as Datum.Str).v))
    }

    /** `JSON_EXTRACT(j, p1, p2, …)`：多路径时返回一个 JSON 数组（MySQL 的语义）。 */
    fun extractMany(source: Datum, paths: List<Datum>): Datum {
        if (source is Datum.Null) return Datum.Null
        val root = parse(source.asString()) ?: return Datum.Null
        val parts = paths.map { p ->
            if (p is Datum.Null) return@map "null"
            val node = navigate(root, p.asString()) ?: return@map "null"
            renderJson(node)
        }
        return Datum.Str(parts.joinToString(",", "[", "]"))
    }

    fun unquote(v: Datum): Datum {
        if (v is Datum.Null) return v
        return Datum.Str(unquote(v.asString()))
    }

    /** `JSON_VALID(x)`：是不是合法 JSON。非法时返回 0 而不是报错（MySQL 的语义）。 */
    fun isValid(v: Datum): Datum {
        if (v is Datum.Null) return Datum.Null
        return Datum.Bool(parse(v.asString()) != null)
    }

    /** `JSON_TYPE(x)`：`OBJECT` / `ARRAY` / `STRING` / `NUMBER` / `BOOLEAN` / `NULL`。 */
    fun typeOf(v: Datum): Datum {
        if (v is Datum.Null) return Datum.Null
        val root = parse(v.asString()) ?: return Datum.Null
        return Datum.Str(typeName(root))
    }

    /** `JSON_LENGTH(x)`：顶层元素个数（对象是键数、数组是长度、标量 1）。 */
    fun lengthOf(v: Datum): Datum {
        if (v is Datum.Null) return Datum.Null
        val root = parse(v.asString()) ?: return Datum.Null
        return Datum.I64(
            when (root) {
                is JsonArray -> root.size.toLong()
                is JsonObject -> root.size.toLong()
                else -> 1L
            }
        )
    }

    /**
     * `JSON_KEYS(x)`：对象的键，按 JSON 文本数组返回。
     *
     * 顺序**不是**文档里的书写顺序，而是 MySQL 的键序：先按**键长**、同长再按键本身。
     * MySQL 存 JSON 时就把对象规范化成这个顺序（`SELECT JSON_OBJECT('b',1,'a',2)`
     * 给的是 `{"a": 2, "b": 1}`），所以 `JSON_KEYS('{"name":1,"age":2}')` 给的是
     * `["age","name"]`。照书写顺序返回的话，同一个表达式在两边会得到不同的数组——
     * 而使用者常把它的结果直接当数组用（比较、再喂给 `JSON_EXTRACT`），
     * 顺序不同就是**静默的结果不同**，不是格式差异。
     */
    fun keysOf(v: Datum): Datum {
        if (v is Datum.Null) return Datum.Null
        val root = parse(v.asString()) ?: return Datum.Null
        if (root !is JsonObject) return Datum.Null
        val keys = root.keys.sortedWith(compareBy({ it.length }, { it }))
        return Datum.Str(keys.joinToString(",", "[", "]") { "\"$it\"" })
    }

    // ── 内部 ──────────────────────────────────────────────────────

    private fun parse(text: String): JsonElement? = runCatching { json.parseToJsonElement(text) }.getOrNull()

    private fun typeName(e: JsonElement): String = when (e) {
        is JsonObject -> "OBJECT"
        is JsonArray -> "ARRAY"
        is JsonNull -> "NULL"
        is JsonPrimitive -> if (e.isString) "STRING" else if (e.content == "true" || e.content == "false") "BOOLEAN" else "NUMBER"
    }

    /**
     * 按路径取值。
     *
     * 路径语法：`$` 之后跟任意个 `.键` / `[下标]`。**语法错就抛异常**
     * （而不是返回 null）：拼错的路径如果静默返回 NULL，使用者会以为"这行没这个字段"。
     */
    fun navigate(root: JsonElement, path: String): JsonElement? {
        var i = 0
        if (i >= path.length || path[i] != '$') {
            throw JsonPathError("JSON 路径必须以 \$ 开头：'$path'")
        }
        i++
        var cur: JsonElement? = root
        while (i < path.length) {
            when (path[i]) {
                '.' -> {
                    i++
                    val start = i
                    while (i < path.length && path[i] != '.' && path[i] != '[') i++
                    val key = path.substring(start, i)
                    if (key.isEmpty()) throw JsonPathError("JSON 路径里出现了空的键名：'$path'")
                    val obj = cur as? JsonObject ?: return null
                    cur = obj[key] ?: return null
                }
                '[' -> {
                    i++
                    val start = i
                    while (i < path.length && path[i] != ']') i++
                    if (i >= path.length) throw JsonPathError("JSON 路径缺少 ']'：'$path'")
                    val idxText = path.substring(start, i)
                    i++      // 跳过 ']'
                    val arr = cur as? JsonArray ?: return null
                    val idx = idxText.trim().toIntOrNull()
                        ?: throw JsonPathError("JSON 路径的下标必须是整数：'$path'")
                    // MySQL 的下标越界返回 NULL，负数从尾部算
                    val real = if (idx < 0) arr.size + idx else idx
                    cur = arr.getOrNull(real) ?: return null
                }
                else -> throw JsonPathError("JSON 路径里出现了无法识别的内容 '${path[i]}'：'$path'")
            }
        }
        return cur
    }

    /** 渲染成 MySQL 的 JSON 文本形式（字符串带引号、紧凑无空格）。 */
    fun renderJson(e: JsonElement): String = when (e) {
        is JsonNull -> "null"
        is JsonPrimitive -> if (e.isString) "\"${escape(e.content)}\"" else e.content
        is JsonArray -> e.joinToString(",", "[", "]") { renderJson(it) }
        is JsonObject -> e.entries.joinToString(",", "{", "}") { (k, v) -> "\"${escape(k)}\":${renderJson(v)}" }
    }

    /** 去掉 JSON 字符串的引号并解转义（`JSON_UNQUOTE`）。 */
    fun unquote(text: String): String {
        if (text.length < 2 || !text.startsWith("\"") || !text.endsWith("\"")) return text
        val body = text.substring(1, text.length - 1)
        val sb = StringBuilder(body.length)
        var i = 0
        while (i < body.length) {
            val c = body[i]
            if (c != '\\' || i + 1 >= body.length) {
                sb.append(c); i++; continue
            }
            when (val n = body[i + 1]) {
                'n' -> sb.append('\n')
                't' -> sb.append('\t')
                'r' -> sb.append('\r')
                'b' -> sb.append('\b')
                'f' -> sb.append('\u000C')
                'u' -> {
                    val hex = body.substring(i + 2, minOf(i + 6, body.length))
                    val code = hex.toIntOrNull(16)
                    if (code == null) sb.append(n) else sb.append(code.toChar())
                    i += 4
                }
                else -> sb.append(n)
            }
            i += 2
        }
        return sb.toString()
    }

    private fun escape(s: String): String = buildString(s.length + 8) {
        for (c in s) when (c) {
            '"' -> append("\\\"")
            '\\' -> append("\\\\")
            '\n' -> append("\\n")
            '\r' -> append("\\r")
            '\t' -> append("\\t")
            else -> append(c)
        }
    }
}

/** 路径语法错误。**必须报错**，不能返回 NULL——见 [JsonFunctions.navigate]。 */
class JsonPathError(message: String) : Exception(message)
