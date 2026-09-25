package com.cloudea.yangbase.index.skip

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.TableSchema

/**
 * 区间的单侧边界。`inclusive=false` 表示严格不等（`>` / `<`）。
 *
 * NULL 不允许作为边界：SQL 里 `col > NULL` 恒为未知，永不满足。
 * 若允许传进来，剪枝逻辑会从「恒不满足」推出「可以剪掉所有块」——
 * 逻辑上没错，但一旦 SQL 层是用它表达别的东西（例如 `IS NOT NULL` 的误译），
 * 就会变成静默漏行。所以直接拒绝，让错误在构造点暴露。
 */
data class Bound(val value: Datum, val inclusive: Boolean) {
    init {
        require(value !is Datum.Null) { "区间边界不能是 NULL；IS NULL 请用 IsNull 谓词" }
    }
}

/**
 * 跳数索引的输入谓词（设计文档 §8.5）。
 *
 * ⚠️ **这不是行过滤器**。跳数索引只能回答一个问题：
 * 「这个数据块**是否一定不含**满足谓词的行」。回答「一定不含」就整块跳过；
 * 回答「可能含」**必须**仍然读取该块并逐行复核。
 *
 * 这条语义是硬约束：把「可能含」当成「含」会漏行，把「可能不含」当成「不含」会多读。
 * 前者不报错、不降级、静默丢数据——本文件里每个判定都按这个方向保守取值。
 */
sealed interface SkipPredicate {

    /** 该谓词引用的列下标（组合谓词返回全部子谓词引用列）。 */
    val columns: Set<Int>

    /** `col = v`。等值可用 set / bloom 索引剪枝。 */
    data class Equals(val column: Int, val value: Datum) : SkipPredicate {
        init { require(value !is Datum.Null) { "Equals 的值不能是 NULL；IS NULL 请用 IsNull 谓词" } }
        override val columns: Set<Int> get() = setOf(column)
    }

    /** `col IN (...)`。 */
    data class InList(val column: Int, val values: List<Datum>) : SkipPredicate {
        init { require(values.none { it is Datum.Null }) { "InList 的元素不能含 NULL" } }
        override val columns: Set<Int> get() = setOf(column)
    }

    /**
     * `col <> v` / `col != v`。
     *
     * 剪枝能力很弱且**方向容易搞反**：只有「块内非 NULL 值全都等于 v」时才能跳过。
     * （块内有 NULL 不影响——`NULL <> v` 是未知，本来就不满足。）
     */
    data class NotEquals(val column: Int, val value: Datum) : SkipPredicate {
        init { require(value !is Datum.Null) { "NotEquals 的值不能是 NULL" } }
        override val columns: Set<Int> get() = setOf(column)
    }

    /** `<` `<=` `>` `>=` `BETWEEN`。单侧边界时另一侧传 null。 */
    data class Range(
        val column: Int,
        val lower: Bound? = null,
        val upper: Bound? = null,
    ) : SkipPredicate {
        init { require(lower != null || upper != null) { "Range 至少要有一个边界" } }
        override val columns: Set<Int> get() = setOf(column)

        /** 按 SQL 语义判断（NULL 视为不满足）。用于块内逐行复核。 */
        fun contains(v: Datum): Boolean {
            if (v is Datum.Null) return false
            val lo = lower
            if (lo != null) {
                val c = v.compareTo(lo.value)
                if (if (lo.inclusive) c < 0 else c <= 0) return false
            }
            val hi = upper
            if (hi != null) {
                val c = v.compareTo(hi.value)
                if (if (hi.inclusive) c > 0 else c >= 0) return false
            }
            return true
        }
    }

    /** `IS NULL` / `IS NOT NULL`。 */
    data class IsNull(val column: Int, val negated: Boolean = false) : SkipPredicate {
        override val columns: Set<Int> get() = setOf(column)
    }

    /**
     * `col LIKE pattern`。
     *
     * 只支持 `%` 与 `_`（无 ESCAPE 子句）。SQL 层的 LIKE 语义以它为准，
     * 若将来要支持 `ESCAPE`，这里必须同步——否则剪枝会基于错误的字面量切分。
     */
    data class Like(val column: Int, val pattern: String) : SkipPredicate {
        override val columns: Set<Int> get() = setOf(column)

        /** 模式中的字面量片段（按 `%` / `_` 切分后长度 ≥1 的连续字面量）。 */
        val literals: List<String> = LikeMatcher.literalSegments(pattern)

        /** 模式的字面量前缀；模式以通配符开头时为 null。 */
        val leadingLiteral: String? = LikeMatcher.leadingLiteral(pattern)
    }

    /** 合取。**任一**子谓词可剪枝 ⇒ 整块可剪枝。 */
    data class And(val children: List<SkipPredicate>) : SkipPredicate {
        override val columns: Set<Int> get() = children.flatMapTo(LinkedHashSet()) { it.columns }
    }

    /** 析取。**全部**子谓词可剪枝 ⇒ 整块可剪枝。 */
    data class Or(val children: List<SkipPredicate>) : SkipPredicate {
        override val columns: Set<Int> get() = children.flatMapTo(LinkedHashSet()) { it.columns }
    }

    /**
     * 对该行求值。三值逻辑里的「未知」按**不满足**处理——与 SQL 的 `WHERE` 一致。
     * 仅供扫描器复核与属性测试使用；SQL 层应保留自己的过滤器。
     */
    fun evaluate(row: List<Datum>): Boolean = when (this) {
        is Equals -> row[column].let { it !is Datum.Null && it.compareTo(value) == 0 }
        is InList -> values.any { row[column].compareTo(it) == 0 }
        is NotEquals -> row[column].let { it !is Datum.Null && it.compareTo(value) != 0 }
        is Range -> contains(row[column])
        is IsNull -> if (negated) row[column] !is Datum.Null else row[column] is Datum.Null
        is Like -> row[column].let { it !is Datum.Null && LikeMatcher.matches(it.asString(), pattern) }
        is And -> children.all { it.evaluate(row) }
        is Or -> children.any { it.evaluate(row) }
    }

    /** 便于 EXPLAIN 与测试消息。 */
    fun describe(): String = when (this) {
        is Equals -> "[${column}] = $value"
        is InList -> "[${column}] IN (${values.joinToString(",")})"
        is NotEquals -> "[${column}] != $value"
        is Range -> buildString {
            when {
                lower != null && upper != null ->
                    append("[${column}] BETWEEN ${lower.value} AND ${upper.value}" +
                        if (!lower.inclusive || !upper.inclusive) "(开区间)" else "")
                lower != null -> append("[${column}] ${if (lower.inclusive) ">=" else ">"} ${lower.value}")
                else -> append("[${column}] ${if (upper!!.inclusive) "<=" else "<"} ${upper.value}")
            }
        }
        is IsNull -> "[${column}] IS ${if (negated) "NOT " else ""}NULL"
        is Like -> "[${column}] LIKE '${pattern}'"
        is And -> children.joinToString(" AND ", "(", ")") { it.describe() }
        is Or -> children.joinToString(" OR ", "(", ")") { it.describe() }
    }
}

/** 谓词列表（合取）对一行的求值。 */
fun List<SkipPredicate>.allMatch(row: List<Datum>): Boolean = all { it.evaluate(row) }

/**
 * LIKE 的匹配与字面量切分。**索引侧与查询侧必须用同一套实现**——
 * 两侧对同一个模式切出不同的字面量，会直接导致漏行。
 */
object LikeMatcher {

    /**
     * 支持 `%`（任意长度）与 `_`（恰好一个字符）的匹配。
     * 用经典的「星号回溯」迭代实现，避免递归在长文本 + 多 `%` 时爆栈。
     */
    fun matches(text: String, pattern: String): Boolean {
        var t = 0
        var p = 0
        var star = -1
        var mark = 0
        while (t < text.length) {
            if (p < pattern.length && pattern[p] != '%' &&
                (pattern[p] == '_' || pattern[p] == text[t])
            ) {
                p++; t++
            } else if (p < pattern.length && pattern[p] == '%') {
                star = p; p++; mark = t
            } else if (star >= 0) {
                p = star + 1; mark++; t = mark
            } else {
                return false
            }
        }
        while (p < pattern.length && pattern[p] == '%') p++
        return p == pattern.length
    }

    /** 按 `%` / `_` 切出的连续字面量片段（可能含空串：`%%`、`_%` 等）。 */
    fun literalSegments(pattern: String): List<String> {
        val out = ArrayList<String>()
        val cur = StringBuilder()
        for (ch in pattern) {
            if (ch == '%' || ch == '_') {
                out.add(cur.toString()); cur.setLength(0)
            } else cur.append(ch)
        }
        out.add(cur.toString())
        return out.filter { it.isNotEmpty() }
    }

    /**
     * 模式的字面量前缀。`LIKE 'abc%'` → `"abc"`；`LIKE '%abc'` → null；
     * `LIKE 'a_c%'` → `"a"`（`_` 处停下：**匹配该模式的每个值确实都以 `"a"` 开头**，
     * 因此这个前缀是正确但较弱的界——`_` 之后就没有确定的前缀了）。
     */
    fun leadingLiteral(pattern: String): String? {
        val sb = StringBuilder()
        for (ch in pattern) {
            if (ch == '%' || ch == '_') break
            sb.append(ch)
        }
        return sb.toString().ifEmpty { null }
    }
}
