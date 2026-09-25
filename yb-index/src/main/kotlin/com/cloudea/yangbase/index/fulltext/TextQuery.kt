package com.cloudea.yangbase.index.fulltext

/**
 * 全文检索的查询表达式（设计文档 §8.6.7）。
 *
 * 刻意**不提供任意嵌套的 And/Or 组合**：WAND 只需处理「单层 OR」，而 AND/短语
 * 走交集扫描。层级一多，"哪条路径负责打分、哪条负责过滤"就说不清楚，
 * 语义会在边界上静默漂移。需要的表达能力由 [Boolean] 的 must/should/mustNot 覆盖。
 *
 * 每个叶子都携带 `field`：一个查询可以同时命中多个字段的索引，
 * 但**同一个索引只索引一个字段**，[FullTextIndex] 会校验字段一致。
 */
sealed interface TextQuery {

    val field: String

    /** 单个词条。文本必须**恰好**切成一个词，否则请用 [AnyOf] / [AllOf] / [Phrase]。 */
    data class Term(override val field: String, val text: String) : TextQuery

    /** 前缀查询 `prefix*`：由 FST 枚举展开成若干词条的 OR。 */
    data class Prefix(override val field: String, val text: String) : TextQuery

    /** 短语：切出的词必须**按词位相邻**出现（slop=0）。 */
    data class Phrase(override val field: String, val text: String) : TextQuery

    /** 任意一个词命中即可（自然语言模式的默认语义），分数为命中词之和。 */
    data class AnyOf(override val field: String, val text: String) : TextQuery

    /** 所有词都必须命中。 */
    data class AllOf(override val field: String, val text: String) : TextQuery

    /**
     * 布尔查询。[must] 全部命中、[should] 至少一个命中（空表示不要求）、
     * [mustNot] 一个都不能命中。
     */
    data class Boolean(
        override val field: String,
        val must: List<TextQuery> = emptyList(),
        val should: List<TextQuery> = emptyList(),
        val mustNot: List<TextQuery> = emptyList(),
    ) : TextQuery {
        init {
            require(must.isNotEmpty() || should.isNotEmpty()) { "Boolean 至少要有 must 或 should" }
            require(should.all { it !is Boolean }) { "should 里不能再嵌 Boolean（语义会含糊）" }
            require(mustNot.all { it !is Boolean }) { "mustNot 里不能再嵌 Boolean" }
            require(must.all { it !is Boolean }) { "must 里不能再嵌 Boolean" }
        }
    }

    /** 命中全部文档（按 docId 顺序），分数为 0。用于纯过滤场景。 */
    data class MatchAll(override val field: String) : TextQuery
}

/** `MATCH ... AGAINST` 的两种模式（§8.6.7）。 */
enum class MatchMode {
    /** 自然语言模式：全部词做 OR 打分。 */
    NATURAL_LANGUAGE,

    /** 布尔模式：`+词` 必须、`-词` 排除、`"短语"` 短语、其余为 should。 */
    BOOLEAN,
}

/**
 * `MATCH(col) AGAINST('...' IN BOOLEAN MODE)` 的解析。
 *
 * ⚠️ 只做**语法**解析，不做分词——分词由 [Analyzer] 在规划阶段完成，
 * 保证索引期与查询期用的是同一套切分（用错分析器会静默返回空集）。
 */
object MatchAgainstParser {

    fun parse(field: String, text: String, mode: MatchMode): TextQuery {
        require(field.isNotEmpty()) { "字段名不能为空" }
        return when (mode) {
            MatchMode.NATURAL_LANGUAGE -> {
                require(text.isNotBlank()) { "自然语言模式的查询文本不能为空" }
                TextQuery.AnyOf(field, text)
            }
            MatchMode.BOOLEAN -> parseBoolean(field, text)
        }
    }

    private fun parseBoolean(field: String, text: String): TextQuery {
        val must = ArrayList<TextQuery>()
        val should = ArrayList<TextQuery>()
        val mustNot = ArrayList<TextQuery>()
        for (raw in splitTokens(text)) {
            when {
                raw.length >= 2 && raw.startsWith('"') && raw.endsWith('"') ->
                    must.add(TextQuery.Phrase(field, raw.substring(1, raw.length - 1)))
                raw.startsWith('+') && raw.length > 1 -> must.add(body(field, raw.substring(1)))
                raw.startsWith('-') && raw.length > 1 -> mustNot.add(body(field, raw.substring(1)))
                else -> should.add(body(field, raw))
            }
        }
        require(must.isNotEmpty() || should.isNotEmpty()) { "布尔模式的查询没有任何有效子句: '$text'" }
        return TextQuery.Boolean(field, must, should, mustNot)
    }

    /** `词*` 是前缀查询，`词` 默认按「任意一个词命中」处理（与 Lucene 的 BooleanQuery 一致）。 */
    private fun body(field: String, raw: String): TextQuery {
        val star = raw.indexOf('*')
        return if (star == raw.length - 1 && star > 0) {
            TextQuery.Prefix(field, raw.substring(0, star))
        } else {
            TextQuery.AnyOf(field, raw.replace("*", ""))
        }
    }

    /** 按空白切分，但保留 `"..."` 为一整段。 */
    private fun splitTokens(text: String): List<String> {
        val out = ArrayList<String>()
        val sb = StringBuilder()
        var inQuote = false
        for (c in text) {
            when {
                c == '"' -> { inQuote = !inQuote; sb.append(c) }
                c.isWhitespace() && !inQuote -> { if (sb.isNotEmpty()) { out.add(sb.toString()); sb.setLength(0) } }
                else -> sb.append(c)
            }
        }
        if (sb.isNotEmpty()) out.add(sb.toString())
        return out
    }
}
