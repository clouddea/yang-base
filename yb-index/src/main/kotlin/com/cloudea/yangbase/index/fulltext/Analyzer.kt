package com.cloudea.yangbase.index.fulltext

/** 一个词条。`position` 是**词位序号**（不是字符偏移）——短语查询靠它判定相邻。 */
data class Token(
    val term: String,
    val position: Int,
    val startOffset: Int = 0,
    val endOffset: Int = 0,
)

/**
 * 分析链（设计文档 §8.6.4）。
 *
 * ⚠️ **索引时与查询时必须用同一个分析器**。用错分析器的后果是静默的：
 * 索引时切出「数据库」，查询时切出「数据」「据库」，两者永不相交，查询返回空集
 * 却不报任何错。所以 [`Analyzer.name`] 会被写进段元信息，检索时强制校验
 * （见 `FullTextIndex.search`）。
 */
interface Analyzer {
    /** 稳定标识，写进段元信息用于自校验。**语义变化必须改名字。** */
    val name: String

    fun analyze(text: String): List<Token>
}

/** 英文/ASCII 分词：小写化 + 去停用词。CJK 字符按分隔符处理（请改用 [ChineseNgramAnalyzer]）。 */
class StandardAnalyzer(
    private val stopWords: Set<String> = DEFAULT_STOP_WORDS,
    /** 小于该长度的词条丢弃。1 表示不丢。 */
    private val minLength: Int = 1,
    /** 是否保留停用词（保留即不做去除，只小写化）。 */
    private val keepStopWords: Boolean = false,
) : Analyzer {

    override val name: String =
        "standard(min=$minLength,stop=${if (keepStopWords) "keep" else stopWords.size})"

    override fun analyze(text: String): List<Token> {
        val out = ArrayList<Token>(16)
        var i = 0
        var position = 0
        while (i < text.length) {
            // 词条 = [A-Za-z0-9_] 的连续段；其余字符（含 CJK）都是分隔符
            if (!isAsciiWordChar(text[i])) { i++; continue }
            val start = i
            while (i < text.length && isAsciiWordChar(text[i])) i++
            val raw = text.substring(start, i)
            val term = raw.lowercase()
            // 停用词与过短词**占用词位**（留下空隙）：
            // 这样 "quick the fox" 不会被短语查询 "quick fox" 命中
            val slot = position++
            if (term.length < minLength) continue
            if (!keepStopWords && term in stopWords) continue
            out.add(Token(term, slot, start, i))
        }
        return out
    }

    private fun isAsciiWordChar(c: Char): Boolean =
        c in 'a'..'z' || c in 'A'..'Z' || c in '0'..'9' || c == '_'

    companion object {
        /** 一份精简的英文停用词表；不需要与任何第三方实现逐字对齐。 */
        val DEFAULT_STOP_WORDS: Set<String> = setOf(
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from",
            "has", "have", "he", "in", "is", "it", "its", "of", "on", "or", "that",
            "the", "this", "to", "was", "were", "will", "with",
        )
    }
}

/**
 * 中英混合的**兜底**分析器：CJK 段切 n-gram（默认二元），ASCII 段按词切。
 *
 * 这是设计文档 §8.6.4 明确写出的「兜底模式」：
 * > 建议**双模式**：检索模式用 Jieba 分词；兜底模式 `ngram(2,3)`，
 * > 保证任何查询子串都能被匹配到（代价是索引膨胀 3–5 倍）。
 *
 * 本模块**没有**引入 jieba / HanLP 依赖（避免为分词引入外部依赖与词典体积）。
 * 需要真正的分词时，实现 [Analyzer] 接口即可替换——`FullTextIndex` 只依赖接口。
 * 二元切分对子串查询是完备的：任何子串都能拆成连续的二元组，因此
 * 「二元组全部命中 + 相邻」⇒ 子串命中（这正是把 [TextQuery.Phrase] 用在
 * 中文上的语义）。
 */
class ChineseNgramAnalyzer(
    private val ngramN: Int = 2,
    private val stopWords: Set<String> = StandardAnalyzer.DEFAULT_STOP_WORDS,
) : Analyzer {

    override val name: String = "cjk-ngram(n=$ngramN)"

    override fun analyze(text: String): List<Token> {
        val out = ArrayList<Token>(text.length)
        var i = 0
        var position = 0
        while (i < text.length) {
            val c = text[i]
            when {
                isCjk(c) -> {
                    // CJK 段整段收集后切 n-gram，词位 = 段内字符下标，
                    // 于是相邻的二元组天然相差 1 个词位，短语查询即「连续子串」
                    val start = i
                    while (i < text.length && isCjk(text[i])) i++
                    val run = text.substring(start, i)
                    if (run.length < ngramN) {
                        out.add(Token(run, position, start, i))
                        position++
                    } else {
                        for (k in 0..run.length - ngramN) {
                            out.add(Token(run.substring(k, k + ngramN), position + k, start + k, start + k + ngramN))
                        }
                        position += run.length
                    }
                }
                isAsciiWordChar(c) -> {
                    val start = i
                    while (i < text.length && isAsciiWordChar(text[i])) i++
                    val term = text.substring(start, i).lowercase()
                    val slot = position++
                    if (term in stopWords && term.length <= 3) continue
                    out.add(Token(term, slot, start, i))
                }
                else -> i++
            }
        }
        return out
    }

    private fun isCjk(c: Char): Boolean =
        c.code in 0x4E00..0x9FFF || c.code in 0x3400..0x4DBF ||
            c.code in 0xF900..0xFAFF || c.code in 0x3040..0x30FF

    private fun isAsciiWordChar(c: Char): Boolean =
        c in 'a'..'z' || c in 'A'..'Z' || c in '0'..'9' || c == '_'
}

/** 任意可打印字符的纯 n-gram 分析器（不分词）。用于 `LIKE '%x%'` 式的子串检索。 */
class NgramAnalyzer(private val n: Int = 3) : Analyzer {
    init { require(n >= 1) { "n 必须 >= 1" } }
    override val name: String = "ngram(n=$n)"

    override fun analyze(text: String): List<Token> {
        if (text.length < n) return listOf(Token(text.lowercase(), 0))
        val out = ArrayList<Token>(text.length - n + 1)
        for (i in 0..text.length - n) {
            out.add(Token(text.substring(i, i + n).lowercase(), i, i, i + n))
        }
        return out
    }
}
