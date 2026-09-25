package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import java.nio.charset.Charset

/**
 * 会话字符集（设计文档 §11.1「字符集」一行，以及 §11.1.1 的陷阱 T1 / T2）。
 *
 * ## 为什么这一小块值得单独一个文件
 *
 * 字符集搞错的症状**不是报错，是乱码**——而乱码在应用层看起来就像数据被写坏了。
 * §11.1.1 的 T1、T2 两条陷阱都是别人踩过的真实事故：
 *
 * - **T1**：Connector/J 会发 `SET character_set_results = NULL`，
 *   语义是「按列的字符集返回原始字节，不做转换」。如果把它当成一个普通变量
 *   悄悄忽略掉，而服务端又按别的编码去写字符串，中文就是乱码。
 * - **T2**：`PREPARE` 与 `EXECUTE` 之间会话状态**可能漂移**
 *   （`SET NAMES gbk` 之后 prepare、再切回来 execute），
 *   按 `PREPARE` 时刻的编码去解码 `EXECUTE` 的参数就会读出乱码。
 *   TiDB 有过这个真实 bug。因此**每次 `COM_STMT_EXECUTE` 都重新探测**（见 MysqlServer）。
 *
 * ## 支持的集合：刻意就是这四个
 *
 * `utf8mb4`（默认）、`utf8`、`latin1`、`binary`。
 * 不在表里的（`gbk`、`big5`…）**显式报 1115**，而不是"收下但按 utf8mb4 处理"——
 * 后者正是 T1 要防的那种静默错误：客户端以为自己在用 gbk，服务端却在用 utf8mb4，
 * 中文写进去看着正常，读出来是乱码，而这条链路上没有任何一处报错。
 * §11.1.1 的应对栏对此的措辞是「要么完整支持按列字符集，要么在其余位置显式拒绝」。
 *
 * ## 为什么 `character_set_results = NULL` 可以当作 utf8mb4
 *
 * 它的语义是「不做转换，按列自身的字符集给原始字节」。而本系统**所有字符串列
 * 都是 utf8mb4**（见 [com.cloudea.yangbase.common.ColumnDef]），所以"不做转换"
 * 与"转成 utf8mb4"是同一件事。这不是取巧，是这两件事在本系统里恰好等价。
 */
enum class SessionCharset(
    /** 规范名。`SET NAMES` 与 `@@character_set_*` 都回它。 */
    val canonical: String,
    /** MySQL 的字符集编号，要写进握手包与列定义里。 */
    val collationId: Int,
    /** MySQL 的排序规则名，`SHOW VARIABLES` 里要用。 */
    val collationName: String,
    private val charset: Charset,
) {
    /** 默认。四字节 UTF-8，`utf8mb4_0900_ai_ci`（MySQL 8 的默认排序规则）。 */
    UTF8MB4("utf8mb4", 255, "utf8mb4_0900_ai_ci", Charsets.UTF_8),

    /** MySQL 里的 `utf8` 是「最多 3 字节的 UTF-8」，即 utf8mb3。 */
    UTF8("utf8", 33, "utf8_general_ci", Charsets.UTF_8),

    LATIN1("latin1", 8, "latin1_swedish_ci", Charsets.ISO_8859_1),

    /** 不做任何转换。用于二进制列与 `SET NAMES binary`。 */
    BINARY("binary", 63, "binary", Charsets.ISO_8859_1),
    ;

    /** 把客户端发来的字节解成字符串（`COM_QUERY` 的 SQL 文本、文本协议的值）。 */
    fun decode(bytes: ByteArray): String = String(bytes, charset)

    /**
     * 把字符串编成发给客户端的字节。
     *
     * `latin1` 放不下中文等非 Latin-1 字符。MySQL 在这种情况下会**报错**
     * （而不是悄悄替换成 `?`），这里照做：静默替换会让客户端拿到一个
     * 「看起来正常、但内容已经不是原来的」字符串。
     */
    fun encode(s: String): ByteArray {
        if (this == BINARY || this == LATIN1) {
            val out = ByteArray(s.length)
            for (i in s.indices) {
                val c = s[i].code
                if (c > 0xFF) {
                    throw SqlException(
                        MySqlErrors.ER_CANT_AGGREGATE_2COLLATIONS, "HY000",
                        "字符串含有 $canonical 无法表示的字符（'${s[i]}'，U+%04X）".format(c) +
                            "。请用 SET NAMES utf8mb4，或把该列改成 utf8mb4。",
                    )
                }
                out[i] = c.toByte()
            }
            return out
        }
        return s.toByteArray(charset)
    }

    /** 只判断能否表示，不产生异常——用于决定是否需要走「显式拒绝」这条路。 */
    fun canEncode(s: String): Boolean =
        if (this == BINARY || this == LATIN1) s.all { it.code <= 0xFF } else true

    override fun toString(): String = canonical

    companion object {
        /** 见类注释：`NULL` 的语义在本系统里与 utf8mb4 等价。 */
        val DEFAULT = UTF8MB4

        /** 排序规则名 → 字符集。`utf8mb4_0900_ai_ci` / `utf8mb4_general_ci` 都归 utf8mb4。 */
        private val BY_NAME: Map<String, SessionCharset> = buildMap {
            // 显式写 SessionCharset.entries：buildMap 的作用域里 `entries`
            // 指的是这个 MutableMap 自己的 entries（踩过）
            for (c in SessionCharset.entries) {
                put(c.canonical, c)
                put(c.collationName, c)
                put("${c.canonical}_bin", c)          // utf8mb4_bin 等，字节序不同但编码相同
            }
            // 常见别名
            put("utf8mb3", UTF8)
            put("utf8mb4_general_ci", UTF8MB4)
            put("utf8mb4_unicode_ci", UTF8MB4)
            put("utf8_general_ci", UTF8)
            put("utf8_bin", UTF8)
        }

        val supportedNames: List<String> = entries.map { it.canonical }

        /**
         * 按名字解析。**解析不出来必须抛异常**，不要回退到默认值——
         * 回退就是 T1 那种静默错误：客户端以为在用 gbk，服务端在用 utf8mb4。
         */
        fun parse(name: String): SessionCharset {
            val k = name.trim().trim('\'').trim('"').lowercase()
            // MySQL 里 `SET character_set_results = NULL` 表示不转换，见类注释
            if (k == "null") return DEFAULT
            // `SET NAMES DEFAULT` 是回到默认字符集
            if (k == "default") return DEFAULT
            BY_NAME[k]?.let { return it }
            throw SqlException(
                MySqlErrors.ER_UNKNOWN_CHARACTER_SET, "42000",
                "Unknown character set: '$name'。本系统支持：${supportedNames.joinToString(", ")}" +
                    "（设计文档 §11.1 的字符集范围；不在其中的字符集会被显式拒绝，" +
                    "而不是收下之后按 utf8mb4 处理——那会以乱码的形式静默出错）",
            )
        }

        /** 握手包里的字符集编号要反查回枚举（Connector/J 会用它做初始 charset）。 */
        fun ofCollationId(id: Int): SessionCharset =
            entries.firstOrNull { it.collationId == id } ?: DEFAULT
    }
}
