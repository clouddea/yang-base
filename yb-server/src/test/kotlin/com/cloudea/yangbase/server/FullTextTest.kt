package com.cloudea.yangbase.server

import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 全文检索端到端测试（设计文档 §8.6）。
 *
 * 在此之前 `MATCH ... AGAINST` 完全不存在，倒排索引也没有接到 SQL 上。
 * 这个文件验证的是**接线**：建索引 → 写入时维护 → 查询时用它。
 *
 * 最重要的一组断言是**索引与数据的一致性**：索引维护漏一次，检索就会返回
 * 「曾经匹配但现在已经被删掉/改掉」的行。那类错误不报错，只给过期结果，
 * 比查不到更难发现。
 */
class FullTextTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-ft")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "ft-test", dataDir = dataDir.toString(), allowNonDurableStorage = true)
        )
        server.start()
        port = server.boundPort
        Class.forName("com.mysql.cj.jdbc.Driver")
    }

    @AfterTest
    fun tearDown() {
        runCatching { server.stop() }
        runCatching { dataDir.toFile().deleteRecursively() }
    }

    private fun connect(): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/ftdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun Connection.st(block: (Statement) -> Unit) = createStatement().use(block)

    private fun Statement.query(sql: String): List<List<Any?>> =
        executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            val out = ArrayList<List<Any?>>()
            while (rs.next()) out.add((1..n).map { rs.getObject(it) })
            out
        }

    /** 英文语料 —— 用 standard 分词器。 */
    private fun setupEnglish(c: Connection) = c.st { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS ftdb")
        s.execute("USE ftdb")
        s.execute("DROP TABLE IF EXISTS articles")
        s.execute(
            """
            CREATE TABLE articles (
                id BIGINT NOT NULL, title VARCHAR(200), body TEXT,
                PRIMARY KEY (id)
            )
            """.trimIndent()
        )
        s.execute("CREATE FULLTEXT INDEX ft_body ON articles(body) WITH PARSER standard")
    }

    /** 中文语料 —— 必须用 ngram 分词器。 */
    private fun setupChinese(c: Connection) = c.st { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS ftdb")
        s.execute("USE ftdb")
        s.execute("DROP TABLE IF EXISTS docs")
        s.execute(
            """
            CREATE TABLE docs (
                id BIGINT NOT NULL, content VARCHAR(300),
                PRIMARY KEY (id)
            )
            """.trimIndent()
        )
        s.execute("CREATE FULLTEXT INDEX ft_content ON docs(content) WITH PARSER ngram")
    }

    // ── 基本检索 ──────────────────────────────────────────────────

    @Test
    fun `英文全文检索命中正确的行`() {
        connect().use { c ->
            setupEnglish(c)
            c.st { s ->
                s.execute(
                    """
                    INSERT INTO articles VALUES
                    (1, 'A', 'the quick brown fox jumps over the lazy dog'),
                    (2, 'B', 'a fast brown rabbit runs across the field'),
                    (3, 'C', 'database systems and storage engines'),
                    (4, 'D', 'object storage is cheap and durable')
                    """.trimIndent()
                )
                val hits = s.query("SELECT id FROM articles WHERE MATCH(body) AGAINST('brown')")
                    .map { it[0] as Long }.sorted()
                assertEquals(listOf(1L, 2L), hits, "brown 应命中 1 和 2")

                val fox = s.query("SELECT id FROM articles WHERE MATCH(body) AGAINST('fox')")
                    .map { it[0] as Long }
                assertEquals(listOf(1L), fox)

                assertEquals(
                    0,
                    s.query("SELECT id FROM articles WHERE MATCH(body) AGAINST('nonexistentword')").size,
                    "查不到词应返回空集，而不是报错",
                )
            }
        }
    }

    @Test
    fun `多个词是 OR 语义 - 自然语言模式`() {
        connect().use { c ->
            setupEnglish(c)
            c.st { s ->
                s.execute(
                    """
                    INSERT INTO articles VALUES
                    (1, 'A', 'alpha beta'),
                    (2, 'B', 'gamma delta'),
                    (3, 'C', 'epsilon zeta')
                    """.trimIndent()
                )
                // MySQL 自然语言模式下 AGAINST('alpha gamma') 是「命中任一词」
                val hits = s.query("SELECT id FROM articles WHERE MATCH(body) AGAINST('alpha gamma')")
                    .map { it[0] as Long }.sorted()
                assertEquals(listOf(1L, 2L), hits)
            }
        }
    }

    @Test
    fun `布尔模式的必须与排除`() {
        connect().use { c ->
            setupEnglish(c)
            c.st { s ->
                s.execute(
                    """
                    INSERT INTO articles VALUES
                    (1, 'A', 'quick brown fox'),
                    (2, 'B', 'quick brown rabbit'),
                    (3, 'C', 'quick red fox')
                    """.trimIndent()
                )
                // +quick 必须有，-rabbit 必须没有
                val hits = s.query(
                    "SELECT id FROM articles WHERE MATCH(body) AGAINST('+quick -rabbit' IN BOOLEAN MODE)"
                ).map { it[0] as Long }.sorted()
                assertEquals(listOf(1L, 3L), hits, "排除项必须真的排除")
            }
        }
    }

    @Test
    fun `布尔模式的前缀查询`() {
        connect().use { c ->
            setupEnglish(c)
            c.st { s ->
                s.execute(
                    """
                    INSERT INTO articles VALUES
                    (1, 'A', 'database systems'),
                    (2, 'B', 'datacenter power'),
                    (3, 'C', 'storage engine')
                    """.trimIndent()
                )
                val hits = s.query(
                    "SELECT id FROM articles WHERE MATCH(body) AGAINST('data*' IN BOOLEAN MODE)"
                ).map { it[0] as Long }.sorted()
                assertEquals(listOf(1L, 2L), hits, "前缀查询应命中 database 与 datacenter")
            }
        }
    }

    @Test
    fun `布尔模式的短语查询`() {
        connect().use { c ->
            setupEnglish(c)
            c.st { s ->
                s.execute(
                    """
                    INSERT INTO articles VALUES
                    (1, 'A', 'the quick brown fox'),
                    (2, 'B', 'brown quick fox')
                    """.trimIndent()
                )
                val hits = s.query(
                    """SELECT id FROM articles WHERE MATCH(body) AGAINST('"quick brown"' IN BOOLEAN MODE)"""
                ).map { it[0] as Long }
                assertEquals(listOf(1L), hits, "短语必须按词位相邻，2 的词序不同不应命中")
            }
        }
    }

    // ── 中文（ngram）──────────────────────────────────────────────

    @Test
    fun `中文全文检索`() {
        connect().use { c ->
            setupChinese(c)
            c.st { s ->
                s.execute(
                    """
                    INSERT INTO docs VALUES
                    (1, '对象存储是很便宜的'),
                    (2, '关系型数据库需要事务支持'),
                    (3, '存算分离是云原生的趋势')
                    """.trimIndent()
                )
                val hits = s.query("SELECT id FROM docs WHERE MATCH(content) AGAINST('对象存储')")
                    .map { it[0] as Long }
                assertTrue(1L in hits, "「对象存储」应命中第 1 行，实际 $hits")

                val hits2 = s.query("SELECT id FROM docs WHERE MATCH(content) AGAINST('数据库')")
                    .map { it[0] as Long }
                assertTrue(2L in hits2, "「数据库」应命中第 2 行，实际 $hits2")
            }
        }
    }

    @Test
    fun `standard 分词器切不动中文 - 这是必须显式选 ngram 的理由`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS ftdb")
                s.execute("USE ftdb")
                s.execute("DROP TABLE IF EXISTS bad_zh")
                s.execute("CREATE TABLE bad_zh (id BIGINT NOT NULL, content VARCHAR(300), PRIMARY KEY (id))")
                s.execute("CREATE FULLTEXT INDEX ft_bad ON bad_zh(content) WITH PARSER standard")
                s.execute("INSERT INTO bad_zh VALUES (1, '对象存储是很便宜的')")
                // standard 把 CJK 全部当分隔符 → 索引里一个词都没有。
                // 此时**必须报错而不是返回空集**：空集会让使用者以为
                // 「就是没有匹配的行」，而真正的问题是分词器选错了。
                // 静默返回错误答案比显式失败危险得多。
                val e = assertFailsWith<SQLException> {
                    s.query("SELECT id FROM bad_zh WHERE MATCH(content) AGAINST('对象存储')")
                }
                assertTrue(
                    e.message!!.contains("ngram"),
                    "错误信息应直接给出解法（用 ngram）：${e.message}",
                )
            }
        }
    }

    // ── 索引维护的一致性 ──────────────────────────────────────────

    @Test
    fun `删除后检索不到`() {
        connect().use { c ->
            setupEnglish(c)
            c.st { s ->
                s.execute("INSERT INTO articles VALUES (1,'A','unique marker here'),(2,'B','other text')")
                assertEquals(1, s.query("SELECT id FROM articles WHERE MATCH(body) AGAINST('marker')").size)

                s.execute("DELETE FROM articles WHERE id = 1")
                assertEquals(
                    0, s.query("SELECT id FROM articles WHERE MATCH(body) AGAINST('marker')").size,
                    "被删除的行不能再被检索到——索引维护漏了会得到过期结果",
                )
            }
        }
    }

    @Test
    fun `更新后旧词查不到新词查得到`() {
        connect().use { c ->
            setupEnglish(c)
            c.st { s ->
                s.execute("INSERT INTO articles VALUES (1,'A','originalword')")
                assertEquals(1, s.query("SELECT id FROM articles WHERE MATCH(body) AGAINST('originalword')").size)

                s.execute("UPDATE articles SET body = 'replacementword' WHERE id = 1")
                assertEquals(
                    0, s.query("SELECT id FROM articles WHERE MATCH(body) AGAINST('originalword')").size,
                    "改掉的旧词不应再命中",
                )
                assertEquals(
                    1, s.query("SELECT id FROM articles WHERE MATCH(body) AGAINST('replacementword')").size,
                    "新词必须能命中",
                )
            }
        }
    }

    @Test
    fun `建索引时回填已有数据`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS ftdb")
                s.execute("USE ftdb")
                s.execute("DROP TABLE IF EXISTS backfill")
                s.execute("CREATE TABLE backfill (id BIGINT NOT NULL, body VARCHAR(200), PRIMARY KEY (id))")
                // 先写数据，后建索引
                s.execute("INSERT INTO backfill VALUES (1,'existing content'),(2,'another row')")
                s.execute("CREATE FULLTEXT INDEX ft_b ON backfill(body) WITH PARSER standard")

                // 不回填的话，新建的索引只能查到之后写入的行——
                // 使用者会以为索引坏了（事实上确实坏了）
                assertEquals(
                    1, s.query("SELECT id FROM backfill WHERE MATCH(body) AGAINST('existing')").size,
                    "建索引时必须把已有数据一并索引进去",
                )
            }
        }
    }

    @Test
    fun `全文检索与其它谓词组合`() {
        connect().use { c ->
            setupEnglish(c)
            c.st { s ->
                s.execute(
                    """
                    INSERT INTO articles VALUES
                    (1, 'A', 'searchable text one'),
                    (2, 'B', 'searchable text two'),
                    (3, 'C', 'searchable text three')
                    """.trimIndent()
                )
                // 全文谓词负责取候选，其余谓词逐行过滤
                val hits = s.query(
                    "SELECT id FROM articles WHERE MATCH(body) AGAINST('searchable') AND id >= 2"
                ).map { it[0] as Long }.sorted()
                assertEquals(listOf(2L, 3L), hits, "剩余谓词必须仍然生效")
            }
        }
    }

    @Test
    fun `没有全文索引时给出可操作的报错`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS ftdb")
                s.execute("USE ftdb")
                s.execute("DROP TABLE IF EXISTS noidx")
                s.execute("CREATE TABLE noidx (id BIGINT NOT NULL, body VARCHAR(200), PRIMARY KEY (id))")
                s.execute("INSERT INTO noidx VALUES (1,'hello world')")

                // 静默返回空集是最糟的处理：使用者会以为「就是没有匹配的行」
                val e = assertFailsWith<SQLException> {
                    s.query("SELECT id FROM noidx WHERE MATCH(body) AGAINST('hello')")
                }
                assertTrue(
                    e.message!!.contains("索引"),
                    "错误信息应告诉使用者要建索引：${e.message}",
                )
            }
        }
    }

    @Test
    fun `未知分词器在 DDL 时被拒绝`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS ftdb")
                s.execute("USE ftdb")
                s.execute("DROP TABLE IF EXISTS wtf")
                s.execute("CREATE TABLE wtf (id BIGINT NOT NULL, body VARCHAR(200), PRIMARY KEY (id))")
                assertFailsWith<SQLException> {
                    s.execute("CREATE FULLTEXT INDEX ft_w ON wtf(body) WITH PARSER bogus")
                }
            }
        }
    }
}
