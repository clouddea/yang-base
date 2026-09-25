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
 * MySQL 协议兼容中**最容易静默出错**的那几处（设计文档 §11.1、§11.1.1）。
 *
 * 这些用例的共同点：出错时**不报错**，而是给出看起来正常的结果。
 * 乱码看起来像数据写坏了；漏结果集看起来像数据丢了；游标不支持则表现为
 * 客户端在大结果集上失败——而测试里通常只有几行数据，永远碰不到。
 */
class ProtocolCompatTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-proto")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "proto", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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

    private fun url(extra: String = "") = "jdbc:mysql://127.0.0.1:$port/pdb" +
        "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=UTF-8$extra"

    private fun connect(extra: String = ""): Connection = DriverManager.getConnection(url(extra), "root", "")

    private fun <T> Connection.q(b: (Statement) -> T): T = createStatement().use(b)

    private fun str(c: Connection, sql: String): String? = c.q { s ->
        s.executeQuery(sql).use { rs -> if (rs.next()) rs.getString(1) else null }
    }

    private fun setup(c: Connection, ddl: String) = c.q { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS pdb")
        s.execute("USE pdb")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute(ddl)
    }

    // ── 字符集（陷阱 T1 / T2）──────────────────────────────────────

    /**
     * ★ 中文往返。这是 T1 那条陷阱的直接检验：任何一处编码没对齐，
     * 这里读回来的就不是原来那个字符串，而且**不会有任何报错**。
     */
    @Test
    fun `中文与四字节 emoji 都能往返`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, s VARCHAR(64), PRIMARY KEY (id))")
            // 含引号的那个用 SQL 的 `''` 转义写法
            val samples = listOf("数据库", "对象存储是唯一真相源", "𠮷野家", "emoji🙂🎉", "quote'inside", "双引号\"在内")
            c.q { s ->
                for ((i, v) in samples.withIndex()) {
                    // SQL 字面量的 `'` 要写成 `''`
                    val lit = v.replace("'", "''")
                    s.execute("INSERT INTO t VALUES (${i + 1}, '$lit')")
                }
            }
            c.q { s ->
                for ((i, v) in samples.withIndex()) {
                    s.executeQuery("SELECT s FROM t WHERE id = ${i + 1}").use { rs ->
                        rs.next()
                        assertEquals(v, rs.getString(1), "第 ${i + 1} 个字符串往返后变了——编码没对齐")
                    }
                }
            }
        }
    }

    /** 预编译路径（二进制协议）也必须往返正确。 */
    @Test
    fun `预编译语句的中文参数往返正确`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, s VARCHAR(64), PRIMARY KEY (id))")
            c.prepareStatement("INSERT INTO t VALUES (?, ?)").use { ps ->
                ps.setLong(1, 1L)
                ps.setString(2, "中文参数🙂")
                ps.executeUpdate()
            }
            c.prepareStatement("SELECT s FROM t WHERE id = ?").use { ps ->
                ps.setLong(1, 1L)
                ps.executeQuery().use { rs ->
                    rs.next()
                    assertEquals("中文参数🙂", rs.getString(1))
                }
            }
        }
    }

    /**
     * 不支持的字符集**必须显式报错**，而不是收下之后按 utf8mb4 处理。
     *
     * 后者正是 §11.1.1 T1 描述的静默错误：客户端以为在用 gbk、服务端在用 utf8mb4，
     * 链路上没有任何一处报错。
     */
    @Test
    fun `不支持的字符集被显式拒绝`() {
        connect().use { c ->
            val e = assertFailsWith<SQLException> {
                c.q { s -> s.execute("SET NAMES gbk") }
            }
            assertTrue(
                e.message!!.contains("Unknown character set"),
                "应当明确说这是不支持的字符集：${e.message}",
            )
            // 支持的名字仍然要正常工作
            c.q { s -> s.execute("SET NAMES utf8mb4") }
        }
    }

    /** `SET character_set_results = NULL` 是 Connector/J 会发的写法（T1），不能被拒。 */
    @Test
    fun `character_set_results 设为 NULL 不被拒绝`() {
        connect().use { c ->
            c.q { s -> s.execute("SET character_set_results = NULL") }
            c.q { s ->
                s.executeQuery("SELECT '中文'").use { rs ->
                    rs.next()
                    assertEquals("中文", rs.getString(1))
                }
            }
        }
    }

    /** `@@character_set_client` 要如实回报**会话**的值，不能写死 utf8mb4。 */
    @Test
    fun `会话字符集如实回报`() {
        connect().use { c ->
            c.q { s ->
                s.execute("SET NAMES latin1")
                s.executeQuery("SELECT @@character_set_client").use { rs ->
                    rs.next()
                    assertEquals("latin1", rs.getString(1))
                }
                s.execute("SET NAMES utf8mb4")
                s.executeQuery("SELECT @@character_set_client").use { rs ->
                    rs.next()
                    assertEquals("utf8mb4", rs.getString(1))
                }
            }
        }
    }

    // ── 多语句 ────────────────────────────────────────────────────

    /**
     * ★ 多语句：**每一个**结果集的收尾包都要带 `SERVER_MORE_RESULTS_EXISTS`，
     * 除了最后一条。
     *
     * 漏掉 OK 那半边的话，`INSERT; SELECT` 会让客户端在 OK 之后就停止读取，
     * 把 SELECT 的结果集留在连接里——下一个查询读到的是上一个查询的残留，
     * 于是**结果整体错位一格**，而且不报错。
     */
    @Test
    fun `多语句的结果集不会错位`() {
        connect("&allowMultiQueries=true").use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id))")
            c.q { s ->
                // JDBC 的标准遍历：`getMoreResults()` 返回 false 时要用
                // `getUpdateCount() == -1` 区分「下一个结果是 OK」与「没有更多结果」。
                // 判定标准是**能不能走到最后那个结果集**：只要某个收尾包漏了
                // SERVER_MORE_RESULTS_EXISTS，这个循环就会提前结束，
                // 于是要么拿不到结果集、要么拿到上一批的残留。
                var ret = s.execute("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); SELECT id FROM t")
                val updates = ArrayList<Int>()
                var ids: List<Long>? = null
                var guard = 0
                while (guard++ < 10) {
                    if (ret) {
                        s.resultSet.use { rs ->
                            val out = ArrayList<Long>()
                            while (rs.next()) out.add(rs.getLong(1))
                            ids = out
                        }
                    } else {
                        val n = s.updateCount
                        if (n == -1) break
                        updates.add(n)
                    }
                    ret = s.getMoreResults()
                }
                assertEquals(listOf(1, 1), updates, "两个 INSERT 的 OK 结果必须都送回来")
                assertEquals(listOf(1L, 2L), ids, "最后那个 SELECT 的结果集丢了或错位了")
            }
            assertEquals(2, c.q { s -> s.executeQuery("SELECT id FROM t").use { rs ->
                var n = 0; while (rs.next()) n++; n
            } })
        }
    }

    /** 没协商 `CLIENT_MULTI_STATEMENTS` 时，多语句必须被拒绝而不是照执行。 */
    @Test
    fun `未协商多语句时被拒绝`() {
        connect().use { c ->          // 默认不开 allowMultiQueries
            val e = assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT 1; SELECT 2") }
            }
            assertTrue(
                e.message!!.contains("CLIENT_MULTI_STATEMENTS"),
                "错误信息应指出是连接没开多语句：${e.message}",
            )
        }
    }

    // ── 服务端游标（陷阱 T6）─────────────────────────────────────

    /**
     * ★ 服务端游标：`useCursorFetch=true` 时 Connector/J 走
     * `CURSOR_TYPE_READ_ONLY` + `COM_STMT_FETCH` 分批取数。
     *
     * 不支持它时，客户端会在**大结果集**上失败——而普通的几行数据测不出来，
     * 所以这里特意灌 500 行并把 fetch size 调小，逼它真的分批取。
     */
    @Test
    fun `服务端游标能分批取完整个结果集`() {
        connect("&useCursorFetch=true&defaultFetchSize=50").use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), PRIMARY KEY (id))")
            c.q { s ->
                s.execute("INSERT INTO t VALUES " + (1..500).joinToString(",") { "($it, 'v$it')" })
            }

            c.prepareStatement("SELECT id, v FROM t ORDER BY id").use { ps ->
                ps.fetchSize = 50          // ★ 小于结果集，强制走 COM_STMT_FETCH
                ps.executeQuery().use { rs ->
                    var n = 0
                    var last = 0L
                    while (rs.next()) {
                        val id = rs.getLong(1)
                        assertEquals("v$id", rs.getString(2), "第 $id 行的值错了——游标分批取数错位")
                        assertTrue(id > last, "游标返回的行序被打乱了：$id 不大于 $last")
                        last = id
                        n++
                    }
                    assertEquals(500, n, "游标分批取数漏了行")
                }
            }
        }
    }

    /** 游标只取一部分就关掉，不能影响连接上后续的查询。 */
    @Test
    fun `提前关闭游标不影响后续查询`() {
        connect("&useCursorFetch=true&defaultFetchSize=10").use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id))")
            c.q { s -> s.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)") }
            c.prepareStatement("SELECT id FROM t").use { ps ->
                ps.fetchSize = 10
                ps.executeQuery().use { rs ->
                    rs.next()
                    assertEquals(1L, rs.getLong(1))
                }                    // 只取一行就关
            }
            // 连接必须仍然干净可用
            c.q { s ->
                s.executeQuery("SELECT COUNT(*) FROM t").use { rs ->
                    rs.next()
                    assertEquals(5L, rs.getLong(1))
                }
            }
        }
    }

    // ── CLIENT_DEPRECATE_EOF ─────────────────────────────────────

    /**
     * ★ 协商 `CLIENT_DEPRECATE_EOF` 的连接也必须能正常取数。
     *
     * 协商成功时结果集的**收尾包从 EOF 换成 OK 包**，形状不同（多两个
     * lenenc 字段）。写错的话客户端会一直等下一个包或者错位解析——
     * 而**没有协商的连接走的还是老路径**，所以普通的用例完全测不到它。
     *
     * 这条用例的第一件事是**证明协议位真的协商上了**：不证明的话，
     * 客户端没协商时它照样全绿，等于什么都没测（"防伪措施"）。
     */
    @Test
    fun `协商 DEPRECATE_EOF 的连接能正常取数`() {
        connect("&useServerPrepStmts=true&useCursorFetch=true&allowMultiQueries=true").use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(16), PRIMARY KEY (id))")
            c.q { s -> s.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')") }

            // ① 先证明这条连接真的协商了 DEPRECATE_EOF，否则下面的断言毫无意义
            val flags = str(c, "SELECT @@yangbase_protocol_flags")
            assertTrue(
                flags != null && flags.contains("deprecate_eof"),
                "这条连接没有协商 CLIENT_DEPRECATE_EOF，用例会空跑：flags=$flags",
            )

            // ② 文本协议的结果集（收尾包是 0xFE 开头的 OK）
            c.q { s ->
                s.executeQuery("SELECT id, v FROM t ORDER BY id").use { rs ->
                    val out = ArrayList<String>()
                    while (rs.next()) out.add("${rs.getLong(1)}=${rs.getString(2)}")
                    assertEquals(listOf("1=a", "2=b", "3=c"), out)
                }
            }
            // ③ 二进制协议 + 服务端游标（收尾包同样换了形状）
            c.prepareStatement("SELECT id FROM t ORDER BY id").use { ps ->
                ps.fetchSize = 2
                ps.executeQuery().use { rs ->
                    var n = 0
                    while (rs.next()) n++
                    assertEquals(3, n)
                }
            }
            // ④ 收尾包换形状之后，多语句的 SERVER_MORE_RESULTS_EXISTS 仍然要对
            //    （它的置位就发生在那个换过形状的 OK 包里）
            c.q { s ->
                s.execute("SELECT id FROM t; SELECT 9")
                var ret = true
                var sawNine = false
                var guard = 0
                while (ret && guard++ < 10) {
                    s.resultSet?.use { rs ->
                        while (rs.next()) if (rs.getLong(1) == 9L) sawNine = true
                    }
                    ret = s.getMoreResults() || s.updateCount != -1
                }
                assertTrue(sawNine, "第二个结果集没读到——MORE_RESULTS 位在 OK 形式的收尾包里丢了")
            }
        }
    }

    // ── 工具链需要的边角 ──────────────────────────────────────────

    /** `mysqldump` 会用的 `LOCK TABLES` 必须是「无操作的成功」，而不是报错。 */
    @Test
    fun `LOCK TABLES 与 SET 序列不打断 dump`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id))")
            c.q { s ->
                s.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                s.execute("SET NAMES utf8mb4")
                s.execute("SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO'")
                s.execute("LOCK TABLES t WRITE")
                s.execute("UNLOCK TABLES")
                s.executeQuery("SHOW CREATE TABLE t").use { rs ->
                    rs.next()
                    assertTrue(rs.getString(2).contains("CREATE TABLE"), "SHOW CREATE TABLE 必须给得出 DDL")
                }
            }
        }
    }
}
