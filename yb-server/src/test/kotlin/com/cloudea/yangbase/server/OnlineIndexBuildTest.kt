package com.cloudea.yangbase.server

import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.IndexState
import com.cloudea.yangbase.common.ReadTs
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 在线索引构建（设计文档 §8.7）。
 *
 * ## 这组用例盯的是「静默少行」，不是「建得成建不成」
 *
 * 从已有数据的表上建索引，最容易出的不是报错，而是**索引里少了几项**：
 * 查询走上索引就不再回退扫描，于是那些行凭空消失——不报错、不降级。
 *
 * 漏项的窗口只有一个：`read_ts` 与「索引发布进 schema」之间提交的写入。
 * 它在真实系统里很窄（一次 catalog 写的时间），靠并发碰运气是测不出来的。
 * 所以这里**直接构造这个窗口**：先取 read_ts，再插入，再让构建跑。
 * 这需要走引擎 API 而不是 JDBC——JDBC 那条路径会在同一条语句里把
 * read_ts 和发布挨着做完，窗口小到测不到。
 *
 * 另外两条用例盯状态机本身：
 * BUILDING 期间**查询不能用它**（用了就是少行），写入却必须维护它
 * （不维护就永远补不回来）。这两条互为反面，写反任何一条都会丢数据。
 */
class OnlineIndexBuildTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-online-idx")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "oidx", dataDir = dataDir.toString(), allowNonDurableStorage = true)
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
        "jdbc:mysql://127.0.0.1:$port/odx?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun <T> Connection.q(b: (Statement) -> T): T = createStatement().use(b)

    private fun setup(c: Connection, ddl: String) = c.q { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS odx")
        s.execute("USE odx")
        s.execute("DROP TABLE IF EXISTS t")
        s.execute(ddl)
    }

    private fun table() = server.engine.catalog.allTables().first { it.name == "t" }

    // ── 状态机 ────────────────────────────────────────────────────

    @Test
    fun `建索引期间查询不能用它 - 建完才可用`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
            c.q { s -> s.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)") }

            val t = table()
            val readTs = runBlocking { server.engine.currentReadTs() }
            // 发布成 BUILDING：写入从此维护它，但查询不许用
            val published = runBlocking {
                server.engine.catalog.addIndex(
                    t.databaseId, t.name,
                    com.cloudea.yangbase.common.IndexDef(
                        id = 0, name = "idx_v", kind = IndexKind.ORDERED,
                        columns = listOf("v"), state = IndexState.BUILDING,
                    ),
                )
            }
            val created = published.indexes.first { it.name == "idx_v" }
            assertEquals(IndexState.BUILDING, created.state)
            assertTrue(
                published.activeIndexes.none { it.name == "idx_v" },
                "BUILDING 的索引绝不能出现在 activeIndexes 里——查询会拿它取数，然后少返回行",
            )

            // 构建（含追平）
            runBlocking { server.engine.onlineIndexBuilder.build(published, created, readTs) }
            val active = runBlocking {
                server.engine.catalog.setIndexState(published.databaseId, t.name, "idx_v", IndexState.ACTIVE)
            }
            assertTrue(active.activeIndexes.any { it.name == "idx_v" }, "构建完必须翻成 ACTIVE")

            // 现在它必须真的能用，而且结果与全表扫描一致
            c.q { s ->
                s.executeQuery("SELECT id FROM t WHERE v = 20").use { rs ->
                    val ids = ArrayList<Long>()
                    while (rs.next()) ids.add(rs.getLong(1))
                    assertEquals(listOf(2L), ids)
                }
            }
        }
    }

    /**
     * ★ 核心用例：**构建窗口内提交的写入不能丢**。
     *
     * 手工把窗口撑开：先取 read_ts，再插入两行，然后才发布 BUILDING 并构建。
     * 那两行既不在 read_ts 的快照里（回填看不到），又因为索引那时还不在 schema 里
     * 而没有被写入路径维护——**只有追平阶段能救它们**。
     */
    @Test
    fun `构建窗口内提交的写入不会从索引里消失`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
            c.q { s -> s.execute("INSERT INTO t VALUES (1,10),(2,20)") }

            val t = table()
            // ① 快照时间戳：此刻表里只有 2 行
            val readTs = runBlocking { server.engine.currentReadTs() }

            // ② 窗口内的写入：它们晚于 read_ts，快照回填看不到
            c.q { s -> s.execute("INSERT INTO t VALUES (3,30),(4,20)") }

            // ③ 发布 + 构建 + 追平
            val published = runBlocking {
                server.engine.catalog.addIndex(
                    t.databaseId, t.name,
                    com.cloudea.yangbase.common.IndexDef(
                        id = 0, name = "idx_v", kind = IndexKind.ORDERED,
                        columns = listOf("v"), state = IndexState.BUILDING,
                    ),
                )
            }
            val created = published.indexes.first { it.name == "idx_v" }
            runBlocking { server.engine.onlineIndexBuilder.build(published, created, readTs) }
            runBlocking {
                server.engine.catalog.setIndexState(published.databaseId, t.name, "idx_v", IndexState.ACTIVE)
            }

            // ④ 通过索引取数：4 行都必须在
            val viaIndex = runBlocking {
                server.engine.lookupByIndex(
                    table(), created, listOf(com.cloudea.yangbase.common.Datum.I64(20)),
                    ReadTs(Long.MAX_VALUE),
                )
            }
            val ids = viaIndex.map { (it[0] as com.cloudea.yangbase.common.Datum.I64).v }.sorted()
            assertEquals(listOf(2L, 4L), ids, "窗口内插入的行从索引里消失了——查询会静默少返回行")
        }
    }

    /**
     * 构建窗口内的**更新**必须让旧值那一项失效。
     *
     * 只补新值、不抹旧值的话，`WHERE v = 10` 会命中一行其实已经是 20 的记录。
     */
    @Test
    fun `构建窗口内的更新不会留下旧值的索引项`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
            c.q { s -> s.execute("INSERT INTO t VALUES (1,10)") }

            val t = table()
            val readTs = runBlocking { server.engine.currentReadTs() }
            // 窗口内把 v 从 10 改成 20
            c.q { s -> s.execute("UPDATE t SET v = 20 WHERE id = 1") }

            val published = runBlocking {
                server.engine.catalog.addIndex(
                    t.databaseId, t.name,
                    com.cloudea.yangbase.common.IndexDef(
                        id = 0, name = "idx_v", kind = IndexKind.ORDERED,
                        columns = listOf("v"), state = IndexState.BUILDING,
                    ),
                )
            }
            val created = published.indexes.first { it.name == "idx_v" }
            runBlocking { server.engine.onlineIndexBuilder.build(published, created, readTs) }

            val stale = runBlocking {
                server.engine.lookupByIndex(
                    table(), created, listOf(com.cloudea.yangbase.common.Datum.I64(10)),
                    ReadTs(Long.MAX_VALUE),
                )
            }
            assertTrue(stale.isEmpty(), "旧值 10 的索引项没有被清掉，实际查到 ${stale.size} 行")
            val fresh = runBlocking {
                server.engine.lookupByIndex(
                    table(), created, listOf(com.cloudea.yangbase.common.Datum.I64(20)),
                    ReadTs(Long.MAX_VALUE),
                )
            }
            assertEquals(1, fresh.size, "新值 20 的索引项必须存在")
        }
    }

    /** 唯一索引在回填时发现重复必须失败，而且索引**停在 BUILDING**（查询不受影响）。 */
    @Test
    fun `唯一索引遇到已有重复值会失败且不影响查询`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
            c.q { s -> s.execute("INSERT INTO t VALUES (1,10),(2,10)") }

            val e = assertFailsWith<java.sql.SQLException> {
                c.q { s -> s.execute("CREATE UNIQUE INDEX uk_v ON t (v)") }
            }
            assertTrue(e.message!!.contains("重复"), "错误信息应说明是重复值：${e.message}")

            // 索引可能停在 BUILDING；无论哪种状态，查询都必须仍然拿得到两行
            c.q { s ->
                s.executeQuery("SELECT id FROM t WHERE v = 10").use { rs ->
                    val ids = ArrayList<Long>()
                    while (rs.next()) ids.add(rs.getLong(1))
                    assertEquals(listOf(1L, 2L), ids.sorted(), "建索引失败不该影响查询")
                }
            }
        }
    }

    /** 建成功后 `SHOW CREATE TABLE` 才列出它；构建期间对外不可见。 */
    @Test
    fun `SHOW CREATE TABLE 只列可用的索引`() {
        connect().use { c ->
            setup(c, "CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
            c.q { s -> s.execute("CREATE INDEX idx_v ON t (v)") }
            c.q { s ->
                s.executeQuery("SHOW CREATE TABLE t").use { rs ->
                    rs.next()
                    val ddl = rs.getString(2)
                    assertTrue(ddl.contains("idx_v"), "建完的索引应当出现在 DDL 里：$ddl")
                }
            }
        }
    }
}
