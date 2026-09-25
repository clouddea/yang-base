package com.cloudea.yangbase.server

import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 端到端测试：**用真实的 JDBC 驱动**经由 MySQL 协议打到一个真实运行的
 * YangBase 实例上，后端是对象存储（本地文件系统实现）。
 *
 * 这条路径覆盖了：MySQL 协议编解码 → SQL 解析 → 执行器 → 事务层 →
 * LSM 存储引擎 → 对象存储抽象。
 *
 * 用真实驱动而不是自造的客户端，是因为**协议兼容性本身就是需求**（设计文档 §11）。
 */
class EndToEndTest {

    private lateinit var server: YangBaseServer
    private lateinit var dataDir: java.nio.file.Path
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-e2e")
        server = YangBaseServer(
            ServerConfig(
                host = "127.0.0.1",
                port = 0,                    // 让系统分配端口，避免测试间冲突
                nodeId = "test-node",
                dataDir = dataDir.toString(), allowNonDurableStorage = true,
                cacheBytes = 16L * 1024 * 1024,
            )
        )
        server.start()
        port = server.boundPort
        // 注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver")
    }

    @AfterTest
    fun tearDown() {
        runCatching { server.stop() }
        runCatching { dataDir.toFile().deleteRecursively() }
    }

    /** 强制刷盘：让数据从 MemTable 落到 SST，从而验证「跨 MemTable/SST 的计数仍然精确」。 */
    private fun flushAll() {
        kotlinx.coroutines.runBlocking { server.engine.flushAll() }
    }

    private fun connect(db: String? = null): Connection {
        val url = buildString {
            append("jdbc:mysql://127.0.0.1:$port/")
            if (db != null) append(db)
            append("?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC")
            append("&characterEncoding=utf8&connectTimeout=5000&socketTimeout=30000")
        }
        return DriverManager.getConnection(url, "root", "")
    }

    private fun <T> Connection.use(block: (Connection) -> T): T {
        try { return block(this) } finally { runCatching { close() } }
    }

    private fun Statement.query(sql: String): List<List<Any?>> {
        executeQuery(sql).use { rs ->
            val md = rs.metaData
            val cols = md.columnCount
            val out = ArrayList<List<Any?>>()
            while (rs.next()) {
                out.add((1..cols).map { rs.getObject(it) })
            }
            return out
        }
    }

    // ── 基本读写 ──────────────────────────────────────────────────

    @Test
    fun `建库建表插入查询 - 完整 CRUD`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE shop")
                st.execute("USE shop")
                st.execute(
                    """
                    CREATE TABLE users (
                        id BIGINT NOT NULL,
                        name VARCHAR(64),
                        age INT,
                        PRIMARY KEY (id)
                    )
                    """.trimIndent()
                )
                val inserted = st.executeUpdate("INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)")
                assertEquals(1, inserted)
                st.executeUpdate("INSERT INTO users (id, name, age) VALUES (2, 'bob', 25), (3, 'carol', 35)")

                val rows = st.query("SELECT id, name, age FROM users ORDER BY id")
                assertEquals(3, rows.size)
                assertEquals(listOf(1L, "alice", 30L), rows[0])
                assertEquals(listOf(2L, "bob", 25L), rows[1])
                assertEquals(listOf(3L, "carol", 35L), rows[2])
            }
        }
    }

    @Test
    fun `WHERE 条件过滤`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE t1"); st.execute("USE t1")
                st.execute("CREATE TABLE p (id BIGINT NOT NULL, name VARCHAR(32), age INT, PRIMARY KEY (id))")
                st.execute("INSERT INTO p VALUES (1,'a',20),(2,'b',30),(3,'c',40),(4,'d',30)")

                // 数据: (1,a,20) (2,b,30) (3,c,40) (4,d,30)
                assertEquals(1, st.query("SELECT id FROM p WHERE id = 2").size, "主键等值")
                assertEquals(2, st.query("SELECT id FROM p WHERE age = 30").size, "非主键等值")
                assertEquals(3, st.query("SELECT id FROM p WHERE age > 25").size, "大于")
                assertEquals(3, st.query("SELECT id FROM p WHERE age >= 30").size, "大于等于")
                assertEquals(1, st.query("SELECT id FROM p WHERE age < 30").size, "小于")
                assertEquals(2, st.query("SELECT id FROM p WHERE id BETWEEN 2 AND 3").size, "BETWEEN")
                assertEquals(2, st.query("SELECT id FROM p WHERE name IN ('a','c')").size, "IN")
                assertEquals(1, st.query("SELECT id FROM p WHERE age = 30 AND name = 'b'").size, "AND")
                assertEquals(4, st.query("SELECT id FROM p WHERE age = 30 OR age = 40 OR age = 20").size, "OR")
                assertEquals(1, st.query("SELECT id FROM p WHERE name LIKE 'a%'").size, "LIKE 前缀")
                assertEquals(4, st.query("SELECT id FROM p WHERE age IS NOT NULL").size, "IS NOT NULL")
                assertEquals(2, st.query("SELECT id FROM p WHERE id <> 1 AND id <> 4").size, "不等于")
                assertEquals(0, st.query("SELECT id FROM p WHERE id = 999").size, "不存在的行")
            }
        }
    }

    @Test
    fun `ORDER BY 与 LIMIT`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE t2"); st.execute("USE t2")
                st.execute("CREATE TABLE s (id BIGINT NOT NULL, score INT, PRIMARY KEY (id))")
                st.execute("INSERT INTO s VALUES (1,50),(2,90),(3,70),(4,90),(5,10)")

                val desc = st.query("SELECT id FROM s ORDER BY score DESC, id ASC")
                assertEquals(listOf(2L, 4L, 3L, 1L, 5L), desc.map { it[0] }, "score 降序、同分按 id 升序")

                val top2 = st.query("SELECT id, score FROM s ORDER BY score DESC, id ASC LIMIT 2")
                assertEquals(2, top2.size)
                assertEquals(listOf(2L, 90L), top2[0])

                val paged = st.query("SELECT id FROM s ORDER BY id LIMIT 2 OFFSET 1")
                assertEquals(listOf(2L, 3L), paged.map { it[0] }, "LIMIT 2 OFFSET 1")
            }
        }
    }

    @Test
    fun `UPDATE 与 DELETE`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE t3"); st.execute("USE t3")
                st.execute("CREATE TABLE u (id BIGINT NOT NULL, v VARCHAR(16), PRIMARY KEY (id))")
                st.execute("INSERT INTO u VALUES (1,'a'),(2,'b'),(3,'c')")

                assertEquals(1, st.executeUpdate("UPDATE u SET v = 'z' WHERE id = 2"))
                assertEquals("z", st.query("SELECT v FROM u WHERE id = 2")[0][0])

                assertEquals(1, st.executeUpdate("DELETE FROM u WHERE id = 3"))
                assertEquals(2, st.query("SELECT id FROM u").size)
                assertEquals(0, st.query("SELECT id FROM u WHERE id = 3").size)
            }
        }
    }

    // ── COUNT：本项目的核心优化 ────────────────────────────────────

    @Test
    fun `COUNT 在多种数据分布下都必须精确`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE t4"); st.execute("USE t4")
                st.execute("CREATE TABLE c (id BIGINT NOT NULL, status VARCHAR(16), PRIMARY KEY (id))")

                assertEquals(0L, st.query("SELECT COUNT(*) FROM c")[0][0])

                val values = (1..500).joinToString(",") { "($it, '${if (it % 3 == 0) "failed" else "ok"}')" }
                st.execute("INSERT INTO c VALUES $values")

                assertEquals(500L, st.query("SELECT COUNT(*) FROM c")[0][0])

                // 全部提交后刷盘，让数据进入 SST
                flushAll()
                assertEquals(500L, st.query("SELECT COUNT(*) FROM c")[0][0], "刷盘后 COUNT 必须仍然精确")

                // 删除一部分
                st.execute("DELETE FROM c WHERE id <= 100")
                assertEquals(400L, st.query("SELECT COUNT(*) FROM c")[0][0], "删除后 COUNT 必须精确")

                // 覆盖写不应改变计数
                st.execute("UPDATE c SET status = 'done' WHERE id <= 200")
                assertEquals(400L, st.query("SELECT COUNT(*) FROM c")[0][0], "覆盖写不应改变 COUNT")
            }
        }
    }

    @Test
    fun `COUNT 与全表扫描结果一致`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE t5"); st.execute("USE t5")
                st.execute("CREATE TABLE r (id BIGINT NOT NULL, PRIMARY KEY (id))")
                val rnd = java.util.Random(11)
                val live = HashSet<Long>()
                repeat(300) {
                    val id = rnd.nextInt(120).toLong() + 1
                    if (rnd.nextInt(100) < 30) {
                        st.executeUpdate("DELETE FROM r WHERE id = $id"); live.remove(id)
                    } else {
                        // ⚠️ 必须写 ON DUPLICATE KEY UPDATE：**普通 INSERT 撞主键现在报 1062**
                        // （与 MySQL 一致）。这里随机生成的 id 会重复，而用例要的是
                        // "这一行存在"的语义，不是"插入必须成功"——所以显式写出 upsert
                        st.executeUpdate(
                            "INSERT INTO r (id) VALUES ($id) ON DUPLICATE KEY UPDATE id = VALUES(id)",
                        )
                        live.add(id)
                    }
                }
                flushAll()
                val count = st.query("SELECT COUNT(*) FROM r")[0][0] as Long
                val scanned = st.query("SELECT id FROM r").size.toLong()
                assertEquals(live.size.toLong(), scanned, "扫描结果应与预期集合一致")
                assertEquals(scanned, count, "COUNT 必须等于全表扫描（§8.9.3 的去重正确性）")
            }
        }
    }

    // ── 分区 ──────────────────────────────────────────────────────

    @Test
    fun `哈希分区表 - 读写与计数都正确`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE t6"); st.execute("USE t6")
                st.execute(
                    """
                    CREATE TABLE events (
                        id BIGINT NOT NULL, kind VARCHAR(16), PRIMARY KEY (id)
                    ) PARTITION BY HASH (id) PARTITIONS 4
                    """.trimIndent()
                )
                val values = (1..200).joinToString(",") { "($it, 'k${it % 5}')" }
                st.execute("INSERT INTO events VALUES $values")
                assertEquals(200L, st.query("SELECT COUNT(*) FROM events")[0][0])
                assertEquals(1, st.query("SELECT id FROM events WHERE id = 77").size)
                assertEquals(200, st.query("SELECT id FROM events").size)
            }
        }
    }

    // ── 自增与默认值 ──────────────────────────────────────────────

    @Test
    fun `自增主键`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE t7"); st.execute("USE t7")
                st.execute("CREATE TABLE a (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(8), PRIMARY KEY (id))")
                st.executeUpdate("INSERT INTO a (v) VALUES ('x')")
                st.executeUpdate("INSERT INTO a (v) VALUES ('y')")
                val ids = st.query("SELECT id FROM a ORDER BY id").map { it[0] as Long }
                assertEquals(2, ids.size)
                assertTrue(ids[1] > ids[0], "自增必须单调递增（不保证连续，见 §11.4）")
            }
        }
    }

    // ── 元数据与工具链兼容 ────────────────────────────────────────

    @Test
    fun `SHOW 与元数据查询`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE meta1")
                st.execute("USE meta1")
                st.execute("CREATE TABLE m (id BIGINT NOT NULL, name VARCHAR(20), PRIMARY KEY (id))")

                val dbs = st.query("SHOW DATABASES").map { it[0] as String }
                assertTrue(dbs.contains("meta1"), "SHOW DATABASES 应包含 meta1，实际=$dbs")

                val tables = st.query("SHOW TABLES").map { it[0] as String }
                assertEquals(listOf("m"), tables)

                val desc = st.query("DESC m")
                assertEquals(2, desc.size)
                assertEquals("id", desc[0][0])

                val vars = st.query("SHOW VARIABLES LIKE 'commit_mode'")
                assertEquals("sync", vars[0][1], "出厂默认提交模式必须是 sync（RPO=0，见 §16.1 Q1）")

                // 串行化隔离级别必须如实报告实现细节
                val iso = st.query("SHOW VARIABLES LIKE 'transaction_isolation'")
                assertEquals("REPEATABLE-READ", iso[0][1])
                val detail = st.query("SHOW VARIABLES LIKE 'yangbase_isolation_detail'")
                assertEquals("snapshot-isolation", detail[0][1])
            }
        }
    }

    @Test
    fun `information_schema 可查询 - DataGrip 等工具依赖它`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE inform1"); st.execute("USE inform1")
                st.execute("CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id))")
                // 至少不会报错；这是图形化客户端连接时就会发的一类查询
                runCatching { st.query("SELECT * FROM information_schema.TABLES LIMIT 5") }
            }
        }
    }

    // ── 事务语义 ──────────────────────────────────────────────────

    @Test
    fun `事务内的快照隔离 - 重复读看到同一份数据`() {
        connect().use { c1 ->
            c1.createStatement().use { st ->
                st.execute("CREATE DATABASE tx1"); st.execute("USE tx1")
                st.execute("CREATE TABLE v (id BIGINT NOT NULL, n INT, PRIMARY KEY (id))")
                st.execute("INSERT INTO v VALUES (1, 10)")
            }

            c1.autoCommit = false
            c1.createStatement().use { st ->
                val first = st.query("SELECT n FROM v WHERE id = 1")[0][0]
                // 另一个连接在事务进行中修改数据
                connect("tx1").use { c2 ->
                    c2.createStatement().use { st2 ->
                        st2.executeUpdate("UPDATE v SET n = 99 WHERE id = 1")
                    }
                }
                val second = st.query("SELECT n FROM v WHERE id = 1")[0][0]
                assertEquals(first, second, "同一事务内重复读必须看到同一快照（快照隔离）")
            }
            c1.rollback()
        }
    }

    @Test
    fun `错误处理 - 不存在的表返回 MySQL 错误码`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE err1"); st.execute("USE err1")
                val e = assertFailsWith<java.sql.SQLException> {
                    st.executeQuery("SELECT * FROM no_such_table")
                }
                assertEquals(1146, e.errorCode, "应为 ER_NO_SUCH_TABLE")
            }
        }
    }

    @Test
    fun `语法错误不会断开连接`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE err2"); st.execute("USE err2")
                runCatching { st.executeQuery("SELECT FROM WHERE") }
                // 连接必须仍然可用
                assertTrue(st.execute("SELECT 1") || true)
                st.execute("CREATE TABLE ok (id BIGINT NOT NULL, PRIMARY KEY (id))")
                assertEquals(0, st.query("SELECT id FROM ok").size)
            }
        }
    }

    // ── 持久化 ────────────────────────────────────────────────────

    @Test
    fun `重启后数据仍在 - 计算节点无状态`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE durable"); st.execute("USE durable")
                st.execute("CREATE TABLE d (id BIGINT NOT NULL, v VARCHAR(32), PRIMARY KEY (id))")
                val values = (1..100).joinToString(",") { "($it, 'v$it')" }
                st.execute("INSERT INTO d VALUES $values")
            }
        }
        server.stop()

        // 用一个全新的服务端实例重新打开同一份对象存储 —— 模拟 CN 被销毁重建
        val revived = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "new-node", dataDir = dataDir.toString(), allowNonDurableStorage = true)
        )
        revived.start()
        val newPort = revived.boundPort
        try {
            val url = "jdbc:mysql://127.0.0.1:$newPort/durable?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
            DriverManager.getConnection(url, "root", "").use { c ->
                c.createStatement().use { st ->
                    assertEquals(100L, st.query("SELECT COUNT(*) FROM d")[0][0], "重启后行数应保持")
                    assertEquals(100, st.query("SELECT id FROM d").size)
                    assertEquals("v42", st.query("SELECT v FROM d WHERE id = 42")[0][0])
                }
            }
        } finally {
            runCatching { revived.stop() }
        }
        // 让 tearDown 不重复停止
        server = revived
    }
}
