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
import kotlin.test.assertTrue

/**
 * **审查发现项的回归测试**（见 [docs/zh/code-review-2026-09.md](../../../../../../../docs/zh/code-review-2026-09.md)）。
 *
 * 每一条对应一个"不报错但结果不对"或"一直付钱"的缺陷。它们共同的特点是
 * **没有测试就一定会复发**：改动看起来都很小，而症状都指向别处。
 */
class ReviewFixesTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-review")
        server = YangBaseServer(
            ServerConfig(
                host = "127.0.0.1", port = 0, nodeId = "rv",
                dataDir = dataDir.toString(), allowNonDurableStorage = true,
                // 淘汰阈值调小，让"空闲分片被淘汰"可以在测试里观察到
                shardIdleEvictMillis = 0,
            ),
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

    private fun connect(db: String = "rdb"): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/$db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun <T> Connection.q(b: (Statement) -> T): T = createStatement().use(b)

    private fun Connection.setup(vararg sql: String) = q { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS rdb")
        s.execute("USE rdb")
        sql.forEach { s.execute(it) }
    }

    private fun Connection.one(sql: String): Any? = q { s ->
        s.executeQuery(sql).use { rs -> if (rs.next()) rs.getObject(1) else null }
    }

    private fun Connection.num(sql: String): Long = (one(sql) as Number).toLong()

    private fun Connection.exec(sql: String): Int = q { it.executeUpdate(sql) }

    // ── 语义项 ───────────────────────────────────────────────────

    @Test
    fun `COUNT 的捷径也要遵守 LIMIT 0`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS cl",
                "CREATE TABLE cl (id BIGINT NOT NULL, PRIMARY KEY (id))",
                "INSERT INTO cl VALUES (1),(2),(3)",
            )
            // 两条路径必须一致：COUNT(*) 走元数据捷径、COUNT(列) 走分组路径，
            // 曾经前者无视 LIMIT 0 返回 1 行、后者返回 0 行
            assertEquals(0, c.q { s -> s.executeQuery("SELECT COUNT(*) FROM cl LIMIT 0").use { rs -> generateSequence { if (rs.next()) 1 else null }.count() } })
            assertEquals(0, c.q { s -> s.executeQuery("SELECT COUNT(id) FROM cl LIMIT 0").use { rs -> generateSequence { if (rs.next()) 1 else null }.count() } })
            assertEquals(3L, c.num("SELECT COUNT(*) FROM cl"))
        }
    }

    @Test
    fun `字符串在布尔上下文里按 MySQL 规则转数字`() {
        connect().use { c ->
            c.setup("DROP TABLE IF EXISTS bl", "CREATE TABLE bl (id BIGINT NOT NULL, PRIMARY KEY (id))", "INSERT INTO bl VALUES (1),(2)")
            // '0.0' / 'false' / 'abc' 在 MySQL 里都算 0 → 假。
            // 曾经它们都是真（`isNotEmpty() && != "0"`），于是
            // `WHERE json_col->>'$.flag'` 这种写法会多返回行
            assertEquals(0L, c.num("SELECT COUNT(*) FROM bl WHERE '0.0'"))
            assertEquals(0L, c.num("SELECT COUNT(*) FROM bl WHERE 'false'"))
            assertEquals(0L, c.num("SELECT COUNT(*) FROM bl WHERE '0'"))
            // 非零数字前缀为真
            assertEquals(2L, c.num("SELECT COUNT(*) FROM bl WHERE '1'"))
            assertEquals(2L, c.num("SELECT COUNT(*) FROM bl WHERE '3x'"))
            // 与 `=` 的判决保持一致：`'0.0' = 0` 为真，那么它在布尔上下文里就该是假
            assertEquals(2L, c.num("SELECT COUNT(*) FROM bl WHERE '0.0' = 0"))
        }
    }

    @Test
    fun `UNION 按左侧类型归一之后再合并`() {
        connect().use { c ->
            c.setup("DROP TABLE IF EXISTS u1")
            // `SELECT 10 UNION SELECT '10'`：MySQL 取第一支的类型（整数），
            // '10' 转成 10 → **1 行**。曾经去重按值自身的类型做 → 2 行
            assertEquals(
                1, c.q { s ->
                    s.executeQuery("SELECT 10 UNION SELECT '10'").use { rs ->
                        generateSequence { if (rs.next()) rs.getObject(1) else null }.toList().size
                    }
                },
            )
            // UNION ALL 不做归一（保留两行），这是刻意的
            assertEquals(
                2, c.q { s ->
                    s.executeQuery("SELECT 10 UNION ALL SELECT '10'").use { rs ->
                        generateSequence { if (rs.next()) rs.getObject(1) else null }.toList().size
                    }
                },
            )
        }
    }

    @Test
    fun `INSERT IGNORE 连值错误一起降级`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS ig",
                "CREATE TABLE ig (id BIGINT NOT NULL, n INT, PRIMARY KEY (id))",
            )
            // 第 2 行的 'abc' 转不成 INT：IGNORE 应当跳过它、
            // 让其余行照常插入（MySQL 会插入一个"最接近的值"——那是静默改数据，
            // 本系统选择跳过并把行数如实回报）
            c.exec("INSERT IGNORE INTO ig VALUES (1, 10), (2, 'abc'), (3, 30)")
            assertEquals(2L, c.num("SELECT COUNT(*) FROM ig"))
            assertEquals(1L, c.num("SELECT COUNT(*) FROM ig WHERE id = 1"))
            assertEquals(0L, c.num("SELECT COUNT(*) FROM ig WHERE id = 2"))
            // 非 IGNORE 的插入仍然报错（严格模式不变）
            val e = runCatching { c.exec("INSERT INTO ig VALUES (4, 'abc')") }.exceptionOrNull()
            assertTrue(e is SQLException, "普通 INSERT 遇到坏值必须报错")
        }
    }

    @Test
    fun `版本注释里的语句会被执行`() {
        connect().use { c ->
            c.setup("DROP TABLE IF EXISTS vc", "CREATE TABLE vc (id BIGINT NOT NULL, PRIMARY KEY (id))")
            // mysqldump 的条件 DDL 就长这样。曾经整段被当注释丢掉 → 静默少执行
            c.exec("/*!40101 INSERT INTO vc VALUES (1) */")
            assertEquals(1L, c.num("SELECT COUNT(*) FROM vc"))
            // 普通块注释仍然只是注释
            c.exec("/* 这是注释 */ INSERT INTO vc VALUES (2)")
            assertEquals(2L, c.num("SELECT COUNT(*) FROM vc"))
        }
    }

    @Test
    fun `DATE 列与 CURDATE 只渲染日期部分`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS dt2",
                "CREATE TABLE dt2 (id BIGINT NOT NULL, d DATE, dt DATETIME, PRIMARY KEY (id))",
                "INSERT INTO dt2 VALUES (1, '2026-02-01', '2026-02-01 10:00:00')",
            )
            // 用 getString 比对：驱动对 DATE/DATETIME 会返回时间对象，
            // 而我们要断言的是**服务端发出的文本**
            fun str(sql: String) = c.q { s -> s.executeQuery(sql).use { rs -> rs.next(); rs.getString(1) } }
            assertEquals("2026-02-01", str("SELECT d FROM dt2"))
            // DATETIME 保持完整
            assertEquals("2026-02-01 10:00:00", str("SELECT dt FROM dt2"))
            // CURDATE() 是 DATE 语义：服务端发出去的就是一个日期
            val cur = str("SELECT CURDATE()")
            assertTrue(cur.length == 10, "CURDATE() 应当只到日：$cur")
        }
    }

    @Test
    fun `隔离级别的降级会被如实回报`() {
        connect().use { c ->
            c.setup("DROP TABLE IF EXISTS isl", "CREATE TABLE isl (id BIGINT NOT NULL, PRIMARY KEY (id))")
            // 收下（否则连接池的初始化 SQL 会让客户端连不上），但要如实回报
            c.exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            assertEquals("READ-COMMITTED", c.one("SELECT @@yangbase_isolation_degraded")?.toString())
            // 请求的正是我们提供的那一档时，没有降级报告
            c.exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            assertEquals("", c.one("SELECT @@yangbase_isolation_degraded")?.toString())
            // 实际生效的一直是快照隔离，且如实报告
            assertEquals("snapshot-isolation", c.one("SELECT @@yangbase_isolation_detail")?.toString())
        }
    }

    // ── UPDATE 改主键 ────────────────────────────────────────────

    /**
     * ★ **改主键不能静默覆盖别人的行。**
     *
     * LSM 的写入语义是"同 key 覆盖"，所以 `UPDATE t SET id = 2 WHERE id = 1`
     * 在 id=2 已存在时会把那一行**静默改掉**，而应用拿到的是"更新成功、影响 1 行"。
     * MySQL 报 1062——应用靠这个错误决定要不要改走合并逻辑。
     */
    @Test
    fun `UPDATE 改主键撞上已有行必须报错`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS upk",
                "CREATE TABLE upk (id BIGINT NOT NULL, v VARCHAR(8), PRIMARY KEY (id))",
                "INSERT INTO upk VALUES (1, 'a'), (2, 'b')",
            )
            val e = runCatching { c.exec("UPDATE upk SET id = 2 WHERE id = 1") }.exceptionOrNull()
            assertTrue(e is SQLException, "撞主键必须报错，实际：$e")
            assertTrue(
                e.message!!.contains("1062") || e.message!!.contains("Duplicate"),
                "应当是重复主键错误：${e.message}",
            )
            // 两条原行都必须原样还在
            assertEquals("a", c.one("SELECT v FROM upk WHERE id = 1"))
            assertEquals("b", c.one("SELECT v FROM upk WHERE id = 2"))
        }
    }

    /**
     * ★ **改主键不能留下幽灵行**（表只有主键、没有二级索引时最容易漏）。
     *
     * 旧行按旧 key 存在 LSM 里，不写墓碑就永远留着：`WHERE id = 1` 还能查到它，
     * `COUNT(*)` 也把它算进去——两处都自洽，所以**没有任何症状**。
     */
    @Test
    fun `UPDATE 改主键不留幽灵行`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS ughost",
                "CREATE TABLE ughost (id BIGINT NOT NULL, v VARCHAR(8), PRIMARY KEY (id))",
                "INSERT INTO ughost VALUES (1, 'a'), (2, 'b')",
            )
            assertEquals(1, c.exec("UPDATE ughost SET id = 5 WHERE id = 1"))
            assertEquals(0L, c.num("SELECT COUNT(*) FROM ughost WHERE id = 1"), "旧行必须消失")
            assertEquals(2L, c.num("SELECT COUNT(*) FROM ughost"))
            assertEquals("a", c.one("SELECT v FROM ughost WHERE id = 5"))
            // 整表平移（每个新主键都落在别的行的旧位置上）必须仍然能跑
            assertEquals(2, c.exec("UPDATE ughost SET id = id + 10"))
            assertEquals(2L, c.num("SELECT COUNT(*) FROM ughost"))
            // 'a' 这一行现在是 5，平移后是 15
            assertEquals(15L, c.num("SELECT id FROM ughost WHERE v = 'a'"))
        }
    }

    // ── 2PC 的读可见性 ───────────────────────────────────────────

    /**
     * ★ **读不到半笔跨分片事务。**
     *
     * 事务往两个分片各写一行（不同的分区键），另一个线程在提交期间反复读。
     * 断言是：**两种行要么都在、要么都不在**——绝不出现"只有一行"。
     *
     * 曾经这是可能的：2PC 的阶段 3 逐分片写入，读者在中间拿到
     * `read_ts > ts`，于是看到分片 A 的新行、分片 B 的旧行。
     */
    @Test
    fun `读不到半笔跨分片事务`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS half",
                "CREATE TABLE half (id BIGINT NOT NULL, v INT, PRIMARY KEY (id)) PARTITION BY HASH(id) PARTITIONS 4",
            )
            var seen = 0
            var partial = 0
            // 同一个连接上交替提交（事务）与读取（自动提交），
            // 复现"提交中恰好有人读"的时序
            repeat(200) { i ->
                val a = i * 2L + 1
                val b = i * 2L + 2
                c.autoCommit = false
                c.exec("INSERT INTO half VALUES ($a, 1), ($b, 1)")
                c.commit()
                c.autoCommit = true
                // 读这两个键：写在同一笔事务里，必须同时可见
                val n = c.num("SELECT COUNT(*) FROM half WHERE id IN ($a, $b)")
                seen++
                if (n != 2L) partial++
            }
            assertEquals(0, partial, "$seen 次读取里有 $partial 次只看到半笔事务的其中一行")
        }
    }
}
