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
 * **写入路径的兼容性与回归**：upsert 三兄弟、`INSERT ... SELECT`、
 * 以及四个"静默毁数据"的缺陷的回归测试。
 *
 * ## 这个文件里每一条都对应一个真实缺陷或真实缺口
 *
 * | 用例 | 之前的行为 |
 * |---|---|
 * | `DROP INDEX i ON t` | **把整张表删了**（Parser 产出 DropTableStmt，执行器照做） |
 * | `DROP TABLE a, b` | 只删第一张，返回成功 |
 * | `A UNION B UNION ALL C` | 三条边共用最后一个 `ALL`——行数不对且不报错 |
 * | DECIMAL 列写入 | 值按浮点编码写、按定点解码 → **整行错位/读出垃圾** |
 * | BLOB 列比较 | 存成字符串，`WHERE blob = 'x'` 永远匹配不到 |
 * | `REPLACE INTO` | 被当普通 INSERT（撞键就报 1062） |
 * | `INSERT IGNORE` | 同上 |
 * | `INSERT ... SELECT` | 1064 语法错 |
 *
 * 前四条是**静默**的：语句返回成功，数据不对。所以它们必须有回归测试——
 * 没有测试的话，下一次重构会把它们原样带回来。
 */
class WritePathCompatibilityTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-writepath")
        server = YangBaseServer(
            ServerConfig(
                host = "127.0.0.1", port = 0, nodeId = "wp",
                dataDir = dataDir.toString(), allowNonDurableStorage = true,
            )
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
        "jdbc:mysql://127.0.0.1:$port/wdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun <T> Connection.q(b: (Statement) -> T): T = createStatement().use(b)

    private fun Connection.setup(vararg sql: String) = q { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS wdb")
        s.execute("USE wdb")
        sql.forEach { s.execute(it) }
    }

    private fun Connection.one(sql: String): Any? = q { s ->
        s.executeQuery(sql).use { rs -> if (rs.next()) rs.getObject(1) else null }
    }

    /**
     * 取一个数。
     *
     * ⚠️ 必须经 `Number` 归一：驱动对 `COUNT(*)` 返回 `Long`、对 `INT` 列返回
     * `Integer`、对 `DECIMAL` 返回 `BigDecimal`。测试里写 `assertEquals(2L, ...)`
     * 会在"值对、类型不对"上失败——那是测试自己的问题，不是被测代码的。
     */
    private fun Connection.num(sql: String): Long = (one(sql) as Number).toLong()

    /** 取一个值的字符串形式（比对显示口径时用，避免类型差异干扰）。 */
    private fun Connection.text(sql: String): String? = one(sql)?.toString()

    private fun Connection.rows(sql: String): List<List<Any?>> = q { s ->
        s.executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            val out = ArrayList<List<Any?>>()
            while (rs.next()) out.add((1..n).map { rs.getObject(it) })
            out
        }
    }

    private fun Connection.exec(sql: String): Int = q { it.executeUpdate(sql) }

    // ── 一、静默毁数据的四个缺陷 ──────────────────────────────────

    @Test
    fun `DROP INDEX 只删索引 不删表`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS di",
                "CREATE TABLE di (id BIGINT NOT NULL, v INT, PRIMARY KEY (id), KEY idx_v (v))",
                "INSERT INTO di VALUES (1,10),(2,20)",
            )
            c.exec("DROP INDEX idx_v ON di")
            // ★ 表必须还在，数据也必须还在。曾经这条语句会 `catalog.dropTable`
            assertEquals(2L, c.num("SELECT COUNT(*) FROM di"), "DROP INDEX 不能把表删掉")
            // 索引确实没了
            assertEquals(
                0L,
                c.num("SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_NAME = 'di' AND INDEX_NAME = 'idx_v'"),
                "索引本身应当被删掉",
            )
            // 删掉索引之后仍然能读
            assertEquals(20L, c.num("SELECT v FROM di WHERE id = 2"))
        }
    }

    @Test
    fun `DROP TABLE 多表要全删`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS d1", "DROP TABLE IF EXISTS d2", "DROP TABLE IF EXISTS d3",
                "CREATE TABLE d1 (id BIGINT, PRIMARY KEY (id))",
                "CREATE TABLE d2 (id BIGINT, PRIMARY KEY (id))",
                "CREATE TABLE d3 (id BIGINT, PRIMARY KEY (id))",
            )
            c.exec("DROP TABLE d1, d2, d3")
            // 曾经只有第一个被删，其余两张**静默留了下来**
            assertEquals(
                0L,
                c.num("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='wdb' AND TABLE_NAME IN ('d1','d2','d3')"),
                "三个表都该被删掉",
            )
        }
    }

    @Test
    fun `UNION 混合 ALL 时每条边各管各的`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS ua",
                "CREATE TABLE ua (v INT, PRIMARY KEY (v))",
                "INSERT INTO ua VALUES (1)",
                "DROP TABLE IF EXISTS ub",
                "CREATE TABLE ub (v INT, PRIMARY KEY (v))",
                "INSERT INTO ub VALUES (1)",
                "DROP TABLE IF EXISTS uc",
                "CREATE TABLE uc (v INT, PRIMARY KEY (v))",
                "INSERT INTO uc VALUES (1)",
            )
            // A UNION ALL B UNION C：第一条边保留重复、第二条边去重。
            // 曾经两条边共用最后一个 ALL → 全去重或全保留，行数都不对
            // 判别用例：`A UNION B UNION ALL C`
            //
            // 正确的结合方式是 ((A DISTINCT B) ALL C)：A、B 去重后剩 1 行，
            // 再 ALL 上 C 的 1 行 → **2 行**。
            // 曾经三条边共用最后一个 ALL（这里是 true）→ 全部 ALL → 3 行；
            // 共用 false → 全部 DISTINCT → 1 行。两种都不报错，只是行数不对。
            assertEquals(
                2, c.rows("SELECT v FROM ua UNION SELECT v FROM ub UNION ALL SELECT v FROM uc").size,
                "A UNION B UNION ALL C 应当是 2 行",
            )
            // 反向：`A UNION ALL B UNION C` → ((A ALL B) DISTINCT C) → 去重后 1 行
            assertEquals(
                1, c.rows("SELECT v FROM ua UNION ALL SELECT v FROM ub UNION SELECT v FROM uc").size,
                "A UNION ALL B UNION C 应当是 1 行",
            )
            // 全 ALL 保留三行
            assertEquals(
                3, c.rows("SELECT v FROM ua UNION ALL SELECT v FROM ub UNION ALL SELECT v FROM uc").size,
                "全 ALL 应当是 3 行",
            )
        }
    }
    @Test
    fun `DECIMAL 列必须写入与读出同一个数`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS money",
                "CREATE TABLE money (id BIGINT NOT NULL, amount DECIMAL(12,2), note VARCHAR(16), PRIMARY KEY (id))",
            )
            c.exec("INSERT INTO money VALUES (1, 12.34, 'a'), (2, '99.90', 'b'), (3, 100, 'c')")
            // 曾经：F64 按 8 字节 IEEE754 写、按定点解码 → 读出的是垃圾，
            // 而且只消耗 2 字节，**这一行后面所有列全部错位**（note 会读到别处）
            assertEquals("12.34", c.one("SELECT amount FROM money WHERE id = 1").toString())
            assertEquals("99.90", c.one("SELECT amount FROM money WHERE id = 2").toString())
            assertEquals("100.00", c.one("SELECT amount FROM money WHERE id = 3").toString())
            // 同一行后面的列也必须完好（错位的话这里会失败）
            assertEquals("a", c.one("SELECT note FROM money WHERE id = 1"))
            assertEquals("c", c.one("SELECT note FROM money WHERE id = 3"))
            // 标度按列声明统一：1.005 → 1.01（四舍五入），而不是存成 1.005
            c.exec("INSERT INTO money VALUES (4, 1.005, 'd')")
            assertEquals("1.01", c.one("SELECT amount FROM money WHERE id = 4").toString())
            // 聚合仍然正确
            assertEquals("213.25", c.one("SELECT SUM(amount) FROM money").toString())
        }
    }

    @Test
    fun `BLOB 列写入后可以按值查到`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS blobs",
                "CREATE TABLE blobs (id BIGINT NOT NULL, b BLOB, PRIMARY KEY (id))",
            )
            c.exec("INSERT INTO blobs VALUES (1, 'abc')")
            // 曾经 BLOB 列存成 Str、比较时 Str 与 Bin 走"异类"分支 → 永不相等
            assertEquals(1L, c.num("SELECT COUNT(*) FROM blobs WHERE b = 'abc'"), "字符串字面量必须能查到 BLOB 值")
            // 驱动对 BLOB 列可能给 byte[] 也可能给 String（取决于列类型声明），两种都接受
            val raw = c.one("SELECT b FROM blobs WHERE id = 1")
            assertEquals("abc", when (raw) {
                is ByteArray -> String(raw)
                else -> raw.toString()
            })
        }
    }

    @Test
    fun `列宽与取值范围按 MySQL 严格模式校验`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS w",
                "CREATE TABLE w (id BIGINT NOT NULL, s VARCHAR(5), t TINYINT, i INT, PRIMARY KEY (id))",
            )
            // 超长必须报 1406，而不是静默截断（MySQL 5.7 起的严格模式行为）
            val tooLong = assertFailsWith<SQLException> { c.exec("INSERT INTO w VALUES (1, 'abcdefgh', 1, 1)") }
            assertEquals(1406, tooLong.errorCode, "超长要报 1406：${tooLong.message}")
            // 宽度按**字符数**算，不是字节数：5 个汉字是 5 个字符，必须能存
            c.exec("INSERT INTO w VALUES (1, '数据库系统', 1, 1)")
            // 整数越界必须报 1264，而不是回绕
            val overflow = assertFailsWith<SQLException> { c.exec("INSERT INTO w VALUES (2, 'ok', 999, 1)") }
            assertEquals(1264, overflow.errorCode, "TINYINT 越界要报 1264：${overflow.message}")
            assertFailsWith<SQLException> { c.exec("INSERT INTO w VALUES (3, 'ok', -200, 1)") }
            // 边界值本身合法
            c.exec("INSERT INTO w VALUES (4, 'ok', 127, 2147483647)")
            c.exec("INSERT INTO w VALUES (5, 'ok', -128, -2147483648)")
            assertEquals(3L, c.num("SELECT COUNT(*) FROM w"))
            // ⚠️ 读路径不校验：哪怕存量数据超长，也必须读得出来
            assertEquals("数据库系统", c.one("SELECT s FROM w WHERE id = 1"))
        }
    }

    // ── 二、upsert 三兄弟 ────────────────────────────────────────

    @Test
    fun `INSERT ON DUPLICATE KEY UPDATE 插入与更新两条路`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS ups",
                "CREATE TABLE ups (id BIGINT NOT NULL, n INT, seen INT, PRIMARY KEY (id))",
            )
            // 插入路径
            assertEquals(1, c.exec("INSERT INTO ups VALUES (1, 10, 1) ON DUPLICATE KEY UPDATE n = VALUES(n), seen = seen + 1"))
            assertEquals(10L, c.num("SELECT n FROM ups WHERE id = 1"))
            // 更新路径：VALUES(n) 必须是**新行**的值（20），seen 自增
            assertEquals(2, c.exec("INSERT INTO ups VALUES (1, 20, 1) ON DUPLICATE KEY UPDATE n = VALUES(n), seen = seen + 1"))
            assertEquals(20L, c.num("SELECT n FROM ups WHERE id = 1"), "VALUES(n) 必须取新值；取旧值的话这里还是 10")
            assertEquals(2L, c.num("SELECT seen FROM ups WHERE id = 1"))
            // 没有 VALUES() 的表达式按**旧行**求值
            c.exec("INSERT INTO ups VALUES (1, 30, 9) ON DUPLICATE KEY UPDATE n = n + 100")
            assertEquals(120L, c.num("SELECT n FROM ups WHERE id = 1"))
        }
    }

    @Test
    fun `ON DUPLICATE KEY UPDATE 在唯一键上也触发`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS upu",
                "CREATE TABLE upu (id BIGINT NOT NULL, email VARCHAR(32), n INT, PRIMARY KEY (id), UNIQUE KEY uq_email (email))",
                "INSERT INTO upu VALUES (1, 'a@x.com', 1)",
            )
            // 主键不同、唯一键相同 → 也是"重复"，必须走 UPDATE 而不是报 1062
            c.exec("INSERT INTO upu VALUES (2, 'a@x.com', 5) ON DUPLICATE KEY UPDATE n = VALUES(n)")
            assertEquals(1L, c.num("SELECT COUNT(*) FROM upu"))
            assertEquals(5L, c.num("SELECT n FROM upu WHERE email = 'a@x.com'"))
        }
    }

    @Test
    fun `REPLACE INTO 先删后插`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS rep",
                "CREATE TABLE rep (id BIGINT NOT NULL, v VARCHAR(8), PRIMARY KEY (id))",
                "INSERT INTO rep VALUES (1, 'old'), (2, 'keep')",
            )
            // 主键冲突：整行被替换（旧值 old 不能留下）
            assertEquals(2, c.exec("REPLACE INTO rep VALUES (1, 'new')"), "替换一行影响 2 行（删 + 插）")
            assertEquals("new", c.one("SELECT v FROM rep WHERE id = 1"))
            assertEquals(2L, c.num("SELECT COUNT(*) FROM rep"))
            // 没有冲突：普通插入，只影响 1 行
            assertEquals(1, c.exec("REPLACE INTO rep VALUES (3, 'fresh')"))
            assertEquals(3L, c.num("SELECT COUNT(*) FROM rep"))
            assertEquals("keep", c.one("SELECT v FROM rep WHERE id = 2"))
        }
    }

    @Test
    fun `REPLACE INTO 在唯一键上替换掉冲突行`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS repu",
                "CREATE TABLE repu (id BIGINT NOT NULL, email VARCHAR(32), PRIMARY KEY (id), UNIQUE KEY uq (email))",
                "INSERT INTO repu VALUES (1, 'x@y.com')",
            )
            // 新行的 id=2 与旧行不同，但 email 相同 → 旧行必须被删掉（MySQL 语义）
            c.exec("REPLACE INTO repu VALUES (2, 'x@y.com')")
            assertEquals(1L, c.num("SELECT COUNT(*) FROM repu"))
            assertEquals(2L, c.num("SELECT id FROM repu WHERE email = 'x@y.com'"))
        }
    }

    @Test
    fun `INSERT IGNORE 跳过重复行 其余照插`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS ign",
                "CREATE TABLE ign (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))",
                "INSERT INTO ign VALUES (1, 1)",
            )
            // 撞键的那一行跳过，新的那一行照插
            c.exec("INSERT IGNORE INTO ign VALUES (1, 99), (2, 2)")
            assertEquals(2L, c.num("SELECT COUNT(*) FROM ign"))
            assertEquals(1L, c.num("SELECT v FROM ign WHERE id = 1"), "已存在的行不能被改动")
            assertEquals(2L, c.num("SELECT v FROM ign WHERE id = 2"))
            // 普通 INSERT 仍然报 1062（IGNORE 不能污染默认行为）
            val e = assertFailsWith<SQLException> { c.exec("INSERT INTO ign VALUES (1, 5)") }
            assertEquals(1062, e.errorCode)
        }
    }

    @Test
    fun `upsert 在事务里也看得到自己的写`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS uptx",
                "CREATE TABLE uptx (id BIGINT NOT NULL, n INT, PRIMARY KEY (id))",
            )
            c.autoCommit = false
            // 同一个事务里先插后 upsert：第二次必须走 UPDATE 而不是报 1062，
            // 也不是插出第二行（那说明它没看见自己的写）
            c.exec("INSERT INTO uptx VALUES (1, 1)")
            c.exec("INSERT INTO uptx VALUES (1, 2) ON DUPLICATE KEY UPDATE n = VALUES(n)")
            assertEquals(2L, c.num("SELECT n FROM uptx WHERE id = 1"))
            assertEquals(1L, c.num("SELECT COUNT(*) FROM uptx"))
            c.commit()
            c.autoCommit = true
            assertEquals(1L, c.num("SELECT COUNT(*) FROM uptx"))
            assertEquals(2L, c.num("SELECT n FROM uptx WHERE id = 1"))
        }
    }

    // ── 三、INSERT ... SELECT ────────────────────────────────────

    @Test
    fun `INSERT SELECT 复制数据并按列对齐`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS src", "DROP TABLE IF EXISTS dst",
                "CREATE TABLE src (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))",
                "CREATE TABLE dst (id BIGINT NOT NULL, v INT, tag VARCHAR(8), PRIMARY KEY (id))",
                "INSERT INTO src VALUES (1,10),(2,20),(3,30)",
            )
            // 指定列清单：未列出的列取默认值（这里是 NULL）
            assertEquals(2, c.exec("INSERT INTO dst (id, v, tag) SELECT id, v * 2, 'copy' FROM src WHERE v >= 20"))
            assertEquals(2L, c.num("SELECT COUNT(*) FROM dst"))
            assertEquals(40L, c.num("SELECT v FROM dst WHERE id = 2"))
            assertEquals("copy", c.one("SELECT tag FROM dst WHERE id = 3"))
            // 不写列清单时按位置对齐
            c.exec("INSERT INTO dst SELECT id + 100, v, 'again' FROM src")
            assertEquals(5L, c.num("SELECT COUNT(*) FROM dst"))
            // 列数对不上必须报错，而不是静默错位
            val e = assertFailsWith<SQLException> { c.exec("INSERT INTO dst SELECT id, v FROM src") }
            assertTrue(e.message!!.contains("count") || e.errorCode == 1222, "列数不符要报错：${e.message}")
        }
    }

    @Test
    fun `INSERT SELECT 可以配合 UNION 与聚合`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS s2", "DROP TABLE IF EXISTS agg",
                "CREATE TABLE s2 (id BIGINT NOT NULL, city VARCHAR(8), amount INT, PRIMARY KEY (id))",
                "CREATE TABLE agg (city VARCHAR(8), total BIGINT, PRIMARY KEY (city))",
                "INSERT INTO s2 VALUES (1,'bj',10),(2,'bj',20),(3,'sh',5)",
            )
            c.exec("INSERT INTO agg SELECT city, SUM(amount) FROM s2 GROUP BY city")
            assertEquals(2L, c.num("SELECT COUNT(*) FROM agg"))
            assertEquals(30L, c.num("SELECT total FROM agg WHERE city = 'bj'"))
            assertEquals(5L, c.num("SELECT total FROM agg WHERE city = 'sh'"))
        }
    }

    // ── 四、函数库接进真实查询路径 ────────────────────────────────

    @Test
    fun `CASE 与标量函数在真实查询里可用`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS fn", "DROP TABLE IF EXISTS fn2",
                "CREATE TABLE fn (id BIGINT NOT NULL, name VARCHAR(16), amount DECIMAL(10,2), dt DATETIME, PRIMARY KEY (id))",
                "INSERT INTO fn VALUES (1,'alice',10.50,'2026-02-01 10:00:00'),(2,'bob',-3.25,'2026-03-05 08:30:00')",
            )
            val rows = c.rows(
                "SELECT id, UPPER(name), ABS(amount), DATE_FORMAT(dt, '%Y/%m'), " +
                    "CASE WHEN amount > 0 THEN 'in' ELSE 'out' END FROM fn ORDER BY id",
            ).map { row -> row.map { it.toString() } }
            assertEquals(listOf("1", "ALICE", "10.50", "2026/02", "in"), rows[0])
            assertEquals(listOf("2", "BOB", "3.25", "2026/03", "out"), rows[1])
            // 函数用在 WHERE 与 GROUP BY 里
            assertEquals(1L, c.num("SELECT COUNT(*) FROM fn WHERE LENGTH(name) = 3"))
            assertEquals(2L, c.num("SELECT COUNT(DISTINCT CASE WHEN amount > 0 THEN 'p' ELSE 'n' END) FROM fn"))
            // 建表 + 查表（走一遍真实的解析-执行路径）
            c.setup("DROP TABLE IF EXISTS fn2", "CREATE TABLE fn2 (id BIGINT NOT NULL, s VARCHAR(32), PRIMARY KEY (id))")
            c.exec("INSERT INTO fn2 VALUES (1, '  12.7  ')")
            assertEquals("12.70", c.one("SELECT CAST(TRIM(s) AS DECIMAL(10,2)) FROM fn2").toString())
        }
    }

    @Test
    fun `NOW 在同一条语句里是常量`() {
        connect().use { c ->
            c.setup(
                "DROP TABLE IF EXISTS nw",
                "CREATE TABLE nw (id BIGINT NOT NULL, PRIMARY KEY (id))",
            )
            c.exec("INSERT INTO nw VALUES (1),(2),(3),(4),(5)")
            // 语句级常量：五行必须看到同一个时刻（曾经逐行读时钟）
            assertEquals(1L, c.num("SELECT COUNT(DISTINCT NOW()) FROM nw WHERE id <= 5"))
        }
    }
}
