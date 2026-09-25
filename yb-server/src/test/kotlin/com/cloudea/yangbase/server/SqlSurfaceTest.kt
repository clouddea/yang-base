package com.cloudea.yangbase.server

import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * SQL 能力面的端到端测试：**聚合 / GROUP BY / HAVING / JOIN / UNION / 子查询**。
 *
 * 这些能力各自都有一组纯逻辑单元测试（`AggregateTest` / `JoinTest`），
 * 这里要回答的是另一个问题：**接进真正的执行路径之后还对不对**。
 * 差别不是形式主义——单元测试里 `Aggregates.compute` 是被直接调用的，
 * 而真实路径要经过「访问路径选择 → 取数 → 分组 → HAVING → 排序 → 投影 → LIMIT」
 * 这一串，任何一环把顺序搞错（比如把 HAVING 当 WHERE 用、把外连接当内连接算）
 * 都只有在这一层才看得出来。
 *
 * 断言里刻意混入**与外连接/三值逻辑**相关的用例：它们是"结果少几行/多几行"
 * 的高发区，而且都不会报错。
 */

class SqlSurfaceTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var server: YangBaseServer
    private var port: Int = 0


    /**
     * ★ **无 FROM 的查询也要支持全部子句**（2026-09-25 实测后补）。
     *
     * 这不是"少支持一个语法"，而是**标准 `mysql` 命令行客户端连不上**：
     * 它每次建立连接都会发 `select @@version_comment limit 1`，而我们的解析器
     * 在"无 FROM"时提前 return、把后面的子句全丢掉并报 1064；客户端收到语法错误后
     * 丢掉连接 → 用户看到的是 `ERROR 2013 Lost connection`，与真正的原因隔了十万八千里。
     *
     * 同样重要的是**子句不能被静默忽略**：曾经的实现自己拼一行就返回，
     * `LIMIT` / `WHERE` / `GROUP BY` / `HAVING` 全部被丢——结果"看起来完全正常"。
     * 下面每个期望值都对齐真实 MySQL 8.0 的实测输出。
     */
    @Test
    fun `无 FROM 的查询支持 LIMIT 与 WHERE`() {
        connect().use { c ->
            fun value(sql: String) = one(c, sql)
            fun count(sql: String) = rows(c, sql).size

            // `SELECT 1 LIMIT 1` —— mysql CLI 的连接握手就靠这一形态
            assertEquals(1L, (value("SELECT 1 LIMIT 1") as Number).toLong())
            // LIMIT 0 必须返回 0 行（客户端用它探测查询是否存在）
            assertEquals(0, count("SELECT 1 LIMIT 0"))
            assertEquals(0, count("SELECT @@version_comment LIMIT 0"))
            // OFFSET 越界 → 0 行
            assertEquals(0, count("SELECT 1 LIMIT 1 OFFSET 5"))
            assertEquals(0, count("SELECT 1 LIMIT 1,1"))
            // WHERE 决定这一行在不在（MySQL：SELECT 1 WHERE 0 → 0 行）
            assertEquals(1, count("SELECT 1 WHERE 1=1"))
            assertEquals(0, count("SELECT 1 WHERE 0"))
            // 聚合：无 FROM 等价于"只有一行的虚拟表"
            assertEquals(1L, (value("SELECT COUNT(*)") as Number).toLong())
            assertEquals(0L, (value("SELECT COUNT(*) WHERE 0") as Number).toLong())
        }
    }

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-sqlsurf")
        server = YangBaseServer(
            ServerConfig(
                host = "127.0.0.1", port = 0, nodeId = "sql-surf",
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
        "jdbc:mysql://127.0.0.1:$port/sdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun <T> Connection.q(b: (Statement) -> T): T = createStatement().use(b)

    /** 取第一列的一行。 */
    private fun one(c: Connection, sql: String): Any? = c.q { s ->
        s.executeQuery(sql).use { rs -> if (rs.next()) rs.getObject(1) else null }
    }

    private fun rows(c: Connection, sql: String): List<List<Any?>> = c.q { s ->
        s.executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            val out = ArrayList<List<Any?>>()
            while (rs.next()) out.add((1..n).map { rs.getObject(it) })
            out
        }
    }

    /** 取一个布尔表达式的结果（Connector/J 对 TINYINT 返回数字，所以走 getBoolean）。 */
    private fun bool(c: Connection, sql: String): Boolean = c.q { s ->
        s.executeQuery(sql).use { rs -> rs.next(); rs.getBoolean(1) }
    }

    private fun names(c: Connection, sql: String): List<String> = c.q { s ->
        s.executeQuery(sql).use { rs ->
            (1..rs.metaData.columnCount).map { rs.metaData.getColumnLabel(it) }
        }
    }

    private fun setup(c: Connection) = c.q { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS sdb")
        s.execute("USE sdb")
        s.execute("DROP TABLE IF EXISTS orders")
        s.execute("DROP TABLE IF EXISTS users")
        s.execute(
            "CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR(32), city VARCHAR(32), PRIMARY KEY (id))"
        )
        s.execute(
            "CREATE TABLE orders (id BIGINT NOT NULL, user_id BIGINT, amount INT, PRIMARY KEY (id))"
        )
        s.execute("INSERT INTO users VALUES (10,'alice','bj'),(11,'bob','bj'),(12,'carol','sh')")
        // 订单 3 的 user_id 是 NULL；订单 4 指向不存在的用户 99
        s.execute("INSERT INTO orders VALUES (1,10,100),(2,11,200),(3,NULL,300),(4,99,400)")
    }

    private fun withDb(b: (Connection) -> Unit) = connect().use { c ->
        c.q { s -> s.execute("USE sdb") }
        b(c)
    }

    // ── 聚合与分组 ────────────────────────────────────────────────

    @Test
    fun `全局聚合 - 空表也返回一行`() {
        connect().use { c ->
            setup(c)
            assertEquals(4L, one(c, "SELECT COUNT(*) FROM orders"))
            assertEquals(1000L, one(c, "SELECT SUM(amount) FROM orders"), "100+200+300+400")
            assertEquals(250.0, one(c, "SELECT AVG(amount) FROM orders"))

            c.q { s -> s.execute("DROP TABLE orders") }
            c.q { s -> s.execute("CREATE TABLE orders (id BIGINT NOT NULL, user_id BIGINT, amount INT, PRIMARY KEY (id))") }
            // 空表上的聚合必须返回**一行**：COUNT 给 0、SUM 给 NULL
            assertEquals(0L, one(c, "SELECT COUNT(*) FROM orders"))
            assertEquals(null, one(c, "SELECT SUM(amount) FROM orders"))
        }
    }

    /** ★ `SUM`/`AVG` 必须忽略 NULL，而不是把它当 0。 */
    @Test
    fun `聚合忽略 NULL`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("INSERT INTO orders VALUES (5,10,NULL)") }
            // 4 行有金额（100,200,300,400），NULL 那行不参与
            assertEquals(1000L, one(c, "SELECT SUM(amount) FROM orders"))
            assertEquals(
                250.0, one(c, "SELECT AVG(amount) FROM orders"),
                "把 NULL 当 0 会得到 200——一个安静的错答案",
            )
            assertEquals(5L, one(c, "SELECT COUNT(*) FROM orders"), "COUNT(*) 数行数，含 NULL")
            assertEquals(4L, one(c, "SELECT COUNT(amount) FROM orders"), "COUNT(列) 只数非空")
        }
    }

    @Test
    fun `GROUP BY 与 HAVING`() {
        connect().use { c ->
            setup(c)
            val g = rows(c, "SELECT city, COUNT(*) FROM users GROUP BY city ORDER BY city")
            assertEquals(listOf(listOf("bj", 2L), listOf("sh", 1L)), g)

            // HAVING 作用于**分组之后**：分组数 1 的那组被滤掉
            val h = rows(c, "SELECT city, COUNT(*) AS n FROM users GROUP BY city HAVING COUNT(*) > 1")
            assertEquals(listOf(listOf("bj", 2L)), h)
        }
    }

    @Test
    fun `HAVING 可以引用聚合别名`() {
        connect().use { c ->
            setup(c)
            assertEquals(
                listOf(listOf("bj", 2L)),
                rows(c, "SELECT city, COUNT(*) AS n FROM users GROUP BY city HAVING n > 1"),
            )
        }
    }

    @Test
    fun `ORDER BY 可以引用聚合与别名`() {
        connect().use { c ->
            setup(c)
            // bj = 10+11 = 21，sh = 12
            val byAgg = rows(c, "SELECT city, SUM(id) FROM users GROUP BY city ORDER BY SUM(id) DESC")
            assertEquals("bj", byAgg[0][0], "bj 的 id 之和 21 最大")
            val byAlias = rows(c, "SELECT city, SUM(id) AS s FROM users GROUP BY city ORDER BY s DESC")
            assertEquals("bj", byAlias[0][0], "ORDER BY 引用别名")
            val asc = rows(c, "SELECT city, SUM(id) AS s FROM users GROUP BY city ORDER BY s ASC")
            assertEquals("sh", asc[0][0])
        }
    }

    @Test
    fun `GROUP BY 上的列可以自由选出`() {
        connect().use { c ->
            setup(c)
            assertEquals(2, rows(c, "SELECT city FROM users GROUP BY city").size)
        }
    }

    /** ★ ONLY_FULL_GROUP_BY：非分组列必须报错，而不是"每组随便挑一行"。 */
    @Test
    fun `选非分组列报 1055`() {
        connect().use { c ->
            setup(c)
            val e = assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT city, name FROM users GROUP BY city") }
            }
            assertTrue(e.message!!.contains("ONLY_FULL_GROUP_BY"), "应当指出是哪条规则：${e.message}")
        }
    }

    @Test
    fun `WHERE 里的聚合报错`() {
        connect().use { c ->
            setup(c)
            assertFailsWith<SQLException> { c.q { s -> s.execute("SELECT id FROM orders WHERE SUM(amount) > 1") } }
        }
    }

    // ── GROUP_CONCAT 与 DISTINCT ──────────────────────────────────

    @Test
    fun `GROUP_CONCAT 与 DISTINCT`() {
        connect().use { c ->
            setup(c)
            assertEquals("bj,sh", one(c, "SELECT GROUP_CONCAT(DISTINCT city) FROM users"))
            assertEquals(2L, one(c, "SELECT COUNT(DISTINCT city) FROM users"))
        }
    }

    /**
     * ★ `SELECT DISTINCT` 必须真的去重。
     *
     * 曾经它被解析出来之后**丢掉**（"暂不区分"），于是查询照常返回，
     * 只是每一行重复的都还在——没有报错、没有警告，调用方拿到的行数偏多。
     * 这类缺陷在分页与对账里会直接表现为错误的数字。
     */
    @Test
    fun `SELECT DISTINCT 必须真的去重`() {
        connect().use { c ->
            setup(c)
            assertEquals(listOf("bj", "sh"), rows(c, "SELECT DISTINCT city FROM users").map { it[0] })
            // 分组路径也要去重
            assertEquals(
                listOf(listOf("bj", 2L), listOf("sh", 1L)),
                rows(c, "SELECT DISTINCT city, COUNT(*) FROM users GROUP BY city ORDER BY city"),
            )
            // 去重发生在 LIMIT 之前，否则 `DISTINCT ... LIMIT 1` 会先截断再丢重复
            assertEquals(1, rows(c, "SELECT DISTINCT city FROM users LIMIT 1").size)
        }
    }

    /** `DISTINCT` 下按不在结果里的列排序是错的（MySQL 报 3065），不能静默按某一行的值排。 */
    @Test
    fun `DISTINCT 下 ORDER BY 非投影列报错`() {
        connect().use { c ->
            setup(c)
            val e = assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT DISTINCT city FROM users ORDER BY name") }
            }
            assertTrue(e.message!!.contains("3065") || e.message!!.contains("DISTINCT"), e.message!!)
        }
    }

    // ── 连接 ──────────────────────────────────────────────────────

    @Test
    fun `内连接`() {
        connect().use { c ->
            setup(c)
            val r = rows(
                c,
                "SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id ORDER BY o.id",
            )
            assertEquals(listOf(listOf(1L, "alice"), listOf(2L, "bob")), r)
        }
    }

    /** ★ 外连接必须保留未匹配的行（漏了就是静默少行）。 */
    @Test
    fun `左连接保留未匹配的行`() {
        connect().use { c ->
            setup(c)
            val r = rows(
                c,
                "SELECT o.id, u.name FROM orders o LEFT JOIN users u ON o.user_id = u.id ORDER BY o.id",
            )
            assertEquals(4, r.size, "左表四行都要在")
            assertEquals(listOf(1L, "alice"), r[0])
            assertEquals(listOf(2L, "bob"), r[1])
            assertEquals(listOf(3L, null), r[2], "user_id 为 NULL 的订单匹配不到用户")
            assertEquals(listOf(4L, null), r[3], "用户 99 不存在")
        }
    }

    @Test
    fun `右连接保留未匹配的右行`() {
        connect().use { c ->
            setup(c)
            val r = rows(
                c,
                "SELECT u.name, o.id FROM orders o RIGHT JOIN users u ON o.user_id = u.id ORDER BY u.name",
            )
            assertEquals(3, r.size, "carol 没有订单也要出现")
            assertEquals(listOf("carol", null), r.first { it[0] == "carol" })
        }
    }

    @Test
    fun `逗号连接是笛卡尔积`() {
        connect().use { c ->
            setup(c)
            assertEquals(12L, one(c, "SELECT COUNT(*) FROM orders, users"))
        }
    }

    @Test
    fun `连接后仍然可以分组`() {
        connect().use { c ->
            setup(c)
            val r = rows(
                c,
                "SELECT u.city, COUNT(*) AS n FROM orders o JOIN users u ON o.user_id = u.id " +
                    "GROUP BY u.city ORDER BY u.city",
            )
            assertEquals(listOf(listOf("bj", 2L)), r, "只有两个订单能匹配上用户，且都在 bj")
        }
    }

    /** ★ 歧义列必须报 1052，而不是随手取一边。 */
    @Test
    fun `连接里的歧义列报错`() {
        connect().use { c ->
            setup(c)
            val e = assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT id FROM orders o JOIN users u ON o.user_id = u.id") }
            }
            assertTrue(e.message!!.contains("ambiguous"), "应当说是歧义：${e.message}")
            // 限定写法正常（两张表各有 id，写清楚就行）
            assertEquals(
                listOf(listOf(1L, 10L), listOf(2L, 11L)),
                rows(c, "SELECT o.id, u.id FROM orders o JOIN users u ON o.user_id = u.id ORDER BY o.id"),
            )
        }
    }

    @Test
    fun `连接里未匹配的 NULL 不参与比较`() {
        connect().use { c ->
            setup(c)
            // 订单 3 的 user_id 是 NULL：`NULL < 任何值` 是未知，不该被匹配上
            val r = rows(
                c,
                "SELECT o.id FROM orders o JOIN users u ON o.user_id < u.id ORDER BY o.id",
            )
            assertTrue(r.none { it[0] == 3L }, "NULL 参与比较是未知，不能匹配：$r")
        }
    }

    @Test
    fun `SELECT 星号在连接上展开两边的列`() {
        connect().use { c ->
            setup(c)
            val n = names(c, "SELECT * FROM orders o JOIN users u ON o.user_id = u.id")
            assertEquals(listOf("id", "user_id", "amount", "id", "name", "city"), n)
        }
    }

    // ── UNION ─────────────────────────────────────────────────────

    @Test
    fun `UNION 去重 而 UNION ALL 保留重复`() {
        connect().use { c ->
            setup(c)
            val u = rows(c, "SELECT city FROM users WHERE city='bj' UNION SELECT city FROM users WHERE city='bj'")
            assertEquals(1, u.size, "UNION 去重（两侧各有 2 行 bj，去重后 1 行）")
            val ua = rows(
                c,
                "SELECT city FROM users WHERE city='bj' UNION ALL SELECT city FROM users WHERE city='bj'",
            )
            assertEquals(4, ua.size, "UNION ALL 保留重复行（2 + 2）")
            // 两侧都非空时也要看得到
            val both = rows(c, "SELECT city FROM users WHERE city='bj' UNION SELECT city FROM users WHERE city='sh'")
            assertEquals(2, both.size)
        }
    }

    // ── 子查询 ────────────────────────────────────────────────────

    @Test
    fun `标量子查询`() {
        connect().use { c ->
            setup(c)
            assertEquals(4L, one(c, "SELECT id FROM orders WHERE amount = (SELECT MAX(amount) FROM orders)"))
        }
    }

    @Test
    fun `IN 子查询`() {
        connect().use { c ->
            setup(c)
            val r = rows(c, "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY name")
            assertEquals(listOf(listOf("alice"), listOf("bob")), r)
        }
    }

    @Test
    fun `EXISTS 子查询`() {
        connect().use { c ->
            setup(c)
            assertEquals(3L, one(c, "SELECT COUNT(*) FROM users WHERE EXISTS (SELECT 1 FROM orders)"), "子查询非空，三个用户都成立")
            assertEquals(0L, one(c, "SELECT COUNT(*) FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE amount > 9999)"))
        }
    }

    /** ★ 子查询含 NULL 时 `NOT IN` 一行都不返回（SQL 的三值逻辑）。 */
    @Test
    fun `NOT IN 遇到 NULL 返回空集`() {
        connect().use { c ->
            setup(c)
            // 子查询里有 NULL（订单 3 的 user_id）
            assertEquals(
                0L,
                one(c, "SELECT COUNT(*) FROM users WHERE id NOT IN (SELECT user_id FROM orders)"),
                "`x NOT IN (...)` 里只要有一个 NULL，找不到 x 时结果是未知——必须一行都不返回。" +
                    "把未知当成假会返回一堆本该被排除的行",
            )
            // 去掉 NULL 之后就正常了
            assertEquals(
                1L,
                one(
                    c,
                    "SELECT COUNT(*) FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE user_id IS NOT NULL)",
                ),
            )
        }
    }

    /**
     * **标量位置**的相关子查询：`SELECT name, (SELECT … WHERE o.user_id = u.id) FROM users u`。
     *
     * 它曾经是"明确报错"的（要按关联键分组、按外层行的键去查，那一版没做）。
     * 现在做的是同一件事的另一半：内层按关联键**扫一次**建表，外层每行查表
     * （见 [com.cloudea.yangbase.sql.CorrelatedScalars]）。
     */
    @Test
    fun `标量位置的相关子查询按关联键给值`() {
        connect().use { c ->
            setup(c)
            assertEquals(
                listOf(listOf("alice", 1L), listOf("bob", 1L), listOf("carol", 0L)),
                rows(
                    c,
                    "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) FROM users u ORDER BY u.id",
                ),
            )
        }
    }

    // ── 三值逻辑（此前是错的，改动后必须钉住）──────────────────────

    /**
     * ★ `WHERE v < 10` **不能**返回 `v IS NULL` 的行。
     *
     * 改动前 `Datum.compareTo` 把 NULL 排在最前，于是 `NULL < 10` 被判成真，
     * 那些行被错误地返回给查询。单表查询里"多返回几行 NULL"很容易被当成正常数据，
     * 它是靠连接用例才露出来的。
     */
    @Test
    fun `与 NULL 的比较是未知 - 不多返回行`() {
        connect().use { c ->
            setup(c)
            // 恰好 3 行「非 NULL」满足 < 100；关键是**第 4 行（NULL）不能被算进来**
            assertEquals(3L, one(c, "SELECT COUNT(*) FROM orders WHERE user_id < 100"))
            // 这两条是判据：任何值都不满足，若 NULL 被当成"比任何数都小"就会多出来
            assertEquals(0L, one(c, "SELECT COUNT(*) FROM orders WHERE user_id > 99"))
            assertEquals(2L, one(c, "SELECT COUNT(*) FROM orders WHERE user_id <> 10"), "只有 11 与 99")
            assertEquals(3L, one(c, "SELECT COUNT(*) FROM orders WHERE user_id IS NOT NULL"))
            assertEquals(1L, one(c, "SELECT COUNT(*) FROM orders WHERE user_id IS NULL"))
        }
    }

    /** `<=>` 是空值安全等于，不能退化成 `=`。 */
    @Test
    fun `空值安全等于`() {
        connect().use { c ->
            setup(c)
            assertTrue(bool(c, "SELECT NULL <=> NULL"), "两个 NULL 空值安全等于为真")
            assertTrue(!bool(c, "SELECT 1 <=> NULL"), "1 与 NULL 空值安全等于为假")
            assertTrue(bool(c, "SELECT 1 <=> 1"))
            // 与 `=` 的差别正在这里：`=` 在 NULL 上给的是**未知**，不是真
            assertTrue(bool(c, "SELECT (NULL = NULL) IS NULL"), "`NULL = NULL` 的结果是 NULL（未知）")
        }
    }

    // ── EXPLAIN 要说实话 ──────────────────────────────────────────

    // ── 相关子查询（改写成半/反连接）────────────────────────────

    /**
     * ★ 相关 `EXISTS` 必须**每个外层行只输出一次**。
     *
     * 改写成连接时最容易犯的错是直接用内连接：一个用户有多个订单时，
     * 内连接会输出多行，而 `EXISTS` 只问"有没有"。那是一个不报错、
     * 只是行数翻倍的缺陷（而且 `COUNT(*)` 会给出一个看起来很合理的数字）。
     */
    @Test
    fun `相关 EXISTS 不产生重复行`() {
        connect().use { c ->
            setup(c)
            // alice(10) 有 1 个订单、bob(11) 有 1 个、carol(12) 没有
            val r = rows(
                c,
                "SELECT u.name FROM users u WHERE EXISTS " +
                    "(SELECT 1 FROM orders o WHERE o.user_id = u.id) ORDER BY u.name",
            )
            assertEquals(listOf(listOf("alice"), listOf("bob")), r, "每个外层行只输出一次")
        }
    }

    /** 一个外层行匹配多个内层行时，仍然只输出一次（这是半连接与内连接的分界）。 */
    @Test
    fun `外层行匹配多行时 EXISTS 也只输出一次`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("INSERT INTO orders VALUES (5,10,500),(6,10,600)") }
            val r = rows(
                c,
                "SELECT u.name FROM users u WHERE EXISTS " +
                    "(SELECT 1 FROM orders o WHERE o.user_id = u.id)",
            )
            assertEquals(listOf(listOf("alice"), listOf("bob")), r, "alice 有 3 个订单，也只是一行")
        }
    }

    @Test
    fun `相关 NOT EXISTS`() {
        connect().use { c ->
            setup(c)
            val r = rows(
                c,
                "SELECT u.name FROM users u WHERE NOT EXISTS " +
                    "(SELECT 1 FROM orders o WHERE o.user_id = u.id)",
            )
            assertEquals(listOf(listOf("carol")), r)
        }
    }

    @Test
    fun `相关 IN 与 NOT IN`() {
        connect().use { c ->
            setup(c)
            assertEquals(
                listOf(listOf("alice"), listOf("bob")),
                rows(c, "SELECT u.name FROM users u WHERE u.id IN (SELECT o.user_id FROM orders o WHERE o.user_id = u.id) ORDER BY u.name"),
            )
            assertEquals(
                listOf(listOf("carol")),
                rows(c, "SELECT u.name FROM users u WHERE u.id NOT IN (SELECT o.user_id FROM orders o WHERE o.user_id = u.id)"),
            )
        }
    }

    /**
     * ★ 相关 `NOT IN` 的 NULL 规则：**该外层行那一组**里出现 NULL 才变未知。
     *
     * 这条规则很容易被笼统地理解成"内表只要有 NULL 就一行不返回"——那**不对**：
     * 相关子查询的比较集合是**每一组自己的值**。关联列正好是比较列时，
     * NULL 那一行匹配不上任何外层行，于是它对每一组都没有影响
     * （`u.id NOT IN (SELECT o.user_id FROM orders o WHERE o.user_id = u.id)` 里
     * 那个 `user_id IS NULL` 的订单改变不了任何结果）。
     *
     * 只有 NULL 落在**某一组内部**时，那一组才变成"未知"。下面两条用例把这个区别钉住。
     */
    @Test
    fun `相关 NOT IN 的 NULL 只影响所在的那一组`() {
        connect().use { c ->
            setup(c)
            // 关联列 = 比较列：NULL 那行匹配不上任何外层行 → 对它所在组无影响
            c.q { s -> s.execute("INSERT INTO orders VALUES (7,NULL,700)") }
            assertEquals(
                listOf(listOf("carol")),
                rows(
                    c,
                    "SELECT u.name FROM users u WHERE u.id NOT IN " +
                        "(SELECT o.user_id FROM orders o WHERE o.user_id = u.id)",
                ),
                "carol 那一组是空集，`NOT IN ()` 为真——NULL 那行不属于任何组",
            )
        }
    }

    /** NULL 落在组**内部**（比较列不是关联列）时，那一组变成"未知"。 */
    @Test
    fun `相关 NOT IN 组内出现 NULL 则该组不可返回`() {
        connect().use { c ->
            setup(c)
            // 给 alice(10) 加一个 amount 为 NULL 的订单：她那一组的 amount 集合含 NULL
            c.q { s -> s.execute("INSERT INTO orders VALUES (8,10,NULL)") }
            val names = rows(
                c,
                "SELECT u.name FROM users u WHERE u.id NOT IN " +
                    "(SELECT o.amount FROM orders o WHERE o.user_id = u.id) ORDER BY u.name",
            ).map { it[0] }
            assertTrue(
                "alice" !in names,
                "alice 那一组的 amount 里有 NULL → 她那一组是未知，不该返回：$names",
            )
            // bob 那组只有 {200}，10 不在其中 → 返回
            assertTrue("bob" in names, "bob 那组没有 NULL，`11 NOT IN (200)` 为真：$names")
        }
    }

    @Test
    fun `相关子查询可以带内层自己的条件`() {
        connect().use { c ->
            setup(c)
            // 只有金额 > 150 的订单才算匹配 → alice 的 100 不算
            val r = rows(
                c,
                "SELECT u.name FROM users u WHERE EXISTS " +
                    "(SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 150)",
            )
            assertEquals(listOf(listOf("bob")), r)
        }
    }

    /** 非等值关联明确报错，而不是退化成一个把 IO 打爆的双层循环。 */
    @Test
    fun `非等值相关子查询明确报错`() {
        connect().use { c ->
            setup(c)
            val e = assertFailsWith<SQLException> {
                c.q { s ->
                    s.execute("SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.amount > u.id)")
                }
            }
            assertTrue(e.message!!.contains("等值"), "应当说明要求等值关联：${e.message}")
        }
    }

    /** 相关子查询与其它谓词混用时，剩余谓词仍然生效。 */
    @Test
    fun `相关子查询与普通谓词混用`() {
        connect().use { c ->
            setup(c)
            val r = rows(
                c,
                "SELECT u.name FROM users u WHERE u.city = 'bj' AND EXISTS " +
                    "(SELECT 1 FROM orders o WHERE o.user_id = u.id) ORDER BY u.name",
            )
            assertEquals(listOf(listOf("alice"), listOf("bob")), r)

            // 普通谓词把结果缩到 1 行：剩余 WHERE 必须还在起作用
            val r2 = rows(
                c,
                "SELECT u.name FROM users u WHERE u.id = 10 AND EXISTS " +
                    "(SELECT 1 FROM orders o WHERE o.user_id = u.id)",
            )
            assertEquals(listOf(listOf("alice")), r2)
        }
    }

    // ── 并集级的 ORDER BY / LIMIT ────────────────────────────────

    /** `A UNION B ORDER BY x` 的 ORDER BY 作用于**整个并集**。 */
    @Test
    fun `UNION 的 ORDER BY 作用于整个并集`() {
        connect().use { c ->
            setup(c)
            val r = rows(
                c,
                "SELECT city FROM users WHERE city='sh' UNION ALL SELECT city FROM users WHERE city='bj' " +
                    "ORDER BY city",
            )
            assertEquals(listOf(listOf("bj"), listOf("bj"), listOf("sh")), r, "排序要作用于拼接后的全部行")
            // DESC + LIMIT 一起用
            val r2 = rows(
                c,
                "SELECT city FROM users WHERE city='sh' UNION ALL SELECT city FROM users WHERE city='bj' " +
                    "ORDER BY city DESC LIMIT 1",
            )
            assertEquals(listOf(listOf("sh")), r2)
        }
    }

    /** 序号写在第一个分支里在 MySQL 里是语法错误，必须报错而不是悄悄丢掉那次排序。 */
    @Test
    fun `UNION 的 ORDER BY 写在分支里报错`() {
        connect().use { c ->
            setup(c)
            val e = assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT id FROM users ORDER BY id UNION SELECT id FROM users") }
            }
            assertTrue(e.message!!.contains("ORDER BY"), "${e.message}")
        }
    }

    /**
     * ★ `ORDER BY 1` 是**序号**，不是常量。
     *
     * 当成常量的话查询照常返回，只是顺序完全没变——而 `ORDER BY 1 LIMIT 1` 正是
     * 取 top-1 的常用写法，使用者会拿到错误的"最大值"。
     */
    @Test
    fun `ORDER BY 序号`() {
        connect().use { c ->
            setup(c)
            assertEquals(
                listOf(400L, 300L, 200L, 100L),
                rows(c, "SELECT amount FROM orders ORDER BY 1 DESC").map { it[0] },
                "ORDER BY 1 应当按第 1 个输出列排序",
            )
            // 多列输出时序号指向对应的那一列
            assertEquals(
                "alice",
                rows(c, "SELECT name, id FROM users ORDER BY 2 LIMIT 1")[0][0],
                "ORDER BY 2 是 id，最小的是 10（alice）",
            )
            // 越界必须报错
            assertFailsWith<SQLException> { c.q { s -> s.execute("SELECT id FROM users ORDER BY 5") } }
        }
    }

    /** 分组要在 Extra 里如实说 `Using temporary`（执行器确实建了分组表）。 */
    @Test
    fun `EXPLAIN 对分组说出 Using temporary`() {
        connect().use { c ->
            setup(c)
            val e = rows(c, "EXPLAIN SELECT city, COUNT(*) FROM users GROUP BY city")
            assertEquals(1, e.size)
            val extra = e[0].last()!!.toString()
            assertTrue(extra.contains("Using temporary"), "分组会在内存里建分组表，必须说出来：$extra")
        }
    }

    /** 连接要**每个来源一行**（MySQL 的形状），并如实说连接路径没走索引。 */
    @Test
    fun `EXPLAIN 对连接每个表给一行`() {
        connect().use { c ->
            setup(c)
            val e = rows(c, "EXPLAIN SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id")
            assertEquals(2, e.size, "两个来源就该有两行；挤成一行会让人以为只读了一张表")
            assertEquals(listOf("o", "u"), e.map { it[2] }, "第二列是 table（这里用的是别名）")
            assertTrue(e[0].last()!!.toString().contains("full scan"), "连接路径不做索引选择，要如实写")
        }
    }

    // ── 视图 ──────────────────────────────────────────────────────

    @Test
    fun `视图可以查询并叠加外层的 WHERE 与排序`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("CREATE VIEW v AS SELECT id, name, city FROM users WHERE city = 'bj'") }
            assertEquals(2, rows(c, "SELECT * FROM v").size, "视图自己的 WHERE 生效")
            assertEquals(
                listOf(listOf("alice")),
                rows(c, "SELECT name FROM v WHERE id = 10"),
                "外层的 WHERE 叠加在视图结果之上",
            )
            assertEquals(
                listOf(listOf("bob"), listOf("alice")),
                rows(c, "SELECT name FROM v ORDER BY id DESC").map { it },
            )
        }
    }

    /** ★ 视图的列名要能被客户端看到（`SELECT *` 展开、`information_schema.COLUMNS`）。 */
    @Test
    fun `视图的输出列被记录下来`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("CREATE VIEW v AS SELECT id AS uid, name FROM users") }
            assertEquals(listOf("uid", "name"), names(c, "SELECT * FROM v"))
            assertEquals(
                listOf("uid", "name"),
                rows(
                    c,
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = 'v' ORDER BY ORDINAL_POSITION",
                ).map { it[0] },
            )
            // 视图与表在 TABLES 里必须能区分
            assertEquals(
                "VIEW",
                one(c, "SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_NAME = 'v'"),
            )
        }
    }

    /** 视图定义要能取出来（`SHOW CREATE VIEW` / `information_schema.VIEWS`）。 */
    @Test
    fun `SHOW CREATE VIEW 与 information_schema 里的视图`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("CREATE VIEW v AS SELECT id FROM users WHERE city = 'bj'") }
            val ddl = rows(c, "SHOW CREATE VIEW v")[0][1].toString()
            assertTrue(ddl.contains("CREATE VIEW `v`"), "要给出可再次执行的 DDL：$ddl")
            assertTrue(ddl.contains("city = 'bj'"), "定义要原样带出来（含字符串字面量）：$ddl")
            assertEquals(
                "NO",
                one(c, "SELECT IS_UPDATABLE FROM information_schema.VIEWS WHERE TABLE_NAME = 'v'"),
                "本系统的视图是只读的，必须如实写 NO",
            )
        }
    }

    /**
     * ★ `SHOW CREATE TABLE v`（v 是视图）必须给**视图**的 DDL，而不是一份假的建表语句。
     *
     * 给了假 DDL 的话，mysqldump 会拿它去重建：视图变成一张空表，而且不报错。
     */
    @Test
    fun `对视图用 SHOW CREATE TABLE 也给视图 DDL`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("CREATE VIEW v AS SELECT id FROM users") }
            val r = rows(c, "SHOW CREATE TABLE v")
            assertEquals(
                listOf("View", "Create View", "character_set_client", "collation_connection"),
                names(c, "SHOW CREATE TABLE v"),
                "列名要与 MySQL 一致（视图的 SHOW CREATE 是四列）",
            )
            assertEquals("v", r[0][0], "第一列是视图名")
            assertTrue(r[0][1].toString().startsWith("CREATE VIEW"), "实际：${r[0][1]}")
        }
    }

    @Test
    fun `OR REPLACE 与 DROP VIEW`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("CREATE VIEW v AS SELECT id FROM users") }
            // 同名再建（不带 OR REPLACE）必须报错
            assertFailsWith<SQLException> { c.q { s -> s.execute("CREATE VIEW v AS SELECT id FROM orders") } }
            // 带 OR REPLACE 就换定义
            c.q { s -> s.execute("CREATE OR REPLACE VIEW v AS SELECT amount FROM orders") }
            assertEquals(listOf("amount"), names(c, "SELECT * FROM v"))
            c.q { s -> s.execute("DROP VIEW v") }
            assertFailsWith<SQLException> { c.q { s -> s.execute("SELECT * FROM v") } }
            // IF EXISTS 不报错
            c.q { s -> s.execute("DROP VIEW IF EXISTS v") }
        }
    }

    /** 视图是只读的：对它写入要明确报错，而不是以某种方式"成功"。 */
    @Test
    fun `视图不可写`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("CREATE VIEW v AS SELECT id, name FROM users") }
            assertFailsWith<SQLException> { c.q { s -> s.execute("INSERT INTO v VALUES (1,'x')") } }
            assertFailsWith<SQLException> { c.q { s -> s.execute("UPDATE v SET name='x'") } }
            assertFailsWith<SQLException> { c.q { s -> s.execute("DELETE FROM v") } }
        }
    }

    /** 视图可以参与分组/聚合（外层子句作用在视图结果上）。 */
    @Test
    fun `视图上可以分组`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("CREATE VIEW v AS SELECT city, id FROM users") }
            assertEquals(
                listOf(listOf("bj", 2L), listOf("sh", 1L)),
                rows(c, "SELECT city, COUNT(*) FROM v GROUP BY city ORDER BY city"),
            )
        }
    }

    /** 视图套视图。 */
    @Test
    fun `视图可以建在视图上`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("CREATE VIEW v1 AS SELECT id, city FROM users WHERE city = 'bj'") }
            c.q { s -> s.execute("CREATE VIEW v2 AS SELECT id FROM v1 WHERE id > 10") }
            assertEquals(listOf(listOf(11L)), rows(c, "SELECT id FROM v2"))
        }
    }

    // ── JSON 函数与运算符 ────────────────────────────────────────

    private fun setupJson(c: Connection) {
        setup(c)
        c.q { s ->
            s.execute("CREATE TABLE docs (id BIGINT NOT NULL, j JSON, PRIMARY KEY (id))")
            s.execute(
                """INSERT INTO docs VALUES
                   (1, '{"name":"alice","age":30,"tags":["a","b"]}'),
                   (2, '{"name":"bob","age":25,"tags":[]}'),
                   (3, '{"nested":{"k":"v"}}')""",
            )
        }
    }

    /**
     * ★ `->` 与 `->>` 的区别是**类型**，不是格式。
     *
     * `->` 给的是 JSON（字符串带引号），`->>` 给的是字符串（去引号）。
     * 混为一谈的表现是 `WHERE j ->> '\$.name' = 'alice'` 永远匹配不上——
     * 拿到的是 `"alice"`。不报错，只是没有结果。
     */
    @Test
    fun `JSON 取值运算符`() {
        connect().use { c ->
            setupJson(c)
            assertEquals("\"alice\"", one(c, "SELECT j -> '\$.name' FROM docs WHERE id = 1"), "-> 给 JSON，带引号")
            assertEquals("alice", one(c, "SELECT j ->> '\$.name' FROM docs WHERE id = 1"), "->> 给字符串")
            // 因此按值过滤要用 ->>
            assertEquals(1L, one(c, "SELECT COUNT(*) FROM docs WHERE j ->> '\$.name' = 'alice'"))
            // 数字不加引号（`->>` 给的是**文本**——MySQL 的 `JSON_UNQUOTE` 结果就是字符串）
            assertEquals("30", one(c, "SELECT j ->> '\$.age' FROM docs WHERE id = 1"))
            // ★ 但文本不等于"不能比大小"：数字文本必须按数值参与比较。
            // 少了这一步，`'30' > 25` 会因为"字符串排在所有数值之后"而为真，
            // 于是下面第一条会返回 3 行（整表），过滤条件静默失效
            assertEquals(1L, one(c, "SELECT COUNT(*) FROM docs WHERE j ->> '\$.age' > 26"))
            assertEquals(0L, one(c, "SELECT COUNT(*) FROM docs WHERE j ->> '\$.age' > 100"))
            assertEquals(1L, one(c, "SELECT COUNT(*) FROM docs WHERE j ->> '\$.age' = 30"))
            // 嵌套与数组下标
            assertEquals("v", one(c, "SELECT j ->> '\$.nested.k' FROM docs WHERE id = 3"))
            assertEquals("b", one(c, "SELECT j ->> '\$.tags[1]' FROM docs WHERE id = 1"))
            assertEquals("a", one(c, "SELECT j ->> '\$.tags[0]' FROM docs WHERE id = 1"))
        }
    }

    @Test
    fun `JSON_EXTRACT 与相关函数`() {
        connect().use { c ->
            setupJson(c)
            assertEquals("alice", one(c, "SELECT JSON_UNQUOTE(JSON_EXTRACT(j, '\$.name')) FROM docs WHERE id = 1"))
            assertEquals("OBJECT", one(c, "SELECT JSON_TYPE(j) FROM docs WHERE id = 1"))
            assertEquals("ARRAY", one(c, "SELECT JSON_TYPE(j -> '\$.tags') FROM docs WHERE id = 1"))
            assertEquals(2L, (one(c, "SELECT JSON_LENGTH(j -> '\$.tags') FROM docs WHERE id = 1") as Number).toLong())
            assertEquals(3L, (one(c, "SELECT JSON_LENGTH(j) FROM docs WHERE id = 1") as Number).toLong())
            assertEquals("[\"age\",\"name\",\"tags\"]", one(c, "SELECT JSON_KEYS(j) FROM docs WHERE id = 1"))
            assertTrue(bool(c, "SELECT JSON_VALID(j) FROM docs WHERE id = 1"))
        }
    }

    /** 路径不存在返回 NULL（MySQL 语义），但**路径语法错必须报错**。 */
    @Test
    fun `JSON 路径不存在给 NULL 语法错给报错`() {
        connect().use { c ->
            setupJson(c)
            assertEquals(null, one(c, "SELECT j ->> '\$.nope' FROM docs WHERE id = 1"), "路径不存在是 NULL")
            assertEquals(null, one(c, "SELECT j ->> '\$.tags[99]' FROM docs WHERE id = 1"), "下标越界也是 NULL")
            // 语法错要报错：静默返回 NULL 会让"所有行都没这个字段"
            assertFailsWith<SQLException> { c.q { s -> s.execute("SELECT j -> 'name' FROM docs") } }
            assertFailsWith<SQLException> { c.q { s -> s.execute("SELECT j -> '\$.tags[x]' FROM docs") } }
        }
    }

    // ── 标量位置的相关子查询 ──────────────────────────────────────

    /**
     * ★ 标量相关子查询：内层按关联键**扫一次**建表，外层每行按键查值。
     *
     * 三行里正好三种情况：有匹配、有匹配、**没有匹配**（`u12` 没有订单）。
     * 最后一种必须给 NULL——给 0 是一个看起来正常的错答案。
     */
    @Test
    fun `标量相关子查询给每一行一个值`() {
        connect().use { c ->
            setup(c)
            val r = rows(
                c,
                "SELECT id, (SELECT MAX(amount) FROM orders o WHERE o.user_id = u.id) FROM users u ORDER BY id",
            )
            assertEquals(listOf(listOf(10L, 100L), listOf(11L, 200L), listOf(12L, null)), r)
            assertEquals(
                listOf(1L, 1L, 0L),
                rows(c, "SELECT id, (SELECT COUNT(*) FROM orders o WHERE o.user_id = users.id) FROM users ORDER BY id")
                    .map { it[1] },
                "COUNT 在没有匹配行时是 0（聚合在空集上仍有一个值），不是 NULL",
            )
        }
    }

    /**
     * ★ 内层的 `WHERE` 里可以有一个**只引用外层**的条件。
     *
     * 它等价于"内层一行都不匹配"——有聚合时是**空集上的聚合**（`COUNT` 给 0），
     * 不是 NULL。把这两者混同，会在共享聚合的查询里给出一个看起来正常的 NULL。
     */
    @Test
    fun `标量相关子查询里只引用外层的条件`() {
        connect().use { c ->
            setup(c)
            // users 里 bj 两个（10/11，各有 1 个订单）、sh 一个（12，没有订单）
            assertEquals(
                listOf(listOf(10L, 1L), listOf(11L, 1L), listOf(12L, 0L)),
                rows(
                    c,
                    """
                    SELECT u.id, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id AND u.city = 'bj')
                    FROM users u ORDER BY u.id
                    """,
                ),
            )
        }
    }

    /** 标量相关子查询也能出现在**比较**里（WHERE / HAVING 位置的值）。 */
    @Test
    fun `标量相关子查询参与比较`() {
        connect().use { c ->
            setup(c)
            assertEquals(
                listOf(11L),
                rows(
                    c,
                    "SELECT id FROM users u WHERE (SELECT SUM(amount) FROM orders o WHERE o.user_id = u.id) > 150",
                ).map { it[0] },
            )
            // 没有订单的行：子查询给 NULL，比较结果是 UNKNOWN，不通过
            assertEquals(
                listOf(12L),
                rows(
                    c,
                    "SELECT id FROM users u WHERE (SELECT MAX(amount) FROM orders o WHERE o.user_id = u.id) IS NULL",
                ).map { it[0] },
            )
        }
    }

    /** `ORDER BY` + `LIMIT` 让内层在每个键上只挑一行（"每组最新的一条"这个经典写法）。 */
    @Test
    fun `标量相关子查询里可以排序取第一条`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("INSERT INTO orders VALUES (5,10,500)") }
            val r = rows(
                c,
                """
                SELECT id, (SELECT amount FROM orders o WHERE o.user_id = u.id ORDER BY amount DESC LIMIT 1)
                FROM users u ORDER BY id
                """,
            )
            assertEquals(listOf(listOf(10L, 500L), listOf(11L, 200L), listOf(12L, null)), r)
        }
    }

    /**
     * ★ 内层按关联键分组后有**多行**时必须报 1242，不能"随便取一行"。
     *
     * 取第一行会给出一个看起来正常、却取决于扫描顺序的值——同一份数据
     * 换一次 compaction 就可能变成另一个数。
     */
    @Test
    fun `标量相关子查询多行报 1242`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("INSERT INTO orders VALUES (5,10,500)") }
            val e = assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT id, (SELECT amount FROM orders o WHERE o.user_id = u.id) FROM users u") }
            }
            assertTrue(e.message!!.contains("more than 1 row"), e.message!!)
        }
    }

    /** 谓词位置与标量位置可以同时出现（一个改写成连接、一个按行查表）。 */
    @Test
    fun `谓词与标量位置的相关子查询可以共存`() {
        connect().use { c ->
            setup(c)
            val r = rows(
                c,
                """
                SELECT u.id, (SELECT SUM(amount) FROM orders o2 WHERE o2.user_id = u.id)
                FROM users u
                WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 150)
                ORDER BY u.id
                """,
            )
            assertEquals(listOf(listOf(11L, 200L)), r)
        }
    }

    /**
     * ★ `NOT IN` 的两条 NULL 规则最容易写错，而且都**不报错**：
     *
     * - 内层是**空集**时 `x NOT IN (空集)` 恒为**真**——哪怕 x 是 NULL。
     *   按"x 是 NULL 就未知"一刀切，会少返回那一行。
     * - 内层出现过 NULL（或外层值是 NULL 且内层非空）时结果是**未知**，
     *   找不到也不能返回——按"没找到就返回"处理会多返回一批行。
     */
    @Test
    fun `NOT IN 的 NULL 与空集语义`() {
        connect().use { c ->
            setup(c)
            c.q { s ->
                s.execute("INSERT INTO users VALUES (14, NULL, 'gz')")
                s.execute("DROP TABLE IF EXISTS tags")
                s.execute("CREATE TABLE tags (id BIGINT NOT NULL, user_id BIGINT, tag VARCHAR(16), PRIMARY KEY (id))")
                s.execute("INSERT INTO tags VALUES (1, 10, 'x')")
            }
            val sql =
                "SELECT id FROM users u WHERE u.name NOT IN (SELECT t.tag FROM tags t WHERE t.user_id = u.id) ORDER BY id"
            // 11/12/14 在内层都没有对应行 → 空集 → 全部为真（14 的 name 是 NULL 也一样）
            assertEquals(listOf(10L, 11L, 12L, 14L), rows(c, sql).map { it[0] })
            // 给 14 一个非 NULL 的标签：`NULL NOT IN ('y')` 是未知 → 那一行不返回
            c.q { s -> s.execute("INSERT INTO tags VALUES (2, 14, 'y')") }
            assertEquals(listOf(10L, 11L, 12L), rows(c, sql).map { it[0] })
            // 11 的标签是 NULL：`'bob' NOT IN (NULL)` 也是未知
            c.q { s -> s.execute("INSERT INTO tags VALUES (3, 11, NULL)") }
            assertEquals(listOf(10L, 12L), rows(c, sql).map { it[0] })
        }
    }

    /** 不支持的形状必须报错，且错误要说清为什么。 */
    @Test
    fun `标量相关子查询的错误形状要报错`() {
        connect().use { c ->
            setup(c)
            // 非等值关联：只能按行驱动，代价是外层行数 × 内层扫描
            assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT id, (SELECT MAX(amount) FROM orders o WHERE o.amount > u.id) FROM users u") }
            }
            // 关联键写错（外层别名是 u，却写了 users）——不能退化成内层的裸列
            assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT id, (SELECT MAX(amount) FROM orders o WHERE o.user_id = user_x.id) FROM users u") }
            }
            // 连接查询里的标量相关子查询：关联键对不上合成行
            assertFailsWith<SQLException> {
                c.q { s ->
                    s.execute(
                        "SELECT u.id, (SELECT COUNT(*) FROM orders o2 WHERE o2.user_id = u.id) " +
                            "FROM users u JOIN orders o ON o.user_id = u.id",
                    )
                }
            }
            // 分组查询里的标量相关子查询：按行算好的值对不上分组后的行
            assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT city, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) FROM users u GROUP BY city") }
            }
            // ★ `IN` / `NOT IN` 的两侧必须是普通的列：表达式虽然能参与匹配，
            // 但会让 `NOT IN` 的 NULL 规则定位错列——结果多几行或少几行且不报错
            assertFailsWith<SQLException> {
                c.q { s ->
                    s.execute(
                        "SELECT id FROM users u WHERE u.id + 1 NOT IN (SELECT o.user_id FROM orders o WHERE o.user_id = u.id)",
                    )
                }
            }
        }
    }

    // ── 窗口函数 ──────────────────────────────────────────────────

    /**
     * 窗口函数的表：`amt` 有重复（两个 20）、有 NULL、有两个分区。
     * 这三样正好是排名（同键并列）、聚合（NULL 跳过）、分区（边界）各自的考点。
     *
     * 按 `amt` 排序是 `NULL(5) < 5(4) < 10(1) < 20(2) = 20(3)`，
     * 按 `id` 排序就是插入顺序。
     */
    private fun setupSales(c: Connection) = c.q { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS sdb")
        s.execute("USE sdb")
        s.execute("DROP TABLE IF EXISTS sales")
        s.execute("CREATE TABLE sales (id BIGINT NOT NULL, region VARCHAR(8), amt INT, PRIMARY KEY (id))")
        s.execute(
            "INSERT INTO sales VALUES (1,'east',10),(2,'east',20),(3,'east',20),(4,'west',5),(5,'west',NULL)"
        )
    }

    /**
     * ★ 窗口函数**不改变行数**——它给每一行加一个值，不是把行归约掉。
     *
     * 把它实现成"聚合的别名"的表现就在这里：5 行的表会只返回 1 行，
     * 而且不报错、不警告。
     */
    @Test
    fun `窗口函数不改变行数`() {
        connect().use { c ->
            setupSales(c)
            assertEquals(5, rows(c, "SELECT id, ROW_NUMBER() OVER () FROM sales").size)
            // `SELECT *, 窗口函数` 也要把 `*` 展开成真实的列，而不是一列 NULL
            val r = rows(c, "SELECT *, ROW_NUMBER() OVER (ORDER BY id) FROM sales ORDER BY id")
            assertEquals(4, r[0].size)
            assertEquals(listOf(1L, "east", 10L, 1L), r[0])
        }
    }

    /** ★ `ROW_NUMBER` 递推，`RANK` 并列——同键的行名次相同。 */
    @Test
    fun `ROW_NUMBER 与 RANK 在同键上的区别`() {
        connect().use { c ->
            setupSales(c)
            val r = rows(
                c,
                """
                SELECT id, ROW_NUMBER() OVER (ORDER BY amt), RANK() OVER (ORDER BY amt),
                       DENSE_RANK() OVER (ORDER BY amt)
                FROM sales ORDER BY id
                """,
            )
            assertEquals(
                listOf(
                    listOf(1L, 3L, 3L, 3L),
                    listOf(2L, 4L, 4L, 4L),
                    // 与 id=2 同键：RANK 并列（两个都是 4），DENSE_RANK 只进一格
                    listOf(3L, 5L, 4L, 4L),
                    listOf(4L, 2L, 2L, 2L),
                    // NULL 在 ASC 里排最前（与 MySQL 一致）
                    listOf(5L, 1L, 1L, 1L),
                ),
                r,
            )
        }
    }

    /** ★ 分区：每个分区各自从头开始累计，绝不跨分区。 */
    @Test
    fun `SUM OVER 是分区内累计的`() {
        connect().use { c ->
            setupSales(c)
            val r = rows(
                c,
                """
                SELECT id, SUM(amt) OVER (PARTITION BY region ORDER BY id),
                       COUNT(*) OVER (PARTITION BY region)
                FROM sales ORDER BY id
                """,
            )
            assertEquals(
                listOf(
                    listOf(1L, 10L, 3L),
                    listOf(2L, 30L, 3L),
                    listOf(3L, 50L, 3L),
                    // west 分区重新从 0 开始；NULL 不参与求和，但算在行数里
                    listOf(4L, 5L, 2L),
                    listOf(5L, 5L, 2L),
                ),
                r,
            )
        }
    }

    /**
     * ★ 同一个帧的两种写法走**两条不同的代码路径**（增量累加 / 逐位置重算），
     * 结果必须逐值相同。
     *
     * `ORDER BY id` 用的是默认帧，起点是 `UNBOUNDED PRECEDING` → 增量；
     * `ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW` 起点是 `PRECEDING` → 重算，
     * 而一百万行远大于这里的 5 行，于是两者夹出来的是**同一个帧**。
     * 两种写法给出同一个答案，才对得起"SQL 里同一个问题的两种写法"。
     */
    @Test
    fun `增量与重算两条路径结果一致`() {
        connect().use { c ->
            setupSales(c)
            for (fn in listOf("SUM(amt)", "AVG(amt)", "MIN(amt)", "MAX(amt)", "COUNT(amt)", "COUNT(*)")) {
                val incremental = rows(c, "SELECT $fn OVER (ORDER BY id) FROM sales ORDER BY id")
                val recomputed = rows(
                    c,
                    "SELECT $fn OVER (ORDER BY id ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) " +
                        "FROM sales ORDER BY id",
                )
                assertEquals(incremental, recomputed, "$fn 在两条路径上结果不同")
            }
        }
    }

    @Test
    fun `LAG 与 LEAD 取相邻行`() {
        connect().use { c ->
            setupSales(c)
            val r = rows(
                c,
                """
                SELECT id, LAG(amt) OVER (ORDER BY id), LEAD(amt) OVER (ORDER BY id),
                       LAG(amt, 1, -1) OVER (ORDER BY id)
                FROM sales ORDER BY id
                """,
            )
            assertEquals(
                listOf(
                    listOf(1L, null, 20L, -1L),
                    listOf(2L, 10L, 20L, 10L),
                    listOf(3L, 20L, 5L, 20L),
                    listOf(4L, 20L, null, 20L),
                    // 最后一行没有下一行，LEAD 给 NULL
                    listOf(5L, 5L, null, 5L),
                ),
                r,
            )
        }
    }

    /**
     * ★ `LAST_VALUE` 的默认帧是"到当前行为止"，**不是**整个分区。
     *
     * 这是窗口函数里最经典的一个坑：`LAST_VALUE(x) OVER (ORDER BY k)` 给的是
     * 当前行自己的值。要整个分区的最后一个值，必须显式写出完整的帧。
     * 两条都测，因为"默认帧是什么"这件事只能靠断言钉住。
     */
    @Test
    fun `LAST_VALUE 的默认帧到当前行为止`() {
        connect().use { c ->
            setupSales(c)
            // 按 amt 排序是 NULL(id5) < 5(id4) < 10(id1) < 20(id2) = 20(id3)。
            // 默认帧是 RANGE（按**值**而不是按行数）且只到 CURRENT ROW，
            // 而 RANGE 的 CURRENT ROW 含**所有同键的行**：
            // 所以 id2 的 LAST_VALUE(id) 是它的同键行 id3，不是它自己
            assertEquals(
                listOf(5L, 4L, 1L, 3L, 3L),
                rows(c, "SELECT LAST_VALUE(id) OVER (ORDER BY amt) FROM sales ORDER BY amt").map { it[0] },
            )
            // 显式写出完整的帧，才是"整个分区的最后一个值"
            assertEquals(
                List(5) { 3L },
                rows(
                    c,
                    "SELECT LAST_VALUE(id) OVER (ORDER BY amt ROWS BETWEEN UNBOUNDED PRECEDING " +
                        "AND UNBOUNDED FOLLOWING) FROM sales ORDER BY amt",
                ).map { it[0] },
            )
        }
    }

    /** 滑动帧：两端都跟着当前行走，走的是"逐位置重算"那条路。 */
    @Test
    fun `滑动的帧`() {
        connect().use { c ->
            setupSales(c)
            val r = rows(
                c,
                "SELECT id, SUM(amt) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                    "FROM sales ORDER BY id",
            )
            assertEquals(
                listOf(
                    listOf(1L, 30L),     // 10+20
                    listOf(2L, 50L),     // 10+20+20
                    listOf(3L, 45L),     // 20+20+5
                    listOf(4L, 25L),     // 20+5+NULL
                    listOf(5L, 5L),      // 5+NULL
                ),
                r,
            )
        }
    }

    /** NTILE：前 `n mod k` 个桶各多放一行。 */
    @Test
    fun `NTILE 分桶`() {
        connect().use { c ->
            setupSales(c)
            assertEquals(
                listOf(1L, 1L, 1L, 2L, 2L),
                rows(c, "SELECT NTILE(2) OVER (ORDER BY id) FROM sales ORDER BY id").map { it[0] },
            )
            assertEquals(
                listOf(1L, 1L, 2L, 3L, 4L),
                rows(c, "SELECT NTILE(4) OVER (ORDER BY id) FROM sales ORDER BY id").map { it[0] },
            )
        }
    }

    /**
     * ★ 有 `GROUP BY` 时，窗口函数作用在**分组之后**的行上：
     * 窗口的入参本身可以是一个聚合（`SUM(SUM(amt))` = 对每组的和再求和）。
     */
    @Test
    fun `窗口函数作用在分组之后的行上`() {
        connect().use { c ->
            setupSales(c)
            val r = rows(
                c,
                """
                SELECT region, SUM(amt), SUM(SUM(amt)) OVER (ORDER BY region)
                FROM sales GROUP BY region ORDER BY region
                """,
            )
            assertEquals(listOf(listOf("east", 50L, 50L), listOf("west", 5L, 55L)), r)
        }
    }

    /** 窗口函数在 `ORDER BY` 之前算完，所以可以按它排序（别名也可以）。 */
    @Test
    fun `按窗口函数排序`() {
        connect().use { c ->
            setupSales(c)
            val r = rows(c, "SELECT id, ROW_NUMBER() OVER (ORDER BY amt DESC) AS rn FROM sales ORDER BY rn")
            assertEquals(listOf(2L, 3L, 1L, 4L, 5L), r.map { it[0] })
        }
    }

    /**
     * ★ 不支持的形状**必须报错**。
     *
     * 每一条都对应一种"能跑出结果、但结果没有意义"的写法：静默当 NULL、
     * 拿扫描顺序当顺序、把 RANGE 当 ROWS……全都不会报错，只会给错答案。
     */
    @Test
    fun `窗口函数的错误形状要报错`() {
        connect().use { c ->
            setupSales(c)
            // WHERE 在窗口函数之前求值：那里没有值
            assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT id FROM sales WHERE ROW_NUMBER() OVER () = 1") }
            }
            // RANK 没有 ORDER BY 时"排第几"本身没有定义
            assertFailsWith<SQLException> { c.q { s -> s.execute("SELECT RANK() OVER () FROM sales") } }
            // GROUPS 帧不支持
            assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT SUM(amt) OVER (ORDER BY id GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM sales") }
            }
            // RANGE 的 n PRECEDING 要按键值做区间运算；当成行数会给出错行
            assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT SUM(amt) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM sales") }
            }
            // 窗口函数少了 OVER
            assertFailsWith<SQLException> { c.q { s -> s.execute("SELECT ROW_NUMBER() FROM sales") } }
            // 有 GROUP BY 时，窗口函数里引用了不在 GROUP BY 里的列
            assertFailsWith<SQLException> { c.q { s -> s.execute("SELECT id, SUM(amt) OVER () FROM sales GROUP BY id") } }
            // 不支持的名字要说清楚，而不是当成普通函数
            assertFailsWith<SQLException> { c.q { s -> s.execute("SELECT MEDIAN(amt) OVER () FROM sales") } }
        }
    }

    // ── 按列类型解释字面量（同一段字面量只能有一种含义）────────────

    /**
     * ★ **每一个**读字面量的地方都要按列类型解释它，包括 `IN`。
     *
     * `=` 那一处早就转了（见 `evalBinary`），而 `IN` 曾经直接用字面量的原始类型
     * 去比：`id IN ('10','11')` 里字符串永不等于数字，查询**静默返回空集**。
     * 这条路径很容易被 JDBC 触发——参数按字符串绑定时就是这个形状。
     */
    @Test
    fun `IN 的字面量按列类型解释`() {
        connect().use { c ->
            setup(c)
            assertEquals(2, rows(c, "SELECT id FROM users WHERE id IN ('10','11')").size)
            assertEquals(2, rows(c, "SELECT id FROM users WHERE id IN (10, 11)").size)
            assertEquals(1, rows(c, "SELECT id FROM users WHERE id NOT IN ('10','11')").size)
        }
    }

    /**
     * ★ `BETWEEN` 同样要转换，而且必须遵守三值逻辑。
     *
     * 两端都是字符串字面量、列是 DATETIME——不转换则两端都比不过，
     * `BETWEEN` 返回空集、`NOT BETWEEN` 反而返回**全部**行。
     * 值为 NULL 时两者都该是 UNKNOWN（正反都不通过）。
     */
    @Test
    fun `BETWEEN 按列类型解释且遵守三值逻辑`() {
        connect().use { c ->
            setup(c)
            c.q { s ->
                s.execute("DROP TABLE IF EXISTS events")
                s.execute("CREATE TABLE events (id BIGINT NOT NULL, dt DATETIME, v INT, PRIMARY KEY (id))")
                s.execute("INSERT INTO events VALUES (1,'2026-03-01 00:00:00',5),(2,'2026-09-01 00:00:00',NULL)")
            }
            assertEquals(
                listOf(1L),
                rows(c, "SELECT id FROM events WHERE dt BETWEEN '2026-01-01 00:00:00' AND '2026-06-30 00:00:00'")
                    .map { it[0] },
            )
            assertEquals(
                listOf(2L),
                rows(c, "SELECT id FROM events WHERE dt NOT BETWEEN '2026-01-01 00:00:00' AND '2026-06-30 00:00:00'")
                    .map { it[0] },
            )
            // v = NULL：正反都返回 0 行（UNKNOWN 不是真，也不是假）
            assertEquals(1, rows(c, "SELECT id FROM events WHERE v BETWEEN 1 AND 10").size)
            assertEquals(
                0, rows(c, "SELECT id FROM events WHERE v NOT BETWEEN 1 AND 10").size,
                "NULL NOT BETWEEN 是 UNKNOWN：把未知当假会让这一行被返回",
            )
        }
    }

    /**
     * ★ 整数运算**溢出必须报错**，不能回绕——与 `SUM` 同一个立场。
     *
     * 回绕给出的数字**看起来完全正常**，只是符号反了（`MAX + 1` 变成 `MIN`）。
     * 使用者拿它做对账或计数时，看到的是一个"合法"的 BIGINT，
     * 没有任何线索指向"这里溢出了"。同一个加法在 `SUM` 里报错、单独写却回绕，
     * 是一处没有理由的差异。
     */
    @Test
    fun `整数运算溢出报错而不是回绕`() {
        connect().use { c ->
            setup(c)
            val big = "9223372036854775807"
            val e = assertFailsWith<SQLException> { c.q { s -> s.executeQuery("SELECT $big + 1") } }
            assertTrue(e.message!!.contains("溢出"), e.message!!)
            assertFailsWith<SQLException> { c.q { s -> s.executeQuery("SELECT $big * 2") } }
            assertFailsWith<SQLException> { c.q { s -> s.executeQuery("SELECT -9223372036854775808 - 1") } }
            // 取负也有同一个边界（64 位里没有对应的正数）
            assertFailsWith<SQLException> { c.q { s -> s.executeQuery("SELECT -(-9223372036854775808)") } }
            // `/` 按浮点算（与 MySQL 一致），所以整数除法不会溢出：5/2 是 2.5
            assertEquals(2.5, one(c, "SELECT 5 / 2"))
            // 不溢出的照常算
            assertEquals(3L, one(c, "SELECT 1 + 2"))
            assertEquals(0L, one(c, "SELECT 5 % 5"))
            // ★ BIGINT 的最小值是一个**合法**字面量，必须精确。
            // 负号折进字面量之前，它被当成浮点数（2^63 存不进 i64），值就差了 1
            assertEquals(Long.MIN_VALUE, one(c, "SELECT -9223372036854775808"))
            // 而超出 BIGINT 的整数字面量必须报错，不能退化成丢精度的浮点
            assertFailsWith<SQLException> { c.q { s -> s.executeQuery("SELECT 9223372036854775809") } }
        }
    }

    /** ★ `LIKE` / `NOT LIKE` 遇到 NULL 也是 UNKNOWN。 */
    @Test
    fun `LIKE 遇到 NULL 给 UNKNOWN`() {
        connect().use { c ->
            setup(c)
            c.q { s -> s.execute("INSERT INTO users VALUES (13,'dave',NULL)") }
            assertEquals(
                listOf(12L),
                rows(c, "SELECT id FROM users WHERE city NOT LIKE 'b%'").map { it[0] },
                "city 为 NULL 的那一行既不是 LIKE 也不是 NOT LIKE",
            )
            assertEquals(2L, one(c, "SELECT COUNT(*) FROM users WHERE city LIKE 'b%'"))
        }
    }

    /**
     * ★ `SELECT *, 其它项` 必须把 `*` 展开成**真实的列**。
     *
     * 曾经只有"投影里仅有一个 `*`"这一条快路径是对的，混写时每一列都变成 NULL、
     * 列名还叫 `*`——结果集形状正常、数据全空。
     */
    @Test
    fun `星号与其它项混写要展开真实列`() {
        connect().use { c ->
            setup(c)
            assertEquals(listOf("id", "name", "city", "one"), names(c, "SELECT *, 1 AS one FROM users"))
            assertEquals(listOf(10L, "alice", "bj", 1L), rows(c, "SELECT *, 1 AS one FROM users ORDER BY id")[0])
        }
    }

    /**
     * ★ `GROUP BY` 下的 `COUNT(*)` **不能**走元数据捷径。
     *
     * 元数据给的是"全表一个数"，而在分组查询里要按组各数一次。
     * 走了捷径的话，`SELECT COUNT(*) FROM users GROUP BY city` 只返回**一行**、
     * 数字是全表行数——不报错，而且只在元数据路径可用时才错
     * （事务里有未提交写入时会回退到扫描，于是同一句 SQL 时而对时而错）。
     */
    @Test
    fun `GROUP BY 下的 COUNT 不能用元数据捷径`() {
        connect().use { c ->
            setup(c)
            assertEquals(
                listOf(listOf("bj", 2L), listOf("sh", 1L)),
                rows(c, "SELECT city, COUNT(*) FROM users GROUP BY city ORDER BY city"),
            )
            assertEquals(
                listOf(2L, 1L),
                rows(c, "SELECT COUNT(*) FROM users GROUP BY city ORDER BY city").map { it[0] },
            )
            // HAVING 也一样：它要在分组之后过滤，捷径会把整张表当一个组
            assertEquals(
                listOf(2L),
                rows(c, "SELECT COUNT(*) FROM users GROUP BY city HAVING COUNT(*) > 1").map { it[0] },
            )
        }
    }

    /** ★ `DISTINCT` / `UNION` 按 **SQL 的等值语义**去重，不是按对象的结构相等。 */
    @Test
    fun `DISTINCT 与 UNION 按数值相等去重`() {
        connect().use { c ->
            setup(c)
            c.q { s ->
                s.execute("DROP TABLE IF EXISTS t_int")
                s.execute("DROP TABLE IF EXISTS t_dbl")
                s.execute("CREATE TABLE t_int (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
                s.execute("CREATE TABLE t_dbl (id BIGINT NOT NULL, v DOUBLE, PRIMARY KEY (id))")
                s.execute("INSERT INTO t_int VALUES (1,10),(2,20)")
                s.execute("INSERT INTO t_dbl VALUES (1,10.0)")
            }
            // 10(INT) 与 10.0(DOUBLE) 在 SQL 里是同一个值 → 去重之后只剩两行
            assertEquals(
                2, rows(c, "SELECT v FROM t_int UNION SELECT v FROM t_dbl").size,
                "跨数值类型的相等值必须被判为重复",
            )
            // `UNION ALL` 保留重复：2 行 + 1 行 = 3 行
            assertEquals(3, rows(c, "SELECT v FROM t_int UNION ALL SELECT v FROM t_dbl").size)
        }
    }

    // ── 行锁（SELECT ... FOR UPDATE，设计文档 §4.4）──────────────────

    /**
     * ★ `FOR UPDATE` 的意义是**挡住另一个事务**。
     *
     * 断言打在"第二个事务必须等"上，而不是"语法被接受了"——后者就算行锁完全没生效
     * 也照样通过。这里用**连接级**的互斥来验证：A 锁住一行不提交，B 就改不动它。
     */
    @Test
    fun `FOR UPDATE 会挡住另一个事务`() {
        connect().use { c1 ->
            setup(c1)
            c1.q { s -> s.execute("INSERT INTO users VALUES (20,'eve','gz')") }
            // 两个连接：A 在事务里锁住一行，B 想改同一行
            connect().use { c2 ->
                c1.autoCommit = false
                c2.autoCommit = false
                try {
                    // A 锁住 id=20
                    assertEquals(20L, one(c1, "SELECT id FROM users WHERE id = 20 FOR UPDATE"))
                    // B 等 0.5 秒拿不到 → 1205。把等待上限调小，避免用例本身等 50 秒
                    c2.createStatement().use { st ->
                        st.execute("SET SESSION innodb_lock_wait_timeout = 1")
                    }
                    val t0 = System.currentTimeMillis()
                    val e = assertFailsWith<SQLException> {
                        c2.q { s -> s.execute("SELECT id FROM users WHERE id = 20 FOR UPDATE") }
                    }
                    assertTrue(e.message!!.contains("Lock wait timeout") || e.message!!.contains("1205"), e.message!!)
                    assertTrue(System.currentTimeMillis() - t0 >= 900, "至少要等满 1 秒才超时")
                    // A 提交之后，B 应当立刻拿到
                    c1.commit()
                    assertEquals(20L, one(c2, "SELECT id FROM users WHERE id = 20 FOR UPDATE"))
                    c2.commit()
                } finally {
                    runCatching { c1.rollback() }
                    runCatching { c2.rollback() }
                    c1.autoCommit = true
                    c2.autoCommit = true
                }
            }
        }
    }

    /** 事务回滚 / 提交之后锁必须被放掉，否则行会被永久占住（直到 TTL）。 */
    @Test
    fun `事务结束会释放行锁`() {
        connect().use { c1 ->
            setup(c1)
            c1.q { s -> s.execute("INSERT INTO users VALUES (21,'frank','gz')") }
            connect().use { c2 ->
                c1.autoCommit = false
                c2.autoCommit = false
                c2.createStatement().use { st -> st.execute("SET SESSION innodb_lock_wait_timeout = 2") }
                try {
                    one(c1, "SELECT id FROM users WHERE id = 21 FOR UPDATE")
                    c1.rollback()
                    // 回滚之后 B 必须能立刻拿到（不是等 2 秒超时）
                    assertEquals(21L, one(c2, "SELECT id FROM users WHERE id = 21 FOR UPDATE"))
                    c2.commit()
                } finally {
                    runCatching { c1.rollback() }
                    runCatching { c2.rollback() }
                    c1.autoCommit = true
                    c2.autoCommit = true
                }
            }
        }
    }

    /**
     * ★ autocommit 下 `FOR UPDATE` 的锁**只在语句期间有效**（MySQL 语义）。
     *
     * 不释放的话，一句自动提交的查询会把行**永久**锁住——而使用者完全不会
     * 意识到自己开了锁（他甚至没写 BEGIN）。
     */
    @Test
    fun `autocommit 下 FOR UPDATE 语句结束即释放`() {
        connect().use { c1 ->
            setup(c1)
            connect().use { c2 ->
                c2.createStatement().use { st -> st.execute("SET SESSION innodb_lock_wait_timeout = 3") }
                // c1 是 autocommit（默认）：语句结束锁就该没了
                assertEquals(10L, one(c1, "SELECT id FROM users WHERE id = 10 FOR UPDATE"))
                assertEquals(10L, one(c2, "SELECT id FROM users WHERE id = 10 FOR UPDATE"))
            }
        }
    }

    /** 共享锁之间不互斥（两个事务都能 `LOCK IN SHARE MODE` 读同一行）。 */
    @Test
    fun `共享锁之间不互斥`() {
        connect().use { c1 ->
            setup(c1)
            connect().use { c2 ->
                c1.autoCommit = false
                c2.autoCommit = false
                c2.createStatement().use { st -> st.execute("SET SESSION innodb_lock_wait_timeout = 2") }
                try {
                    assertEquals(10L, one(c1, "SELECT id FROM users WHERE id = 10 LOCK IN SHARE MODE"))
                    assertEquals(10L, one(c2, "SELECT id FROM users WHERE id = 10 LOCK IN SHARE MODE"))
                    c1.commit()
                    c2.commit()
                } finally {
                    runCatching { c1.rollback() }
                    runCatching { c2.rollback() }
                    c1.autoCommit = true
                    c2.autoCommit = true
                }
            }
        }
    }

    /** 暂不支持的形状必须报错（而不是"只锁一半"）。 */
    @Test
    fun `FOR UPDATE 的不支持形状要报错`() {
        connect().use { c ->
            setup(c)
            // 连接查询：MySQL 会锁住每张表的行，而本系统只定义了按基表主键加锁
            assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id FOR UPDATE") }
            }
            // 修饰符会改变语义，不能忽略
            assertFailsWith<SQLException> { c.q { s -> s.execute("SELECT id FROM users FOR UPDATE NOWAIT") } }
            // 系统库的虚拟表没有真实的行
            assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT TABLE_NAME FROM information_schema.TABLES FOR UPDATE") }
            }
        }
    }

    // ── 错误信息必须指向真正的问题 ────────────────────────────────

    @Test
    fun `UNION 列数不一致报错`() {
        connect().use { c ->
            setup(c)
            val e = assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT id, name FROM users UNION SELECT id, name, city FROM users") }
            }
            assertTrue(e.message!!.contains("different number of columns"), "${e.message}")
        }
    }

    /**
     * 写在**最后一个分支之后**的 ORDER BY 是合法的：它作用于整个并集。
     *
     * （此前这里断言的是"UNION 分支带 ORDER BY 报错"——那时子句会被挂到右侧那个
     * 分支上，只排一半。现在并集级的子句解析出来了，这条断言要跟着变成新语义。）
     */
    @Test
    fun `UNION 末尾的 ORDER BY 合法且作用于整个并集`() {
        connect().use { c ->
            setup(c)
            val r = rows(c, "SELECT id FROM users WHERE id = 12 UNION SELECT id FROM users WHERE id = 10 ORDER BY id")
            assertEquals(listOf(listOf(10L), listOf(12L)), r)
        }
    }

    @Test
    fun `标量子查询返回多行报 1242`() {
        connect().use { c ->
            setup(c)
            val e = assertFailsWith<SQLException> {
                c.q { s -> s.execute("SELECT id FROM orders WHERE amount = (SELECT amount FROM orders)") }
            }
            assertTrue(e.message!!.contains("more than 1 row"), "${e.message}")
        }
    }
}
