package com.cloudea.yangbase.server

import java.io.File
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * **`mysqldump` / `mysql` 这类标准 MySQL 工具的兼容面**。
 *
 * ## 为什么单独一个文件
 *
 * 这些工具与 Connector/J 用的是**同一套协议**，但它们踩的路径完全不同：
 * Connector/J 宽容、而 libmysqlclient **严格**，而且工具会依赖一批"没人写进文档
 * 但一定要有"的东西——`SHOW FULL TABLES` 的第二列、`SHOW TABLE STATUS` 的列序、
 * 用户变量、`@@collation_database`……少一个的后果不是"少个功能"，而是
 * **工具直接崩掉或第一张表就停住**（真事：`@@collation_database` 返回 NULL 时，
 * mysqldump 会 `strncpy(dest, NULL, 30)` 段错误，core dump 里
 * `rsi=0x0 rdx=0x1e`）。
 *
 * 所以这里逐条盯住"工具要求什么"，而不是"我们实现了什么"。
 */
class MysqldumpCompatTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-dumpcompat")
        server = YangBaseServer.forTesting(
            store = com.cloudea.yangbase.objectstore.LocalFileObjectStore(dataDir),
            port = 0,
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

    private fun connect(db: String = "dc"): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/$db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun <T> Connection.q(b: (Statement) -> T): T = createStatement().use(b)

    private fun Connection.exec(sql: String) = q { it.execute(sql) }

    private fun Connection.setup() = q { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS dc")
        s.execute("USE dc")
    }

    private fun Connection.grid(sql: String): List<List<Any?>> = q { s ->
        s.executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            buildList {
                while (rs.next()) add((1..n).map { rs.getObject(it) })
            }
        }
    }

    private fun Connection.header(sql: String): List<String> = q { s ->
        s.executeQuery(sql).use { rs ->
            (1..rs.metaData.columnCount).map { rs.metaData.getColumnLabel(it) }
        }
    }

    private fun Connection.value(sql: String): Any? = q { s ->
        s.executeQuery(sql).use { rs -> if (rs.next()) rs.getObject(1) else null }
    }

    // ── 工具依赖的元数据形状 ──────────────────────────────────────

    @Test
    fun `SHOW FULL TABLES 必须给出 Table_type 列`() {
        connect().use { c ->
            c.setup()
            c.exec("DROP VIEW IF EXISTS v1")
            c.exec("DROP TABLE IF EXISTS t1")
            c.exec("CREATE TABLE t1 (id BIGINT PRIMARY KEY)")
            c.exec("CREATE VIEW v1 AS SELECT id FROM t1")

            // mysqldump 读 row[1] 区分表与视图；列数少一个 → 它读到 NULL → 段错误
            assertEquals(listOf("Tables_in_dc", "Table_type"), c.header("SHOW FULL TABLES"))
            val rows = c.grid("SHOW FULL TABLES").associate { it[0].toString() to it[1].toString() }
            assertEquals("BASE TABLE", rows["t1"])
            assertEquals("VIEW", rows["v1"])
            // 不带 FULL 时仍然只有一列（MySQL 如此）
            assertEquals(listOf("Tables_in_dc"), c.header("SHOW TABLES"))
        }
    }

    @Test
    fun `SHOW TABLES FROM 指定的库优先于当前库`() {
        connect().use { c ->
            c.setup()
            c.q { it.execute("CREATE DATABASE IF NOT EXISTS other") }
            c.exec("DROP TABLE IF EXISTS t_here")
            c.exec("CREATE TABLE t_here (id BIGINT PRIMARY KEY)")
            c.q { s -> s.execute("USE other"); s.execute("DROP TABLE IF EXISTS t_there"); s.execute("CREATE TABLE t_there (id BIGINT PRIMARY KEY)") }
            c.q { it.execute("USE dc") }

            assertEquals(listOf("t_here"), c.grid("SHOW TABLES FROM dc").map { it[0].toString() })
            assertEquals(listOf("t_there"), c.grid("SHOW TABLES FROM other").map { it[0].toString() })
        }
    }

    @Test
    fun `CREATE DATABASE 不改变当前库`() {
        connect().use { c ->
            c.setup()
            c.exec("DROP DATABASE IF EXISTS side")
            assertEquals("dc", c.value("SELECT DATABASE()").toString())
            c.exec("CREATE DATABASE side")
            // MySQL：只建库，当前库不变。曾经这里会顺手切过去，于是紧接着的
            // 不带库名的 CREATE TABLE 会**静默建到 side 里**——
            // 初始化脚本/迁移脚本/备份回灌都是"建完库接着建表"的写法
            assertEquals("dc", c.value("SELECT DATABASE()").toString(), "CREATE DATABASE 不该切换当前库")
            c.exec("DROP TABLE IF EXISTS side_probe")
            c.exec("CREATE TABLE side_probe (id BIGINT PRIMARY KEY)")
            assertEquals(
                listOf("side_probe"),
                c.grid("SHOW TABLES FROM dc").map { it[0].toString() },
                "建表必须落在当前库 dc，而不是刚建的 side",
            )
            assertEquals(0, c.grid("SHOW TABLES FROM side").size)
        }
    }

    @Test
    fun `SHOW TABLE STATUS 的列名与行数`() {
        connect().use { c ->
            c.setup()
            c.exec("DROP TABLE IF EXISTS ts")
            c.exec("CREATE TABLE ts (id BIGINT PRIMARY KEY, v INT)")
            c.exec("INSERT INTO ts VALUES (1,1),(2,2),(3,3)")

            val cols = c.header("SHOW TABLE STATUS")
            // 列名与顺序按 MySQL 8.0 的 18 列：mysqldump 按**位置**索引它们
            assertEquals(
                listOf(
                    "Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length",
                    "Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment",
                    "Create_time", "Update_time", "Check_time", "Collation", "Checksum",
                    "Create_options", "Comment",
                ),
                cols,
            )
            val row = c.grid("SHOW TABLE STATUS LIKE 'ts'").single()
            assertEquals("ts", row[0].toString())
            assertNotNull(row[1], "Engine 不能是 NULL（工具用它判断表类型）")
            assertEquals(3L, (row[4] as Number).toLong(), "Rows 应当是元数据里的精确行数（0 次 GET）")
        }
    }

    @Test
    fun `SHOW TRIGGERS 返回空集而不是报错`() {
        connect().use { c ->
            c.setup()
            c.exec("DROP TABLE IF EXISTS trg")
            c.exec("CREATE TABLE trg (id BIGINT PRIMARY KEY)")
            // 每张表都会被问一次；报错会让任何导出工具在第一张表上停住
            assertEquals(0, c.grid("SHOW TRIGGERS LIKE 'trg'").size)
            assertTrue(c.header("SHOW TRIGGERS LIKE 'trg'").isNotEmpty())
        }
    }

    // ── 工具依赖的会话行为 ────────────────────────────────────────

    @Test
    fun `用户变量能存能取且不遮蔽系统变量`() {
        connect().use { c ->
            c.exec("SET @a = 42")
            assertEquals(42L, (c.value("SELECT @a") as Number).toLong())
            c.exec("SET @b = 'hi'")
            assertEquals("hi", c.value("SELECT @b"))
            // `@version` 与 `@@version` 是两个命名空间，不能互相遮蔽
            c.exec("SET @version = 'mine'")
            assertEquals("mine", c.value("SELECT @version"))
            assertTrue(c.value("SELECT @@version").toString().isNotEmpty())
            // 未赋值的用户变量是 NULL（MySQL 语义），而不是报错
            assertEquals(null, c.value("SELECT @never_set"))
            // `:=` 与 `=` 等价
            c.exec("SET @c := 7")
            assertEquals(7L, (c.value("SELECT @c") as Number).toLong())
        }
    }

    @Test
    fun `SET 的裸标识符值按字符串处理`() {
        connect().use { c ->
            // mysqldump 的产物里成片出现这一形态；当成列引用只会得到 NULL，
            // 随后在别处以 "Unknown character set: ''" 这种指错方向的错误冒出来
            c.exec("SET character_set_client = utf8mb4")
            assertEquals("utf8mb4", c.value("SELECT @@character_set_client"))
            c.exec("SET character_set_client = 'latin1'")
            assertEquals("latin1", c.value("SELECT @@character_set_client"))
        }
    }

    @Test
    fun `ALTER TABLE ENABLE DISABLE KEYS 是空操作`() {
        connect().use { c ->
            c.setup()
            c.exec("DROP TABLE IF EXISTS ekeys")
            c.exec("CREATE TABLE ekeys (id BIGINT PRIMARY KEY, v VARCHAR(8))")
            c.exec("INSERT INTO ekeys VALUES (1,'a')")
            // 每张表的导入/导出都会发这两句；当成语法错误会让导入在第一个表就失败
            c.exec("ALTER TABLE ekeys DISABLE KEYS")
            c.exec("ALTER TABLE ekeys ENABLE KEYS")
            assertEquals(1L, (c.value("SELECT COUNT(*) FROM ekeys") as Number).toLong())
        }
    }

    @Test
    fun `客户端会读的会话变量不能是 NULL`() {
        connect().use { c ->
            // `@@collation_database` 为 NULL 时 mysqldump 会段错误（strncpy(dest, NULL, 30)），
            // 所以这几个"每个库一个值"的变量必须给出真实值
            assertNotNull(c.value("SELECT @@collation_database"), "@@collation_database 不能是 NULL")
            assertNotNull(c.value("SELECT @@character_set_database"), "@@character_set_database 不能是 NULL")
            assertNotNull(c.value("SELECT @@collation_server"))
            assertNotNull(c.value("SELECT @@character_set_server"))
        }
    }

    // ── 端到端往返（本机装了 mysqldump 时才跑）────────────────────

    @Test
    fun `mysqldump 导出后能灌回一个全新实例`() {
        val tool = listOf(File("/usr/bin/mysqldump"), File("/usr/local/bin/mysqldump"))
            .firstOrNull { it.canExecute() }
        org.junit.jupiter.api.Assumptions.assumeTrue(tool != null, "本机没有 mysqldump，跳过往返用例")

        connect().use { c ->
            c.setup()
            c.exec("DROP VIEW IF EXISTS vpaid")
            c.exec("DROP TABLE IF EXISTS dumpme")
            c.exec(
                "CREATE TABLE dumpme (id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(32) NOT NULL, " +
                    "price DECIMAL(12,2), note VARCHAR(64), created DATE, flag TINYINT, " +
                    "payload BLOB, status ENUM('new','paid'), KEY idx_price (price))",
            )
            c.exec(
                "INSERT INTO dumpme (name, price, note, created, flag, payload, status) VALUES " +
                    "('中文商品', 12.34, NULL, '2026-01-02', 1, 'bin', 'new'), " +
                    "('quote', -0.01, 'back\\\\slash', '2026-12-31', 0, NULL, 'paid')",
            )
            c.exec("CREATE VIEW vpaid AS SELECT id, name FROM dumpme WHERE status = 'paid'")

            val dump = File.createTempFile("yangbase-dump", ".sql")
            val p = ProcessBuilder(
                tool!!.absolutePath, "-h", "127.0.0.1", "-P", "$port", "-u", "root",
                "--databases", "dc",
            ).redirectOutput(dump).redirectErrorStream(false).start()
            assertTrue(p.waitFor(120, java.util.concurrent.TimeUnit.SECONDS), "mysqldump 超时")
            assertEquals(0, p.exitValue(), "mysqldump 失败：${dump.readText().takeLast(400)}")

            // 灌进一个**全新实例**：这才叫往返（同一个实例会读到自己的缓存与内存表）
            val dir2 = Files.createTempDirectory("yangbase-dumprestore")
            val server2 = YangBaseServer.forTesting(
                store = com.cloudea.yangbase.objectstore.LocalFileObjectStore(dir2), port = 0,
            )
            server2.start()
            try {
                val restore = ProcessBuilder(
                    "mysql", "-h", "127.0.0.1", "-P", "${server2.boundPort}", "-u", "root",
                ).redirectInput(dump).redirectErrorStream(true).start()
                val out = restore.inputStream.readBytes().decodeToString()
                assertTrue(restore.waitFor(120, java.util.concurrent.TimeUnit.SECONDS), "恢复超时")
                assertEquals(0, restore.exitValue(), "恢复失败：$out")

                DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1:${server2.boundPort}/dc?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                    "root", "",
                ).use { c2 ->
                    // 数据、类型、NULL、中文、转义、视图、索引——逐项对照
                    val before = c.grid(
                        "SELECT id,name,price,note,created,flag,payload,status FROM dumpme ORDER BY id",
                    ).map { it.map { v -> v?.toString() } }
                    val after = c2.grid(
                        "SELECT id,name,price,note,created,flag,payload,status FROM dumpme ORDER BY id",
                    ).map { it.map { v -> v?.toString() } }
                    assertEquals(before, after, "往返之后每一行、每一个字段都必须一致")

                    val ddl = c2.grid("SHOW CREATE TABLE dumpme").first()[1].toString()
                    assertTrue(ddl.contains("PRIMARY KEY"), "主键必须重建：$ddl")
                    assertTrue(ddl.contains("idx_price"), "二级索引必须重建：$ddl")
                    assertEquals(
                        "VIEW",
                        c2.grid("SHOW FULL TABLES").first { it[0].toString() == "vpaid" }[1].toString(),
                        "视图必须重建",
                    )
                    assertEquals(1L, (c2.value("SELECT COUNT(*) FROM vpaid") as Number).toLong())
                }
            } finally {
                runCatching { server2.stop() }
                runCatching { dir2.toFile().deleteRecursively() }
                runCatching { dump.delete() }
            }
        }
    }
}
