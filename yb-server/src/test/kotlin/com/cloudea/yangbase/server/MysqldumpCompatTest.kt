package com.cloudea.yangbase.server

import java.io.DataInputStream
import java.io.File
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

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

    /**
     * 用原始 socket 做一次「服务器会不会主动打招呼」的探针。
     *
     * 「端口在监听」与「能服务」是两件事：`ServerSocket.bind` 成功之后、
     * accept 循环真正跑起来之前，内核照样会替你把连接排进 backlog。
     * 客户端连上去、然后**等一个永远不来的问候包**——这正是 CI 上
     * 「0 输出、120s 不退出」的样子，而本地因为机器快，窗口太窄从不复现。
     *
     * 把这条探针放在启动 `mysql` 客户端之前，是为了在客户端挂住的那一刻
     * **能分清责任方**：探针过了 → 服务端是好的，问题在客户端侧；
     * 探针超时 → 就是服务端没准备好，这件事与测试无关，是产品缺陷。
     */
    private fun probeGreeting(port: Int, timeoutMs: Int = 5_000): String {
        Socket().use { s ->
            s.soTimeout = timeoutMs
            s.connect(InetSocketAddress("127.0.0.1", port), timeoutMs)
            val ins = DataInputStream(s.getInputStream())
            val lenBytes = ByteArray(3).also { ins.readFully(it) }
            val len = (lenBytes[0].toInt() and 0xFF) or
                ((lenBytes[1].toInt() and 0xFF) shl 8) or
                ((lenBytes[2].toInt() and 0xFF) shl 16)
            ins.readUnsignedByte() // sequence id，首包恒为 0
            val payload = ByteArray(len).also { ins.readFully(it) }
            check(payload.isNotEmpty() && payload[0].toInt() == 10) {
                "首包不是 Protocol::Handshake（期望 0x0a，实得 " +
                    (payload.firstOrNull()?.let { "0x%02x".format(it) } ?: "空包") + "）"
            }
            val nul = payload.indexOfFirst { it == 0.toByte() }
            val version = if (nul > 1) String(payload, 1, nul - 1, Charsets.US_ASCII) else ""
            check(version.isNotEmpty()) { "问候包里的 server version 是空的" }
            return version
        }
    }

    /**
     * 超时那一刻把 JVM 里的**虚拟线程**也 dump 出来。
     *
     * 连接处理走的是虚拟线程（`Thread.getAllStackTraces()` 看不见它们），
     * 而 yb-mysql 的每条连接正好是一个虚拟线程。没有这份转储，
     * 「客户端在等服务器」与「服务器卡在自己的某一行」只能靠猜。
     */
    private fun threadDump(): String = runCatching {
        val jcmd = File(System.getProperty("java.home"), "bin/jcmd")
        if (!jcmd.canExecute()) return@runCatching "<找不到 jcmd，跳过线程转储>"
        // ⚠️ `Thread.dump_to_file` 拒绝覆盖已存在的文件（用 CREATE_NEW 打开），
        // 而 `File.createTempFile` 恰恰先把文件建出来了——上一版因此拿到一份空转储，
        // 报错信息里那一节是空的。这里改成「新目录 + 尚不存在的文件名」。
        val dir = Files.createTempDirectory("yangbase-tdump")
        val out = File(dir.toFile(), "threads.txt")
        val p = ProcessBuilder(
            jcmd.absolutePath,
            ProcessHandle.current().pid().toString(),
            "Thread.dump_to_file",
            "-format=text",
            out.absolutePath,
        ).redirectErrorStream(true).start()
        p.waitFor(20, TimeUnit.SECONDS)
        val raw = runCatching { out.readText() }.getOrDefault("<读不到转储>")
        dir.toFile().deleteRecursively()
        if (raw.isBlank()) return@runCatching "<jcmd 产出了空文件：Thread.dump_to_file 可能拒绝写目标路径>"
        // 只留与我们有关的线程：截断末尾 N 个字符会把 yb-conn 那一段切掉（第一版就切掉了）。
        val blocks = raw.split(Regex("(?m)^#\\d+ \""))
        val keep = blocks.filter { b ->
            listOf("yb-conn", "yb-mysql", "Atomically", "ForkJoin", "Test worker", "yb-shard").any { b.contains(it) }
        }
        if (keep.isEmpty()) raw.takeLast(8_000) else keep.joinToString("\n\n") { "#\"" + it }

    }.getOrElse { "<线程转储失败：${it::class.simpleName}: ${it.message}>" }

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

        // 客户端也要有：mysqldump 在而 mysql 不在时，恢复那一段会直接抛 IOException
        val mysqlClient = listOf(File("/usr/bin/mysql"), File("/usr/local/bin/mysql"), File("/usr/bin/mariadb"))
            .firstOrNull { it.canExecute() }
        org.junit.jupiter.api.Assumptions.assumeTrue(mysqlClient != null, "本机没有 mysql 客户端，跳过往返用例")

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
            // 协议级 trace：只在这个用例里打开。
            // ⚠️ 光靠 stderr 不行：Gradle 默认不把测试进程的 stderr 打进 CI 日志
            // （断言消息会显示，println/stderr 不会）。所以这里把 System.err 复制一份到文件，
            // 失败时连同 trace 一起塞进断言消息——客户端挂住时，"服务端收到了哪一步"
            // 是唯一能定位的线索。
            val serverTrace = File.createTempFile("yangbase-proto-trace", ".log")
            val realErr = System.err
            runCatching {
                System.setErr(
                    java.io.PrintStream(
                        object : java.io.OutputStream() {
                            private val a = java.io.FileOutputStream(serverTrace, true)
                            override fun write(b: Int) { realErr.write(b); a.write(b) }
                            override fun write(b: ByteArray, off: Int, len: Int) {
                                realErr.write(b, off, len); a.write(b, off, len)
                            }
                            override fun flush() { realErr.flush(); a.flush() }
                        },
                        true,
                    ),
                )
            }
            System.setProperty("yb.mysql.trace", "1")
            server2.start()
            // 「先证明服务端能服务，再让客户端上」：探针失败时立刻断在这里，
            // 报错指向服务端；探针通过而客户端仍挂 → 责任方是客户端侧。
            val greeting = probeGreeting(server2.boundPort)

            // 冒烟：先用同一个客户端跑一条 `SELECT 1`。
            // 这一步把「连不上」与「灌数据时卡住」分开——
            // 前者是握手/协议问题，后者是某条 SQL 或结果处理的问题。
            run {
                val smokeOut = File.createTempFile("yangbase-smoke", ".log")
                val smoke = ProcessBuilder(
                    mysqlClient!!.absolutePath, "-h", "127.0.0.1", "-P", "${server2.boundPort}", "-u", "root",
                    "--password=", "--connect-timeout=10", "--protocol=TCP", "--unbuffered",
                    "-e", "SELECT 1",
                ).redirectOutput(smokeOut).redirectErrorStream(true).start()
                val finished = smoke.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)
                val smokeText = runCatching { smokeOut.readText() }.getOrDefault("")
                smokeOut.delete()
                if (!finished) runCatching { smoke.destroyForcibly() }
                assertTrue(
                    finished && smoke.exitValue() == 0,
                    "客户端连不上 server2（SELECT 1 冒烟失败）：" +
                        if (finished) "退出码 ${smoke.exitValue()}，输出：$smokeText"
                        else "30s 内没有退出，输出：$smokeText",
                )
            }
            val restoreOut = File.createTempFile("yangbase-restore", ".log")
            try {
                val restore = ProcessBuilder(
                    mysqlClient!!.absolutePath, "-h", "127.0.0.1", "-P", "${server2.boundPort}", "-u", "root",
                    // 显式空密码：不传 -p 时客户端可能反过来**交互式**索要密码，
                    // 而它从 /dev/tty 读，所以把 dump 喂给 stdin 也解救不了它——
                    // CI 上就是这样 0 输出、120s 不退出。
                    "--password=",
                    // 连不上时 10s 内快速失败并打印原因，而不是挂到测试超时。
                    "--connect-timeout=10",
                    // 强制 TCP：不依赖 socket 路径探测，`-h 127.0.0.1` 的行为才确定。
                    "--protocol=TCP",
                    // 逐条打印语句，并且**每条都落到磁盘**。少了 `--unbuffered`，
                    // 输出会卡在 stdio 的 4KB 缓冲里，进程被 kill 时一个字也留不下——
                    // 上一版就是这么拿到「0 字符输出」的。
                    "--verbose",
                    "--unbuffered",
                )
                    // ⚠️ 输出必须落到**文件**，不能用管道 + readBytes()：
                    // readBytes() 会阻塞到子进程关掉 stdout，且**没有任何超时**。
                    // 一旦 mysql 客户端卡在等服务器响应，测试就永远不返回——
                    // 2026-10-26 在 CI 上就这么挂了 45 分钟直到 job 超时（本地有构建缓存，没跑到）。
                    .redirectInput(dump)
                    .redirectOutput(restoreOut)
                    .redirectErrorStream(true)
                    .start()
                // 超时那一刻，客户端的输出是**唯一**的线索：直接断言失败的话，报告里
                // 只有一句「恢复超时」，看不出它是卡在等服务器响应、卡在某条 SQL 上，
                // 还是根本没连上。所以先强制结束子进程（否则它会把 stdout 文件继续占着
                // 写，读到的内容也不稳定），再把能拿到的东西全部塞进断言消息。
                if (!restore.waitFor(120, java.util.concurrent.TimeUnit.SECONDS)) {
                    runCatching { restore.destroyForcibly() }
                    runCatching { restore.waitFor(10, java.util.concurrent.TimeUnit.SECONDS) }
                    val partial = runCatching { restoreOut.readText() }.getOrDefault("<读取失败>")
                    fail(
                        buildString {
                            appendLine("恢复超时（120s）：mysql 客户端没有退出，已 destroyForcibly()")
                            appendLine("进程是否存活：${restore.isAlive}")
                            appendLine("dump 文件：${dump.absolutePath}（${dump.length()} 字节）")
                            appendLine("server2 握手探针：通过（server version = $greeting）→ 服务端能服务，问题在客户端侧")
                            appendLine("server2 活跃连接数：${server2.liveConnectionCount}")
                            appendLine()
                            appendLine("──── 服务端协议 trace（最后 6000 字符）────")
                            appendLine(runCatching { serverTrace.readText() }.getOrDefault("").takeLast(6_000))
                            appendLine("客户端输出（${partial.length} 字符，取末尾 4000）：")
                            appendLine(if (partial.isEmpty()) "<空：客户端一个字都没输出>" else partial.takeLast(4_000))
                            appendLine()
                            appendLine("──── 服务端线程转储（含虚拟线程，取末尾 12000 字符）────")
                            append(threadDump().takeLast(12_000))
                        },
                    )
                }
                val out = restoreOut.readText()
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
                System.clearProperty("yb.mysql.trace")
                runCatching { System.setErr(realErr) }
                runCatching { serverTrace.delete() }
                runCatching { server2.stop() }
                runCatching { dir2.toFile().deleteRecursively() }
                runCatching { dump.delete() }
                runCatching { restoreOut.delete() }
            }
        }
    }
}
