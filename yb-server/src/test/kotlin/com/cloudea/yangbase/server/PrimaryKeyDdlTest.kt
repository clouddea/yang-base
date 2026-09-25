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
 * **主键的两种写法必须等价**：列级 `id INT PRIMARY KEY` 与表级 `PRIMARY KEY (id)`。
 *
 * ## 为什么这个文件必须存在
 *
 * 列级写法是**最常见的建表写法**，而它曾经被解析出来又丢掉：`ColumnAst.primaryKey`
 * 这个字段在 AST 里躺着、却没有任何人读。后果不是"少一个约束"，而是**整张表只留最后一行**：
 *
 *  - 主键为空 ⇒ `KeyEncoder.encodeKey` 对空列清单返回 `ByteArray(0)`；
 *  - 于是每一行都用**同一个 key** 写入；
 *  - LSM 的同 key 覆盖语义下，`INSERT INTO t VALUES (1,…),(2,…)` 之后只剩第二行；
 *  - 而 `COUNT(*)` 走的是写者维护的元数据计数，它**照常加 2**。
 *
 * 也就是说：扫描少一行、计数多一行、**两边都不报错**。这个缺陷进过一次审查、
 * 被列在"用了就会出事"那一档，然后**又回来了**——因为没有测试盯着列级写法
 * （当时的用例写的都是表级）。所以这里的断言刻意从"表级/列级结果一致"这个角度写，
 * 而不是分别去测两种写法的行为。
 */
class PrimaryKeyDdlTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-pkddl")
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

    private fun connect(): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/pkdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun <T> Connection.q(b: (Statement) -> T): T = createStatement().use(b)

    private fun Connection.exec(sql: String) = q { it.execute(sql) }

    private fun Connection.one(sql: String): Any? = q { s ->
        s.executeQuery(sql).use { rs -> if (rs.next()) rs.getObject(1) else null }
    }

    private fun Connection.num(sql: String): Long = (one(sql) as Number).toLong()

    private fun Connection.rows(sql: String): Int = q { s ->
        s.executeQuery(sql).use { rs -> generateSequence { if (rs.next()) 1 else null }.count() }
    }

    private fun Connection.setup() = q { s ->
        s.execute("CREATE DATABASE IF NOT EXISTS pkdb")
        s.execute("USE pkdb")
    }

    @Test
    fun `列级主键与表级主键行为一致`() {
        connect().use { c ->
            c.setup()
            c.exec("DROP TABLE IF EXISTS pk_col")
            c.exec("DROP TABLE IF EXISTS pk_tbl")
            // 同一个表结构的两种写法
            c.exec("CREATE TABLE pk_col (id INT PRIMARY KEY, v VARCHAR(8))")
            c.exec("CREATE TABLE pk_tbl (id INT, v VARCHAR(8), PRIMARY KEY (id))")

            for (t in listOf("pk_col", "pk_tbl")) {
                c.exec("INSERT INTO $t VALUES (1,'a'),(2,'b'),(3,'c')")
                assertEquals(3L, c.num("SELECT COUNT(*) FROM $t"), "$t: COUNT 必须是 3")
                assertEquals(3, c.rows("SELECT * FROM $t"), "$t: **扫描**也必须看到 3 行（空主键会让它只剩最后一行）")
                assertEquals(1L, c.num("SELECT COUNT(*) FROM $t WHERE id = 1"), "$t: 点查要能查到第一行")
            }

            // 主键约束真的生效：重复主键要报 1062，而不是静默覆盖
            val e = runCatching { c.exec("INSERT INTO pk_col VALUES (1,'dup')") }.exceptionOrNull()
            assertTrue(e is SQLException, "撞主键必须报错，实际：$e")
            assertEquals(3L, c.num("SELECT COUNT(*) FROM pk_col"))
        }
    }

    @Test
    fun `列级主键进 information_schema 与 SHOW CREATE TABLE`() {
        connect().use { c ->
            c.setup()
            c.exec("DROP TABLE IF EXISTS pk_meta")
            c.exec("CREATE TABLE pk_meta (id BIGINT PRIMARY KEY, v INT)")

            // 客户端 introspection 靠这两处判断"有没有主键"，缺了它们会当成无主键表。
            // 注意 `SHOW CREATE TABLE` 返回两列（表名, DDL），要取**第二列**
            val ddl = c.q { st ->
                st.executeQuery("SHOW CREATE TABLE pk_meta").use { rs ->
                    assertTrue(rs.next()); rs.getString(2)
                }
            }
            assertTrue(ddl.contains("PRIMARY KEY"), "SHOW CREATE TABLE 必须写出主键：$ddl")

            val keyName = c.one(
                "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE " +
                    "WHERE TABLE_NAME = 'pk_meta' AND CONSTRAINT_NAME = 'PRIMARY'",
            )
            assertEquals("id", keyName?.toString(), "information_schema 里必须有主键列")
        }
    }

    @Test
    fun `列级与表级主键同时出现时报错`() {
        connect().use { c ->
            c.setup()
            c.exec("DROP TABLE IF EXISTS pk_both")
            val e = runCatching {
                c.exec("CREATE TABLE pk_both (id INT PRIMARY KEY, v INT, PRIMARY KEY (id))")
            }.exceptionOrNull()
            // MySQL 也是错误（multiple primary key defined）；静默挑一个会让另一处语义消失
            assertTrue(e is SQLException, "同时声明两个主键必须报错，实际：$e")
        }
    }

    @Test
    fun `列级主键隐含 NOT NULL`() {
        connect().use { c ->
            c.setup()
            c.exec("DROP TABLE IF EXISTS pk_nn")
            c.exec("CREATE TABLE pk_nn (id INT PRIMARY KEY, v INT)")
            val e = runCatching { c.exec("INSERT INTO pk_nn (id, v) VALUES (NULL, 1)") }.exceptionOrNull()
            assertTrue(e is SQLException, "主键列不接受 NULL，实际：$e")
        }
    }
}
