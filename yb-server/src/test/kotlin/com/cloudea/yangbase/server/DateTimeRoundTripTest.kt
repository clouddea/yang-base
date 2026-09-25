package com.cloudea.yangbase.server

import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * **时间值的往返必须稳定**：写进去什么，读出来就是什么。
 *
 * ## 为什么这条值得单独一个文件
 *
 * 时间字面量在系统里有两处解释它的地方，而它们是**两个不同的时区**：
 *
 * - 解析（`Datum.parseDateTime`）按 **UTC**；
 * - 渲染（`MysqlServer.textValue` / `writeBinaryDatetime`）曾经按
 *   `ZoneId.systemDefault()`。
 *
 * 两边不一致的后果不是报错，而是**静默漂移**：`INSERT '2026-02-01 10:00:00'` 之后
 * `SELECT` 出来的可能是 `2026-02-01 18:00:00`（服务器在 UTC+8 时差 8 小时）。
 * 只在"服务器时区不是 UTC"时出现，所以开发机上跑 UTC 镜像永远看不到。
 *
 * 这与设计文档里那条「**全系统只能有一个时间字面量解析器**」是同一类问题：
 * 同一个字符串在两处解出两个不同的时刻，`WHERE dt = '...'` 就会静默返回空集。
 * 谓词那条路已经统一到 UTC 了（见 `coerceTo`），**渲染这条路也必须统一**。
 *
 * ## 断言的是"往返"，不是"某个具体值"
 *
 * 刻意不写 `assertEquals("2026-02-01 10:00:00", ...)` 之外的时区假设——
 * 用例要在任何 `TZ` 下都通过，而它要钉住的性质就是"不漂移"。
 */
class DateTimeRoundTripTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-datetz")
        server = YangBaseServer(
            ServerConfig(
                host = "127.0.0.1", port = 0, nodeId = "tz",
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
        "jdbc:mysql://127.0.0.1:$port/tdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    @Test
    fun `DATETIME 写入后读出来必须一模一样`() {
        connect().use { c ->
            c.createStatement().use { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS tdb")
                s.execute("USE tdb")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute("CREATE TABLE t (id BIGINT NOT NULL, dt DATETIME, d DATE, PRIMARY KEY (id))")
                s.execute("INSERT INTO t VALUES (1, '2026-02-01 10:00:00', '2026-02-01')")
                s.executeQuery("SELECT dt, d FROM t WHERE id = 1").use { rs ->
                    rs.next()
                    assertEquals(
                        "2026-02-01 10:00:00", rs.getString(1),
                        "DATETIME 往返漂移了——解析按 UTC 而渲染按服务器时区（当前 TZ=${java.util.TimeZone.getDefault().id}）",
                    )
                    assertEquals("2026-02-01", rs.getString(2), "DATE 往返漂移了")
                }
            }
        }
    }

    /** 时间字面量的**比较**也必须与它自己一致（这条早就统一到 UTC 了，作为对照）。 */
    @Test
    fun `时间字面量的比较与往返用同一个时区`() {
        connect().use { c ->
            c.createStatement().use { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS tdb")
                s.execute("USE tdb")
                s.execute("DROP TABLE IF EXISTS t2")
                s.execute("CREATE TABLE t2 (id BIGINT NOT NULL, dt DATETIME, PRIMARY KEY (id))")
                s.execute("INSERT INTO t2 VALUES (1, '2026-02-01 10:00:00')")
                s.executeQuery("SELECT COUNT(*) FROM t2 WHERE dt = '2026-02-01 10:00:00'").use { rs ->
                    rs.next()
                    assertEquals(1L, rs.getLong(1), "写进去的那个字面量必须能被同一个字面量查出来")
                }
            }
        }
    }

    /** `NOW()` 与写进去的当前时刻也要一致。 */
    @Test
    fun `NOW 写入后读出来也是同一个时刻`() {
        connect().use { c ->
            c.createStatement().use { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS tdb")
                s.execute("USE tdb")
                s.execute("DROP TABLE IF EXISTS t3")
                s.execute("CREATE TABLE t3 (id BIGINT NOT NULL, dt DATETIME, PRIMARY KEY (id))")
                s.execute("INSERT INTO t3 VALUES (1, NOW())")
                s.executeQuery("SELECT dt FROM t3 WHERE id = 1").use { rs ->
                    rs.next()
                    val read = java.time.LocalDateTime.parse(
                        rs.getString(1).replace(' ', 'T'),
                    )
                    val nowUtc = java.time.LocalDateTime.now(java.time.ZoneOffset.UTC)
                    val diff = java.time.Duration.between(read, nowUtc).abs().toMinutes()
                    assertEquals(
                        true, diff < 5,
                        "NOW() 写进去又读出来的时刻应当就是现在（UTC）：读到 $read，UTC 现在 $nowUtc，" +
                            "差 $diff 分钟——说明写入与读取用了不同的时区",
                    )
                }
            }
        }
    }
}
