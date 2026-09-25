package com.cloudea.yangbase.server

import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.LocalFileObjectStore
import com.cloudea.yangbase.storage.DiskCache
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 二级缓存（本地 NVMe，设计文档 §6.7）。
 *
 * 它要解决的问题很具体：`BlockCache` 是**内存**缓存，CN 一重启就全空，
 * 于是重建之后的一段时间里所有读都打回对象存储。加上本地 NVMe 之后，
 * 这段「重新变热」的过程不产生任何对象存储请求。
 *
 * **省的是请求数，不是延迟**——延迟只是顺带。所以断言打在请求数上。
 */
class DiskCacheTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var cacheDir: java.nio.file.Path
    private lateinit var counting: CountingObjectStore
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-dc")
        cacheDir = Files.createTempDirectory("yangbase-dc-cache")
        counting = CountingObjectStore(LocalFileObjectStore(dataDir))
        server = startServer()
        Class.forName("com.mysql.cj.jdbc.Driver")
    }

    @AfterTest
    fun tearDown() {
        runCatching { server.stop() }
        runCatching { dataDir.toFile().deleteRecursively() }
        runCatching { cacheDir.toFile().deleteRecursively() }
    }

    private fun startServer(): YangBaseServer {
        val s = YangBaseServer.forTesting(
            store = counting, port = 0, diskCacheDir = cacheDir.toString(),
        )
        s.start()
        port = s.boundPort
        return s
    }

    private fun connect(): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/dcdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun Connection.st(block: (Statement) -> Unit) = createStatement().use(block)

    private fun <T> Connection.q(block: (Statement) -> T): T = createStatement().use(block)

    private fun Statement.query(sql: String): List<List<Any?>> =
        executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            val out = ArrayList<List<Any?>>()
            while (rs.next()) out.add((1..n).map { rs.getObject(it) })
            out
        }

    private fun seed(rows: Int) {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS dcdb")
                s.execute("USE dcdb")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute("CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(64), PRIMARY KEY (id))")
                (1..rows step 200).forEach { start ->
                    val values = (start until minOf(start + 200, rows + 1))
                        .joinToString(",") { "($it,'value-$it')" }
                    s.execute("INSERT INTO t VALUES $values")
                }
                s.query("SELECT COUNT(*) FROM t")
            }
        }
        runBlocking { server.engine.flushAll() }
    }

    // ── 基本行为 ──────────────────────────────────────────────────

    @Test
    fun `缓存目录会被真正写入`() {
        seed(500)
        connect().use { c ->
            c.st { s ->
                s.execute("USE dcdb")
                // 读数据块，触发二级缓存写入
                repeat(5) { s.query("SELECT v FROM t WHERE id = ${it * 50 + 1}") }
            }
        }
        val files = Files.walk(cacheDir).filter { Files.isRegularFile(it) }.count()
        assertTrue(files > 0, "二级缓存目录里应当有文件，实际为空")
    }

    // ── 核心价值：重启后不产生请求 ────────────────────────────────

    @Test
    fun `重启后读取不产生对象存储请求`() {
        seed(1000)
        // 第一次：把要用的块读进二级缓存
        connect().use { c ->
            c.st { s ->
                s.execute("USE dcdb")
                repeat(20) { s.query("SELECT v FROM t WHERE id = ${it * 40 + 1}") }
            }
        }

        // 重启 CN：内存缓存全空，但二级缓存还在
        server.stop()
        server = startServer()

        connect().use { c ->
            c.st { s ->
                s.execute("USE dcdb")
                counting.reset()
                // 与预热时**相同**的那些点查
                repeat(20) { s.query("SELECT v FROM t WHERE id = ${it * 40 + 1}") }
                val gets = counting.count("get_range")
                assertEquals(
                    0L, gets,
                    "重启后读同一批数据应命中二级缓存、0 次对象存储请求，实际 $gets 次：" +
                        counting.keyReport(),
                )
            }
        }
    }

    @Test
    fun `没有二级缓存时重启会产生请求 - 对照`() {
        seed(1000)
        connect().use { c ->
            c.st { s ->
                s.execute("USE dcdb")
                repeat(20) { s.query("SELECT v FROM t WHERE id = ${it * 40 + 1}") }
            }
        }

        // 换一个**不带**二级缓存的引擎
        server.stop()
        val plain = YangBaseServer.forTesting(store = counting, port = 0)
        plain.start()
        try {
            val c = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:${plain.boundPort}/dcdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "root", "",
            )
            c.use { cc ->
                cc.st { s -> s.execute("USE dcdb") }
                counting.reset()
                repeat(20) { i -> cc.st { s -> s.query("SELECT v FROM t WHERE id = ${i * 40 + 1}") } }
                assertTrue(
                    counting.count("get_range") > 0,
                    "没有二级缓存时重启**必然**要回对象存储取数据——" +
                        "这条对照断言说明上一条测的是二级缓存的效果，而不是别的什么在起作用",
                )
            }
        } finally {
            runCatching { plain.stop() }
        }
        server = startServer()
    }

    // ── 安全性：缓存必须是纯缓存 ──────────────────────────────────

    @Test
    fun `删掉缓存目录不影响正确性`() {
        seed(300)
        connect().use { c ->
            c.st { s ->
                s.execute("USE dcdb")
                repeat(10) { s.query("SELECT v FROM t WHERE id = ${it * 30 + 1}") }
            }
        }

        // 把缓存目录整个删掉——P1 要求本地的一切必须可丢弃
        runBlocking { server.engine.cache_.secondLevel?.clear() }
        server.stop()
        cacheDir.toFile().deleteRecursively()

        server = startServer()
        connect().use { c ->
            c.st { s ->
                s.execute("USE dcdb")
                // 结果必须完全正确，只是要多走几次对象存储
                repeat(10) {
                    assertEquals(
                        "value-${it * 30 + 1}",
                        s.query("SELECT v FROM t WHERE id = ${it * 30 + 1}")[0][0],
                    )
                }
                assertEquals(300L, s.query("SELECT COUNT(*) FROM t")[0][0])
            }
        }
    }

    @Test
    fun `缓存文件损坏时当作未命中而不是报错`() {
        seed(200)
        connect().use { c ->
            c.st { s ->
                s.execute("USE dcdb")
                s.query("SELECT v FROM t WHERE id = 1")
            }
        }
        // 把所有缓存文件的内容改成垃圾
        Files.walk(cacheDir).filter { Files.isRegularFile(it) }.forEach {
            runCatching { Files.write(it, "garbage".toByteArray()) }
        }

        server.stop()
        server = startServer()
        connect().use { c ->
            c.st { s ->
                s.execute("USE dcdb")
                // **一个缓存不该有能力让系统不可用**：坏了就当未命中，回对象存储取
                assertEquals("value-1", s.query("SELECT v FROM t WHERE id = 1")[0][0])
            }
        }
    }

    // ── 淘汰 ──────────────────────────────────────────────────────

    @Test
    fun `容量超限时淘汰且不影响正确性`() {
        // 容量设得极小，强制触发淘汰
        val tiny = Files.createTempDirectory("yangbase-dc-tiny")
        try {
            val dc = DiskCache(tiny, maxBytes = 8 * 1024)
            runBlocking {
                repeat(50) { i ->
                    dc.put("block-$i", ByteArray(1024) { (i and 0xFF).toByte() })
                }
                val st = dc.stats()
                assertTrue(st.evictions > 0, "超出容量后应发生淘汰，实际 ${st.evictions} 次")
                assertTrue(
                    st.bytes <= 8 * 1024,
                    "淘汰后占用应回到上限之下，实际 ${st.bytes} 字节",
                )
                // 最近写入的必须还在
                assertTrue(dc.get("block-49") != null, "最近写入的块应当仍在缓存里")
            }
        } finally {
            tiny.toFile().deleteRecursively()
        }
    }

    @Test
    fun `只缓存不可变对象 - 可变的键不写入二级缓存`() {
        val dc = DiskCache(cacheDir, maxBytes = 1024 * 1024)
        val cache = com.cloudea.yangbase.storage.BlockCache(1024 * 1024)
        cache.secondLevel = dc
        runBlocking {
            // 可变键（如 manifest/CURRENT）：只进内存
            cache.getOrLoad("manifest/CURRENT", immutableKey = false) { "v1".toByteArray() }
            assertTrue(dc.get("manifest/CURRENT") == null, "内容会变的键绝不能进二级缓存")

            // 不可变键（SST 块）：进二级缓存
            cache.getOrLoad("blk:1001:L0:42", immutableKey = true) { "data".toByteArray() }
            assertTrue(dc.get("blk:1001:L0:42") != null, "不可变块应当进二级缓存")
        }
    }
}
