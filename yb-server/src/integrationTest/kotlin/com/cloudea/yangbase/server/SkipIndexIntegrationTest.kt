package com.cloudea.yangbase.server

import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.EmbeddedS3Server
import com.cloudea.yangbase.objectstore.S3ObjectStore
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 跳数索引的端到端测试（设计文档 §8.5）。
 *
 * 在此之前，跳数索引的构件是齐的、单元测试是绿的，但它**从来没有被写过、
 * 也从来没有被用过**——`SkipIndexBuilder` 没人调用，`BlockPruner` 没人问。
 * 那种状态比没有实现更糟：它有测试、有文档、看起来是完成的，却对系统行为零影响。
 *
 * 这个文件验证的是**接线**：建索引 → 写 SST 时顺带建 `.skp` → 扫描时用它跳过块。
 * 断言打在**读了多少字节**上——延迟受环境影响，读的字节数是确定性的。
 */
@org.junit.jupiter.api.Tag("integration")
class SkipIndexIntegrationTest {

    private var s3: EmbeddedS3Server? = null
    private lateinit var server: YangBaseServer
    private lateinit var counting: CountingObjectStore
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        val emb = EmbeddedS3Server("yangbase-skip")
        emb.start()
        s3 = emb
        val raw = S3ObjectStore.create(
            bucket = "yangbase-skip", endpoint = emb.endpoint,
            region = "us-east-1", accessKey = "t", secretKey = "t",
        )
        counting = CountingObjectStore(raw, recordKeys = true)
        server = YangBaseServer.forTesting(store = counting, port = 0)
        server.start()
        port = server.boundPort
        Class.forName("com.mysql.cj.jdbc.Driver")
    }

    @AfterTest
    fun tearDown() {
        runCatching { server.stop() }
        s3?.let { runCatching { it.stop() } }
    }

    private fun connect(): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/skipdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
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

    /**
     * 造一张数据**按 ts 有序**的表——这是 minmax 能剪枝的前提。
     * 数据若完全乱序，每个块的 min/max 都会覆盖全域，索引就退化为「永远剪不掉」。
     */
    private fun seed(withSkip: Boolean) {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS skipdb")
                s.execute("USE skipdb")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute(
                    """
                    CREATE TABLE t (
                        id BIGINT NOT NULL,
                        ts BIGINT NOT NULL,
                        payload VARCHAR(64),
                        PRIMARY KEY (id)
                        ${if (withSkip) ", KEY idx_ts (ts) USING MINMAX" else ""}
                    )
                    """.trimIndent()
                )
                // 5000 行，ts 单调递增 → 每个块的时间范围很窄
                (1..5000 step 500).forEach { start ->
                    val values = (start until minOf(start + 500, 5001))
                        .joinToString(",") { "($it, ${it * 10}, 'payload-$it')" }
                    s.execute("INSERT INTO t VALUES $values")
                }
                s.query("SELECT COUNT(*) FROM t")
            }
        }
        runBlocking { server.engine.flushAll() }
    }

    // ── 写路径 ────────────────────────────────────────────────────

    @Test
    fun `建了跳数索引后 flush 会写出 skp 对象`() {
        seed(withSkip = true)
        val skp = runBlocking { counting.listAll("index/") }.filter { it.contains("/skip/") || it.contains(".skp") }
        assertTrue(
            skp.isNotEmpty(),
            "配了跳数索引的表在 flush 时必须写出 .skp，实际一个都没有：" +
                runBlocking { counting.listAll("index/") }.take(10).toString(),
        )
    }

    @Test
    fun `没建跳数索引时不写 skp - 零开销`() {
        seed(withSkip = false)
        val skp = runBlocking { counting.listAll("index/") }.filter { it.contains("/skip/") || it.contains(".skp") }
        assertTrue(skp.isEmpty(), "没有跳数索引的表不该产生任何 .skp 对象，实际 ${skp.size} 个")
    }

    // ── 读路径 ────────────────────────────────────────────────────

    /**
     * ⚠️ 必须在**零缓存**的引擎上测：缓存一旦生效，两条路径都变成 0 字节，
     * 「谁读得更少」就退化成 0 与 0 的比较。这类断言看起来在测性能，
     * 实际什么都没测——比不写更糟，因为它给出虚假的安全感。
     */
    private fun measureColdCached(sql: String): Long {
        val cold = YangBaseServer.forTesting(store = counting, port = 0, cacheBytes = 0)
        cold.start()
        return try {
            val c = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:${cold.boundPort}/skipdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "root", "",
            )
            c.use {
                it.st { s -> s.execute("USE skipdb") }
                counting.reset()
                it.q { s -> s.query(sql) }
                counting.bytesOut("get_range") + counting.bytesOut("get")
            }
        } finally {
            runCatching { cold.stop() }
        }
    }

    /**
     * ⚠️ 区间必须落在**数据末尾**才测得出剪枝。
     *
     * 数据是 `ts = id * 10` 单调递增的，一个 64KB 的块装得下近千行。
     * 查 `ts ∈ [1000,1100)` 只覆盖第一个块的一小段——**没有任何一个完整的块
     * 可以被排除**，剪枝为 0 是正确行为，不是 bug。我第一版就是这么写错的。
     */
    private val tailRange = "SELECT id FROM t WHERE ts >= 48000 AND ts < 48100"

    @Test
    fun `跳数索引让窄区间查询读得更少`() {
        seed(withSkip = true)
        val narrow = measureColdCached(tailRange)
        val wide = measureColdCached("SELECT id FROM t")
        assertTrue(
            narrow < wide,
            "末尾窄区间(ts 48000..48100)读的字节($narrow)应少于全表($wide)——否则剪枝没有生效",
        )
    }

    @Test
    fun `谓词无法下推时读满全表 - 对照`() {
        // 对照说明上一条测的是**剪枝**，而不是别的什么在起作用：
        // `ts + 0` 让谓词无法下推到块级，同样是那个区间，就得读满全表
        seed(withSkip = true)
        val pushable = measureColdCached(tailRange)
        val blocked = measureColdCached("SELECT id FROM t WHERE ts + 0 >= 48000 AND ts + 0 < 48100")
        assertTrue(
            blocked > pushable,
            "不可下推的谓词($blocked 字节)应当比可下推的($pushable 字节)读得多——" +
                "否则说明剪枝根本没参与，前面的断言是别的原因造成的",
        )
    }

    @Test
    fun `剪枝结果与全表扫描一致 - 绝不漏行`() {
        seed(withSkip = true)
        connect().use { c ->
            c.st { s ->
                s.execute("USE skipdb")
                // 属性测试：随机取若干区间，与「禁用剪枝」的结果对比。
                // 剪枝裁错是**静默漏行**，所以每一条边界都要对一遍
                val rnd = java.util.Random(7)
                repeat(30) {
                    val lo = rnd.nextInt(45000).toLong()
                    val hi = lo + rnd.nextInt(3000)
                    val viaSkip = s.query("SELECT id FROM t WHERE ts >= $lo AND ts < $hi")
                        .map { it[0] as Long }.sorted()
                    // `ts + 0` 让谓词无法下推 → 强制走全表逐行过滤
                    val viaScan = s.query("SELECT id FROM t WHERE ts + 0 >= $lo AND ts + 0 < $hi")
                        .map { it[0] as Long }.sorted()
                    assertEquals(viaScan, viaSkip, "区间 [$lo, $hi) 上剪枝结果必须与全扫一致")
                }
            }
        }
    }

    /**
     * ★ 剪枝的字面量必须按**列类型**解释——与过滤用同一套解释。
     *
     * `ts` 是 BIGINT（块边界按整数编码），而 `'48000'` 是字符串字面量。
     * 过滤路径（`Eval.evalBinary`）会按列类型把 `'48000'` 转成 48000，能匹配到行；
     * 剪枝路径若不转，就会拿字符串的编码去和整数的边界比大小——比不过，
     * 于是**整块被跳过**。两条路径各算各的，查询静默返回 0 行。
     *
     * 这也是 JDBC 最常见的形状：参数按字符串绑定时就是这个样子。
     */
    @Test
    fun `类型不匹配的字面量也要按列类型剪枝`() {
        seed(withSkip = true)
        connect().use { c ->
            c.st { s -> s.execute("USE skipdb") }
            // ⚠️ 三种形状都要测，因为错误的编码**只在某些方向**才会剪掉行：
            // 字符串编码排在所有整数编码之前，于是它作为**下界**只是"更宽松"（不剪），
            // 作为**上界或等值**才会把整块判否（剪光）。
            // 而同一个列上只保留第一条约束，所以 `>= 'a' AND < 'b'` 恰好落在安全的那一侧
            fun both(typedSql: String, quotedSql: String, why: String) {
                val typed = c.q { s -> s.query(typedSql).map { it[0] } }
                val quoted = c.q { s -> s.query(quotedSql).map { it[0] } }
                assertTrue(typed.isNotEmpty(), "先确认 $typedSql 确实有行，否则比较没有意义")
                assertEquals(typed, quoted, why)
            }
            both(
                "SELECT id FROM t WHERE ts = 48000",
                "SELECT id FROM t WHERE ts = '48000'",
                "等值的字符串字面量把整个块判否 → 行静默消失",
            )
            both(
                "SELECT id FROM t WHERE ts < 1000",
                "SELECT id FROM t WHERE ts < '1000'",
                "上界的字符串字面量把所有块判否 → 行静默消失",
            )
            both(
                "SELECT id FROM t WHERE ts BETWEEN 48000 AND 48100",
                "SELECT id FROM t WHERE ts BETWEEN '48000' AND '48100'",
                "BETWEEN 的上界同理",
            )
            // 负数边界：负号在解析期折进字面量，它才成为一个可下推的 Literal。
            // 保序编码里负数排在正数**之前**，编码写错会让"低下界"变成"全剪掉"
            both(
                "SELECT id FROM t WHERE ts >= -1 AND ts < 100",
                "SELECT id FROM t WHERE ts + 0 >= -1 AND ts + 0 < 100",
                "负数下界：剪枝与全扫必须一致（`ts + 0` 让谓词无法下推）",
            )
            // 上界是负数：所有块都该被判否，结果必须与全扫一致（都是 0 行）
            assertEquals(
                0,
                c.q { s -> s.query("SELECT id FROM t WHERE ts < -1").size },
                "ts 全为正数，上界 -1 应当剪掉所有块且不返回任何行",
            )
        }
    }

    @Test
    fun `区间外查询读到零个块`() {
        seed(withSkip = true)
        connect().use { c ->
            c.st { s -> s.execute("USE skipdb"); s.query("SELECT id FROM t WHERE ts >= 0 AND ts < 10") }
        }
        counting.reset()
        val n = connect().use { c ->
            c.st { s -> s.execute("USE skipdb") }
            c.q { s -> s.query("SELECT id FROM t WHERE ts >= 99999990 AND ts < 99999999").size }
        }
        assertEquals(0, n)
        // 全表 ts 最大 50000；查 99999990 以上应当每个块都被 minmax 判否
        assertEquals(
            0L, counting.bytesOut("get_range"),
            "所有块都被判否时不应读任何数据块，实际读了 ${counting.bytesOut("get_range")} 字节：" +
                counting.keyReport(),
        )
    }
}
