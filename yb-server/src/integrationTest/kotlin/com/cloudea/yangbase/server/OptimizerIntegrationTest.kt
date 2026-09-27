package com.cloudea.yangbase.server

import com.cloudea.yangbase.common.stats.NdvSource
import com.cloudea.yangbase.common.stats.TableStats
import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.EmbeddedS3Server
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.S3ObjectStore
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
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
 * **统计信息 + 基于代价的优化器 + EXPLAIN 的集成测试**（设计文档 §10.2、§10.3、§8.9.6）。
 *
 * 这里要证明的四件事，每一件都在别处无法验证：
 *
 *  1. `ANALYZE TABLE` 真的把统计对象写进了对象存储，且内容与表本身一致；
 *  2. `ANALYZE` **一个数据块都不读**（§13.3）——只碰常驻内存的 `.idx` 与 manifest；
 *  3. `EXPLAIN SELECT COUNT(*)` 报出 `METADATA_COUNT`（§8.9.6）——这是使用者
 *     能验证「计数走了 0 次请求那条路」的**唯一**手段；
 *  4. 统计信息**只影响计划，不影响结果**——这是代价模型唯一不可违反的不变量。
 *     选错路径只是慢，选错结果就没有意义了。
 *
 * 断言尽量落在**请求数**上（原则 P4）：延迟受环境影响，请求数是确定性的。
 */
@org.junit.jupiter.api.Tag("integration")
class OptimizerIntegrationTest {

    private lateinit var backend: IntegrationStore
    private lateinit var server: YangBaseServer
    private lateinit var counting: CountingObjectStore
    private var port: Int = 0

    private val json = Json { ignoreUnknownKeys = true }

    @BeforeTest
    fun setUp() {
        backend = IntegrationStore("yangbase-opt")
        val raw = backend.store
        // recordKeys 打开：断言失败时要能直接看到「这几次请求到底打在哪」，
        // 而不是对着一个数字猜
        counting = CountingObjectStore(raw, recordKeys = true)
        server = YangBaseServer.forTesting(store = counting, allowNonAtomicPointerSwitch = backend.oss, port = 0)
        server.start()
        port = server.boundPort
        Class.forName("com.mysql.cj.jdbc.Driver")
    }

    @AfterTest
    fun tearDown() {
        runCatching { server.stop() }
        if (::backend.isInitialized) runCatching { backend.close() }
    }

    // ── 夹具 ──────────────────────────────────────────────────────

    private fun connect(): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/optdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
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

    /** 按**列名**取结果——`EXPLAIN` 的契约就是这些名字，按位置写测试会让契约悄悄漂移。 */
    private fun Statement.queryNamed(sql: String): List<Map<String, Any?>> =
        executeQuery(sql).use { rs ->
            val names = (1..rs.metaData.columnCount).map { rs.metaData.getColumnLabel(it) }
            val out = ArrayList<Map<String, Any?>>()
            while (rs.next()) out.add(names.withIndex().associate { (i, n) -> n to rs.getObject(i + 1) })
            out
        }

    private fun Statement.explain(sql: String): Map<String, Any?> {
        val rows = queryNamed("EXPLAIN $sql")
        assertEquals(1, rows.size, "EXPLAIN 应恰好返回一行")
        return rows[0]
    }

    private fun explainType(c: Connection, sql: String): String =
        c.q { s -> s.explain(sql)["type"] as String }

    /** 建表 + 灌数据，然后落盘——否则一切都在内存里，测不出 IO 也测不出统计。 */
    private fun setup(c: Connection, rows: Int = 2000, ddl: String = DDL) {
        c.st { s ->
            s.execute("CREATE DATABASE IF NOT EXISTS optdb")
            s.execute("USE optdb")
            s.execute("DROP TABLE IF EXISTS t")
            s.execute(ddl)
        }
        if (rows > 0) {
            c.st { s ->
                (1..rows step 500).forEach { start ->
                    val values = (start until minOf(start + 500, rows + 1))
                        .joinToString(",") { "($it, 'u$it', ${it % 50}, 'c${it % 7}')" }
                    s.execute("INSERT INTO t VALUES $values")
                }
            }
        }
        flush()
    }

    private fun flush() = runBlocking { server.engine.flushAll() }

    private fun statsKeys(): List<String> = runBlocking { server.engine.store.listAll("stats/") }

    private fun analyze(c: Connection, table: String = "t"): List<Map<String, Any?>> =
        c.q { s -> s.queryNamed("ANALYZE TABLE $table") }

    // ── 1. 统计对象存在且内容可信 ─────────────────────────────────

    @Test
    fun `ANALYZE 写出统计对象 - 内容与表一致且体积很小`() {
        connect().use { c ->
            setup(c, rows = 2000)

            val before = statsKeys()
            assertTrue(before.isEmpty(), "ANALYZE 之前不该有统计对象：$before")

            val result = analyze(c)
            assertEquals(
                listOf("Table", "Op", "Msg_type", "Msg_text"),
                result[0].keys.toList(),
                "结果集形状应与 MySQL 的 ANALYZE TABLE 一致",
            )
            val status = result.first { it["Msg_type"] == "status" }["Msg_text"] as String
            assertTrue(status.contains("统计版本 v"), status)
            assertTrue(status.contains("0 次数据块读"), "摘要里必须写明它没读数据块：$status")

            // 两个对象：一个表级（规划路径读的就是它）+ 每个分区一个列级
            val keys = statsKeys()
            val tableKey = keys.single { it.endsWith(".json") && it.contains("/partitions.v") }
            val colKeys = keys.filter { it.contains("/colstats.v") }
            val tableId = server.engine.catalog.allTables().single { it.name == "t" }.tableId
            assertEquals("stats/table/$tableId/partitions.v000000000002.json", tableKey)
            assertEquals(listOf("stats/table/$tableId/0/colstats.v000000000002.json"), colKeys)

            // 内容必须与表本身一致——统计错了比没有统计更糟（优化器会信它）
            val text = String(runBlocking { server.engine.store.get(tableKey) }, Charsets.UTF_8)
            val stats = json.decodeFromString(TableStats.serializer(), text)
            assertEquals(2000L, stats.rowCount)
            assertTrue(stats.rowCountExact, "行数来自 manifest 的精确计数，必须标为精确")
            assertEquals(2L, stats.statsVersion, "统计版本 = ANALYZE 后的表元数据版本（DDL 自动失效靠它）")
            assertEquals(2000L, stats.partitions.sumOf { it.flushedRows }, "落盘行数应与 .idx 一致")
            assertTrue(stats.blockCount > 0, "块数必须真的数出来了——它是「行数→请求数」的换算系数")

            val id = assertNotNull(stats.column("id"), "主键列必须有统计")
            assertEquals("1", id.min)
            assertEquals("2000", id.max)
            assertEquals(0L, id.nullCount)

            // 低基数列（7 个取值）的 ndv 是**精确**的：块级 valueCounts 的并集
            val city = assertNotNull(stats.column("city"))
            assertEquals(7L, city.ndv, "city 有 7 个不同取值")
            assertTrue(city.ndvExact, "块级值计数的并集是精确的")
            assertEquals(NdvSource.VALUES, city.ndvSource)

            // 数值列（INT，不收集 valueCounts）只有上界——这一点必须如实标注
            val n = assertNotNull(stats.column("n"))
            assertEquals(NdvSource.RANGE, n.ndvSource)
            assertTrue(!n.ndvExact, "上界不能被标成精确值")
            assertTrue(n.ndv >= 50, "`it % 50` 有 50 个取值，上界不该小于它，实际 ${n.ndv}")
            val hist = assertNotNull(n.histogram, "数值列应带直方图（范围选择率的唯一来源）")
            assertEquals(n.observedRows, hist.totalCount, "直方图总行数必须等于观测到的非空行数")

            // 体积（原则 P3）：表级对象要能整块读进内存、供**每一次**规划使用
            val tableBytes = runBlocking { server.engine.store.head(tableKey) }.size
            val colBytes = colKeys.sumOf { key -> runBlocking { server.engine.store.head(key) }.size }
            assertTrue(
                tableBytes < 64 * 1024,
                "表级统计对象必须小到能常驻内存（P3），实际 $tableBytes 字节",
            )
            println("[STATS] 表级 $tableBytes 字节 / 分区级 $colBytes 字节（$tableKey）")
        }
    }

    /**
     * **宽表的统计对象体积**（原则 P3）。
     *
     * 表级对象是**每一次查询规划**都要读的那一个，所以它的体积有硬上限：
     * 它必须能整体常驻内存。这条断言的存在理由是——直方图（32 桶/列）会让
     * 体积随列数线性增长，而这个增长只要没人盯着，就会在某次「多加几个桶」
     * 的改动里悄悄越界，然后表现为「规划变慢了」，而不是任何一条报错。
     */
    @Test
    fun `宽表的表级统计对象仍在几百 KB 以内`() {
        connect().use { c ->
            val cols = (1..19).map { "c$it INT" }.joinToString(", ")
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS optdb")
                s.execute("USE optdb")
                s.execute("DROP TABLE IF EXISTS w")
                s.execute("CREATE TABLE w (id BIGINT NOT NULL, $cols, PRIMARY KEY (id))")
            }
            c.st { s ->
                (1..800 step 200).forEach { start ->
                    val values = (start until minOf(start + 200, 801)).joinToString(",") { id ->
                        "($id," + (1..19).joinToString(",") { k -> "${id % (k + 3)}" } + ")"
                    }
                    s.execute("INSERT INTO w VALUES $values")
                }
            }
            flush()

            analyze(c, table = "w")
            // 这个用例里只建了 w 一张表，所以表级对象唯一
            val tableKey = statsKeys().single { it.contains("/partitions.v") }
            val bytes = runBlocking { server.engine.store.head(tableKey) }.size
            val stats = json.decodeFromString(
                TableStats.serializer(),
                String(runBlocking { server.engine.store.get(tableKey) }, Charsets.UTF_8),
            )
            assertEquals(800L, stats.rowCount)
            assertEquals(20, stats.columns.size)

            // 阈值 96 KB 是**量出来的**，不是拍的：
            //   · 直方图只存非空桶的下标与计数（桶边界由 min/max/桶数推导）⇒ 实测 46 KB
            //   · 逐桶把 lo/hi/count 三元组都写下来（冗余旧形态）      ⇒ 实测 113 KB
            // 阈值卡在这两个数之间，因此它同时守住两件事：够小（P3），
            // 以及「不要把那 60% 的冗余再加回来」。
            assertTrue(
                bytes < 96 * 1024,
                "20 列 / 800 行的表级统计对象为 $bytes 字节（实测应约 46 KB）。" +
                    "它是每一次查询规划都要读进内存的那一个对象（P3）；" +
                    "体积的主要来源是每列 32 桶的直方图，压缩它即可，但不要动格式版本以外的语义。",
            )
            println("[STATS] 20 列宽表：表级 $bytes 字节（${"%.0f".format(bytes / 20.0)} 字节/列）")
        }
    }

    // ── 2. ANALYZE 不读数据块（§13.3）─────────────────────────────

    @Test
    fun `ANALYZE 只读常驻内存的 idx - 数据块读取为 0`() {
        connect().use { c ->
            setup(c, rows = 4000)
            // 预热：让 shard 打开、manifest 与 .idx 进内存。
            // 不预热的话测到的是「打开分片」的一次性成本，而不是 ANALYZE 本身。
            c.st { s -> s.query("SELECT id FROM t WHERE id = 42") }

            counting.reset()
            analyze(c)

            // ★ §13.3：ANALYZE 的全部输入是常驻内存的块级元数据，因此「不读数据块」
            // 是**结构上**成立的。这条断言就是它的守卫——一旦有人在采集里加一次
            // `.dat` 读取（例如为了拿真实分布），这里立刻红。
            val dataReads = counting.keysFor("get_range").filter { it.first.contains("/dat") }
            assertEquals(
                emptyList(), dataReads,
                "ANALYZE 读到了数据块：$dataReads\n${counting.keyReport()}",
            )
            assertEquals(
                0L, counting.bytesOut("get_range"),
                "ANALYZE 不应产生任何范围读（`.idx` 已在内存里）：\n${counting.keyReport()}",
            )
            // 写入是允许的（统计对象 + catalog 的版本切换），但要能说清是几次
            assertTrue(counting.count("put") > 0, "ANALYZE 必须真的把统计写进对象存储")
            println("[IO] ANALYZE：${counting.optIoSummary()}")
        }
    }

    // ── 3. EXPLAIN 能显示 METADATA_COUNT（§8.9.6）────────────────

    @Test
    fun `EXPLAIN 显示 COUNT 走的是 METADATA_COUNT - 且 rows 与结果一致`() {
        connect().use { c ->
            setup(c, rows = 2000)

            val plan = c.q { s -> s.explain("SELECT COUNT(*) FROM t") }
            assertEquals("METADATA_COUNT", plan["type"], "§8.9.6：用户靠这一列确认 0 次请求的计数路径生效了")
            assertEquals("t", plan["table"])
            assertEquals(2000L, (plan["rows"] as Number).toLong(), "COUNT 的 rows 必须是精确行数，不是估算")
            val extra = plan["Extra"] as String
            assertTrue(extra.contains("0 object reads"), "Extra 必须写明它不读对象存储：$extra")

            // 反向的诚实：计数路径可用时，`select` 在 tryCount 处就返回了，从不排序。
            // 此时若还写 `Using filesort`，那是对反方向的谎报（说了不存在的工作）。
            val orderedCount = c.q { s -> s.explain("SELECT COUNT(*) FROM t ORDER BY id") }
            assertEquals("METADATA_COUNT", orderedCount["type"])
            assertTrue(
                !(orderedCount["Extra"] as String).contains("Using filesort"),
                "元数据计数路径根本不进 finishSelect，不该报 Using filesort：${orderedCount["Extra"]}",
            )

            // 与真实执行对账：EXPLAIN 说的行数必须等于实际返回值，否则它是在说谎
            val actual = c.q { s -> s.query("SELECT COUNT(*) FROM t")[0][0] as Long }
            assertEquals(actual, (plan["rows"] as Number).toLong())

            // EXPLAIN 本身也必须几乎不花 IO：它是规划，不是执行
            counting.reset()
            c.q { s -> s.explain("SELECT COUNT(*) FROM t WHERE id > 1") }
            assertEquals(
                0L, counting.bytesOut("get_range"),
                "EXPLAIN 是规划、不是执行，不该读数据：\n${counting.keyReport()}",
            )

            // ⭐ EXPLAIN 的关键价值在**退化时**才体现出来：COUNT 有未提交写时
            // 只能走扫描，此时必须把原因写出来，否则使用者会以为这条优化不存在
            c.autoCommit = false
            c.st { s -> s.execute("USE optdb"); s.execute("INSERT INTO t VALUES (99999,'x',1,'c0')") }
            val degraded = c.q { s -> s.explain("SELECT COUNT(*) FROM t") }
            assertTrue(
                degraded["type"] != "METADATA_COUNT",
                "有未提交写时元数据计数不覆盖它们，不能继续报 METADATA_COUNT",
            )
            assertTrue(
                (degraded["Extra"] as String).contains("uncommitted writes"),
                "退化的原因必须写出来，而不是静默：${degraded["Extra"]}",
            )
            c.rollback()
        }
    }

    // ── 4. 统计只影响计划，不影响结果 ─────────────────────────────

    @Test
    fun `有统计与没有统计的结果逐行相同`() {
        connect().use { c ->
            setup(c, rows = 2000)

            // 覆盖各条访问路径：点查 / 主键范围 / 二级索引等值 / 全表 / 计数 / 排序 + LIMIT
            val suite = listOf(
                "SELECT id, v FROM t WHERE id = 42",
                "SELECT id FROM t WHERE id > 1990",
                "SELECT id FROM t WHERE id >= 100 AND id <= 110",
                "SELECT id FROM t WHERE city = 'c3' ORDER BY id LIMIT 20",
                "SELECT id, n FROM t WHERE n = 7 ORDER BY id DESC LIMIT 30",
                "SELECT COUNT(*) FROM t",
                "SELECT COUNT(*) FROM t WHERE id > 1000",
                "SELECT v FROM t WHERE n IS NULL",
                "SELECT id FROM t WHERE v LIKE 'u1%' AND id < 200",
            )

            val before = suite.map { sql -> c.q { s -> s.query(sql) } }
            val beforePlan = c.q { s -> s.explain("SELECT id FROM t WHERE n = 7") }

            analyze(c)

            val after = suite.map { sql -> c.q { s -> s.query(sql) } }
            assertEquals(
                before, after,
                "统计信息只应改变计划，不应改变任何一行结果。" +
                    "如果这里红了，说明某条访问路径的语义与它替代的那条不等价。",
            )

            // 计划确实换了脑子（否则上面那条断言是「什么都没测」）：
            // ANALYZE 之前是硬编码兜底链，之后才是代价比较。Extra 里那句话
            // 就是「现在是哪种模式」的开关，别处看不出来。
            val afterPlan = c.q { s -> s.explain("SELECT id FROM t WHERE n = 7") }
            assertTrue(
                (beforePlan["Extra"] as String).contains("no statistics"),
                "未经 ANALYZE 的表必须自报「走的是兜底链」：${beforePlan["Extra"]}",
            )
            assertTrue(
                !(afterPlan["Extra"] as String).contains("no statistics"),
                "ANALYZE 之后不该再报「没有统计」：${afterPlan["Extra"]}",
            )
            println("[PLAN] ANALYZE 前：${beforePlan["type"]} / ${beforePlan["Extra"]}")
            println("[PLAN] ANALYZE 后：${afterPlan["type"]} / ${afterPlan["Extra"]}")

            // 新启用的键区间路径：只有下界的主键范围不再退化成全表扫描（§10.2 R1）
            assertEquals("range", explainType(c, "SELECT id FROM t WHERE id > 1500"))

            // EXPLAIN 的**契约**（§8.9.6）：至少要有这五列，缺一列使用者的脚本就断了。
            // 按列名断言，不按位置——位置一变，按位置写的测试会静默地测到别的列。
            c.st { s ->
                s.executeQuery("EXPLAIN SELECT COUNT(*) FROM t").use { rs ->
                    val labels = (1..rs.metaData.columnCount).map { rs.metaData.getColumnLabel(it) }
                    listOf("table", "type", "key", "rows", "Extra").forEach {
                        assertTrue(labels.contains(it), "EXPLAIN 的结果集缺少 `$it` 列，实际 $labels")
                    }
                }
            }

            // `ORDER BY` 的 Extra 必须如实：执行器目前总是把结果捞进内存再排，
            // 所以**只要语句里有 ORDER BY 就该有 `Using filesort`**——包括
            // `ORDER BY id`（主键序扫描其实不用排，但现在真的排了）。
            // 这条断言是「Extra 不许说谎」的守卫：反过来，没有 ORDER BY 时也不能报。
            assertTrue(
                (c.q { s -> s.explain("SELECT id FROM t WHERE id > 1500 ORDER BY n")["Extra"] as String })
                    .contains("Using filesort"),
                "有 ORDER BY 就必须写 Using filesort（执行器真的会排序）",
            )
            assertTrue(
                !(c.q { s -> s.explain("SELECT id FROM t WHERE id > 1500")["Extra"] as String })
                    .contains("Using filesort"),
                "没有 ORDER BY 时不该出现 Using filesort",
            )

            // 把每条访问路径的 EXPLAIN 打一遍：这是使用者核对「优化器选了哪条路」
            // 的唯一入口，让它出现在测试输出里，比写在文档里可核对得多。
            listOf(
                "SELECT COUNT(*) FROM t",
                "SELECT COUNT(*) FROM t WHERE id > 1000",
                "SELECT id, v FROM t WHERE id = 42",
                "SELECT id FROM t WHERE id > 1500",
                "SELECT id FROM t WHERE n = 7",
                "SELECT id FROM t WHERE city = 'c3' ORDER BY id LIMIT 20",
                "SELECT id FROM t WHERE id + 0 > 1500",
            ).forEach { sql ->
                val r = c.q { s -> s.explain(sql) }
                println("[EXPLAIN] $sql → type=${r["type"]} key=${r["key"]} rows=${r["rows"]} Extra=${r["Extra"]}")
            }
        }
    }

    // ── 5. 主键范围：确有计划上的证据，且确实读得少 ────────────────

    @Test
    fun `主键范围查询走 range 且比全表扫描读得少`() {
        connect().use { c -> setup(c, rows = 4000) }

        // 零缓存的引擎上比字节数：缓存命中时两条路径都是 0，比较就退化成 0 与 0
        val bounded = measureCold { c -> c.q { s -> s.query("SELECT id FROM t WHERE id > 3900").size } }
        // `id + 0` 让谓词无法成为键区间，强制走全表扫描——结果必须一样
        val scanned = measureCold { c -> c.q { s -> s.query("SELECT id FROM t WHERE id + 0 > 3900").size } }

        assertEquals(scanned.first, bounded.first, "两条路径的结果必须一致：100 行")
        assertEquals(100, bounded.first)
        assertTrue(
            bounded.second.bytesOut("get_range") < scanned.second.bytesOut("get_range"),
            "键区间扫描读的字节(${bounded.second.bytesOut("get_range")}B)必须少于全表扫描" +
                "(${scanned.second.bytesOut("get_range")}B)——否则 range 路径没有意义。\n" +
                "range：${bounded.second}\n扫描：${scanned.second}",
        )
        println(
            "[IO] 键区间 ${bounded.second.bytesOut("get_range")}B vs 全表扫描 " +
                "${scanned.second.bytesOut("get_range")}B"
        )
    }

    /**
     * 在一个**全新的零缓存引擎**上跑一段查询，返回 (结果, IO 快照)。
     *
     * 零缓存是关键：只有这样「读了多少」才等于「这个查询结构上需要读多少」，
     * 而不是「上一次跑完还剩什么在内存里」。
     */
    private fun <T> measureCold(block: (Connection) -> T): Pair<T, OptIoSnapshot> {
        val cold = YangBaseServer.forTesting(store = counting, allowNonAtomicPointerSwitch = backend.oss, port = 0, cacheBytes = 0)
        cold.start()
        try {
            val cc = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:${cold.boundPort}/optdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "root", "",
            )
            cc.use {
                counting.reset()
                val r = block(cc)
                return r to OptIoSnapshot(counting.snapshot(), listOf("get", "get_range").associateWith { op -> counting.bytesOut(op) })
            }
        } finally {
            runCatching { cold.stop() }
        }
    }

    private companion object {
        const val DDL =
            "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), " +
                "PRIMARY KEY (id), KEY idx_n (n))"
    }
}

/**
 * 一次测量的 IO 快照。
 *
 * 不能直接用 `counting.snapshot()`——那是活的计数器，下一次测量会把它清掉，
 * 断言失败时打印出来的会是**另一次**的明细。
 */
private class OptIoSnapshot(val ops: Map<String, Long>, val outBytes: Map<String, Long>) {
    fun bytesOut(op: String): Long = outBytes[op] ?: 0L
    override fun toString() = ops.entries.sortedByDescending { it.value }
        .joinToString(", ") { "${it.key}=${it.value}" } +
        "；读入字节 total=" + ops.keys.sumOf { bytesOut(it) }
}

/** 把计数器的分布打出来，断言失败时能直接看到钱花在哪。 */
private fun CountingObjectStore.optIoSummary(): String {
    val ops = listOf("get", "get_range", "put", "delete", "list", "head").filter { count(it) > 0 }
    return ops.joinToString(", ") { "$it=${count(it)}" } + "；出流量 ${ops.sumOf { bytesOut(it) }} B"
}
