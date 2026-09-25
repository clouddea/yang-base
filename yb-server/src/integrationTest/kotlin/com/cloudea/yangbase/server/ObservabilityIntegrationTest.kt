package com.cloudea.yangbase.server

import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.EmbeddedS3Server
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.S3ObjectStore
import kotlinx.coroutines.runBlocking
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.time.LocalDate
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * 可观测性的**集成测试**（设计文档 §12.5）：一个真实运行的 CN +
 * 真实的 MySQL 协议 + 真实的 HTTP 抓取 + 真实的对象存储（内嵌 S3）。
 *
 * ## 这个文件要证明的核心命题
 *
 * **监控自己不是成本。** 原则 P4 说「请求数是一等成本」，那么可观测性就必须回答
 * 一个自己给自己出的难题：为了知道「一次查询打了几个对象」，它自己又打了几个对象？
 * 答案是**零**——指标全部来自内存计数器，抓取时只做加法与字符串拼接。
 * 这件事不能靠读代码相信，只能靠断言：`MetricsNoIoIntegrationTest` 用
 * [CountingObjectStore] 把「抓取窗口内对象存储请求数没有变化」变成一条硬断言。
 *
 * 审计是唯一的例外（它要写 `ops/audit/{date}.jsonl`），因此它单独验证：
 * 批量（N 条记录一次 PUT）、异步、失败隔离。
 *
 * 与 `yb-server/src/test/.../ObservabilityTest.kt` 的分工：那边测**判定逻辑**
 * （分类、阈值、归因），这边测**装配**（协议层埋点 → 采集器 → HTTP 端点）。
 * 埋点接错时，两边会分别红——这个组合能一次指出断在哪一段。
 */
abstract class ObservabilityClusterFixture {

    private var s3: EmbeddedS3Server? = null

    /** 生产的分层：指标装饰在最外，计数夹具在里面（见 `YangBaseServer.start`）。 */
    protected lateinit var counting: CountingObjectStore
    protected lateinit var server: YangBaseServer
    protected var port: Int = 0
    protected var metricsPort: Int = 0

    private val http: HttpClient = HttpClient.newHttpClient()

    /** 慢查询阈值。默认 0：集成测试里每条语句都算慢，省得为了触发阈值去构造慢查询。 */
    protected open fun slowQueryMillis(): Long = 0

    protected open fun auditEnabled(): Boolean = true

    /**
     * 块缓存容量。传 0 表示**完全不缓存**（见 `YangBaseServer.forTesting` 的说明）。
     *
     * 需要它的理由：缓存命中时读路径一次对象存储请求都不发，「指标 vs 事实」就变成
     * 0 与 0 的比较——对得再齐也证明不了读路径的埋点是对的。
     */
    protected open fun cacheBytes(): Long = 256L * 1024 * 1024

    @BeforeTest
    fun startCluster() {
        val emb = EmbeddedS3Server("yangbase-obs")
        emb.start()
        s3 = emb
        val raw = S3ObjectStore.create(
            bucket = "yangbase-obs",
            endpoint = emb.endpoint,
            region = "us-east-1",
            accessKey = "t",
            secretKey = "t",
        )
        // recordKeys 打开：断言失败时能直接看到「这些请求打在哪」
        counting = CountingObjectStore(raw, recordKeys = true)
        server = YangBaseServer.forTesting(
            store = counting,
            port = 0,
            metricsPort = 0,               // 由系统分配端口
            cacheBytes = cacheBytes(),
            slowQueryMillis = slowQueryMillis(),
            auditEnabled = auditEnabled(),
        )
        server.start()
        port = server.boundPort
        metricsPort = server.metricsBoundPort
        assertTrue(metricsPort > 0, "观测端点必须已经起来（--metrics-port 0 表示随机端口）")
        Class.forName("com.mysql.cj.jdbc.Driver")
    }

    @AfterTest
    fun stopCluster() {
        runCatching { server.stop() }
        s3?.let { runCatching { it.stop() } }
    }

    // ── 夹具 ──────────────────────────────────────────────────────

    protected fun connect(): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    protected fun <T> Connection.use(block: (Connection) -> T): T {
        try {
            return block(this)
        } finally {
            runCatching { close() }
        }
    }

    /** 建库建表插数据：让 CN 真的在干活，否则「指标正常」是没有意义的。 */
    protected fun seed(c: Connection, db: String = "obs", table: String = "t") {
        c.createStatement().use { st ->
            st.execute("CREATE DATABASE IF NOT EXISTS $db")
            st.execute("USE $db")
            st.execute("CREATE TABLE IF NOT EXISTS $table (id BIGINT NOT NULL, v VARCHAR(32), PRIMARY KEY (id))")
            st.executeUpdate("INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        }
    }

    protected fun httpGet(path: String): Pair<Int, String> {
        val r = http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$metricsPort$path")).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        return r.statusCode() to r.body()
    }

    /** 抓一次 `/metrics` 并解析成 `序列 → 值`（重复序列会直接失败，那是非法输出）。 */
    protected fun scraped(): Map<String, Double> {
        val (code, body) = httpGet("/metrics")
        assertEquals(200, code, body)
        return parseSeries(body)
    }

    protected fun series(m: Map<String, Double>, name: String): Double = m[name] ?: 0.0

    /**
     * 与 `ServerMetrics.READ_OPS` 一致的四类读。
     *
     * 刻意包含 LIST：埋点的口径是这四类之和，少算一类会让「指标 vs 事实」的
     * 比对在长读路径上出现无意义的偏差。
     */
    protected fun readOps(): Long =
        counting.count("get") + counting.count("get_range") + counting.count("list") + counting.count("head")

    protected val OPS = listOf("get", "get_range", "put", "put_cond", "delete", "list", "head")

    protected fun metricOps(m: Map<String, Double>): Map<String, Double> =
        OPS.associateWith { series(m, "yb_objectstore_requests_total{op=\"$it\",cached=\"false\"}") }

    /** 全部查询类型读取的对象数之和（与「读请求数」应当严格相等）。 */
    protected fun metricQueryObjects(m: Map<String, Double>): Double =
        listOf("point", "range", "scan", "fulltext", "other")
            .sumOf { series(m, "yb_query_objects_fetched_sum{type=\"$it\"}") }
}

/**
 * ★ **最重要的一条断言**：指标采集与探活**不产生任何对象存储请求**。
 *
 * 做法：让 CN 先完成一轮真实的建库-建表-写入-查询（此时缓存、索引、WAL 都在
 * 正常状态），把审计缓冲清空，然后在一个只包含 HTTP 抓取的窗口里读两次计数夹具。
 * 两次读数不变 = 这个窗口里对象存储一次都没被碰过。
 *
 * 窗口里可能落进**与抓取无关**的后台请求（compaction 读 `.dat`、租约续期、
 * 审计刷写）。这种窗口不能拿来测抓取的开销，所以用例会换一个窗口重测——
 * 但绝不放松判据：真正的「抓取会打对象存储」会**每一个**窗口都失败。
 */
@org.junit.jupiter.api.Tag("integration")
class MetricsNoIoIntegrationTest : ObservabilityClusterFixture() {

    override fun slowQueryMillis(): Long = 500

    @Test
    fun `★ 抓取指标与探活不产生任何对象存储请求`() {
        connect().use { c ->
            seed(c)
            c.createStatement().use { st -> st.executeQuery("SELECT v FROM t WHERE id = 1").close() }
        }
        // 审计缓冲清空：空闲的后台刷新在缓冲为空时直接返回，一次请求都不发
        runBlocking { server.auditLog?.flush() }

        var attempts = 0
        while (true) {
            attempts++
            val opsBefore = counting.totalOps()
            // 基准读数本身也是一次抓取（`yb_metrics_scrapes_total` 在渲染前自增），
            // 因此下面窗口内的 5 次抓取应当让它正好 +5
            val scrapesBefore = series(scraped(), "yb_metrics_scrapes_total")

            var lastBody = ""
            repeat(5) { lastBody = httpGet("/metrics").second }
            repeat(3) { assertEquals(200, httpGet("/healthz").first) }

            val opsAfter = counting.totalOps()
            val scrapesAfter = series(parseSeries(lastBody), "yb_metrics_scrapes_total")
            assertEquals(
                5.0, scrapesAfter - scrapesBefore,
                "5 次 /metrics 抓取必须真的落到端点上，否则这条测试可能什么都没测",
            )

            if (opsAfter == opsBefore) return

            // 窗口里落进了后台请求：那不是抓取的开销
            assertTrue(
                attempts < 8,
                "连续 $attempts 个窗口都有后台请求落地，无法测量抓取的开销：" +
                    counting.keyReport(),
            )
            runBlocking { server.auditLog?.flush() }
        }
    }
}

/**
 * 观测面的集成验证：端点上的指标是否**覆盖**了设计文档 §12.5 的要求、
 * 数字是否**等于事实**、审计是否落成了对象。
 */
@org.junit.jupiter.api.Tag("integration")
class ObservabilityIntegrationTest : ObservabilityClusterFixture() {

    /**
     * 关掉块缓存。
     *
     * 这一类的两个用例都在问「指标说的数字是否等于对象存储真收到的请求」，
     * 而缓存命中会让真收到的请求数变成 0——两个 0 相等，什么也没证明。
     * 关掉缓存后，每一次读都必然落到对象存储上，等式的两边才是活的。
     * 稳态（缓存命中、0 请求）那一侧由 `IoBudgetIntegrationTest` 守护。
     */
    override fun cacheBytes(): Long = 0

    @Test
    fun `观测端点暴露第 12 章要求的核心指标`() {
        connect().use { c ->
            seed(c)
            c.createStatement().use { st ->
                st.executeQuery("SELECT v FROM t WHERE id = 1").close()   // 点查
                st.executeQuery("SELECT v FROM t WHERE id > 1").close()   // 范围
                st.executeQuery("SELECT COUNT(*) FROM t").close()         // 元数据计数（0 GET 的类别）
            }
        }
        val m = scraped()

        assertEquals(1.0, series(m, "yb_up"), "CN 在运行")
        assertTrue("yb_metrics_scrapes_total" in m)

        // 对象存储：请求数 / 字节 / 耗时，且 {cached} 维度可分辨
        assertPositive(m, "yb_objectstore_requests_total{op=\"put\",cached=\"false\"}")
        assertPresent(m, "yb_objectstore_requests_total{op=\"get\",cached=\"false\"}")
        assertPresent(m, "yb_objectstore_requests_total{cached=\"true\",op=\"get\"}", "缓存命中的折算值")
        assertPresent(m, "yb_objectstore_bytes_total{direction=\"out\",op=\"get\"}")
        assertPresent(m, "yb_objectstore_bytes_total{direction=\"in\",op=\"put\"}")
        assertPresent(m, "yb_objectstore_request_duration_seconds_count{op=\"get\"}")

        // 写路径：提交延迟 / 攒批 / 摊薄倍数
        assertPresent(m, "yb_commit_duration_seconds_count")
        assertPresent(m, "yb_group_commit_batch_size_count")
        assertTrue(
            series(m, "yb_group_commit_dilution_ratio") >= 1.0,
            "每个 PUT 至少摊掉 1 个事务，实际 ${series(m, "yb_group_commit_dilution_ratio")}",
        )
        assertPositive(m, "yb_wal_objects_total")

        // 事务：单分片 / 2PC 比值
        assertPositive(m, "yb_transactions_total{result=\"commit\"}")
        assertPresent(m, "yb_transactions_total{result=\"rollback\"}")
        assertPresent(m, "yb_txn_2pc_total")
        assertPresent(m, "yb_txn_single_shard_ratio")

        // 租约：单机协调（SingleProcessLease）下恒为 0，但**必须暴露**——
        // 聚合侧要能区分「没发生过丢租约」与「这个版本没有这条指标」
        assertPresent(m, "yb_lease_acquisitions_total{result=\"ok\"}", "单机模式下恒为 0，非零即告警")
        assertPresent(m, "yb_lease_acquisitions_total{result=\"failed\"}")
        assertPresent(m, "yb_lease_lost_total", "★ 非零即告警")

        // 查询：耗时 + ★ 每查询对象数（0 次 GET 是一个必须可分辨的类别）
        assertPositive(m, "yb_query_duration_seconds_count{type=\"point\"}")
        assertPresent(m, "yb_query_objects_fetched_bucket{type=\"point\",le=\"0\"}", "0 次 GET 的分布格子")
        assertPositive(m, "yb_slow_queries_total", "本类阈值取 0，任何一条语句都算慢")

        // 存储：compaction / GC / 缓存 / 常驻索引
        assertPresent(m, "yb_compaction_bytes_total{direction=\"read\"}")
        assertPresent(m, "yb_compaction_active")
        assertPresent(m, "yb_garbage_objects_pending")
        assertPresent(m, "yb_gc_watermark_lag_seconds")
        assertTrue(series(m, "yb_open_shards") >= 1.0, "至少有分片被打开")
        assertPresent(m, "yb_block_cache_misses_total{tier=\"mem\"}")
        assertPresent(m, "yb_block_cache_hits_total{tier=\"mem\"}")
        assertPresent(m, "yb_block_cache_coalesced_requests_total{tier=\"mem\"}")
        assertPresent(m, "yb_block_cache_hit_rate{tier=\"mem\"}")
        assertPresent(m, "yb_index_resident_bytes")
        assertPresent(m, "yb_memtable_bytes")

        // 审计（唯一被允许写对象存储的观测手段）
        assertPresent(m, "yb_audit_records_written_total")
        assertPresent(m, "yb_audit_dropped_total")
    }

    /** 断言这个序列存在。**消息里带上实际值**——缺的是它还是值不对，一眼可分。 */
    private fun assertPresent(m: Map<String, Double>, name: String, why: String = "") {
        assertTrue(name in m, "缺少指标 $name${if (why.isEmpty()) "" else "（$why）"}")
    }

    private fun assertPositive(m: Map<String, Double>, name: String, why: String = "") {
        val v = series(m, name)
        assertTrue(
            v > 0.0,
            "$name 应当是正数，实际 $v${if (why.isEmpty()) "" else "（$why）"}；相关量：" +
                listOf(
                    "yb_wal_objects_total",
                    "yb_transactions_total{result=\"commit\"}",
                    "yb_txn_single_shard_total",
                    "yb_txn_2pc_total",
                    "yb_commit_duration_seconds_count",
                    "yb_group_commit_batches_total",
                    "yb_group_commit_groups_total",
                    "yb_statements_observed_total",
                    "yb_flush_total",
                ).joinToString(", ") { "$it=${series(m, it)}" },
        )
    }

    /**
     * 指标说的请求数必须**等于**对象存储实际收到的请求数。
     *
     * 这是整套埋点的地基：指标可以「看起来合理」而实际上是错的（少埋一个方法、
     * 把重试算成一次、把缓存命中算成请求），只有和独立的计数夹具逐项对比才分得清。
     *
     * 两个读数都在**静止窗口**里取（见 [awaitStaticWindow]），然后比较**增量**：
     * 计数据是每个用例新建的（从 0 起），而指标注册表是进程级的（跨用例累积），
     * 比绝对值会把前一个用例的历史算进来。
     */
    @Test
    fun `指标里的请求数与对象存储实际收到的请求数逐项相等`() {
        val base = awaitStaticWindow()

        connect().use { c ->
            seed(c)
            // 落盘：让随后的点查与范围查**必须**去对象存储取 `.dat` 块。
            // 不落盘的话行还留在 MemTable 里，读路径一次请求都不发——于是「指标 == 事实」
            // 只在写路径上成立，而读路径（最容易漏埋的那一半）根本没被测到。
            flush()
            c.createStatement().use { st ->
                st.executeQuery("SELECT v FROM t WHERE id = 1").close()
                st.executeQuery("SELECT v FROM t WHERE id > 1").close()
                st.executeUpdate("INSERT INTO t VALUES (4, 'd')")
            }
        }

        val now = awaitStaticWindow()
        val deltas = OPS.associateWith { op ->
            ((now.first[op] ?: 0L) - (base.first[op] ?: 0L)) to
                (now.second.getValue(op) - base.second.getValue(op))
        }
        var checked = 0
        for (op in OPS) {
            val (actual, claimed) = deltas.getValue(op)
            assertEquals(
                actual.toDouble(),
                claimed,
                "op=$op：指标声称 $claimed 次，对象存储实际收到 $actual 次。" +
                    "对不上说明埋点漏了这个方法或重复计数了\n${counting.keyReport()}",
            )
            if (actual > 0) checked++
        }
        // 非平凡性：必须真的发生过几类操作，且**必须包含真实的读**——
        // 只在写路径上对得齐，等于没测到读路径的埋点（那才是最容易漏的一半）。
        val readKinds = listOf("get", "get_range", "list", "head").filter { deltas.getValue(it).first > 0 }
        assertTrue(
            checked >= 3 && readKinds.isNotEmpty(),
            "这一轮至少要有几类操作真的发生过（且必须包含读），否则比较的只是两组 0。" +
                "本轮的增量（计数据, 指标）=$deltas\n${counting.keyReport()}",
        )
    }

    /** 让 MemTable 落盘，把数据推进 SST 层——否则读路径全在内存里，读指标恒为 0。 */
    private fun flush() = runBlocking { server.engine.flushAll() }

    /**
     * 取一个**静止窗口**的读数：先读计数据、再读指标、再读计数据，
     * 首尾相同就说明中间那次指标读数落在「没有请求落地」的窗口里，两者严格可比。
     *
     * 背景活动（compaction、审计刷写、group commit 的收尾）会在任意时刻打一次请求，
     * 不做这个对齐就直接比较，得到的是随机失败。
     */
    private fun awaitStaticWindow(): Pair<Map<String, Long>, Map<String, Double>> {
        repeat(8) {
            val c1 = counting.snapshot()
            val m = metricOps(scraped())
            val c2 = counting.snapshot()
            if (c1 == c2) return c1 to m
        }
        fail("连续 8 次都没能取到一个静止的观测窗口（后台一直在打对象存储）：\n${counting.keyReport()}")
    }

    /**
     * ★ `yb_query_objects_fetched` 直接对应 §14.2 的 IO 预算，因此它必须是
     * **实测**而不是估算：一次查询前后，该指标的增长必须等于对象存储在这段时间里
     * 真正收到的读请求数。
     */
    @Test
    fun `每查询对象读取数等于该查询真正打出的读请求数`() {
        connect().use { c ->
            seed(c)
            // 落盘 + 块缓存关闭（见本类的 cacheBytes）：这次点查**必然**要去对象存储
            // 取一次 `.dat` 块。于是 claimed 是个非零的实测值——上面那条等式才有内容。
            flush()
            c.createStatement().use { st ->
                val claimed = measureOnePointQuery(st)
                assertTrue(
                    claimed >= 1.0,
                    "落盘之后、缓存关闭的情况下，一次点查至少要读一个对象（`.dat` 块）。" +
                        "读到 0 说明这次测量是空转——等式两边都是 0，什么都没验证：" +
                        "\n${counting.keyReport()}",
                )
                // §14.2：冷缓存的点查 ≤ 4 次 GET（`.idx` 常驻内存，稳态则是 ≤ 1 次，
                // 由 `IoBudgetIntegrationTest` 用热缓存断言）。这里挡住的是**多算**：
                // 把一次去重后的读算成多次，指标就会凭空膨胀。
                assertTrue(
                    claimed <= 4.0,
                    "冷缓存点查读取的对象数应当 ≤ 4（设计文档 §14.2），实际 $claimed：" +
                        "\n${counting.keyReport()}",
                )
            }
        }
    }

    /**
     * 观测一次点查：返回指标声称的对象读取数，并**要求它与对象存储实际收到的
     * 读请求数严格相等**。
     *
     * 不相等有两种来源：窗口里落了别的读（compaction / 别的语句）——那是观测噪声，
     * 换一个窗口重测；或者埋点的口径错了——那换多少个窗口都不会相等。
     * 因此这里重试的是**窗口**，不是判据。
     */
    private fun measureOnePointQuery(st: Statement): Double {
        var claimed = -1.0
        var actual = -1.0
        repeat(8) {
            val readsBefore = readOps()
            val claimedBefore = metricQueryObjects(scraped())
            st.executeQuery("SELECT v FROM t WHERE id = 2").close()
            claimed = metricQueryObjects(scraped()) - claimedBefore
            actual = (readOps() - readsBefore).toDouble()
            if (claimed == actual) return claimed
        }
        fail(
            "指标声称这个查询读了 $claimed 个对象，对象存储实际收到 $actual 次读请求。" +
                "口径对不上（埋点漏了/多算了读路径）：\n${counting.keyReport()}",
        )
    }

    /**
     * DDL 落成审计对象。
     *
     * 断言的是**契约**：键的形状（`ops/audit/{date}.jsonl`）、JSONL 一行一条、
     * 多条 DDL 摊进**同一个**对象（批量），以及审计的写入本身也被计成
     * `yb_audit_records_written_total`——审计是唯一允许写对象存储的观测手段，
     * 因此它必须自己可见。
     */
    @Test
    fun `DDL 落成审计对象且键形状符合约定`() {
        val date = LocalDate.now().toString()
        val audit = server.auditLog ?: fail("审计未启用：本用例要求 auditEnabled = true")
        val writtenBefore = auditWritten()

        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE IF NOT EXISTS obs_audit")
                st.execute("USE obs_audit")
                st.execute("CREATE TABLE audit_t (id BIGINT NOT NULL, PRIMARY KEY (id))")
                st.execute("CREATE INDEX idx_id ON audit_t (id)")
            }
        }
        // 显式刷新（生产由 2 秒一次的后台刷新负责）
        runBlocking { audit.flush() }

        assertTrue(
            auditWritten() > writtenBefore,
            "写出的审计记录数必须被计数（否则审计静默失效时没人知道）",
        )

        val key = ObjectKeys.auditLog(date)
        assertTrue(
            Regex("ops/audit/\\d{4}-\\d{2}-\\d{2}\\.jsonl").matches(key),
            "审计对象的键形状是运维契约（按日期可直接列举）：$key",
        )
        val body = runBlocking { String(counting.get(key), StandardCharsets.UTF_8) }
        val lines = body.trim().lines()
        assertTrue(lines.size >= 2, "多条 DDL 应当落在同一个对象里（一次 PUT）：\n$body")
        assertTrue(body.contains("\"action\":\"ddl.create_table\""), body)
        assertTrue(body.contains("\"action\":\"ddl.create_index\""), body)
        assertTrue(body.contains("\"target\":\"audit_t\""), body)
        assertTrue(body.contains("CREATE TABLE audit_t"), "审计要留下原始 SQL：$body")
        // 每条记录都能单独解析（JSONL 的契约：一行一条、可流式消费）
        for (line in lines) {
            assertTrue(line.startsWith("{") && line.endsWith("}"), "非法 JSONL 行：$line")
            assertTrue(line.contains("\"ts\":"), line)
        }
    }

    /** 超过阈值的语句被计数（阈值见 [slowQueryMillis]，本类取 0）。 */
    @Test
    fun `超过慢查询阈值的语句被计数`() {
        connect().use { c ->
            seed(c)
            val before = series(scraped(), "yb_slow_queries_total")
            c.createStatement().use { st -> st.executeQuery("SELECT v FROM t WHERE id = 1").close() }
            val after = series(scraped(), "yb_slow_queries_total")
            assertTrue(after > before, "阈值为 0 时每条语句都算慢：$before -> $after")
        }
    }

    @Test
    fun `healthz 报告进程状态且能区分健康`() {
        connect().use { c -> seed(c) }
        val (code, body) = httpGet("/healthz")
        assertEquals(200, code, body)
        assertTrue(body.startsWith("status=ok"), body)
        val fields = body.trim().lines().associate { it.substringBefore('=') to it.substringAfter('=') }
        assertTrue(fields.getValue("open_shards").toInt() >= 1, body)
        assertTrue(fields.getValue("tables").toInt() >= 1, body)
        assertTrue(fields.getValue("statements_observed").toInt() >= 1, body)
        assertTrue(fields.getValue("uptime_seconds").toInt() >= 0, body)
        assertTrue(fields.getValue("group_commit_dilution").toDouble() >= 0.0, body)
    }

    private fun auditWritten(): Long = Metrics.auditWritten.value()
}

/** 把 Prometheus 文本解析成 `序列 → 值`；**重复序列直接失败**（那是非法的抓取结果）。 */
private fun parseSeries(text: String): Map<String, Double> {
    val out = HashMap<String, Double>()
    for (raw in text.lineSequence()) {
        val line = raw.trim()
        if (line.isEmpty() || line.startsWith("#")) continue
        val sp = line.lastIndexOf(' ')
        require(sp > 0) { "非法指标行：$line" }
        val series = line.substring(0, sp)
        val v = line.substring(sp + 1).toDoubleOrNull() ?: error("非法指标值：$line")
        require(out.put(series, v) == null) { "重复的指标序列：$series" }
    }
    return out
}
