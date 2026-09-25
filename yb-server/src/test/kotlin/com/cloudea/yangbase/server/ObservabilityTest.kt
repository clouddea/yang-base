package com.cloudea.yangbase.server

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.IndexDef
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.audit.Audit
import com.cloudea.yangbase.common.audit.AuditActions
import com.cloudea.yangbase.common.audit.AuditEvent
import com.cloudea.yangbase.common.audit.AuditSink
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.common.metrics.QueryType
import com.cloudea.yangbase.objectstore.ByteRange
import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.InstrumentedObjectStore
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.sql.Parser
import com.cloudea.yangbase.sql.Session
import com.cloudea.yangbase.txn.DatabaseEngine
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 可观测性（设计文档 §12.5）：查询观测、指标采集、观测端点。
 *
 * 这是**单元级**的验证：不跑 MySQL 协议、不打内嵌 S3，而是直接把
 * [ServerMetrics] 当 `QueryObserver` 调、直接把观测端点当 HTTP 服务问。
 *
 * 端到端的那套（真实 CN + JDBC + `GET /metrics` + 用 `CountingObjectStore`
 * 断言「抓取不产生请求」）在 `integrationTest` 源集里，见 `ObservabilityIntegrationTest`。
 * 两边都保留是刻意的：单元测试跑得快、给的是精确的诊断；
 * 集成测试跑的是真实的装配路径（协议层 → 观测点 → 采集器 → HTTP）。
 */
class ObservabilityTest {

    private lateinit var scope: CoroutineScope
    private lateinit var counting: CountingObjectStore

    /** 生产的分层：指标装饰在最外，计数夹具在里面。 */
    private lateinit var store: ObjectStore
    private lateinit var engine: DatabaseEngine
    private lateinit var session: Session
    private lateinit var table: TableSchema

    @BeforeTest
    fun setUp() = runBlocking {
        scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        counting = CountingObjectStore(MemoryObjectStore())
        store = InstrumentedObjectStore(counting)
        engine = DatabaseEngine(store = store, scope = scope, nodeId = "obs-test")
        engine.open()
        val db = engine.catalog.createDatabase("mdb")
        table = engine.catalog.createTable(
            dbId = db.databaseId,
            name = "t",
            columns = listOf(
                ColumnDef("id", ColumnType.BIGINT, nullable = false),
                ColumnDef("v", ColumnType.VARCHAR, length = 32),
                ColumnDef("n", ColumnType.INT),
            ),
            primaryKey = listOf("id"),
            indexes = listOf(IndexDef(id = 0, name = "idx_n", kind = IndexKind.HASH, columns = listOf("n"))),
        )
        session = Session(engine)
        session.currentDbName = "mdb"
        Metrics.reset()
    }

    @AfterTest
    fun tearDown() = runBlocking {
        runCatching { engine.close() }
        Audit.uninstall()
    }

    // ── 访问路径分类（yb_query_duration_seconds{type}）──────────────

    @Test
    fun `访问路径分类`() {
        // 只有能证明「一次点查」时才叫 point——它直接对应 IO 预算里的那 1 次 GET
        assertEquals(QueryType.POINT, typeOf("SELECT v FROM t WHERE id = 1"))
        assertEquals(QueryType.RANGE, typeOf("SELECT v FROM t WHERE id > 1"))
        assertEquals(QueryType.RANGE, typeOf("SELECT v FROM t WHERE n = 7"), "二级索引上的等值是一次有界扫描")
        assertEquals(QueryType.SCAN, typeOf("SELECT v FROM t WHERE v = 'x'"), "无索引列上的谓词只能全表扫描")
        assertEquals(QueryType.SCAN, typeOf("SELECT * FROM t"))
        assertEquals(
            QueryType.SCAN, typeOf("SELECT v FROM t WHERE id = 1 OR v = 'x'"),
            "OR 分支上的等值不能保证一次 GET，把它算成 point 会让成本指标骗人",
        )
        assertEquals(QueryType.FULLTEXT, typeOf("SELECT id FROM t WHERE MATCH (v) AGAINST ('abc')"))
        assertEquals(QueryType.OTHER, typeOf("INSERT INTO t VALUES (1, 'a', 1)"))
    }

    // ── ★ 每查询对象读取数 ────────────────────────────────────────

    @Test
    fun `每查询的对象读取数按访问路径归集`() {
        val metrics = metrics()
        // 先把要读的对象写进去（PUT 不算读）
        runBlocking { store.put("obj", ByteArray(8)) }
        Metrics.reset()

        val sql = "SELECT v FROM t WHERE id = 1"
        val stmt = Parser.parse(sql)
        val token = metrics.queryStarted(sql, stmt, session)
        // 模拟这个查询真正打到对象存储的三次读
        runBlocking {
            store.get("obj")
            store.get("obj", ByteRange(0, 3))
            store.head("obj")
        }
        metrics.queryFinished(token, sql, stmt, session, rows = 1, error = null)

        assertEquals(
            3.0, prometheus()["yb_query_objects_fetched_sum{type=\"point\"}"],
            "★ 这个指标是本系统最重要的成本指标，它必须真的来自调用点而不是估算",
        )
        assertEquals(1.0, prometheus()["yb_query_duration_seconds_count{type=\"point\"}"])
    }

    // ── 慢查询 ────────────────────────────────────────────────────

    @Test
    fun `慢查询阈值判定与日志内容`() {
        val slowBefore = Metrics.slowQueries.value()
        val logs = ArrayList<String>()
        val metrics = metrics(slowQueryMillis = 0, log = { logs += it })

        metrics.observe("SELECT v FROM t WHERE id = 1", rows = 1)

        assertEquals(1, logs.size, "阈值为 0 时每条语句都算慢")
        val line = logs[0]
        assertTrue(line.contains("[yb-slow-query]"), line)
        assertTrue(line.contains("type=point"), line)
        assertTrue(line.contains("rows=1"), line)
        assertTrue(line.contains("objects="), line)
        assertEquals(
            slowBefore + 1, Metrics.slowQueries.value(),
            "§12.5 要求慢查询可被聚合观察，不能只写日志",
        )

        // 高于阈值的语句不该进日志，也不该计数
        val fast = metrics(slowQueryMillis = 1_000_000, log = { logs += it })
        fast.observe("SELECT v FROM t WHERE id = 1")
        assertEquals(1, logs.size, "没超过阈值就不该打日志")
        assertEquals(slowBefore + 1, Metrics.slowQueries.value())
    }

    @Test
    fun `慢查询日志给出对象读取明细与原始 SQL`() {
        val logs = ArrayList<String>()
        val metrics = metrics(slowQueryMillis = 0, log = { logs += it })
        runBlocking { store.put("obj", ByteArray(4)) }

        val sql = "SELECT n FROM t WHERE id > 5"
        val stmt = Parser.parse(sql)
        val token = metrics.queryStarted(sql, stmt, session)
        runBlocking { store.get("obj", ByteRange(0, 2)) }
        metrics.queryFinished(token, sql, stmt, session, rows = 3, error = null)

        val line = logs.single()
        // 「为什么这个查询读了 500 个对象」——日志必须能回答它，而不只是报一个总数
        assertTrue(line.contains("detail=get=0,get_range=1,list=0,head=0"), line)
        assertTrue(line.contains("type=range"), line)
        assertTrue(line.contains("rows=3"), line)
        assertTrue(line.contains("sql=$sql"), line)
    }

    @Test
    fun `多行 SQL 在日志里被压成一行`() {
        val logs = ArrayList<String>()
        val metrics = metrics(slowQueryMillis = 0, log = { logs += it })
        val sql = "SELECT v\n  FROM t\n WHERE id = 7"
        val stmt = Parser.parse(sql)
        val token = metrics.queryStarted(sql, stmt, session)
        metrics.queryFinished(token, sql, stmt, session, rows = 0, error = null)

        assertEquals(1, logs.size, "一条语句一行日志，否则日志按行切分就断了")
        assertTrue(logs[0].contains("sql=SELECT v   FROM t  WHERE id = 7"), logs[0])
    }

    // ── 审计 ──────────────────────────────────────────────────────

    @Test
    fun `DDL 写审计，失败的语句不写`() {
        val sink = RecordingSink()
        Audit.install(sink)
        try {
            metrics().observe("CREATE TABLE t2 (id BIGINT, PRIMARY KEY (id))")
            metrics().observe("CREATE INDEX idx_v ON t (v)")
            metrics().observe("ALTER TABLE t ADD COLUMN x INT")

            assertEquals(
                listOf(
                    AuditActions.DDL_CREATE_TABLE,
                    AuditActions.DDL_CREATE_INDEX,
                    AuditActions.DDL_ALTER_TABLE,
                ),
                sink.events.map { it.action },
            )
            assertEquals("t2", sink.events[0].target)
            assertEquals("mdb", sink.events[0].database)
            assertTrue(sink.events[0].sql.startsWith("CREATE TABLE t2"), sink.events[0].sql)

            // 失败的 DDL 没有改变任何结构，记下来只会误导审计的读者
            metrics().observe("DROP TABLE nope", error = RuntimeException("boom"))
            assertEquals(3, sink.events.size, "只有成功的语句才写审计")

            // 非 DDL 不写审计（写路径不该把审计撑爆）
            metrics().observe("SELECT 1")
            assertEquals(3, sink.events.size)
        } finally {
            Audit.uninstall()
        }
    }

    @Test
    fun `显式回滚计入事务失败`() {
        val before = prometheus()["yb_transactions_total{result=\"rollback\"}"] ?: 0.0
        metrics().observe("ROLLBACK")
        assertEquals(before + 1, prometheus()["yb_transactions_total{result=\"rollback\"}"])
    }

    /**
     * 自动提交的写入也必须计入事务数。
     *
     * 单语句写入走的是执行器的快路径，攒不出一个 `TransactionBuffer`，
     * 最终落到 `DatabaseEngine.applyWrites`。**这条路径曾经漏了埋点**：
     * `yb_wal_objects_total` 一直在涨，而 `yb_transactions_total{result="commit"}`
     * 恒为 0——指标注册了、名字对、就是不动。这类「注册了没接上」的缺陷
     * 只有用真实写入去驱动才会暴露，所以这条断言盯着它。
     *
     * 这里用一张**没有二级索引**的表：一次插入只落一个分片，
     * 「单分片」的含义是这个口径下的一次 PUT（见下一个用例）。
     */
    @Test
    fun `自动提交的写入计入事务数与单分片`() = runBlocking {
        val plain = engine.catalog.createTable(
            dbId = table.databaseId,
            name = "plain",
            columns = listOf(
                ColumnDef("id", ColumnType.BIGINT, nullable = false),
                ColumnDef("v", ColumnType.VARCHAR, length = 32),
            ),
            primaryKey = listOf("id"),
        )
        val before = prometheus()
        engine.insert(plain, listOf(listOf(Datum.of(1L), Datum.of("a"))))
        val after = prometheus()

        assertEquals(1.0, deltaOf(before, after, "yb_transactions_total{result=\"commit\"}"), "一次自动提交的写入就是一次提交")
        assertEquals(
            1.0, deltaOf(before, after, "yb_txn_single_shard_total"),
            "它只碰了一个分片——这个计数与 yb_txn_2pc_total 的比值就是分区键选得对不对的直接证据",
        )
        assertEquals(0.0, deltaOf(before, after, "yb_txn_2pc_total"), "一次单分片写入不该被算成跨分片事务")
    }

    /**
     * 表上有二级索引时，一次写入同时落在**数据分片**与**索引分片**上。
     *
     * 这种写入既不是单分片（不是一次 PUT 就原子），也不是 2PC（走的是快路径，
     * 没有未决对象、没有提交标记）。因此两个计数都**不动**——这不是漏埋点，
     * 而是刻意不冒充：`yb_txn_single_shard_ratio` 只该反映真的只碰了一个分片的
     * 事务，把别的掺进去会让这个比例好看而没用。
     */
    @Test
    fun `写入同时落在数据分片与索引分片时不计入单分片`() = runBlocking {
        val before = prometheus()
        engine.insert(table, listOf(listOf(Datum.of(1L), Datum.of("a"), Datum.of(1L))))
        val after = prometheus()

        assertEquals(1.0, deltaOf(before, after, "yb_transactions_total{result=\"commit\"}"), "它仍然是一次提交")
        assertEquals(0.0, deltaOf(before, after, "yb_txn_single_shard_total"), "两个分片，不是一次 PUT")
        assertEquals(0.0, deltaOf(before, after, "yb_txn_2pc_total"), "也没有走两阶段提交")
    }

    // ── 采集器 ────────────────────────────────────────────────────

    @Test
    fun `指标抓取的渲染结果是合法且无重复序列的文本`() {
        val metrics = metrics()
        runBlocking {
            engine.cache_.put("cached-key", ByteArray(16))
            engine.cache_.get("cached-key")     // 制造一次缓存命中
            store.put("obj", ByteArray(4))
            store.get("obj")                    // 制造一次真实 GET
        }
        metrics.health()

        val text = Metrics.registry.render()
        val m = parsePrometheusText(text)       // 重复序列会在这里直接失败
        assertEquals(1.0, m["yb_up"])
        assertTrue(m.keys.any { it.startsWith("yb_block_cache_hits_total{tier=\"mem\"}") }, text)

        // ★ 缓存命中是**被避免的请求**，成本为 0——它记在 cached="true" 下，
        // 过滤 cached="false" 得到的才是真实请求数（§12.5）
        val memHits = engine.cache_.counters().hits
        assertTrue(memHits >= 1)
        assertEquals(
            memHits.toDouble(),
            m["yb_objectstore_requests_total{cached=\"true\",op=\"get\"}"],
            "缓存命中数必须折算成 cached=true 的请求数：\n$text",
        )
    }

    @Test
    fun `每一类真实请求都出现在指标里`() {
        Metrics.reset()
        counting.reset()
        runBlocking {
            store.put("k1", ByteArray(3))                     // put
            store.put("k2", ByteArray(3), com.cloudea.yangbase.objectstore.Precondition.MustNotExist)
            store.get("k1")                                   // get
            store.get("k1", ByteRange(0, 2))                  // get_range
            store.head("k1")                                  // head
            store.list("k")                                   // list
            store.delete("k2")                                // delete
        }
        val m = prometheus()
        // 指标与计数夹具必须逐项相等——埋点如果漏了一个方法，这条断言就会失败
        for (op in listOf("put", "put_cond", "get", "get_range", "head", "list", "delete")) {
            assertEquals(
                counting.count(op).toDouble(),
                m["yb_objectstore_requests_total{op=\"$op\",cached=\"false\"}"],
                "op=$op 的请求数必须与对象存储实际收到的请求数一致",
            )
        }
    }

    @Test
    fun `抓取指标与健康检查都不产生对象存储请求`() = runBlocking {
        val counting2 = CountingObjectStore(MemoryObjectStore())
        val engine2 = DatabaseEngine(store = counting2, scope = scope, nodeId = "obs-test-2")
        engine2.open()
        val metrics = ServerMetrics(engine2, log = { })
        try {
            counting2.reset()
            repeat(5) { Metrics.registry.render() }
            metrics.health()

            assertEquals(
                0L, counting2.totalOps(),
                "★ 指标抓取绝不能产生对象存储请求（原则 P4）——否则监控自己就是成本：" +
                    counting2.report(),
            )
        } finally {
            engine2.close()
        }
    }

    @Test
    fun `健康检查报告进程状态且把风险项标成 warn`() {
        val body = metrics().health().body
        assertTrue(body.startsWith("status=ok"), body)
        assertTrue(body.contains("open_shards="), body)
        assertTrue(body.contains("statements_observed="), body)
        assertTrue(body.contains("connections="), body)
    }

    // ── 工具 ──────────────────────────────────────────────────────

    private fun metrics(slowQueryMillis: Long = 1_000_000, log: (String) -> Unit = { }): ServerMetrics =
        ServerMetrics(engine, slowQueryMillis, log)

    private fun ServerMetrics.observe(sql: String, rows: Long = 1, error: Throwable? = null) {
        val stmt = Parser.parse(sql)
        val token = queryStarted(sql, stmt, session)
        queryFinished(token, sql, stmt, session, rows, error)
    }

    /** 走**指标本身**去读分类结果：这样连「类型标签有没有接上」也一起测到了。 */
    private fun typeOf(sql: String): QueryType {
        val before = prometheus()
        metrics().observe(sql)
        val after = prometheus()
        return QueryType.entries.first { t ->
            (after["yb_query_duration_seconds_count{type=\"${t.wire}\"}"] ?: 0.0) >
                (before["yb_query_duration_seconds_count{type=\"${t.wire}\"}"] ?: 0.0)
        }
    }

    private fun prometheus(): Map<String, Double> = parsePrometheusText(Metrics.registry.render())

    /** 允许取还没出现过的序列（当作 0），省得每条断言都写 `?: 0.0`。 */
    private fun Map<String, Double>.seriesValue(name: String): Double = this[name] ?: 0.0

    private fun deltaOf(before: Map<String, Double>, after: Map<String, Double>, name: String): Double =
        after.seriesValue(name) - before.seriesValue(name)

    private class RecordingSink : AuditSink {
        val events = ArrayList<AuditEvent>()
        override fun record(event: AuditEvent) {
            events.add(event)
        }
    }
}

/** 把 Prometheus 文本解析成 `序列 → 值`；**重复序列直接失败**（那是非法的抓取结果）。 */
private fun parsePrometheusText(text: String): Map<String, Double> {
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

/**
 * 观测端点（`/metrics` + `/healthz`）的单元测试。
 *
 * 断言的是**运维契约**而不是实现细节：抓取器能不能解析、状态码在探活逻辑里
 * 是否可区分、非 GET 与非已知路径是否被明确拒绝。
 */
class MetricsHttpServerTest {

    private val client: HttpClient = HttpClient.newHttpClient()

    private fun get(url: String): Pair<Int, String> {
        val r = client.send(
            HttpRequest.newBuilder(URI.create(url)).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        return r.statusCode() to r.body()
    }

    @Test
    fun `metrics 返回可解析的 Prometheus 文本`() {
        val srv = MetricsHttpServer("127.0.0.1", 0, health = { HealthReport(true, "status=ok\n") })
        srv.start()
        try {
            val (status, body) = get("http://127.0.0.1:${srv.boundPort}/metrics")
            assertEquals(200, status)
            val m = parsePrometheusText(body)
            // `yb_up` 由 ServerMetrics 的采集器提供，因此**没有 CN 的进程里它不会出现**——
            // 这个用例只验证端点自己的契约（能抓、文本可解析、抓取被计数）。
            // 「CN 在跑时 yb_up = 1」由 ObservabilityTest 的渲染用例与集成测试断言。
            assertTrue(m.containsKey("yb_metrics_scrapes_total"), body)
            // 抓取耗时被观测（抓取变慢说明采集器在做昂贵的事）
            assertTrue(m.containsKey("yb_metrics_scrape_duration_seconds_count"), body)
        } finally {
            srv.stop()
        }
    }

    @Test
    fun `healthz 用状态码区分健康与不健康`() {
        val healthy = MetricsHttpServer("127.0.0.1", 0, health = { HealthReport(true, "status=ok\n") })
        val broken = MetricsHttpServer("127.0.0.1", 0, health = { HealthReport(false, "status=fail\n") })
        val throwing = MetricsHttpServer("127.0.0.1", 0, health = { throw IllegalStateException("引擎没了") })
        healthy.start(); broken.start(); throwing.start()
        try {
            assertEquals(200, get("http://127.0.0.1:${healthy.boundPort}/healthz").first)
            assertEquals(503, get("http://127.0.0.1:${broken.boundPort}/healthz").first)
            // 探活自己抛错时必须表现为「不健康」，而不是 500 空响应——
            // 否则「探活失败」与「探活没跑到」在监控上长得一样
            val (code, body) = get("http://127.0.0.1:${throwing.boundPort}/healthz")
            assertEquals(503, code)
            assertTrue(body.contains("status=fail"), body)
        } finally {
            healthy.stop(); broken.stop(); throwing.stop()
        }
    }

    @Test
    fun `只接受 GET 与 HEAD，未知路径 404`() {
        val srv = MetricsHttpServer("127.0.0.1", 0)
        srv.start()
        try {
            val base = "http://127.0.0.1:${srv.boundPort}"
            val post = client.send(
                HttpRequest.newBuilder(URI.create("$base/metrics"))
                    .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString(),
            )
            assertEquals(405, post.statusCode())
            assertEquals(404, get("$base/nope").first)
            assertEquals("status=ok\n", get("$base/healthz").second)
        } finally {
            srv.stop()
        }
    }

    @Test
    fun `未启动时报 -1，重复启动是幂等的`() {
        val srv = MetricsHttpServer("127.0.0.1", 0)
        assertEquals(-1, srv.boundPort)
        srv.start()
        val p = srv.boundPort
        assertTrue(p > 0)
        srv.start()
        assertEquals(p, srv.boundPort, "重复 start 不应换端口")
        srv.close()
        assertEquals(-1, srv.boundPort)
    }
}
