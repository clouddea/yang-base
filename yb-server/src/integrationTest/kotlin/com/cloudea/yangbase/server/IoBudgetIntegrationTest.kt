package com.cloudea.yangbase.server

import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.EmbeddedS3Server
import com.cloudea.yangbase.objectstore.ObjectStore
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
 * **IO 预算的集成测试**（设计文档 §14.2）。
 *
 * 这个文件的存在理由写在 CLAUDE.md 里：
 *
 * > 比测延迟更可靠的是**测请求数**——延迟受环境影响大，请求数是确定性的。
 *
 * 延迟断言在 CI 上必然不稳定，跑在 MinIO 上还是内嵌服务器上结果也不一样；
 * 而「这次点查打了 1 次 GET」是一个**不随环境变化**的事实。
 * 本系统全部的性能主张最终都落在「引入了多少对象存储请求」上（原则 P4），
 * 因此每条主张都应该有一条对应的、确定性的断言。
 *
 * **两种后端，同一套断言**：
 *  - 默认：项目内置的 `EmbeddedS3Server`（真实 HTTP + 真实条件写），无需外部依赖
 *  - 可选：设 `YB_S3_ENDPOINT` 指向真实 MinIO / S3，同一套用例原样跑
 */
@org.junit.jupiter.api.Tag("integration")
class IoBudgetIntegrationTest {

    private var s3: EmbeddedS3Server? = null
    private lateinit var server: YangBaseServer
    private lateinit var counting: CountingObjectStore
    private var port: Int = 0

    private fun cfg(name: String): String? = System.getProperty(name) ?: System.getenv(name)
    private val externalEndpoint: String? = cfg("YB_S3_ENDPOINT")

    /**
     * 指向真实后端时的两个开关（都用 `-P` 传，见 `scripts/minio.sh`）：
     *  - `YB_S3_PATH_STYLE`：寻址风格。**阿里云 OSS 必须 false**（它拒绝路径式）
     *  - `YB_S3_OSS`：按 OSS 的原语工作（`x-oss-forbid-overwrite` + 无 ETag CAS），
     *    并显式接受 manifest 指针切换的非原子降级
     */
    private val externalPathStyle: Boolean? = cfg("YB_S3_PATH_STYLE")?.toBooleanStrictOrNull()
    private val externalOss: Boolean = cfg("YB_S3_OSS")?.toBooleanStrictOrNull() ?: false
    private val externalCaps: com.cloudea.yangbase.objectstore.ObjectStoreCapabilities
        get() = if (externalOss) com.cloudea.yangbase.objectstore.ObjectStoreCapabilities.OSS
        else com.cloudea.yangbase.objectstore.ObjectStoreCapabilities.FULL

    @BeforeTest
    fun setUp() {
        val raw: ObjectStore = if (externalEndpoint != null) {
            S3ObjectStore.ensureBucket(
                bucket = cfg("YB_S3_BUCKET") ?: "yangbase-io",
                endpoint = externalEndpoint,
                region = cfg("YB_S3_REGION") ?: "us-east-1",
                accessKey = cfg("YB_S3_ACCESS_KEY"),
                secretKey = cfg("YB_S3_SECRET_KEY"),
            )
            S3ObjectStore.create(
                bucket = cfg("YB_S3_BUCKET") ?: "yangbase-io",
                endpoint = externalEndpoint,
                region = cfg("YB_S3_REGION") ?: "us-east-1",
                accessKey = cfg("YB_S3_ACCESS_KEY"),
                secretKey = cfg("YB_S3_SECRET_KEY"),
                pathStyle = externalPathStyle,
                caps = externalCaps,
            ).also { wipe(it) }
        } else {
            val emb = EmbeddedS3Server("yangbase-io")
            emb.start()
            s3 = emb
            S3ObjectStore.create(
                bucket = "yangbase-io", endpoint = emb.endpoint,
                region = "us-east-1", accessKey = "t", secretKey = "t",
            )
        }

        // recordKeys 打开：断言失败时能直接看到「这几百次 GET 到底打在哪」，
        // 否则只能对着一个数字猜
        counting = CountingObjectStore(raw, recordKeys = true)
        server = YangBaseServer.forTesting(
            store = counting, port = 0,
            allowNonAtomicPointerSwitch = externalOss,
        )
        server.start()
        port = server.boundPort
        Class.forName("com.mysql.cj.jdbc.Driver")
    }

    private fun wipe(store: ObjectStore) = runBlocking {
        store.listAll("").forEach { runCatching { store.delete(it) } }
    }

    @AfterTest
    fun tearDown() {
        runCatching { server.stop() }
        s3?.let { runCatching { it.stop() } }
    }

    private fun connect(): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/iodb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    /** 执行并丢弃结果。 */
    private fun Connection.st(block: (Statement) -> Unit) = createStatement().use(block)

    /** 执行并**返回**结果——需要拿到查询结果时用它，`st` 的返回类型是 Unit。 */
    private fun <T> Connection.q(block: (Statement) -> T): T = createStatement().use(block)

    private fun Statement.query(sql: String): List<List<Any?>> =
        executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            val out = ArrayList<List<Any?>>()
            while (rs.next()) out.add((1..n).map { rs.getObject(it) })
            out
        }

    private fun setupTable(c: Connection, ddl: String, rows: Int = 0) {
        c.st { s ->
            s.execute("CREATE DATABASE IF NOT EXISTS iodb")
            s.execute("USE iodb")
            s.execute("DROP TABLE IF EXISTS t")
            s.execute(ddl)
        }
        if (rows > 0) {
            c.st { s ->
                // 分批插入，避免单条 SQL 过大
                (1..rows step 500).forEach { start ->
                    val values = (start until minOf(start + 500, rows + 1))
                        .joinToString(",") { "($it, 'u$it', ${it % 50}, 'c${it % 7}')" }
                    s.execute("INSERT INTO t VALUES $values")
                }
            }
        }
    }

    /** 让 MemTable 落盘，把数据推进 SST 层——否则一切都在内存里，测不出 IO。 */
    private fun flush() = runBlocking { server.engine.flushAll() }

    // ── 点查 ──────────────────────────────────────────────────────

    @Test
    fun `点查稳态只应产生 1 次 GET`() {
        connect().use { c ->
            setupTable(c, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), PRIMARY KEY (id))", rows = 1000)
            flush()
            c.st { s ->
                s.query("SELECT v FROM t WHERE id = 42")     // 预热：填充 manifest / .idx 缓存

                counting.reset()
                val r = s.query("SELECT v FROM t WHERE id = 43")   // 测量
                assertEquals(1, r.size)

                // ★ 这条断言守护整个存储层设计：
                // manifest 与 .idx 常驻内存 → 点查只剩一次按字节范围的 .dat 读取
                val gets = counting.count("get")
                assertTrue(gets <= 1, "稳态点查应 ≤ 1 次 GET，实际 $gets 次：" + counting.summary())
            }
        }
    }

    /**
     * 冷启动的一次点查。
     *
     * 这个数字比稳态大，**不是缺陷而是结构**：它包含「把分片打开」的一次性成本——
     * 读 manifest 指针、读 manifest、LIST 并重放 WAL、加载 `.idx`。
     * 这部分在一个 CN 的生命周期内只付一次，之后所有查询都是稳态。
     *
     * 断言这个上限的价值在于：**分片打开成本一旦无意识地膨胀，必须在这里被发现**。
     * 曾经 `startWriter` 为了拿 seq 又读了一遍 manifest 指针——每次打开都白付一次 GET，
     * 就是靠这条断言暴露出来的。
     */
    @Test
    fun `冷启动点查的请求数有上界`() {
        connect().use { c ->
            setupTable(c, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), PRIMARY KEY (id))", rows = 1000)
            flush()

            // 复用同一个对象存储，但换一个全新的引擎实例：缓存是进程级的，
            // 新引擎意味着 manifest 与 .idx 缓存全空——这才是真正的冷启动。
            // 只清计数器是测不出冷缓存的，线程池里的东西还在。
            val cold = YangBaseServer.forTesting(
            store = counting, port = 0,
            allowNonAtomicPointerSwitch = externalOss,
        )
            cold.start()
            try {
                counting.reset()
                val table = cold.engine.catalog.allTables().first { it.name == "t" }
                val got = runBlocking { cold.engine.get(table, pk(999L), cold.engine.currentReadTs()) }
                requireNotNull(got) { "冷起点查必须仍然读得到数据" }

                val total = counting.totalOps()
                assertTrue(
                    total <= 8,
                    "冷启动点查（含打开分片）应 ≤ 8 次请求，实际 $total 次：" + counting.summary() +
                        "\n请求明细：\n  " + counting.keyReport(),
                )
                // 数据本身只应该被精确地读一次——范围读而不是整对象下载
                assertEquals(
                    1L, counting.count("get_range"),
                    "点查应只做一次范围读，而不是下载整个数据对象",
                )
            } finally {
                runCatching { cold.stop() }
            }
        }
    }

    /**
     * 查询成本与「分片打开」成本必须分开看。
     *
     * 这条断言把**缓存对 IO 的贡献**钉死：同一个查询连打两次，
     * 第二次的对象存储请求必须是 0。
     */
    @Test
    fun `重复执行同一个查询不产生新的对象存储请求`() {
        connect().use { c ->
            setupTable(c, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), PRIMARY KEY (id))", rows = 1000)
            flush()
            c.st { s ->
                s.query("SELECT v FROM t WHERE id = 42")   // 预热

                counting.reset()
                repeat(20) { s.query("SELECT v FROM t WHERE id = 42") }
                assertEquals(
                    0L, counting.totalOps(),
                    "缓存命中时重复查询应为 0 次对象存储请求，实际：" + counting.summary(),
                )
            }
        }
    }

    // ── COUNT(*) ──────────────────────────────────────────────────

    @Test
    fun `COUNT 应完全不读数据对象`() {
        connect().use { c ->
            setupTable(c, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), PRIMARY KEY (id))", rows = 2000)
            flush()
            c.st { s -> s.query("SELECT COUNT(*) FROM t") }   // 预热

            counting.reset()
            val n = c.q { s -> s.query("SELECT COUNT(*) FROM t")[0][0] as Long }
            assertEquals(2000L, n)

            // 精确行数由 manifest 维护，与数据量无关
            assertEquals(0, counting.count("get"), "COUNT(*) 应 0 次 GET：" + counting.summary())
        }
    }

    @Test
    fun `COUNT 比全表扫描读得少`() {
        connect().use { c ->
            setupTable(c, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), PRIMARY KEY (id))", rows = 5000)
            flush()
            // COUNT 走 manifest 上的精确行数，但前提是分片已打开；
            // 先打开它，否则测到的是「分片打开成本」而不是「查询成本」
            c.st { s -> s.query("SELECT COUNT(*) FROM t") }
        }

        val count = measureCold { c -> c.q { s -> s.query("SELECT COUNT(*) FROM t")[0][0] as Long } }
        val scan = measureCold { c -> c.q { s -> assertEquals(5000, s.query("SELECT id FROM t").size) } }

        assertEquals(5000L, count.first)
        assertTrue(
            count.second.bytesOut("get_range") < scan.second.bytesOut("get_range"),
            "COUNT 读的数据字节必须比全表扫描少。\nCOUNT：${count.second}\n扫描：${scan.second}",
        )
    }

    /**
     * ★ 计数列上的**谓词计数**（0 次 GET）也必须按列类型解释字面量。
     *
     * `status` 是 VARCHAR 计数列，而 `200` 是数字字面量。扫描路径会按列类型把它
     * 转成 `'200'` 再比较（能查到行），元数据路径若不转，就会拿整数编码去查一张
     * 按字符串编码的「值 → 计数」表——查不到就是 **0**，而 0 是一个完全正常的计数。
     *
     * 两句 SQL 是同一个问题：`status = 200`（走元数据）与
     * `CONCAT(status,'') = '200'`（表达式让它无法走元数据，只能扫描）。
     * 两条路径必须给出同一个数字，这也是这条测试能自证的方式。
     */
    @Test
    fun `计数列的元数据计数也要按列类型解释字面量`() {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS iodb")
                s.execute("USE iodb")
                s.execute("DROP TABLE IF EXISTS cnt")
                s.execute("CREATE TABLE cnt (id BIGINT NOT NULL, status VARCHAR(16), PRIMARY KEY (id))")
                s.execute("INSERT INTO cnt VALUES (1,'200'),(2,'200'),(3,'500')")
            }
            flush()
            c.st { s -> s.query("SELECT COUNT(*) FROM cnt") }      // 预热：打开分片

            counting.reset()
            val viaMetadata = c.q { s -> s.query("SELECT COUNT(*) FROM cnt WHERE status = 200")[0][0] as Long }
            val dataRead = counting.bytesOut("get_range")
            val viaScan = c.q { s -> s.query("SELECT COUNT(*) FROM cnt WHERE CONCAT(status,'') = '200'")[0][0] as Long }

            assertEquals(0L, dataRead, "谓词计数应当不读数据块，实际读了 $dataRead 字节：" + counting.summary())
            assertEquals(viaScan, viaMetadata, "元数据计数与扫描必须给出同一个数字")
            assertEquals(2L, viaMetadata)
        }
    }

    // ── 窗口函数与标量相关子查询（都是"行已取回之后"的算子）──────

    /**
     * ★ 窗口函数**不引入任何额外读取**：它在一批已经取回的行上算。
     *
     * 断言方式：同一个表的两次冷启动查询——一次分组聚合、一次窗口聚合——
     * 读的**数据字节必须相同**。窗口若被实现成"每行重扫一遍表"，
     * 这里会看到成倍的差异。
     */
    @Test
    fun `窗口函数不引入额外读取`() {
        connect().use { c ->
            setupTable(c, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), PRIMARY KEY (id))", rows = 2000)
            flush()
        }
        val plain = measureCold { c -> c.q { s -> s.query("SELECT city, SUM(n) FROM t GROUP BY city").size } }
        val windowed = measureCold { c ->
            c.q { s -> s.query("SELECT id, SUM(n) OVER (PARTITION BY city) FROM t").size }
        }
        assertEquals(2000, windowed.first)
        assertEquals(
            plain.second.bytesOut("get_range"), windowed.second.bytesOut("get_range"),
            "窗口聚合与分组聚合读的数据字节必须相同（前者不该额外扫表）：\n" +
                "分组 ${plain.second}\n窗口 ${windowed.second}",
        )
    }

    /**
     * ★ 标量相关子查询的内层**只扫一次**，不随外层行数增长。
     *
     * 这是它和"每外层行跑一次子查询"的全部区别所在。200 行外层 × 每行扫一次内层
     * 会让读取量差两个数量级——而那种实现的结果**完全正确**，只是把对象存储的
     * IO 打爆了，所以只能靠请求数断言发现。
     */
    @Test
    fun `标量相关子查询的内层只扫一次`() {
        connect().use { c ->
            setupTable(c, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), PRIMARY KEY (id))", rows = 1000)
            c.st { s ->
                s.execute("DROP TABLE IF EXISTS probe_outer")
                s.execute("CREATE TABLE probe_outer (id BIGINT NOT NULL, k INT, PRIMARY KEY (id))")
                // 200 行，每行的 k 命中内层的 20 行（n = id % 50）
                (0 until 200).forEach { i ->
                    s.execute("INSERT INTO probe_outer VALUES (${i + 1}, ${i % 50})")
                }
            }
            flush()
        }
        val outer = measureCold { c -> c.q { s -> s.query("SELECT id FROM probe_outer").size } }
        val inner = measureCold { c -> c.q { s -> s.query("SELECT id FROM t").size } }
        val correlated = measureCold { c ->
            c.q { s ->
                s.query("SELECT o.id, (SELECT MAX(n) FROM t i WHERE i.n = o.k) FROM probe_outer o").size
            }
        }
        assertEquals(200, correlated.first)
        val oneOfEach = outer.second.bytesOut("get_range") + inner.second.bytesOut("get_range")
        assertTrue(
            correlated.second.bytesOut("get_range") <= oneOfEach + 64 * 1024,
            "相关子查询应当只扫内层一次：外层 ${outer.second.bytesOut("get_range")} + " +
                "内层 ${inner.second.bytesOut("get_range")} 字节，实际读了 " +
                "${correlated.second.bytesOut("get_range")} 字节（若按行驱动会是内层的 200 倍）",
        )
    }

    // ── 只读事务 ──────────────────────────────────────────────────

    @Test
    fun `只读事务开始不产生任何对象存储请求`() {
        connect().use { c ->
            setupTable(c, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), PRIMARY KEY (id))", rows = 100)
            flush()
            c.st { s -> s.query("SELECT COUNT(*) FROM t") }   // 预热，排除首次加载的噪声

            counting.reset()
            c.autoCommit = false
            c.st { s ->
                s.query("SELECT COUNT(*) FROM t")             // 事务内的第一次读
            }
            val duringTx = counting.count("get")
            c.commit()

            // 设计文档 §4.2：TSO 预取让事务开始不需要访问对象存储。
            // 允许 1 次是给 TSO 预取池耗尽时的兜底留余量。
            assertTrue(
                duringTx <= 1,
                "只读事务开始应 0 次对象存储请求（含 TSO 分配），实际 $duringTx 次：" + counting.summary(),
            )
        }
    }

    // ── 组提交 ────────────────────────────────────────────────────

    @Test
    fun `组提交把每次事务的 PUT 数摊薄`() {
        connect().use { c ->
            setupTable(c, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), PRIMARY KEY (id))")

            val threads = 8
            val perThread = 25
            counting.reset()
            val pool = java.util.concurrent.Executors.newFixedThreadPool(threads)
            val done = java.util.concurrent.CountDownLatch(threads)
            repeat(threads) { t ->
                pool.submit {
                    try {
                        connect().use { cc ->
                            cc.st { s -> s.execute("USE iodb") }
                            for (i in 1..perThread) {
                                cc.st { s -> s.execute("INSERT INTO t VALUES (${t * 1000 + i}, 'v', $i, 'c')") }
                            }
                        }
                    } finally {
                        done.countDown()
                    }
                }
            }
            done.await()
            pool.shutdown()

            val puts = counting.count("put")
            val txs = threads * perThread
            // 设计文档 §6.3.4：组提交把 N 个并发事务攒成少数几个 WAL 对象
            assertTrue(
                puts < txs,
                "组提交后 PUT 数($puts)应少于事务数($txs)——否则组提交没生效：" + counting.summary(),
            )
            println("[IO] 组提交：$txs 个事务产生 $puts 个 PUT，摊薄 ${"%.1f".format(txs.toDouble() / puts)} tx/PUT")
        }
    }

    // ── 二级索引 ──────────────────────────────────────────────────

    /**
     * 二级索引 vs 全表扫描。
     *
     * ⚠️ 必须在**零缓存**的引擎上测：缓存一旦生效，两条路径都变成 0 次请求，
     * 「谁读得更少」就退化成 0 与 0 的比较。这类断言看起来在测性能，
     * 实际上什么都没测——比不写更糟，因为它给出虚假的安全感。
     */
    @Test
    fun `二级索引查找的 IO 显著低于全表扫描`() {
        connect().use { c ->
            setupTable(
                c,
                """
                CREATE TABLE t (
                    id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8),
                    PRIMARY KEY (id), KEY idx_n (n)
                )
                """.trimIndent(),
                rows = 4000,
            )
            flush()
        }

        val viaIndex = measureCold { c -> c.q { s -> s.query("SELECT id FROM t WHERE n = 7").size } }
        // `n + 0` 让谓词无法下推到索引，强制走全表扫描
        val viaScan = measureCold { c -> c.q { s -> s.query("SELECT id FROM t WHERE n + 0 = 7").size } }

        assertEquals(viaScan.first, viaIndex.first, "两条路径的结果必须一致")
        assertTrue(
            viaIndex.second.bytesOut("get_range") < viaScan.second.bytesOut("get_range"),
            "索引路径读的字节(${viaIndex.second.bytesOut("get_range")}B)应少于全表扫描" +
                "(${viaScan.second.bytesOut("get_range")}B)——否则索引只是白占存储。\n" +
                "索引路径：" + viaIndex.second + "\n扫描路径：" + viaScan.second,
        )
        println(
            "[IO] 索引 ${viaIndex.second.bytesOut("get_range")}B vs 扫描 ${viaScan.second.bytesOut("get_range")}B"
        )
    }

    /**
     * 在一个**全新的零缓存引擎**上跑一段查询，返回 (结果, 请求统计)。
     *
     * 零缓存是关键：只有这样，「读了多少」才等于「这个查询结构上需要读多少」，
     * 而不是「上次跑完还剩什么在内存里」。
     */
    private fun <T> measureCold(block: (Connection) -> T): Pair<T, IoSnapshot> {
        val cold = YangBaseServer.forTesting(
            store = counting, port = 0, cacheBytes = 0,
            allowNonAtomicPointerSwitch = externalOss,
        )
        cold.start()
        try {
            val cc = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:${cold.boundPort}/iodb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "root", "",
            )
            cc.use {
                counting.reset()
                val r = block(cc)
                return r to IoSnapshot(
                    counting.snapshot(),
                    listOf("get", "get_range", "put", "list", "head").associateWith { counting.bytesOut(it) },
                )
            }
        } finally {
            runCatching { cold.stop() }
        }
    }

    // ── 分区裁剪 ──────────────────────────────────────────────────

    @Test
    fun `分区裁剪让区间外查询不产生任何数据读取`() {
        connect().use { c ->
            setupTable(
                c,
                """
                CREATE TABLE t (
                    id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8),
                    PRIMARY KEY (id)
                )
                PARTITION BY RANGE (id) (
                    PARTITION p0 VALUES LESS THAN (1000),
                    PARTITION p1 VALUES LESS THAN (2000),
                    PARTITION p2 VALUES LESS THAN (3000),
                    PARTITION pmax VALUES LESS THAN MAXVALUE
                )
                """.trimIndent(),
                rows = 4000,
            )
            flush()

            // 全部数据都在 id < 4000 的范围内；查 > 100000 应当被裁剪成零 IO
            counting.reset()
            val empty = c.q { s -> s.query("SELECT id FROM t WHERE id > 100000") }
            assertEquals(0, empty.size)
            assertEquals(
                0L, counting.count("get"),
                "所有分区都被裁掉时应产生 0 次 GET，实际：" + counting.summary(),
            )
        }
    }

    @Test
    fun `分区裁剪减少区间查询读取的字节`() {
        connect().use { c ->
            val ddl = """
                CREATE TABLE t (
                    id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8),
                    PRIMARY KEY (id)
                )
                PARTITION BY RANGE (id) (
                    PARTITION p0 VALUES LESS THAN (1000),
                    PARTITION p1 VALUES LESS THAN (2000),
                    PARTITION p2 VALUES LESS THAN (3000),
                    PARTITION p3 VALUES LESS THAN (4000),
                    PARTITION pmax VALUES LESS THAN MAXVALUE
                )
            """.trimIndent()
            setupTable(c, ddl, rows = 4000)
            flush()
        }

        // 同样用零缓存引擎——缓存会把两边都压成 0
        val narrow = measureCold { c -> c.q { s -> s.query("SELECT id FROM t WHERE id >= 100 AND id < 600").size } }
        val wide = measureCold { c -> c.q { s -> s.query("SELECT id FROM t").size } }

        assertEquals(500, narrow.first)
        assertEquals(4000, wide.first)
        assertTrue(
            narrow.second.bytesOut("get_range") < wide.second.bytesOut("get_range"),
            "窄区间应比全表读得少——否则分区裁剪没生效。\n" +
                "窄区间：${narrow.second}\n全表：${wide.second}",
        )
        println("[IO] 分区裁剪：窄区间 ${narrow.second} vs 全表 ${wide.second}")
    }

    // ── 崩溃恢复 ──────────────────────────────────────────────────

    @Test
    fun `崩溃恢复后不丢已提交的数据`() {
        val ddl = "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), PRIMARY KEY (id))"
        val committed: List<Long>
        run {
            val c = connect()
            c.use {
                setupTable(c, ddl)
                c.st { s ->
                    for (i in 1..50) s.execute("INSERT INTO t VALUES ($i, 'v$i', $i, 'c')")
                }
                committed = c.q { s -> s.query("SELECT id FROM t").map { it[0] as Long } }
                assertEquals(50, committed.size)
            }
        }

        // 直接销毁进程内的一切（不 flush、不优雅关闭），再用同一个对象存储重开
        server.stop()
        val revived = YangBaseServer.forTesting(
            store = counting, port = 0,
            allowNonAtomicPointerSwitch = externalOss,
        )
        revived.start()
        try {
            val table = revived.engine.catalog.allTables().first { it.name == "t" }
            val rows = runBlocking { revived.engine.scanAll(table, revived.engine.currentReadTs()) }
            assertEquals(
                committed.sorted(), rows.map { it[0].asLong() }.sorted(),
                "崩溃恢复后已提交的行必须一行不少（它们在上一次 WAL 里）",
            )
        } finally {
            runCatching { revived.stop() }
        }
    }

    // ── 快照隔离 ──────────────────────────────────────────────────

    @Test
    fun `长读事务不会被并发写入干扰 - 快照隔离`() {
        connect().use { reader ->
            setupTable(reader, "CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), n INT, city VARCHAR(8), PRIMARY KEY (id))")
            reader.st { s -> s.execute("INSERT INTO t VALUES (1,'a',1,'c'),(2,'b',2,'c')") }

            reader.autoCommit = false
            val first = reader.q { s -> s.query("SELECT id FROM t").map { it[0] as Long } }
            assertEquals(listOf(1L, 2L), first.sorted())

            // 另一个连接插入并提交
            connect().use { writer ->
                writer.st { s ->
                    s.execute("USE iodb")
                    s.execute("INSERT INTO t VALUES (3,'c',3,'c')")
                }
            }

            val second = reader.q { s -> s.query("SELECT id FROM t").map { it[0] as Long } }
            assertEquals(
                listOf(1L, 2L), second.sorted(),
                "读事务在自己的快照内重复读必须看到同一批行",
            )
            reader.commit()

            val after = reader.q { s -> s.query("SELECT id FROM t").map { it[0] as Long } }
            assertEquals(listOf(1L, 2L, 3L), after.sorted(), "提交后应看到新数据")
        }
    }

    // ── 工具 ──────────────────────────────────────────────────────

    private fun pk(v: Long): ByteArray =
        com.cloudea.yangbase.common.KeyEncoder.encodeDatum(com.cloudea.yangbase.common.Datum.I64(v), com.cloudea.yangbase.common.ColumnType.BIGINT)
}

/**
 * 一次测量的 IO 快照。
 *
 * 不能直接用 `counting.snapshot()`——那是活的计数器，下一次测量会把它清掉；
 * 断言失败时打印出来的就会是**另一次**的明细。
 */
private class IoSnapshot(val ops: Map<String, Long>, val outBytes: Map<String, Long>) {
    /** 某类操作读入的字节数。 */
    fun bytesOut(op: String): Long = outBytes[op] ?: 0L
    override fun toString() = ops.entries.sortedByDescending { it.value }
        .joinToString(", ") { "${it.key}=${it.value}" } +
        "；读入字节 total=" + ops.keys.sumOf { bytesOut(it) }
}

/** 把计数器的分布打出来，断言失败时能直接看到钱花在哪。 */
private fun CountingObjectStore.summary(): String {
    val ops = listOf("get", "put", "delete", "list", "head").filter { count(it) > 0 }
    return ops.joinToString(", ") { "$it=${count(it)}" } + "；出流量 ${ops.sumOf { bytesOut(it) }} B"
}
