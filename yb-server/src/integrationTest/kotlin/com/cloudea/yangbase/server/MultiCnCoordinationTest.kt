package com.cloudea.yangbase.server

import com.cloudea.yangbase.ms.MetaServiceNode
import com.cloudea.yangbase.ms.TcpMetaTransport
import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.EmbeddedS3Server
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.S3ObjectStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import java.net.ServerSocket
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * **多 CN 的端到端**（设计文档 §2.1 的部署视图：`CN × N + MS × 3`）。
 *
 * ## 这个文件补的是什么
 *
 * 在此之前，租赁 / fencing / 行锁的断言**全部在单进程内**（`LeaseAndRecoveryTest`
 * 用两个 `LeaseManager`，一致性测试用同一个 JVM 里的客户端）。而多 CN 的失效模式
 * 恰恰只在**跨进程**时才出现：
 *
 * - 两个 CN 各自缓存的 schema / manifest 是否真的能看到对方提交的数据（§4.5）；
 * - 行锁是否真的挡住了**另一个进程**里的事务（§4.4 的锁表在 MS 上）；
 * - 接管分片时 epoch 是否被超越、旧写者的数据是否被当成新写者的日志（§4.3）。
 *
 * 所以这里起的是**真东西**：一个内嵌 S3（真实 HTTP + 真实条件写）、3 个走 **TCP**
 * 的 MS 节点、2 个各自独立装配的 CN 进程（同一 JVM、不同端口与服务实例，但只通过
 * 对象存储与 MS 通信——这正是生产里两台机器的通信路径）。
 *
 * ## 为什么这比"两个 LeaseManager"强
 *
 * 两个 `LeaseManager` 共享一个进程内的 `ObjectStore` 对象，很多错误（比如忘了
 * 把 epoch 写进 WAL 前缀、schema 缓存不失效）在那里根本不会发生。而这里任何一方
 * 少了一个环节，断言就会失败——因为唯一能让两边看到同一个世界的东西，
 * 就是对象存储与 MS 本身。
 */
@org.junit.jupiter.api.Tag("integration")
class MultiCnCoordinationTest {

    private var s3: EmbeddedS3Server? = null
    private lateinit var store: ObjectStore
    private lateinit var counting: CountingObjectStore
    private val scope = CoroutineScope(SupervisorJob())
    private val msNodes = ArrayList<MetaServiceNode>()

    /** MS 集群的地址（`127.0.0.1:<port>`）。 */
    private var msPeers: List<String> = emptyList()

    private val cnServers = ArrayList<YangBaseServer>()

    /** 配置读取：优先系统属性（`-P` 透传），与 `S3IntegrationTest` 同一套约定。 */
    private fun cfg(name: String): String? = System.getProperty(name) ?: System.getenv(name)

    /** 外部对象存储的端点。为 null 时用内嵌服务器。 */
    private val externalEndpoint: String? = cfg("YB_S3_ENDPOINT")
    private val externalPathStyle: Boolean? = cfg("YB_S3_PATH_STYLE")?.toBooleanStrictOrNull()
    private val externalOss: Boolean = cfg("YB_S3_OSS")?.toBooleanStrictOrNull() ?: false

    @BeforeTest
    fun setUp() {
        if (externalEndpoint != null) {
            // ── 真实对象存储（MinIO / S3 / OSS）────────────────────────
            S3ObjectStore.ensureBucket(
                bucket = cfg("YB_S3_BUCKET") ?: "yangbase-multicn",
                endpoint = externalEndpoint,
                region = cfg("YB_S3_REGION") ?: "us-east-1",
                accessKey = cfg("YB_S3_ACCESS_KEY"),
                secretKey = cfg("YB_S3_SECRET_KEY"),
                pathStyle = externalPathStyle,
            )
            val raw = S3ObjectStore.create(
                bucket = cfg("YB_S3_BUCKET") ?: "yangbase-multicn",
                endpoint = externalEndpoint,
                region = cfg("YB_S3_REGION") ?: "us-east-1",
                accessKey = cfg("YB_S3_ACCESS_KEY"),
                secretKey = cfg("YB_S3_SECRET_KEY"),
                pathStyle = externalPathStyle,
                caps = if (externalOss) com.cloudea.yangbase.objectstore.ObjectStoreCapabilities.OSS
                else com.cloudea.yangbase.objectstore.ObjectStoreCapabilities.FULL,
            )
            // 数据跨运行持久化：**必须先清空**，否则上一个用例/上一次运行的
            // catalog 与分片会被当成"已经是这个数据库"的状态，断言全线错位
            wipe(raw)
            counting = CountingObjectStore(raw)
            store = counting
        } else {
            val emb = EmbeddedS3Server("yangbase-multicn")
            emb.start()
            s3 = emb
            val raw = S3ObjectStore.create(
                bucket = "yangbase-multicn", endpoint = emb.endpoint,
                region = "us-east-1", accessKey = "t", secretKey = "t",
            )
            counting = CountingObjectStore(raw)
            store = counting
        }

        // 3 个 MS 节点，**走真实 TCP**：端口先探出来再显式绑定。
        // 用系统分配的端口（而不是写死 9001–9003）是为了能在同一台机器上并行跑用例
        val ports = (1..3).map { freePort() }
        msPeers = ports.map { "127.0.0.1:$it" }
        val transport = TcpMetaTransport()
        for ((i, addr) in msPeers.withIndex()) {
            val node = MetaServiceNode(
                nodeId = "ms-${i + 1}",
                address = addr,
                peers = msPeers.filter { it != addr },
                store = store,
                scope = scope,
                transport = transport,
                serveTcp = true,
                bindHost = "127.0.0.1",
                bindPort = ports[i],
                // ⚠️ 选举超时在线性后端上必须**放大**：MS 的每次追加都是一次真实 PUT
                // （内存 store 是纳秒级，真实后端是几十毫秒）。超时太小，一次慢复制
                // 就足以让跟随者发起选举，集群在负载下反复换领导者——不报错，
                // 只是每次换都丢掉锁表（§4.4）。生产默认值也是这个量级
                electionTimeoutMillis = if (externalEndpoint != null) 1_500L..3_000L else 200L..400L,
            )
            runBlocking { node.start() }
            msNodes.add(node)
        }
        awaitLeader()
        Class.forName("com.mysql.cj.jdbc.Driver")
    }

    /** 清空桶。真实对象存储会跨运行保留数据，必须显式清理（与 `S3IntegrationTest` 同一做法）。 */
    private fun wipe(target: ObjectStore) = runBlocking {
        val keys = target.listAll("")
        if (keys.isNotEmpty()) println("[MultiCnCoordinationTest] 清理上次残留的 ${keys.size} 个对象")
        for (k in keys) runCatching { target.delete(k) }
    }

    @AfterTest
    fun tearDown() {
        cnServers.forEach { runCatching { it.stop() } }
        cnServers.clear()
        runBlocking { msNodes.forEach { runCatching { it.stop() } } }
        msNodes.clear()
        runCatching { s3?.stop() }
    }

    /** 探一个空闲端口。**用完就关**：MS 绑定它之前有一小段窗口，但端口被占会直接启动失败（可见，不会静默）。 */
    private fun freePort(): Int = ServerSocket(0).use { it.localPort }

    private fun awaitLeader() = runBlocking {
        val deadline = System.currentTimeMillis() + if (externalEndpoint != null) 60_000 else 10_000
        while (System.currentTimeMillis() < deadline) {
            if (msNodes.any { it.raft.currentRole == com.cloudea.yangbase.ms.RaftNode.Role.LEADER && !it.stopped }) return@runBlocking
            kotlinx.coroutines.delay(20)
        }
        throw IllegalStateException("MS 集群没有选出领导者：${msNodes.map { it.describe() }}")
    }

    /** 起一个 CN：`coordMode = ms`，连上面那个 MS 集群。 */
    private fun startCn(
        nodeId: String,
        leaseTtlMillis: Long = 3_000,
        /**
         * 时间戳窗口（毫秒）。0 = 每次取时间戳都要问 MS（验证降级用）。
         *
         * 端到端用例取 100ms：**跨 CN 的可见性延迟 ≤ 一个窗口**，窗口小一点
         * 用例才跑得快，而这条延迟本身是窗口模型的固有代价（见
         * `WindowTso.batchMillis`）。
         */
        tsoBatchMillis: Long = 100,
    ): YangBaseServer {
        val server = YangBaseServer(
            ServerConfig(
                host = "127.0.0.1", port = 0, nodeId = nodeId,
                coordMode = "ms",
                msPeers = msPeers,
                msNodeId = nodeId,          // CN 自己不跑 MS
                leaseTtlMillis = leaseTtlMillis,
                tsoBatchMillis = tsoBatchMillis,
                // OSS 这类后端没有 ETag CAS：写入需要显式接受指针切换的非原子降级
                allowNonAtomicPointerSwitch = externalOss,
            ),
            injectedStore = counting,
        )
        server.start()
        cnServers.add(server)
        return server
    }

    private fun connect(server: YangBaseServer, db: String = "mdb"): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:${server.boundPort}/$db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun Connection.exec(sql: String) = createStatement().use { it.execute(sql) }

    private fun Connection.queryLong(sql: String): Long = createStatement().use { s ->
        s.executeQuery(sql).use { rs -> assertTrue(rs.next(), "查询没有返回行：$sql"); rs.getLong(1) }
    }

    /**
     * 等到某个查询出现期望的值（上限 [timeoutMillis]）。
     *
     * 只用于**跨 CN 可见性**的断言：延迟 ≤ 一个时间戳窗口，见用例里的说明。
     * 其它断言一律用即时比较——把"最终一致"当成默认会让真正的问题被重试掩盖。
     */
    private fun Connection.assertEventuallyVisible(
        sql: String,
        expected: Long,
        what: String,
        timeoutMillis: Long = 2_000,
    ) {
        val deadline = System.currentTimeMillis() + timeoutMillis
        var last: Long? = null
        while (System.currentTimeMillis() < deadline) {
            last = runCatching { queryLong(sql) }.getOrNull()
            if (last == expected) return
            Thread.sleep(20)
        }
        assertEquals(expected, last, "$what（等待 ${timeoutMillis}ms 仍未可见）")
    }

    // ── 跨 CN 的数据可见性 ────────────────────────────────────────

    /**
     * ★ **两个 CN 各自写入，并且互相看得见。**
     *
     * 让 CN2 看见 CN1 的写入靠的不是"等一会儿"：CN1 提交时的 `commit_ts` 来自
     * MS 发的那一段窗口，而 CN2 的 `read_ts` 只能来自 MS 水位（只会更大），
     * 所以"先提交的必然可见"是**结构上**成立的。
     *
     * 反过来，没有 MS 时这条性质依赖"各 CN 时钟大致同步"——那是一个假设，
     * 而这里是一个不变量。
     */
    @Test
    fun `两个 CN 各自写入且互相可见`() {
        val cn1 = startCn("cn-1")
        val cn2 = startCn("cn-2")
        try {
            // CN1 建表并写入
            connect(cn1, "mdb").use { c ->
                c.exec("CREATE DATABASE IF NOT EXISTS mdb")
                c.exec("USE mdb")
                c.exec("DROP TABLE IF EXISTS t_a")
                c.exec("CREATE TABLE t_a (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
                c.exec("INSERT INTO t_a VALUES (1,10),(2,20),(3,30)")
            }
            // CN2 读：必须看得见 CN1 刚提交的行。
            //
            // ⚠️ 这里**允许在一个窗口（100ms）之内重试**，它不是抖动：
            // 时间戳区间是**独占**的，本 CN 的 `read_ts` 可能还低于另一个 CN
            // 刚从它自己的窗口里发出的 `commit_ts`（那个值最多超前一个窗口）。
            // 这是当前窗口模型的固有语义，写在用户文档里（compatibility.md §4.4.3）
            connect(cn2, "mdb").use { c ->
                c.exec("USE mdb")
                c.assertEventuallyVisible("SELECT COUNT(*) FROM t_a", 3L, "CN2 必须看得见 CN1 刚提交的行")
                assertEquals(20L, c.queryLong("SELECT v FROM t_a WHERE id = 2"))
            }
            // CN2 写自己的表（不同分片 → 不争租约），CN1 也要看得见
            connect(cn2, "mdb").use { c ->
                c.exec("USE mdb")
                c.exec("DROP TABLE IF EXISTS t_b")
                c.exec("CREATE TABLE t_b (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
                c.exec("INSERT INTO t_b VALUES (1,100)")
            }
            connect(cn1, "mdb").use { c ->
                c.exec("USE mdb")
                c.assertEventuallyVisible("SELECT v FROM t_b WHERE id = 1", 100L, "CN1 必须看得见 CN2 刚提交的行")
            }
        } finally {
            cn1.stop(); cn2.stop()
        }
    }

    // ── 跨 CN 的目录传播 ──────────────────────────────────────────

    /**
     * ★ **CN1 刚建的库，CN2 的新连接必须立刻能用。**
     *
     * 目录在 CN 之间是**异步传播**的（轮询版本号 / MS 通知，§4.5），所以这里有一个
     * 真实存在的窗口：CN1 上 `CREATE DATABASE d` 返回时，`d` 已经写进对象存储，
     * 但 CN2 的内存目录要等它下一次刷新才知道。窗口里若只按本地目录回答，
     * CN2 会对一个**确实存在**的库报 `Unknown database 'd'`——使用者看到的是
     * 「我刚建的库没了」，重试一下又好了，于是它会被当成偶发抖动而永远查不清。
     *
     * 因此 `USE` / `COM_INIT_DB` / 握手初始库这三条路径都必须**先刷新一次目录再判**
     * （[com.cloudea.yangbase.txn.Catalog.findDatabaseRefreshing]）。
     *
     * 这个用例刻意**不加等待、不做重试**，并且 CN2 侧只靠连接串里的库名
     * （握手路径，不补一句 `USE`）——一旦退回"只看本地目录"，它立刻红。
     */
    @Test
    fun `CN1 新建的库，CN2 的新连接必须立刻能用`() {
        val cn1 = startCn("cn-cat-1")
        val cn2 = startCn("cn-cat-2")
        try {
            connect(cn1, "").use { c ->
                c.exec("CREATE DATABASE catw")
                c.exec("USE catw")
                c.exec("DROP TABLE IF EXISTS t_cat")
                c.exec("CREATE TABLE t_cat (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
                c.exec("INSERT INTO t_cat VALUES (1, 7)")
            }
            connect(cn2, "catw").use { c ->
                assertEquals(7L, c.queryLong("SELECT v FROM t_cat WHERE id = 1"))
            }
        } finally {
            cn1.stop(); cn2.stop()
        }
    }

    // ── 跨 CN 的行锁 ──────────────────────────────────────────────

    /**
     * ★ **`FOR UPDATE` 挡住的是另一个 CN 上的事务**——这才是行锁存在的意义。
     *
     * 锁表在 MS 上（§4.4），而不是在各个 CN 的内存里：本进程内的一个并发集合
     * 对另一个进程毫无约束力，那种实现会让"读了改"看起来安全、实际不安全。
     */
    @Test
    fun `行锁跨 CN 生效`() {
        val cn1 = startCn("cn-lock-1")
        val cn2 = startCn("cn-lock-2")
        try {
            connect(cn1, "lk").use { c ->
                c.exec("CREATE DATABASE IF NOT EXISTS lk")
                c.exec("USE lk")
                c.exec("DROP TABLE IF EXISTS acct")
                c.exec("CREATE TABLE acct (id BIGINT NOT NULL, balance INT, PRIMARY KEY (id))")
                c.exec("INSERT INTO acct VALUES (1,100)")
            }
            connect(cn1, "lk").use { a ->
                connect(cn2, "lk").use { b ->
                    a.exec("USE lk")
                    b.exec("USE lk")
                    a.autoCommit = false
                    b.autoCommit = false
                    try {
                        // CN1 锁住这一行
                        assertEquals(100L, a.queryLong("SELECT balance FROM acct WHERE id = 1 FOR UPDATE"))
                        // CN2 等 1 秒拿不到 → 1205（把等待上限调小，别让用例等 50 秒）
                        b.exec("SET SESSION innodb_lock_wait_timeout = 1")
                        val e = assertFailsWith<SQLException>("CN2 必须被 CN1 的行锁挡住") {
                            b.queryLong("SELECT balance FROM acct WHERE id = 1 FOR UPDATE")
                        }
                        assertEquals(1205, e.errorCode, "锁等待超时必须报 1205：${e.message}")
                        // CN1 提交之后 CN2 立刻能拿到（不是等 TTL）
                        a.commit()
                        assertEquals(100L, b.queryLong("SELECT balance FROM acct WHERE id = 1 FOR UPDATE"))
                        b.commit()
                    } finally {
                        runCatching { a.rollback() }
                        runCatching { b.rollback() }
                        a.autoCommit = true
                        b.autoCommit = true
                    }
                }
            }
        } finally {
            cn1.stop(); cn2.stop()
        }
    }

    // ── 分片接管与 fencing ────────────────────────────────────────

    /**
     * ★ **一个 CN 停下之后，另一个 CN 接管分片：epoch 被超越，且数据不丢。**
     *
     * 这是 `wal/e{epoch}/` 那道防线的端到端验证：接管者的写入必须落在**更大的**
     * epoch 前缀下，于是旧写者当年写在同一分片里的对象不会被当成新写者的日志
     * 重放进去（那是静默的数据损坏）。
     *
     * 同时验证"接管之后能读到前任写下的数据"——分片交接不能丢数据。
     */
    @Test
    fun `分片接管后 epoch 被超越且数据不丢`() {
        val cn1 = startCn("cn-take-1", leaseTtlMillis = 2_000)
        try {
            connect(cn1, "tk").use { c ->
                c.exec("CREATE DATABASE IF NOT EXISTS tk")
                c.exec("USE tk")
                c.exec("DROP TABLE IF EXISTS t")
                c.exec("CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
                c.exec("INSERT INTO t VALUES (1,10),(2,20)")
            }
        } finally {
            // 干净关闭：写者会释放租约（`LsmStore.close` 先落盘再释放），
            // 所以接管不需要等 TTL——这正是"正确关闭"该有的行为
            cn1.stop()
        }
        val epochBefore = maxWalEpoch()

        val cn2 = startCn("cn-take-2", leaseTtlMillis = 2_000)
        try {
            connect(cn2, "tk").use { c ->
                c.exec("USE tk")
                // 前任写的数据必须还在
                assertEquals(2L, c.queryLong("SELECT COUNT(*) FROM t"), "分片接管后不能丢前任写下的行")
                // 同一个分片继续写
                c.exec("INSERT INTO t VALUES (3,30)")
                assertEquals(3L, c.queryLong("SELECT COUNT(*) FROM t"))
            }
            val epochAfter = maxWalEpoch()
            assertTrue(
                epochAfter > epochBefore,
                "接管者必须拿到**更大的** epoch（旧写者写在 wal/e$epochBefore/ 下的对象才能被忽略）：" +
                    "before=$epochBefore after=$epochAfter",
            )
        } finally {
            cn2.stop()
        }
    }

    // ── MS 不可用时的降级（§4.2）──────────────────────────────────

    /**
     * ★ **MS 全部不可用时：读还能继续，写明确失败。**
     *
     * §4.2 把这条降级语义定成了刻意的选择：读是缓存友好、用户可感知代价小的操作；
     * 而写事务在拿不到新的时间戳区间时必须**明确报错**——静默地用一个可能重复的
     * 时间戳继续写，会让 MVCC 失去全序，且完全无声。
     */
    @Test
    fun `MS 全部不可用时读继续写失败`() {
        // 窗口压到 0：每一次取时间戳都必须问 MS，于是"MS 不可用"这条路径立刻可见
        val cn = startCn("cn-degrade", leaseTtlMillis = 2_000, tsoBatchMillis = 0)
        try {
            connect(cn, "dg").use { c ->
                c.exec("CREATE DATABASE IF NOT EXISTS dg")
                c.exec("USE dg")
                c.exec("DROP TABLE IF EXISTS t")
                c.exec("CREATE TABLE t (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
                c.exec("INSERT INTO t VALUES (1,10),(2,20)")

                // 把 3 个 MS 节点全部停掉
                runBlocking { msNodes.forEach { it.stop() } }

                // 读：继续可用——用**已经取到的**水位（§4.2 的"剩余额度"）。
                // 它会退化成一个陈旧的快照（最多一个窗口时长），但那是安全的：
                // 快照隔离下读到一个更早的时刻不会看到未提交或半份的数据
                assertEquals(2L, c.queryLong("SELECT COUNT(*) FROM t"), "MS 不可用时读应当继续（用已取到的水位）")

                // 先直接问一次写路径：窗口已用尽，取时间戳必须失败
                val tsoFailed = runCatching {
                    kotlinx.coroutines.runBlocking { cn.coordination!!.tso.next() }
                }.isFailure
                assertTrue(tsoFailed, "窗口用尽且 MS 不可用时，取 commit_ts 必须失败（而不是发一个可能重复的号）")

                // 写：必须明确失败，而不是"看起来成功了"
                val e = assertFailsWith<SQLException>("MS 不可用时写事务必须明确失败") {
                    c.exec("INSERT INTO t VALUES (3,30)")
                }
                assertTrue(
                    e.message!!.contains("元数据服务不可用") || e.message!!.contains("没有领导者"),
                    "错误信息要指向真正的原因（MS 不可用）：${e.message}",
                )
                // 表里仍然是 2 行——失败是**真的**失败，没有半份写入
                assertEquals(2L, c.queryLong("SELECT COUNT(*) FROM t"))
            }
        } finally {
            cn.stop()
        }
    }

    /**
     * 对象存储里出现过的**最大 WAL epoch**（从对象键里读）。
     *
     * 刻意不读"租约对象"：MS 模式的租约在共识状态里（`meta/ms/...` 的日志与快照），
     * 根本没有 per-shard 的租约对象。而 `wal/e{N}/` 这个前缀**两种模式都有**——
     * 它才是 fencing 真正依赖的东西（旧写者的对象落在旧前缀下，因而不被引用）。
     */
    private fun maxWalEpoch(): Long = runBlocking {
        val keys = counting.listAll("")
        keys.mapNotNull { Regex("/wal/e(\\d+)/").find(it)?.groupValues?.get(1)?.toLongOrNull() }
            .maxOrNull() ?: 0L
    }
}
