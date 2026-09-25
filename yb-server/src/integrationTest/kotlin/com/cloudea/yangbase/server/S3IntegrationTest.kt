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
 * S3 集成测试：把整条链路跑在一个**真实的 S3 兼容 HTTP 服务**之上。
 *
 * 与 [EndToEndTest] 的区别在于持久层：这里走的是 S3 协议（HTTP + 条件写 + Range + LIST），
 * 而不是本地文件系统。因此它覆盖了只有在网络边界上才会暴露的问题——签名、
 * 头部编码、条件写的真实语义、分页 LIST。
 *
 * **两种后端，同一套断言**：
 *  - 默认：项目内置的 `EmbeddedS3Server`（真实 HTTP + 真实条件写），**不需要任何外部依赖**
 *  - 可选：用 `-P` 属性把 `YB_S3_ENDPOINT` 指向真实 MinIO / S3 / OSS，同一套用例原样跑
 *
 * ```
 * # 内嵌
 * ./gradlew integrationTest
 * # 真实 MinIO（⚠️ 必须用 -P 而不是环境变量：Gradle 守护进程不继承新 export 的变量）
 * ./gradlew integrationTest -PYB_S3_ENDPOINT=http://127.0.0.1:9000 -PYB_S3_BUCKET=yangbase \
 *   -PYB_S3_ACCESS_KEY=yangbase -PYB_S3_SECRET_KEY=yangbase123
 * ```
 */
@org.junit.jupiter.api.Tag("integration")
class S3IntegrationTest {

    private var s3: EmbeddedS3Server? = null
    private lateinit var server: YangBaseServer
    private var port: Int = 0
    private lateinit var counting: CountingObjectStore

    /** 配置读取：优先系统属性（`-P` 透传），回退到环境变量。 */
    private fun cfg(name: String): String? =
        System.getProperty(name) ?: System.getenv(name)

    /** 外部对象存储的端点。为 null 时用内嵌服务器。 */
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
        val raw = if (externalEndpoint != null) {
            // 真实对象存储（MinIO / S3 / OSS）。**必须先确保 bucket 存在**——
            // 真实对象存储不会自动建桶，否则首次 PUT 直接 NoSuchBucket。
            S3ObjectStore.ensureBucket(
                bucket = cfg("YB_S3_BUCKET") ?: "yangbase",
                endpoint = externalEndpoint,
                region = cfg("YB_S3_REGION") ?: "us-east-1",
                accessKey = cfg("YB_S3_ACCESS_KEY"),
                secretKey = cfg("YB_S3_SECRET_KEY"),
                pathStyle = externalPathStyle,
            )
            // 数据会跨测试运行持久化，因此先把上次的残留清掉
            val store = S3ObjectStore.create(
                bucket = cfg("YB_S3_BUCKET") ?: "yangbase",
                endpoint = externalEndpoint,
                region = cfg("YB_S3_REGION") ?: "us-east-1",
                accessKey = cfg("YB_S3_ACCESS_KEY"),
                secretKey = cfg("YB_S3_SECRET_KEY"),
                pathStyle = externalPathStyle,
                caps = externalCaps,
            )
            wipe(store)
            store
        } else {
            val emb = EmbeddedS3Server("yangbase-it")
            emb.start()
            s3 = emb
            S3ObjectStore.create(
                bucket = "yangbase-it",
                endpoint = emb.endpoint,
                region = "us-east-1",
                accessKey = "test",
                secretKey = "test",
            )
        }

        // 在真实 S3 客户端外面套一层计数器：这样能直接断言「这个查询打了几次对象存储」
        counting = CountingObjectStore(raw)

        server = YangBaseServer.forTesting(
            store = counting, port = 0,
            allowNonAtomicPointerSwitch = externalOss,
        )
        server.start()
        port = server.boundPort
        Class.forName("com.mysql.cj.jdbc.Driver")
        println("[S3IntegrationTest] 后端 = ${externalEndpoint ?: "内嵌 S3 兼容服务器"}")
    }

    /** 清空 bucket。真实对象存储会跨运行保留数据，必须显式清理。 */
    private fun wipe(store: com.cloudea.yangbase.objectstore.ObjectStore) = runBlocking {
        val keys = store.listAll("")
        if (keys.isNotEmpty()) println("[S3IntegrationTest] 清理上次残留的 ${keys.size} 个对象")
        for (k in keys) runCatching { store.delete(k) }
    }

    @AfterTest
    fun tearDown() {
        runCatching { server.stop() }
        s3?.let { runCatching { it.stop() } }
    }

    /** 直接测对象存储语义时用的 store —— 与主测试走同一后端。 */
    private fun externalOrEmbeddedStore(): S3ObjectStore = if (externalEndpoint != null) {
        S3ObjectStore.create(
            bucket = cfg("YB_S3_BUCKET") ?: "yangbase",
            endpoint = externalEndpoint,
            region = cfg("YB_S3_REGION") ?: "us-east-1",
            accessKey = cfg("YB_S3_ACCESS_KEY"),
            secretKey = cfg("YB_S3_SECRET_KEY"),
                pathStyle = externalPathStyle,
                caps = externalCaps,
        )
    } else {
        S3ObjectStore.create(
            bucket = "yangbase-it", endpoint = s3!!.endpoint,
            region = "us-east-1", accessKey = "t", secretKey = "t",
        )
    }

    private fun connect(db: String? = null): Connection {
        val url = buildString {
            append("jdbc:mysql://127.0.0.1:$port/")
            if (db != null) append(db)
            append("?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC")
        }
        return DriverManager.getConnection(url, "root", "")
    }

    private fun Statement.query(sql: String): List<List<Any?>> =
        executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            val out = ArrayList<List<Any?>>()
            while (rs.next()) out.add((1..n).map { rs.getObject(it) })
            out
        }

    private fun flushAll() = runBlocking { server.engine.flushAll() }

    @Test
    fun `完整 CRUD 跑在真实 S3 协议之上`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE s3db")
                st.execute("USE s3db")
                st.execute("CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(32), PRIMARY KEY (id))")

                st.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
                assertEquals(3, st.query("SELECT id FROM t ORDER BY id").size)
                assertEquals("b", st.query("SELECT v FROM t WHERE id = 2")[0][0])

                st.executeUpdate("UPDATE t SET v = 'B' WHERE id = 2")
                assertEquals("B", st.query("SELECT v FROM t WHERE id = 2")[0][0])

                // 刷盘：把数据从 MemTable 推到 SST（走真实的 multipart 之外的 PUT 路径）
                flushAll()
                assertEquals(3, st.query("SELECT id FROM t").size, "刷盘后数据必须仍可读")

                st.executeUpdate("DELETE FROM t WHERE id = 3")
                assertEquals(2, st.query("SELECT id FROM t").size)
            }
        }
    }

    @Test
    fun `对象存储真的收到了请求 - 不是空跑`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE s3db2"); st.execute("USE s3db2")
                st.execute("CREATE TABLE x (id BIGINT NOT NULL, PRIMARY KEY (id))")
                counting.reset()
                st.execute("INSERT INTO x VALUES (1),(2),(3)")
                assertTrue(counting.count("put") + counting.count("put_cond") > 0,
                    "写入必须真的落到对象存储上。${counting.report()}")
            }
        }
    }

    @Test
    fun `点查稳态的 GET 次数在预算内`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE s3db3"); st.execute("USE s3db3")
                st.execute("CREATE TABLE p (id BIGINT NOT NULL, v VARCHAR(32), PRIMARY KEY (id))")
                st.execute("INSERT INTO p VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
                flushAll()

                st.query("SELECT v FROM p WHERE id = 3")   // 预热：填充 manifest 与 .idx 缓存

                counting.reset()
                st.query("SELECT v FROM p WHERE id = 4")   // 测量：数据块已在块缓存里

                val reads = counting.count("get") + counting.count("get_range") + counting.count("head")
                assertTrue(reads <= 2, "点查稳态的读请求数超预算（≤2）。${counting.report()}")
            }
        }
    }

    @Test
    fun `COUNT 走元数据路径 - 零数据读取`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE s3db4"); st.execute("USE s3db4")
                st.execute("CREATE TABLE cnt (id BIGINT NOT NULL, s VARCHAR(16), PRIMARY KEY (id))")
                val values = (1..300).joinToString(",") { "($it, '${if (it % 4 == 0) "x" else "y"}')" }
                st.execute("INSERT INTO cnt VALUES $values")
                flushAll()

                assertEquals(300L, st.query("SELECT COUNT(*) FROM cnt")[0][0])

                // 再刷一次盘，确保 manifest 的行数水位已推进
                flushAll()
                counting.reset()
                val n = st.query("SELECT COUNT(*) FROM cnt")[0][0]
                assertEquals(300L, n, "COUNT 必须精确")

                val dataReads = counting.count("get_range")
                assertEquals(0L, dataReads,
                    "COUNT 不应读取任何数据块（应走 manifest 的精确行数）。${counting.report()}")
            }
        }
    }

    @Test
    fun `重启后从 S3 恢复 - 计算节点无状态`() {
        connect().use { c ->
            c.createStatement().use { st ->
                st.execute("CREATE DATABASE s3dur"); st.execute("USE s3dur")
                st.execute("CREATE TABLE d (id BIGINT NOT NULL, v VARCHAR(32), PRIMARY KEY (id))")
                val values = (1..200).joinToString(",") { "($it, 'v$it')" }
                st.execute("INSERT INTO d VALUES $values")
                flushAll()
            }
        }

        // 销毁整个计算节点
        server.stop()

        // 用一个全新的 CN 实例重新打开同一份 S3 数据
        val revived = YangBaseServer.forTesting(
            store = counting, port = 0,
            allowNonAtomicPointerSwitch = externalOss,
        )
        revived.start()
        try {
            val url = "jdbc:mysql://127.0.0.1:${revived.boundPort}/s3dur?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
            DriverManager.getConnection(url, "root", "").use { c ->
                c.createStatement().use { st ->
                    assertEquals(200L, st.query("SELECT COUNT(*) FROM d")[0][0], "重启后行数应保持")
                    assertEquals("v7", st.query("SELECT v FROM d WHERE id = 7")[0][0])
                }
            }
        } finally {
            runCatching { revived.stop() }
        }
        server = revived
    }

    @Test
    fun `条件写语义真的生效 - 并发写同一 key 只有一个成功`() {
        val store = externalOrEmbeddedStore()
        runBlocking {
            val key = "conditional-test/lease.json"
            store.put(key, "owner-1".toByteArray())
            val etag = store.head(key).etag

            // 阿里云 OSS 没有 ETag CAS（实测 `If-Match` → 400 NotImplemented）。
            // 这里要断言的是**如实拒绝**，而不是"假装成功"：真正的互斥在 OSS 上
            // 必须由 MS 的租约提供（§4.3），对象存储这一层做不到
            if (externalOss) {
                var casRefused = false
                try {
                    store.put(key, "owner-2".toByteArray(), com.cloudea.yangbase.objectstore.Precondition.MustMatch(etag))
                } catch (_: com.cloudea.yangbase.common.CapabilityNotSupportedException) {
                    casRefused = true
                }
                assertTrue(casRefused, "OSS 上没有 If-Match：必须**明确拒绝**，不能悄悄退化成 last-writer-wins")

                // 原子创建仍然要有：OSS 用 x-oss-forbid-overwrite 提供它
                var createRejectedOss = false
                try {
                    store.put(key, "xxx".toByteArray(), com.cloudea.yangbase.objectstore.Precondition.MustNotExist)
                } catch (_: com.cloudea.yangbase.common.PreconditionFailedException) {
                    createRejectedOss = true
                }
                assertTrue(createRejectedOss, "MustNotExist 对已存在的对象必须失败（OSS: 409 FileAlreadyExists）")
                assertEquals("owner-1", String(store.get(key)), "MustNotExist 失败时不能覆盖")
                return@runBlocking
            }

            // 第一个写者用 CAS 成功
            store.put(key, "owner-2".toByteArray(), com.cloudea.yangbase.objectstore.Precondition.MustMatch(etag))

            // 陈旧的 etag 必须被拒绝 —— 这是 fencing 的基础
            var rejected = false
            try {
                store.put(key, "owner-stale".toByteArray(), com.cloudea.yangbase.objectstore.Precondition.MustMatch(etag))
            } catch (_: com.cloudea.yangbase.common.ConcurrentModificationException) {
                rejected = true
            } catch (_: com.cloudea.yangbase.common.PreconditionFailedException) {
                rejected = true
            }
            assertTrue(rejected, "陈旧 etag 的写入必须被拒绝，否则 fencing 失效")

            // MustNotExist 也必须生效
            var createRejected = false
            try {
                store.put(key, "xxx".toByteArray(), com.cloudea.yangbase.objectstore.Precondition.MustNotExist)
            } catch (_: com.cloudea.yangbase.common.PreconditionFailedException) {
                createRejected = true
            }
            assertTrue(createRejected, "MustNotExist 对已存在的对象必须失败")
        }
    }

    @Test
    fun `LIST 分页正确 - 恢复路径依赖它`() {
        val store = externalOrEmbeddedStore()
        runBlocking {
            // 写入足够多的对象触发分页（每页 7 个）
            for (i in 1..25) {
                store.put("paging/obj-${"%03d".format(i)}", "v$i".toByteArray())
            }
            val all = store.listAll("paging/")
            assertEquals(25, all.size, "跨页列举必须拿到全部对象")
            assertEquals(all.sorted(), all, "LIST 必须按字典序返回——恢复路径依赖这个顺序")
            assertTrue(all.first() == "paging/obj-001")
        }
    }

    @Test
    fun `Range 读取返回正确字节 - 点查只取需要的那一段`() {
        val store = externalOrEmbeddedStore()
        runBlocking {
            val data = ByteArray(1000) { (it % 256).toByte() }
            store.put("range-test/blob", data)
            val slice = store.get("range-test/blob", com.cloudea.yangbase.objectstore.ByteRange(100, 199))
            assertEquals(100, slice.size)
            for (i in 0 until 100) assertEquals(data[100 + i], slice[i], "Range 读取的字节必须精确对上")
        }
    }
}
