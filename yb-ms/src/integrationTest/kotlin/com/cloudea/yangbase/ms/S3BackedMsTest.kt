package com.cloudea.yangbase.ms

import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.EmbeddedS3Server
import com.cloudea.yangbase.objectstore.S3ObjectStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * **日志落在真实对象存储上**的 MS 集群（而不是内存 store）。
 *
 * 这一层是必要的：内存 store 的 PUT 是纳秒级的，而真实后端（S3 / 内嵌 S3 走 HTTP）
 * 是毫秒级的——**共识的活性问题只在后者出现**。这个文件的由来正是如此：
 * 协调模式基准在把 MS 的日志放到内嵌 S3 之后，租约授予报"多数派未能确认"，
 * 而同一套用例在内存 store 上全绿。
 */
@org.junit.jupiter.api.Tag("integration")
class S3BackedMsTest {

    private var s3: EmbeddedS3Server? = null
    private lateinit var store: CountingObjectStore
    private val scope = CoroutineScope(SupervisorJob())

    /** 配置读取：优先系统属性（`-P` 透传），与 yb-server 的集成测试同一套约定。 */
    private fun cfg(name: String): String? = System.getProperty(name) ?: System.getenv(name)

    private val externalEndpoint: String? = cfg("YB_S3_ENDPOINT")

    @BeforeTest
    fun setUp() {
        if (externalEndpoint != null) {
            // 真实对象存储（MinIO / S3 / OSS）：MS 的**日志真的落在它上面**，
            // 这是共识活性唯一能被真实检验的地方——每次追加都是一次真实 PUT
            S3ObjectStore.ensureBucket(
                bucket = cfg("YB_S3_BUCKET") ?: "yangbase-ms-s3",
                endpoint = externalEndpoint,
                region = cfg("YB_S3_REGION") ?: "us-east-1",
                accessKey = cfg("YB_S3_ACCESS_KEY"),
                secretKey = cfg("YB_S3_SECRET_KEY"),
                pathStyle = cfg("YB_S3_PATH_STYLE")?.toBooleanStrictOrNull(),
            )
            val raw = S3ObjectStore.create(
                bucket = cfg("YB_S3_BUCKET") ?: "yangbase-ms-s3",
                endpoint = externalEndpoint,
                region = cfg("YB_S3_REGION") ?: "us-east-1",
                accessKey = cfg("YB_S3_ACCESS_KEY"),
                secretKey = cfg("YB_S3_SECRET_KEY"),
                pathStyle = cfg("YB_S3_PATH_STYLE")?.toBooleanStrictOrNull(),
                caps = if (cfg("YB_S3_OSS")?.toBooleanStrictOrNull() == true) {
                    com.cloudea.yangbase.objectstore.ObjectStoreCapabilities.OSS
                } else {
                    com.cloudea.yangbase.objectstore.ObjectStoreCapabilities.FULL
                },
            )
            // 跨运行会留下上一次的 Raft 日志与状态：不清掉的话，新集群会带着旧任期/
            // 旧日志启动（MS 是允许整体重启的，但"上一次运行的日志"不是重启，
            // 那会让任期与提交位点从中间开始，断言随之错位）
            runBlocking {
                val keys = raw.listAll("")
                if (keys.isNotEmpty()) println("[S3BackedMsTest] 清理上次残留的 ${keys.size} 个对象")
                for (k in keys) runCatching { raw.delete(k) }
            }
            store = CountingObjectStore(raw)
        } else {
            val emb = EmbeddedS3Server("yangbase-ms-s3")
            emb.start()
            s3 = emb
            val raw = S3ObjectStore.create("yangbase-ms-s3", emb.endpoint, "us-east-1", "t", "t")
            store = CountingObjectStore(raw)
        }
    }

    @AfterTest
    fun tearDown() {
        runCatching { s3?.stop() }
    }

    @Test
    fun `日志落 S3 时租约授予与时间戳都应当成功`() = runBlocking {
        val cluster = InProcessMetaCluster.start(
            3, store, scope,
            // 真实后端上一次追加就是一次真实 PUT（几十毫秒）：超时太小会让集群
            // 在负载下反复换领导者（§4.4 的实测教训），所以这里跟着放大。
            //
            // 内嵌 S3 也算"真实后端"：它走的是真 HTTP。CI 的 2 核 runner 上，
            // 300–600ms 的选举超时**低于噪声底盘**（GC + Gradle + 两个 JVM + HTTP），
            // 于是这个用例偶发变成"测选举"，而不是它要测的"日志落在 S3 上"。
            // 选举敏捷性有专门的用例（`RaftHighLatencyElectionTest`），不靠这里守。
            electionTimeoutMillis = 1_500L..3_000L,
            // 提交预算显式放大到 5s（默认推导值在这里只有 300ms）：CI 的 2 核 runner 上
            // S3 落盘路径（HTTP + 本地内嵌 S3）会被 CPU 争抢拉到几百毫秒，
            // 300ms 内凑不齐多数派确认，表现为「提交超时（300ms）」。
            // 这里只放宽客户端等待，不动选举超时——重选行为仍然是这个用例要测的东西。
            commitTimeoutMillis = 5_000,
        )
        try {
            cluster.awaitLeader(if (externalEndpoint != null) 60_000 else 10_000)
            val client = MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, "cn-s3")
            // 时间戳
            assertTrue(client.tso.next().value > 0)
            // 租约：这一步在基准里曾经失败（多数派未能确认）
            val shard = ShardId(TableId(1), PartitionId(0))
            val lease = client.leases.acquire(shard)
            assertEquals(1L, lease.epoch)
            client.leases.release(lease)
            val again = client.leases.acquire(shard)
            assertTrue(again.epoch > lease.epoch)
            client.leases.release(again)
        } finally {
            cluster.stopAll()
        }
    }
}
