package com.cloudea.yangbase.server

import com.cloudea.yangbase.common.CapabilityNotSupportedException
import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.PreconditionFailedException
import com.cloudea.yangbase.objectstore.AtomicCreateStyle
import com.cloudea.yangbase.objectstore.EmbeddedS3Server
import com.cloudea.yangbase.objectstore.ObjectStoreCapabilities
import com.cloudea.yangbase.objectstore.Precondition
import com.cloudea.yangbase.objectstore.S3ObjectStore
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * **阿里云 OSS 兼容性测试**（2026-09-25 实测后补）。
 *
 * OSS 与 S3 的差别是**服务端强加的**，三条都用内嵌服务器如实复现：
 *
 * | 原语 | S3 / MinIO | 阿里云 OSS（实测） |
 * |---|---|---|
 * | 原子创建 | `If-None-Match: *` → 412 | `x-oss-forbid-overwrite` → **409 FileAlreadyExists** |
 * | 比较交换 | `If-Match` → 412 | ❌ 400 `NotImplemented` |
 * | 寻址 | 路径式可用 | ❌ 路径式被拒（`SecondLevelDomainForbidden`） |
 *
 * 所以这里测两件事：
 *  1. **OSS 模式下整条写入链路真的能跑**（建表-插入-查询-重开）；
 *  2. 没有 CAS 时，指针切换要**显式同意**才降级——默认必须拒绝（P6：宁可拒绝，
 *     也不要悄悄失去互斥）。
 */
class OssCompatibilityTest {

    private lateinit var s3: EmbeddedS3Server
    private lateinit var store: S3ObjectStore
    private lateinit var server: YangBaseServer
    private var port: Int = 0
    private lateinit var dataDir: java.nio.file.Path

    private fun ossStore(): S3ObjectStore = S3ObjectStore.create(
        bucket = "oss-it",
        endpoint = s3.endpoint,
        region = "cn-hangzhou",
        accessKey = "t",
        secretKey = "t",
        caps = ObjectStoreCapabilities.OSS,
    )

    @BeforeTest
    fun setUp() {
        s3 = EmbeddedS3Server("oss-it").apply { ossStyle = true; start() }
        store = ossStore()
        dataDir = Files.createTempDirectory("yangbase-oss")
    }

    @AfterTest
    fun tearDown() {
        runCatching { server.stop() }
        runCatching { store.close() }
        runCatching { s3.stop() }
        runCatching { dataDir.toFile().deleteRecursively() }
    }

    // ── 原语层 ────────────────────────────────────────────────────

    @Test
    fun `OSS 风格下原子创建走 forbid-overwrite 并用 409 表示已存在`() = runBlocking {
        store.put("k1", "v".toByteArray(), Precondition.MustNotExist)   // 首次：成功
        assertEquals(1, s3.requestCount("PUT"))

        val e = assertFailsWith<PreconditionFailedException> {
            store.put("k1", "v2".toByteArray(), Precondition.MustNotExist)
        }
        // 409 在 S3 语义下是"并发冲突、可重试"，在 OSS 语义下是"已经存在"这个**确定结论**：
        // 必须映射成前置条件失败，而不是让重试层对着一个永远不会成功的请求反复重发
        assertTrue(e.message!!.contains("已存在"), e.message)
        assertEquals("v", String(store.get("k1")), "冲突时不能覆盖")
    }

    @Test
    fun `没有 CAS 的后端上 MustMatch 必须明确拒绝`() = runBlocking {
        store.put("k3", "v".toByteArray())
        assertFailsWith<CapabilityNotSupportedException> {
            store.put("k3", "v2".toByteArray(), Precondition.MustMatch(com.cloudea.yangbase.objectstore.ETag("\"x\"")))
        }
    }

    @Test
    fun `OSS 预设如实声明能力`() {
        val caps = ObjectStoreCapabilities.OSS
        assertTrue(caps.conditionalCreate, "原子创建由 x-oss-forbid-overwrite 提供")
        assertEquals(AtomicCreateStyle.FORBID_OVERWRITE, caps.atomicCreateStyle)
        assertTrue(!caps.conditionalUpdate, "OSS 没有 ETag CAS——这一点必须如实声明，否则 CAS 会退化成 last-writer-wins")
    }

    // ── 端到端：OSS 模式下的写入链路 ──────────────────────────────

    @Test
    fun `OSS 模式下的引擎能建表插入查询并重开`() {
        server = YangBaseServer.forTesting(store = store, port = 0, allowNonAtomicPointerSwitch = true)
        server.start()
        port = server.boundPort
        Class.forName("com.mysql.cj.jdbc.Driver")

        DriverManager.getConnection(
            "jdbc:mysql://127.0.0.1:$port/ossdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
            "root", "",
        ).use { c: Connection ->
            c.createStatement().use { s: Statement ->
                s.execute("CREATE DATABASE IF NOT EXISTS ossdb")
                s.execute("USE ossdb")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute("CREATE TABLE t (id BIGINT NOT NULL, v VARCHAR(16), PRIMARY KEY (id))")
                assertEquals(3, s.executeUpdate("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')"))
            }
            // 触发一次 flush：这一步会**切换 manifest 指针**——正是 OSS 上没有 CAS 的那一步
            runBlocking { server.engine.flushAll() }
            c.createStatement().use { s ->
                s.executeQuery("SELECT COUNT(*) FROM t").use { rs ->
                    rs.next()
                    assertEquals(3L, rs.getLong(1), "flush 之后行数必须仍然正确")
                }
            }
        }

        // 重开：从对象存储恢复（走 manifest + SST + WAL）
        server.stop()
        server = YangBaseServer.forTesting(store = ossStore(), port = 0, allowNonAtomicPointerSwitch = true)
        server.start()
        port = server.boundPort
        DriverManager.getConnection(
            "jdbc:mysql://127.0.0.1:$port/ossdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
            "root", "",
        ).use { c ->
            c.createStatement().use { s ->
                s.executeQuery("SELECT COUNT(*) FROM t").use { rs ->
                    rs.next()
                    assertEquals(3L, rs.getLong(1), "重开之后数据必须还在")
                }
                s.executeQuery("SELECT v FROM t WHERE id = 2").use { rs ->
                    rs.next()
                    assertEquals("b", rs.getString(1))
                }
            }
        }
    }

    /**
     * ★ **没有显式同意时，必须在启动阶段就拒绝**——而不是"能跑但悄悄失去互斥"。
     *
     * 这条守的是原则 P6：后端能力不够时，宁可拒绝服务，也不要让一个
     * 「看起来正常」的系统在并发下破坏线性化。拒绝信息还必须指向**真正的原因**
     * 与那个开关的名字，否则运维只会看到一个"启动失败"。
     */
    @Test
    fun `不显式同意时启动必须被拒绝并指出原因`() {
        server = YangBaseServer.forTesting(store = store, port = 0, allowNonAtomicPointerSwitch = false)
        val failure = runCatching { server.start() }.exceptionOrNull()

        assertNotNull(failure, "没有 ETag CAS 的后端上，未经同意不得启动")
        val msg = generateSequence(failure) { it.cause }.joinToString(" | ") { it.message ?: "" }
        assertTrue(
            msg.contains("If-Match") && msg.contains("i-accept-nonatomic-pointer-switch"),
            "拒绝信息要同时给出原因与开关名：$msg",
        )
        // 而且不能是"声称支持却没做到"那一类（那类必须永远拒绝，没有开关可开）
        assertTrue(
            !msg.contains("陈旧 etag 的写入被接受了"),
            "如实声明不支持与静默忽略必须区分开：$msg",
        )
    }

    @Test
    fun `弱化模式下过期的写者仍被代际检查挡掉`() = runBlocking {
        // 代际（epoch）检查是弱化模式之后**剩下的那一道**防线，必须单独验证：
        // 指针切换不再原子，但"旧写者不许覆盖新写者"这条规则不能一起丢掉
        val ms = com.cloudea.yangbase.storage.ManifestStore(store, allowNonAtomicPointerSwitch = true)
        val shard = com.cloudea.yangbase.common.ShardId(
            com.cloudea.yangbase.common.TableId(1), com.cloudea.yangbase.common.PartitionId(0),
        )
        val fresh = com.cloudea.yangbase.storage.Manifest.empty(com.cloudea.yangbase.common.Epoch(5), "new")
        ms.commit(shard, fresh, null)

        val stale = fresh.copy(writerEpoch = 4, seq = fresh.seq + 1)
        assertFailsWith<ConcurrentModificationException> { ms.commit(shard, stale, null) }
    }
}
