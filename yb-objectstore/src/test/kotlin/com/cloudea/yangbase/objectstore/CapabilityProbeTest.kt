package com.cloudea.yangbase.objectstore

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 能力探测的测试。
 *
 * 重点是**否定用例**：一个从不失败的探测毫无价值。这里模拟的
 * 「静默忽略条件头」正是 GCS S3 互操作层的真实故障模式——
 * 请求成功返回，但前置条件根本没生效。在这种后端上跑 YangBase
 * 会在并发下损坏数据而**不报任何错**，所以探测必须能把它认出来。
 */
class CapabilityProbeTest {

    /** 正常后端：条件写完全生效。 */
    @Test
    fun `标准后端应全部通过`() = runBlocking {
        val r = CapabilityProbe.probe(MemoryObjectStore())
        assertTrue(
            r.allCriticalPassed,
            "标准内存后端应通过全部关键能力，实际:\n${r.report()}",
        )
        assertTrue(r.capabilities.conditionalCreate, "条件创建")
        assertTrue(r.capabilities.conditionalUpdate, "比较交换")
        assertTrue(r.capabilities.conditionalWriteVerified, "条件写应被标记为已实测")
        assertTrue(r.capabilities.strongReadAfterWrite, "读己之写")
        assertTrue(r.capabilities.strongListConsistency, "LIST 一致性")
    }

    /**
     * 模拟 GCS 互操作层的行为：**静默忽略写请求上的条件头**。
     * 写入照常成功，前置条件形同虚设。
     */
    private class IgnoresConditionalHeaders(private val delegate: ObjectStore) : ObjectStore {
        override val caps get() = delegate.caps
        override suspend fun get(key: String, range: ByteRange?) = delegate.get(key, range)
        override suspend fun put(key: String, data: ByteArray, precondition: Precondition?) =
            delegate.put(key, data, precondition = null)   // ← 条件被丢弃

        override suspend fun delete(key: String) = delegate.delete(key)
        override suspend fun list(prefix: String, pageToken: String?, limit: Int) =
            delegate.list(prefix, pageToken, limit)

        override suspend fun head(key: String) = delegate.head(key)    }

    /** 模拟弱一致 LIST：刚写入的对象在列举中看不到。 */
    private class EventuallyConsistentList(private val delegate: ObjectStore) : ObjectStore {
        override val caps get() = delegate.caps
        override suspend fun get(key: String, range: ByteRange?) = delegate.get(key, range)
        override suspend fun put(key: String, data: ByteArray, precondition: Precondition?) =
            delegate.put(key, data, precondition)

        override suspend fun delete(key: String) = delegate.delete(key)
        override suspend fun list(prefix: String, pageToken: String?, limit: Int) =
            ListPage(emptyList(), null)   // ← 永远列举不到东西

        override suspend fun head(key: String) = delegate.head(key)
    }

    @Test
    fun `必须检出静默忽略条件头的后端`() = runBlocking {
        val r = CapabilityProbe.probe(IgnoresConditionalHeaders(MemoryObjectStore()))
        assertFalse(
            r.capabilities.conditionalCreate,
            "后端忽略了 If-None-Match，条件创建必须被判定为不支持",
        )
        assertFalse(
            r.capabilities.conditionalUpdate,
            "后端忽略了 If-Match，比较交换必须被判定为不支持",
        )
        assertFalse(r.capabilities.conditionalWriteVerified, "被忽略的条件写不能算作已验证")
        assertFalse(r.allCriticalPassed, "这种后端必须导致探测整体失败，从而拒绝启动")

        val detail = r.findings.first { it.name.startsWith("条件创建") }.detail
        assertTrue(
            detail.contains("忽略") || detail.contains("条件"),
            "失败原因要说清楚，便于运维定位。实际: $detail",
        )
    }

    @Test
    fun `必须检出弱一致的 LIST`() = runBlocking {
        val r = CapabilityProbe.probe(EventuallyConsistentList(MemoryObjectStore()))
        assertFalse(r.capabilities.strongListConsistency, "列举不到刚写入的对象，不能算强一致")
        // LIST 是恢复路径的关键：弱一致 LIST 会让崩溃恢复漏掉 WAL 对象
        assertFalse(r.allCriticalPassed, "弱一致 LIST 属于关键能力缺失")
    }

    @Test
    fun `探测结束后不留痕迹`() = runBlocking {
        val backing = MemoryObjectStore()
        CapabilityProbe.probe(backing)
        assertTrue(
            backing.keys().isEmpty(),
            "探测必须在隔离前缀下进行并清理干净，实际残留: ${backing.keys()}",
        )
    }

    @Test
    fun `探测可在已有数据上安全执行`() = runBlocking {
        val backing = MemoryObjectStore()
        backing.put("shard/1/0/manifest/CURRENT", "existing".toByteArray())
        backing.put("catalog/db/1.json", "{}".toByteArray())

        CapabilityProbe.probe(backing)

        assertTrue(backing.keys().contains("shard/1/0/manifest/CURRENT"), "既有数据不得被动到")
        assertTrue(backing.keys().contains("catalog/db/1.json"))
    }

    @Test
    fun `probeAndMerge 只会收紧而不会放宽声明`() = runBlocking {
        // 声明说「支持条件写」，实测说不支持 → 结果必须是不支持
        val declared = ObjectStoreCapabilities.FULL
        val r = CapabilityProbe.probeAndMerge(IgnoresConditionalHeaders(MemoryObjectStore()), declared)
        assertFalse(
            r.capabilities.conditionalCreate,
            "实测结果必须能推翻厂商/配置的声明——这正是探测存在的意义",
        )
    }

    @Test
    fun `探测耗时在一秒量级`() = runBlocking {
        val r = CapabilityProbe.probe(MemoryObjectStore())
        assertTrue(r.probeMillis < 10_000, "探测不应拖慢启动太久，实际 ${r.probeMillis}ms")
    }
}
