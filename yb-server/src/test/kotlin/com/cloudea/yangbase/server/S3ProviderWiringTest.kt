package com.cloudea.yangbase.server

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * `--s3-provider` 与 `ServerConfig` 的接线。
 *
 * 解析规则本身在 `yb-objectstore` 的 `S3ProviderTest` 里测过；这里只测**服务端有没有真的用上**
 * ——「解析正确但没接上」是这类改动最典型的漏法（配置项加了、buildStore 没读）。
 *
 * 这些用例**不连网**：`S3ObjectStore.create` 只装配客户端，真正的连接发生在能力探测阶段，
 * 而它不在 `buildStore` 里。
 */
class S3ProviderWiringTest {

    private fun build(
        provider: String? = null,
        endpoint: String? = null,
        region: String? = null,
        pathStyle: Boolean? = null,
        oss: Boolean = false,
    ) = YangBaseServer.buildStore(
        ServerConfig(
            s3Bucket = "unit-test-bucket",
            s3Provider = provider,
            s3Endpoint = endpoint,
            s3Region = region,
            s3PathStyle = pathStyle,
            s3Oss = oss,
            s3AccessKey = "ak",
            s3SecretKey = "sk",
        ),
    )

    @Test
    fun `provider oss 真的改变了能力声明`() {
        val store = build(provider = "oss")
        assertFalse(store.caps.conditionalUpdate, "OSS 没有 ETag CAS，服务端必须按这个声明启动")
        assertEquals(store.caps.atomicCreateStyle.name, "FORBID_OVERWRITE")
    }

    @Test
    fun `兼容开关 s3Oss 与 provider oss 等价`() {
        assertEquals(build(provider = "oss").caps, build(oss = true).caps)
    }

    @Test
    fun `默认仍按标准 S3 语义`() {
        val store = build()
        assertTrue(store.caps.conditionalUpdate)
        assertTrue(store.caps.conditionalCreate)
    }

    @Test
    fun `需要显式端点的后端在缺端点时给出可执行的报错`() {
        // MinIO / R2 的端点推不出来：报错必须说清楚"要给什么"，而不是 NPE 或连接超时
        val e = assertFailsWith<IllegalArgumentException> { build(provider = "minio") }
        assertTrue(e.message!!.contains("--s3-endpoint"), e.message)
    }

    @Test
    fun `有端点模板的后端可以省略 endpoint`() {
        // 不抛异常即说明端点由 region 模板展开成功
        build(provider = "cos", region = "ap-shanghai")
        build(provider = "obs", region = "cn-east-3")
        build(provider = "b2", region = "us-west-005")
    }

    @Test
    fun `显式 path-style 覆盖预设`() {
        // 这里只能间接验证：装配不抛异常，且能力不受影响。
        // 真正的寻址行为由 S3ProviderTest 断言（解析层）与 conformance 套件（端到端）保证
        build(provider = "oss", pathStyle = true)
    }
}
