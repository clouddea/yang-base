package com.cloudea.yangbase.objectstore

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * `S3Provider` / `S3Target` 的解析规则。
 *
 * 为什么值得单独测：这里的错误**不会以异常的形式出现**。寻址风格填错，
 * 阿里云 OSS 报的是 `SecondLevelDomainForbidden`；能力声明填错，条件写会被
 * 静默忽略（GCS 互操作层）或直接被拒（OSS 400）。三种症状都指向"网络/权限"，
 * 而真正的原因在这里。所以宁可把「解析出来的四个参数」逐条断言死。
 */
class S3ProviderTest {

    @Test
    fun `不指定 provider 时按标准 S3 语义`() {
        val t = S3Target.resolve(null, endpoint = null, region = null, pathStyle = null)
        assertEquals(S3Provider.AWS, t.provider)
        assertNull(t.endpoint, "AWS 不传 endpoint：交给 SDK 内置端点解析")
        assertEquals("us-east-1", t.region)
        assertNull(t.pathStyle, "null = 交给 S3ObjectStore.create 的自动规则")
        assertEquals(ObjectStoreCapabilities.FULL, t.caps)
    }

    @Test
    fun `oss 预设：端点按 region 展开、虚拟主机式、无 ETag CAS`() {
        val t = S3Target.resolve("oss", endpoint = null, region = null, pathStyle = null)
        assertEquals("https://oss-cn-hangzhou.aliyuncs.com", t.endpoint, "默认杭州")
        assertEquals(false, t.pathStyle, "OSS 拒绝路径式寻址")
        assertFalse(t.caps.conditionalUpdate, "OSS 没有 If-Match")
        assertEquals(AtomicCreateStyle.FORBID_OVERWRITE, t.caps.atomicCreateStyle)

        val bj = S3Target.resolve("OSS", endpoint = null, region = "cn-beijing", pathStyle = null)
        assertEquals("https://oss-cn-beijing.aliyuncs.com", bj.endpoint, "region 展开 + id 大小写不敏感")
    }

    @Test
    fun `兼容开关 --s3-oss 等价于 provider oss`() {
        val legacy = S3Target.resolve(null, null, null, null, legacyOssFlag = true)
        val modern = S3Target.resolve("oss", null, null, null)
        assertEquals(modern, legacy)
    }

    @Test
    fun `显式参数永远覆盖预设`() {
        val t = S3Target.resolve(
            providerId = "minio",
            endpoint = "http://10.0.0.5:9000",
            region = "eu-west-1",
            pathStyle = false,
        )
        assertEquals("http://10.0.0.5:9000", t.endpoint)
        assertEquals("eu-west-1", t.region)
        assertEquals(false, t.pathStyle, "用户显式写 false 就必须是 false，哪怕 MinIO 默认路径式")
    }

    @Test
    fun `未指定 region 时用预设默认值`() {
        assertEquals("ap-guangzhou", S3Target.resolve("cos", null, null, null).region)
        assertEquals("cn-north-4", S3Target.resolve("obs", null, null, null).region)
        assertEquals("auto", S3Target.resolve("r2", null, null, null).region, "R2 的 region 就是字面量 auto")
        assertEquals("us-west-004", S3Target.resolve("b2", null, null, null).region)
        // 显式给空串等同没给：否则 --s3-region "" 会拼出 https://oss-.aliyuncs.com
        assertEquals("cn-hangzhou", S3Target.resolve("oss", null, "", null).region)
    }

    @Test
    fun `region 模板展开到 COS OBS B2 的端点`() {
        assertEquals("https://cos.ap-shanghai.myqcloud.com", S3Target.resolve("cos", null, "ap-shanghai", null).endpoint)
        assertEquals("https://obs.cn-east-3.myhuaweicloud.com", S3Target.resolve("obs", null, "cn-east-3", null).endpoint)
        assertEquals("https://s3.us-east-005.backblazeb2.com", S3Target.resolve("b2", null, "us-east-005", null).endpoint)
    }

    @Test
    fun `没有端点模板的后端必须显式给 endpoint`() {
        // AWS 例外：SDK 自己有端点解析
        assertNull(S3Target.resolve("aws", null, null, null).endpoint)
        // MinIO / R2 的端点带主机名或账号 id，推导不出来
        assertNull(S3Target.resolve("minio", null, null, null).endpoint)
        assertNull(S3Target.resolve("r2", null, null, null).endpoint)
    }

    @Test
    fun `未实测的后端必须诚实标注`() {
        // 这不是装饰：`verified = false` 决定文档怎么写、也决定用户该不该先跑 conformance
        assertTrue(S3Provider.AWS.verified)
        assertTrue(S3Provider.MINIO.verified)
        assertTrue(S3Provider.OSS.verified, "OSS 在真实服务上实测过（2026-09-25 杭州）")
        listOf(S3Provider.COS, S3Provider.OBS, S3Provider.R2, S3Provider.B2).forEach {
            assertFalse(it.verified, "${it.id} 还没实测过，不能标成 verified")
        }
    }

    @Test
    fun `未知 provider 的报错要列出全部合法值`() {
        val e = assertFailsWith<IllegalArgumentException> {
            S3Target.resolve("ceph", null, null, null)
        }
        S3Provider.entries.forEach { assertTrue(e.message!!.contains(it.id), "报错里应列出 ${it.id}：${e.message}") }
    }
}
