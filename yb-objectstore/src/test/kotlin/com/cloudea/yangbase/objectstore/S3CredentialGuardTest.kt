package com.cloudea.yangbase.objectstore

import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * S3 密钥的**入参守卫**。
 *
 * 起因是真实的一次启动失败（2026-09-27，阿里云 OSS）：`--s3-access-key $AK` 里的
 * `$AK` 没有导出，shell 于是**安安静静地传了一个空参数**——命令行看起来完全正常，
 * 报错却出现在很远的地方（能力探测的 403 / InvalidAccessKeyId），指向"密钥内容不对"，
 * 而真正的原因（变量是空的）没有出现在任何一条输出里。
 *
 * 这一层守卫的判据有三条，缺一条都会漏掉一种真实误用：
 *
 * | 传了什么 | 含义 | 处理 |
 * |---|---|---|
 * | 两个都不传 | 走默认凭据链（ECS 实例角色、环境变量） | ✅ 允许 |
 * | 只传一个 | 一定是漏了 | ❌ 报错（否则会在下面某一层报一个莫名其妙的签名错） |
 * | 传了但是空串 | 多半是 `$VAR` 没导出 | ❌ 报错，并把"去 echo 一下变量长度"写进消息 |
 */
class S3CredentialGuardTest {

    private fun create(accessKey: String?, secretKey: String?) = S3ObjectStore.create(
        bucket = "some-bucket",
        endpoint = "http://127.0.0.1:1",   // 不会真的连：守卫在构建客户端之前就触发
        region = "cn-hangzhou",
        accessKey = accessKey,
        secretKey = secretKey,
        pathStyle = false,
    )

    @Test
    fun `密钥是空串时必须明确报错并指出环境变量这个原因`() {
        // `--s3-access-key $AK` 且 $AK 未导出 —— 传进来的就是空串
        val e = assertFailsWith<IllegalArgumentException> { create("", "secret") }
        assertTrue(e.message!!.contains("空串"), "要说清是空串，实际：${e.message}")
        assertTrue(
            e.message!!.contains("环境变量"),
            "要点出最常见的原因（变量没导出），否则使用者会去怀疑密钥本身，实际：${e.message}",
        )

        assertFailsWith<IllegalArgumentException> { create("key", "") }
        assertFailsWith<IllegalArgumentException> { create("   ", "key") }
    }

    @Test
    fun `只给一半密钥时必须报错`() {
        // 少给一半时若放行，SDK 会去走默认凭据链：ECS 上可能"碰巧"能跑，
        // 而本地开发机上会报一个与真正原因无关的凭据错误
        assertFailsWith<IllegalArgumentException> { create("key", null) }
        assertFailsWith<IllegalArgumentException> { create(null, "secret") }
    }

    @Test
    fun `两个都不给是合法的——那是实例角色这类默认凭据链`() {
        // null 与 "" 是**两回事**：null 表示"由默认凭据链决定"，
        // 在 ECS 上配合实例角色是正常且推荐的部署方式
        assertNotNull(create(null, null))
    }

    @Test
    fun `密钥齐全时正常构建`() {
        assertNotNull(create("some-key", "some-secret"))
    }
}
