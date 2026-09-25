package com.cloudea.yangbase.server

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 命令行的数值解析。
 *
 * ## 为什么这些断言值得存在
 *
 * 这些参数的默认值都是**为生产挑的**，而"配错了却没生效"是最难发现的一类问题：
 * 集群照常启动、照常服务，只是行为与配置不符。`--ms-election-timeout-ms` 尤其如此
 * ——它被配小了之后的症状是「集群时不时重新选举」（不报错，只是每次换届都丢掉行锁表），
 * 从现象反推回这个参数几乎不可能。
 *
 * 所以立场是：**不认识的值宁可拒绝启动**，而且错误信息里要带上参数名与期望格式。
 */
class CliOptionsTest {

    private val default = ServerConfig.DEFAULT_MS_ELECTION_TIMEOUT

    @Test
    fun `区间写法两端都取`() {
        assertEquals(500L..1_000L, parseMillisRange("500-1000", default, "--x"))
        assertEquals(200L..400L, parseMillisRange("200-400", default, "--x"))
    }

    @Test
    fun `单个数字表示上界翻倍`() {
        assertEquals(800L..1_600L, parseMillisRange("800", default, "--x"))
    }

    @Test
    fun `缺省时用默认值`() {
        assertEquals(default, parseMillisRange(null, default, "--x"))
    }

    @Test
    fun `不认识的值拒绝启动并说清格式`() {
        for (bad in listOf("abc", "", "500-", "-500", "500-1000-1500", "0", "-1", "1e3")) {
            val e = assertFailsWith<IllegalArgumentException>("'$bad' 应该被拒绝") {
                parseMillisRange(bad, default, "--ms-election-timeout-ms")
            }
            assertTrue(
                e.message!!.contains("--ms-election-timeout-ms"),
                "错误信息必须带上参数名（否则运维不知道该改哪一行）：${e.message}",
            )
        }
    }
}
