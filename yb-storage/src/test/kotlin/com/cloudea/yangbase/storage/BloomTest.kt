package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.OrderPreservingCodec
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 布隆过滤器的回归测试。
 *
 * 这里最重要的一条不变式是：**绝不能有假阴性**。
 * 一个假阴性会让存在的行静默读不到——数据「消失」了却不报任何错。
 * 这正是实现期间真实踩到的缺陷（构建与查询用了不同的模数）。
 */
class BloomTest {

    private fun keys(vararg s: String) = s.map { it.toByteArray() }

    @Test
    fun `已插入的 key 必须全部命中 - 无假阴性`() {
        val ks = (1..1000).map { OrderPreservingCodec.encodeLong(it.toLong()) }
        val bloom = BloomData.build(ks, bitsPerKey = 10)
        for (k in ks) {
            assertTrue(bloom.mightContain(k), "假阴性！key=${k.toList()}")
        }
    }

    @Test
    fun `已插入的 key 必须全部命中 - 各种规模`() {
        // 覆盖会引起位数组填充到非整字节的规模（原缺陷在 500 位附近暴露）
        for (n in intArrayOf(1, 2, 7, 50, 100, 499, 500, 501, 1000, 4096)) {
            val ks = (1..n).map { "key-$it".toByteArray() }
            val bloom = BloomData.build(ks, bitsPerKey = 10)
            for (k in ks) {
                assertTrue(bloom.mightContain(k), "n=$n 时出现假阴性: ${String(k)}")
            }
        }
    }

    @Test
    fun `含 0x00 字节的 key 不产生假阴性`() {
        // 保序编码后的整数天然含大量 0x00，是最容易踩坑的输入
        val ks = (0L until 200L).map { OrderPreservingCodec.encodeLong(it) }
        val bloom = BloomData.build(ks, bitsPerKey = 10)
        for (k in ks) assertTrue(bloom.mightContain(k), "含 0x00 的 key 出现假阴性")
    }

    @Test
    fun `空输入不崩溃且保守返回`() {
        val bloom = BloomData.build(emptyList(), bitsPerKey = 10)
        assertTrue(bloom.mightContain("anything".toByteArray()), "无 bloom 时应保守返回「可能存在」")
    }

    @Test
    fun `不存在的 key 大多数被正确排除 - 假阳性率可控`() {
        val existing = (1..1000).map { "exists-$it".toByteArray() }
        val bloom = BloomData.build(existing, bitsPerKey = 10)
        var fp = 0
        val probes = 2000
        for (i in 1..probes) {
            if (bloom.mightContain("absent-$i".toByteArray())) fp++
        }
        val rate = fp.toDouble() / probes
        // 10 bits/key 的理论假阳性率约 1%，留足余量
        assertTrue(rate < 0.05, "假阳性率过高: ${"%.2f".format(rate * 100)}%")
    }

    @Test
    fun `xxhash 雪崩性 - 相近输入产生不同哈希`() {
        val h1 = XxHash64.hash("key-1".toByteArray(), 0)
        val h2 = XxHash64.hash("key-2".toByteArray(), 0)
        val h3 = XxHash64.hash("key-1".toByteArray(), 1)
        assertTrue(h1 != h2, "不同输入应产生不同哈希")
        assertTrue(h1 != h3, "不同种子应产生不同哈希")
        // 输出位分布不应退化
        val bits = java.lang.Long.bitCount(h1)
        assertTrue(bits in 15..50, "哈希位分布异常: $bits 位为 1")
    }

    @Test
    fun `反转单个位会改变哈希 - 不是恒等函数`() {
        val base = ByteArray(16) { it.toByte() }
        val seen = HashSet<Long>()
        for (i in base.indices) {
            val copy = base.copyOf()
            copy[i] = (copy[i].toInt() xor 0x01).toByte()
            seen.add(XxHash64.hash(copy, 42))
        }
        assertTrue(seen.size >= 14, "xxhash 对不同输入的区分度不足: 仅 ${seen.size} 个不同值")
    }

    @Test
    fun `短输入与长输入都可用`() {
        for (len in intArrayOf(0, 1, 7, 8, 15, 16, 31, 32, 33, 100, 1000)) {
            val k = ByteArray(len) { (it * 31).toByte() }
            val bloom = BloomData.build(listOf(k), bitsPerKey = 10)
            assertTrue(bloom.mightContain(k), "长度 $len 的 key 出现假阴性")
        }
    }
}
