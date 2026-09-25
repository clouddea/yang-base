package com.cloudea.yangbase.index.fulltext

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * FST 词典的正确性。
 *
 * 这里的断言方向与布隆测试同理：**查不到已插入的词条是灾难**（静默丢结果），
 * 所以每条测试都拿 `HashMap` 当参照物做等价性对比，而不是只看几个样例。
 */
class FstTest {

    private fun buildOf(entries: List<Pair<String, Int>>): Fst {
        val b = FstBuilder()
        for ((k, v) in entries.sortedBy { it.first }) b.add(k, v)
        return b.build()
    }

    @Test
    fun `与 HashMap 等价 - 随机词条`() {
        val rnd = Random(20260924)
        val keys = HashSet<String>()
        while (keys.size < 5000) {
            val len = 1 + rnd.nextInt(20)
            val sb = StringBuilder()
            repeat(len) { sb.append(('a' + rnd.nextInt(26))) }
            keys.add(sb.toString())
        }
        val sorted = keys.sorted()
        val reference = sorted.withIndex().associate { it.value to it.index }
        val fst = buildOf(sorted.mapIndexed { i, s -> s to i })

        for ((k, v) in reference) {
            assertEquals(v, fst.get(k), "词条 '$k' 的输出不对")
        }
        // 不存在的词条必须返回 null（不能因为自动机走通就返回垃圾值）
        assertNull(fst.get(""))
        assertNull(fst.get("zzzzzzzzzzzzzzzzzzzzzzzzzz"))
        assertNull(fst.get(sorted.first() + "!"))
        assertNull(fst.get(sorted.first().dropLast(1).ifEmpty { "x" } + "\u0001"))
        assertEquals(keys.size, fst.size)
    }

    @Test
    fun `输出值为 0 与不存在必须区分`() {
        val fst = buildOf(listOf("aaa" to 0, "bbb" to 7))
        assertEquals(0, fst.get("aaa"))
        assertTrue(fst.contains("aaa"))
        assertNull(fst.get("aa"))       // 前缀但不是词条
        assertFalse(fst.contains("aa"))
        assertNull(fst.get("aaaa"))     // 词条的后缀
        assertEquals(7, fst.get("bbb"))
    }

    @Test
    fun `一个词条是另一个词条的前缀`() {
        val fst = buildOf(listOf("a" to 1, "ab" to 2, "abc" to 3, "b" to 4))
        assertEquals(1, fst.get("a"))
        assertEquals(2, fst.get("ab"))
        assertEquals(3, fst.get("abc"))
        assertEquals(4, fst.get("b"))
    }

    @Test
    fun `最小化 - 后缀共享确实发生`() {
        // 100 个词条共享 4 字节前缀 "root"：节点数应远小于朴素 trie 的节点数
        val entries = (0 until 100).map { "root-" + it.toString().padStart(3, '0') to it }
        val fst = buildOf(entries)
        val naiveNodes = 1 + 100 * 9      // 粗略上界：每个词条一条独立路径
        assertTrue(fst.nodeCount < naiveNodes / 3,
            "最小化没生效？节点数=${fst.nodeCount}")
        // 后缀共享的极端例子：4 个词条共享同一个叶节点
        val fst2 = buildOf(listOf("xa" to 1, "ya" to 1, "za" to 1, "xaa" to 1))
        assertTrue(fst2.nodeCount <= 8, "后缀共享不足: ${fst2.nodeCount}")
    }

    @Test
    fun `按字典序枚举 - 与排序后的词条表一致`() {
        val words = listOf("app", "apple", "application", "apply", "banana", "band", "bandana")
        val fst = buildOf(words.mapIndexed { i, w -> w to i })

        val all = ArrayList<String>()
        assertTrue(fst.forEachTerm("") { t, _ -> all.add(t); true })
        assertContentEquals(words.sorted(), all)

        val app = ArrayList<String>()
        fst.forEachTerm("app") { t, _ -> app.add(t); true }
        assertContentEquals(listOf("app", "apple", "application", "apply"), app)

        val band = ArrayList<String>()
        fst.forEachTerm("band") { t, _ -> band.add(t); true }
        assertContentEquals(listOf("band", "bandana"), band)

        val none = ArrayList<String>()
        fst.forEachTerm("zzz") { t, _ -> none.add(t); true }
        assertTrue(none.isEmpty())
    }

    @Test
    fun `枚举可以提前中止`() {
        val fst = buildOf((0 until 100).map { "k" + it.toString().padStart(2, '0') to it })
        val seen = ArrayList<String>()
        val completed = fst.forEachTerm("") { t, _ -> seen.add(t); seen.size < 5 }
        assertFalse(completed)
        assertEquals(5, seen.size)
    }

    @Test
    fun `序列化往返一致`() {
        val rnd = Random(7)
        val entries = (0 until 2000).map {
            val len = 1 + rnd.nextInt(12)
            val s = (0 until len).joinToString("") { _ -> ('a' + rnd.nextInt(5)).toString() }
            s
        }.distinct().sorted().mapIndexed { i, s -> s to i * 3 }
        val fst = buildOf(entries)
        val bytes = fst.toBytes()
        val back = Fst.fromBytes(bytes)

        assertEquals(fst.size, back.size)
        assertEquals(fst.nodeCount, back.nodeCount)
        assertEquals(fst.arcCount, back.arcCount)
        assertEquals(fst.maxKeyLen, back.maxKeyLen)
        assertEquals(fst.byteSize, bytes.size.toLong())
        for ((k, v) in entries) {
            assertEquals(v, back.get(k), "往返后词条 '$k' 输出不对")
        }
        assertNull(back.get("zzzzzzzzzzzz"))

        val all = ArrayList<String>()
        back.forEachTerm("") { t, _ -> all.add(t); true }
        assertContentEquals(entries.map { it.first }, all)
    }

    @Test
    fun `损坏的字节被拒绝`() {
        val fst = buildOf(listOf("hello" to 1, "world" to 2))
        val bytes = fst.toBytes()
        assertFailsWith<IllegalArgumentException>("魔数被破坏") {
            val bad = bytes.copyOf(); bad[0] = 'X'.code.toByte(); Fst.fromBytes(bad)
        }
        assertFailsWith<IllegalArgumentException>("内容被篡改（校验和）") {
            val bad = bytes.copyOf(); bad[bytes.size / 2] = (bad[bytes.size / 2] + 1).toByte()
            Fst.fromBytes(bad)
        }
        assertFailsWith<IllegalArgumentException>("长度被截断") {
            Fst.fromBytes(bytes.copyOf(bytes.size - 1))
        }
    }

    @Test
    fun `UTF-8 词条`() {
        val words = listOf("数据", "数据库", "数据库系统", "索引", "倒排索引", "café", "naïve")
            .sorted()
        val fst = buildOf(words.mapIndexed { i, w -> w to i })
        for ((i, w) in words.withIndex()) assertEquals(i, fst.get(w), "中文词条 '$w' 查不到")
        val all = ArrayList<String>()
        fst.forEachTerm("") { t, _ -> all.add(t); true }
        assertContentEquals(words, all)
        // 前缀枚举在 UTF-8 下按字节序，与 String 的 UTF-16 序不同——
        // 断言用字节序参照，避免把实现细节当成契约
        val prefixed = ArrayList<String>()
        fst.forEachTerm("数据") { t, _ -> prefixed.add(t); true }
        assertContentEquals(words.filter { it.startsWith("数据") }.sortedBy { it }, prefixed)
    }

    @Test
    fun `未按字典序 add 会被拒绝`() {
        val b = FstBuilder()
        b.add("b", 1)
        assertFailsWith<IllegalArgumentException> { b.add("a", 2) }
        assertFailsWith<IllegalArgumentException> { b.add("b", 3) }
    }

    @Test
    fun `空 FST 可用`() {
        val fst = FstBuilder().build()
        assertEquals(0, fst.size)
        assertNull(fst.get("x"))
        val all = ArrayList<String>()
        fst.forEachTerm("") { t, _ -> all.add(t); true }
        assertTrue(all.isEmpty())
        val back = Fst.fromBytes(fst.toBytes())
        assertEquals(0, back.size)
        assertNull(back.get("x"))
    }

    @Test
    fun `最小化让弧数远少于朴素字典树`() {
        // 朴素字典树的弧数 = 词条字节数之和（每条词条一条独立路径）。
        // 5000 个共享 19 字节前缀、5 位数字后缀的词条，后缀共享后弧数应少一个量级。
        val entries = (0 until 5000).map { "com.example.tenant." + it.toString().padStart(5, '0') to it }
        val fst = buildOf(entries)
        val trieArcs = entries.sumOf { it.first.toByteArray().size }
        assertTrue(fst.arcCount * 4 < trieArcs,
            "弧数 ${fst.arcCount} 相比朴素字典树的 $trieArcs 条弧没有明显压缩")
        // 单字典树的最坏情况：每个节点 8 字节固定开销（outputs+targets），
        // 因此只对「词条足够长」的数据集断言每词条开销——短词条下 FST 未必省
        assertTrue(fst.byteSize < 40L * fst.size,
            "每词条 ${fst.byteSize.toDouble() / fst.size} 字节，超出预期")
    }
}
