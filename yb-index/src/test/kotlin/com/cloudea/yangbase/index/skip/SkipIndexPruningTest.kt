package com.cloudea.yangbase.index.skip

import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.FileId
import com.cloudea.yangbase.common.IndexId
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.objectstore.ByteRange
import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.storage.BlockCache
import com.cloudea.yangbase.storage.SstHandle
import com.cloudea.yangbase.storage.SstIndex
import com.cloudea.yangbase.storage.SstReader
import com.cloudea.yangbase.storage.SstWriter
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.cbor.Cbor
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 跳数索引的剪枝测试（设计文档 §8.5）。
 *
 * 这里最重的两条断言：
 *  1. **`WHERE ts > 极大值` 必须产生 0 次数据块 GET**（用 [CountingObjectStore] 数请求，
 *     而不是测延迟——请求数是确定性的）；
 *  2. **剪枝结果与不剪枝的全扫描逐次相等**（剪枝只能排除「一定不含」，
 *     判反方向就是静默漏行，不报错、不降级）。
 *
 * 数据块的读取走**范围 GET**，因此 `count("get_range")` 就是数据块读取次数
 * （`.idx` / `.skp` 都是整对象 GET，记在 `count("get")` 里）。
 */
class SkipIndexPruningTest {

    private val schema = TableSchema(
        tableId = 2001,
        databaseId = 1,
        name = "events",
        version = 1,
        columns = listOf(
            ColumnDef("id", ColumnType.BIGINT, nullable = false),
            ColumnDef("ts", ColumnType.BIGINT),
            ColumnDef("name", ColumnType.VARCHAR, length = 64),
        ),
        primaryKey = listOf("id"),
    )

    private val shard = ShardId(TableId(2001), PartitionId(0))
    private val indexId = IndexId(7)
    private val fileId = FileId("1")
    private val cbor = Cbor { ignoreUnknownKeys = true }

    private val COL_TS = 1
    private val COL_NAME = 2

    // ── 夹具 ────────────────────────────────────────────────────────

    private class Row(val id: Long, val ts: Long, val name: String)

    private val rows: List<Row> = (1..240).map { i -> Row(i.toLong(), 1000L + i, "n$i") }

    private fun keyOf(id: Long): ByteArray =
        KeyEncoder.encodeKey(listOf(Datum.I64(id)), schema, listOf(0))

    private fun rowOf(r: Row): ByteArray =
        RowCodec.encode(listOf(Datum.I64(r.id), Datum.I64(r.ts), Datum.Str(r.name)))

    private fun datumsOf(r: Row): List<Datum> =
        listOf(Datum.I64(r.id), Datum.I64(r.ts), Datum.Str(r.name))

    private class Fixture(
        val counting: CountingObjectStore,
        val cache: BlockCache,
        val handle: SstHandle,
        val dataBlocks: Int,
    )

    /**
     * 数 GET **尝试**次数。
     *
     * [CountingObjectStore] 只在成功返回后计数，因此「读一个不存在的对象」在它眼里是
     * 0 次 GET——但那次请求其实是花掉的。验证 `knownMissing` 这类「不该再试」的逻辑
     * 必须数尝试而不是数成功。
     */
    private class GetAttempts(private val delegate: ObjectStore) : ObjectStore by delegate {
        val gets = java.util.concurrent.atomic.AtomicInteger()
        override suspend fun get(key: String, range: ByteRange?): ByteArray {
            gets.incrementAndGet()
            return delegate.get(key, range)
        }
    }

    /** 建一个真实 SST（走 [SstWriter] 的块切分），并用同样的行喂一个跳数索引。 */
    private suspend fun buildFixture(
        rws: List<Row> = rows,
        blockSizeBytes: Int = 512,
        countColumns: Set<Int> = emptySet(),
    ): Fixture {
        val mem = MemoryObjectStore()
        val counting = CountingObjectStore(mem, recordKeys = true)
        val w = SstWriter(schema, level = 1, blockSizeBytes = blockSizeBytes, countColumns = countColumns)
        for (r in rws.sortedWith(compareBy({ it.id }, { -it.ts }))) {
            w.add(keyOf(r.id), r.ts, rowOf(r))
        }
        val res = w.finish()
        counting.put(ObjectKeys.sstData(shard, 1, fileId), res.data)
        counting.put(ObjectKeys.sstIndex(shard, 1, fileId), cbor.encodeToByteArray(SstIndex.serializer(), res.index))
        val cache = BlockCache(4L * 1024 * 1024)
        val handle = SstReader(counting, cache).loadIndex(shard, 1, fileId)
        return Fixture(counting, cache, handle, res.index.blocks.size)
    }

    private fun skipDef(kind: IndexKind, columns: List<Int>, granularity: Int = 1) = SkipIndexDef(
        indexId = indexId,
        kind = kind,
        columns = columns,
        columnNames = columns.map { schema.columns[it].name },
        granularity = granularity,
    )

    /** 按 SST 的真实块边界喂索引，保证 `.skp` 的块下标与 SST 的块下标一一对应。 */
    private fun buildSkip(
        fixture: Fixture,
        kind: IndexKind,
        columns: List<Int>,
        granularity: Int = 1,
        maxSetDistinct: Int = 256,
        rws: List<Row> = rows,
    ): SkipIndexFile {
        val def = skipDef(kind, columns, granularity)
        val builder = SkipIndexBuilder(def, schema, maxSetDistinct = maxSetDistinct)
        val blocks = fixture.handle.index.blocks
        var cur = -1
        for (r in rws.sortedWith(compareBy({ it.id }, { -it.ts }))) {
            val key = keyOf(r.id)
            var b = if (cur < 0) 0 else cur
            while (b + 1 < blocks.size && ByteArrayComparator.compare(blocks[b + 1].firstKey, key) <= 0) b++
            if (b != cur) {
                if (cur >= 0) builder.endBlock()
                builder.beginBlock(b)
                cur = b
            }
            builder.observe(datumsOf(r))
        }
        if (cur >= 0) builder.endBlock()
        return builder.build(fileId.value.toString())
    }

    private fun scanner(fx: Fixture) = SkipIndexScanner(SstReader(fx.counting, fx.cache), schema)

    private suspend fun scan(
        fx: Fixture,
        predicates: List<SkipPredicate>,
        skip: SkipIndexFile? = null,
        skipColumns: List<Int>? = null,
        readTs: ReadTs = ReadTs(Long.MAX_VALUE),
        disablePruning: Boolean = false,
    ): ScanResult = scanner(fx).scan(
        fx.handle,
        ScanOptions(
            predicates = predicates,
            readTs = readTs,
            skipIndex = skip,
            skipIndexColumns = skipColumns,
            disablePruning = disablePruning,
        ),
    )

    // ── ★ 核心断言：0 次数据块 GET ──────────────────────────────────

    @Test
    fun `有 minmax 跳数索引时 ts 大于极大值应为 0 次数据块 GET`() = runTest {
        val fx = buildFixture()
        val skp = buildSkip(fx, IndexKind.SKIP_MINMAX, listOf(COL_TS))
        SkipIndexStore(fx.counting, fx.cache).write(skipDef(IndexKind.SKIP_MINMAX, listOf(COL_TS)), skp)
        val blocks = fx.handle.index.blocks.size
        assertTrue(blocks >= 4, "夹具需要多个数据块才能体现剪枝，实际 $blocks")

        fx.counting.reset()
        val res = scan(
            fx,
            listOf(SkipPredicate.Range(COL_TS, lower = Bound(Datum.I64(9_000_000L), inclusive = false))),
            skip = skp,
            skipColumns = listOf(COL_TS),
        )

        assertEquals(0, res.rows.size)
        assertEquals(blocks, res.stats.blocksTotal)
        assertEquals(0, res.stats.blocksRead)
        assertEquals(blocks, res.stats.pruneStats!!.prunedByPredicate)
        // ★ 这条断言守护整个 §8.5：一个「不可能满足」的范围谓词不应读到任何数据块
        assertEquals(0, fx.counting.count("get_range"), "数据块读取应为 0：${fx.counting.keysFor("get_range")}")
        assertEquals(0, fx.counting.bytesOut("get_range"))
    }

    @Test
    fun `只有 idx 的列统计时同样 0 次数据块 GET`() = runTest {
        // 诚实记录实现实情：min/max **本来就在 .idx 里**（P3 的常驻内存元数据），
        // 因此 minmax 剪枝连 .skp 都不需要——它已经是 0 次额外请求。
        val fx = buildFixture()
        fx.counting.reset()
        val res = scan(
            fx,
            listOf(SkipPredicate.Range(COL_TS, upper = Bound(Datum.I64(1L), inclusive = true))),
        )
        assertEquals(0, res.rows.size)
        assertEquals(0, res.stats.blocksRead)
        assertEquals(0, fx.counting.count("get_range"))
    }

    @Test
    fun `每文件至多 1 次 GET，之后常驻内存`() = runTest {
        val fx = buildFixture()
        val skp = buildSkip(fx, IndexKind.SKIP_BLOOM, listOf(COL_NAME))
        val def = skipDef(IndexKind.SKIP_BLOOM, listOf(COL_NAME))
        SkipIndexStore(fx.counting, fx.cache).write(def, skp)

        // 换一个空缓存 = 模拟「另一台 CN / 进程重启」，冷路径才真的会去对象存储
        val store = SkipIndexStore(fx.counting, BlockCache(4L * 1024 * 1024))
        fx.counting.reset()
        assertNotNull(store.load(def, fileId.value.toString()))
        assertEquals(1, fx.counting.count("get"), "冷读应恰好 1 次 GET")

        fx.counting.reset()
        assertNotNull(store.load(def, fileId.value.toString()))
        assertEquals(0, fx.counting.count("get"), "第二次必须命中缓存（P3：常驻内存）")
    }

    // ── ★ 核心断言：剪枝与全扫描逐次相等 ────────────────────────────

    @Test
    fun `剪枝扫描与全扫描的结果逐次相等`() = runTest {
        val fx = buildFixture()
        val minmax = buildSkip(fx, IndexKind.SKIP_MINMAX, listOf(COL_TS, COL_NAME))
        val bloom = buildSkip(fx, IndexKind.SKIP_BLOOM, listOf(COL_NAME))
        val ngram = buildSkip(fx, IndexKind.SKIP_NGRAM_BLOOM, listOf(COL_NAME))
        val sc = scanner(fx)

        val predicates = listOf(
            SkipPredicate.Equals(COL_TS, Datum.I64(1100L)),
            SkipPredicate.Equals(COL_TS, Datum.I64(999_999L)),
            SkipPredicate.Equals(COL_NAME, Datum.Str("n42")),
            SkipPredicate.Equals(COL_NAME, Datum.Str("没有这个值")),
            SkipPredicate.InList(COL_NAME, listOf(Datum.Str("n7"), Datum.Str("n8"), Datum.Str("zz"))),
            SkipPredicate.NotEquals(COL_TS, Datum.I64(1100L)),
            SkipPredicate.Range(COL_TS, lower = Bound(Datum.I64(1100L), inclusive = true), upper = Bound(Datum.I64(1200L), inclusive = true)),
            SkipPredicate.Range(COL_TS, lower = Bound(Datum.I64(1100L), inclusive = false)),
            SkipPredicate.Range(COL_TS, upper = Bound(Datum.I64(1050L), inclusive = false)),
            SkipPredicate.IsNull(COL_NAME),
            SkipPredicate.IsNull(COL_NAME, negated = true),
            SkipPredicate.Like(COL_NAME, "n1%"),
            SkipPredicate.Like(COL_NAME, "%n4%"),
            SkipPredicate.Like(COL_NAME, "n_2"),
            SkipPredicate.And(
                listOf(
                    SkipPredicate.Range(COL_TS, lower = Bound(Datum.I64(1100L), inclusive = true)),
                    SkipPredicate.Equals(COL_NAME, Datum.Str("n110")),
                )
            ),
            SkipPredicate.Or(
                listOf(
                    SkipPredicate.Equals(COL_TS, Datum.I64(1100L)),
                    SkipPredicate.Like(COL_NAME, "n22%"),
                )
            ),
            SkipPredicate.Or(
                listOf(
                    SkipPredicate.Equals(COL_NAME, Datum.Str("n110")),
                    SkipPredicate.Equals(COL_NAME, Datum.Str("没有这个值")),
                )
            ),
        )

        for (p in predicates) {
            val full = sc.scan(fx.handle, ScanOptions(predicates = listOf(p), disablePruning = true))
            for ((si, s) in listOf(
                minmax to listOf(COL_TS, COL_NAME),
                bloom to listOf(COL_NAME),
                ngram to listOf(COL_NAME),
            )) {
                val pruned = sc.scan(
                    fx.handle,
                    ScanOptions(predicates = listOf(p), skipIndex = si, skipIndexColumns = s),
                )
                assertEquals(
                    full.rows.map { it.values },
                    pruned.rows.map { it.values },
                    "谓词 ${p.describe()} 在 ${si.kind} 下结果不一致（剪枝判反方向 = 静默漏行）",
                )
                assertTrue(
                    pruned.stats.blocksRead <= full.stats.blocksRead,
                    "剪枝后读的块不应超过全扫描：${p.describe()} ${si.kind}",
                )
            }
        }
    }

    @Test
    fun `LIKE 子串不会被 min max 静默剪错`() = runTest {
        // 子串关系不蕴含任何序关系："aab" 包含于 "zaab" 但 "aab" < "zaab"；
        // 而 "zzz" < "aazzz" 却又是子串。用 min/max 剪子串就是漏行。
        val rws = listOf(
            Row(1, 10, "zaab"),
            Row(2, 11, "zzz"),
            Row(3, 12, "aazzz"),
            Row(4, 13, "mmm"),
        )
        val fx = buildFixture(rws, blockSizeBytes = 1)
        assertTrue(fx.dataBlocks >= 4, "每行应各自成块，实际 ${fx.dataBlocks}")
        val minmaxOnly = buildSkip(fx, IndexKind.SKIP_MINMAX, listOf(COL_NAME), rws = rws)

        val res = scan(
            fx,
            listOf(SkipPredicate.Like(COL_NAME, "%aab%")),
            skip = minmaxOnly,
            skipColumns = listOf(COL_NAME),
        )
        assertEquals(listOf(Datum.Str("zaab")), res.rows.map { it.values[COL_NAME] })

        val res2 = scan(
            fx,
            listOf(SkipPredicate.Like(COL_NAME, "%zzz%")),
            skip = minmaxOnly,
            skipColumns = listOf(COL_NAME),
        )
        assertEquals(
            setOf(Datum.Str("zzz"), Datum.Str("aazzz")),
            res2.rows.map { it.values[COL_NAME] }.toSet(),
        )
    }

    @Test
    fun `ngram 布隆能剪掉不含子串的块`() = runTest {
        val fx = buildFixture()
        val ngram = buildSkip(fx, IndexKind.SKIP_NGRAM_BLOOM, listOf(COL_NAME))
        val res = scan(
            fx,
            listOf(SkipPredicate.Like(COL_NAME, "%999999%")),
            skip = ngram,
            skipColumns = listOf(COL_NAME),
        )
        assertEquals(0, res.rows.size)
        assertTrue(res.stats.pruneStats!!.prunedByPredicate > 0, "ngram 布隆应剪掉块: ${res.stats}")
    }

    @Test
    fun `set 收集超限时整块放弃而不是截断`() = runTest {
        // maxSetDistinct = 8：块内去重值超过 8 就必须放弃（不完整的集合会漏行）
        val fx = buildFixture(blockSizeBytes = 1 shl 20)
        assertEquals(1, fx.dataBlocks)
        val skp = buildSkip(fx, IndexKind.SKIP_SET, listOf(COL_NAME), maxSetDistinct = 8)
        assertNull(skp.entries.first().columns[0]!!.values, "超限必须放弃收集")
        assertNull(skp.entries.first().columns[0]!!.bloom, "位图由完整集合构建，集合没了位图也必须是 null")

        val store = SkipIndexStore(fx.counting, fx.cache)
        store.write(skipDef(IndexKind.SKIP_SET, listOf(COL_NAME)), skp)
        val res = scan(
            fx,
            listOf(SkipPredicate.Equals(COL_NAME, Datum.Str("n1"))),
            skip = skp,
            skipColumns = listOf(COL_NAME),
        )
        // 保守：虽然 .skp 没有集合，但 .idx 的 min/max 仍能定位（等价于退化为 minmax）
        assertEquals("n1", (res.rows.single().values[COL_NAME] as Datum.Str).v)
    }

    @Test
    fun `多块合并成索引项时 set 仍是完整的`() = runTest {
        val fx = buildFixture()
        assertTrue(fx.dataBlocks >= 4)
        val skp = buildSkip(fx, IndexKind.SKIP_SET, listOf(COL_NAME), granularity = 4)
        assertTrue(skp.entries.size < fx.dataBlocks, "granularity=4 应把多个块合成一项")
        for (e in skp.entries) {
            val vs = e.columns[0]!!.values
            assertNotNull(vs, "集合完整时不应放弃收集")
            assertEquals(vs.map { String(it, Charsets.ISO_8859_1) }.toSet().size, vs.size, "集合里不应有重复值")
        }
        // 完整性抽查：每一行都必须能在「它所在块映射到的索引项」的集合里找到
        val map = skp.blockToEntryMap()
        val blocks = fx.handle.index.blocks
        for (r in rows) {
            val key = keyOf(r.id)
            var b = 0
            while (b + 1 < blocks.size && ByteArrayComparator.compare(blocks[b + 1].firstKey, key) <= 0) b++
            val enc = KeyEncoder.encodeDatum(Datum.Str(r.name), ColumnType.VARCHAR)
            assertTrue(
                skp.entries[map[b]].columns[0]!!.mightContainValue(enc),
                "行 ${r.name}（块 $b → 项 ${map[b]}）不在集合里 ⇒ 集合不完整 ⇒ 会漏行",
            )
        }
    }

    @Test
    fun `键范围剪枝在上界越过后提前终止`() = runTest {
        val fx = buildFixture()
        val to = keyOf(60)
        val res = scanner(fx).scan(fx.handle, ScanOptions(keyTo = to))
        assertTrue(res.stats.pruneStats!!.prunedByKey > 0)
        assertEquals(60, res.rows.size)
        assertEquals(60L, (res.rows.last().values[0] as Datum.I64).v)

        val from = keyOf(200)
        val res2 = scanner(fx).scan(fx.handle, ScanOptions(keyFrom = from))
        assertEquals(41, res2.rows.size)
        assertTrue(res2.stats.pruneStats!!.prunedByKey > 0)
    }

    @Test
    fun `版本剪枝整块在快照之后时跳过`() = runTest {
        val rws = (1..40).map { Row(it.toLong(), 5000L + it, "n$it") }
        val fx = buildFixture(rws, blockSizeBytes = 128)
        val res = scanner(fx).scan(fx.handle, ScanOptions(readTs = ReadTs(1000L)))
        assertEquals(0, res.rows.size)
        assertEquals(0, res.stats.blocksRead)
        assertEquals(fx.dataBlocks, res.stats.pruneStats!!.prunedByTs)
        assertEquals(0, fx.counting.count("get_range"))
    }

    @Test
    fun `And 只需一个子句可剪 Or 需要全部可剪`() = runTest {
        val fx = buildFixture()
        val pruner = BlockPruner(fx.handle, schema)
        // 块 0 一定含 id=1（ts=1001），但绝不含 ts=9999999
        val prunable = SkipPredicate.Equals(COL_TS, Datum.I64(9_999_999L))
        val notPrunable = SkipPredicate.Equals(COL_TS, Datum.I64(1_001L))
        assertFalse(pruner.canSkipBlock(0, notPrunable), "夹具前提：该谓词在本块无法否定")

        assertTrue(pruner.canSkipBlock(0, SkipPredicate.And(listOf(prunable, notPrunable))))
        assertFalse(pruner.canSkipBlock(0, SkipPredicate.Or(listOf(prunable, notPrunable))))
        assertTrue(pruner.canSkipBlock(0, SkipPredicate.Or(listOf(prunable, prunable))))
        assertFalse(pruner.canSkipBlock(0, SkipPredicate.And(listOf(notPrunable, notPrunable))))
    }

    // ── LIKE 与工具函数 ─────────────────────────────────────────────

    @Test
    fun `LIKE 匹配与字面量切分`() {
        assertTrue(LikeMatcher.matches("abc", "abc"))
        assertTrue(LikeMatcher.matches("abcd", "abc%"))
        assertTrue(LikeMatcher.matches("xabc", "%abc"))
        assertTrue(LikeMatcher.matches("aXc", "a_c"))
        assertFalse(LikeMatcher.matches("ac", "a_c"))
        assertTrue(LikeMatcher.matches("anything", "%"))
        assertFalse(LikeMatcher.matches("abc", "abcd"))
        assertTrue(LikeMatcher.matches("%", "%"))
        assertTrue(LikeMatcher.matches("a%b", "a\\%b").not(), "无 ESCAPE：反斜杠是普通字符")
        assertTrue(LikeMatcher.matches("a%b", "a%b"))

        assertEquals(listOf("ab", "cd"), LikeMatcher.literalSegments("ab%cd"))
        assertEquals(listOf("ab"), LikeMatcher.literalSegments("%ab%"))
        assertEquals(listOf("ab"), LikeMatcher.literalSegments("_ab_"))
        assertEquals(emptyList(), LikeMatcher.literalSegments("%_%"))

        assertEquals("abc", LikeMatcher.leadingLiteral("abc%"))
        assertNull(LikeMatcher.leadingLiteral("%abc"))
        assertNull(LikeMatcher.leadingLiteral("_abc"))
        // `a_c%`：`_` 之后不再有确定的前缀，只能取到 "a"——弱，但仍然**正确**
        // （匹配该模式的每个值都以 "a" 开头）
        assertEquals("a", LikeMatcher.leadingLiteral("a_c%"))
        assertEquals("abc", LikeMatcher.leadingLiteral("abc"))
    }

    @Test
    fun `前缀区间的上界是严格上界`() {
        val f = "abc".toByteArray()
        val succ = BlockPruner.prefixSuccessor(f)!!
        assertTrue(ByteArrayComparator.compare(succ, f) > 0)
        // 所有以 "abc" 为前缀的串都必须 < succ
        for (t in listOf("abc", "abcd", "abc\u0000", "abczzzzz")) {
            assertTrue(ByteArrayComparator.compare(t.toByteArray(), succ) < 0, "$t 应 < succ")
        }
        assertNull(BlockPruner.prefixSuccessor(byteArrayOf(0xFF.toByte(), 0xFF.toByte())))
        // 把最后一个非 0xFF 字节加一：'c' → 'd'，因此上界是 "abd"
        assertEquals(listOf<Byte>(0x61, 0x62, 0x64), BlockPruner.prefixSuccessor(f)!!.toList())
        // 有更长的前缀时按最后一位加一：'d' → 'e'
        assertEquals(listOf<Byte>(0x61, 0x62, 0x63, 0x65), BlockPruner.prefixSuccessor("abcd".toByteArray())!!.toList())
        assertEquals(listOf<Byte>(0x61, 0x63), BlockPruner.prefixSuccessor(byteArrayOf(0x61, 0x62, 0xFF.toByte()))!!.toList())
    }

    @Test
    fun `谓词对一行的求值与 SQL 三值逻辑一致`() {
        val row = listOf(Datum.I64(1), Datum.I64(100), Datum.Null)
        assertTrue(SkipPredicate.Equals(COL_TS, Datum.I64(100)).evaluate(row))
        assertTrue(SkipPredicate.IsNull(COL_NAME).evaluate(row))
        assertFalse(SkipPredicate.IsNull(COL_NAME, negated = true).evaluate(row))
        // NULL 参与比较一律为「未知」→ 不满足
        assertFalse(SkipPredicate.Range(COL_NAME, lower = Bound(Datum.Str("a"), true)).evaluate(row))
        assertFalse(SkipPredicate.NotEquals(COL_NAME, Datum.Str("x")).evaluate(row))
        assertFalse(SkipPredicate.InList(COL_NAME, listOf(Datum.Str("x"))).evaluate(row))
        assertFalse(SkipPredicate.Like(COL_NAME, "%").evaluate(row), "NULL LIKE '%' 是未知 ⇒ 不满足")
    }

    @Test
    fun `列摘要的保守语义`() {
        val allNull = ColumnSummary(min = null, max = null, nulls = 5, minmaxCollected = true)
        assertTrue(allNull.allNull)
        val notCollected = ColumnSummary(min = null, max = null, nulls = 5, minmaxCollected = false)
        assertFalse(notCollected.allNull, "没收集过 min/max 不等于「全是 NULL」")

        val noSet = ColumnSummary(min = null, max = null, nulls = 0, values = null)
        assertTrue(noSet.mightContainValue("x".toByteArray()), "集合缺失时必须保守返回 true")
        assertFalse(noSet.allNonNullEqual("x".toByteArray()))

        val one = ColumnSummary(min = null, max = null, nulls = 0, values = listOf("v".toByteArray()))
        assertTrue(one.allNonNullEqual("v".toByteArray()))
        assertFalse(one.allNonNullEqual("w".toByteArray()))
    }

    @Test
    fun `块到索引项的映射覆盖所有块`() = runTest {
        val fx = buildFixture()
        val skp = buildSkip(fx, IndexKind.SKIP_MINMAX, listOf(COL_TS), granularity = 3)
        val map = skp.blockToEntryMap()
        assertEquals(fx.dataBlocks, map.size)
        assertTrue(map.all { it >= 0 }, "每个块都必须映射到某一项")
        for ((ei, e) in skp.entries.withIndex()) {
            for (b in e.blockFrom..e.blockTo) assertEquals(ei, map[b])
        }
    }

    @Test
    fun `没有 skp 时 load 返回 null 且只尝试 1 次 GET`() = runTest {
        val fx = buildFixture()
        // ⚠️ CountingObjectStore 只在**成功**返回后计数（失败时的 GET 也花了一次请求），
        // 因此这里用一个只数尝试次数的小包装来验证 knownMissing 的效果。
        val attempts = GetAttempts(fx.counting)
        val store = SkipIndexStore(attempts, fx.cache)
        val def = skipDef(IndexKind.SKIP_BLOOM, listOf(COL_NAME))
        assertNull(store.load(def, "no-such-file"))
        assertNull(store.load(def, "no-such-file"))
        assertEquals(1, attempts.gets.get(), "缺失必须被记住（knownMissing），不能每次查询都白打一次 GET")
    }

    @Test
    fun `类型或列不匹配的 skp 必须被忽略`() = runTest {
        val fx = buildFixture()
        val store = SkipIndexStore(fx.counting, fx.cache)
        val bloom = buildSkip(fx, IndexKind.SKIP_BLOOM, listOf(COL_NAME))
        store.write(skipDef(IndexKind.SKIP_BLOOM, listOf(COL_NAME)), bloom)
        // 用 minmax 的定义去读 bloom 的文件：类型不匹配 → 当作不存在
        assertNull(store.load(skipDef(IndexKind.SKIP_MINMAX, listOf(COL_TS)), fileId.value.toString()))
        // 列不匹配同理
        assertNull(store.load(skipDef(IndexKind.SKIP_BLOOM, listOf(COL_TS)), fileId.value.toString()))
    }

    @Test
    fun `索引定义必须拒绝非跳数索引`() {
        val e = runCatching {
            SkipIndexDef(indexId, IndexKind.FULLTEXT, listOf(COL_TS))
        }.exceptionOrNull()
        assertNotNull(e)
        val e2 = runCatching {
            SkipIndexDef(indexId, IndexKind.SKIP_BLOOM, emptyList())
        }.exceptionOrNull()
        assertNotNull(e2)
    }

    @Test
    fun `扫描器拒绝引用不存在列的谓词`() = runTest {
        val fx = buildFixture()
        val e = runCatching {
            scanner(fx).scan(fx.handle, ScanOptions(predicates = listOf(SkipPredicate.Equals(9, Datum.I64(1)))))
        }.exceptionOrNull()
        assertNotNull(e)
        assertTrue(e.message!!.contains("不存在的列"), e.message!!)
    }

    @Test
    fun `扫描可以提前终止且统计正确`() = runTest {
        val fx = buildFixture()
        val stats = scanner(fx).scanInto(fx.handle, ScanOptions(limit = 5)) { true }
        assertEquals(5, stats.rowsReturned)
        assertEquals(5, stats.rowsExamined)

        var delivered = 0
        val stats2 = scanner(fx).scanInto(fx.handle, ScanOptions()) { delivered++; delivered < 3 }
        assertEquals(3, delivered, "第 3 行已交给 sink，由 sink 决定终止")
        assertEquals(2, stats2.rowsReturned, "sink 返回 false 表示「不要这一行」")
    }

    @Test
    fun `索引文件体积远小于数据文件`() = runTest {
        // 2000 行、8KiB 块：minmax 摘要只有每项的 min/max，应比数据小一个数量级（P3）
        val big = (1..2000).map { Row(it.toLong(), 1000L + it, "name-$it") }
        val fx = buildFixture(big, blockSizeBytes = 8192)
        val skp = buildSkip(fx, IndexKind.SKIP_MINMAX, listOf(COL_TS, COL_NAME), granularity = 4, rws = big)
        val skpBytes = SkipIndexStore(fx.counting, fx.cache)
            .write(skipDef(IndexKind.SKIP_MINMAX, listOf(COL_TS, COL_NAME), granularity = 4), skp)
        val dataBytes = fx.handle.index.blocks.sumOf { it.length.toLong() }
        assertTrue(
            skpBytes * 10 < dataBytes,
            "跳数索引应比数据小一个数量级（P3）：skp=$skpBytes data=$dataBytes",
        )
    }
}
