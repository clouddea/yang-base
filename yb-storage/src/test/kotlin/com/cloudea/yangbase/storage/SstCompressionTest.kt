package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.Checksums
import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.CorruptDataException
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.FileId
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.Varint
import com.cloudea.yangbase.objectstore.ByteRange
import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.MemoryObjectStore
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.cbor.Cbor
import java.io.ByteArrayOutputStream
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * SST 块级压缩的测试（设计文档 §6.4、§13.3）。
 *
 * 这里守的是四条：
 *  1. **往返无损**——压过与没压过解出来的逻辑内容必须逐条相同
 *  2. **向后兼容**——加压缩之前写下的对象（未压缩块 + 没有 `compression` 字段的 `.idx`）照样能读
 *  3. **零成本原则**——压完更大时不压，且此时输出的字节与「关闭压缩」逐字节一致
 *  4. **请求数不变**——压缩影响的是字节数，不是请求数（P4）
 */
class SstCompressionTest {

    private val schema = TableSchema(
        tableId = 1001,
        databaseId = 1,
        name = "users",
        version = 1,
        columns = listOf(
            ColumnDef("id", ColumnType.BIGINT, nullable = false),
            ColumnDef("name", ColumnType.VARCHAR, length = 64),
            ColumnDef("age", ColumnType.INT),
        ),
        primaryKey = listOf("id"),
    )

    private val shard = ShardId(TableId(1001), PartitionId(0))
    private val fileId = FileId("0000000000001")

    private fun keyOf(id: Long): ByteArray = KeyEncoder.encodeKey(listOf(Datum.I64(id)), schema, listOf(0))

    private fun rowOf(id: Long, name: String, age: Long): ByteArray =
        RowCodec.encode(listOf(Datum.I64(id), Datum.Str(name), Datum.I64(age)))

    /**
     * BLOB 版 schema，只用于「数据压不动」的场景。
     *
     * VARCHAR 装不下真随机字节（解码器会按长度前缀校验）。注意载荷高熵**并不**
     * 等于整块压不动：行与行之间有定长框架（flags、长度前缀、类型标记）会原样重复，
     * 那部分仍会被 LZ4 拿走。所以高熵用例断言的是「没有块被压大」这个不变式，
     * 见 `零成本原则 - 高熵数据上压缩绝不放大任何一个块`。
     */
    private val blobSchema = TableSchema(
        tableId = 1002,
        databaseId = 1,
        name = "blobs",
        version = 1,
        columns = listOf(
            ColumnDef("id", ColumnType.BIGINT, nullable = false),
            ColumnDef("payload", ColumnType.BLOB),
        ),
        primaryKey = listOf("id"),
    )

    private fun blobKeyOf(id: Long): ByteArray =
        KeyEncoder.encodeKey(listOf(Datum.I64(id)), blobSchema, listOf(0))

    private fun blobRow(id: Long, payload: ByteArray): ByteArray =
        RowCodec.encode(listOf(Datum.I64(id), Datum.Bin(payload)))

    // ── 夹具 ──────────────────────────────────────────────────────

    /** 写一批键 1..n、值形如 `user_<i>` 的行（Text 类数据，可压）。 */
    private fun writeRows(n: Int, blockSizeBytes: Int = 4096, compress: Boolean = true): SstWriteResult {
        val w = SstWriter(schema, level = 1, blockSizeBytes = blockSizeBytes, compress = compress)
        var ts = 100L
        for (i in 1..n) {
            ts += 1
            w.add(keyOf(i.toLong()), ts, rowOf(i.toLong(), "user_$i", (i % 60).toLong()))
        }
        return w.finish()
    }

    private fun cbor(idx: SstIndex): ByteArray = Cbor.encodeToByteArray(SstIndex.serializer(), idx)

    private suspend fun putSst(store: ObjectStore, res: SstWriteResult, level: Int = 1) {
        store.put(ObjectKeys.sstData(shard, level, fileId), res.data)
        store.put(ObjectKeys.sstIndex(shard, level, fileId), cbor(res.index))
    }

    private suspend fun open(store: ObjectStore, level: Int = 1): Pair<SstReader, SstHandle> {
        val reader = SstReader(store, BlockCache(4L * 1024 * 1024))
        return reader to reader.loadIndex(shard, level, fileId)
    }

    /**
     * 按块读回整份 SST，把每个条目压成一行文本。
     *
     * 用它比较「压过」与「没压过」的输出：两者只要有一字节读错，
     * 这里就会给出差异（而且是逐块、逐条的差异，不是一句「不一致」）。
     */
    private suspend fun dump(res: SstWriteResult): List<String> {
        val store = MemoryObjectStore()
        putSst(store, res)
        val (reader, handle) = open(store)
        val out = ArrayList<String>()
        for (b in handle.index.blocks.indices) {
            for (e in reader.readBlock(handle, b)) {
                out.add("${hex(e.userKey)}@${e.commitTs}=${hex(e.value)}")
            }
        }
        return out
    }

    private fun hex(b: ByteArray?): String = b?.joinToString("") { "%02x".format(it) } ?: "∅"

    /** 列统计的可比较形式（[ColumnStat] 里有 ByteArray，data class 的 equals 比不了内容）。 */
    private fun colStatsSignature(stats: List<ColumnStat>): List<String> = stats.map { s ->
        "${hex(s.min)}..${hex(s.max)}|null=${s.nullCount}|" +
            (s.valueCounts?.joinToString(",") { "${hex(it.value)}×${it.count}" } ?: "-")
    }

    // ── 1. 往返 ───────────────────────────────────────────────────

    @Test
    fun `压缩往返 - 可压缩数据逐条与未压缩输出相同`() = runTest {
        val compressed = writeRows(n = 3000, compress = true)
        val plain = writeRows(n = 3000, compress = false)

        assertTrue(
            compressed.index.blocks.any { it.compression == Compression.LZ4 },
            "文本数据应当至少有一个块被压缩",
        )
        assertEquals(
            compressed.index.blocks.size, plain.index.blocks.size,
            "压缩不参与块切分：块数与未压缩时必须一致",
        )
        assertEquals(dump(plain), dump(compressed), "压缩必须无损：解出来的条目序列应逐条相同")

        assertTrue(
            compressed.data.size < plain.data.size,
            "可压缩数据上压缩应当减小体积：压缩后=${compressed.data.size} 原=${plain.data.size}",
        )
    }

    @Test
    fun `压缩往返 - 全零与高重复值`() = runTest {
        // 所有行的值完全一样：LZ4 最擅长的形状
        val w = SstWriter(schema, level = 1, blockSizeBytes = 64 * 1024, compress = true)
        val row = rowOf(7, "same-value-over-and-over", 30)
        val plain = SstWriter(schema, level = 1, blockSizeBytes = 64 * 1024, compress = false)
        for (i in 1L..2000L) {
            w.add(keyOf(i), 1000, row)
            plain.add(keyOf(i), 1000, row)
        }
        val res = w.finish()
        val plainRes = plain.finish()

        assertTrue(res.index.blocks.all { it.compression == Compression.LZ4 }, "高重复数据应全部命中压缩")
        assertEquals(dump(plainRes), dump(res))
        val ratio = res.data.size.toDouble() / plainRes.data.size
        println("[压缩率] 高重复行: ${plainRes.data.size} → ${res.data.size} 字节（${"%.1f".format(ratio * 100)}%）")
        assertTrue(ratio < 0.5, "高重复数据的压缩率应显著（实测 ${"%.1f".format(ratio * 100)}%）")
    }

    @Test
    fun `压缩往返 - 空值与长文本混合`() = runTest {
        val rnd = java.util.Random(11)
        val w = SstWriter(schema, level = 1, blockSizeBytes = 2048, compress = true)
        val plain = SstWriter(schema, level = 1, blockSizeBytes = 2048, compress = false)
        var ts = 0L
        for (i in 1L..500L) {
            ts += 1
            val isTombstone = rnd.nextInt(10) == 0
            val v = if (isTombstone) null else rowOf(i, "name-${rnd.nextInt(50)}", rnd.nextInt(90).toLong())
            w.add(keyOf(i), ts, v)
            plain.add(keyOf(i), ts, v)
        }
        val res = w.finish()
        assertEquals(dump(plain.finish()), dump(res), "含墓碑与变长值的块也必须无损往返")
    }

    // ── 2. 向后兼容 ───────────────────────────────────────────────

    /**
     * 加压缩之前写下的 `.idx` 与 `.dat`：手工按 v1 布局构造，**不经过任何新代码**。
     *
     * 用一份独立的旧字段表（[LegacyIndex]）来编码 `.idx`，而不是「关掉新字段再编码一遍」——
     * 后者会随实现一起漂移，测不出「旧文件读不出来」这类退化。
     */
    @Test
    fun `旧格式 - 没有 compression 字段的 idx 与未压缩块仍可读`() = runTest {
        val store = MemoryObjectStore()
        val entries = listOf(
            Triple(keyOf(1), 100L, rowOf(1, "legacy-1", 10)),
            Triple(keyOf(2), 200L, rowOf(2, "legacy-2", 20)),
            Triple(keyOf(3), 300L, rowOf(3, "legacy-3", 30)),
        )
        val block = legacyBlock(entries)
        store.put(ObjectKeys.sstData(shard, 1, fileId), block)
        store.put(
            ObjectKeys.sstIndex(shard, 1, fileId),
            Cbor.encodeToByteArray(
                LegacyIndex.serializer(),
                LegacyIndex(
                    level = 1,
                    rows = 3,
                    tombstones = 0,
                    minKey = entries.first().first,
                    maxKey = entries.last().first,
                    minTs = 100,
                    maxTs = 300,
                    blocks = listOf(
                        LegacyBlock(
                            offset = 0,
                            length = block.size,
                            entries = 3,
                            rows = 3,
                            tombstones = 0,
                            minTs = 100,
                            maxTs = 300,
                            firstKey = entries.first().first,
                        )
                    ),
                    bloom = BloomData.build(entries.map { it.first }, bitsPerKey = 10),
                    schemaVersion = 1,
                    deduplicated = true,
                ),
            ),
        )

        val (reader, handle) = open(store)

        assertEquals(SST_FORMAT_VERSION_PLAIN, handle.index.formatVersion, "旧文件的版本号是 1")
        assertEquals(Compression.NONE, handle.index.blocks[0].compression, "缺字段应默认未压缩")
        assertEquals(3L, handle.index.rows)

        val hit = reader.get(handle, keyOf(2), ReadTs(400))
        assertTrue(hit is LookupResult.Found, "旧文件里的 key 必须查得到，实际=$hit")
        assertContentEquals(rowOf(2, "legacy-2", 20), (hit as LookupResult.Found).value)
        assertTrue(reader.get(handle, keyOf(99), ReadTs(400)) is LookupResult.NotFound)

        // 范围扫描与元数据计数也要在这份旧文件上成立
        assertEquals(3L, reader.visibleRows(handle, ReadTs(400)), "旧文件的 COUNT 元数据路径必须照常工作")
        assertEquals(3, reader.iterator(handle, null, null).let { itr ->
            var n = 0
            while (itr.peek() != null) { n++; itr.advance() }
            n
        })
    }

    @Test
    fun `旧格式 - 旧 idx 里的多版本与墓碑语义不变`() = runTest {
        val store = MemoryObjectStore()
        val entries = listOf(
            Triple(keyOf(1), 300L, null),                          // 墓碑（最新）
            Triple(keyOf(1), 200L, rowOf(1, "mid", 20)),
            Triple(keyOf(1), 100L, rowOf(1, "old", 10)),
        )
        val block = legacyBlock(entries)
        store.put(ObjectKeys.sstData(shard, 1, fileId), block)
        store.put(
            ObjectKeys.sstIndex(shard, 1, fileId),
            Cbor.encodeToByteArray(
                LegacyIndex.serializer(),
                LegacyIndex(
                    level = 1, rows = 0, tombstones = 1,
                    minKey = entries.first().first, maxKey = entries.first().first,
                    minTs = 100, maxTs = 300,
                    blocks = listOf(
                        LegacyBlock(
                            offset = 0, length = block.size, entries = 3, rows = 0, tombstones = 1,
                            minTs = 100, maxTs = 300, firstKey = entries.first().first,
                        )
                    ),
                    bloom = BloomData.build(listOf(entries.first().first), bitsPerKey = 10),
                    schemaVersion = 1, deduplicated = false,
                ),
            ),
        )

        val (reader, handle) = open(store)
        assertTrue(reader.get(handle, keyOf(1), ReadTs(400)) is LookupResult.Deleted, "最新版本是墓碑 → 已删除")
        val old = reader.get(handle, keyOf(1), ReadTs(150))
        assertTrue(old is LookupResult.Found, "更早的快照应看到历史版本")
        assertContentEquals(rowOf(1, "old", 10), (old as LookupResult.Found).value)
    }

    // ── 3. 零成本原则 ─────────────────────────────────────────────

    @Test
    fun `零成本原则 - 高熵数据上压缩绝不放大任何一个块`() = runTest {
        // 载荷、主键、commit_ts 全部随机：连 ts 高位那种「每行都一样的字节」
        // 都不留给压缩机。BLOB 列才装得下这种载荷。
        val rnd = java.util.Random(20260924)
        val small = 400
        val ids = (1..small).map { rnd.nextLong() }.sorted()
        val payloads = MutableList(ids.size) { ByteArray(150).also { rnd.nextBytes(it) } }
        val tss = List(ids.size) { rnd.nextLong() }
        // 三行超大随机载荷：它们各自独占一个块，块内再没有第二个行框架可被引用，
        // 于是 LZ4 的输出必然比输入长（约 1/255 的流开销）——这三块**必然**回退。
        // 「回退分支真的被走到」这个保证就落在这里，不靠运气。
        val giantIdx = listOf(3, small / 2, small - 4)
        for (i in giantIdx) payloads[i] = ByteArray(64 * 1024).also { rnd.nextBytes(it) }

        fun write(compress: Boolean): SstWriteResult {
            val w = SstWriter(blobSchema, level = 1, blockSizeBytes = 4096, compress = compress)
            for (i in ids.indices) w.add(blobKeyOf(ids[i]), tss[i], blobRow(ids[i], payloads[i]))
            return w.finish()
        }

        val auto = write(compress = true)
        val off = write(compress = false)

        // ★ 零成本原则的可断言形式：无论数据多压不动，**没有任何块会变大**。
        // 注意「高熵数据 ⇒ 一个块都不压」是个错误的期望：行与行之间总有定长框架
        // （flags 字节、行长度前缀、类型标记）会逐行原样重复，LZ4 抓的就是它，
        // 随机载荷上能省下千分之几。压不动就不压，才是这条原则。
        assertEquals(off.index.blocks.size, auto.index.blocks.size, "压缩不改变块的切分")
        var fellBack = 0
        for (i in auto.index.blocks.indices) {
            val a = auto.index.blocks[i]
            val b = off.index.blocks[i]
            assertTrue(a.length <= b.length, "第 $i 块被压大了：${b.length} → ${a.length}")
            if (a.compression != Compression.NONE) continue
            fellBack++
            // 回退的块不能被包上任何壳：它与关闭压缩时写出的字节逐字节相同
            assertContentEquals(
                off.data.copyOfRange(b.offset.toInt(), b.offset.toInt() + b.length),
                auto.data.copyOfRange(a.offset.toInt(), a.offset.toInt() + a.length),
                "回退的第 $i 块必须与关闭压缩逐字节相同",
            )
        }
        assertTrue(fellBack >= giantIdx.size, "三行超大载荷所在块必须回退，实际回退了 $fellBack 块")

        // 版本号与真实形态一致：只要有一个块真压了，读者就必须理解压缩（→ v2）；
        // 全部回退时仍是 v1，旧读者照样能读
        assertEquals(
            if (fellBack == auto.index.blocks.size) SST_FORMAT_VERSION_PLAIN else SST_FORMAT_VERSION_COMPRESSED,
            auto.index.formatVersion,
        )
        // 意图字段记录「配置是开启的」，供运维回答「这个文件为什么没压」
        assertEquals(Compression.LZ4, auto.index.compression)

        assertEquals(dump(off), dump(auto), "压缩不得改变任何一条数据")
        // 端到端：这份数据上开启压缩也不会让对象变大
        assertTrue(auto.data.size <= off.data.size, "压缩把文件变大了：${off.data.size} → ${auto.data.size}")
        println(
            "[零成本] 高熵数据: ${off.data.size} → ${auto.data.size} 字节" +
                "（${auto.index.blocks.size} 块，$fellBack 块回退为未压缩）"
        )
    }

    @Test
    fun `零成本原则 - 压缩后不变小的块不采用压缩`() {
        // 直接测编码器：一个 20 字节的高熵块
        val encoder = BlockEncoder(compress = true)
        val tiny = ByteArray(20) { (it * 37 + 11).toByte() }
        val encoded = encoder.encode(tiny)
        assertEquals(Compression.NONE, encoded.compression, "20 字节的块压缩只会有头开销")
        assertTrue(encoded.bytes === tiny, "未压缩路径应零拷贝返回原数组")

        val empty = encoder.encode(ByteArray(0))
        assertEquals(Compression.NONE, empty.compression)

        // 大块随机数据：LZ4 的 bit 流本身有约 1/255 的开销，找不到可引用的重复
        // 时输出必然更长。这是回退判定的**确定性**用例，不依赖数据碰巧。
        val big = ByteArray(64 * 1024).also { java.util.Random(7).nextBytes(it) }
        val bigEncoded = encoder.encode(big)
        assertEquals(Compression.NONE, bigEncoded.compression, "64 KiB 随机数据必须回退为未压缩")
        assertTrue(bigEncoded.bytes === big, "回退路径不能复制")

        val off = BlockEncoder(compress = false)
        assertEquals(Compression.NONE, off.encode(ByteArray(4096)).compression)
        assertTrue(!off.enabled)
    }

    @Test
    fun `关闭压缩 - 块布局与旧格式逐字节一致`() = runTest {
        val res = writeRows(n = 500, blockSizeBytes = 2048, compress = false)

        assertEquals(SST_FORMAT_VERSION_PLAIN, res.index.formatVersion)
        assertEquals(Compression.NONE, res.index.compression)
        assertTrue(res.index.blocks.all { it.compression == Compression.NONE })

        // 手工按 v1 布局解析 .dat：块区间必须恰好是 [varint 条目数][条目…][crc32c]
        for (b in res.index.blocks) {
            val raw = res.data.copyOfRange(b.offset.toInt(), b.offset.toInt() + b.length)
            val crc = Checksums.crc32c(raw, 0, raw.size - 4)
            assertEquals(crc, java.nio.ByteBuffer.wrap(raw, raw.size - 4, 4).int, "块 $b 的 CRC 应覆盖前 length-4 字节")
            val (count, _) = Varint.readUnsigned(raw, 0)
            assertEquals(b.entries, count, "块首的条目数应与 .idx 记录一致")
            val entries = SstWriter.readBlock(raw.copyOfRange(0, raw.size - 4))
            assertEquals(count, entries.size)
        }
    }

    // ── 4. 块级而非文件级 ─────────────────────────────────────────

    @Test
    fun `块级压缩 - 任意单块只凭自己的字节区间就能解出`() = runTest {
        val counting = CountingObjectStore(MemoryObjectStore(), recordKeys = true)
        val res = writeRows(n = 2000, blockSizeBytes = 2048, compress = true)
        putSst(counting, res)
        val (reader, handle) = open(counting)
        val target = handle.index.blocks.size / 2
        assertTrue(handle.index.blocks[target].compression == Compression.LZ4, "目标块应是压缩块")

        counting.reset()
        val b = handle.index.blocks[target]
        // 只取这一个块的字节区间——文件级压缩下这段字节根本无法单独解压
        val raw = counting.get(ObjectKeys.sstData(shard, 1, fileId), ByteRange(b.offset, b.offset + b.length - 1))
        assertEquals(1L, counting.count("get_range"), "应恰好 1 次范围 GET")
        assertEquals(b.length.toLong(), counting.bytesOut("get_range"), "传输的字节数应等于该块的长度")

        val payload = BlockDecoder.decode(raw, 0, raw.size - 4, b.compression)
        val entries = SstWriter.readBlock(payload)
        assertEquals(b.entries, entries.size)

        // 与读取器读同一块的结果逐条一致
        val fromReader = reader.readBlock(handle, target).map { "${hex(it.userKey)}@${it.commitTs}=${hex(it.value)}" }
        val manual = entries.map { "${hex(it.userKey)}@${it.commitTs}=${hex(it.value)}" }
        assertEquals(manual, fromReader)
    }

    @Test
    fun `块级压缩 - 压缩不改变块切分与块级统计`() = runTest {
        val compressed = writeRows(n = 2000, blockSizeBytes = 2048, compress = true)
        val plain = writeRows(n = 2000, blockSizeBytes = 2048, compress = false)

        assertEquals(plain.index.blocks.size, compressed.index.blocks.size)
        for (i in compressed.index.blocks.indices) {
            val a = compressed.index.blocks[i]
            val b = plain.index.blocks[i]
            // 块的划分由 key 边界决定，压缩只改「怎么存」：
            // 首 key、条目数、行数、ts 范围、列统计都必须一模一样
            assertContentEquals(b.firstKey, a.firstKey, "块 $i 的首键不应受压缩影响")
            assertEquals(b.entries, a.entries, "块 $i 的条目数")
            assertEquals(b.rows, a.rows, "块 $i 的行数")
            assertEquals(b.minTs, a.minTs, "块 $i 的 minTs")
            assertEquals(b.maxTs, a.maxTs, "块 $i 的 maxTs")
            assertEquals(b.tsCounts, a.tsCounts, "块 $i 的 tsCounts")
            // 列统计里带 ByteArray，data class 的 equals 对数组是引用比较，必须比内容
            assertEquals(colStatsSignature(b.colStats), colStatsSignature(a.colStats), "块 $i 的列统计")
        }
    }

    @Test
    fun `块级压缩 - 恰好一个块与块边界切分`() = runTest {
        // 空文件：没有块可压，读路径也必须正常
        val empty = SstWriter(schema, level = 1, compress = true).finish()
        assertTrue(empty.index.blocks.isEmpty())
        assertEquals(0, empty.data.size)
        val store = MemoryObjectStore()
        putSst(store, empty)
        val (reader, handle) = open(store)
        assertTrue(reader.get(handle, keyOf(1), ReadTs(10)) is LookupResult.NotFound)
        assertEquals(0L, reader.visibleRows(handle, ReadTs(10)))

        // 正好一个块
        val one = writeRows(n = 3, blockSizeBytes = 64 * 1024, compress = true)
        assertEquals(1, one.index.blocks.size, "小数据不应被切成多块")

        // 精确落在块边界上：每 3 条切一块
        val sameRow = rowOf(1, "fixed-width-value", 30)
        val entrySize = ByteArrayOutputStream()
            .also { SstWriter.writeEntry(it, keyOf(1), 100, sameRow) }
            .size()
        val w = SstWriter(schema, level = 1, blockSizeBytes = entrySize * 3, compress = true)
        for (i in 1L..12L) w.add(keyOf(i), 100, sameRow)
        val res = w.finish()
        assertEquals(4, res.index.blocks.size, "12 条 / 每块 3 条 = 4 块（边界处必须切开）")
        assertTrue(res.index.blocks.all { it.entries == 3 }, "每块的条目数应恰好为 3")
        assertEquals(dump(writeFixedRows(sameRow, entrySize * 3, compress = false)), dump(res))
    }

    /** 同一条行重复 12 次，块大小正好是 3 条 → 边界落在 key 上，切块必须精确。 */
    private fun writeFixedRows(row: ByteArray, blockSizeBytes: Int, compress: Boolean): SstWriteResult {
        val w = SstWriter(schema, level = 1, blockSizeBytes = blockSizeBytes, compress = compress)
        for (i in 1L..12L) w.add(keyOf(i), 100, row)
        return w.finish()
    }

    // ── 5. 损坏与防御 ─────────────────────────────────────────────

    @Test
    fun `损坏的压缩块 - CRC 在解压之前拦下`() = runTest {
        val res = writeRows(n = 800, blockSizeBytes = 2048, compress = true)
        val store = MemoryObjectStore()
        putSst(store, res)
        val corrupt = res.data.copyOf()
        val b = res.index.blocks.first { it.compression == Compression.LZ4 }
        // 在压缩载荷中部翻一个 bit（不是长度前缀、也不是 CRC 尾部）
        val at = b.offset.toInt() + b.length / 2
        corrupt[at] = (corrupt[at].toInt() xor 0x40).toByte()
        store.put(ObjectKeys.sstData(shard, 1, fileId), corrupt)

        val (reader, handle) = open(store)
        val e = assertFailsWith<CorruptDataException> { reader.readBlock(handle, res.index.blocks.indexOf(b)) }
        assertTrue(e.message!!.contains("CRC"), "必须是 CRC 校验失败，而不是别的问题：${e.message}")
    }

    @Test
    fun `损坏的长度前缀 - 拒绝解压而不是按声明分配内存`() {
        // 声明 2 GiB 未压缩长度：不能照着它去 new ByteArray
        val huge = ByteArrayOutputStream().apply {
            Varint.writeUnsigned(this, Int.MAX_VALUE)
            write(0xFF); write(0x00); write(0x01)
        }.toByteArray()
        val e1 = assertFailsWith<CorruptDataException> { BlockDecoder.decode(huge, Compression.LZ4) }
        assertTrue(e1.message!!.contains("上限"), "应明确拒绝超限长度：${e1.message}")

        // 只有长度前缀、没有载荷
        val noBody = ByteArrayOutputStream().apply { Varint.writeUnsigned(this, 100) }.toByteArray()
        assertFailsWith<CorruptDataException> { BlockDecoder.decode(noBody, Compression.LZ4) }

        // 空块
        assertFailsWith<CorruptDataException> { BlockDecoder.decode(ByteArray(0), Compression.LZ4) }

        // 载荷被截断（解压器必须在损坏输入上安全失败）
        val truncated = ByteArrayOutputStream().apply {
            Varint.writeUnsigned(this, 4096)
            write(0x10); write(0x20)
        }.toByteArray()
        assertFailsWith<CorruptDataException> { BlockDecoder.decode(truncated, Compression.LZ4) }

        // 未压缩形态则不拷贝、原样返回
        val plain = byteArrayOf(1, 2, 3, 4)
        assertTrue(BlockDecoder.decode(plain, Compression.NONE) === plain)
    }

    @Test
    fun `未知的 idx 版本 - 明确拒绝而不是当成 v1 读`() = runTest {
        val store = MemoryObjectStore()
        val res = writeRows(n = 10)
        store.put(ObjectKeys.sstData(shard, 1, fileId), res.data)
        store.put(
            ObjectKeys.sstIndex(shard, 1, fileId),
            cbor(res.index.copy(formatVersion = SST_FORMAT_VERSION_MAX + 1)),
        )
        val e = assertFailsWith<CorruptDataException> { open(store) }
        assertTrue(e.message!!.contains("不支持的 .idx 格式版本"), e.message!!)
    }

    // ── 6. 与存储引擎的接线 ───────────────────────────────────────

    private suspend fun openLsm(
        store: ObjectStore,
        compress: Boolean,
        memCapacity: Long = 32L * 1024 * 1024,
        blockSize: Int = 16 * 1024,
    ): LsmStore {
        val scope = kotlinx.coroutines.CoroutineScope(
            kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.Default
        )
        val lsm = LsmStore(
            store = store, shard = shard, schema = schema,
            cache = BlockCache(8L * 1024 * 1024), nodeId = "test-node",
            scope = scope, commitMode = CommitMode.SYNC,
            memtableCapacity = memCapacity, blockSize = blockSize, compress = compress,
        )
        lsm.open()
        return lsm
    }

    private suspend fun put(lsm: LsmStore, id: Long, name: String, age: Long, ts: Long) {
        lsm.write(WalBatch("tx-$id-$ts", ts, listOf(WalOp(keyOf(id), rowOf(id, name, age)))))
    }

    private suspend fun getRow(lsm: LsmStore, id: Long, ts: Long): List<Datum>? =
        lsm.get(keyOf(id), ReadTs(ts))?.let { RowCodec.decode(it, schema) }

    @Test
    fun `引擎接线 - 压缩开启后点查与 COUNT 的请求数不变`() = runTest {
        val counting = CountingObjectStore(MemoryObjectStore())
        val lsm = openLsm(counting, compress = true)
        for (i in 1L..300L) put(lsm, i, "user_$i", (i % 60), 100 + i)
        lsm.flush()

        getRow(lsm, 42, 10000)                       // 预热：填充 manifest / .idx / 数据块
        counting.reset()
        val row = getRow(lsm, 43, 10000)
        assertEquals(Datum.Str("user_43"), row!![1])
        assertTrue(
            counting.count("get") + counting.count("get_range") <= 1,
            "压缩不得改变点查的请求模式（稳态 ≤ 1 次 GET）。${counting.report()}",
        )

        counting.reset()
        val before = counting.totalOps()
        assertEquals(300L, lsm.countAll(ReadTs(100000)), "COUNT 仍须精确")
        assertEquals(before, counting.totalOps(), "COUNT 仍须是 0 次请求。${counting.report()}")

        // 确认这份文件确实压了——否则上面两条断言什么都没证明
        val idxKey = counting.list(ObjectKeys.sstLevel(shard, 0)).keys.first { it.endsWith(".idx") }
        val idx = Cbor.decodeFromByteArray(SstIndex.serializer(), counting.get(idxKey))
        assertTrue(idx.blocks.any { it.compression == Compression.LZ4 }, "flush 出的文件里应含压缩块")
        assertEquals(SST_FORMAT_VERSION_COMPRESSED, idx.formatVersion, "含压缩块的文件必须是 v2")
        lsm.close()
    }

    @Test
    fun `引擎接线 - 写侧开关不影响读侧 - 两种配置互读`() = runTest {
        val store = MemoryObjectStore()
        val writerCompressed = openLsm(store, compress = true)
        for (i in 1L..200L) put(writerCompressed, i, "user_$i", 20, 100 + i)
        writerCompressed.flush()
        writerCompressed.close()

        // 读侧没有任何压缩开关：换一个「关掉压缩」的引擎读同一份数据
        val readerPlain = openLsm(store, compress = false)
        for (i in 1L..200L) {
            assertEquals(Datum.Str("user_$i"), getRow(readerPlain, i, 10000)!![1], "压缩文件应被未压缩配置的引擎读到：id=$i")
        }
        // 反向：这个引擎接着写自己的（未压缩）文件，再由「开启压缩」的引擎读
        for (i in 201L..400L) put(readerPlain, i, "user_$i", 20, 100 + i)
        readerPlain.flush()
        readerPlain.close()

        val readerCompressed = openLsm(store, compress = true)
        for (i in 201L..400L) {
            assertEquals(Datum.Str("user_$i"), getRow(readerCompressed, i, 10000)!![1], "未压缩文件应被压缩配置的引擎读到：id=$i")
        }
        assertEquals(400L, readerCompressed.countAll(ReadTs(100000)))
        readerCompressed.close()
    }

    @Test
    fun `引擎接线 - 压缩不影响崩溃恢复与 compaction`() = runTest {
        val store = MemoryObjectStore()
        val lsm = openLsm(store, compress = true, blockSize = 2048)

        // 多写几批、多 flush，把 L0 撑到触发 compaction
        var ts = 100L
        for (batch in 1..6) {
            for (i in 1L..150L) {
                ts += 1
                put(lsm, i, "user_$i-b$batch", (i % 60), ts)
            }
            lsm.flush()
        }
        lsm.compact(protectReadTs = 0L)
        val stats = lsm.stats()
        assertTrue(stats.levelFiles.drop(1).sum() > 0, "compaction 应产出 L1 文件，实际=${stats.levelFiles}")
        for (i in 1L..150L) {
            assertEquals(Datum.Str("user_$i-b6"), getRow(lsm, i, 10_000)!![1], "compaction 后应看到最新值：id=$i")
        }
        lsm.close()

        // 重开（走 manifest + SST，等价于重启）
        val reopened = openLsm(store, compress = true, blockSize = 2048)
        for (i in 1L..150L) {
            assertEquals(Datum.Str("user_$i-b6"), getRow(reopened, i, 10_000)!![1], "重启后丢数据：id=$i")
        }
        assertEquals(150L, reopened.countAll(ReadTs(100000)))
        reopened.close()
    }

    // ── 7. 压缩率实测 ─────────────────────────────────────────────

    @Test
    fun `压缩率 - 真实 SST 数据上的实测收益`() = runTest {
        suspend fun measure(compress: Boolean): Pair<Long, Long> {
            val store = MemoryObjectStore()
            val lsm = openLsm(store, compress = compress, blockSize = 64 * 1024)
            var ts = 1_000_000L
            // 模拟一张典型的 TP 表：自增主键 + 重复度高的短文本 + 窄数值列
            for (i in 1L..20_000L) {
                ts += 1
                put(lsm, i, "user_${i % 5000}", (i % 90), ts)
            }
            lsm.flush()
            lsm.close()
            var dat = 0L
            var idx = 0L
            for (k in store.keys()) {
                val size = store.head(k).size
                when {
                    k.endsWith(".dat") -> dat += size
                    k.endsWith(".idx") -> idx += size
                }
            }
            println(
                "[压缩率] ${if (compress) "LZ4 " else "关闭"}  " +
                    "数据块=${dat / 1024} KiB  索引=${idx / 1024} KiB"
            )
            return dat to idx
        }

        val (plain, plainIdx) = measure(compress = false)
        val (lz4, lz4Idx) = measure(compress = true)
        val ratio = lz4.toDouble() / plain
        println(
            "[压缩率] 20,000 行 SST 数据块：$plain → $lz4 字节" +
                "（节省 ${"%.1f".format((1 - ratio) * 100)}%，压缩率 ${"%.2f".format(ratio)}）"
        )
        assertTrue(lz4 < plain, "真实数据上压缩必须减小体积：$plain → $lz4")
        // P3：`.idx` 要常驻内存、要能部分解析，压缩它只会两头都亏——收益是零星的，
        // 代价是每次取块级统计都要先解压整个索引。所以它**不随压缩开关变小**：
        // 唯一的差别是每个块多记一个 compression 字节（≈3 字节/块的元数据开销）。
        // 反过来说，若哪天有人给 `.idx` 也套上 LZ4，这份高度重复的索引会立刻大幅缩小，
        // 下面的下界就会失败。
        assertTrue(lz4Idx >= plainIdx, "压缩不得作用于 .idx（P3）：$plainIdx → $lz4Idx")
        val idxOverhead = (lz4Idx - plainIdx).toDouble() / plainIdx
        assertTrue(
            idxOverhead < 0.02,
            "`.idx` 的增长只能是块级 compression 字段的元数据开销，实际 ${"%.2f".format(idxOverhead * 100)}%",
        )
    }
}

// ── 旧格式（v1）的字段表：刻意不含 compression ──────────────────────
//
// 这是「向下兼容」测试的基准：它描述的是一份加压缩之前写下的 `.idx`。
// 真实文件由当时的代码写出，字段集合就是下面这些；只要有人把新字段的默认值
// 去掉，这份旧文件就会读不出来，测试随即失败。

@Serializable
private data class LegacyIndex(
    val formatVersion: Int = 1,
    val level: Int,
    val rows: Long,
    val tombstones: Long,
    val minKey: ByteArray,
    val maxKey: ByteArray,
    val minTs: Long,
    val maxTs: Long,
    val blocks: List<LegacyBlock>,
    val bloom: BloomData,
    val schemaVersion: Long = 0,
    val deduplicated: Boolean = false,
)

@Serializable
private data class LegacyBlock(
    val offset: Long,
    val length: Int,
    val entries: Int,
    val rows: Int,
    val tombstones: Int,
    val minTs: Long,
    val maxTs: Long,
    val firstKey: ByteArray,
    val colStats: List<ColumnStat> = emptyList(),
    val tsCounts: List<TsBucket> = emptyList(),
)

/**
 * 手工拼一个 v1 形态的数据块：`[varint 条目数][条目…][crc32c 4B]`。
 *
 * 刻意不复用写路径——这份字节要代表「压缩功能存在之前的实现」写下的东西。
 */
private fun legacyBlock(entries: List<Triple<ByteArray, Long, ByteArray?>>): ByteArray {
    val body = ByteArrayOutputStream()
    Varint.writeUnsigned(body, entries.size)
    for ((k, ts, v) in entries) SstWriter.writeEntry(body, k, ts, v)
    val raw = body.toByteArray()
    val full = ByteArray(raw.size + 4)
    System.arraycopy(raw, 0, full, 0, raw.size)
    val crc = Checksums.crc32c(full, 0, raw.size)
    full[raw.size] = ((crc ushr 24) and 0xFF).toByte()
    full[raw.size + 1] = ((crc ushr 16) and 0xFF).toByte()
    full[raw.size + 2] = ((crc ushr 8) and 0xFF).toByte()
    full[raw.size + 3] = (crc and 0xFF).toByte()
    return full
}
