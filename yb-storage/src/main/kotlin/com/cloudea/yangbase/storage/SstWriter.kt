package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.BigEndian
import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.Checksums
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.Varint
import java.io.ByteArrayOutputStream

/**
 * 构建一个 SST 文件，产出 `.dat`（数据块）与 `.idx`（索引与统计）两份字节。
 *
 * 两条必须遵守的不变式：
 *  1. **输入必须按内部键序**（userKey 升序、commitTs 降序）——乱序输入会静默产生损坏文件
 *  2. **同一个 userKey 的所有版本必须落在同一个数据块内**——否则块级计数会重复。
 *     实现方式：只在 key 边界处切块。
 */
class SstWriter(
    private val schema: TableSchema,
    private val level: Int,
    private val blockSizeBytes: Int = 64 * 1024,
    /**
     * 是否对数据块做块级压缩（设计文档 §6.4「默认开启，`compress=false` 可关」）。
     *
     * 关掉它有两个正当理由：数据本身已经压过（JPEG、已压缩的文本），再压只是白烧 CPU；
     * 排查格式问题时需要一个「所见即所得」的模式。
     *
     * **它只影响写**——读什么由文件里的 `compression` 字段决定（P2：对象一经写出即
     * 不可变，压缩是写时决定的，读时只能读格式里记录的结果）。
     */
    private val compress: Boolean = true,
    /** 需要收集「值→计数」的列下标（低基数列）。用于 §8.9.4 的 0-GET 谓词计数。 */
    private val countColumns: Set<Int> = emptySet(),
    /** 每个块内单列值计数的上限，超过则放弃收集（防止高基数列撑爆 `.idx`）。 */
    private val maxDistinctPerBlock: Int = 256,
    /** 每个块内 commit_ts 分桶数的上限。超过就放弃该块的精确计数表。 */
    private val maxTsBucketsPerBlock: Int = 1024,
    /**
     * 跳数索引的块观察者（设计文档 §8.5）。
     *
     * 写成时收集统计量而不是事后重扫：写 SST 的那一刻数据正好在手上，
     * 再读一遍就是白付一次 IO。为空时不产生任何额外开销——
     * 连解码都不会发生。
     *
     * 公开只读：写完之后调用方要靠**同一批**观察者把 `.skp` 落盘。
     * 重新造一批新的会得到一份空统计量——索引照样写出，只是永远剪不掉任何块。
     */
    val blockObservers: List<SstBlockObserver> = emptyList(),
) {
    private val data = ByteArrayOutputStream()
    private val blocks = ArrayList<BlockIndexEntry>()

    /** 块编码器持有复用缓冲，因此**每个写者一个**（见 [BlockEncoder] 的线程安全说明）。 */
    private val blockEncoder = BlockEncoder(compress)

    /** 是否至少有一个块真的被压缩了。它决定 `.idx` 的格式版本号。 */
    private var anyBlockCompressed = false

    private var cur = ByteArrayOutputStream()
    private var acc = BlockAccumulator()

    /**
     * 当前块里已写入的行值，供跳数索引观察者使用。
     *
     * 只在配了观察者时才收集——否则每条写入都要多留一份引用，
     * 而绝大多数部署并不需要跳数索引。
     */
    private val blockEntries = ArrayList<ObservedEntry>()

    private class ObservedEntry(val value: ByteArray?)

    private var totalRows = 0L
    private var totalTombstones = 0L
    /** 是否有任何 userKey 在本文件内出现了多次。有的话本文件就不是去重的。 */
    private var sawDuplicateKey = false
    private var lastKeyOverall: ByteArray? = null
    private var fileMinKey: ByteArray? = null
    private var fileMaxKey: ByteArray? = null
    private var fileMinTs = Long.MAX_VALUE
    private var fileMaxTs = Long.MIN_VALUE
    private val bloomKeys = ArrayList<ByteArray>()

    var lastInternalKey: ByteArray? = null
        private set

    /**
     * 追加一个版本。调用者必须保证顺序：userKey 升序、同 key 内 commitTs 降序。
     */
    fun add(userKey: ByteArray, commitTs: Long, value: ByteArray?) {
        val isTombstone = value == null

        // 只有在 key 边界处才允许切块，保证同一 key 的版本不被拆开
        val startsNewKey = acc.lastUserKey == null || !userKey.contentEquals(acc.lastUserKey!!)
        if (startsNewKey && cur.size() >= blockSizeBytes && acc.entries > 0) {
            flushBlock()
        }

        if (startsNewKey) {
            // 同一 key 的版本按 ts 降序相邻，因此「上一个 key 与本 key 相同」
            // 就等价于「本文件含多版本」。
            val prev = lastKeyOverall
            if (prev != null && prev.contentEquals(userKey)) sawDuplicateKey = true
            lastKeyOverall = userKey
            if (acc.lastUserKey != null) {
                if (acc.lastUserKeyIsTombstone) acc.tombstones++ else acc.rows++
            }
            acc.lastUserKey = userKey
            acc.lastUserKeyIsTombstone = isTombstone
            if (acc.firstKey == null) acc.firstKey = userKey
            bloomKeys.add(userKey)
        }

        writeEntry(cur, userKey, commitTs, value)
        if (blockObservers.isNotEmpty()) blockEntries.add(ObservedEntry(if (isTombstone) null else value))
        acc.entries++
        acc.minTs = minOf(acc.minTs, commitTs)
        acc.maxTs = maxOf(acc.maxTs, commitTs)
        acc.observeTs(commitTs, isTombstone)
        fileMinTs = minOf(fileMinTs, commitTs)
        fileMaxTs = maxOf(fileMaxTs, commitTs)

        if (fileMinKey == null || ByteArrayComparator.compare(userKey, fileMinKey!!) < 0) fileMinKey = userKey
        if (fileMaxKey == null || ByteArrayComparator.compare(userKey, fileMaxKey!!) > 0) fileMaxKey = userKey

        if (!isTombstone) acc.observeRow(value!!)
    }

    private fun flushBlock() {
        if (acc.entries == 0) return
        // 跳数索引：把这一块的行交给观察者。**必须在重置累加器之前**，
        // 因为块的编号与 `.idx` 里的块序必须严格对齐——错一格，
        // 剪枝就会用另一个块的统计量去判断，进而**漏掉本该读的块**。
        if (blockObservers.isNotEmpty()) {
            val blockIdx = blocks.size
            for (o in blockObservers) o.beginBlock(blockIdx)
            for (e in blockEntries) {
                if (e.value == null) continue
                for (o in blockObservers) o.observe(RowCodec.decode(e.value, schema))
            }
            for (o in blockObservers) o.endBlock()
        }
        blockEntries.clear()
        // 结算块内最后一个 key
        if (acc.lastUserKey != null) {
            if (acc.lastUserKeyIsTombstone) acc.tombstones++ else acc.rows++
        }

        val body = cur.toByteArray()
        val offset = data.size().toLong()
        // 块前缀记录条目数，便于读取时预分配
        val header = ByteArrayOutputStream(8)
        Varint.writeUnsigned(header, acc.entries)
        val headerBytes = header.toByteArray()

        // 逻辑载荷 = 条目数 + 条目字节；压缩作用在它上面（见 BlockEntry 的格式说明）
        val payload = ByteArray(headerBytes.size + body.size)
        System.arraycopy(headerBytes, 0, payload, 0, headerBytes.size)
        System.arraycopy(body, 0, payload, headerBytes.size, body.size)
        val encoded = blockEncoder.encode(payload)

        val full = ByteArray(encoded.bytes.size + 4)
        System.arraycopy(encoded.bytes, 0, full, 0, encoded.bytes.size)
        val crc = Checksums.crc32c(full, 0, full.size - 4)
        full[full.size - 4] = ((crc ushr 24) and 0xFF).toByte()
        full[full.size - 3] = ((crc ushr 16) and 0xFF).toByte()
        full[full.size - 2] = ((crc ushr 8) and 0xFF).toByte()
        full[full.size - 1] = (crc and 0xFF).toByte()

        data.write(full)
        if (encoded.compression != Compression.NONE) anyBlockCompressed = true

        blocks.add(
            BlockIndexEntry(
                offset = offset,
                length = full.size,
                entries = acc.entries,
                rows = acc.rows,
                tombstones = acc.tombstones,
                minTs = acc.minTs,
                maxTs = acc.maxTs,
                firstKey = acc.firstKey ?: ByteArray(0),
                compression = encoded.compression,
                colStats = acc.buildStats(),
                tsCounts = acc.buildTsCounts(),
            )
        )
        totalRows += acc.rows
        totalTombstones += acc.tombstones

        cur = ByteArrayOutputStream(blockSizeBytes)
        acc = BlockAccumulator()
    }

    fun finish(): SstWriteResult {
        flushBlock()
        val idx = SstIndex(
            // 版本号只表达「读者必须理解什么才能读这个文件」：一个块都没压时，
            // 读者并不需要理解压缩，标成 2 只会让旧读者拒绝一份它本可正确读取的文件。
            formatVersion = if (anyBlockCompressed) SST_FORMAT_VERSION_COMPRESSED else SST_FORMAT_VERSION_PLAIN,
            // 记录写入意图（供审计/巡检）；解码不看它，看每个块的 compression
            compression = if (compress) Compression.LZ4 else Compression.NONE,
            level = level,
            rows = totalRows,
            tombstones = totalTombstones,
            minKey = fileMinKey ?: ByteArray(0),
            maxKey = fileMaxKey ?: ByteArray(0),
            minTs = if (fileMinTs == Long.MAX_VALUE) 0 else fileMinTs,
            maxTs = if (fileMaxTs == Long.MIN_VALUE) 0 else fileMaxTs,
            blocks = blocks,
            bloom = BloomData.build(bloomKeys, bitsPerKey = 10),
            schemaVersion = schema.version,
            // 只有当文件内每个 key 确实只出现一次时才标记为已去重——
            // COUNT 的元数据路径依赖这个标记（设计文档 §8.9.3）
            deduplicated = !sawDuplicateKey,
        )
        return SstWriteResult(data.toByteArray(), idx)
    }

    /** 当前已写入的数据字节数，供调用方决定何时轮转。 */
    fun currentSize(): Long = data.size().toLong() + cur.size()

    val isEmpty: Boolean get() = blocks.isEmpty() && acc.entries == 0

    // ── 块级统计累加器 ──────────────────────────────────────────────

    private inner class BlockAccumulator {
        var entries = 0
        var rows = 0
        var tombstones = 0
        var minTs = Long.MAX_VALUE
        var maxTs = Long.MIN_VALUE
        var firstKey: ByteArray? = null
        var lastUserKey: ByteArray? = null
        var lastUserKeyIsTombstone = false

        private val mins = arrayOfNulls<ByteArray>(schema.columns.size)
        private val maxs = arrayOfNulls<ByteArray>(schema.columns.size)
        private val nulls = LongArray(schema.columns.size)
        private val counts = arrayOfNulls<LinkedHashMap<String, Pair<ByteArray, Long>>>(schema.columns.size)
        private var tsOverflow = false

        /** 逐行解码以累积列统计。这是写路径为读路径付的代价（设计文档 §8.9.9）。 */
        fun observeRow(encoded: ByteArray) {
            val row = RowCodec.decode(encoded, schema)
            for (i in row.indices) {
                val d = row[i]
                val type = schema.columns[i].type
                if (d is Datum.Null) { nulls[i]++; continue }
                val enc = KeyEncoder.encodeDatum(d, type)
                val mn = mins[i]
                if (mn == null || ByteArrayComparator.compare(enc, mn) < 0) mins[i] = enc
                val mx = maxs[i]
                if (mx == null || ByteArrayComparator.compare(enc, mx) > 0) maxs[i] = enc
                if (i in countColumns) {
                    val m = counts[i] ?: LinkedHashMap<String, Pair<ByteArray, Long>>().also { counts[i] = it }
                    // 用 latin1 作为 map 键，保证任意字节都能安全表示
                    val k = String(enc, Charsets.ISO_8859_1)
                    val prev = m[k]
                    if (prev == null) {
                        if (m.size >= maxDistinctPerBlock) {
                            // 基数超限：放弃该列的计数收集，标记为不可用
                            counts[i] = null
                            abandonedColumns.add(i)
                        } else m[k] = enc to 1L
                    } else m[k] = prev.first to (prev.second + 1)
                }
            }
        }

        /** ts → 该 ts 上的存活（非墓碑）条目数。一个提交批次产生大量同 ts 条目，故通常很小。 */
        private val tsLive = sortedMapOf<Long, Long>()

        fun observeTs(ts: Long, isTombstone: Boolean) {
            if (!isTombstone) tsLive[ts] = (tsLive[ts] ?: 0L) + 1
            else tsLive.putIfAbsent(ts, 0L)
            // 提交批次足够大时，不同 ts 的个数远小于条目数；但「一事务一行」的负载
            // 会让它退化成 1:1，把 .idx 撑到与数据同量级。超过上限就不再收集——
            // 计数路径会回退到精确扫描，而不会给出错误的估算值（§8.9.7）。
            if (tsLive.size > maxTsBucketsPerBlock) tsOverflow = true
        }

        /** 转成累积计数表，供 §8.9.2 的精确 MVCC 计数做二分查找。 */
        fun buildTsCounts(): List<TsBucket> {
            if (tsLive.isEmpty() || tsOverflow) return emptyList()
            var cum = 0L
            val out = ArrayList<TsBucket>(tsLive.size)
            for ((ts, cnt) in tsLive) {
                cum += cnt
                out.add(TsBucket(ts, cum))
            }
            return out
        }

        fun buildStats(): List<ColumnStat> = List(schema.columns.size) { i ->
            val vc = counts[i]?.takeIf { i !in abandonedColumns }
                ?.values?.map { ValueCount(it.first, it.second) }
            ColumnStat(mins[i], maxs[i], nulls[i], vc)
        }
    }

    private val abandonedColumns = HashSet<Int>()

    // ── 条目编码 ──────────────────────────────────────────────────

    companion object {
        fun writeEntry(
            out: ByteArrayOutputStream,
            userKey: ByteArray,
            commitTs: Long,
            value: ByteArray?,
        ) {
            Varint.writeUnsigned(out, userKey.size)
            out.write(userKey)
            // commitTs 取反：块内也保持「新版本在前」的阅读顺序
            BigEndian.writeLong(out, commitTs.inv())
            val flags = if (value == null) 1 else 0
            out.write(flags)
            if (value != null) {
                Varint.writeUnsigned(out, value.size)
                out.write(value)
            }
        }

        /**
         * 解码一个块的全部条目。
         *
         * [bytes] 是**解压后的逻辑载荷**（`[varint 条目数][条目…]`），不含末尾 CRC，
         * 也不含压缩头——取出它的是 [BlockDecoder]。压缩算法的选择只影响
         * 「怎么存」，不影响「怎么解析」：一个 LZ4 块与一个未压缩块解出来的
         * 逻辑载荷完全同构，因此这里不需要知道压缩的存在。
         */
        fun readBlock(bytes: ByteArray): List<BlockEntry> {
            val (count, n0) = Varint.readUnsigned(bytes, 0)
            var p = n0
            val out = ArrayList<BlockEntry>(count)
            repeat(count) {
                val (klen, n1) = Varint.readUnsigned(bytes, p); p += n1
                val key = bytes.copyOfRange(p, p + klen); p += klen
                val inv = BigEndian.readLong(bytes, p); p += 8
                val flags = bytes[p].toInt(); p += 1
                val isTombstone = (flags and 1) != 0
                if (isTombstone) {
                    out.add(BlockEntry(key, inv.inv(), true, null))
                } else {
                    val (vlen, n2) = Varint.readUnsigned(bytes, p); p += n2
                    val v = bytes.copyOfRange(p, p + vlen); p += vlen
                    out.add(BlockEntry(key, inv.inv(), false, v))
                }
            }
            return out
        }
    }
}
