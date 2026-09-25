package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.CommitTs
import com.cloudea.yangbase.common.CorruptDataException
import com.cloudea.yangbase.common.Checksums
import com.cloudea.yangbase.common.FileId
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import kotlinx.serialization.cbor.Cbor

/** 一个已加载索引的 SST 文件句柄。索引常驻内存，数据块按需拉取。 */
class SstHandle(
    val shard: ShardId,
    val level: Int,
    val fileId: FileId,
    val index: SstIndex,
) {
    val dataKey: String get() = ObjectKeys.sstData(shard, level, fileId)
    val indexKey: String get() = ObjectKeys.sstIndex(shard, level, fileId)

    fun dataKeyFor(blockIdx: Int): String = "${dataKey}#${index.blocks[blockIdx].offset}"

    override fun toString(): String = "Sst($level/$fileId rows=${index.rows})"
}

/**
 * SST 读取器。
 *
 * 「点查 = 1 次 GET」的实现路径（设计文档 §2.3、§6.4）：
 *  manifest（常驻） → bloom 判定 → `.idx`（常驻）→ 二分定位块 → **1 次范围 GET**
 *
 * 因为 `.idx` 在内存里，第 4 步之前的所有操作都是零 IO。
 *
 * 块级压缩（§6.4）不改这条路径的**形状**：压的是块而不是文件，范围读、块级剪枝、
 * 请求数一分不变，省下的是传输与存储的字节数。文件级压缩会让范围读失效，
 * 所以它不是一个可选项。（P4：请求数是一等成本；压缩影响的是字节数。）
 */
class SstReader(
    private val store: ObjectStore,
    private val cache: BlockCache,
) {
    private val cbor = Cbor { ignoreUnknownKeys = true }

    /** 读取并解析 `.idx`。大部分情况下命中缓存，0 次 GET。 */
    suspend fun loadIndex(shard: ShardId, level: Int, fileId: FileId): SstHandle {
        val key = ObjectKeys.sstIndex(shard, level, fileId)
        // .idx 不可变（文件名带文件 id）→ 可以进二级缓存
        val bytes = cache.getOrLoad("idx:$key", immutableKey = true) { store.get(key) }
        val idx = cbor.decodeFromByteArray(SstIndex.serializer(), bytes)
        // 同时接受 1 与 2：v1 是加压缩之前写下的文件，它们永远要能读（见 SstFormat 的版本常量）
        if (idx.formatVersion < SST_FORMAT_VERSION_PLAIN || idx.formatVersion > SST_FORMAT_VERSION_MAX) {
            throw CorruptDataException(
                "不支持的 .idx 格式版本: ${idx.formatVersion}" +
                    "（本实现支持 $SST_FORMAT_VERSION_PLAIN..$SST_FORMAT_VERSION_MAX）"
            )
        }
        return SstHandle(shard, level, fileId, idx)
    }

    /** 读取一个数据块。带 CRC 校验的去重缓存读取。 */
    suspend fun readBlock(handle: SstHandle, blockIdx: Int): List<BlockEntry> {
        val key = handle.dataKeyFor(blockIdx)
        // 数据块同理：同一个 (文件, 块号) 永远对应同一段字节
        val raw = cache.getOrLoad(key, immutableKey = true) { store.get(handle.dataKey, rangeOf(handle, blockIdx)) }
        verifyCrc(raw)
        // 压缩算法**只从格式里读**（P2）：写时用什么压的，全部记在 .idx 上，读时没有第二个真相。
        // 顺序也是刻意的——先验 CRC 再解压，损坏的压缩流体根本进不了解压器。
        val payload = BlockDecoder.decode(raw, 0, raw.size - 4, handle.index.blocks[blockIdx].compression)
        return SstWriter.readBlock(payload)
    }

    private fun rangeOf(handle: SstHandle, blockIdx: Int) = com.cloudea.yangbase.objectstore.ByteRange(
        handle.index.blocks[blockIdx].offset,
        handle.index.blocks[blockIdx].offset + handle.index.blocks[blockIdx].length - 1,
    )

    private fun verifyCrc(raw: ByteArray) {
        if (raw.size < 4) throw CorruptDataException("数据块过短")
        val expected = com.cloudea.yangbase.common.BigEndian.readInt(raw, raw.size - 4)
        val actual = Checksums.crc32c(raw, 0, raw.size - 4)
        if (expected != actual) {
            throw CorruptDataException("数据块 CRC 校验失败: 期望=${"%08x".format(expected)} 实际=${"%08x".format(actual)}")
        }
    }

    /**
     * 点查。[userKey] 在 [readTs] 处的可见版本。
     * 至多产生 **1 次** 范围 GET（且仅当 `.idx` 已在内存中时）。
     */
    suspend fun get(handle: SstHandle, userKey: ByteArray, readTs: ReadTs): LookupResult {
        val idx = handle.index
        if (!idx.bloom.mightContain(userKey)) return LookupResult.NotFound
        val bi = locateBlock(idx, userKey) ?: return LookupResult.NotFound

        val entries = readBlock(handle, bi)
        var bestTs = Long.MIN_VALUE
        var bestValue: ByteArray? = null
        var any = false
        for (e in entries) {
            val c = ByteArrayComparator.compare(e.userKey, userKey)
            if (c > 0) break
            if (c < 0) continue
            if (e.commitTs <= readTs.value && e.commitTs > bestTs) {
                bestTs = e.commitTs
                bestValue = e.value
                any = true
            }
        }
        if (!any) return LookupResult.NotFound
        return if (bestValue == null) LookupResult.Deleted else LookupResult.Found(bestValue)
    }

    /** 二分定位可能包含 [userKey] 的块。块首键有序，且同一 key 的版本不跨块。 */
    private fun locateBlock(idx: SstIndex, userKey: ByteArray): Int? {
        if (idx.blocks.isEmpty()) return null
        if (ByteArrayComparator.compare(userKey, idx.minKey) < 0) return null
        if (ByteArrayComparator.compare(userKey, idx.maxKey) > 0) return null
        var lo = 0
        var hi = idx.blocks.size - 1
        var ans = 0
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            if (ByteArrayComparator.compare(idx.blocks[mid].firstKey, userKey) <= 0) {
                ans = mid; lo = mid + 1
            } else hi = mid - 1
        }
        return ans
    }

    /** 范围扫描迭代器。 */
    fun iterator(
        handle: SstHandle,
        from: ByteArray?,
        to: ByteArray?,
        toInclusive: Boolean = false,
        /** 块级剪枝（跳数索引）。为 null 时每个块都会被读。 */
        skipper: BlockSkipper? = null,
    ): RowIterator = SstIterator(this, handle, from, to, toInclusive, skipper)

    /**
     * 该文件在 [readTs] 处的可见行数——**纯元数据计算，0 次 GET**（设计文档 §8.9.2）。
     *
     * 返回 -1 表示无法从元数据得出（需要回源），调用方必须回退到精确扫描
     * 而不是使用估算值（§8.9.7 的「宁可慢，不可错」）。
     *
     * 注意：仅对 leveled 文件（每 key 单版本）精确。L0 需要上层做去重。
     */
    fun visibleRows(handle: SstHandle, readTs: ReadTs): Long {
        val idx = handle.index
        if (idx.maxTs <= readTs.value) return idx.rows          // 整个文件可见
        if (idx.minTs > readTs.value) return 0L                 // 整个文件不可见
        if (!idx.deduplicated) return -1L                       // L0：多版本，需要去重
        var sum = 0L
        for (b in idx.blocks) {
            val r = b.visibleRowsAt(readTs.value)
            if (r < 0) return -1L
            sum += r
        }
        return sum
    }
}

/**
 * 按需拉取数据块的 SST 迭代器。只读取与 `[from, to)` 真正重叠的块——
 * 这是范围扫描不会退化成全文件读取的保证。
 */
class SstIterator(
    private val reader: SstReader,
    private val handle: SstHandle,
    private val from: ByteArray?,
    private val to: ByteArray?,
    private val toInclusive: Boolean = false,
    /**
     * 块级剪枝（设计文档 §8.5）。
     *
     * 这是跳数索引**唯一**省下 IO 的地方：被它判否的块不会被
     * [SstReader.readBlock] 读进来，因此一次 GET 都不产生。
     * 在别处「跳过」是没有意义的——块一旦读了，IO 就已经花掉了。
     */
    private val skipper: BlockSkipper? = null,
) : RowIterator {

    /** 被剪枝跳过的块数。测试与监控据此断言「剪枝真的生效了」。 */
    var blocksSkipped: Int = 0
        private set

    private val blocks = handle.index.blocks
    private var blockIdx: Int = startBlock()
    private var entries: List<BlockEntry> = emptyList()
    private var pos = 0
    private var cur: OrderedEntry? = null

    private fun startBlock(): Int {
        if (from == null) return 0
        if (blocks.isEmpty()) return 0
        var lo = 0; var hi = blocks.size - 1; var ans = 0
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            if (ByteArrayComparator.compare(blocks[mid].firstKey, from) <= 0) { ans = mid; lo = mid + 1 } else hi = mid - 1
        }
        return ans
    }

    private suspend fun loadNextBlock(): Boolean {
        while (blockIdx < blocks.size) {
            val firstKey = blocks[blockIdx].firstKey
            // 块首已超过上界 → 后续块都不需要了
            if (to != null && ByteArrayComparator.compare(firstKey, to) > 0) return false
            // ★ 跳数索引剪枝：在**读块之前**问一句。
            // 顺序很重要——先读再判断等于没剪，IO 已经花掉了。
            if (skipper != null && !skipper.mightMatchBlock(handle, blockIdx)) {
                blocksSkipped++
                blockIdx++
                continue
            }
            // 闭区间时，块首恰好等于上界也要读
            entries = reader.readBlock(handle, blockIdx)
            blockIdx++
            pos = 0
            if (entries.isNotEmpty()) return true
        }
        return false
    }

    override suspend fun peek(): OrderedEntry? {
        if (cur == null) cur = advanceToNext()
        return cur
    }

    override suspend fun advance() {
        cur = advanceToNext()
    }

    private suspend fun advanceToNext(): OrderedEntry? {
        while (true) {
            if (pos < entries.size) {
                val e = entries[pos]
                pos++
                if (from != null && ByteArrayComparator.compare(e.userKey, from) < 0) continue
                if (to != null) {
                    val c = ByteArrayComparator.compare(e.userKey, to)
                    if (c > 0 || (c == 0 && !toInclusive)) return null
                }
                return OrderedEntry(e.userKey, CommitTs(e.commitTs), e.value)
            }
            if (!loadNextBlock()) return null
        }
    }
}
