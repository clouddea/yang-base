package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.CorruptDataException
import com.cloudea.yangbase.common.Varint
import kotlinx.serialization.Serializable
import net.jpountz.lz4.LZ4Exception
import net.jpountz.lz4.LZ4Factory
import java.io.ByteArrayOutputStream

/**
 * `.idx` 格式版本：文件里**只有未压缩块**。
 *
 * v1 的读者可以正确读它——正因如此，`compress = false` 写出的文件
 * 与加压缩之前逐字节相同。
 */
const val SST_FORMAT_VERSION_PLAIN: Int = 1

/**
 * `.idx` 格式版本：文件里**存在 LZ4 压缩块**。
 *
 * 必须与 v1 区分开：一个只懂 v1 的读者若接受它，会把压缩字节当成条目字节
 * 解析（不报错、不降级，直接静默读出垃圾），因此版本号是**协作式的拒绝机制**。
 */
const val SST_FORMAT_VERSION_COMPRESSED: Int = 2

/** 当前读者能理解的最高版本。 */
const val SST_FORMAT_VERSION_MAX: Int = SST_FORMAT_VERSION_COMPRESSED

/**
 * `.idx` 对象的格式（设计文档 §6.4）。
 *
 * **这是「点查 = 1 次 GET」与「COUNT = 0 次 GET」的实现载体**：
 * 它小到可以整体常驻内存，且携带了块级的 commit_ts 范围（计数用）
 * 与列级 min/max/值计数（剪枝与谓词计数用）。
 *
 * 用 CBOR 而非手写二进制：它只在加载时解析一次，之后常驻内存，
 * 因此可读性与低代码量比解析速度更值钱。
 *
 * **格式的演进方式：只新增带默认值的字段**（`kotlinx-serialization` 对缺失字段
 * 用默认值填充，`encodeDefaults` 默认为 false 因此默认值不占字节）。
 * 于是「旧文件永远读得出来」是结构性的，而不是靠一次性兼容代码。
 */
@Serializable
data class SstIndex(
    val formatVersion: Int = SST_FORMAT_VERSION_PLAIN,
    /**
     * 写出本文件时**配置**的压缩算法（`NONE` = 关闭了压缩）。
     *
     * ⚠️ 它记录的是意图，**不是**解码依据：`compress = true` 时高熵块会回退成
     * 未压缩存储，因此 LZ4 的文件里同样可能有未压缩块。每个块真正用了什么，
     * 看 [BlockIndexEntry.compression]——那一层才是唯一的事实。
     */
    val compression: Compression = Compression.NONE,
    val level: Int,
    /** 非墓碑行的**去重**计数：同一 userKey 只按最新版本算一次。 */
    val rows: Long,
    /** 最新版本为墓碑的 userKey 数。 */
    val tombstones: Long,
    val minKey: ByteArray,
    val maxKey: ByteArray,
    /** 文件内最小的 commitTs，用于 §8.9.2 的 MVCC 感知计数。 */
    val minTs: Long,
    /** 文件内最大的 commitTs。 */
    val maxTs: Long,
    val blocks: List<BlockIndexEntry>,
    val bloom: BloomData,
    val schemaVersion: Long = 0,
    /** 该文件是否对多版本做了去重（leveled 层为 true）。 */
    val deduplicated: Boolean = false,
) {
    /** 键是否可能在本文件内（bloom 判定）。 */
    fun mightContain(key: ByteArray): Boolean = bloom.mightContain(key)

    fun encodedSizeEstimate(): Long = 200L + blocks.sumOf { it.estimatedBytes() }
}

@Serializable
data class BlockIndexEntry(
    val offset: Long,
    val length: Int,
    /** 版本条目数（含墓碑与多版本）。 */
    val entries: Int,
    /** 本块内最新版本非墓碑的 userKey 数。 */
    val rows: Int,
    val tombstones: Int,
    val minTs: Long,
    val maxTs: Long,
    val firstKey: ByteArray,
    /**
     * 本块在 `.dat` 里的**存储形态**。解码必须看它，不能看 [SstIndex.compression]。
     *
     * 旧文件的 `.idx` 没有这个字段 → 默认 `NONE`，恰好等于它们的真实形态，
     * 这就是「已写下的未压缩 SST 照样能读」的全部秘密。
     */
    val compression: Compression = Compression.NONE,
    val colStats: List<ColumnStat> = emptyList(),
    /**
     * 块内的 commit_ts 累积计数表，用于**精确**的 MVCC 感知计数（§8.9.2）。
     *
     * ⚠️ 实现期间发现的设计修正：原设计声称「单写者 ⇒ 文件内 commit_ts 沿块序单调，
     * 因此一个 read_ts 至多切到一个块」。**这个推理是错的**——一个 MemTable flush
     * 出的文件，其所有块共享同一段时间窗口，ts 在块之间并不单调。
     *
     * 正确且更简单的做法：在每个块内记录「ts → 截至该 ts 的存活条目累积数」。
     * 计数时对每块做一次二分查找即可，**依然精确、依然 0 次 GET**。
     * 代价是每块约 +1% 的元数据体积（块内不同 ts 的个数远小于条目数，
     * 因为一个提交批次会产生大量同 ts 的条目）。
     */
    val tsCounts: List<TsBucket> = emptyList(),
) {
    fun estimatedBytes(): Long =
        80L + firstKey.size + colStats.sumOf { it.estimatedBytes() } + tsCounts.size * 16L

    /**
     * 本块在 readTs 处的可见存活行数。**纯元数据计算，0 次 GET。**
     * 仅对 leveled 文件（每 key 只有一个版本）精确——L0 需要上层去重。
     */
    fun visibleRowsAt(readTs: Long): Long {
        if (rows == 0) return 0L
        if (maxTs <= readTs) return rows.toLong()          // 整块可见
        if (minTs > readTs) return 0L                       // 整块不可见
        if (tsCounts.isEmpty()) return -1L                  // 无表可用，需回源读取
        // 二分找到最大的 ts <= readTs
        var lo = 0
        var hi = tsCounts.size - 1
        var ans = -1
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            if (tsCounts[mid].ts <= readTs) { ans = mid; lo = mid + 1 } else hi = mid - 1
        }
        return if (ans < 0) 0L else tsCounts[ans].liveCumulative
    }
}

/**
 * 块内 commit_ts 的累积计数。
 * @param ts 一个出现过的 commit_ts
 * @param liveCumulative 块内所有 `ts <= 本 ts` 且非墓碑的条目数
 */
@Serializable
data class TsBucket(val ts: Long, val liveCumulative: Long)

/**
 * 块级列统计。三个用途：
 *  - `min`/`max` → 跳数索引（minmax）剪枝，见 §8.5
 *  - `nullCount` → `IS NULL` 谓词与优化器估算
 *  - `valueCounts` → **谓词 COUNT 的 0-GET 路径**，见 §8.9.4（仅低基数列收集）
 */
@Serializable
data class ColumnStat(
    val min: ByteArray? = null,
    val max: ByteArray? = null,
    val nullCount: Long = 0,
    /** 仅低基数列：值 → 该块内的出现次数。null 表示未收集。 */
    val valueCounts: List<ValueCount>? = null,
) {
    fun estimatedBytes(): Long = 40L + (min?.size ?: 0) + (max?.size ?: 0) +
        (valueCounts?.sumOf { 16L + it.value.size } ?: 0L)

    fun countOf(encodedValue: ByteArray): Long {
        val vc = valueCounts ?: return -1
        for (v in vc) if (v.value.contentEquals(encodedValue)) return v.count
        return 0
    }
}

@Serializable
data class ValueCount(val value: ByteArray, val count: Long)

/** 写出一个 SST 的产物：数据字节 + 索引。两者会分别 PUT 成 `.dat` 与 `.idx`。 */
class SstWriteResult(
    val data: ByteArray,
    val index: SstIndex,
) {
    val dataSize: Long get() = data.size.toLong()
}

// ── 块级压缩（设计文档 §6.4）────────────────────────────────────────

/**
 * 数据块的压缩算法。
 *
 * ⚠️ **枚举常量名是磁盘格式的一部分**：kotlinx-serialization 按名字编码枚举，
 * 把 `LZ4` 改名等于改格式，所有已写出的文件会立刻读不出来。
 */
@Serializable
enum class Compression {
    /**
     * 未压缩。
     *
     * 没有 `compression` 字段的旧 `.idx` 反序列化后就是它——**这是默认值的选择依据**：
     * 压缩是后加的能力，此前写下的每个块都是未压缩的。
     */
    NONE,

    /** LZ4 block 格式，前面带一个 varint 的未压缩长度。 */
    LZ4,
}

/** 一个已编码的数据块：存储字节 + **实际**采用的算法（未必等于配置的算法）。 */
class EncodedBlock(val bytes: ByteArray, val compression: Compression)

/**
 * 数据块的编码器。
 *
 * **每个写者一个实例**（[SstWriter] 内部持有）：它复用一块压缩缓冲区，省掉每块
 * 一次 64 KB 的分配，代价是**非线程安全**。多个分片各自 flush 会并发写 SST，
 * 共享一个实例会互相踩缓冲区——这正是它不能像 [BlockDecoder] 那样做成
 * 单例对象的原因。
 */
class BlockEncoder(private val compress: Boolean) {

    private val lz4 = LZ4Factory.fastestInstance().fastCompressor()
    private var scratch = ByteArray(0)

    /** 配置值。注意某个块最终是否压缩，要看 [encode] 的返回值。 */
    val enabled: Boolean get() = compress

    /**
     * 编码一个逻辑载荷。**压缩后不划算时不压缩**（零成本原则）。
     *
     * 判定必须把 LZ4 形态自己的头开销算进去：它比未压缩形态多一个 varint 长度前缀，
     * 所以「压完变小了」还不足以抵消那个前缀。小块与高熵数据（已压过的图片、
     * 随机字节）恰好落在这个区间——CPU 已经花掉了，再让对象变大就是双重损失。
     */
    fun encode(payload: ByteArray): EncodedBlock {
        if (!compress || payload.isEmpty()) return EncodedBlock(payload, Compression.NONE)
        val maxLen = lz4.maxCompressedLength(payload.size)
        if (scratch.size < maxLen) scratch = ByteArray(maxLen)
        val n = lz4.compress(payload, 0, payload.size, scratch, 0, maxLen)
        if (n + Varint.sizeOf(payload.size) >= payload.size) {
            return EncodedBlock(payload, Compression.NONE)
        }
        val out = ByteArrayOutputStream(n + 8)
        Varint.writeUnsigned(out, payload.size)
        out.write(scratch, 0, n)
        return EncodedBlock(out.toByteArray(), Compression.LZ4)
    }
}

/**
 * 数据块的解码器。
 *
 * **无状态，因此是单例**：任何线程、任何协程都可以直接用。
 * 调用方必须**先**验 CRC——CRC 覆盖的是存储字节，于是解压器只会看到
 * 通过完整性校验的输入。
 */
object BlockDecoder {

    /**
     * 解压后体积的上限。超过即判损坏，**不照着声明的长度去分配内存**：
     * 一个损坏的长度字段若能要求 4 GB 的缓冲区，损坏就升级成了节点 OOM。
     */
    const val MAX_UNCOMPRESSED_BYTES: Int = 64 * 1024 * 1024

    private val lz4 = LZ4Factory.fastestInstance().safeDecompressor()

    /**
     * @param stored 存储字节，**不含**末尾 4 字节 CRC
     * @param offset 起始下标（数据块是范围读出来的，因此调用方手上的数组通常不止这一块）
     * @param length 长度
     * @return 逻辑载荷（即 [SstWriter.readBlock] 的输入）
     */
    fun decode(stored: ByteArray, offset: Int, length: Int, compression: Compression): ByteArray =
        when (compression) {
            // 未压缩时零拷贝：直接借用调用方的数组（解析器只按声明读，不会越界）
            Compression.NONE ->
                if (offset == 0 && length == stored.size) stored else stored.copyOfRange(offset, offset + length)
            Compression.LZ4 -> decodeLz4(stored, offset, length)
        }

    fun decode(stored: ByteArray, compression: Compression): ByteArray =
        decode(stored, 0, stored.size, compression)

    private fun decodeLz4(stored: ByteArray, offset: Int, length: Int): ByteArray {
        if (length <= 0) throw CorruptDataException("LZ4 压缩块为空")
        val (uncompressed, prefixLen) = Varint.readUnsigned(stored, offset)
        if (uncompressed <= 0) throw CorruptDataException("LZ4 压缩块的未压缩长度非法: $uncompressed")
        if (uncompressed > MAX_UNCOMPRESSED_BYTES) {
            throw CorruptDataException(
                "LZ4 压缩块声明的未压缩长度 $uncompressed 超过上限 $MAX_UNCOMPRESSED_BYTES，拒绝解压"
            )
        }
        val bodyLen = length - prefixLen
        if (bodyLen <= 0) throw CorruptDataException("LZ4 压缩块缺少压缩载荷")
        val out = ByteArray(uncompressed)
        val written = try {
            lz4.decompress(stored, offset + prefixLen, bodyLen, out, 0, uncompressed)
        } catch (e: LZ4Exception) {
            throw CorruptDataException("LZ4 解压失败: ${e.message}")
        }
        if (written != uncompressed) {
            throw CorruptDataException("LZ4 解压长度与声明不符: 声明=$uncompressed 实际=$written")
        }
        return out
    }
}

/**
 * SST 数据块内的单条目。
 *
 * 块在 `.dat` 里的存储形态（设计文档 §6.4）：
 * ```
 * 逻辑载荷 = [varint 条目数][条目 0][条目 1]…
 * 存储形态 = 压缩算法决定，末尾统一跟 4 字节 crc32c（对 CRC 之前的**存储字节**取，
 *            因此损坏的压缩流体在解压之前就被拦下）
 *   NONE: [逻辑载荷]
 *   LZ4 : [varint 未压缩长度][lz4(逻辑载荷)]
 * ```
 * 块边界只由 key 边界与块目标大小决定，**与压缩无关**——压缩不会挪动块划分，
 * 因此 `.idx` 里的块级统计、剪枝下标、计数下标全部保持对齐。
 */
class BlockEntry(
    val userKey: ByteArray,
    val commitTs: Long,
    val isTombstone: Boolean,
    val value: ByteArray?,
)
