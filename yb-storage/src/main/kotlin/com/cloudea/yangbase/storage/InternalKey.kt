package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.BigEndian
import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.CommitTs
import com.cloudea.yangbase.common.CorruptDataException
import com.cloudea.yangbase.common.OrderPreservingCodec
import com.cloudea.yangbase.common.ReadTs
import java.io.ByteArrayOutputStream

/**
 * LSM 内部键。
 *
 * ```
 * internalKey = orderPreserving(userKey) || invertedCommitTs(8B)
 * ```
 * 排序语义为 **userKey 升序、commitTs 降序**——同一 userKey 的最新版本排在最前，
 * 读时取第一个可见版本即可短路（设计文档 §6.2）。
 *
 * 取反的原理：`~a > ~b` 当且仅当 `a < b`，因此把取反值按大端（无符号）比较，
 * 得到的正是 commitTs 的降序。
 */
object InternalKey {

    const val TS_SIZE = 8

    /** 构造内部键。 */
    fun encode(userKey: ByteArray, ts: CommitTs): ByteArray {
        val uk = OrderPreservingCodec.encodeBytes(userKey)
        val out = ByteArray(uk.size + TS_SIZE)
        System.arraycopy(uk, 0, out, 0, uk.size)
        val inv = ts.value.inv()
        var off = uk.size
        for (i in 0 until 8) out[off + i] = ((inv ushr (56 - i * 8)) and 0xFF).toByte()
        return out
    }

    /** 解出 userKey 与 commitTs。 */
    fun decode(internalKey: ByteArray): Pair<ByteArray, CommitTs> {
        val (uk, n) = OrderPreservingCodec.decodeBytes(internalKey, 0)
        if (internalKey.size < n + TS_SIZE) throw CorruptDataException("内部键缺少 commitTs")
        val inv = BigEndian.readLong(internalKey, n)
        return uk to CommitTs(inv.inv())
    }

    /** 只解出 userKey，避免不必要的分配（范围扫描的热路径）。 */
    fun userKeyOf(internalKey: ByteArray): ByteArray =
        OrderPreservingCodec.decodeBytes(internalKey, 0).first

    fun commitTsOf(internalKey: ByteArray): CommitTs {
        val (_, n) = OrderPreservingCodec.decodeBytes(internalKey, 0)
        return CommitTs(BigEndian.readLong(internalKey, n).inv())
    }

    /** 提取编码后的 userKey 部分的长度（含终结符）。 */
    fun encodedUserKeyLength(internalKey: ByteArray): Int =
        OrderPreservingCodec.decodeBytes(internalKey, 0).second

    /**
     * 构造「某个 userKey 及其所有版本」的前缀，用于 SeekPrefix。
     * 返回的是编码形式，可直接与内部键做字节比较。
     */
    fun userKeyPrefix(userKey: ByteArray): ByteArray = OrderPreservingCodec.encodeBytes(userKey)

    /** 该内部键是否属于给定的 userKey。 */
    fun belongsTo(internalKey: ByteArray, encodedPrefix: ByteArray): Boolean =
        ByteArrayComparator.compare(
            internalKey, 0, minOf(encodedPrefix.size, internalKey.size),
            encodedPrefix, 0, minOf(encodedPrefix.size, internalKey.size),
        ) == 0 && internalKey.size >= encodedPrefix.size

    /**
     * 构造「seek 到 userKey」用的下界键：commitTs 取最大值，
     * 因为降序排列使得最新版本排在最前。
     */
    fun seekKey(userKey: ByteArray): ByteArray = encode(userKey, CommitTs(Long.MAX_VALUE))

    /** 构造「userKey 的所有版本之后」的上界键（不含）。 */
    fun endOfUserKey(userKey: ByteArray): ByteArray {
        val uk = OrderPreservingCodec.encodeBytes(userKey)
        // 在终结符之后追加一个 0x00，得到严格大于所有该 userKey 版本的最小键
        val out = ByteArray(uk.size + 1)
        System.arraycopy(uk, 0, out, 0, uk.size)
        out[uk.size] = 0
        return out
    }

    /** 解析时间戳字段的偏移。 */
    fun tsOffset(internalKey: ByteArray): Int = encodedUserKeyLength(internalKey)

    /** 比较两个内部键（字节序，与 LSM 的排序一致）。 */
    fun compare(a: ByteArray, b: ByteArray): Int = ByteArrayComparator.compare(a, b)

    /** 供调试输出。 */
    fun describe(internalKey: ByteArray): String {
        val (uk, ts) = decode(internalKey)
        return "(${String(uk, Charsets.UTF_8)}, $ts)"
    }

    /** 拼接辅助：用于把多个分量组成 userKey。 */
    fun concatUserKey(vararg parts: ByteArray): ByteArray {
        val out = ByteArrayOutputStream()
        for (p in parts) out.write(p)
        return out.toByteArray()
    }
}

/** 可见性判断的唯一依据（设计文档 §7.1）。 */
object Visibility {
    /** 行版本对 readTs 可见 ⟺ 其 commitTs ≤ readTs。 */
    fun visible(commitTs: CommitTs, readTs: ReadTs): Boolean = commitTs.value <= readTs.value

    fun visible(commitTs: CommitTs, readTs: CommitTs): Boolean = commitTs.value <= readTs.value
}
