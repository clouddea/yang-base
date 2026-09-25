package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.ByteArrayComparator
import com.cloudea.yangbase.common.CommitTs
import com.cloudea.yangbase.common.ReadTs

/** 合并迭代器里的一条有序条目（尚未做可见性判断）。 */
class OrderedEntry(
    val userKey: ByteArray,
    val commitTs: CommitTs,
    val value: ByteArray?,
)

/**
 * 有序行迭代器。按内部键序产出：userKey 升序、同 key 内 commitTs 降序。
 *
 * 用「peek + advance」而非 Kotlin 的 Sequence，是因为底层读取是挂起操作——
 * Sequence 无法在 `yield` 中挂起。
 */
interface RowIterator {
    /** 当前条目；返回 null 表示已耗尽。 */
    suspend fun peek(): OrderedEntry?

    /** 前进到下一条目。 */
    suspend fun advance()

    /** 释放资源（如关闭块迭代）。 */
    suspend fun close() {}
}

/** 把 MemTable 包装成迭代器。 */
class MemTableIterator(private val mem: MemTable) : RowIterator {
    private val it = mem.allEntries().iterator()
    private var cur: OrderedEntry? = null

    init { /* 惰性初始化在 peek 中完成 */ }

    override suspend fun peek(): OrderedEntry? {
        if (cur == null && it.hasNext()) cur = decode(it.next())
        return cur
    }

    override suspend fun advance() {
        cur = if (it.hasNext()) decode(it.next()) else null
    }

    private fun decode(p: Pair<ByteArray, ByteArray?>): OrderedEntry {
        val ik = p.first
        return OrderedEntry(InternalKey.userKeyOf(ik), InternalKey.commitTsOf(ik), p.second)
    }
}

/**
 * 在多个已排序来源上做 K 路归并，并按 [readTs] 做 MVCC 可见性判断。
 *
 * 单分片内同一 key 可能存在于多个来源（MemTable、L0 的多个文件、L1+ 的某层），
 * 归并时取 **commitTs 最大且 ≤ readTs** 的那个版本——这就是快照隔离的读路径
 * （设计文档 §7.1）。
 */
class MergingIterator(
    private val sources: List<RowIterator>,
    private val readTs: ReadTs,
) : RowIterator {

    private var pending: OrderedEntry? = null
    private var eos = false

    override suspend fun peek(): OrderedEntry? {
        if (pending == null && !eos) pending = computeNext()
        return pending
    }

    override suspend fun advance() {
        pending = if (eos) null else computeNext()
    }

    /**
     * 找到最小的 userKey，把所有来源中该 key 的所有版本消费掉，
     * 返回其中最新且可见的版本。墓碑会让它返回 null 但**不终止迭代**。
     */
    private suspend fun computeNext(): OrderedEntry? {
        while (true) {
            var minKey: ByteArray? = null
            for (s in sources) {
                val e = s.peek() ?: continue
                if (minKey == null || ByteArrayComparator.compare(e.userKey, minKey!!) < 0) {
                    minKey = e.userKey
                }
            }
            if (minKey == null) { eos = true; return null }
            val key = minKey!!

            var bestTs = Long.MIN_VALUE
            var bestValue: ByteArray? = null
            var anyVisible = false

            for (s in sources) {
                while (true) {
                    val e = s.peek() ?: break
                    if (ByteArrayComparator.compare(e.userKey, key) != 0) break
                    if (Visibility.visible(e.commitTs, readTs) && e.commitTs.value > bestTs) {
                        bestTs = e.commitTs.value
                        bestValue = e.value
                        anyVisible = true
                    }
                    s.advance()
                }
            }

            if (anyVisible) {
                // 墓碑：该 key 在此快照已被删除，跳过但继续迭代
                if (bestValue != null) return OrderedEntry(key, CommitTs(bestTs), bestValue)
            }
            // 没有可见版本，或可见版本是墓碑 → 继续找下一个 key
        }
    }

    override suspend fun close() {
        for (s in sources) s.close()
    }
}
