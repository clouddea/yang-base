package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.Bytes
import com.cloudea.yangbase.common.CommitTs
import com.cloudea.yangbase.common.ReadTs
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

/** 一次查找的结果。必须区分「已删除」与「不存在」——二者在 LSM 合并中的语义不同。 */
sealed interface LookupResult {
    data class Found(val value: ByteArray) : LookupResult
    /** 找到墓碑：该 key 在 readTs 处已被删除。 */
    data object Deleted : LookupResult
    /** 本层没有该 key（或没有可见版本）。 */
    data object NotFound : LookupResult
}

/** 扫描产出的一行（尚未解码）。 */
class RowVersion(val userKey: ByteArray, val value: ByteArray)

/**
 * 内存中的有序表（设计文档 §6.2）。
 *
 * key = [orderPreserving(userKey) || ~commitTs]，value = 行字节 或 null（墓碑）。
 * 排序为 userKey 升序、commitTs 降序，因此同一 key 的最新版本排在最前，
 * 读时取第一个可见版本即可短路。
 *
 * 与 RocksDB 的关键差异：**写入后不等 WAL 落盘**。持久化由 [WalWriter] 的
 * 组提交统一负责（设计文档 §6.3.4）。
 */
class MemTable(private val capacityBytes: Long = 64L * 1024 * 1024) {

    /**
     * ⚠️ `ConcurrentSkipListMap` **不允许 null 值**，而墓碑恰好用 null 表示。
     * 因此用一层 [Cell] 包装——这是实现期间踩到的真实缺陷（直接存 null 会在
     * `put` 时抛 NPE），不是为了好看。
     */
    private class Cell(val value: ByteArray?)

    private val map = ConcurrentSkipListMap<Bytes, Cell>()
    private val used = AtomicLong(0)
    private val entries = AtomicLong(0)

    /** 该 MemTable 内被写入过的 key 集合的基数（不去重，仅供估算）。 */
    val approximateSize: Long get() = used.get()
    val entryCount: Long get() = entries.get()
    val isFull: Boolean get() = used.get() >= capacityBytes

    /**
     * 清空。
     *
     * 目前只有一个用途：**只读分片**重建本地视图（属主 flush 之后，之前重放进来的
     * WAL 条目已经被 SST 覆盖，留着只会让内存表无限长大）。
     * 写者路径**不该**用它——那里的 MemTable 内容就是未落盘的数据，清掉即丢数据。
     */
    fun clear() {
        map.clear()
        used.set(0)
        entries.set(0)
    }

    fun put(userKey: ByteArray, ts: CommitTs, value: ByteArray?) {
        val ik = InternalKey.encode(userKey, ts)
        val key = Bytes(ik)
        val delta = ik.size.toLong() + (value?.size ?: 0).toLong() + 48 // 对象头与引用的粗略开销
        if (map.put(key, Cell(value)) == null) {
            used.addAndGet(delta)
            entries.incrementAndGet()
        }
    }

    /**
     * 查找 [userKey] 在 [readTs] 处的可见版本。
     * 因为版本按 commitTs 降序，第一个满足 `commitTs <= readTs` 的条目即为答案。
     */
    fun get(userKey: ByteArray, readTs: ReadTs): LookupResult {
        val start = Bytes(InternalKey.seekKey(userKey))
        val prefix = InternalKey.userKeyPrefix(userKey)
        val it = map.tailMap(start, true).entries.iterator()
        while (it.hasNext()) {
            val e = it.next()
            val ik = e.key.data
            if (!InternalKey.belongsTo(ik, prefix)) return LookupResult.NotFound
            val ts = InternalKey.commitTsOf(ik)
            if (Visibility.visible(ts, readTs)) {
                val v = e.value.value
                return if (v == null) LookupResult.Deleted else LookupResult.Found(v)
            }
            // commitTs 更大（更新）→ 对本次快照不可见，继续看更旧的版本
        }
        return LookupResult.NotFound
    }

    /**
     * 范围扫描 `[from, to)`，按 userKey 升序产出每个 key 在 [readTs] 处的可见版本。
     * 墓碑会被跳过（不产出）。
     */
    fun scan(from: ByteArray?, to: ByteArray?, readTs: ReadTs): Sequence<RowVersion> = sequence {
        val it = if (from == null) {
            map.entries.iterator()
        } else {
            map.tailMap(Bytes(InternalKey.seekKey(from)), true).entries.iterator()
        }
        var current: ByteArray? = null
        var decided = false
        while (it.hasNext()) {
            val e = it.next()
            val ik = e.key.data
            val uk = InternalKey.userKeyOf(ik)

            if (current == null || !uk.contentEquals(current!!)) {
                if (to != null && com.cloudea.yangbase.common.ByteArrayComparator.compare(uk, to) >= 0) return@sequence
                current = uk
                decided = false
            }
            if (decided) continue

            val ts = InternalKey.commitTsOf(ik)
            if (Visibility.visible(ts, readTs)) {
                decided = true
                val v = e.value.value
                if (v != null) yield(RowVersion(uk, v))
            }
            // 不可见则继续找更旧的版本（同一 key 的版本在迭代器中相邻）
        }
    }

    /** 冻结为不可变快照，供 flush 使用。 */
    fun snapshot(): List<Pair<ByteArray, ByteArray?>> {
        val out = ArrayList<Pair<ByteArray, ByteArray?>>(map.size)
        for (e in map.entries) out.add(e.key.data to e.value.value)
        return out
    }

    /** 遍历所有条目（含墓碑），按内部键序。flush 与 compaction 的输入。 */
    fun allEntries(): Sequence<Pair<ByteArray, ByteArray?>> = sequence {
        for (e in map.entries) yield(e.key.data to e.value.value)
    }

    /** 本 MemTable 中出现过的最大 commitTs。flush 时作为「行数水位」写进 manifest。 */
    fun maxCommitTs(): Long {
        var max = 0L
        for (e in map.keys) {
            val ts = InternalKey.commitTsOf(e.data).value
            if (ts > max) max = ts
        }
        return max
    }

    /** 收集本 MemTable 中出现过的所有 userKey 及其最新值，用于精确行数维护（§8.9.1）。 */
    fun latestValues(): Map<Bytes, ByteArray?> {
        val out = LinkedHashMap<Bytes, ByteArray?>()
        for (e in map.entries) {
            val uk = Bytes(InternalKey.userKeyOf(e.key.data))
            if (!out.containsKey(uk)) out[uk] = e.value.value
        }
        return out
    }
}
