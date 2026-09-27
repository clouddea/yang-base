package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.PreconditionFailedException
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.objectstore.ETag
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.Precondition
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * 自增主键的**分段分配**（设计文档 §11.4）。
 *
 * ## 为什么不能像 MySQL 那样集中分配
 *
 * MySQL 在服务端集中维护一个计数器，自增值因此连续。这套做法在两个前提下成立：
 * 单一进程持有计数器，以及**计数器可以随进程一起丢掉**（崩溃后从表里扫一遍最大值恢复）。
 * 本系统两个前提都不满足：
 *  - 计算节点可以有 N 个，各自内存在自己的进程里，**彼此看不见**；
 *  - CN 是「可随时销毁重建」的（原则 P5），计数器放在内存里就一定会丢。
 *
 * 后果不是「自增值会跳号」这么轻——两个 CN 各自扫表得到同一个最大值，
 * 然后各自发出同一个 id，两行哈希到同一个分片、主键相同，
 * **后写的那个把先写的覆盖掉**。没有报错、没有冲突、数据少一行。
 * 这正是项目在原则 P1 下最不能接受的那一类损坏。
 *
 * ## 分段：把「集中分配」摊成「一次 CAS 领一千个」
 *
 * ```
 *   对象存储里存一个水位 W  ──▶  CN 用 CAS 把它推到 W+1000
 *                                然后本地放心地发 W+1 … W+1000
 * ```
 *
 * 每个 id 段**只可能被一个 CN 领到**（CAS 保证），因此跨节点天然不冲突；
 * 段本身是内存里的一个计数器，热路径上就是一次 `AtomicLong` 自增——
 * **不产生任何对象存储请求**（P4）。这就是设计文档 §11.4 说的
 * 「CN 请求自增段 → 返回 [base, base+1000)」。
 *
 * 水位对象放在对象存储上而不是内存里，是 P1 的直接要求：它是权威状态，
 * 本地那份段只是**可丢弃的缓存**——CN 崩了，段里没用完的 id 就永久空洞，
 * 这没有任何问题（自增值本来就允许有洞）。
 *
 * ## 已经接受的行为差异（必须写进兼容性文档）
 *
 * 多个 CN 交替插入时自增值会跳跃；事务回滚会留下空洞。
 * 这与 MySQL 8.0 的 `innodb_autoinc_lock_mode=2`（交错模式）一致，
 * 官方文档明确说明该模式下自增值不保证连续。**但依赖 `AUTO_INCREMENT`
 * 连续性的应用会受影响**，这是设计文档 §11.3 明确要求写进用户文档的限制。
 *
 * ## 与 MS 的关系
 *
 * 这里是 §4.7 的「对象存储协调」模式，和 [LeaseManager] 是同一个套路：
 * 零新增基础设施。等元数据服务（§4）落地后，把 [raiseWatermark] 换成一次
 * MS 的 RPC 即可，**本类对外的行为不变**。
 */
class AutoIncAllocator(
    private val store: ObjectStore,
    /**
     * 一次领多少个 id。
     *
     * 偏大：领段的代价是一到两次对象存储往返（几十毫秒），摊到 1000 次插入上
     * 就可以忽略；偏小的收益是「空洞更少」，而空洞本身无害。
     * 所以这里明摆着往大的方向选。
     */
    private val segmentSize: Long = 1000L,
    /**
     * 后端没有 ETag CAS 时是否允许走弱化路径（无前置条件 PUT + 写后回读）。
     *
     * 与 manifest 指针切换、时间戳水位是**同一个开关**
     * （`--i-accept-nonatomic-pointer-switch`）——它们背后是同一件事实：
     * "这个后端没有条件写，而我接受在单写者部署下运行"。
     *
     * ⚠️ 这里比另外两处更危险一点：水位被写**低**会让两个写者发出**相同的 id**。
     * 所以弱化路径下回读校验失败时不是重试，而是**直接拒绝**——
     * 宁可让这条 INSERT 失败，也不要静默地发出可能重复的主键。
     */
    private val allowNonAtomicCas: Boolean = false,
    /**
     * 当前的协调模式（`single` / `objectstore` / `ms`）。**只用来把拒绝信息说清楚**：
     * "这个开关在这里为什么不适用"必须能回答，否则使用者只会反复检查自己有没有传开关。
     */
    private val coordinationMode: String = "single",
) {

    /**
     * 本地已经领到、还没发完的一段 id：`[next, limit)`。
     *
     * `next` 用原子量——同一个 CN 里有多个连接（多个协程）在并发插入，
     * 它们共享这一段，不能让两个协程拿到同一个 id。
     */
    private class Block(val next: AtomicLong, val limit: Long) {

        /** 取下一个 id；段用完返回 null。 */
        fun take(): Long? {
            while (true) {
                val v = next.get()
                if (v >= limit) return null
                if (next.compareAndSet(v, v + 1)) return v
            }
        }

        /** 把段内游标推过 [value]（显式插入了一个落在本段内的值）。 */
        fun skipPast(value: Long) {
            next.updateAndGet { cur -> if (value + 1 > cur) value + 1 else cur }
        }
    }

    private val blocks = ConcurrentHashMap<Long, Block>()
    private val locks = ConcurrentHashMap<Long, Mutex>()

    private fun lockOf(tableId: Long): Mutex = locks.computeIfAbsent(tableId) { Mutex() }

    /**
     * 取下一个自增 id。
     *
     * [seedIfAbsent] 只在**这张表从来没有分配过任何自增段**（水位对象不存在）时
     * 被调用一次，用来兜住「表里已经有数据、但水位对象还没有」的情况——
     * 升级自旧版本、或从备份/时间点恢复回来的表都会是这样。
     * 不去扫这一遍的话，恢复回来的表会从 1 重新发号，把已有行覆盖掉。
     */
    suspend fun nextId(tableId: Long, seedIfAbsent: suspend () -> Long): Long {
        while (true) {
            blocks[tableId]?.take()?.let { return it }
            lockOf(tableId).withLock {
                // 双重检查：等锁期间别人可能已经领了新段
                blocks[tableId]?.take()?.let { return it }
                val fresh = reserveSegment(tableId, seedIfAbsent)
                blocks[tableId] = fresh
                // 新领的段至少有一个 id（segmentSize ≥ 1），取不到是不可能的
                return fresh.take()
                    ?: throw SqlException(
                        MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                        "自增段分配异常：新领到的段为空（segmentSize=$segmentSize）",
                    )
            }
        }
    }

    /**
     * 记录一次**显式指定**的自增值。
     *
     * SQL 语义：`INSERT INTO t (id) VALUES (100000)` 之后，下一个自增值是 100001。
     * 不处理这一步的话，显式插入的大 id 会和之后自增发出的 id 撞上。
     *
     * 代价被压到最低：只有当这个值**落在本地已领段之外**时才去碰对象存储，
     * 而显式插入大 id 本来就罕见。
     */
    suspend fun observeExplicit(tableId: Long, value: Long, seedIfAbsent: suspend () -> Long) {
        // ① 先推本地游标：这个值可能正好落在我们已领的段里，
        //    不推的话后续自增会把它再发一次
        blocks[tableId]?.let { it.skipPast(value) }

        // ② 落在段内就已经安全了，不必再动水位
        blocks[tableId]?.let { if (value < it.limit) return }

        lockOf(tableId).withLock {
            blocks[tableId]?.let { if (value < it.limit) return }
            raiseWatermark(tableId, value, seedIfAbsent)
        }
    }

    /**
     * 把水位抬到「至少发到 [atLeast]」。
     *
     * **只抬不降**：水位是单调的。降低它会重新发出已经用过的 id。
     */
    private suspend fun raiseWatermark(tableId: Long, atLeast: Long, seedIfAbsent: suspend () -> Long) {
        val key = ObjectKeys.autoIncWatermark(TableId(tableId))
        var seed: Long? = null
        var attempts = 0
        while (true) {
            val cur = readWatermark(key)
            val base = if (cur == null) {
                // 水位对象不存在 = 这张表从未分配过。此时表里可能已经有数据
                // （升级、或从恢复点回来），必须从已有的最大值起步。
                seed ?: seedIfAbsent().also { seed = it }
            } else {
                cur.first
            }
            if (atLeast <= base) return          // 已经够高，不必写
            try {
                writeWatermark(key, atLeast, cur)
                return
            } catch (_: ConcurrentModificationException) {
                // 有人抢先了，重读
            } catch (_: PreconditionFailedException) {
                // 同上
            }
            attempts++
            if (attempts >= MAX_ATTEMPTS) {
                throw SqlException(
                    MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                    "无法推进表 $tableId 的自增水位：连续 $attempts 次与别人相撞",
                )
            }
            delay(1L shl minOf(attempts, 6))
        }
    }

    /**
     * 用 CAS 领一个新段。
     *
     * 返回值即「本地可用的 id 区间」。**无论 CAS 成功与否，对象里的水位都只会
     * 前进**——失败重试时重新读到的水位一定 ≥ 上一次读到的，
     * 所以领到的段之间永远不会重叠。
     */
    private suspend fun reserveSegment(tableId: Long, seedIfAbsent: suspend () -> Long): Block {
        val key = ObjectKeys.autoIncWatermark(TableId(tableId))
        // 扫表只在「水位对象不存在」时做，而且只做一次——
        // 它在重试循环里，不缓存的话每次 CAS 失败都会重新扫一遍全表
        var seed: Long? = null
        var attempts = 0
        while (true) {
            val cur = readWatermark(key)
            val base = if (cur == null) {
                seed ?: seedIfAbsent().also { seed = it }
            } else {
                cur.first
            }
            val end = base + segmentSize
            try {
                writeWatermark(key, end, cur)
                // 水位 = end（已发出的最大值），本段可发 (base, end]
                return Block(AtomicLong(base + 1), end + 1)
            } catch (_: ConcurrentModificationException) {
            } catch (_: PreconditionFailedException) {
            }
            attempts++
            if (attempts >= MAX_ATTEMPTS) {
                throw SqlException(
                    MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                    "无法为表 $tableId 领取自增段：连续 $attempts 次与别人相撞",
                )
            }
            delay(1L shl minOf(attempts, 6))
        }
    }

    /** 读水位。返回 `(水位, etag)`；对象不存在返回 null。 */
    private suspend fun readWatermark(key: String): Pair<Long, ETag>? {
        val meta = try {
            store.head(key)
        } catch (_: ObjectNotFoundException) {
            return null
        }
        val bytes = try {
            store.get(key)
        } catch (_: ObjectNotFoundException) {
            return null                 // head 与 get 之间被删了，当作不存在
        }
        val v = String(bytes, Charsets.UTF_8).trim().toLongOrNull() ?: 0L
        return v to meta.etag
    }

    private fun preconditionFor(cur: Pair<Long, ETag>?): Precondition? = when {
        // 「仅当不存在时创建」在不支持 CAS 的后端上依然可用（OSS 走 x-oss-forbid-overwrite）
        cur == null -> Precondition.MustNotExist
        store.caps.conditionalUpdate -> Precondition.MustMatch(cur.second)
        // ── 弱化路径：后端没有 ETag CAS，且调用方已显式同意（见 allowNonAtomicCas）──
        else -> null
    }

    /**
     * 写一次水位。**没有 CAS 的后端上必须显式同意才降级**，并且写后回读。
     *
     * 回读不是为了"更保险"，而是把「两个写者互相覆盖水位」这件**静默**的事
     * 变成一次明确的失败：真发生了的话，两边已经可能发出了重叠的 id 段。
     */
    private suspend fun writeWatermark(key: String, value: Long, cur: Pair<Long, ETag>?) {
        val bytes = value.toString().toByteArray()
        val weak = cur != null && !store.caps.conditionalUpdate
        if (weak && !allowNonAtomicCas) {
            // 拒绝的理由分两种，必须分开说：**没开开关**与**开了也不能开**
            // （多 CN 下这个降级根本不成立——见 DatabaseEngine 里 autoIncrement 的注释）
            val why = if (coordinationMode == "single") {
                "补救：在**单写者**部署下显式接受降级（--i-accept-nonatomic-pointer-switch）。"
            } else {
                "本部署是 --coord-mode $coordinationMode（可能有多个 CN），而自增领段的互斥" +
                    "**完全依赖**这次 CAS，降级会让两个 CN 领到同一段、发出重复的主键——" +
                    "所以这里不能降级。可选：改用有 ETag CAS 的后端、改用 --coord-mode single（单 CN）、" +
                    "或不用 AUTO_INCREMENT（显式给主键）。"
            }
            throw com.cloudea.yangbase.common.CapabilityNotSupportedException(
                "自增水位需要 ETag CAS（If-Match），而这个后端没有：key=$key。$why",
            )
        }
        store.put(key, bytes, preconditionFor(cur))
        if (weak) {
            val back = runCatching { store.get(key) }.getOrNull()
            if (back == null || !back.contentEquals(bytes)) {
                throw SqlException(
                    MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                    "自增水位在写入期间被另一个写者覆盖（后端没有 ETag CAS，已按 " +
                        "--i-accept-nonatomic-pointer-switch 降级）：key=$key。" +
                        "该开关的前提是**单写者部署**——两个写者可能已经发出相同的 id，" +
                        "所以这条写入被拒绝而不是重试。",
                )
            }
        }
    }

    /**
     * 清掉本地的段缓存。
     *
     * 恢复（RESTORE / PITR）之后必须调用：数据被指回一个更早的快照，
     * 但**水位对象是单调的、不会跟着回退**，所以本地缓存的段里可能还留着
     * "属于未来"的 id。丢掉它只会浪费几个号，继续用则可能和恢复回来的行撞上。
     */
    fun invalidate(tableId: Long? = null) {
        if (tableId == null) blocks.clear() else blocks.remove(tableId)
    }

    /**
     * 把这张表的水位**归零**。只给 `TRUNCATE TABLE` 用。
     *
     * MySQL 的 TRUNCATE 会把 AUTO_INCREMENT 重置回 1，这是被广泛依赖的行为
     * （「清空重来，id 从 1 开始」）。这里照做。
     *
     * 归零**不会**造成重号：领段一律走 `MustNotExist` 的 CAS，所以
     * 「水位不存在」这个状态只会被一个节点看到并据以建段，另一个节点
     * 撞上之后重读就会拿到刚建好的水位，接着往后领。加上 `nextId` 的 seed
     * 兜底（水位不存在时扫表取最大值），已经存在的行也覆盖不到。
     */
    suspend fun reset(tableId: Long) {
        blocks.remove(tableId)
        runCatching { store.delete(ObjectKeys.autoIncWatermark(TableId(tableId))) }
    }

    /** 本节点缓存的段还剩多少（诊断用）。 */
    fun remaining(tableId: Long): Long = blocks[tableId]?.let { b ->
        val n = b.limit - b.next.get()
        if (n > 0) n else 0
    } ?: 0L

    private companion object {
        const val MAX_ATTEMPTS = 16
    }
}
