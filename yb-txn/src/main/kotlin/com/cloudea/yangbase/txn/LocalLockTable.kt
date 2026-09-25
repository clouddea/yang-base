package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import kotlinx.coroutines.delay
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * **进程内**的锁表（设计文档 §4.4）：一张内存表 + 等待图死锁检测。
 *
 * 它有两个使用者，而两者需要的东西完全一样，所以只有一份实现：
 *
 * | 使用者 | 场景 | 为什么进程内的表就够 |
 * |---|---|---|
 * | [SingleProcessCoordination] | 单 CN 部署 | 所有连接都在**同一个进程**里，进程内的互斥就是全部互斥 |
 * | MS 的领导者（`yb-ms` 的 `RaftNode`） | 3 节点 MS | 锁由领导者裁定，它自己就是唯一的权威 |
 *
 * ## 为什么不进 Raft 日志
 *
 * ## 为什么不进 Raft 日志
 *
 * §4.4 把锁的范围收窄到三种场景，并显式决定"锁表在 MS 内存中实现，**不持久化**"。
 * 这个取舍的代价被明确接受了：MS 重启意味着所有持锁事务失败（事务会重试），
 * 换来的好处是**每一次 `FOR UPDATE` 不产生一次共识轮次**——否则加锁会成为
 * 整个系统里最贵的一个动作，而它的语义（"别人别改这一行"）在单写者模型下
 * 大部分已经被消解掉了（同一个分片的写本来就串行）。
 *
 * ## 领导者更替时锁会丢——这一条必须说清楚
 *
 * 换了领导者，新领导者的锁表是空的，而旧领导者手里的锁没了。于是可能出现
 * "两个事务都以为自己持有同一行"的窗口。三重缓解：
 *
 * 1. 锁的 [LockHandle.token] 里含**任期**（见 [MetaServiceClient]），
 *    客户端发现任期变了就认为自己的锁**已经失效**，主动放弃事务（而不是继续写）；
 * 2. 真正会因此损坏的数据面操作都还有 2PC 的 primary record 兜底（§7.3）；
 * 3. TTL 兜底：旧领导者的锁最多活 TTL 就全部消失。
 *
 * 换句话说：**锁是"减少冲突"的手段，不是正确性的最后一道防线**。正确性由
 * 单写者 + 2PC + epoch fencing 保证，锁只负责让并发不那么难受。
 */
class LocalLockTable : LockTable {

    private class Holder(val owner: String, val token: String, var expiresAt: Long)

    private class Entry(val key: LockKey) {
        var mode: LockMode = LockMode.EXCLUSIVE
        val holders = LinkedHashMap<String, Holder>()
    }

    private val entries = ConcurrentHashMap<LockKey, Entry>()

    /** token → (键, 持有者)，释放时按 token 找。 */
    private val byToken = ConcurrentHashMap<String, Pair<LockKey, Holder>>()

    /**
     * 等待图：`等待者 → 它正等着的持有者`。
     *
     * 死锁检测只需要这一张表——锁与等待关系都在一个进程里，所以"谁在等谁"
     * 是**全局已知**的，不需要额外的探测机制（InnoDB 也是这么做的，只是它的
     * 等待图要跨会话收集）。
     */
    private val waits = ConcurrentHashMap<String, MutableSet<String>>()

    private val tokenCounter = AtomicLong(0)

    /** 被死锁检测牺牲掉的事务（诊断用）。 */
    val deadlockVictims = AtomicLong(0)

    override suspend fun lock(key: LockKey, mode: LockMode, owner: String, ttlMillis: Long, waitMillis: Long): LockHandle {
        val deadline = System.currentTimeMillis() + waitMillis
        val ttl = if (ttlMillis > 0) ttlMillis else 30_000
        val token = "$owner#${tokenCounter.incrementAndGet()}"
        var waited = false

        while (true) {
            val now = System.currentTimeMillis()
            // ⚠️ 不变量：**表里只存在至少有一个持有者的条目**。
            //
            // 这个不变量是踩过一次坑之后立的：早先的写法是 `computeIfAbsent` 先造一个
            // 空条目放进表里，然后 `pruneExpired` 一看"空的"就把它删掉——接着持有者被加进
            // 那个**已经不在表里**的对象。结果是 `lock()` 正常返回、调用方以为拿到了锁，
            // 而表里什么都没有：**完全不互斥**，且不报任何错。
            // 现在的顺序是"先在本地把持有者加上，再发布进表"，空条目永远不会出现在表里
            var entry = entries[key]
            val created = entry == null
            if (entry == null) entry = Entry(key)

            synchronized(entry) {
                if (!created) {
                    pruneExpired(entry, now)
                    if (entry.holders.isEmpty()) {
                        // 持有者全过期了：把条目摘掉，重新走一轮（下一轮会造一个新的）
                        entries.remove(key, entry)
                        entry = null
                    }
                }
                val e = entry
                if (e != null) {
                    val live = e.holders.values.toList()
                    // 可重入：同一个事务再锁一次**同一行**（同模式）是幂等的。
                    // MySQL 里也是这样——重复加锁不会把自己挡住。
                    // 少了这一条，`FOR UPDATE` 两次读同一行（或同一事务的多条语句命中同一行）
                    // 会把自己算成"等待自己"，然后被死锁检测判成环
                    val mine = live.firstOrNull { it.owner == owner && e.mode == mode }
                    if (mine != null) {
                        byToken[mine.token] = key to mine
                        return handleOf(key, mode, mine.token, mine.expiresAt)
                    }
                    // 锁升级（共享 → 排他）在 MySQL 里也要等，而"等自己"是等不到的。
                    // 明确报错而不是挂在那里直到超时——后者看起来像卡死
                    if (live.any { it.owner == owner } && mode == LockMode.EXCLUSIVE) {
                        throw SqlException(
                            MySqlErrors.ER_LOCK_WAIT_TIMEOUT, "HY000",
                            "同一事务内不能把共享锁升级成排他锁（key=$key）。" +
                                "请在事务里直接用 `FOR UPDATE`（排他锁），或者避免同一行先读后改",
                        )
                    }
                    // 能不能加入，只看"当前这一轮是不是共享的"：
                    // 独占锁要求没有任何活持有者；共享锁要求当前这一轮也是共享的
                    val ok = when (mode) {
                        LockMode.EXCLUSIVE -> live.isEmpty()
                        LockMode.SHARED -> live.isEmpty() || e.mode == LockMode.SHARED
                    }
                    if (ok) {
                        val h = Holder(owner, token, now + ttl)
                        e.holders[token] = h
                        if (live.isEmpty()) e.mode = mode
                        byToken[token] = key to h
                        waits.remove(owner)
                        // ★ 持有者加好之后才发布：这样"表里的条目一定有持有者"始终成立
                        if (created) entries[key] = e
                        return handleOf(key, mode, token, h.expiresAt)
                    }
                    // 记下"我在等谁"，供死锁检测
                    if (mode == LockMode.EXCLUSIVE || e.mode == LockMode.EXCLUSIVE) {
                        waits.getOrPut(owner) { ConcurrentHashMap.newKeySet() }
                            .addAll(live.map { it.owner }.filter { it != owner })
                    }
                }
            }

            if (waited && hasCycle(owner)) {
                // 消息里的等待图要在**移除自己之前**取：移除之后打出来是空的，
                // 而"等待图是空的却报了死锁"会让排查的人以为检测器坏了
                val graph = describeWaits()
                waits.remove(owner)
                deadlockVictims.incrementAndGet()
                throw SqlException(
                    MySqlErrors.ER_DEADLOCK, "40001",
                    "Deadlock found when trying to get lock; try restarting transaction。" +
                        "等待图：$graph。本系统回滚**发起者**：MySQL 选择「工作量最小的事务」，" +
                        "那需要跟踪每个事务改了多少行，而回滚发起者同样能打破环、且判定是确定的",
                )
            }
            waited = true

            if (now >= deadline) {
                waits.remove(owner)
                throw SqlException(
                    MySqlErrors.ER_LOCK_WAIT_TIMEOUT, "HY000",
                    "Lock wait timeout exceeded（等待 ${waitMillis}ms）；key=$key 当前持有者：" +
                        "${holdersOf(key).joinToString(",").ifEmpty { "无" }}",
                )
            }
            delay(10)
        }
    }

    override suspend fun unlock(handle: LockHandle) = unlock(handle.token)

    /** 进程内的锁永远"还在"：没有别人能把它拿走（除非 TTL 过期）。 */
    override suspend fun stillHeld(handles: Collection<LockHandle>): Boolean =
        handles.all { h -> byToken[h.token]?.second?.expiresAt?.let { it > System.currentTimeMillis() } == true }

    fun unlock(token: String) {
        val (key, holder) = byToken.remove(token) ?: return
        entries[key]?.let { entry ->
            synchronized(entry) {
                entry.holders.remove(token)
                // 空了就摘掉（两参数版本：只在表里还是这一个对象时才删，
                // 避免把另一轮刚发布进去的新条目误删）
                if (entry.holders.isEmpty()) entries.remove(key, entry)
            }
        }
        waits.remove(holder.owner)
    }

    /** 一次事务结束时把它的等待边清掉（它不再等了）。 */
    fun forgetWaiter(owner: String) {
        waits.remove(owner)
    }

    /** 当前持有者（诊断与错误信息用）。 */
    fun holdersOf(key: LockKey): List<String> {
        val e = entries[key] ?: return emptyList()
        synchronized(e) { return e.holders.values.map { it.owner } }
    }

    override suspend fun describeLock(key: LockKey): String? =
        holdersOf(key).joinToString(",").ifEmpty { null }

    /** 诊断：当前所有活锁。 */
    fun snapshot(): Map<LockKey, List<String>> =
        entries.entries.associate { (k, e) -> k to synchronized(e) { e.holders.values.map { it.owner } } }

    /**
     * 清掉过期的持有者。
     *
     * **不在这里删条目**（见 [lock] 里的不变量）：摘除空条目由调用方在确认
     * "这个条目就是表里那一个"之后做，否则会把别人刚发布的条目一起删掉。
     */
    private fun pruneExpired(entry: Entry, now: Long) {
        val dead = entry.holders.filterValues { it.expiresAt <= now }.keys
        for (t in dead) {
            entry.holders.remove(t)
            byToken.remove(t)
        }
    }

    /**
     * 等待图里有没有环（从 [start] 出发能否回到自己）。
     *
     * 图很小（只有正在等待的事务），深度优先即可；不需要更聪明的算法。
     */
    private fun hasCycle(start: String): Boolean {
        val seen = HashSet<String>()
        fun dfs(node: String): Boolean {
            if (node == start && seen.isNotEmpty()) return true
            if (!seen.add(node)) return false
            for (next in waits[node] ?: emptySet()) {
                if (dfs(next)) return true
            }
            return false
        }
        return dfs(start)
    }

    private fun describeWaits(): String =
        waits.entries.joinToString("; ") { (k, v) -> "$k 等 [${v.joinToString(",")}]" }

    private fun handleOf(key: LockKey, mode: LockMode, token: String, expiresAt: Long) = object : LockHandle {
        override val key = key
        override val mode = mode
        override val token = token
        override val expiresAtMillis = expiresAt
    }
}
