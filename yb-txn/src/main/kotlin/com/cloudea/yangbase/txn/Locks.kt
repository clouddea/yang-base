package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.PreconditionFailedException
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.Precondition
import kotlinx.coroutines.delay
import java.util.Base64

/**
 * 锁的作用范围（设计文档 §4.4）。
 *
 * ## 锁只处理三种情况——三种之外都**不需要锁**
 *
 * | 场景 | 机制 |
 * |---|---|
 * | 同分片内的写写冲突 | **不需要锁**：单写者队列天然串行（这是单写者模型的核心收益） |
 * | 跨分片事务的提交冲突 | **不需要锁**：2PC 的 primary record 判定（§7.3） |
 * | 跨分片写同一行 | **需要**：这里 |
 * | DDL 与 DML 的冲突 | **需要**：表级意向锁（[LockKey.tableId] 上，`pk` 为空） |
 * | `SELECT ... FOR UPDATE` | **需要**：行锁，语义与 MySQL 对齐 |
 *
 * 把锁的范围收窄是刻意的：锁是**唯一**需要跨节点共识的在线操作，它的每一次使用都在
 * 提交路径上加一个 RTT。能用单写者与 2PC 消解掉的冲突，就不该走锁。
 */
enum class LockMode { SHARED, EXCLUSIVE }

/**
 * 锁的键。
 *
 * - [pkHash] 非空 → **行锁**（`表 + 主键` 的哈希，§4.4）
 * - [pkHash] 为空 → **表级锁**（DDL 与 DML 的意向锁）
 *
 * 用哈希而不是主键原文：锁表是内存里的哈希表，键要短且定长；
 * 哈希碰撞意味着两行共用一把锁——那只会让并发略降，**不会**让数据出错
 * （多一把锁的代价是等待，少一把锁的代价是数据错乱，所以这里选保守的一侧）。
 */
data class LockKey(val tableId: Long, val pkHash: Long? = null) {
    init {
        require(tableId > 0 || tableId == BINDING_TABLE) {
            "tableId 必须为正（或 $BINDING_TABLE，表示跨表绑定的锁）"
        }
    }

    /** 表级锁用的键。 */
    companion object {
        const val BINDING_TABLE = -1L

        fun table(tableId: Long) = LockKey(tableId, null)

        /**
         * 行锁的键：**主键的字节**取哈希。
         *
         * 直接哈希原始字节而不是"拼成字符串再哈希"：主键可能是二进制
         * （`VARBINARY` / 复合键里含 `BLOB`），拼字符串会引入编码歧义——
         * 两行不同的主键可能拼出同一个字符串，于是它们**共用一把锁**（并发降低），
         * 或者更糟：不同的字节被同一个字符串表示（这里选前者）。
         */
        fun row(tableId: Long, pkBytes: ByteArray): LockKey {
            var h = -0x340d631b7bdddcdbL      // FNV-1a 64 位偏移基
            for (b in pkBytes) {
                h = h xor (b.toLong() and 0xFF)
                h *= 0x100000001b3L
            }
            return LockKey(tableId, h)
        }
    }
}

/** 一把已持有的锁。释放与续期都要带上 [token]（防止释放别人的锁）。 */
interface LockHandle {
    val key: LockKey
    val mode: LockMode

    /** 本次持有的唯一凭据。 */
    val token: String

    val expiresAtMillis: Long

    fun isExpired(now: Long = System.currentTimeMillis()): Boolean = now >= expiresAtMillis
}

/**
 * 锁表：行锁与表级锁（设计文档 §4.4）。
 *
 * ## 语义与 MySQL 对齐的两点
 *
 * 1. **等待超时**：`innodb_lock_wait_timeout` 的语义——等到超时就报 1205，
 *    而不是无限等待。默认 50 秒。
 * 2. **死锁**：报 1213 并**回滚当前事务**。MS 模式下用等待图环检测（§4.4）；
 *    对象存储模式没有中心节点可放等待图，只能靠超时——这是一个**已登记的差异**，
 *    见 [ObjectStoreLockTable] 的说明。
 */
interface LockTable {

    /**
     * 加锁。语义与 `SELECT ... FOR UPDATE` / `LOCK IN SHARE MODE` 一致。
     *
     * @param ttlMillis 锁的存活时间。它防的是**持有者崩溃**导致的永久占用：
     *        正常路径下事务提交/回滚会显式释放，超时只是兜底
     * @param waitMillis 最多等多久；0 表示只试一次。超时抛 1205
     * @return 拿到的锁；调用方负责在事务结束时 [unlock]
     */
    suspend fun lock(
        key: LockKey,
        mode: LockMode,
        owner: String,
        ttlMillis: Long = 30_000,
        waitMillis: Long = 50_000,
    ): LockHandle

    /** 释放。释放别人的锁必须失败（否则锁就没有意义了）。 */
    suspend fun unlock(handle: LockHandle)

    /**
     * 这些锁**现在还在吗**？（提交前的自检）
     *
     * 为什么必须有这个动作：MS 模式下锁表是领导者本地状态（§4.4 不持久化），
     * 领导者一换，新领导者的锁表是空的——持锁事务会**以为自己还锁着**。
     * 于是两个事务都基于"我锁住了这一行"做决定，其中一个的更新被另一个覆盖，
     * 而且不报错。所以在提交前问一次"锁还在吗"，不在就明确报 1213 让事务重试。
     *
     * @return true 表示全部仍然持有；false 表示至少有一把已经失效
     */
    suspend fun stillHeld(handles: Collection<LockHandle>): Boolean

    /** 当前是否有活锁（诊断用，不参与判定）。 */
    suspend fun describeLock(key: LockKey): String?
}

/**
 * 把锁放在对象存储上（`ObjectStoreCoordination` 模式的实现）。
 *
 * ## 一个对象一把锁，TTL 写在对象里
 *
 * ```
 *   meta/lock/{tableId}/{pkHash}.json
 *   { "mode": "SHARED", "holders": [ { "owner": "cn-1#tx7", "token": "...", "expiresAt": 1690000000000 } ] }
 * ```
 *
 * 加锁 = 一次条件写：不存在就 `If-None-Match` 创建；存在就 `If-Match` 改（把过期的
 * 持有者清掉再加入自己）。冲突时轮询重试到 [waitMillis] 超时。
 *
 * ## 与 MS 模式的**两处已知差异**（都必须记住）
 *
 * 1. **没有死锁检测**：等待图需要一个中心节点来看全貌，而这里没有。所以并发互相
 *    等待的两个事务只能等到超时（1205），而不是立刻报 1213。MS 模式有检测。
 * 2. **每次加锁是一个 RTT**（几十毫秒）。所以对象存储模式适合"很少用 `FOR UPDATE`"
 *    的负载；高频行锁应当部署 MS。
 *
 * 这两条不是"没实现"，是这种部署模式的物理约束，写在这里以免被当成缺陷去查。
 */
class ObjectStoreLockTable(
    private val store: ObjectStore,
    /** 默认锁 TTL：持有者崩溃后多久自动失效。 */
    private val defaultTtlMillis: Long = 30_000,
) : LockTable {

    private val pollMillis = 20L

    private fun keyOf(key: LockKey) = "meta/lock/${key.tableId}/${key.pkHash ?: "table"}.json"

    override suspend fun lock(
        key: LockKey,
        mode: LockMode,
        owner: String,
        ttlMillis: Long,
        waitMillis: Long,
    ): LockHandle {
        val ttl = if (ttlMillis > 0) ttlMillis else defaultTtlMillis
        val deadline = System.currentTimeMillis() + waitMillis
        val token = randomToken()
        val objKey = keyOf(key)
        var attempts = 0

        while (true) {
            val now = System.currentTimeMillis()
            val meta = try {
                store.head(objKey)
            } catch (_: ObjectNotFoundException) {
                null
            }
            if (meta == null) {
                // 没人持有过 → 用 If-None-Match 抢创建
                val payload = encode(mode, listOf(Holder(owner, token, now + ttl)))
                if (tryPut(objKey, payload, Precondition.MustNotExist)) {
                    return handle(key, mode, token, now + ttl)
                }
            } else {
                val existing = readHolders(objKey)
                val live = existing.filter { it.expiresAt > now }
                // 兼容性：独占锁要求没有任何活持有者；共享锁要求现有模式也是共享
                val compatible = live.isEmpty() ||
                    (mode == LockMode.SHARED && live.all { it.mode == LockMode.SHARED })
                if (compatible) {
                    val next = live + Holder(owner, token, now + ttl)
                    val newMode = if (live.isEmpty() && mode == LockMode.EXCLUSIVE) LockMode.EXCLUSIVE else LockMode.SHARED
                    if (tryPut(objKey, encode(newMode, next), Precondition.MustMatch(meta.etag))) {
                        return handle(key, mode, token, now + ttl)
                    }
                }
            }

            attempts++
            if (now >= deadline) {
                throw SqlException(
                    MySqlErrors.ER_LOCK_WAIT_TIMEOUT, "HY000",
                    "Lock wait timeout exceeded（等待 ${waitMillis}ms）；" +
                        "key=$key 当前持有者：${describeLock(key) ?: "未知"}。" +
                        "对象存储模式没有死锁检测（见 ObjectStoreLockTable 的说明），" +
                        "互相等待只能等到超时",
                )
            }
            delay(minOf(pollMillis + attempts, 200L))
        }
    }

    override suspend fun unlock(handle: LockHandle) {
        val objKey = keyOf(handle.key)
        repeat(8) {
            val now = System.currentTimeMillis()
            val meta = try {
                store.head(objKey)
            } catch (_: ObjectNotFoundException) {
                return
            }
            val holders = readHolders(objKey)
            if (holders.none { it.token == handle.token }) {
                // 我们已经不在里面了（TTL 到期被别人清掉，或重复释放）——不报错
                return
            }
            val rest = holders.filter { it.token != handle.token }
            // 释放**不删除对象**，而是写回"空持有者列表"：条件写只有 put，
            // 删除没有 etag 校验，可能在"检查—删除"之间把刚加入的共享持有者一起删掉。
            // 代价是留下一个小对象，由 GC 回收（它是可重建的派生数据）
            if (tryPut(objKey, encode(if (rest.isEmpty()) LockMode.EXCLUSIVE else LockMode.SHARED, rest), Precondition.MustMatch(meta.etag))) {
                return
            }
            delay(pollMillis)
        }
        // 释放失败不抛异常：锁会随 TTL 过期，而抛异常会让"提交已经成功的事务"报错
        LOCK_RELEASE_FAILURES.inc()
    }

    override suspend fun stillHeld(handles: Collection<LockHandle>): Boolean {
        val now = System.currentTimeMillis()
        for (h in handles) {
            val holders = readHolders(keyOf(h.key))
            val mine = holders.firstOrNull { it.token == h.token } ?: return false
            if (mine.isExpired(now)) return false
        }
        return true
    }

    override suspend fun describeLock(key: LockKey): String? {
        val holders = readHolders(keyOf(key))
        if (holders.isEmpty()) return null
        return holders.filter { !it.isExpired() }.joinToString(",") { "${it.owner}(${it.mode})" }
    }

    private fun handle(key: LockKey, mode: LockMode, token: String, expiresAt: Long) =
        object : LockHandle {
            override val key = key
            override val mode = mode
            override val token = token
            override val expiresAtMillis = expiresAt
        }

    private suspend fun tryPut(key: String, payload: ByteArray, precondition: Precondition): Boolean = try {
        store.put(key, payload, precondition)
        true
    } catch (_: ConcurrentModificationException) {
        false
    } catch (_: PreconditionFailedException) {
        false
    }

    private suspend fun readHolders(key: String): List<Holder> = try {
        decode(String(store.get(key), Charsets.UTF_8))
    } catch (_: ObjectNotFoundException) {
        emptyList()
    }

    private data class Holder(
        val owner: String,
        val token: String,
        val expiresAt: Long,
        /**
         * 该持有者是以哪种模式加入的。
         *
         * 必须**逐持有者**记录，而不是只记整个对象的模式：共享锁是"多个持有者"，而
         * 谁能加入取决于"现有持有者是不是都以共享模式加入"。只看对象级模式的话，
         * 一个先拿了独占锁、随后降级（或对象模式字段被后来的共享写覆盖）的场景
         * 会让独占持有者与新来的共享持有者**同时持有**——锁就静默失效了。
         */
        val mode: LockMode = LockMode.SHARED,
    ) {
        fun isExpired(now: Long = System.currentTimeMillis()) = now >= expiresAt
    }

    private fun encode(mode: LockMode, holders: List<Holder>): ByteArray {
        val list = holders.joinToString(",") {
            """{"owner":"${escape(it.owner)}","token":"${escape(it.token)}","expiresAt":${it.expiresAt},"mode":"${it.mode}"}"""
        }
        return """{"mode":"$mode","holders":[$list]}""".toByteArray()
    }

    private fun decode(text: String): List<Holder> {
        val holders = Regex("""\{"owner":"([^"]*)","token":"([^"]*)","expiresAt":(\d+),"mode":"(\w+)"\}""")
        return holders.findAll(text).map {
            Holder(
                owner = unescape(it.groupValues[1]),
                token = unescape(it.groupValues[2]),
                expiresAt = it.groupValues[3].toLong(),
                mode = runCatching { LockMode.valueOf(it.groupValues[4]) }.getOrDefault(LockMode.SHARED),
            )
        }.toList()
    }

    private fun escape(s: String) = s.replace("\\", "\\\\").replace("\"", "\\\"")
    private fun unescape(s: String) = s.replace("\\\"", "\"").replace("\\\\", "\\")

    private fun randomToken(): String {
        val bytes = ByteArray(9)
        java.security.SecureRandom().nextBytes(bytes)
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
    }

    private companion object {
        /** 释放失败只计数不抛异常（锁会随 TTL 过期），但**必须可见**。 */
        val LOCK_RELEASE_FAILURES = Metrics.registry.counter("yb_lock_release_failures_total")

        init {
            Metrics.registry.help(
                "yb_lock_release_failures_total",
                "锁释放未成功的次数（锁会随 TTL 过期；持续增长说明对象存储在被争用）",
            )
        }
    }
}
