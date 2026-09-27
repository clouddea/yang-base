package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.CommitTs
import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.PreconditionFailedException
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.Precondition
import com.cloudea.yangbase.storage.LeaseProvider
import com.cloudea.yangbase.storage.SingleProcessLease
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * **协调能力的唯一接缝**（设计文档 §4.7 的 Q2 决策）。
 *
 * ## 为什么它必须是一个接口，而且必须在第一天就存在
 *
 * 设计文档把 Q2 定为「双模式，部署时选择」：`MetaServiceClient`（常驻 MS）与
 * `ObjectStoreCoordination`（零常驻成本，用条件写在对象存储上做 CAS）**两个都要交付**。
 * 两种模式在成本与延迟上互有胜负，由部署者取舍：
 *
 * | 维度 | 常驻 MS | 纯对象存储 |
 * |---|---|---|
 * | 常驻实例成本 | 3 台小规格 | **0** |
 * | 提交延迟 | 基线 | +1–2 × RTT |
 * | 租约边界情况的测试难度 | 中 | **高** |
 * | 前提 | 无 | 后端必须支持条件写 |
 *
 * §4.7 因此定下一条硬性约束：**任何业务代码都不许出现具体实现类型**。
 * 否则日后无法回收——这正是"双模式"的代价，也是它的价值。所以业务代码（事务层、
 * SQL 层、存储层）只认这个接口，具体是哪一种由装配层（yb-server）决定。
 *
 * ## 四个职责，正好是四个子接口
 *
 * 设计文档 §4.1 把 MS 的职责列为四件事，它们都是"短期状态"或"派生状态"，
 * 权威副本仍在对象存储上（MS 完全可丢弃）。这里用**组合**而不是把四个签名
 * 再抄一遍：时间戳与租约早就各有成熟的接口实现（[Tso] / [LeaseProvider]），
 * 再包一层门面只会多出一份需要同步维护的签名表。
 *
 * | 职责 | 接口 | 丢失后的后果 |
 * |---|---|---|
 * | TSO 时间戳 | [tso] | 重启后从水位继续，跳号即可 |
 * | 分片写者租约 | [leases] | 全部租约过期，写者重新竞选 |
 * | 锁（行锁 / 表锁） | [locks] | 持锁事务失败回滚 |
 * | Catalog 版本 | [catalogVersion] | 从对象存储重建（`catalog/` 是真相源） |
 */
interface CoordinationService {

    /** 模式名，用于日志与 `SHOW VARIABLES`（`single` / `objectstore` / `ms`）。 */
    val kind: String

    val tso: Tso

    val leases: LeaseProvider

    val locks: LockTable

    /**
     * Catalog 的当前版本号。
     *
     * 它是 CN 侧 schema 缓存的失效判据（§4.5）：版本变了就重读 `catalog/`。
     * MS 模式下 MS 会主动广播失效，这里的轮询是**兜底**——广播丢包时缓存
     * 不会永远陈旧（那会让一个 CN 一直按旧 schema 读新数据，且不报错）。
     */
    suspend fun catalogVersion(): Long

    /**
     * DDL 提交后调用：推进 catalog 版本，让所有 CN 的 schema 缓存失效。
     *
     * 不推进的后果是静默的：别的 CN 会一直按旧 schema 解码新行，
     * 多出来的列被丢掉——不报错，只是数据少了一截。
     */
    suspend fun bumpCatalogVersion(): Long

    /** 释放连接与后台任务。 */
    suspend fun close() {}

    fun describe(): String = "CoordinationService($kind)"
}

/**
 * 时间戳区间的**权威来源**。
 *
 * ## 为什么是"预留一段区间"而不是"分配一个时间戳"
 *
 * 每次提交都去问一次权威方（MS 或对象存储 CAS）会让提交延迟里凭空多一个 RTT，
 * 而 §14.2 的请求预算不允许。所以权威方只做一件事：**把水位往前推一段**，
 * 区间内的号由 CN 自己发。权威方只需保证两件事——区间互不重叠（不发出重复时间戳）、
 * 水位单调（重启后不回退）。
 *
 * ## 为什么区间按**毫秒**算而不按个数算
 *
 * 按个数（"给你 2048 个号"）会让一段写入跨越任意长的时间，commit_ts 于是与真实时间
 * 脱钩——`AS OF TIMESTAMP` 会读出一个不属于那个时刻的数据库，而这是静默的
 * （见 [WindowTso] 的类注释）。所以 [span] 是 `毫秒 × 每毫秒逻辑位数`。
 */
interface TimestampSource {

    /**
     * 预留一段区间。
     *
     * @param atLeast 本次预留**必须**超过的下界（调用方已知的、必须被覆盖的时间戳）
     * @return `[base, through)`，其中 `base >= atLeast`，且与任何已发放的区间不重叠
     */
    suspend fun reserve(atLeast: Long, span: Long): TimestampWindow

    /** 当前水位（已发出区间的上界）。重启后用它避免时间戳回退。 */
    suspend fun watermark(): Long

    /** 诊断名（[WindowTso.describe] 里会带上它，排查时能一眼看出权威方是谁）。 */
    fun describe(): String
}

/** 一次预留的结果：`[base, through)`。 */
data class TimestampWindow(val base: Long, val through: Long)

/**
 * 把 [TimestampSource] 变成 [Tso]：**实时**分配 `commit_ts`，区间只用来兜底。
 *
 * 这段逻辑以前长在 `PersistentTso` 里，现在被两种协调模式共用
 * （§4.7 的两种模式共用同一套测试，也应当共用同一套分配逻辑——
 * 「实时 commit_ts」这类不变量只能有一份实现，两份实现必然漂移）。
 */
class WindowTso(
    private val source: TimestampSource,
    private val logicalBits: Int = 18,
    /**
     * 一次预取覆盖多少**毫秒**（不是多少个号，见 [TimestampSource]）。
     *
     * ## 这个值同时决定两件相反的事
     *
     * 1. **协调请求的频率** = 每写满一个窗口一次（写入期间约 1 次/window）。
     *    窗口越大越省（对象存储模式下每次是一笔条件写，要计费）。
     * 2. **跨 CN 的可见性延迟 ≤ 一个窗口**。原因是窗口是**独占**的：
     *    MSN 把水位往前推一整个窗口给 CN-A，那么 CN-B 下一个窗口的起点
     *    就是「A 的窗口右端」——比真实时间**超前最多一个窗口**。
     *    于是 B 刚提交的行，其 `commit_ts` 可能是「未来」的时间戳，
     *    A 在真实时间追上来之前读不到它。
     *
     * 曾经的默认值是 64 秒（"正常负载下几乎不会触发第二次预取"），
     * 那意味着**跨 CN 的写入可能 64 秒不可见**——一个用 JDBC 读写分离负载均衡
     * 就能撞上的语义事故。现在取 1 秒：可见性延迟 ≤1 秒，代价是写入期间
     * 每秒一次协调请求（MS 模式下是内存里的共识轮次；对象存储模式下一次条件写）。
     *
     * ⚠️ 这条延迟是**当前窗口模型的固有代价**，不是实现瑕疵。要彻底消除它需要
     * 换一种时间戳分配方式（每个提交向权威方要号，或把节点标识编进时间戳低位），
     * 那是一次独立的设计变更。
     */
    private val batchMillis: Long = 1_000,
) : Tso {

    private val mutex = Mutex()
    private var reservedThrough: Long = -1
    private var allocated: Long = -1

    /**
     * 本进程已经发出去的**最大读时间戳**。
     *
     * `commit_ts` 必须**严格大于**它：可见性是 `commit_ts <= read_ts`，
     * 若两者相等，一个在本事务提交**之前**就开始的读者会读到这次写入——
     * 那不是任何时刻的快照。
     */
    private var maxReadTs: Long = -1

    /**
     * 一次向权威方申请的窗口宽度（以逻辑时间戳计）。
     *
     * ⚠️ **下限是 2，不是 0**。窗口是**半开区间** `[base, through)`，
     * 而分配时要求严格递增（`next >= allocated + 1`），再加上读时间戳可能
     * 恰好等于 `base`（下界取了 `maxReadTs`），宽度为 1 的窗口会**一个值都放不下**——
     * 表现为写路径直接失败（`tsoBatchMillis = 0` 的降级场景就是这样）。
     * 宽度 2 是能同时容纳"读时间戳那一格"与"新提交"的最小值。
     */
    private val batchSpan: Long get() = maxOf(batchMillis shl logicalBits, 2L)

    /**
     * 这个窗口还能发出一个**更大的**时间戳吗？
     *
     * ⚠️ 这个判断只能有**一处定义**。曾经 [next] 用 `allocated >= reservedThrough - 1`
     * 决定"要不要预取"，而 [reserveIfNeededLocked] 用 `allocated < reservedThrough`
     * 决定"要不要跳过"——两者在 `allocated == reservedThrough - 1` 这个边界上**结论相反**：
     * next 认为窗口用尽并调用预取，预取却认为"还剩一个"而直接返回。于是
     * next 拿着一个发不出新值的窗口继续算，`next > ceiling` 的兜底把它压回
     * `ceiling`——**连续两次提交拿到同一个时间戳**。
     *
     * 什么时候会踩到：物理时钟越过窗口右端之后（空闲超过一个窗口的 CN 恢复写入）。
     * 重复的 `commit_ts` 会让 MVCC 失去全序，而症状只是"偶尔读到不该读到的版本"。
     */
    private fun hasRoom(nowShifted: Long): Boolean =
        allocated < reservedThrough - 1 && nowShifted < reservedThrough

    /**
     * 预取一段。`force = true` 时**跳过"还有余量吗"的判断**——调用方已经知道
     * 当前段放不下（例如读时间戳顶出了段界），此时"余量"是按 `now` 算的、
     * 会说还有余量，于是什么都不做。
     */
    private suspend fun reserveIfNeededLocked(force: Boolean = false) {
        if (!force && hasRoom(System.currentTimeMillis() shl logicalBits)) return
        val now = System.currentTimeMillis() shl logicalBits
        // 下界要同时满足：不落后于本进程已发出的任何时间戳（含读时间戳），
        // 并且**至少从现在开始**——最后一条保证新段里的时间戳与真实时间对齐
        val atLeast = maxOf(reservedThrough, allocated, maxReadTs, now)
        val w = source.reserve(atLeast, batchSpan)
        reservedThrough = w.through
        // `allocated` 记的是"已发出的最后一个"，而窗口的第一个可用值是 `base`。
        // 所以取 `base - 1`（这样下次 `next()` 正好发出 `base`）——但**不能让它回退**：
        // 当 `atLeast == allocated` 时 `base == allocated`，此时 `base - 1` 会把
        // 已发出的水位往回拉，那正是"重复时间戳"的来源
        allocated = maxOf(w.base - 1, allocated)
    }

    /** 分配 `commit_ts`。**实时**：高位就是此刻的毫秒（设计文档 §4.2）。 */
    override suspend fun next(): CommitTs = mutex.withLock {
        val now = System.currentTimeMillis() shl logicalBits
        // 段用尽，或物理时钟已经越过段界 → 再预取一段（两者合起来就是 [hasRoom] 的否定）。
        // 后者是必需的：段的跨度按毫秒给，长时间没有写入之后再提交就会越过它，
        // 不重新预取就会撞上别的 CN 的区间
        if (!hasRoom(now)) reserveIfNeededLocked()

        val ceiling = reservedThrough - 1
        var next = maxOf(allocated + 1, minOf(now, ceiling), maxReadTs + 1)
        // ★ 顶出段界时**必须重新预取，不能把它夹到 ceiling**。
        //
        // 曾经的写法是 `if (next > ceiling) next = ceiling`：当别的 CN 的读时间戳
        // （`maxReadTs`）超过本段上界时，`next` 被夹回 `ceiling`——而 `ceiling`
        // 很可能 **≤ 已经发出去的 `allocated`**。于是
        //   1. 同一个 `commit_ts` 发给多笔事务 → 写同一行时内部键完全相同 →
        //      MemTable 里后者覆盖前者，**一笔已提交的事务静默消失**；
        //   2. `allocated` 回退，MVCC 失去全序；
        //   3. 水位计数与真实行数长期漂移（§8.9 最怕的静默错误）。
        // 正确做法与"段用尽"是同一件事：重新向权威方要一段，让下界一步跨到
        // `maxReadTs` 之后。拿不到段就抛（写事务必须明确失败，见 §4.2）。
        if (next > ceiling) {
            reserveIfNeededLocked(force = true)
            next = maxOf(allocated + 1, minOf(now, reservedThrough - 1), maxReadTs + 1)
            check(next <= reservedThrough - 1) {
                "时间戳区间重新预取之后仍然放不下（next=$next ceiling=${reservedThrough - 1}）：" +
                    "这是一处实现缺陷，请连同这句话报告"
            }
        }
        allocated = next
        CommitTs(next)
    }

    /**
     * 当前水位（`start_ts`）。**不分配**，只报告。
     *
     * 取 `max(已发出的最后一个, 现在)`：既要盖住本进程已经提交的东西
     * （否则读不到自己刚写的数据），也要盖住别的 CN 在此期间提交的东西。
     */
    override suspend fun peek(): CommitTs = mutex.withLock {
        val now = System.currentTimeMillis() shl logicalBits
        if (now >= reservedThrough) {
            // ★ 读路径在权威方不可用时**不失败**（§4.2 的降级语义）：
            // 「MS 不可用时，CN 用剩余预取额度继续服务**只读事务**；写事务阻塞」。
            //
            // 这里的"剩余额度"就是上一次取到的时间戳水位：读会退化成一个**陈旧的快照**
            // （最多一个窗口的时长，默认 64 秒），但那是**安全**的——快照隔离下
            // 读到一个更早的时刻不会看到未提交或半份的数据。
            //
            // 写路径（[next]）则必须失败：拿不到新的区间就可能发出重复的时间戳，
            // 而重复的 commit_ts 会让 MVCC 失去全序，且完全静默。
            runCatching { reserveIfNeededLocked() }
        }
        val ts = maxOf(allocated, minOf(now, reservedThrough - 1))
        if (ts > maxReadTs) maxReadTs = ts
        CommitTs(ts)
    }

    override suspend fun recover() {
        val w = source.watermark()
        if (w <= 0) return
        // 从水位继续：水位是上一段区间的**上界**，重启后从它之后开始发号，
        // 不回退（时间戳只要求单调，不要求连续，跳过一段是可接受的）
        reservedThrough = maxOf(reservedThrough, w)
        allocated = maxOf(allocated, w)
        maxReadTs = maxOf(maxReadTs, w)
    }

    override fun describe() = "WindowTso(${source.describe()}, batch=${batchMillis}ms)"
}

/**
 * 用**对象存储的条件写**做授时（§4.7 的 `ObjectStoreCoordination` 那一种）。
 *
 * 一次预留 = 一次 CAS。`If-None-Match` / `If-Match` 让并发预留退化成串行：
 * 失败的一方重读水位再来一次。这是无 MS 部署下**唯一**需要跨进程互斥的地方，
 * 而它恰好只需要一次条件写。
 */
class ObjectStoreTimestampSource(
    private val store: ObjectStore,
    private val clusterId: String,
    /**
     * 后端没有 ETag CAS 时，是否允许走**弱化路径**（无前置条件 PUT + 写后回读校验）。
     *
     * 与 manifest 指针切换是**同一个开关**（`--i-accept-nonatomic-pointer-switch`）：
     * 它们背后是同一件事实——"这个后端没有条件写，而我接受在单写者部署下运行"。
     * 分开两个开关只会让人只打开其中一个，然后在别处撞上另一处。
     *
     * ⚠️ 为什么水位**也能**弱化：CAS 在这里的作用是让多个 CN 的预留互斥。
     * 单写者部署下这个互斥是空的（没有第二个预留者），而水位真正要保证的是
     * "重启后不发出比历史更小的号"——那只需要**持久化 + 只增不减**，
     * 不需要原子性。代价是：万一真的出现了第二个写者，它可能把水位**写低**，
     * 于是两边发出重复的时间戳。所以回读校验在这里不是可选项：
     * 它把"默默发重号"变成"当场报错"（见 [reserve]）。
     */
    private val allowNonAtomicCas: Boolean = false,
) : TimestampSource {

    private val watermarkKey = "meta/tso/$clusterId.json"

    /** 水位 CAS 的最大重试次数。超过就**拒绝写入**而不是冒险发出重复时间戳。 */
    private val maxAttempts = 16

    private val watermarkRe = Regex("""\"watermark\"\s*:\s*(\d+)""")

    override suspend fun reserve(atLeast: Long, span: Long): TimestampWindow {
        var attempts = 0
        while (true) {
            val meta = try {
                store.head(watermarkKey)
            } catch (_: ObjectNotFoundException) {
                null
            }
            val current = if (meta == null) 0L else readWatermark()   // 损坏时抛异常，见下
            val base = maxOf(current, atLeast)
            val next = base + span
            val payload = """{"watermark":$next}""".toByteArray()
            val weak = meta != null && !store.caps.conditionalUpdate
            // ★ 「没有 CAS」**不等于**「可以降级」：降级必须由部署者**显式同意**
            // （原则 P6：宁可拒绝服务，也不要悄悄失去互斥）。
            //
            // 这个判断必须在这里、而不是靠下面 `put` 抛异常来兜底：走弱化路径时
            // 我们压根不会发 `If-Match`，于是没有任何一层会因为"没开开关"而报错——
            // 表现就是**开关没开也照样降级**，一个静默的、无声的失去互斥。
            if (weak && !allowNonAtomicCas) {
                throw com.cloudea.yangbase.common.CapabilityNotSupportedException(
                    "时间戳水位需要 ETag CAS（If-Match），而这个后端没有：key=$watermarkKey。" +
                        "补救：在**单写者**部署下显式接受降级（--i-accept-nonatomic-pointer-switch），" +
                        "或改用 --coord-mode ms。",
                )
            }
            val ok = try {
                when {
                    // 「仅当不存在时创建」在不支持 CAS 的后端上依然可用（OSS 走 x-oss-forbid-overwrite）
                    meta == null -> store.put(watermarkKey, payload, Precondition.MustNotExist)
                    !weak -> store.put(watermarkKey, payload, Precondition.MustMatch(meta.etag))
                    // ── 弱化路径：与 ManifestStore 同一套路（无前置条件 PUT + 写后回读）
                    else -> store.put(watermarkKey, payload, null)
                }
                // 弱化路径下的回读校验：写进去的不是我们要写的值，
                // 说明在这两步之间**有另一个预留者**——那么它可能已经发出了
                // 与我们重叠的时间戳区间。此时唯一安全的动作是**拒绝发号**
                if (weak && !writtenValueIs(payload)) {
                    throw SqlException(
                        MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                        "时间戳水位在写入期间被另一个写者覆盖（后端没有 ETag CAS，" +
                            "已按 --i-accept-nonatomic-pointer-switch 降级）。" +
                            "这个开关的前提是**单写者部署**——请确认只有一个 CN 在写这个对象存储。",
                    )
                }
                true
            } catch (_: ConcurrentModificationException) {
                false   // 别人抢先了，重读再来
            } catch (_: PreconditionFailedException) {
                false
            } catch (e: com.cloudea.yangbase.common.CapabilityNotSupportedException) {
                // 后端没有 CAS 且没开开关：给出**补救动作**，而不是让它冒到某条 SQL 上
                // （真事：以前这条异常是在执行第一条语句时才炸出来的，报的是
                //  "key=meta/tso/default.json 不支持条件写"，与"我该怎么启动"无关）
                throw com.cloudea.yangbase.common.CapabilityNotSupportedException(
                    "时间戳水位需要 ETag CAS（If-Match），而这个后端没有：" +
                        "key=$watermarkKey。补救：在**单写者**部署下显式接受降级" +
                        "（--i-accept-nonatomic-pointer-switch），或改用 --coord-mode ms。",
                )
            }
            if (ok) return TimestampWindow(base, next)

            attempts++
            if (attempts >= maxAttempts) {
                // **宁可拒绝写入，也不要发出可能重复的时间戳**。
                // 重复的时间戳是静默的数据损坏；写入失败是可见的、可重试的
                throw SqlException(
                    MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                    "无法预留时间戳：水位对象连续 $attempts 次被并发修改。" +
                        "这通常意味着有大量 CN 在同时启动；请重试。",
                )
            }
            delay(1L shl minOf(attempts, 6))   // 退避，最多 64ms
        }
    }

    override suspend fun watermark(): Long = readWatermark()

    /** 回读校验：刚写下去的值是不是还在（弱化路径专用，见 [reserve]）。 */
    private suspend fun writtenValueIs(expected: ByteArray): Boolean =
        runCatching { store.get(watermarkKey) }.getOrNull()?.contentEquals(expected) == true

    /**
     * 读水位。**损坏时抛异常，绝不静默当成 0**。
     *
     * 静默重置是这个文件里最危险的一种"容错"：水位是我们唯一知道的、别的 CN
     * 已经发到哪儿了的信息。把它当作 0 之后再预留，会算出一个**低于别人已发放区间**
     * 的区间——两个 CN 于是发出重复的时间戳，而重复的时间戳会让 MVCC 失去全序。
     * 症状是偶尔读到不该读到的版本，且完全静默。
     *
     * 读取失败有两种，必须区别对待：对象**不存在**是正常的（全新集群，水位 0）；
     * 对象存在但内容是垃圾，说明写了半个对象或被外部改坏——此时唯一安全的动作是报错。
     */
    private suspend fun readWatermark(): Long {
        val bytes = try {
            store.get(watermarkKey)
        } catch (_: ObjectNotFoundException) {
            return 0
        }
        val text = String(bytes, Charsets.UTF_8)
        return watermarkRe.find(text)?.groupValues?.get(1)?.toLongOrNull()
            ?: throw com.cloudea.yangbase.common.CorruptDataException(
                "TSO 水位对象格式错误: $text。" +
                    "把它当成 0 会让本 CN 预留出一段**低于其他 CN 已发放区间**的时间戳，" +
                    "于是发出重复的 commit_ts——那会让 MVCC 失去全序，且完全静默。" +
                    "请人工确认水位后修复该对象：$watermarkKey",
            )
    }

    override fun describe() = "对象存储水位($watermarkKey)"
}

// ── 两种模式的实现 ────────────────────────────────────────────────

/**
 * Catalog 版本的**权威计数器**（§4.5 的失效判据）。
 *
 * 存在的理由是一个静默的多 CN 缺陷：CN 把 schema 缓存在内存里，而 DDL 只写对象存储。
 * 于是 A 建了表、B 的缓存里没有这张表——B 会报"表不存在"；更糟的是**加列**：
 * B 按旧 schema 解码新行，多出来的列被丢掉（不报错，只是数据少了一截）。
 * 一个单调递增的版本号让每个 CN 能在 5 秒内发现"世界变了"（§4.5 的轮询兜底）。
 */
interface CatalogVersionSource {
    suspend fun current(): Long

    /** DDL 提交后调用。返回新版本号。 */
    suspend fun bump(): Long

    fun describe(): String
}

/** 对象存储上的 catalog 版本：一个用 CAS 递增的小对象。 */
class ObjectStoreCatalogVersion(
    private val store: ObjectStore,
    private val clusterId: String,
) : CatalogVersionSource {

    private val key = "meta/catalog/$clusterId.json"
    private val versionRe = Regex("""\"version"\s*:\s*(\d+)""")

    override suspend fun current(): Long = try {
        val text = String(store.get(key), Charsets.UTF_8)
        versionRe.find(text)?.groupValues?.get(1)?.toLongOrNull() ?: 0L
    } catch (_: ObjectNotFoundException) {
        0L
    }

    override suspend fun bump(): Long {
        var attempts = 0
        while (true) {
            val meta = try {
                store.head(key)
            } catch (_: ObjectNotFoundException) {
                null
            }
            val next = (if (meta == null) 0L else current()) + 1
            val ok = try {
                store.put(
                    key, """{"version":$next}""".toByteArray(),
                    if (meta == null) Precondition.MustNotExist else Precondition.MustMatch(meta.etag),
                )
                true
            } catch (_: ConcurrentModificationException) {
                false
            } catch (_: PreconditionFailedException) {
                false
            }
            if (ok) return next

            attempts++
            // DDL 是低频操作，冲突几乎不会发生；连续失败说明有别的 DDL 在同时进行，
            // 重试即可。**不静默放弃**：版本号不推进等于让所有 CN 的缓存永久陈旧
            if (attempts >= 16) {
                throw SqlException(
                    MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                    "无法推进 catalog 版本号（连续 $attempts 次 CAS 失败）。" +
                        "不推进会让所有 CN 的 schema 缓存永久陈旧，因此这里报错而不是忽略。",
                )
            }
            delay(1L shl minOf(attempts, 6))
        }
    }

    override fun describe() = "对象存储 catalog 版本($key)"
}

/**
 * **零常驻成本**的协调实现：全部用对象存储的条件写做（§4.7 的
 * `ObjectStoreCoordination`）。
 *
 * 这也是单机部署之外的默认选择：本系统存在的理由就是"没有客户的时候不要付固定费用"，
 * 为一个低频数据库常驻三个 Raft 节点恰恰是它想消灭的成本。
 *
 * | 职责 | 实现 | 代价 |
 * |---|---|---|
 * | 时间戳 | [ObjectStoreTimestampSource] | 一次条件写 / 64 秒 |
 * | 租约 | [LeaseManager] | 一次条件写 / 租约周期 |
 * | 行锁 | [ObjectStoreLockTable] | **一次条件写 / 每次加锁** |
 * | Catalog 版本 | [ObjectStoreCatalogVersion] | 一次读 / 5 秒轮询 |
 *
 * ⚠️ 硬性前提：后端必须支持条件写。`conditionalCreate == false` 时这个实现
 * **不能工作**（CAS 会退化成覆盖写，两个 CN 会互相盖掉对方的水位/租约），
 * 所以装配层必须让它启动即失败，而不是静默降级（§3.3、§4.7 的实现约束 2）。
 */
class ObjectStoreCoordination(
    private val store: ObjectStore,
    val clusterId: String,
    holder: String,
    leaseTtlMillis: Long = 15_000,
    batchMillis: Long = 1_000,
    logicalBits: Int = 18,
) : CoordinationService {

    override val kind = "objectstore"

    override val tso: Tso = WindowTso(ObjectStoreTimestampSource(store, clusterId), logicalBits, batchMillis)

    override val leases: LeaseProvider = LeaseManager(store, holder, leaseTtlMillis)

    override val locks: LockTable = ObjectStoreLockTable(store)

    private val versions = ObjectStoreCatalogVersion(store, clusterId)

    override suspend fun catalogVersion(): Long = versions.current()

    override suspend fun bumpCatalogVersion(): Long = versions.bump()

    override fun describe() = "ObjectStoreCoordination(cluster=$clusterId, ${(tso as WindowTso).describe()})"
}

/**
 * **单进程**的协调实现：协调由**部署方式**保证，而不是由代码保证。
 *
 * 对应"一个 CN 在写"的部署。它不是"没有协调"——它是一份显式的假设：
 * 一旦有第二个 CN 打开同一个分片，fencing 就只剩 manifest 的 CAS 一道防线，
 * 两个写者会往同一个 WAL 前缀下写对象，而输的那一方写下的日志仍会被下一个恢复者重放。
 * 所以它**只允许在单 CN 部署下使用**，装配层要负责这件事。
 */
class SingleProcessCoordination(
    val nodeId: String,
    /**
     * 对象存储（可选）。
     *
     * 传了就用**持久化水位**的 TSO：单 CN 部署也会重启，而重启后从一个纯内存的
     * 时钟继续发号，意味着时间戳由"本机时钟"决定——时钟被回拨一次（NTP 校正、
     * 虚拟机迁移）就会发出比历史更小的号，而那是静默的 MVCC 事故。
     * 不传则退化成纯内存 TSO（测试与嵌入式用）。
     */
    store: com.cloudea.yangbase.objectstore.ObjectStore? = null,
    clusterId: String = "default",
    /** 一次预取覆盖多少毫秒（见 `WindowTso.batchMillis` 的说明）。 */
    batchMillis: Long = 1_000,
    /**
     * 后端没有 ETag CAS 时的显式降级开关（`--i-accept-nonatomic-pointer-switch`）。
     *
     * 单进程协调下真正会用到 CAS 的只剩**时间戳水位**一处（租约与行锁都是进程内的），
     * 但它足以让服务在 OSS 上连第一条语句都执行不了——所以这个开关必须一路传到这儿。
     */
    allowNonAtomicCas: Boolean = false,
) : CoordinationService {

    override val kind = "single"

    override val tso: Tso =
        if (store != null) {
            PersistentTso(store, clusterId, batchMillis = batchMillis, allowNonAtomicCas = allowNonAtomicCas)
        } else {
            MonotonicTso()
        }

    override val leases: LeaseProvider = SingleProcessLease

    /**
     * 行锁：**进程内的锁表**（不是空实现）。
     *
     * 这里踩过一次坑：最初写的是 `NoLocks`——理由是"单进程协调由部署保证"。
     * 那条理由对**租约**成立（同一分片只有一个写者），对**行锁完全不成立**：
     * 单 CN 部署下同样有两个连接并发地"读了改"，而快照隔离**不阻止**这种丢更新
     * （它是写偏斜的一种，§7.4）。用空实现的后果是 `SELECT ... FOR UPDATE`
     * 静默地什么也没锁——使用者以为读-改-写是安全的，而它不是。
     */
    override val locks: LockTable = LocalLockTable()

    private val version = java.util.concurrent.atomic.AtomicLong(0)

    override suspend fun catalogVersion(): Long = version.get()

    override suspend fun bumpCatalogVersion(): Long = version.incrementAndGet()

    override fun describe() = "SingleProcessCoordination(node=$nodeId)"
}
