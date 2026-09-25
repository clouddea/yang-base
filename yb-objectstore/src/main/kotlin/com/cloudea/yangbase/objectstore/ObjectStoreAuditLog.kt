package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.audit.AuditEvent
import com.cloudea.yangbase.common.audit.AuditSink
import com.cloudea.yangbase.common.metrics.Metrics
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json

/**
 * 把审计记录写进对象存储的 `ops/audit/{date}.jsonl`（JSONL，一行一条）。
 *
 * ## 三条硬性要求
 *
 * 1. **不能拖慢主流程**：[record] 只把编码好的一行放进内存缓冲就返回，
 *    真正的 PUT 在后台协程里做。它会被调用在 DDL 之后与 GC 删除的循环里——
 *    那里每多一次网络往返都是可以直接感知的。
 * 2. **失败不能影响主流程**：写出失败时记录**回到缓冲**（不丢），并把失败计入
 *    `yb_audit_write_failed_total` 与 stderr。审计写不出去不该让一次 DDL 失败。
 * 3. **内存必须有界**：缓冲超过 [maxBufferedBytes] 时丢弃**最旧**的记录并计入
 *    `yb_audit_dropped_total`。宁可丢审计也不能让 CN 因为审计把内存吃光——
 *    但丢弃必须可见，否则就成了「静默失效」。
 *
 * ## 对象模型
 *
 * 当日对象是**追加语义**的，而对象存储不支持原地追加（原则 P2：对象不可变）。
 * 因此这里采用「进程内持有当日全量内容 + 每次 PUT 覆盖写」：
 * 每个进程每天开始时一次 GET 把已有内容读进内存（1 次请求/天/进程），
 * 之后每次刷新只有 1 次 PUT。当日对象超过 [maxObjectBytes] 后退化为
 * **不可变分片**（[ObjectKeys.auditRolled]），此后不再重写大对象。
 *
 * ⚠️ **已知边界**：多 CN 同时写同一天的同一个对象时是「后写覆盖先写」，
 * 会丢审计。审计在目标场景里由 DDL 与 GC 触发，两者都极少发生，
 * 且 CN 在架构上本来就是「可以随时销毁重建」的无状态进程；
 * 要彻底消除需要 MS 参与（未决的跨节点协调，见设计文档 §16.3）。
 * 明确记下这个边界，好过假装它是原子的。
 */
class ObjectStoreAuditLog(
    private val store: ObjectStore,
    private val scope: CoroutineScope,
    /** 后台刷新间隔。审计不是实时系统，攒起来写才划算。 */
    private val flushIntervalMillis: Long = 2_000,
    /** 当日对象的大小上限；超过后退化为不可变分片。 */
    private val maxObjectBytes: Int = 8 * 1024 * 1024,
    /** 内存缓冲上限；超过后丢弃最旧的记录。 */
    private val maxBufferedBytes: Long = 2L * 1024 * 1024,
    private val log: (String) -> Unit = { System.err.println(it) },
    private val nowMillis: () -> Long = { System.currentTimeMillis() },
    private val dateOf: (Long) -> String = { ms ->
        java.time.LocalDate.ofInstant(java.time.Instant.ofEpochMilli(ms), java.time.ZoneId.systemDefault())
            .toString()
    },
) : AuditSink, AutoCloseable {

    private val json = Json { encodeDefaults = false }

    /** 待写出的行。用 [lock] 保护（而不是协程 Mutex）——[record] 是同步 API。 */
    private val pending = ArrayDeque<String>()
    private var pendingBytes = 0L
    private val lock = Any()

    /** 刷新互斥：同一时刻只有一个刷新在跑，避免两次覆盖写交错。 */
    private val flushMutex = Mutex()

    private var headDate: String? = null
    private var headContent = StringBuilder()
    private var headLoaded = false

    /** 当日对象超过上限后置位：之后每次刷新写一个独立分片。 */
    private var rolled = false
    private var lastRolledSeq = 0L

    private var job: Job? = null

    /** 已写出（或已丢弃）的记录数。测试用它等后台刷新落地。 */
    @Volatile
    var writtenRecords: Long = 0
        private set

    override fun record(event: AuditEvent) {
        val line = try {
            json.encodeToString(AuditEvent.serializer(), event)
        } catch (t: Throwable) {
            Metrics.auditWriteFailed.inc()
            log("[yb-audit] 记录编码失败（已丢弃）: ${t.message}")
            return
        }
        synchronized(lock) {
            pending.addLast(line)
            pendingBytes += line.length + 1
            // 有界缓冲：丢最旧的。审计丢记录是**必须可见**的事件。
            while (pendingBytes > maxBufferedBytes && pending.size > 1) {
                val dropped = pending.removeFirst()
                pendingBytes -= dropped.length + 1
                Metrics.auditDropped.inc()
                log("[yb-audit] 缓冲区已满，丢弃最旧的审计记录（共丢弃 ${Metrics.auditDropped.value()} 条）")
            }
        }
    }

    /** 启动后台刷新。返回的 Job 不需要调用方持有——[close] 会停掉它。 */
    fun start(): Job {
        val j = scope.launch {
            while (isActive) {
                delay(flushIntervalMillis)
                runCatching { flush() }
                    .onFailure { log("[yb-audit] 后台刷新异常: ${it.message}") }
            }
        }
        job = j
        return j
    }

    /**
     * 立即刷新一次。
     *
     * @return 本次是否真的写出了对象（没有待写记录时返回 false）
     */
    suspend fun flush(): Boolean = flushMutex.withLock {
        val batch: List<String>
        synchronized(lock) {
            if (pending.isEmpty()) return false
            batch = pending.toList()
            pending.clear()
            pendingBytes = 0
        }

        val now = nowMillis()
        val date = dateOf(now)

        if (rolled) {
            // 已退化成分片：每个进程每天对象超过上限之后，一次刷新 = 一个不可变对象
            val seq = maxOf(now, lastRolledSeq + 1)
            val key = ObjectKeys.auditRolled(date, seq)
            return try {
                store.put(key, (batch.joinToString("\n", postfix = "\n")).toByteArray())
                lastRolledSeq = seq
                writtenRecords += batch.size
                Metrics.auditWritten.add(batch.size.toLong())
                true
            } catch (t: Throwable) {
                requeue(batch)
                onWriteFailure(t, key)
                false
            }
        }

        if (headDate != date) {
            headDate = date
            headContent = StringBuilder()
            headLoaded = false
        }

        if (!headLoaded) {
            // 一天的第一次刷新：把已有内容读回来（对象存储不能原地追加）
            val existing = try {
                String(store.get(ObjectKeys.auditLog(date)), Charsets.UTF_8)
            } catch (_: ObjectNotFoundException) {
                ""
            } catch (t: Throwable) {
                requeue(batch)
                onWriteFailure(t, ObjectKeys.auditLog(date))
                return false
            }
            headContent = StringBuilder(existing)
            headLoaded = true
        }

        for (line in batch) headContent.append(line).append('\n')

        if (headContent.length > maxObjectBytes) {
            // 当日对象太大：冻结它，从这一批起改写不可变分片。
            // ⚠️ 这里**不能递归调用 flush()**：本函数已持有 flushMutex，而协程的
            // Mutex 不可重入，递归会直接死锁。
            rolled = true
            log("[yb-audit] 当日审计对象超过 ${maxObjectBytes / 1024 / 1024}MB，后续记录改为写入不可变分片")
            val seq = maxOf(now, lastRolledSeq + 1)
            val rollKey = ObjectKeys.auditRolled(date, seq)
            return try {
                store.put(rollKey, batch.joinToString("\n", postfix = "\n").toByteArray())
                lastRolledSeq = seq
                writtenRecords += batch.size
                Metrics.auditWritten.add(batch.size.toLong())
                true
            } catch (t: Throwable) {
                requeue(batch)
                onWriteFailure(t, rollKey)
                false
            }
        }

        val key = ObjectKeys.auditLog(date)
        return try {
            store.put(key, headContent.toString().toByteArray())
            writtenRecords += batch.size
            Metrics.auditWritten.add(batch.size.toLong())
            true
        } catch (t: Throwable) {
            // 内容可能没写进去，下一次刷新必须重新读回来才能确定基线
            headLoaded = false
            headContent.setLength(0)
            requeue(batch)
            onWriteFailure(t, key)
            false
        }
    }

    /** 把没写成功的记录放回缓冲**头部**（它们比新记录更旧，顺序要保持）。 */
    private fun requeue(batch: List<String>) {
        synchronized(lock) {
            for (line in batch.asReversed()) {
                pending.addFirst(line)
                pendingBytes += line.length + 1
            }
        }
    }

    private fun onWriteFailure(t: Throwable, key: String) {
        Metrics.auditWriteFailed.inc()
        log("[yb-audit] 写出失败（记录会重试，不影响主流程）key=$key: ${t::class.simpleName}: ${t.message}")
    }

    /**
     * 停止后台刷新并把残留的记录尽量写出去。
     *
     * 名字刻意不叫 `close`：`AutoCloseable.close()` 是无参同步方法，
     * 同一个类里再放一个同名的挂起函数会在 JVM 签名上打架。
     */
    suspend fun shutdown() {
        job?.cancel()
        job = null
        runCatching { flush() }.onFailure { log("[yb-audit] 关闭时刷新失败: ${it.message}") }
    }

    override fun close() {
        // AutoCloseable 的同步签名：只能取消后台任务，写不出去的记录会留在内存里。
        // 需要「把残留写出去」时用 [shutdown]（它要挂起，因此不能是 close）。
        job?.cancel()
        job = null
    }
}
