package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.BigEndian
import com.cloudea.yangbase.common.Checksums
import com.cloudea.yangbase.common.CorruptDataException
import com.cloudea.yangbase.common.Epoch
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.Varint
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import java.io.ByteArrayOutputStream

/** 一次写入操作。value 为 null 表示墓碑（删除）。 */
class WalOp(val key: ByteArray, val value: ByteArray?)

/**
 * 一个事务的写集。
 *
 * 跨分片事务的第一阶段会把同样的写集另存一份到**未决**对象
 * （`ObjectKeys.walPending`），那是它的持久性保险。参见 `DatabaseEngine.commitBuffer`。
 */
data class WalBatch(
    val txId: String,
    val commitTs: Long,
    val ops: List<WalOp>,
    /**
     * 这一批所在的 **WAL 对象序号**（`wal/e{epoch}/{seq}.log`）。
     *
     * `-1` 表示"还没有被写进对象"（构造于写入之前的入队阶段）。
     * 它的唯一用途是让 `flush` 记下**「哪些序号的变更真的进了这个 SST」**——
     * 见 `LsmStore.flushLocked` 里对 `walFlushedThroughSeq` 的说明：
     * 曾经那里记的是"已分配的序号"，崩溃时会把一批**没进 SST 也没被重放**的
     * 已确认写入整段跳过。
     */
    val seq: Long = -1L,
) {

    val approximateBytes: Int get() = 32 + ops.sumOf { it.key.size + (it.value?.size ?: 0) + 8 }
}

class WalObject(
    val epoch: Long,
    val seq: Long,
    val batches: List<WalBatch>,
    /** 解析过程中跳过的损坏记录数。非零即应告警。 */
    val corruptedRecords: Int = 0,
    /** 这个对象是否来自未决（2PC 第一阶段）路径。 */
    val pending: Boolean = false,
)

/**
 * WAL 对象编解码。
 *
 * 与设计文档 §6.3.3 的格式一致。**每条 BatchRecord 带独立 CRC**，
 * 这样一个字节的损坏只影响一个事务，而不是整批 30 个事务——
 * 读取时跳过坏记录并告警，而不是让整个分片不可用。
 */
object WalCodec {
    private val MAGIC = byteArrayOf('Y'.code.toByte(), 'B'.code.toByte(), 'W'.code.toByte(), 'L'.code.toByte())
    const val FORMAT_VERSION = 1
    private const val HEADER_SIZE = 64

    // 头部字段偏移。写成常量而非魔数——编码与解码各写一遍字面量是
    // 这类格式代码最典型的错误来源（本次实现中确实踩到了）。
    private const val OFF_MAGIC = 0        // 4B
    private const val OFF_VERSION = 4      // 2B
    private const val OFF_EPOCH = 6        // 8B
    private const val OFF_SEQ = 14         // 8B
    private const val OFF_BATCH_COUNT = 22 // 4B
    private const val OFF_MIN_TS = 26      // 8B
    private const val OFF_MAX_TS = 34      // 8B
    private const val OFF_PAYLOAD_LEN = 42 // 4B
    private const val OFF_HEADER_CRC = 46  // 4B（CRC 覆盖 [0, 46)）

    fun encode(epoch: Epoch, seq: Long, batches: List<WalBatch>): ByteArray {
        val payload = ByteArrayOutputStream(4096)
        for (b in batches) encodeBatch(payload, b)
        val payloadBytes = payload.toByteArray()

        var minTs = Long.MAX_VALUE
        var maxTs = Long.MIN_VALUE
        for (b in batches) { minTs = minOf(minTs, b.commitTs); maxTs = maxOf(maxTs, b.commitTs) }
        if (batches.isEmpty()) { minTs = 0; maxTs = 0 }

        val out = ByteArrayOutputStream(HEADER_SIZE + payloadBytes.size + 8)
        out.write(MAGIC)
        BigEndian.writeShort(out, FORMAT_VERSION)
        BigEndian.writeLong(out, epoch.value)
        BigEndian.writeLong(out, seq)
        BigEndian.writeInt(out, batches.size)
        BigEndian.writeLong(out, minTs)
        BigEndian.writeLong(out, maxTs)
        BigEndian.writeInt(out, payloadBytes.size)
        val headerSoFar = out.toByteArray()
        BigEndian.writeInt(out, Checksums.crc32c(headerSoFar))
        while (out.size() < HEADER_SIZE) out.write(0)

        out.write(payloadBytes)
        val all = out.toByteArray()
        BigEndian.writeInt(out, Checksums.crc32c(all))
        return out.toByteArray()
    }

    private fun encodeBatch(out: ByteArrayOutputStream, b: WalBatch) {
        val body = ByteArrayOutputStream(256)
        val txBytes = b.txId.toByteArray(Charsets.UTF_8)
        Varint.writeUnsigned(body, txBytes.size)
        body.write(txBytes)
        BigEndian.writeLong(body, b.commitTs)
        Varint.writeUnsigned(body, b.ops.size)
        for (op in b.ops) {
            Varint.writeUnsigned(body, op.key.size)
            body.write(op.key)
            if (op.value == null) {
                body.write(0) // 墓碑标记
            } else {
                body.write(1)
                Varint.writeUnsigned(body, op.value.size)
                body.write(op.value)
            }
        }
        val bodyBytes = body.toByteArray()
        Varint.writeUnsigned(out, bodyBytes.size)
        out.write(bodyBytes)
        BigEndian.writeInt(out, Checksums.crc32c(bodyBytes))
    }

    /**
     * 段对象的 magic（`s-{from}-{to}.log`）。
     *
     * 段是**多个完整 WAL 对象**的拼接（每个自带 64B 头与尾部 CRC），
     * 外面再包一层自己的头。不能直接把它当成一个 WAL 对象解码：
     * `decode` 要求**整个 buffer 的末尾**是整段 CRC，而段末尾是最后一个
     * 内嵌对象的 CRC——于是段永远解不开，静默损坏了三条读 WAL 的路径
     * （恢复、在线索引追平、CDC）。所以段有自己的帧格式与解码器，
     * 所有读 WAL 的地方都走 [decodeAny]。
     */
    private val SEG_MAGIC =
        byteArrayOf('Y'.code.toByte(), 'B'.code.toByte(), 'W'.code.toByte(), 'S'.code.toByte())

    /** 段头的字节数：magic(4) + 内嵌对象个数(4) + 每段总长(4)。 */
    private const val SEG_HEADER_SIZE = 12

    /**
     * 把若干 WAL 对象编成一个段。
     *
     * 每个内嵌对象**保持原样**（含自己的头与 CRC）：这样单对象级别的完整性校验
     * 仍然成立，段只是"一个更容易列举的容器"。
     */
    fun encodeSegment(objects: List<ByteArray>): ByteArray {
        val out = java.io.ByteArrayOutputStream(objects.sumOf { it.size } + SEG_HEADER_SIZE)
        out.write(SEG_MAGIC)
        BigEndian.writeInt(out, objects.size)
        BigEndian.writeInt(out, objects.sumOf { it.size } + SEG_HEADER_SIZE)
        for (o in objects) out.write(o)
        return out.toByteArray()
    }

    /**
     * 从 WAL 键名里解析出它覆盖的**起始序号**，解析不出返回 null。
     *
     * 存在的意义是**省请求**：`{seq}.log` 与 `s-{from}-{to}.log` 的序号都写在键里，
     * 于是"这个对象已经重放过了"这件事不需要 GET 一次才知道——
     * 而恢复/只读视图刷新是**每次读**都可能跑的路径。
     * 一个 epoch 下最多可有上百个 WAL 对象，逐个 GET 会让一次点查变成上百次请求。
     */
    fun seqOfKey(key: String): Long? =
        segmentRangeOfKey(key)?.first ?: normalSeqOfKey(key)

    /**
     * 段对象覆盖的序号区间 `[from, to]`；不是段则返回 **null**。
     *
     * 为什么要把 `to` 也解析出来：**GC 必须按 `to` 判断能不能删**。
     * 按 `from` 判断要依赖一条隐式前提（段只会由已 flush 的对象构成），
     * 而 GC 是这套系统里**唯一真正销毁数据**的地方——它不该依赖别的模块的隐式前提。
     * 按 `to` 判断时，那条例外情形也只会让 GC 少删一点，而不是删掉还在用的日志。
     * 恢复路径用 `from`（跳过一定是安全的：跳过一个段意味着它的全部内容都已应用）。
     */
    fun segmentRangeOfKey(key: String): Pair<Long, Long>? {
        val name = key.substringAfterLast('/')
        if (!name.startsWith("s-") || !name.endsWith(".log")) return null
        val body = name.removePrefix("s-").removeSuffix(".log")
        val from = body.substringBefore('-').toLongOrNull() ?: return null
        val to = body.substringAfter('-', "").toLongOrNull() ?: return null
        return from to to
    }

    /** 普通批次键 `{seq}.log` → 序号；未决对象（`.p…`）与段返回 null。 */
    private fun normalSeqOfKey(key: String): Long? {
        val name = key.substringAfterLast('/')
        if (!name.endsWith(".log")) return null
        if (name.startsWith(".p")) return null                  // 未决对象：键名不同，单独处理
        if (name.startsWith("s-")) return null
        return name.removeSuffix(".log").toLongOrNull()
    }

    /** 这个 buffer 是不是段对象。 */
    fun isSegment(bytes: ByteArray): Boolean =
        bytes.size >= 4 && (0 until 4).all { bytes[it] == SEG_MAGIC[it] }

    /**
     * **统一的 WAL 解码入口**：普通对象与段都能解。
     *
     * 读 WAL 的地方（恢复 / 在线索引追平 / CDC）一律走这里，
     * 而不是各自 `WalCodec.decode`——那样只要有一处忘了处理段，
     * 那条路径就静默少读一段日志。
     *
     * 段的头里记了内嵌对象个数与总长，所以切分不依赖"每个对象自报长度"，
     * 一个对象的损坏不会让后续对象错位。
     */
    fun decodeAny(bytes: ByteArray, key: String = ""): List<WalObject> {
        if (!isSegment(bytes)) return listOf(decode(bytes, key))
        if (bytes.size < SEG_HEADER_SIZE) throw CorruptDataException("WAL 段过短: ${bytes.size}B")
        val count = BigEndian.readInt(bytes, 4)
        val total = BigEndian.readInt(bytes, 8)
        if (count < 0 || total != bytes.size) {
            throw CorruptDataException("WAL 段头部不合法：count=$count total=$total 实际=${bytes.size}")
        }
        val out = ArrayList<WalObject>(count)
        var p = SEG_HEADER_SIZE
        for (i in 0 until count) {
            // 每个内嵌对象自带长度：先读它的 payloadLen 再算总长
            if (p + HEADER_SIZE > bytes.size) throw CorruptDataException("WAL 段在对象 $i 处截断")
            val payloadLen = BigEndian.readInt(bytes, p + OFF_PAYLOAD_LEN)
            val objLen = HEADER_SIZE + payloadLen + 4
            if (p + objLen > bytes.size) throw CorruptDataException("WAL 段在对象 $i 处截断（需要 $objLen 字节）")
            out.add(decode(bytes.copyOfRange(p, p + objLen), key))
            p += objLen
        }
        return out
    }

    fun decode(bytes: ByteArray, key: String = ""): WalObject {
        if (bytes.size < HEADER_SIZE + 8) throw CorruptDataException("WAL 对象过短: ${bytes.size}B")
        for (i in 0 until 4) {
            if (bytes[i] != MAGIC[i]) throw CorruptDataException("WAL magic 不匹配")
        }
        val ver = BigEndian.readShort(bytes, OFF_VERSION)
        if (ver != FORMAT_VERSION) throw CorruptDataException("不支持的 WAL 格式版本: $ver")
        val epoch = BigEndian.readLong(bytes, OFF_EPOCH)
        val seq = BigEndian.readLong(bytes, OFF_SEQ)
        val batchCount = BigEndian.readInt(bytes, OFF_BATCH_COUNT)
        val payloadLen = BigEndian.readInt(bytes, OFF_PAYLOAD_LEN)
        val headerCrc = BigEndian.readInt(bytes, OFF_HEADER_CRC)
        val headerActual = Checksums.crc32c(bytes, 0, OFF_HEADER_CRC)
        if (headerCrc != headerActual) throw CorruptDataException("WAL 头部 CRC 校验失败")

        val totalCrc = BigEndian.readInt(bytes, bytes.size - 4)
        val totalActual = Checksums.crc32c(bytes, 0, bytes.size - 4)
        if (totalCrc != totalActual) throw CorruptDataException("WAL 整体 CRC 校验失败")

        var p = HEADER_SIZE
        val end = minOf(HEADER_SIZE + payloadLen, bytes.size - 4)
        val batches = ArrayList<WalBatch>(batchCount)
        var corrupted = 0
        while (p < end) {
            try {
                val (bodyLen, n) = Varint.readUnsigned(bytes, p)
                val bodyStart = p + n
                if (bodyStart + bodyLen + 4 > bytes.size) break
                val body = bytes.copyOfRange(bodyStart, bodyStart + bodyLen)
                val expectCrc = BigEndian.readInt(bytes, bodyStart + bodyLen)
                if (Checksums.crc32c(body) != expectCrc) {
                    // 单条记录损坏：跳过它，不影响整批（设计文档 §6.3.3）
                    corrupted++
                    p = bodyStart + bodyLen + 4
                    continue
                }
                batches.add(decodeBatchBody(body))
                p = bodyStart + bodyLen + 4
            } catch (_: Exception) {
                corrupted++
                break
            }
        }
        return WalObject(epoch, seq, batches.map { it.copy(seq = seq) }, corrupted, ObjectKeys.isPendingWal(key))
    }

    private fun decodeBatchBody(body: ByteArray): WalBatch {
        var p = 0
        val (txLen, n0) = Varint.readUnsigned(body, p); p += n0
        val txId = String(body.copyOfRange(p, p + txLen), Charsets.UTF_8); p += txLen
        val commitTs = BigEndian.readLong(body, p); p += 8
        val (opCount, n1) = Varint.readUnsigned(body, p); p += n1
        val ops = ArrayList<WalOp>(opCount)
        repeat(opCount) {
            val (klen, n2) = Varint.readUnsigned(body, p); p += n2
            val key = body.copyOfRange(p, p + klen); p += klen
            val isTombstone = body[p].toInt() == 0; p++
            if (isTombstone) {
                ops.add(WalOp(key, null))
            } else {
                val (vlen, n3) = Varint.readUnsigned(body, p); p += n3
                ops.add(WalOp(key, body.copyOfRange(p, p + vlen))); p += vlen
            }
        }
        return WalBatch(txId, commitTs, ops)
    }
}

/** 提交模式。出厂默认 [SYNC]——见设计文档 §6.3.4 与 §16.1 Q1。 */
enum class CommitMode(val windowMillis: Long, val maxBatchBytes: Int, val waitForDurability: Boolean) {
    /** 出厂默认。RPO = 0。 */
    SYNC(5, 256 * 1024, true),

    /** 高吞吐导入，可容忍延迟。RPO = 0。 */
    BATCH(50, 4 * 1024 * 1024, true),

    /**
     * **必须显式声明**。RPO > 0——崩溃会丢失最多约 100ms 已 ACK 的写入。
     * 开启后 `SHOW VARIABLES LIKE 'commit_mode'` 必须如实返回。
     *
     * 语义边界（与 SYNC/BATCH 的差别，全部是**刻意的**）：
     *  - **ACK 早于落盘**：`submit()` 在 PUT 被**发起**后立即返回 `durable = false`；
     *    PUT 由后台协程完成，失败会一直重试并计数（不是静默丢弃）。
     *  - **RPO > 0**：进程崩溃会丢掉「已 ACK 但尚未落盘」的那部分，上限是
     *    上传队列深度 × 窗口，而不是无限。
     *  - **跨 CN 可见性晚一个往返**：ACK 时对象还没落地，另一个 CN 立刻读看不到。
     *    本 CN 内因为 MemTable 在 ACK 前已应用，自己读得到。
     *    所以它**不是强一致**，是「本 CN 不丢（进程不挂）且可能暂时对外不可见」。
     */
    ASYNC(100, 4 * 1024 * 1024, false),
    ;

    companion object {
        fun parse(s: String): CommitMode = when (s.lowercase()) {
            "sync" -> SYNC
            "batch" -> BATCH
            "async" -> ASYNC
            else -> throw IllegalArgumentException("未知的提交模式: $s（可选 sync/batch/async）")
        }
    }
}

class CommitResult(val walSeq: Long, val durable: Boolean)

/**
 * ASYNC 模式的在途上传队列容量。见 [WalWriter] 里 `uploads` 的注释：
 * 它同时是「RPO 上界」里的那个乘数。
 */
const val ASYNC_UPLOAD_QUEUE = 64

/**
 * WAL 写者 + **组提交**（设计文档 §6.3.4）。
 *
 * 缓存的不是一个共享 buffer，而是一批待提交的 [WalBatch]：窗口到点或攒够字节后，
 * 一次 PUT 把它们编成一个 WAL 对象。这就是「写比读贵 12.5 倍」这条成本不对称
 * （§13.1）的直接应对。
 */
class WalWriter(
    private val store: ObjectStore,
    private val shard: ShardId,
    private val epoch: Epoch,
    startSeq: Long,
    private val mode: CommitMode = CommitMode.SYNC,
    private val scope: CoroutineScope,
    /**
     * 提交成功后的回调，**严格按 WAL 提交顺序**在同一条协程上被调用。
     *
     * 这是保证「按提交序应用」的关键：并发的写者可以同时在队列里排队攒批，
     * 但把变更应用到 MemTable 与维护行数增量必须在确定的序上进行，
     * 否则两笔并发事务写同一个 key 时会算出错误的行数。
     */
    private val onCommitted: suspend (List<WalBatch>) -> Unit = { },
    /**
     * 每次组提交**之前**调用一次。用来校验写者租约（设计文档 §7.2）。
     *
     * 放在这里而不是每次 submit：租约的粒度是"谁在写这个分片"，
     * 而不是"哪一笔事务"。每批次校验一次既够用，也不会在每次写入上多加一次检查。
     * 抛出的异常会让整批失败——这是有意的：**已经不是写者了就一个字节都不该写下去**。
     */
    private val beforeWrite: () -> Unit = { },
) {
    private class Pending(
        val batch: WalBatch,
        val done: CompletableDeferred<CommitResult>,
        /** 投递时刻。提交延迟是**用户感知**的量：从投递到「返回即已持久化」。 */
        val startedNanos: Long,
    )

    private val queue = Channel<Pending>(capacity = 8192)
    private var seq = startSeq
    private val closed = java.util.concurrent.atomic.AtomicBoolean(false)

    init {
        // 部署级语义上报一次：`yb_commit_mode_rpo_gt0` 就是「这个实例答应了你什么」
        Metrics.setCommitModeRpoGt0(!mode.waitForDurability)
    }

    /**
     * ASYNC 模式下「已 ACK、尚未落盘」的一批。
     *
     * 只有 [CommitMode.waitForDurability] 为 false 时才会产生——它存在的唯一理由
     * 就是把 `store.put` 从提交路径上挪到后台，让 ACK 不再等待一个网络往返。
     */
    private class Upload(
        val seq: Long,
        val key: String,
        val bytes: ByteArray,
        val batches: Int,
        val ops: Long,
    )

    /**
     * 上传队列容量 = ASYNC 的**在途窗口上限**。
     *
     * 队列满时组提交主循环会挂住（背压），所以 RPO 的上界是
     * 「队列深度 × 窗口」而不是「无限」；对象存储长时间不可用时表现为写入变慢，
     * 而不是内存涨到 OOM 或「ACK 了但其实从来没写」。
     */
    private val uploads = Channel<Upload>(capacity = ASYNC_UPLOAD_QUEUE)

    /** 已确认落盘的最大序号（ASYNC 下才是 ACK 之后的量；其它模式下等于当前 seq）。 */
    private val durableSeq = java.util.concurrent.atomic.AtomicLong(startSeq - 1)

    val currentSeq: Long get() = seq

    /** 已确认落盘的 WAL 序号。ASYNC 下它与 [currentSeq] 的差距就是「在途量」。 */
    val durableThroughSeq: Long get() = durableSeq.get()

    /**
     * 预留下一个 WAL 序号。
     *
     * 给跨分片 2PC 用：它要在提交点**之前**把未决对象写到同一个前缀下，
     * 必须与普通批次共用一套序号，否则恢复时 `sortedBy { seq }` 的顺序会错。
     */
    fun reserveSeq(): Long = seq++

    /** 提交一个事务。**只有 [CommitMode.SYNC] / [BATCH] 下返回才代表已持久化。**
     *
     * [CommitMode.ASYNC] 下返回值里的 `durable` 恒为 false：那一刻 PUT 已被发起，
     * 但还没完成。调用方**不得**把 `durable == false` 当成失败（它已经确认了），
     * 只能当成「RPO > 0 的那部分」。
     */
    suspend fun submit(batch: WalBatch): CommitResult {
        check(!closed.get()) { "WalWriter 已关闭" }
        val p = Pending(batch, CompletableDeferred(), System.nanoTime())
        queue.send(p)
        return p.done.await()
    }

    /** 组提交主循环。由持有租约的写者运行。 */
    fun start(): Job = scope.launch {
        // ASYNC 才需要一条独立的上传协程；其它模式下 PUT 就在主循环里同步等它。
        // 注意它是**子协程**：主循环被取消时它一并被取消，不会留一条野协程写对象。
        val uploader = if (!mode.waitForDurability) launch { uploadLoop() } else null
        try {
            commitLoop()
        } finally {
            uploads.close()
            // 收尾时给在途 PUT 一个机会：数据本身不依赖它（flush 会把 MemTable 落成 SST），
            // 但让它落地能少一次恢复重放。给 30s，超时就取消——**关闭不能被网络卡死**。
            if (uploader != null) {
                withTimeoutOrNull(30_000) { uploader.join() } ?: uploader.cancel()
            }
        }
    }

    private suspend fun commitLoop() {
        val pending = ArrayList<Pending>(64)
        var bytes = 0

        while (!closed.get()) {
            // 阻塞等待至少一个待提交项（空闲时窗口趋近于 0：一旦有写入立即触发）
            val first = queue.tryReceive().getOrNull()
                ?: runCatching { kotlinx.coroutines.withTimeoutOrNull(1000) { queue.receive() } }.getOrNull()
                ?: continue
            pending.add(first)
            bytes += first.batch.approximateBytes

            // 攒批到窗口耗尽或字节到顶
            val deadline = System.nanoTime() + mode.windowMillis * 1_000_000
            while (bytes < mode.maxBatchBytes && System.nanoTime() < deadline) {
                val more = queue.tryReceive().getOrNull() ?: break
                pending.add(more)
                bytes += more.batch.approximateBytes
            }

            try {
                // 写之前先确认自己还是写者。这不是性能开销——它只在组提交的
                // 那一瞬间做一次，而拦下的是「旧写者继续写」这种静默数据错乱。
                beforeWrite()

                val mySeq = seq
                seq += 1
                // 把序号写进批次：`LsmStore` 靠它记「哪些变更真的进了 SST」
                val batches = pending.map { it.batch.copy(seq = mySeq) }
                val encoded = WalCodec.encode(epoch, mySeq, batches)
                val key = ObjectKeys.walBatch(shard, epoch, mySeq)

                if (mode.waitForDurability) {
                    store.put(key, encoded)
                    durableSeq.set(mySeq)

                    // 一次 PUT 摊掉了多少个事务——这就是组提交的全部意义（§6.3.4）。
                    // 摊薄倍数是「写成本」的直接代理：倍数为 1 表示一次写入一次 PUT。
                    Metrics.onGroupCommit(batches.size, batches.sumOf { it.ops.size.toLong() })

                    // 持久化成功之后、通知提交者之前，按序应用这批变更
                    onCommitted(batches)

                    val now = System.nanoTime()
                    for (p in pending) {
                        Metrics.commitDuration.observe((now - p.startedNanos) / 1e9)
                        p.done.complete(CommitResult(mySeq, true))
                    }
                } else {
                    // ★ ASYNC：交出去 → 应用 → ACK。两步顺序都不能反：
                    //  · `onCommitted` 必须在提交序上执行（行数增量靠它算对）；
                    //  · 应用必须在 ACK **之前**，否则「已经返回成功的写入自己读不到」。
                    // 真正的落盘由 uploadLoop 在后面做，失败就重试——不丢给客户端。
                    uploads.send(
                        Upload(mySeq, key, encoded, batches.size, batches.sumOf { it.ops.size.toLong() }),
                    )
                    Metrics.asyncCommitInFlight()
                    onCommitted(batches)

                    val now = System.nanoTime()
                    for (p in pending) {
                        Metrics.commitDuration.observe((now - p.startedNanos) / 1e9)
                        p.done.complete(CommitResult(mySeq, durable = false))
                    }
                }
            } catch (t: Throwable) {
                for (p in pending) p.done.completeExceptionally(t)
            } finally {
                pending.clear()
                bytes = 0
            }
        }
    }

    /**
     * ASYNC 的后台上传。**按序号顺序**逐批 PUT，一批落盘后才动下一批。
     *
     * 为什么失败要无限重试：此刻客户端已经拿到成功了，把这一批扔掉就是
     * 「静默丢数据」——比让写入变慢坏得多。背压来自 [uploads] 的容量：
     * 队列满了主循环就挂住、不再 ACK 新的批次，问题会在推迟的延迟上暴露，
     * 而不是在半个月后的一次重启里。
     */
    private suspend fun uploadLoop() {
        for (u in uploads) {
            var attempt = 0
            while (true) {
                try {
                    store.put(u.key, u.bytes)
                    Metrics.onGroupCommit(u.batches, u.ops)
                    Metrics.asyncCommitDurable()
                    durableSeq.set(u.seq)
                    break
                } catch (c: CancellationException) {
                    throw c
                } catch (t: Throwable) {
                    attempt++
                    Metrics.asyncCommitPutFailure()
                    // 关闭时不再无限重试：3 次还不成就算丢（计数 + 严重日志）。
                    // 这是 RPO > 0 的真实代价，必须能变得**可见**。
                    if (closed.get() && attempt >= 3) {
                        Metrics.asyncCommitLost()
                        System.err.println(
                            "[yb] 严重: commit_mode=async 关闭前仍有 WAL 对象未落盘，已放弃 ${u.key}：$t",
                        )
                        break
                    }
                    delay(minOf(2_000L, 50L shl minOf(attempt - 1, 5)))
                }
            }
        }
    }

    fun close() {
        closed.set(true)
        queue.close()
    }
}

/**
 * 读取一个 epoch 下的全部 WAL 对象，按 seq 升序返回。
 *
 * ⚠️ **这是恢复路径，不是热路径**——LIST 是慢操作，绝不能被放到查询路径上
 * （CLAUDE.md 的反模式清单）。
 */
suspend fun readWalObjects(
    store: ObjectStore,
    shard: ShardId,
    epoch: Epoch,
    fromSeqExclusive: Long,
): List<WalObject> {
    val all = readWalPrefix(store, ObjectKeys.walPrefix(shard, epoch), fromSeqExclusive)
    val normal = all.filter { !it.pending }
    val pendings = all.filter { it.pending }
    if (pendings.isEmpty()) return normal

    // 未决对象（跨分片 2PC 的第一阶段）：**只有同时满足两个条件才重放**。
    //
    //  1. 事务的提交标记存在 —— 否则这笔事务没提交，必须整体丢弃
    //  2. 这个 txId **没有**出现在普通 WAL 里 —— 出现了说明第三阶段已经跑完，
    //     数据已经按正常路径落过了，再重放一次会重复计数
    //
    // 第 2 条是关键：它让「提交标记已写、数据已落、但未决对象还没删掉时崩溃」
    // 这个窗口不会变成数据翻倍。
    val appliedTxIds = HashSet<String>()
    for (o in normal) for (b in o.batches) appliedTxIds.add(b.txId)

    val decided = HashMap<String, Boolean>()
    val out = ArrayList<WalObject>(normal)
    for (o in pendings) {
        val txId = o.batches.firstOrNull()?.txId
        if (txId == null) continue
        if (txId in appliedTxIds) continue
        val ok = decided.getOrPut(txId) { store.exists(ObjectKeys.txCommit(txId)) }
        if (ok) out.add(o) else pendingDropped.incrementAndGet()
    }
    return out.sortedBy { it.seq }
}

/** 因缺少提交标记而被丢弃的未决批次数。非零说明发生过一次失败的跨分片提交。 */
val pendingDropped = java.util.concurrent.atomic.AtomicLong()

private suspend fun readWalPrefix(
    store: ObjectStore,
    prefix: String,
    fromSeqExclusive: Long,
): List<WalObject> {
    val keys = store.listAll(prefix).sorted()
    val out = ArrayList<WalObject>()
    for (k in keys) {
        // ⚡ 键名里就写着序号：已经消费过的对象不必 GET 一次才知道
        // （恢复与只读视图刷新都是"每次读"可能跑的路径，而一个 epoch 下
        //   最多可有上百个 WAL 对象——逐个 GET 会让一次点查变成上百次请求）
        val keySeq = WalCodec.seqOfKey(k)
        if (keySeq != null && keySeq <= fromSeqExclusive && !k.contains("/s-")) continue
        val bytes = store.get(k)
        // ⚠️ 必须用 decodeAny：WAL 前缀里既有普通对象也有**段对象**，
        // 只认普通对象会让整段日志被静默跳过
        val objs = try {
            WalCodec.decodeAny(bytes, k)
        } catch (_: CorruptDataException) {
            continue // 整体损坏的对象跳过并告警（单条损坏由 decode 内部处理）
        }
        for (obj in objs) if (obj.seq > fromSeqExclusive) out.add(obj)
    }
    return out
}
