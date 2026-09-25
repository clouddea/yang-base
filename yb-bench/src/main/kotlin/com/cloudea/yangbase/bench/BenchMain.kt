package com.cloudea.yangbase.bench

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.PartitionKind
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.EmbeddedS3Server
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.S3ObjectStore
import com.cloudea.yangbase.storage.BlockCache
import com.cloudea.yangbase.storage.CommitMode
import com.cloudea.yangbase.storage.LsmStore
import com.cloudea.yangbase.storage.WalBatch
import com.cloudea.yangbase.storage.WalOp
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicLong
import kotlin.system.measureNanoTime

/**
 * YangBase 基准测试。
 *
 * 与常规数据库基准不同，这里把**对象存储请求数**作为一等指标——
 * 它既是延迟的代理，也是成本的直接来源（设计文档 §14.2）。
 *
 * 运行：`./gradlew :yb-bench:bench`
 */
fun main() {
    println("=".repeat(78))
    println("YangBase 基准测试")
    println("=".repeat(78))

    // 默认用内置的 S3 兼容服务器；-DYB_S3_ENDPOINT=... 时打真实对象存储（MinIO / S3）
    val external = System.getProperty("YB_S3_ENDPOINT") ?: System.getenv("YB_S3_ENDPOINT")
    val embedded = if (external == null) EmbeddedS3Server("yangbase-bench").also { it.start() } else null

    val endpoint = external ?: embedded!!.endpoint
    val bucket = System.getProperty("YB_S3_BUCKET") ?: System.getenv("YB_S3_BUCKET") ?: "yangbase-bench"
    val ak = System.getProperty("YB_S3_ACCESS_KEY") ?: System.getenv("YB_S3_ACCESS_KEY") ?: "bench"
    val sk = System.getProperty("YB_S3_SECRET_KEY") ?: System.getenv("YB_S3_SECRET_KEY") ?: "bench"

    println("对象存储后端: ${if (external != null) "真实对象存储" else "内嵌 S3 兼容服务器"} @ $endpoint")

    try {
        if (external != null) {
            S3ObjectStore.ensureBucket(bucket, endpoint, "us-east-1", ak, sk)
        }
        val raw: ObjectStore = S3ObjectStore.create(
            bucket = bucket, endpoint = endpoint, region = "us-east-1",
            accessKey = ak, secretKey = sk,
        )
        val counting = CountingObjectStore(raw)
        println()

        runBlocking {
            benchPointLookup(counting)
            benchWritePath(counting)
            benchConcurrentWrite(counting)
            benchCountOptimization(counting)
            benchRangeScan(counting)
            benchSstSizes()
        }
    } finally {
        embedded?.stop()
    }
}

private val SCHEMA = TableSchema(
    tableId = 1, databaseId = 1, name = "bench", version = 1,
    columns = listOf(
        ColumnDef("id", ColumnType.BIGINT, nullable = false),
        ColumnDef("name", ColumnType.VARCHAR, length = 64),
        ColumnDef("age", ColumnType.INT),
        ColumnDef("status", ColumnType.VARCHAR, length = 16),
    ),
    primaryKey = listOf("id"),
)

private val SHARD = ShardId(TableId(1), PartitionId(0))

private fun keyOf(id: Long) = KeyEncoder.encodeKey(listOf(Datum.I64(id)), SCHEMA, listOf(0))

private fun rowOf(id: Long): ByteArray = RowCodec.encode(
    listOf(
        Datum.I64(id),
        Datum.Str("user-${id.toString().padStart(8, '0')}"),
        Datum.I64(20 + (id % 50)),
        Datum.Str(if (id % 3 == 0L) "active" else "idle"),
    )
)

private fun newLsm(
    store: ObjectStore,
    memBytes: Long = 32L * 1024 * 1024,
    /** 每个基准段落用自己的分片，避免上一段的数据污染这一段的计数与扫描。 */
    partition: Long = 0,
    blockBytes: Int = 64 * 1024,
): LsmStore {
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    return LsmStore(
        store = store,
        shard = ShardId(TableId(1), PartitionId(partition)),
        schema = SCHEMA,
        blockSize = blockBytes,
        cache = BlockCache(64L * 1024 * 1024), nodeId = "bench",
        scope = scope, commitMode = CommitMode.SYNC,
        memtableCapacity = memBytes,
        countColumns = setOf(3),
    )
}

private fun section(title: String) {
    println()
    println("── $title " + "─".repeat(maxOf(0, 74 - title.length)))
}

private fun pct(values: LongArray, p: Double): Long {
    if (values.isEmpty()) return 0
    val sorted = values.sortedArray()
    return sorted[((sorted.size - 1) * p).toInt()]
}

// ── 1. 点查 ────────────────────────────────────────────────────────

private suspend fun benchPointLookup(store: CountingObjectStore) {
    section("点查延迟（设计目标 §14.1：稳态 p50 ≤ 15ms，冷 p50 ≤ 60ms）")

    // 用 4KB 的块，强制数据分散到多个块上，才能测出真实的按需读取行为
    val lsm = newLsm(store, partition = 1, blockBytes = 4 * 1024)
    lsm.open()

    val n = 2_000
    println("准备 $n 行数据…")
    var ts = 1000L
    for (i in 1..n) {
        ts++
        lsm.write(WalBatch("t$i", ts, listOf(WalOp(keyOf(i.toLong()), rowOf(i.toLong())))))
    }
    lsm.flush()

    // 冷缓存：清空所有缓存后测量
    val cold = newLsm(store, partition = 1, blockBytes = 4 * 1024)
    cold.open()
    val coldSamples = LongArray(100)
    store.reset()
    for (i in 0 until 100) {
        val id = ((i * 17) % n + 1).toLong()
        val t = measureNanoTime { cold.get(keyOf(id), ReadTs(Long.MAX_VALUE)) }
        coldSamples[i] = t / 1000  // 微秒
    }
    val coldGets = store.count("get") + store.count("get_range")
    println("冷缓存:  p50=${pct(coldSamples, 0.50)}µs  p99=${pct(coldSamples, 0.99)}µs  " +
        "总请求=$coldGets (平均 ${"%.2f".format(coldGets / 100.0)} 次/查询)")

    // 稳态：数据块与索引都已在缓存里
    store.reset()
    val warmSamples = LongArray(1000)
    for (i in 0 until 1000) {
        val id = ((i * 13) % n + 1).toLong()
        val t = measureNanoTime { cold.get(keyOf(id), ReadTs(Long.MAX_VALUE)) }
        warmSamples[i] = t / 1000
    }
    val warmGets = store.count("get") + store.count("get_range")
    println("稳态:    p50=${pct(warmSamples, 0.50)}µs  p99=${pct(warmSamples, 0.99)}µs  " +
        "总请求=$warmGets (平均 ${"%.3f".format(warmGets / 1000.0)} 次/查询)")
    println("         ${store.report()}")

    lsm.close(); cold.close()
}

// ── 2. 写路径 ──────────────────────────────────────────────────────

private suspend fun benchWritePath(store: CountingObjectStore) {
    section("写路径（组提交，设计目标 §6.3.4）")

    val lsm = newLsm(store, partition = 2)
    lsm.open()
    store.reset()

    val n = 1_000
    val latencies = LongArray(n)
    var ts = 100_000L
    val startWall = System.nanoTime()
    for (i in 1..n) {
        ts++
        val t = measureNanoTime {
            lsm.write(WalBatch("w$i", ts, listOf(WalOp(keyOf(i.toLong()), rowOf(i.toLong())))))
        }
        latencies[i - 1] = t / 1000
    }
    val elapsedMs = (System.nanoTime() - startWall) / 1_000_000

    val puts = store.count("put") + store.count("put_cond")
    println("提交 $n 个事务，用时 ${elapsedMs}ms")
    println("提交延迟: p50=${pct(latencies, 0.50)}µs  p99=${pct(latencies, 0.99)}µs  " +
        "max=${latencies.maxOrNull()}µs")
    println("吞吐:     ${"%.0f".format(n * 1000.0 / elapsedMs)} TPS")
    println("WAL PUT:  $puts 次（组提交把 $n 个事务摊薄到 $puts 次 PUT，" +
        "平均 ${"%.2f".format(n.toDouble() / maxOf(1L, puts))} 事务/PUT）")
    println("         ${store.report()}")

    lsm.close()
}

// ── 2b. 并发写入（组提交真正生效的场景）────────────────────────────

private suspend fun benchConcurrentWrite(store: CountingObjectStore) {
    section("并发写入（组提交的真实收益）")

    val lsm = newLsm(store, partition = 3)
    lsm.open()
    store.reset()

    val writers = 16
    val perWriter = 200
    val total = writers * perWriter
    // 并发写者共享一个时间戳分配器，必须用原子量而不是 var（否则是数据竞争）
    val ts = java.util.concurrent.atomic.AtomicLong(500_000)

    val startWall = System.nanoTime()
    coroutineScope {
        val jobs = (0 until writers).map { w: Int ->
            async(Dispatchers.Default) {
                for (i in 0 until perWriter) {
                    val id = (w * perWriter + i + 1).toLong()
                    lsm.write(WalBatch("cw-$w-$i", ts.incrementAndGet(), listOf(WalOp(keyOf(id), rowOf(id)))))
                }
            }
        }
        jobs.forEach { it.await() }
    }
    val elapsedMs = (System.nanoTime() - startWall) / 1_000_000

    val puts = store.count("put") + store.count("put_cond")
    println("$writers 个并发写者 × $perWriter 次 = $total 个事务，用时 ${elapsedMs}ms")
    println("吞吐:      ${"%.0f".format(total * 1000.0 / elapsedMs)} TPS")
    println("WAL PUT:   $puts 次 → 平均 ${"%.2f".format(total.toDouble() / maxOf(1L, puts))} 事务/PUT")
    println("           （摊薄倍数越高，写入成本越低——PUT 在对象存储上按次计费）")
    println("           ${store.report()}")

    lsm.close()
}

// ── 3. COUNT 优化 ─────────────────────────────────────────────────

private suspend fun benchCountOptimization(store: CountingObjectStore) {
    section("COUNT 优化（设计目标 §8.9：0 次数据 GET）")

    val lsm = newLsm(store, partition = 4)
    lsm.open()

    val n = 3_000
    println("准备 $n 行…")
    var ts = 200_000L
    for (i in 1..n) {
        ts++
        lsm.write(WalBatch("c$i", ts, listOf(WalOp(keyOf(i.toLong()), rowOf(i.toLong())))))
    }
    lsm.flush()

    // 机制 1：manifest 精确行数
    store.reset()
    val t1 = measureNanoTime {
        val c = lsm.countAll(ReadTs(Long.MAX_VALUE))
        check(c == n.toLong()) { "COUNT 结果错误: $c != $n" }
    }
    println("COUNT(*) 全表:        ${t1 / 1000}µs  " +
        "对象存储请求=${store.totalOps()} 次  → ${if (store.totalOps() == 0L) "★ 0 GET" else store.report()}")

    // 机制 4：低基数列的块级值计数
    store.reset()
    val t2 = measureNanoTime {
        val c = lsm.countByColumnEquals(3, KeyEncoder.encodeDatum(Datum.Str("active"), ColumnType.VARCHAR), ReadTs(Long.MAX_VALUE))
        println("COUNT WHERE status=active: $c 行")
    }
    println("COUNT 带谓词:         ${t2 / 1000}µs  " +
        "对象存储请求=${store.totalOps()} 次  → ${if (store.totalOps() == 0L) "★ 0 GET" else store.report()}")

    // 对照：全表扫描
    store.reset()
    val t3 = measureNanoTime {
        val rows = lsm.scan(null, null, ReadTs(Long.MAX_VALUE))
        check(rows.size == n)
    }
    println("对照——全表扫描:       ${t3 / 1_000_000}ms  " +
        "对象存储请求=${store.totalOps()} 次")
    println("         加速比: ${"%.0f".format(t3.toDouble() / maxOf(1, t1))}×（延迟）")

    lsm.close()
}

// ── 4. 范围扫描 ────────────────────────────────────────────────────

private suspend fun benchRangeScan(store: CountingObjectStore) {
    section("范围扫描")

    val lsm = newLsm(store, partition = 5)
    lsm.open()
    val n = 5_000
    var ts = 300_000L
    for (i in 1..n) {
        ts++
        lsm.write(WalBatch("r$i", ts, listOf(WalOp(keyOf(i.toLong()), rowOf(i.toLong())))))
    }
    lsm.flush()

    for (size in listOf(100, 1000, 2500)) {
        store.reset()
        val start = (n / 2).toLong()
        val t = measureNanoTime {
            val rows = lsm.scan(keyOf(start), keyOf(start + size), ReadTs(Long.MAX_VALUE))
            check(rows.size == size) { "扫描结果数不对: ${rows.size} != $size" }
        }
        val gets = store.count("get") + store.count("get_range")
        println("$size 行: ${"%.2f".format(t / 1_000_000.0)}ms  " +
            "请求=$gets 次  吞吐=${"%.0f".format(size * 1e9 / t)} 行/秒  " +
            "(${"%.1f".format(gets.toDouble() / size * 1000)} 请求/千行)")
    }
    println("         ${store.report()}")
    lsm.close()
}

// ── 5. SST 体积构成 ────────────────────────────────────────────────

private fun benchSstSizes() {
    section("`.idx` 元数据开销（设计文档 §6.4 的索引/数据分离）")

    val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    val mem = com.cloudea.yangbase.objectstore.MemoryObjectStore()
    var ts = 400_000L
    val lsm = LsmStore(
        store = mem, shard = ShardId(TableId(1), PartitionId(6)), schema = SCHEMA,
        cache = BlockCache(8L * 1024 * 1024), nodeId = "b", scope = scope,
        countColumns = setOf(3),
    )
    runBlocking {
        lsm.open()
        val n = 20_000
        for (i in 1..n) {
            ts++
            lsm.write(WalBatch("s$i", ts, listOf(WalOp(keyOf(i.toLong()), rowOf(i.toLong())))))
        }
        lsm.flush()
        closeAfter(lsm, mem)
    }

    var dataBytes = 0L
    var indexBytes = 0L
    for (k in mem.keys()) {
        val size = runBlocking { mem.head(k).size }
        when {
            k.endsWith(".dat") -> dataBytes += size
            k.endsWith(".idx") -> indexBytes += size
        }
    }
    println("数据块(.dat): ${dataBytes / 1024} KiB")
    println("索引  (.idx): ${indexBytes / 1024} KiB")
    println("索引占比:     ${"%.2f".format(indexBytes * 100.0 / maxOf(1, dataBytes))}%  " +
        "← 这部分常驻内存，换来的是点查不用读索引")
}

/** 在 runBlocking 里关闭并统计 .dat/.idx 体积。 */
private suspend fun closeAfter(lsm: LsmStore, mem: com.cloudea.yangbase.objectstore.MemoryObjectStore) {
    // 关闭前先统计（close 会 flush，产生新文件）
}
