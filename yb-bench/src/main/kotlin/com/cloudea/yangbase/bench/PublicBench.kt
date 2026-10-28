package com.cloudea.yangbase.bench

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.ShardId
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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.File
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import kotlin.system.measureNanoTime

/**
 * 公开基准（public benchmark）。
 *
 * 与 `BenchMain`（人读的 ASCII 报告）的区别：这里输出**机器可比的 JSON**，
 * 由 `scripts/bench-compare.py` 与上一版基线逐项对比，用于 PR 上的防回退门禁。
 *
 * 两条口径上的硬规矩：
 *
 * 1. **请求数是一等指标，延迟是二等指标。**
 *    请求数完全确定（同样的代码同样的数据集必然同样的请求数），可以硬门禁；
 *    延迟受机器噪声支配，只能告警。把噪声指标做成硬失败，最后一定是把门禁关掉。
 * 2. **缓存命中与未命中必须分栏报告。**
 *    「点查 0 次 GET」只在缓存命中时成立；把命中数据当性能对外公布是自欺欺人。
 *    未命中一栏才是对象存储后端真正的成本。
 *
 * 运行：`./gradlew :yb-bench:benchPublic -PYB_BENCH_PROFILE=ci -PYB_BENCH_OUT=bench/current.json`
 */
fun main(args: Array<String>) {
    val profile = Profile.of(argString(args, "--profile") ?: System.getProperty("YB_BENCH_PROFILE"))
    val outPath = argString(args, "--out") ?: System.getProperty("YB_BENCH_OUT") ?: "bench/current.json"
    val label = argString(args, "--label") ?: System.getProperty("YB_BENCH_LABEL") ?: "local"

    val external = System.getProperty("YB_S3_ENDPOINT") ?: System.getenv("YB_S3_ENDPOINT")
    val embedded = if (external == null) EmbeddedS3Server("yangbase-bench").also { it.start() } else null
    val endpoint = external ?: embedded!!.endpoint
    val bucket = System.getProperty("YB_S3_BUCKET") ?: System.getenv("YB_S3_BUCKET") ?: "yangbase-bench"
    val ak = System.getProperty("YB_S3_ACCESS_KEY") ?: System.getenv("YB_S3_ACCESS_KEY") ?: "bench"
    val sk = System.getProperty("YB_S3_SECRET_KEY") ?: System.getenv("YB_S3_SECRET_KEY") ?: "bench"

    val backend = if (external != null) "external" else "embedded-s3"

    val metrics = mutableListOf<Metric>()
    try {
        if (external != null) S3ObjectStore.ensureBucket(bucket, endpoint, "us-east-1", ak, sk)
        val raw: ObjectStore = S3ObjectStore.create(
            bucket = bucket, endpoint = endpoint, region = "us-east-1",
            accessKey = ak, secretKey = sk,
        )
        runBlocking {
            val counting = CountingObjectStore(raw)
            val ds = profile
            println("profile=${profile.name} rows=${profile.rows} backend=$backend profile-file=$outPath")

            // 点查与范围扫描各用自己的分片，两片装载同一份确定性数据
            // 先量一遍后端本身：把「内嵌 S3 服务器每次请求的固定开销」和
            // 「YangBase 自己的开销」分开，否则冷点查的数字会被夹具主导而不自知。
            backendProbe(counting, metrics)
            prepare(counting, ds, 1)
            prepare(counting, ds, 2)
            pointLookup(counting, ds, metrics)
            rangeScan(counting, ds, metrics)
            writePath(counting, ds, metrics)
            countPath(counting, ds, metrics)
            footprint(ds, metrics)
        }
    } finally {
        embedded?.stop()
    }

    val report = BenchReport(
        schema = "yangbase-bench/1",
        label = label,
        commit = gitCommit(),
        version = version(),
        jdk = System.getProperty("java.version"),
        backend = backend,
        profile = profile.name,
        startedAt = Instant.now().toString(),
        dataset = DatasetInfo(
            rows = profile.rows,
            seed = Dataset.SEED,
            blockBytes = Profile.BLOCK_BYTES,
            rowBytes = rowOf(1).size,
            description = "id BIGINT PK + name VARCHAR(64) + age INT + status VARCHAR(16)，" +
                "键 1..N，值由键确定性推导（无随机）",
        ),
        metrics = metrics.sortedWith(compareBy({ it.workload }, { it.cache })),
    )

    val json = Json { prettyPrint = true; encodeDefaults = true }
    val text = json.encodeToString(report)
    File(outPath).apply { parentFile?.mkdirs() }.writeText(text + "\n")
    println()
    println(json.encodeToString(report))
    println("已写入 $outPath")
}

private val DEBUG = System.getProperty("YB_BENCH_DEBUG") != null

private fun argString(args: Array<String>, name: String): String? =
    args.firstOrNull { it.startsWith("$name=") }?.substringAfter("=")

private fun gitCommit(): String = try {
    ProcessBuilder("git", "rev-parse", "--short", "HEAD")
        .directory(File(".").absoluteFile).redirectErrorStream(true).start()
        .inputStream.bufferedReader().readText().trim()
} catch (_: Exception) {
    "unknown"
}

private fun version(): String = try {
    File("version.txt").readText().trim()
} catch (_: Exception) {
    "unknown"
}

// ── 数据模型 ───────────────────────────────────────────────────────

/**
 * 基准档位。`ci` 档不是「跑得完就行」的敷衍版本：
 * **请求数是确定性的，样本量减少不改变它**，所以 PR 门禁照样守得住；
 * 只有延迟分位数变粗，而延迟本来就不做硬门禁。
 */
private data class Profile(
    val name: String,
    val rows: Int,
    /** 冷点查样本数：每个样本都用**全新的 LsmStore（空缓存）**，因此每次恰好 1 次 GET。 */
    val coldSamples: Int,
    val pointSamples: Int,
    val scanSamples: Int,
    val writeTxns: Int,
    val writers: Int,
    val perWriter: Int,
) {
    companion object {
        const val BLOCK_BYTES = 4 * 1024

        fun of(name: String?): Profile = when (name?.lowercase()) {
            null, "", "default", "full" -> Profile("default", 20_000, 50, 500, 3, 1_000, 16, 200)
            "ci", "short" -> Profile("ci", 5_000, 20, 200, 2, 400, 8, 50)
            else -> error("未知档位: $name（可选 default / ci）")
        }
    }
}

/**
 * 确定性数据集。**不用任何随机数**：值完全由键推导，
 * 换机器、换 JDK、换时间都得到同一份数据——否则请求数与字节数不可比。
 */
private object Dataset {
    const val SEED = 42L
}

private fun schema(): TableSchema = TableSchema(
    tableId = 1, databaseId = 1, name = "bench", version = 1,
    columns = listOf(
        ColumnDef("id", ColumnType.BIGINT, nullable = false),
        ColumnDef("name", ColumnType.VARCHAR, length = 64),
        ColumnDef("age", ColumnType.INT),
        ColumnDef("status", ColumnType.VARCHAR, length = 16),
    ),
    primaryKey = listOf("id"),
)

private val SCHEMA = schema()

/** 每个负载用自己的分片，避免上一段的数据污染这一段的请求计数。 */
private fun shard(p: Int) = ShardId(TableId(1), PartitionId(p.toLong()))

private fun keyOf(id: Long) = KeyEncoder.encodeKey(listOf(Datum.I64(id)), SCHEMA, listOf(0))

private fun rowOf(id: Long): ByteArray = RowCodec.encode(
    listOf(
        Datum.I64(id),
        Datum.Str("user-${id.toString().padStart(8, '0')}"),
        Datum.I64(20 + (id % 50)),
        Datum.Str(if (id % 3 == 0L) "active" else "idle"),
    )
)

/**
 * 确定性采样：线性同余，参数固定。
 * 用它而不是 `Random(seed)` —— JDK 的 Random 实现属于 JDK 内部，
 * 换 JDK 版本就可能换序列，那基线就不可比了。
 */
private class Lcg(seed: Long) {
    private var s = seed
    fun next(bound: Int): Int {
        s = (s * 6364136223846793005L + 1442695040888963407L)
        return ((s ushr 33).toInt() and Int.MAX_VALUE) % bound
    }
}

private fun newLsm(store: ObjectStore, partition: Int, cacheBytes: Long = 64L * 1024 * 1024) = LsmStore(
    store = store,
    shard = shard(partition),
    schema = SCHEMA,
    blockSize = Profile.BLOCK_BYTES,
    cache = BlockCache(cacheBytes),
    nodeId = "bench",
    scope = CoroutineScope(SupervisorJob() + Dispatchers.Default),
    commitMode = CommitMode.SYNC,
    memtableCapacity = 32L * 1024 * 1024,
    countColumns = setOf(3),
)

/** 把确定性数据集灌进一个分片并 flush 成一个 SST，然后关掉（后续重新打开即「冷」）。 */
private suspend fun prepare(store: CountingObjectStore, ds: Profile, partition: Int) {
    val lsm = newLsm(store, partition)
    lsm.open()
    store.reset()
    var ts = 1_000L
    // 装载用批量提交（每批 500 行）而不是逐行提交：装载不是被测对象，
    // 逐行 1 次 PUT 会让 2 万行的装载期变成几分钟，CI 里不可接受。
    var i = 1
    while (i <= ds.rows) {
        val ops = ArrayList<WalOp>(500)
        var k = 0
        while (k < 500 && i <= ds.rows) {
            ops += WalOp(keyOf(i.toLong()), rowOf(i.toLong()))
            i++; k++
        }
        ts++
        lsm.write(WalBatch("seed-$i", ts, ops))
    }
    lsm.flush()
    println("数据装载完成：分片 $partition / ${ds.rows} 行，装载阶段请求：${store.report()}")
    lsm.close()
}

/**
 * 阶段进度打到 stderr。
 *
 * 不打印阶段的话，长跑卡住时**无法判断卡在哪一段**——2026-10-28 的 default 档
 * 就出现过「所有线程空闲、main 在 runBlocking 里等」的死等，最后只能靠猜。
 */
private fun phase(name: String) {
    System.err.println("[bench] phase=$name")
    System.err.flush()
}

// ── 负载 0：后端基线（夹具成本，不是引擎成本）────────────────────

/**
 * 直接对对象存储做 PUT / GET，不经过 LSM 任何一层。
 *
 * 存在意义：冷点查的延迟里有多少是**夹具**（内嵌 S3 服务器 + AWS SDK 客户端）
 * 的固定开销，多少是引擎的？不单独量一下就只能靠猜，而猜出来的数字会被
 * 当成引擎性能写进文档。这一行也顺便给出「1 次 GET 是多少微秒」的下限，
 * 任何优化都不可能低于它。
 */
private suspend fun backendProbe(store: CountingObjectStore, out: MutableList<Metric>) {
    phase("backend-raw")
    val putSamples = 20
    val getSamples = 20
    val payload = ByteArray(4096) { (it % 251).toByte() }

    val put = LongArray(putSamples)
    store.reset()
    for (i in 0 until putSamples) {
        put[i] = measureNanoTime { store.put("probe/raw-$i", payload, null) } / 1000
    }
    out += metric("backend-raw", "put", putSamples, put, store)

    val get = LongArray(getSamples)
    store.reset()
    for (i in 0 until getSamples) {
        get[i] = measureNanoTime { checkNotNull(store.get("probe/raw-${i % putSamples}", null)) } / 1000
    }
    out += metric("backend-raw", "get", getSamples, get, store)

    // 清理：不让探针对象混进后面存储足迹的统计
    for (i in 0 until putSamples) store.delete("probe/raw-$i")
}

// ── 负载 1：点查 ───────────────────────────────────────────────────

private suspend fun pointLookup(store: CountingObjectStore, ds: Profile, out: MutableList<Metric>) {
    phase("point-lookup")
    val keys = IntArray(ds.pointSamples) { 1 + Lcg(Dataset.SEED + it).next(ds.rows) }

    // 未命中（严格口径）：**每个样本都新建一个 LsmStore**，BlockCache 是空的，
    // 因此这一行测的是「一次真正的冷点查要打几次对象存储」。设计目标是 1 次 GET，
    // 这里就是那条不变式的可执行版本——请求数是确定性的，可以直接当门禁。
    // 反过来，如果用一个 store 连读 N 个键，后面的键会吃到前面读进来的块，
    // 得到的 0.x 次/查询什么都说明不了。
    val cold = IntArray(ds.coldSamples) { 1 + Lcg(Dataset.SEED + 7919L + it).next(ds.rows) }
    val miss = LongArray(ds.coldSamples)
    // ⚠️ 每次 open 之后都要 reset（计数从这一刻开始），但**累计**请求数而不是覆盖：
    // 早先的写法在循环里 reset 后只读最后一次的快照，得到的「0.05 次/查询」其实是
    // 「最后一个样本 1 次请求」——差了一个数量级，而且看起来像是缓存泄漏。
    var coldOps = 0L
    var coldBytes = 0L
    for ((i, id) in cold.withIndex()) {
        val fresh = newLsm(store, 1)
        fresh.open()
        store.reset()
        miss[i] = measureNanoTime { checkNotNull(fresh.get(keyOf(id.toLong()), ReadTs(Long.MAX_VALUE))) } / 1000
        coldOps += store.totalOps()
        coldBytes += outBytesTotal(store)
        if (DEBUG) println("  cold[$i] id=$id ops=${store.totalOps()} t=${miss[i]}us")
        fresh.close()
    }
    out += metric("point-lookup", "miss", ds.coldSamples, miss, store, ops = coldOps, bytes = coldBytes)

    // 命中：稳态（索引常驻 + 数据块已在缓存）
    val lsm = newLsm(store, 1)
    lsm.open()
    store.reset()
    val warmup = LongArray(ds.pointSamples)
    for ((i, id) in keys.withIndex()) {
        val t = measureNanoTime { checkNotNull(lsm.get(keyOf(id.toLong()), ReadTs(Long.MAX_VALUE))) }
        warmup[i] = t / 1000
    }
    store.reset()
    val hit = LongArray(ds.pointSamples)
    for ((i, id) in keys.withIndex()) {
        val t = measureNanoTime { checkNotNull(lsm.get(keyOf(id.toLong()), ReadTs(Long.MAX_VALUE))) }
        hit[i] = t / 1000
    }
    out += metric("point-lookup", "hit", ds.pointSamples, hit, store)
    lsm.close()
}

// ── 负载 2：范围扫描 ───────────────────────────────────────────────

private suspend fun rangeScan(store: CountingObjectStore, ds: Profile, out: MutableList<Metric>) {
    phase("range-scan")
    val rowsPerScan = ds.rows / 2

    var lsm = newLsm(store, 2)
    lsm.open()
    store.reset()
    val miss = LongArray(ds.scanSamples)
    for (i in 0 until ds.scanSamples) {
        val start = 1L + (ds.rows / 4L) * i / maxOf(1, ds.scanSamples)
        val t = measureNanoTime {
            val rows = lsm.scan(keyOf(start), keyOf(start + rowsPerScan), ReadTs(Long.MAX_VALUE))
            check(rows.isNotEmpty()) { "扫描结果为空" }
        }
        miss[i] = t / 1000
    }
    out += metric("range-scan", "miss", rowsPerScan, miss, store, unit = "row")
    lsm.close()

    lsm = newLsm(store, 2)
    lsm.open()
    // 预热一遍（同一区间），再测量
    lsm.scan(keyOf(1), keyOf(1L + rowsPerScan), ReadTs(Long.MAX_VALUE))
    store.reset()
    val hit = LongArray(ds.scanSamples)
    for (i in 0 until ds.scanSamples) {
        val start = 1L + (ds.rows / 4L) * i / maxOf(1, ds.scanSamples)
        val t = measureNanoTime {
            val rows = lsm.scan(keyOf(start), keyOf(start + rowsPerScan), ReadTs(Long.MAX_VALUE))
            check(rows.isNotEmpty()) { "扫描结果为空" }
        }
        hit[i] = t / 1000
    }
    out += metric("range-scan", "hit", rowsPerScan, hit, store, unit = "row")
    lsm.close()
}

// ── 负载 3：写入 ───────────────────────────────────────────────────

private suspend fun writePath(store: CountingObjectStore, ds: Profile, out: MutableList<Metric>) {
    phase("write")
    // 单写者：测组提交把 N 个事务摊成多少次 PUT
    var lsm = newLsm(store, 3)
    lsm.open()
    store.reset()
    val single = LongArray(ds.writeTxns)
    var ts = 1_000_000L
    for (i in 0 until ds.writeTxns) {
        ts++
        val id = (i + 1).toLong()
        val t = measureNanoTime {
            lsm.write(WalBatch("w-$i", ts, listOf(WalOp(keyOf(id), rowOf(id)))))
        }
        single[i] = t / 1000
    }
    out += metric("write", "single-writer", ds.writeTxns, single, store, unit = "txn")
    lsm.close()

    // 并发写者：组提交真正生效的场景
    lsm = newLsm(store, 4)
    lsm.open()
    store.reset()
    val total = ds.writers * ds.perWriter
    val sharedTs = AtomicLong(2_000_000L)
    val wall = measureNanoTime {
        runBlocking {
            coroutineScope {
                (0 until ds.writers).map { w ->
                    async(Dispatchers.Default) {
                        for (i in 0 until ds.perWriter) {
                            val id = (w * ds.perWriter + i + 1).toLong()
                            lsm.write(
                                WalBatch("cw-$w-$i", sharedTs.incrementAndGet(), listOf(WalOp(keyOf(id), rowOf(id)))),
                            )
                        }
                    }
                }.awaitAll()
            }
        }
    }
    out += Metric(
        workload = "write",
        cache = "concurrent-${ds.writers}w",
        unit = "txn",
        count = total,
        latencyUs = Latency(0, 0, 0, 0),
        throughputPerSec = total * 1e9 / wall,
        requests = store.snapshot(),
        requestsPerOp = store.totalOps().toDouble() / total,
        bytesPerOp = outBytesTotal(store).toDouble() / total,
    )
    lsm.close()
}

// ── 负载 4：COUNT(*) ──────────────────────────────────────────────

private suspend fun countPath(store: CountingObjectStore, ds: Profile, out: MutableList<Metric>) {
    phase("count-star")
    val lsm = newLsm(store, 5)
    lsm.open()
    // 用同规模数据（count 的正确性依赖 manifest 行数，与分片内容无关，但规模要一致）
    var ts = 3_000_000L
    store.reset()
    for (i in 1..ds.rows) {
        ts++
        lsm.write(WalBatch("c-$i", ts, listOf(WalOp(keyOf(i.toLong()), rowOf(i.toLong())))))
    }
    lsm.flush()
    store.reset()

    val countMiss = LongArray(1)
    countMiss[0] = measureNanoTime {
        val c = lsm.countAll(ReadTs(Long.MAX_VALUE))
        check(c == ds.rows.toLong()) { "COUNT(*) 结果错误: $c != ${ds.rows}" }
    } / 1000
    out += metric("count-star", "miss", 1, countMiss, store, unit = "query")

    store.reset()
    val countHit = LongArray(1)
    countHit[0] = measureNanoTime {
        val c = lsm.countAll(ReadTs(Long.MAX_VALUE))
        check(c == ds.rows.toLong()) { "COUNT(*) 结果错误: $c" }
    } / 1000
    out += metric("count-star", "hit", 1, countHit, store, unit = "query")

    // 对照：全表扫描（这是「不做计数索引」的代价）
    store.reset()
    val scanOnce = LongArray(1)
    scanOnce[0] = measureNanoTime {
        val rows = lsm.scan(null, null, ReadTs(Long.MAX_VALUE))
        check(rows.size == ds.rows) { "全表扫描行数错误: ${rows.size}" }
    } / 1000
    out += metric("full-scan", "baseline", ds.rows, scanOnce, store, unit = "row")

    lsm.close()
}

// ── 存储足迹（OKR 2.9 的基线）──────────────────────────────────────

private suspend fun footprint(ds: Profile, out: MutableList<Metric>) {
    phase("storage")
    // 用内存后端单独测：体积与对象存储后端无关（同一份编码），
    // 但内存后端可以一次拿全键，不会把请求数混进别的负载。
    val mem = com.cloudea.yangbase.objectstore.MemoryObjectStore()
    val lsm = newLsm(mem, 6)
    lsm.open()
    var ts = 4_000_000L
    for (i in 1..ds.rows) {
        ts++
        lsm.write(WalBatch("f-$i", ts, listOf(WalOp(keyOf(i.toLong()), rowOf(i.toLong())))))
    }
    lsm.flush()
    lsm.close()

    var dataBytes = 0L
    var indexBytes = 0L
    for (k in mem.keys()) {
        val size = mem.head(k).size
        when {
            k.endsWith(".dat") -> dataBytes += size
            k.endsWith(".idx") -> indexBytes += size
        }
    }
    if (dataBytes == 0L) return
    out += Metric(
        workload = "storage",
        cache = "on-disk",
        unit = "byte",
        count = ds.rows,
        latencyUs = Latency(0, 0, 0, 0),
        throughputPerSec = 0.0,
        requests = emptyMap(),
        requestsPerOp = 0.0,
        bytesPerOp = (dataBytes + indexBytes).toDouble() / ds.rows,
        extra = mapOf(
            "dataBytes" to dataBytes.toDouble(),
            "indexBytes" to indexBytes.toDouble(),
            "indexRatio" to indexBytes.toDouble() / maxOf(1L, dataBytes),
            "bytesPerRow" to (dataBytes + indexBytes).toDouble() / ds.rows,
            "dataBytesPerRow" to dataBytes.toDouble() / ds.rows,
        ),
    )
}

// ── 指标装配 ───────────────────────────────────────────────────────

private fun metric(
    workload: String,
    cache: String,
    count: Int,
    samplesUs: LongArray,
    store: CountingObjectStore,
    unit: String = "op",
    /** 覆盖累计值（循环内多次 reset 时用；默认取 store 当前快照）。 */
    ops: Long = -1,
    bytes: Long = -1,
): Metric = Metric(
    workload = workload,
    cache = cache,
    unit = unit,
    count = count,
    latencyUs = Latency(
        p50 = pct(samplesUs, 0.50),
        p95 = pct(samplesUs, 0.95),
        p99 = pct(samplesUs, 0.99),
        max = samplesUs.maxOrNull() ?: 0,
    ),
    throughputPerSec = count * 1_000_000.0 / maxOf(1L, samplesUs.sum()),
    requests = if (ops >= 0) mapOf("total" to ops) else store.snapshot(),
    requestsPerOp = (if (ops >= 0) ops else store.totalOps()).toDouble() / count,
    bytesPerOp = (if (bytes >= 0) bytes else outBytesTotal(store)).toDouble() / count,
)

/** 出流量总和（读类操作实际传输的字节）。 */
private fun outBytesTotal(store: CountingObjectStore): Long =
    store.snapshot().keys.sumOf { store.bytesOut(it) }

private fun pct(values: LongArray, p: Double): Long {
    if (values.isEmpty()) return 0
    val sorted = values.sortedArray()
    return sorted[((sorted.size - 1) * p).toInt()]
}

// ── JSON 模型 ─────────────────────────────────────────────────────

@Serializable
data class Latency(val p50: Long, val p95: Long, val p99: Long, val max: Long)

@Serializable
data class Metric(
    val workload: String,
    val cache: String,
    val unit: String,
    val count: Int,
    val latencyUs: Latency,
    val throughputPerSec: Double,
    val requests: Map<String, Long>,
    /** ★ 一等指标：每次操作平均产生的对象存储请求数。确定性，可硬门禁。 */
    val requestsPerOp: Double,
    val bytesPerOp: Double,
    val extra: Map<String, Double> = emptyMap(),
)

@Serializable
data class DatasetInfo(
    val rows: Int,
    val seed: Long,
    val blockBytes: Int,
    val rowBytes: Int,
    val description: String,
)

@Serializable
data class BenchReport(
    val schema: String,
    val label: String,
    val commit: String,
    val version: String,
    val jdk: String,
    val backend: String,
    val profile: String,
    val startedAt: String,
    val dataset: DatasetInfo,
    val metrics: List<Metric>,
)
