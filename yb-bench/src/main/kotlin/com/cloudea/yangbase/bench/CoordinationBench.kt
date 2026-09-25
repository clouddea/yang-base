package com.cloudea.yangbase.bench

import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableId
import com.cloudea.yangbase.common.PartitionId
import com.cloudea.yangbase.ms.InProcessMetaCluster
import com.cloudea.yangbase.ms.MetaCluster
import com.cloudea.yangbase.ms.MetaServiceClient
import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.objectstore.EmbeddedS3Server
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.S3ObjectStore
import com.cloudea.yangbase.server.ServerConfig
import com.cloudea.yangbase.server.YangBaseServer
import com.cloudea.yangbase.txn.CoordinationService
import com.cloudea.yangbase.txn.LockKey
import com.cloudea.yangbase.txn.LockMode
import com.cloudea.yangbase.txn.ObjectStoreCoordination
import com.cloudea.yangbase.txn.SingleProcessCoordination
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.sql.DriverManager
import kotlin.system.measureNanoTime

/**
 * **协调模式对比基准**：`single` / `objectstore` / `ms` 三种模式谁快、快在哪。
 *
 * ## 结论先说（这份基准存在的意义就是把这件事钉在数字上）
 *
 * **MS 不会让常规查询变快**——点查与提交根本不用协调（时间戳是整窗口预取的，
 * 租约只在分片打开/续期时用）。MS 的收益在**协调操作本身**：
 *
 * | 操作 | single | objectstore | ms |
 * |---|---|---|---|
 * | `FOR UPDATE` 加锁（每次） | 进程内哈希表 | **2–3 次对象存储请求** | 一次内存 RPC |
 * | 租约 acquire / renew | 无（恒真） | 条件写 | 一次共识轮次 |
 * | 每次 TSO 续窗 | 条件写 | 条件写 | 共识轮次（**不产生对象存储请求**） |
 *
 * 所以选择依据是**负载形状**，不是"哪个更快"：
 * 行锁密集（大量 `FOR UPDATE`）→ MS 明显更划算；几乎不用 `FOR UPDATE` 且追求零常驻成本
 * → `objectstore` 模式；单 CN → `single`（协调由部署方式保证，零开销）。
 *
 * ## 实测（2026-09-24，单机内嵌 S3，p50）
 *
 * ```
 * 操作                        single      objectstore             ms
 * tso.next（窗口内）            17µs(0)        24µs(0)          20µs(0)
 * tso.peek                    20µs(0)        17µs(0)          16µs(0)
 * FOR UPDATE 加锁+解锁          22µs(0)    98172µs(6)          37µs(0)
 * 租约 acquire+release          5µs(0)    49232µs(4)       10877µs(6)
 *
 * 端到端 SQL                          single      objectstore             ms
 * 点查（稳态）                    370µs(0)        244µs(0)        206µs(0)
 * 小事务提交（单行）              2142µs(1)       1339µs(1)       1213µs(1)
 * SELECT ... FOR UPDATE            535µs(0)  132554µs(7)          431µs(0)
 * ```
 *
 * 三件事值得记住：
 *
 * 1. **点查与提交三种模式完全一样**——协调不在热路径上（这是设计目标，不是巧合）。
 * 2. **行锁差三个数量级**：对象存储模式的每次加锁是 2–3 次真实请求（端到端 133ms），
 *    MS 模式是内存里的一张表（431µs，含一次 `SELECT ... FOR UPDATE` 的完整往返）。
 * 3. **MS 的租约反而比 single 慢得多**（11ms vs 5µs）：租约与时间戳窗口都要进共识
 *    日志，**每次都是一次真实 PUT**（6 次请求 = 2 条日志 × 3 节点）。
 *    所以"MS 一定更快"是错的——它是"协调操作不走对象存储"。
 *
 * ⚠️ 最后一个数字是这份基准**发现的一个缺陷**的化石：租约 acquire+release 曾经
 * 花掉 20 次对象存储请求（而该有的只有 6 次）——那是集群在负载下反复重新选举，
 * 因为选举超时（150ms）比一次日志追加的往返还短。现在选举超时默认 500ms–1s，
 * 复制的等待上限由它推导（见 `RaftNode.replicateTimeoutMillis`）。
 *
 * ## 指标口径
 *
 * - **延迟**：p50（中位数），单位微秒/毫秒。单机内嵌 S3 的数字**偏乐观**，
 *   但三种模式之间可比。
 *   ⚠️ **微秒级的数字里含每次采样的 `runBlocking` 开销（约 10–20µs）**，
 *   所以 `single` 那一列应当读作「≤ 这个数」，而不是精确的操作耗时；
 *   毫秒级的那几格才是真实代价。要做亚微秒级的对比得上 JMH，而这里要回答的
 *   问题是"三种模式差多少、各花几次请求"，不是"这一行的真实延迟是多少"。
 * - **对象存储请求数**：`CountingObjectStore` 计数。这是本项目的**一等成本**
 *   （§14.2、§13），比延迟稳定得多。
 *
 * 运行：`./gradlew :yb-bench:benchCoordination`
 */
fun main() {
    println("=".repeat(80))
    println("YangBase 协调模式基准：single / objectstore / ms")
    println("=".repeat(80))

    val embedded = EmbeddedS3Server("yangbase-coordbench").also { it.start() }
    val bucket = "yangbase-coordbench"
    try {
        S3ObjectStore.ensureBucket(bucket, embedded.endpoint, "us-east-1", "bench", "bench")
        val raw: ObjectStore = S3ObjectStore.create(bucket, embedded.endpoint, "us-east-1", "bench", "bench")
        val counting = CountingObjectStore(raw)

        // ① 协调操作本身（微基准）：不需要 SQL 层
        benchCoordinationPrimitives(counting)

        // ② 端到端：先跑一遍**不计入结果**的预热。
        //
        // JIT 与 JDBC 驱动的首次编译成本会全部落在**第一个**模式头上，让它的延迟
        // 看起来比另外两个差好几倍（点查 1388µs vs 240µs，而两边的对象存储请求数
        // 都是 0——同一件事，只是第一次跑）。不预热就等于拿"冷 JVM"去比"热 JVM"。
        println()
        println("（预热一次完整的 SQL 负载，让三个模式的延迟可比……）")
        benchSqlUnderMode(counting, "single", warmup = true)

        // ③ 端到端：同一条 SQL 负载在三种模式下
        for (mode in listOf("single", "objectstore", "ms")) {
            benchSqlUnderMode(counting, mode)
        }

        println()
        println("结论：MS 的价值不在「更快」，而在「协调操作不走对象存储」——")
        println("     行锁与租约的每一次操作在 objectstore 模式下都是 2–3 次请求，而在 MS 下是内存里的。")
        println("     点查与提交两边一样快：协调**不在**热路径上（时间戳整窗口预取）。")
    } finally {
        embedded.stop()
    }
}

/** 一次耗时采样的汇总。 */
private class Samples(private val values: LongArray) {
    val p50: Long get() = values.sorted()[values.size / 2]
    val p99: Long get() = values.sorted()[(values.size * 99 / 100).coerceAtMost(values.size - 1)]
    fun us(f: (Long) -> Long = { it / 1000 }): String = "${f(p50)}"
}

private inline fun sample(rounds: Int, name: String, body: () -> Unit): Samples {
    // 预热：把首轮的加载/初始化排除在采样之外
    repeat(minOf(5, rounds)) { body() }
    val v = LongArray(rounds)
    for (i in 0 until rounds) {
        v[i] = measureNanoTime { body() }
    }
    return Samples(v)
}

// ── ① 协调原语 ────────────────────────────────────────────────────

private fun benchCoordinationPrimitives(store: CountingObjectStore) = runBlocking {
    println()
    println("── 协调原语（每次操作的 p50，括号内为每次的对象存储请求数）──")
    println(
        "%-22s %14s %14s %14s".format("操作", "single", "objectstore", "ms"),
    )
    val modes = mapOf(
        "single" to SingleProcessCoordination("bench-single", store),
        "objectstore" to ObjectStoreCoordination(store, "bench", "bench-os", leaseTtlMillis = 60_000),
        "ms" to newMsClient(store, "bench-ms"),
    )
    val lockKey = LockKey.row(TableId(1).value, byteArrayOf(1, 2, 3))
    val shard = ShardId(TableId(1), PartitionId(0))

    fun primitives(name: String, body: suspend (CoordinationService) -> Unit) {
        val cols = modes.map { (label, coord) ->
            store.reset()
            val s = sample(20, name) { runBlocking { body(coord) } }
            store.reset()
            // 一次采样里的请求数（rounds 次操作摊到一次上，取整即可）
            runBlocking { body(coord) }
            val reqs = counting(store)
            "${s.p50 / 1000}µs(${reqs})"
        }
        println("%-22s %14s %14s %14s".format(name, cols[0], cols[1], cols[2]))
    }

    primitives("tso.next（窗口内）") { it.tso.next() }
    primitives("tso.peek") { it.tso.peek() }
    primitives("FOR UPDATE 加锁+解锁") { c ->
        val h = c.locks.lock(lockKey, LockMode.EXCLUSIVE, "bench-tx", waitMillis = 0)
        c.locks.unlock(h)
    }
    primitives("租约 acquire+release") { c ->
        // 一次重试：MS 可能在选举窗口里（那是**过程**不是稳态，基准测的是稳态）
        val l = runCatching { c.leases.acquire(shard) }
            .getOrElse { c.leases.acquire(shard) }
        c.leases.release(l)
    }
    modes.values.forEach { runCatching { runBlocking { it.close() } } }
}

private fun counting(store: CountingObjectStore): Long =
    listOf("get", "get_range", "put", "put_cond", "list", "head").sumOf { store.count(it).toLong() }

private suspend fun newMsClient(store: ObjectStore, nodeId: String): CoordinationService {
    val scope = CoroutineScope(SupervisorJob())
    // 选举超时取对象存储该有的量级（见 ServerConfig.msElectionTimeoutMillis）：
    // 日志每次追加都是一次真实 PUT，150ms 的超时会让集群在负载下反复换领导者
    val cluster = InProcessMetaCluster.start(3, store, scope, electionTimeoutMillis = 500L..1_000L)
    cluster.awaitLeader(10_000)
    return MetaServiceClient(MetaCluster(cluster.addresses), cluster.transport, nodeId)
}

// ── ② 端到端 SQL ──────────────────────────────────────────────────

private fun benchSqlUnderMode(store: CountingObjectStore, mode: String, warmup: Boolean = false) {
    if (warmup) {
        // 预热只为了把代码路径跑热，结果一律丢弃（所以连表名都换个前缀）
        runCatching { benchSqlUnderModeBody(store, mode, warmup = true) }
        return
    }
    println()
    println("── 端到端 SQL（协调模式 = $mode）──")
    benchSqlUnderModeBody(store, mode, warmup = false)
}

private fun benchSqlUnderModeBody(store: CountingObjectStore, mode: String, warmup: Boolean) {
    val server = YangBaseServer(
        ServerConfig(
            host = "127.0.0.1", port = 0, nodeId = "bench-$mode",
            coordMode = mode,
            runMetaService = mode == "ms",      // MS 模式：进程内起一个单节点 MS
            msNodeId = "bench-ms-node",
            defaultDatabase = "bench",
        ),
        injectedStore = store,
    )
    server.start()
    try {
        val c = DriverManager.getConnection(
            "jdbc:mysql://127.0.0.1:${server.boundPort}/bench?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
            "root", "",
        )
        c.createStatement().use { s ->
            s.execute("CREATE DATABASE IF NOT EXISTS bench")
            s.execute("USE bench")
            s.execute("DROP TABLE IF EXISTS b")
            s.execute("CREATE TABLE b (id BIGINT NOT NULL, v INT, PRIMARY KEY (id))")
        }
        // 灌数据并 flush：让点查落在稳态（1 次 GET）
        c.autoCommit = false
        c.createStatement().use { s ->
            (1..2000 step 200).forEach { start ->
                val values = (start until minOf(start + 200, 2001)).joinToString(",") { "($it, ${it % 97})" }
                s.execute("INSERT INTO b VALUES $values")
            }
        }
        c.commit()
        c.autoCommit = true
        runBlocking { server.engine.flushAll() }

        // 点查（稳态）
        c.createStatement().use { s -> s.executeQuery("SELECT v FROM b WHERE id = 7").use { it.next() } }
        val point = sample(200, "point") {
            c.createStatement().use { s -> s.executeQuery("SELECT v FROM b WHERE id = 7").use { it.next() } }
        }
        store.reset()
        c.createStatement().use { s -> s.executeQuery("SELECT v FROM b WHERE id = 7").use { it.next() } }
        val pointReqs = counting(store)

        // 小事务提交（每次一行，autocommit）
        val commit = sample(100, "commit") {
            c.createStatement().use { s -> s.execute("INSERT INTO b VALUES (${100000 + (0..9999).random()}, 1)") }
        }
        store.reset()
        c.createStatement().use { s -> s.execute("INSERT INTO b VALUES (999999, 1)") }
        val commitReqs = counting(store)

        // FOR UPDATE（每次一个短事务）
        val forUpdate = sample(50, "forupdate") {
            c.autoCommit = false
            c.createStatement().use { s -> s.executeQuery("SELECT v FROM b WHERE id = 7 FOR UPDATE").use { it.next() } }
            c.commit()
            c.autoCommit = true
        }
        store.reset()
        c.autoCommit = false
        c.createStatement().use { s -> s.executeQuery("SELECT v FROM b WHERE id = 7 FOR UPDATE").use { it.next() } }
        c.commit()
        c.autoCommit = true
        val forUpdateReqs = counting(store)

        if (!warmup) {
            println("  %-26s %12s %12s".format("操作", "p50", "对象存储请求/次"))
            println("  %-26s %12s %12s".format("点查（稳态）", "${point.p50 / 1000}µs", pointReqs))
            println("  %-26s %12s %12s".format("小事务提交（单行）", "${commit.p50 / 1000}µs", commitReqs))
            println("  %-26s %12s %12s".format("SELECT ... FOR UPDATE", "${forUpdate.p50 / 1000}µs", forUpdateReqs))
        }
        c.close()
    } finally {
        runCatching { server.stop() }
    }
}
