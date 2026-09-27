package com.cloudea.yangbase.bench

import com.cloudea.yangbase.objectstore.CountingObjectStore
import com.cloudea.yangbase.server.YangBaseServer
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.util.Locale
import kotlin.math.ceil

/** SQL → 优化器 → 持久化索引的性能门禁，延迟仅报告，正确性和 IO 减少才决定通过。 */
fun main() {
    val rows = System.getProperty("YB_INDEX_ROWS", "6000").toInt().also { require(it >= 4000 && it % 4 == 0) }
    val samples = System.getProperty("YB_INDEX_SAMPLES", "3").toInt().also { require(it >= 1) }
    val report = Path.of(System.getProperty("YB_INDEX_REPORT", "build/reports/index-bench/results.tsv"))
    Files.createDirectories(report.toAbsolutePath().parent)
    val output = mutableListOf("case\tstatus\trows\tplan\tindex_gets\tscan_gets\tindex_bytes\tscan_bytes\tindex_p50_ms\tindex_p95_ms\tscan_p50_ms\tdetail")
    IndexBenchStore().use { backend ->
        println("Index benchmark: backend=${backend.endpoint ?: "embedded S3"} bucket=${backend.bucket} prefix=${backend.prefix} rows=$rows samples=$samples")
        val counting = CountingObjectStore(backend.store, recordKeys = true)
        fun start() = YangBaseServer.forTesting(counting, port = 0, cacheBytes = 0,
            allowNonAtomicPointerSwitch = backend.oss, slowQueryMillis = Long.MAX_VALUE).also { it.start() }
        var server = start()
        try {
            val cases = indexCases(rows)
            connect(server).use { c ->
                c.createStatement().use { it.execute("CREATE DATABASE ib"); it.execute("USE ib") }
                cases.distinctBy { it.table }.forEach { fixture ->
                    println("Seed ${fixture.table}")
                    c.createStatement().use { s ->
                        s.execute("CREATE TABLE ${fixture.table} (id BIGINT NOT NULL, v BIGINT, w BIGINT, txt VARCHAR(256), flag VARCHAR(16), payload VARCHAR(256), PRIMARY KEY(${if (fixture.partition.isEmpty()) "id" else "id,v"}) ${if (fixture.table.startsWith("ft_")) "" else fixture.index}) ${fixture.partition}")
                        if (fixture.table.startsWith("ft_")) s.execute("CREATE FULLTEXT INDEX ix ON ${fixture.table}(txt) WITH PARSER ${fixture.table.removePrefix("ft_")}")
                        for (from in 1..rows step 500) {
                            val values = (from..minOf(rows, from + 499)).joinToString(",") { id ->
                                val v = when (fixture.data) {
                                    "sparse" -> if (id > rows - 10) 42 else if (id % 2 == 0) 0 else 100
                                    "partition" -> id % 4
                                    "hash-partition" -> id % 100
                                    else -> id
                                }
                                val txt = if (id > rows - 10) "rareuniquetoken 稀有标记" else "ordinary document 普通文档"
                                // 固定种子、较宽且不易压缩的行，确保有足够多个 SST 块。
                                val payload = java.util.Random(id.toLong()).let { r -> (1..160).map { ('a'.code + r.nextInt(26)).toChar() }.joinToString("") }
                                "($id,$v,${id % 4},'$txt','${if (id % 2 == 0) "yes" else "no"}','$payload')"
                            }
                            s.execute("INSERT INTO ${fixture.table} VALUES $values")
                        }
                    }
                }
            }
            runBlocking { server.engine.flushAll() }
            server.stop()
            server = start() // 销毁写入时的内存状态，强制从真实后端恢复。
            val failures = mutableListOf<String>()
            connect(server).use { c ->
                c.createStatement().use { it.execute("USE ib") }
                // 同一只读快照：TSO 窗口续领不应随机混进单次索引 IO 预算。
                c.autoCommit = false
                for (case in cases) {
                    try {
                        val line = measureCase(c, counting, case, samples)
                        output += line
                        println(line)
                    } catch (e: Exception) {
                        val detail = (e.message ?: e.javaClass.simpleName).replace('\n', ' ').replace('\t', ' ')
                        failures += "${case.name}: $detail"
                        output += "${case.name}\tFAIL\t${case.expected.size}\t\t\t\t\t\t\t\t\t$detail"
                        println("FAIL ${case.name}: $detail")
                    }
                    Files.write(report, output)
                }
                c.rollback()
                c.autoCommit = true
            }
            println("Report: ${report.toAbsolutePath()}")
            check(failures.isEmpty()) { "${failures.size}/${cases.size} failed:\n${failures.joinToString("\n")}" }
            println("PASS ${cases.size}/${cases.size}")
        } finally {
            server.stop()
            Files.write(report, output)
        }
    }
}

private fun connect(server: YangBaseServer): Connection = DriverManager.getConnection(
    "jdbc:mysql://127.0.0.1:${server.boundPort}/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC", "root", "")

private data class IndexCase(
    val name: String, val table: String, val predicate: String, val baseline: String,
    val expected: List<Long>, val index: String = "", val partition: String = "", val data: String = "ordered",
    val plan: String = "ALL", val evidence: String? = null, val count: Boolean = false,
    val maxGets: Long? = null,
) {
    val sql get() = "SELECT ${if (count) "COUNT(*)" else "id"} FROM $table${if (predicate.isEmpty()) "" else " WHERE $predicate"}"
    val scan get() = "SELECT ${if (count) "COUNT(*)" else "id"} FROM $table WHERE $baseline"
}

private fun indexCases(n: Int): List<IndexCase> {
    val tail = (n - 9..n).map(Int::toLong)
    val point = listOf((n - 5).toLong())
    return buildList {
        add(IndexCase("primary-point", "base", "id=${n - 5}", "id+0=${n - 5}", point, plan = "const", maxGets = 1))
        add(IndexCase("primary-range", "base", "id>=${n - 9}", "id+0>=${n - 9}", tail, plan = "range"))
        add(IndexCase("count-all", "base", "", "id+0>0", listOf(n.toLong()), plan = "METADATA_COUNT", count = true, maxGets = 0))
        add(IndexCase("count-values", "base", "flag='yes'", "CONCAT(flag,'')='yes'", listOf(n / 2L), plan = "METADATA_COUNT", count = true, maxGets = 0))
        add(IndexCase("secondary-ordered", "ordered_idx", "v=${n - 5}", "v+0=${n - 5}", point, ", KEY ix(v)", plan = "ref"))
        add(IndexCase("secondary-hash", "hash_idx", "v=${n - 5}", "v+0=${n - 5}", point, ", KEY ix(v) USING HASH", plan = "ref"))
        add(IndexCase("secondary-unique", "unique_idx", "v=${n - 5}", "v+0=${n - 5}", point, ", UNIQUE KEY ix(v)", plan = "ref"))
        add(IndexCase("secondary-composite", "composite_idx", "w=${(n - 5) % 4} AND v=${n - 5}", "w+0=${(n - 5) % 4} AND v+0=${n - 5}", point, ", KEY ix(w,v)", plan = "ref"))
        add(IndexCase("skip-minmax", "minmax_idx", "v>=${n - 9}", "v+0>=${n - 9}", tail, ", KEY ix(v) USING MINMAX"))
        for (kind in listOf("SET", "BLOOM")) {
            add(IndexCase("skip-${kind.lowercase()}", "${kind.lowercase()}_idx", "v=42", "v+0=42", tail,
                ", KEY ix(v) USING $kind", data = "sparse", evidence = ".skp"))
        }
        add(IndexCase("skip-ngram", "ngram_idx", "txt LIKE '%rareuniquetoken%'", "CONCAT(txt,'') LIKE '%rareuniquetoken%'", tail,
            ", KEY ix(txt) USING NGRAM", evidence = ".skp"))
        for ((parser, token) in listOf("standard" to "rareuniquetoken", "ngram" to "稀有标记")) {
            add(IndexCase("fulltext-$parser", "ft_$parser", "MATCH(txt) AGAINST('$token')", "CONCAT(txt,'') LIKE '%$token%'", tail,
                ", FULLTEXT KEY ix(txt) WITH PARSER $parser", plan = "fulltext", evidence = "postings.bin"))
        }
        for ((kind, ddl) in listOf(
            "range" to "PARTITION BY RANGE(v) (PARTITION p0 VALUES LESS THAN (1), PARTITION p1 VALUES LESS THAN (2), PARTITION p2 VALUES LESS THAN (3), PARTITION p3 VALUES LESS THAN MAXVALUE)",
            "list" to "PARTITION BY LIST(v) (PARTITION p0 VALUES IN (0), PARTITION p1 VALUES IN (1), PARTITION p2 VALUES IN (2), PARTITION p3 VALUES IN (3))",
            "hash" to "PARTITION BY HASH(v) PARTITIONS 4",
        )) add(IndexCase("partition-$kind", "part_$kind", "v=3", "v+0=3", (1..n).filter { it % (if (kind == "hash") 100 else 4) == 3 }.map(Int::toLong),
            partition = ddl, data = if (kind == "hash") "hash-partition" else "partition"))
        add(IndexCase("partition-key-routing", "part_key", "id=${n - 5} AND v=3",
            "id+0=${n - 5} AND v+0=3", point,
            partition = "PARTITION BY KEY(id,v) PARTITIONS 4", data = "partition", plan = "const", maxGets = 1))
    }
}

private data class Reading(val result: List<Long>, val gets: Long, val bytes: Long, val millis: Double, val keys: Set<String>)

private fun query(c: Connection, sql: String): List<Long> = c.createStatement().use { s ->
    s.executeQuery(sql).use { rs -> buildList { while (rs.next()) add(rs.getLong(1)) }.sorted() }
}

private fun read(c: Connection, store: CountingObjectStore, sql: String): Reading {
    store.reset()
    val start = System.nanoTime()
    val result = query(c, sql)
    val millis = (System.nanoTime() - start) / 1e6
    check(store.count("list") == 0L) { "热路径发生 LIST: ${store.keyReport()}" }
    check(store.count("put") + store.count("put_cond") == 0L) { "只读查询发生 PUT" }
    return Reading(result, store.count("get") + store.count("get_range"),
        store.bytesOut("get") + store.bytesOut("get_range"), millis,
        (store.keysFor("get") + store.keysFor("get_range")).map { it.first }.toSet())
}

private fun measureCase(c: Connection, store: CountingObjectStore, case: IndexCase, samples: Int): String {
    // 数据块缓存禁用；这两次只把元数据/恢复成本从稳态查询中分离。
    query(c, case.scan)
    query(c, case.sql)
    val plan = c.createStatement().use { s -> s.executeQuery("EXPLAIN ${case.sql}").use { r ->
        check(r.next()); r.getString("type")
    } }
    check(plan.equals(case.plan, ignoreCase = true)) { "预期计划 ${case.plan}，实际 $plan" }
    val scans = mutableListOf<Reading>()
    val indexed = mutableListOf<Reading>()
    repeat(samples) { iteration ->
        // 交替顺序，减少网络/JIT 漂移对报告延迟的影响。
        if (iteration % 2 == 0) { scans += read(c, store, case.scan); indexed += read(c, store, case.sql) }
        else { indexed += read(c, store, case.sql); scans += read(c, store, case.scan) }
    }
    (scans + indexed).forEach { check(it.result == case.expected) { "结果与独立生成的期望不符: ${it.result.size} != ${case.expected.size}" } }
    for ((scan, idx) in scans.zip(indexed)) {
        check(scan.gets >= 4 && scan.bytes > 0) { "全扫描对照未产生足够 IO: GET=${scan.gets}, bytes=${scan.bytes}" }
        check(idx.bytes <= scan.bytes * 0.6) { "读取字节未减少至少 40%: index=${idx.bytes}, scan=${scan.bytes}, GET=${idx.gets}/${scan.gets}" }
        check(idx.gets < scan.gets) { "GET 未减少: ${idx.gets}/${scan.gets}" }
        case.maxGets?.let { check(idx.gets <= it) { "超出 GET 预算: ${idx.gets}>$it" } }
        case.evidence?.let { suffix -> check(idx.keys.any { it.endsWith(suffix) }) { "没有实际读取 $suffix" } }
        if (case.name.startsWith("partition-")) {
            fun shards(r: Reading) = r.keys.filter { it.endsWith(".dat") }.map { it.substringBefore("/sst/") }.toSet()
            check(shards(scan).size == 4 && shards(idx).size == 1) { "分区未从 4 裁到 1: ${shards(scan)} -> ${shards(idx)}" }
        }
    }
    fun pct(values: List<Reading>, p: Double) = String.format(Locale.ROOT, "%.3f", values.map { it.millis }.sorted()[(ceil(values.size * p).toInt() - 1).coerceAtLeast(0)])
    val a = indexed.first(); val b = scans.first()
    return listOf(case.name, "PASS", case.expected.size, plan, a.gets, b.gets, a.bytes, b.bytes,
        pct(indexed, 0.5), pct(indexed, 0.95), pct(scans, 0.5), "result equality; IO gate").joinToString("\t")
}
