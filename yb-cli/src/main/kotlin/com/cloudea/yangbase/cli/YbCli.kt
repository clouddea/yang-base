package com.cloudea.yangbase.cli

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.audit.Audit
import com.cloudea.yangbase.objectstore.LocalFileObjectStore
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import com.cloudea.yangbase.objectstore.ObjectStoreAuditLog
import com.cloudea.yangbase.objectstore.S3ObjectStore
import com.cloudea.yangbase.txn.DatabaseEngine
import com.cloudea.yangbase.txn.GarbageCollector
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlin.system.exitProcess

/**
 * `yb-cli` —— 面向**运维**的命令行工具。
 *
 * 与 `yb-bench` 的区别是目标：bench 回答「它快不快」，cli 回答
 * 「现在存储里是什么、有多少垃圾、能不能安全删掉」。后者是长期运行一个
 * 对象存储数据库真正需要的能力——因为对象存储上**看不见的东西才是问题**：
 * 数据在哪个分片、manifest 有多大、哪些对象已经没人引用了。
 *
 * 所有命令都**直接操作对象存储**，不需要 CN 在线。这是本架构的一个直接好处：
 * 计算层随时可以销毁，运维动作不应该依赖它（原则 P1、P5）。
 */
/**
 * 参数错误。
 *
 * 命令**抛异常**而不是自己 `exitProcess`——那样会让整个 CLI 无法被测试
 * （测试进程会直接被杀掉），而且把一个可恢复的错误变成了进程终止。
 * 退出码由 [YbCli.main] 统一决定。
 */
class CliError(message: String, val code: Int = 2) : RuntimeException(message)

object YbCli {

    @JvmStatic
    fun main(args: Array<String>) {
        if (args.isEmpty() || args[0] in setOf("-h", "--help", "help")) {
            usage()
            return
        }
        try {
            run(args[0], args.drop(1))
        } catch (e: CliError) {
            System.err.println("错误: ${e.message}")
            if (e.code == 2) usage()
            exitProcess(e.code)
        } catch (e: com.cloudea.yangbase.common.CapabilityNotSupportedException) {
            // ★ 能力缺失要给**命令行上的出路**，不能只把内部名字抛出来。
            // 这一层是唯一知道"本命令的名字长什么样"的地方：存储层只知道
            // `allowNonAtomicPointerSwitch`，而使用者手里敲的是
            // `--i-accept-nonatomic-pointer-switch`
            System.err.println(
                "执行失败: ${e.message}\n" +
                    "  · 若这是阿里云 OSS 这类**没有 ETag CAS** 的后端，且确认是单写者部署，\n" +
                    "    可加 --i-accept-nonatomic-pointer-switch 接受降级（含义见 yb-cli help）",
            )
            exitProcess(1)
        } catch (e: Exception) {
            System.err.println("执行失败: ${e.message}")
            exitProcess(1)
        }
    }

    /** 执行一条命令。**不终止进程**——测试可以直接调它。 */
    fun run(cmd: String, argList: List<String>) = runBlocking {
        val opts = parseOpts(argList)
        when (cmd) {
            "ls" -> ls(opts)
            "shards" -> shards(opts)
            "gc" -> gc(opts)
            "recover-check" -> recoverCheck(opts)
            "cost" -> cost(opts)
            "backup" -> backup(opts)
            "backups" -> listBackups(opts)
            "restore" -> restore(opts)
            "pitr" -> pitr(opts)
            "cdc" -> cdc(opts)
            "analyze-sql" -> analyzeSql(opts)
            else -> throw CliError("未知命令: $cmd（用 yb-cli help 查看可用命令）")
        }
    }

    private fun usage() {
        println(
            """
            yb-cli —— YangBase 运维命令行

            用法: yb-cli <命令> [选项]

            命令:
              ls              列出对象存储中的全部对象（按前缀归组统计）
              shards          列出所有分片及其规模
              gc              垃圾回收。默认**只标记不删除**，加 --delete 才真删
              recover-check   检查恢复路径的健康度（悬空 manifest、断链的 SST）
              cost            按设计文档 §13 的成本模型估算月度存储与请求费用
              backup          创建一份备份（只写清单，与数据量无关）
              backups         列出可用备份
              restore         从备份恢复（切 manifest 指针）
              pitr            时间点恢复：把某张表切回某个时刻
              analyze-sql     迁移风险扫描：找出目标 SQL 里本系统不支持或语义不同的部分
              cdc             变更流：从 WAL 读出变更事件（JSON Lines）

            对象存储选项（三者必须成套）:
              --fs <dir>              使用本地目录（开发用）
              --endpoint <url>        S3 兼容端点（MinIO / OSS / COS / R2）
              --bucket <name>         bucket 名
              --region <name>         区域，默认 us-east-1
              --access-key <k>        访问密钥
              --secret-key <k>        访问密钥
              --path-style <b>        寻址风格：true=路径式（MinIO 默认）/ false=虚拟主机式
              --s3-oss                后端是阿里云 OSS：改用它的原语（原子创建走
                                      x-oss-forbid-overwrite、无 ETag CAS），
                                      寻址默认切到三级域名
              --i-accept-nonatomic-pointer-switch
                                      接受这个后端上协调写入失去原子性（单写者部署，
                                      与 yb-server 的同名开关含义一致）。**写命令**
                                      （restore / pitr / gc --delete 之外的写路径）
                                      在没有 ETag CAS 的后端上需要它，否则明确报错
              也可用环境变量 YB_S3_ENDPOINT / YB_S3_BUCKET / YB_S3_ACCESS_KEY /
              YB_S3_SECRET_KEY / YB_S3_PATH_STYLE / YB_S3_OSS

            backup / restore / pitr 选项:
              --note <text>       备份备注
              --from <key>        备份对象键（restore 用；省略则用最新的一份）
              --table <name>      pitr 的目标表
              --to <ISO 时间>     pitr 的目标时刻，如 "2026-09-24 10:00:00"
              --yes               restore / pitr 必须显式确认（不带它只打印将要做什么）

            cdc 选项:
              --since <ISO 时间>  从这个时刻开始读（默认读全部保留窗口内的）
              --table <name>      只订阅这张表（可省略 = 全部表）
              --limit <n>         最多输出多少条，默认 1000
              --follow            持续跟随（轮询），直到被中断

            analyze-sql 选项:
              --path <file|dir>   要扫描的 .sql 文件或目录

            gc 选项:
              --delete                真正执行删除（默认只打印将要删除的内容）
              --window-hours <n>      安全窗口，默认 24 小时（下限 1 小时，见 --help-why）
            """.trimIndent()
        )
    }

    // ── 选项 ──────────────────────────────────────────────────────

    private fun parseOpts(args: List<String>): Map<String, String> {
        val out = LinkedHashMap<String, String>()
        var i = 0
        while (i < args.size) {
            val a = args[i]
            if (!a.startsWith("--")) { i++; continue }
            val key = a.removePrefix("--")
            val next = args.getOrNull(i + 1)
            if (next != null && !next.startsWith("--")) {
                out[key] = next; i += 2
            } else {
                out[key] = "true"; i += 1
            }
        }
        return out
    }

    /**
     * `--i-accept-nonatomic-pointer-switch`（与 `yb-server` 同名同义）。
     *
     * 判据只认命令行开关，**不读环境变量**：这是一个"我知道后果"的开关，
     * 让它从环境里悄悄生效，等于让一次误配置在无人察觉的情况下削弱互斥。
     */
    private fun acceptNonAtomic(o: Map<String, String>): Boolean =
        o.containsKey("i-accept-nonatomic-pointer-switch")

    private fun openStore(o: Map<String, String>): ObjectStore {
        o["fs"]?.let { return LocalFileObjectStore(java.nio.file.Path.of(it)) }
        val endpoint = o["endpoint"] ?: System.getenv("YB_S3_ENDPOINT")
        if (endpoint != null) {
            val bucket = o["bucket"] ?: System.getenv("YB_S3_BUCKET") ?: "yangbase"
            return S3ObjectStore.create(
                bucket = bucket,
                endpoint = endpoint,
                region = o["region"] ?: System.getenv("YB_S3_REGION") ?: "us-east-1",
                accessKey = o["access-key"] ?: System.getenv("YB_S3_ACCESS_KEY"),
                secretKey = o["secret-key"] ?: System.getenv("YB_S3_SECRET_KEY"),
                // 寻址风格：MinIO/自建网关要路径式（默认），阿里云 OSS 要虚拟主机式
                pathStyle = (o["path-style"] ?: System.getenv("YB_S3_PATH_STYLE"))?.toBooleanStrictOrNull(),
                // 后端是阿里云 OSS：改用它的原语（`x-oss-forbid-overwrite` 做原子创建、
                // 如实声明「无 ETag CAS」）。只读命令（ls/shards/cost/recover-check）
                // 用不上条件写，但 `gc --delete` / `restore` / `pitr` 会写——
                // 在那个后端上按 S3 语义发条件头会被拒（400），所以这里必须能声明
                caps = if (o.containsKey("s3-oss") || System.getenv("YB_S3_OSS") == "true") {
                    com.cloudea.yangbase.objectstore.ObjectStoreCapabilities.OSS
                } else {
                    com.cloudea.yangbase.objectstore.ObjectStoreCapabilities.FULL
                },
            )
        }
        throw CliError("必须指定对象存储：--fs <dir> 或 --endpoint <url>")
    }

    /**
     * 打开一个引擎。
     *
     * ⚠️ [readOnly] 默认 **true**，这不是保守，是安全要求：
     * 运维命令（`ls`/`shards`/`cost`/`recover-check`/`backup`）只需读，
     * 但一旦以**写者身份**打开分片，在 `single` 协调模式下会让 CLI 与线上 CN
     * 同时成为同一分片的写者（那条模式下 epoch 恒为 1、抢租约永远成功），
     * 两者会往同一个 `wal/e1/` 前缀下写对象并互相覆盖——**静默丢数据**。
     *
     * 只有明确要改数据的命令（`restore`/`pitr`）才传 false，且它们自身
     * 要求 `--yes`。
     */
    private fun engineOf(
        store: ObjectStore,
        readOnly: Boolean = true,
        allowNonAtomicPointerSwitch: Boolean = false,
    ): DatabaseEngine {
        val e = DatabaseEngine(
            store,
            CoroutineScope(SupervisorJob() + Dispatchers.Default),
            nodeId = "cli",
            readOnly = readOnly,
            // ★ 与 yb-server 的 `--i-accept-nonatomic-pointer-switch` 是**同一件事**：
            // 恢复类命令（restore / pitr）要写数据 → 要时间戳 → 在 OSS 这类没有
            // ETag CAS 的后端上会撞上"时间戳水位需要 CAS"。不给这个开关的话，
            // 那些命令在 OSS 上**没有任何办法**跑起来（而它们在 S3/MinIO 上正常），
            // 使用者只会看到一句与"我想做恢复"无关的错误。
            allowNonAtomicPointerSwitch = allowNonAtomicPointerSwitch,
        )
        if (allowNonAtomicPointerSwitch) {
            // 与 yb-server 同款告警：**降级必须看得见**。运维命令跑完就退出，
            // 没有 /metrics、也没人会回头翻日志，所以这句只能打在标准输出上
            println(
                "⚠️ 已按 --i-accept-nonatomic-pointer-switch 运行：本后端没有 ETag CAS，" +
                    "manifest 指针切换 / 时间戳水位 / 自增段水位在**单写者**部署下退化为「写后回读校验」",
            )
        }
        runBlocking { e.open() }
        return e
    }

    // ── 命令 ──────────────────────────────────────────────────────

    /**
     * 一次容量盘点：**按前缀归组统计对象数与字节数**。
     *
     * 「看不见的东西才是问题」——数据在哪个分片、哪类对象占了多少空间，
     * 是这套系统最需要可见性的一面。
     *
     * ⚠️ **绝不能逐对象 `head`。** 这里曾经是 `for (k in keys) store.head(k).size`，
     * 一个十万对象的桶就是十万次计费请求——**做一次容量评估比一整天的业务请求还多**
     * （P4：请求数是一等成本）。现在的大小来自 LIST 响应本身（[ListPage.sizes]），
     * 0 次额外请求；后端不返回大小时，退化为**有上限的采样 + 外推**，
     * 并把"这是估算值"如实印出来（原则 P6：显式降级），而不是偷偷发十万次请求。
     */
    private suspend fun ls(o: Map<String, String>) {
        val store = openStore(o)
        val summary = summarize(store)
        if (summary.totalObjects == 0) {
            println("（空）")
            return
        }
        println("${"前缀".padEnd(34)} ${"对象数".padStart(8)} ${"字节".padStart(14)}")
        for (g in summary.groups.sortedByDescending { it.objects }) {
            println("${g.prefix.padEnd(34)} ${g.objects.toString().padStart(8)} ${g.bytes.toString().padStart(14)}")
        }
        println("-".repeat(60))
        val mark = if (summary.sizesComplete) "" else "（估算）"
        println(
            "${"合计$mark".padEnd(34)} ${summary.totalObjects.toString().padStart(8)} " +
                "${summary.totalBytes.toString().padStart(14)}"
        )
        if (!summary.sizesComplete) {
            println()
            if (summary.sampled > 0) {
                println("注：本后端在 LIST 里不返回对象大小，字节数是**采样 ${summary.sampled} 个对象后外推**的")
                println("    估算值（对象数仍然精确）。要看精确值请用支持在 LIST 中返回 size 的后端。")
            } else {
                println("注：本后端在 LIST 里不返回对象大小，且采样也失败了——**字节数未知**，")
                println("    上面显示的只是已知部分。对象数是精确的。")
            }
        }
    }

    /** 容量盘点的一行。 */
    internal data class GroupStat(val prefix: String, val objects: Int, val bytes: Long)

    /** [summarize] 的结果。[sizesComplete] 为 false 时 [totalBytes] 是外推出来的。 */
    internal data class StorageSummary(
        val groups: List<GroupStat>,
        val totalObjects: Int,
        val totalBytes: Long,
        val sizesComplete: Boolean,
        val sampled: Int,
    )

    /** 后端不给大小时，最多采样这么多个对象来外推。**它是上限，不是目标**。 */
    private const val SIZE_SAMPLE_LIMIT = 200

    /**
     * 分页扫描一次，得到按前缀归组的容量摘要。
     *
     * 大小优先取 [ListPage.sizes]（免费）；缺失的部分只在**前 [SIZE_SAMPLE_LIMIT] 个**
     * 对象上补一次 `head`，其余按键数等比外推。
     */
    internal suspend fun summarize(store: ObjectStore): StorageSummary {
        val counts = HashMap<String, Int>()          // 分组 → 对象数（精确）
        val knownBytes = HashMap<String, Long>()     // 分组 → LIST 直接给出的大小之和
        val missingByGroup = HashMap<String, Int>()  // 分组 → LIST 没给大小的对象数
        val sample = ArrayList<String>()             // 待补采样的键（有上限）
        var total = 0

        var token: String? = null
        do {
            val page = store.list("", token, 1000)
            for (k in page.keys) {
                val g = groupOf(k)
                counts[g] = (counts[g] ?: 0) + 1
                total++
                val sz = page.sizes[k]
                if (sz != null) {
                    knownBytes[g] = (knownBytes[g] ?: 0L) + sz
                } else {
                    missingByGroup[g] = (missingByGroup[g] ?: 0) + 1
                    if (sample.size < SIZE_SAMPLE_LIMIT) sample.add(k)
                }
            }
            token = page.nextToken
        } while (token != null)

        if (missingByGroup.isEmpty()) {
            return StorageSummary(
                groups = counts.map { (g, n) -> GroupStat(g, n, knownBytes[g] ?: 0L) },
                totalObjects = total,
                totalBytes = knownBytes.values.sum(),
                sizesComplete = true,
                sampled = 0,
            )
        }

        // ── 降级路径：只对**没拿到大小**的对象补采样，且总量有上限 ──
        val sampledBytes = HashMap<String, Long>()
        val sampledOk = HashMap<String, Int>()
        var sampledCount = 0
        var sampledTotal = 0L
        for (k in sample) {
            val sz = runCatching { store.head(k).size }.getOrNull() ?: continue
            val g = groupOf(k)
            sampledBytes[g] = (sampledBytes[g] ?: 0L) + sz
            sampledOk[g] = (sampledOk[g] ?: 0) + 1
            sampledCount++
            sampledTotal += sz
        }
        // 采样失败（对象在列举之后被删、或权限不足）时平均值为 0：字节数会被低估，
        // 所以上面那行提示必须写清"估算"而不是假装精确
        val mean = if (sampledCount > 0) sampledTotal.toDouble() / sampledCount else 0.0

        var estimatedTotal = 0L
        val groups = counts.map { (g, n) ->
            val missing = missingByGroup[g] ?: 0
            // 该组里既没拿到 LIST 大小、又没采样到的对象：只能按键数等比外推
            val unresolved = (missing - (sampledOk[g] ?: 0)).coerceAtLeast(0)
            val b = (knownBytes[g] ?: 0L) + (sampledBytes[g] ?: 0L) + Math.round(mean * unresolved)
            estimatedTotal += b
            GroupStat(g, n, b)
        }
        return StorageSummary(groups, total, estimatedTotal, sizesComplete = false, sampled = sampledCount)
    }

    /** 把对象键归成人类能看懂的一组。零填充的名字在这一步帮不上忙，但前缀能。 */
    private fun groupOf(key: String): String = when {
        key.startsWith("catalog/db/") -> "catalog/db/"
        key.startsWith("catalog/table/") && key.endsWith("/CURRENT") -> "catalog/table/*/CURRENT"
        key.startsWith("catalog/table/") && key.contains("schema.v") -> "catalog/table/*/schema"
        key.startsWith("catalog/table/") -> "catalog/table/*/*"
        key.startsWith("catalog/byname/") -> "catalog/byname/"
        key.startsWith("catalog/") -> "catalog/（其它）"
        key.contains("/manifest/") -> "shard/*/*/manifest/"
        key.contains("/wal/") -> "shard/*/*/wal/"
        key.endsWith(".idx") && key.contains("/sst/") -> "shard/*/*/sst/*.idx"
        key.endsWith(".dat") && key.contains("/sst/") -> "shard/*/*/sst/*.dat"
        key.startsWith("shard/") -> "shard/（其它）"
        key.startsWith("index/") -> "index/（倒排等）"
        key.startsWith("stats/") -> "stats/"
        key.startsWith("ops/garbage/") -> "ops/garbage/"
        key.startsWith("meta/") -> "meta/"
        else -> key.substringBefore('/', key).let { "（其它）$it/" }
    }

    private suspend fun shards(@Suppress("UNUSED_PARAMETER") o: Map<String, String>) {
        val store = openStore(o)
        val engine = engineOf(store)
        println("${"表".padEnd(20)} ${"分片".padStart(6)} ${"文件".padStart(6)} ${"行数".padStart(12)} ${"常驻索引".padStart(12)}")
        var totalRows = 0L
        for (t in engine.catalog.allTables()) {
            for (shard in engine.allShards(t)) {
                val lsm = runCatching { engine.shardStore(shard, t) }.getOrNull() ?: continue
                val st = lsm.stats()
                val rows = st.rowCountExact.takeIf { it >= 0 } ?: -1
                if (rows > 0) totalRows += rows
                println(
                    "${t.name.take(20).padEnd(20)} ${shard.partition.value.toString().padStart(6)} " +
                        "${st.levelFiles.sum().toString().padStart(6)} " +
                        "${(if (rows >= 0) rows.toString() else "?").padStart(12)} " +
                        "${"${st.residentIndexBytes / 1024}K".padStart(12)}"
                )
            }
        }
        println("合计行数: $totalRows")
    }

    private suspend fun gc(o: Map<String, String>) {
        val windowHours = (o["window-hours"]?.toLongOrNull() ?: 24L) * 60 * 60 * 1000
        // ★ 安全窗口是**唯一**保护"已写入但还没被 manifest 引用"的对象的机制：
        // SST 是先写 `.dat`、再写 `.idx`、最后提交 manifest；WAL 对象 PUT 成功即算
        // 提交但要等下一次 flush 才进 manifest。窗口调到很小（或 0）时，
        // `--delete` 会把**在途对象**当成垃圾删掉，随后写者提交的 manifest
        // 指向不存在的对象——分片直接不可读，那是本系统里唯一真正销毁数据的操作。
        // 所以这里设下限，要绕过必须显式写 `--i-know-what-im-doing`
        if (windowHours < 60L * 60 * 1000 && !o.containsKey("i-know-what-im-doing")) {
            throw CliError(
                "安全窗口 ${windowHours / 3600000} 小时太短：它是保护「刚写入但还没进 manifest」" +
                    "的对象的唯一机制（SST 与 WAL 都可能在这个窗口内）。建议 ≥ 1 小时；" +
                    "确实要用更短的窗口，请加 --i-know-what-im-doing",
            )
        }
        val doDelete = o["delete"] == "true"
        val store = openStore(o)
        val engine = engineOf(store)
        val gc = GarbageCollector(store, windowHours)

        // ★ GC 是这套系统里**唯一真正销毁数据**的动作，必须留下审计记录（§12.5）。
        // 没有它，对象存储上「谁在什么时候删了什么」将完全不可考。
        // 审计是批量异步写的：N 条记录摊进一次 PUT，且写失败不影响 GC 本身。
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val audit = ObjectStoreAuditLog(store, scope)
        audit.start()
        Audit.install(audit)
        try {
            gcWithAudit(engine, gc, windowHours, doDelete)
        } finally {
            // 顺序不能反：先摘落点、再冲刷，最后几条记录就会丢
            Audit.uninstallIf(audit)
            audit.shutdown()
            scope.cancel()
        }
    }

    private suspend fun gcWithAudit(
        engine: DatabaseEngine,
        gc: GarbageCollector,
        windowHours: Long,
        doDelete: Boolean,
    ) {
        println("安全窗口: ${windowHours / 3600000} 小时")
        var totalGarbage = 0
        var totalFenced = 0
        val marks = ArrayList<GarbageCollector.MarkResult>()
        for (t in engine.catalog.allTables()) {
            for (shard in engine.allShards(t)) {
                val m = gc.mark(t, shard)
                if (m.totalGarbage > 0) {
                    println("分片 ${m.shard}: 待回收 ${m.garbage.size} 个，被 fence 的旧写者 WAL ${m.fencedWal.size} 个")
                    marks.add(m)
                }
                totalGarbage += m.garbage.size
                totalFenced += m.fencedWal.size
            }
        }

        // ★ 被删表遗留的分片：GC 的遍历集合一直是"活表"，于是这些分片
        // **从来不在任何清单里**——用户以为删掉的数据不再付钱，实际一直在付。
        // 超过保留期（30 天，墓碑仍可恢复的窗口）之后它们就是确定的可回收物
        val dropped = gc.droppedTableObjects()
        val droppedCount = dropped.values.sumOf { it.size }
        if (droppedCount > 0) {
            println()
            println("被删除的表（超过 30 天保留期）遗留: ${dropped.size} 个分片根，$droppedCount 个对象")
            marks.add(
                GarbageCollector.MarkResult(
                    shard = "(已删除的表)",
                    garbage = dropped.values.flatten(),
                    fencedWal = emptyList(),
                    reachable = 0,
                ),
            )
            totalGarbage += droppedCount
        }

        // ★ MS 的 Raft 日志：快照边界之下的条目永远不会再被读
        // （恢复只加载 `index > snapshotLastIndex` 的），却每条一个对象、永不删除
        val msLogs = gc.msLogObjects()
        if (msLogs.isNotEmpty()) {
            println("MS Raft 日志（快照边界之下，可回收）: ${msLogs.size} 个对象")
            marks.add(
                GarbageCollector.MarkResult(
                    shard = "(MS 日志)",
                    garbage = msLogs,
                    fencedWal = emptyList(),
                    reachable = 0,
                ),
            )
            totalGarbage += msLogs.size
        }

        println()
        println("待回收对象: $totalGarbage")
        // 这两个数字必须分开报：被 fence 的 WAL 是 §4.3 的**正常代价**，
        // 而 compaction 残留是 GC 的日常目标。混在一起看会以为「泄漏很严重」。
        println("其中被 fence 的旧写者 WAL: $totalFenced（正常现象，见设计文档 §4.3）")
        println("其中 compaction / 过期 WAL 残留: ${totalGarbage - totalFenced}")

        if (!doDelete) {
            // 无论有没有东西可回收，都要明确说清「这次一条也没删」。
            // 只打印「没有可回收的对象」然后退出，会让使用者不确定
            // 是「真的没有」还是「工具什么都没干」——运维脚本也会因此误判。
            println()
            if (totalGarbage == 0) {
                println("没有可回收的对象（全部对象都仍被 manifest 引用）。")
            }
            println("这是**标记**阶段，未删除任何对象。")
            println("标记清单将在安全窗口（${windowHours / 3600000} 小时）之后才可被清扫。")
            println("执行删除：yb-cli gc --delete（会先写清单，再清理超窗口的旧清单）")
            val key = gc.writeManifest(System.currentTimeMillis(), marks)
            if (key != null) println("已写入标记清单: $key")
            return
        }

        if (totalGarbage == 0) {
            println("没有可回收的对象。")
        }

        if (marks.isNotEmpty()) gc.writeManifest(System.currentTimeMillis(), marks)
        // 推进水位：之后的时间旅行查询会在水位之前明确报错，而不是读到残缺历史
        val ts = gc.advanceWatermark()
        println("GC 水位已推进到 $ts")
        val swept = gc.sweep()
        println("清扫完成：清单 ${swept.manifests} 份，删除 ${swept.deleted} 个对象，失败 ${swept.failed} 个")
        if (swept.manifests == 0 && swept.deleted == 0) {
            println("（本次没有超过安全窗口的清单——这是**预期**的：刚标记的清单要等窗口过去）")
        }
    }

    /** 检查恢复路径的健康度。坏掉的 manifest 指针比坏掉的数据更致命。 */
    private suspend fun recoverCheck(o: Map<String, String>) {
        val store = openStore(o)
        val engine = engineOf(store)
        var problems = 0
        for (t in engine.catalog.allTables()) {
            for (shard in engine.allShards(t)) {
                val cur = ObjectKeys.manifestCurrent(shard)
                val ok = runCatching { store.get(cur) }.isSuccess
                if (!ok) {
                    println("✗ ${t.name} 分片 ${shard.partition.value}: CURRENT 指针缺失（分片从未写入过，通常是正常的）")
                    continue
                }
                val lsm = runCatching { engine.shardStore(shard, t) }.getOrNull()
                if (lsm == null) {
                    println("✗ ${t.name} 分片 ${shard.partition.value}: 无法打开")
                    problems++
                    continue
                }
                // 每个被 manifest 引用的 SST 都必须两个对象都在——缺一个就读不了那块数据
                val mf = com.cloudea.yangbase.storage.ManifestStore(store).load(shard)?.first ?: continue
                for (ref in mf.allFiles()) {
                    val fid = com.cloudea.yangbase.common.FileId(ref.fileId)
                    for (k in listOf(
                        ObjectKeys.sstData(shard, ref.level, fid),
                        ObjectKeys.sstIndex(shard, ref.level, fid),
                    )) {
                        if (runCatching { store.head(k) }.isFailure) {
                            println("✗ 缺失对象: $k（被 manifest 引用但不存在）")
                            problems++
                        }
                    }
                }
            }
        }
        println(if (problems == 0) "✓ 恢复路径健康：所有 manifest 引用的对象都在" else "发现 $problems 个问题")
    }

    // ── 备份 / 恢复 / PITR（设计文档 §12.3）──────────────────────

    private suspend fun backup(o: Map<String, String>) {
        val store = openStore(o)
        val engine = engineOf(store)
        val mgr = com.cloudea.yangbase.txn.SnapshotManager(store, engine)
        val key = mgr.backup(nodeId = "cli", note = o["note"] ?: "")
        val m = mgr.loadBackup(key)
        println("备份已创建: $key")
        println("覆盖 ${m.shards.size} 个分片、${m.catalogTables.size} 张表")
        // 备份的成本与数据量无关——这是「数据不可变」的直接推论，值得说清楚
        println("（备份只记录了 manifest 引用，未拷贝任何数据）")
    }

    private suspend fun listBackups(o: Map<String, String>) {
        val store = openStore(o)
        val engine = engineOf(store)
        val mgr = com.cloudea.yangbase.txn.SnapshotManager(store, engine)
        val keys = mgr.listBackups()
        if (keys.isEmpty()) {
            println("没有任何备份。用 yb-cli backup 创建一份。")
            return
        }
        println("${"时间".padEnd(22)} ${"分片".padStart(6)} ${"表".padStart(5)}  备注")
        for (k in keys) {
            val m = runCatching { mgr.loadBackup(k) }.getOrNull() ?: continue
            val ts = java.time.Instant.ofEpochMilli(m.createdAt).toString().replace("T", " ").take(19)
            println("${ts.padEnd(22)} ${m.shards.size.toString().padStart(6)} ${m.catalogTables.size.toString().padStart(5)}  ${m.note}")
            println("  key: $k")
        }
    }

    private suspend fun restore(o: Map<String, String>) {
        val store = openStore(o)
        // 这两个命令会改数据（切换 CURRENT 指针）→ 需要时间戳 → 在无 CAS 的后端上
        // 必须有显式同意才能降级（见 engineOf 的说明）
        val engine = engineOf(store, readOnly = false, allowNonAtomicPointerSwitch = acceptNonAtomic(o))
        // ⚠️ 开关必须**同时**给 SnapshotManager：切 CURRENT 指针的是它内部的 ManifestStore。
        // 只给引擎的话，"引擎开好了、备份也做了，restore 却在切指针时报条件写能力不足"
        val mgr = com.cloudea.yangbase.txn.SnapshotManager(
            store, engine, allowNonAtomicPointerSwitch = acceptNonAtomic(o),
        )
        val key = o["from"] ?: mgr.listBackups().lastOrNull()
            ?: throw CliError("没有任何备份可恢复")
        if (o["yes"] != "true") {
            println("将把 CURRENT 指针切回备份 $key 的状态。")
            println("数据对象不会被删除（不可变），但之后的写入会**不再可见**。")
            println("确认请加 --yes。")
            return
        }
        val r = mgr.restore(key)
        println("已恢复 ${r.restoredShards} 个分片")
        if (r.missingSnapshots.isNotEmpty()) {
            println("⚠️ 以下快照已丢失，未能恢复：")
            r.missingSnapshots.forEach { println("  - $it") }
        }
    }

    private suspend fun pitr(o: Map<String, String>) {
        val store = openStore(o)
        // 这两个命令会改数据（切换 CURRENT 指针）→ 需要时间戳 → 在无 CAS 的后端上
        // 必须有显式同意才能降级（见 engineOf 的说明）
        val engine = engineOf(store, readOnly = false, allowNonAtomicPointerSwitch = acceptNonAtomic(o))
        val tableName = o["table"] ?: throw CliError("pitr 需要 --table <name>")
        val target = o["to"] ?: throw CliError("pitr 需要 --to \"YYYY-MM-DD HH:MM:SS\"")
        val millis = com.cloudea.yangbase.common.Datum.parseDateTime(target)
            ?: throw CliError("无法解析的时刻 '$target'")

        val t = engine.catalog.allTables().firstOrNull { it.name.equals(tableName, true) }
            ?: throw CliError("表 '$tableName' 不存在")
        // ⚠️ 开关必须**同时**给 SnapshotManager：切 CURRENT 指针的是它内部的 ManifestStore。
        // 只给引擎的话，"引擎开好了、备份也做了，restore 却在切指针时报条件写能力不足"
        val mgr = com.cloudea.yangbase.txn.SnapshotManager(
            store, engine, allowNonAtomicPointerSwitch = acceptNonAtomic(o),
        )

        val earliest = mgr.earliestRestorableMillis(t)
        println("最早可恢复时刻: ${
            earliest?.let { java.time.Instant.ofEpochMilli(it) } ?: "（无数据）"
        }")
        if (earliest != null && millis < earliest) {
            // 明确报错而不是恢复到「最接近的」那一个——恢复错时刻会给出一个
            // 看起来正常、但不属于那个时间点的数据库
            throw CliError("该时刻的数据已不可恢复（早于最早可恢复时刻）")
        }
        if (o["yes"] != "true") {
            println("将把表 '$tableName' 恢复到 $target。确认请加 --yes。")
            return
        }
        val r = mgr.restoreTableTo(t, millis)
        println("已恢复 ${r.restoredShards} 个分片")
        if (r.missingSnapshots.isNotEmpty()) {
            println("⚠️ 部分分片该时刻无数据：${r.missingSnapshots.joinToString(", ")}")
        }
    }

    // ── 变更流（M5，Q5 决策）─────────────────────────────────────

    /**
     * 从 WAL 读出变更事件，输出 JSON Lines。
     *
     * **不需要额外的 binlog**：WAL 本来就是不可变、有序、带 commit_ts 的日志，
     * 变更订阅直接读它即可。这是「对象存储是唯一真相源」带来的直接好处。
     *
     * ⚠️ 可读的历史 = GC 安全窗口（默认 24 小时）。WAL 在 flush 之后会被回收，
     * 需要更长保留期就该把事件落到自己的存储里——这正是 CDC 的常规用法。
     */
    private suspend fun cdc(o: Map<String, String>) {
        val store = openStore(o)
        val engine = engineOf(store)
        val limit = o["limit"]?.toIntOrNull() ?: 1000
        val follow = o["follow"] == "true"

        val tables = o["table"]?.let { name ->
            listOf(
                engine.catalog.allTables().firstOrNull { it.name.equals(name, true) }
                    ?: throw CliError("表 '$name' 不存在"),
            )
        } ?: engine.catalog.allTables()

        val stream = com.cloudea.yangbase.txn.ChangeStream(store, tables)

        if (o["since"] != null) {
            val millis = com.cloudea.yangbase.common.Datum.parseDateTime(o["since"]!!)
                ?: throw CliError("无法解析的时刻 '${o["since"]}'")
            emitChanges(stream.pollSince(millis, limit), engine, tables)
            if (!follow) return
        }

        do {
            val changes = stream.poll(limit)
            emitChanges(changes, engine, tables)
            if (follow) kotlinx.coroutines.delay(1000)
        } while (follow)
    }

    private fun emitChanges(
        changes: List<com.cloudea.yangbase.txn.ChangeStream.Change>,
        engine: DatabaseEngine,
        tables: List<com.cloudea.yangbase.common.TableSchema>,
    ) {
        for (c in changes) {
            val t = tables.firstOrNull { it.tableId == c.tableId }
            val json = buildString {
                append("{\"ts\":").append(c.commitTs)
                append(",\"shard\":\"").append(c.shard).append('"')
                append(",\"table\":\"").append(t?.name ?: c.tableId).append('"')
                append(",\"op\":\"").append(c.op).append('"')
                append(",\"key\":\"").append(c.keyHex()).append('"')
                if (c.value != null && t != null) {
                    val row = runCatching { c.decodeRow(t) }.getOrNull()
                    if (row != null) {
                        append(",\"row\":{")
                        append(
                            t.columns.indices.joinToString(",") { i ->
                                "\"${t.columns[i].name}\":${jsonValue(row.getOrNull(i))}"
                            }
                        )
                        append("}")
                    }
                }
                append("}")
            }
            println(json)
        }
    }

    private fun jsonValue(d: com.cloudea.yangbase.common.Datum?): String = when (d) {
        null, is com.cloudea.yangbase.common.Datum.Null -> "null"
        is com.cloudea.yangbase.common.Datum.I64 -> d.v.toString()
        is com.cloudea.yangbase.common.Datum.F64 -> d.v.toString()
        is com.cloudea.yangbase.common.Datum.Bool -> if (d.v) "true" else "false"
        else -> "\"" + d.toString().replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    }

    // ── 迁移风险扫描（M5，Q8 决策的「三件必做事」之一）────────────

    /**
     * 扫描目标 SQL，找出本系统**不支持**或**语义不同**的部分。
     *
     * 设计文档 §16.1 的 Q8 决策把「默认 REPEATABLE READ」建立在三件事之上，
     * 这是其中之一：**使用者必须能在迁移前知道自己会踩到什么**。
     * 一个只做事后警告的系统等于没有警告。
     */
    private fun analyzeSql(o: Map<String, String>) {
        val pathArg = o["path"] ?: throw CliError("analyze-sql 需要 --path <file|dir>")
        val root = java.nio.file.Path.of(pathArg)
        val files = if (java.nio.file.Files.isDirectory(root)) {
            java.nio.file.Files.walk(root).use { s ->
                s.filter { it.toString().endsWith(".sql", ignoreCase = true) }.toList()
            }
        } else listOf(root)

        if (files.isEmpty()) {
            println("没有找到 .sql 文件")
            return
        }

        var totalFindings = 0
        for (f in files) {
            val text = runCatching { String(java.nio.file.Files.readAllBytes(f), Charsets.UTF_8) }.getOrNull() ?: continue
            val findings = scanSql(text)
            if (findings.isEmpty()) continue
            println("── ${f.fileName} ──")
            for ((line, kind, msg) in findings) {
                println("  $kind 行 $line: $msg")
            }
            println()
            totalFindings += findings.size
        }
        println("共发现 $totalFindings 处需要注意的地方。")
        if (totalFindings == 0) println("没有发现已知的兼容性问题。")
    }

    /** 返回 (行号, 级别, 说明)。 */
    private fun scanSql(text: String): List<Triple<Int, String, String>> {
        val out = ArrayList<Triple<Int, String, String>>()
        text.split('\n').forEachIndexed { i, raw ->
            val line = raw.trim()
            if (line.startsWith("--") || line.isEmpty()) return@forEachIndexed
            val u = line.uppercase()

            // ── 完全不支持：会明确报错，但最好在迁移前就知道 ──
            if (Regex("\\bJOIN\\b").containsMatchIn(u)) {
                out.add(Triple(i + 1, "不支持", "JOIN：本系统暂不支持连接查询"))
            }
            if (Regex("\\bGROUP\\s+BY\\b").containsMatchIn(u)) {
                out.add(Triple(i + 1, "不支持", "GROUP BY：聚合目前只有 COUNT"))
            }
            if (Regex("\\b(SUM|AVG|MIN|MAX)\\s*\\(").containsMatchIn(u)) {
                out.add(Triple(i + 1, "不支持", "聚合函数：目前只支持 COUNT"))
            }
            if (Regex("\\bUNION\\b").containsMatchIn(u)) {
                out.add(Triple(i + 1, "不支持", "UNION"))
            }
            if (Regex("\\bIN\\s*\\(\\s*SELECT").containsMatchIn(u)) {
                out.add(Triple(i + 1, "不支持", "子查询"))
            }
            if (Regex("\\b(CREATE|DROP)\\s+(VIEW|TRIGGER|PROCEDURE|FUNCTION|EVENT)\\b").containsMatchIn(u)) {
                out.add(Triple(i + 1, "不支持", "视图/触发器/存储过程"))
            }
            if (Regex("\\bFOREIGN\\s+KEY\\b").containsMatchIn(u)) {
                out.add(Triple(i + 1, "忽略", "外键：解析后忽略，不做约束校验与级联"))
            }
            if (Regex("\\bLOAD\\s+DATA\\b").containsMatchIn(u)) {
                out.add(Triple(i + 1, "不支持", "LOAD DATA INFILE"))
            }
            if (Regex("\\bLOCK\\s+TABLES\\b").containsMatchIn(u)) {
                out.add(Triple(i + 1, "不支持", "LOCK TABLES（mysqldump 依赖它）"))
            }

            // ── 支持但语义不同：静默的差异，最危险 ──
            if (Regex("FOR\\s+UPDATE|FOR\\s+SHARE|LOCK\\s+IN\\s+SHARE\\s+MODE").containsMatchIn(u)) {
                out.add(Triple(i + 1, "语义不同", "SELECT ... FOR UPDATE：无行锁，会退化为普通快照读"))
            }
            if (u.contains("ISOLATION LEVEL") && !u.contains("REPEATABLE")) {
                out.add(Triple(i + 1, "语义不同", "SET TRANSACTION ISOLATION LEVEL：本系统只有快照隔离，该语句会被拒绝"))
            }
            if (Regex("\\b(SAVEPOINT|ROLLBACK\\s+TO)\\b").containsMatchIn(u)) {
                out.add(Triple(i + 1, "不支持", "SAVEPOINT"))
            }
            if (Regex("\\bCHARACTER\\s+SET\\s+(?!utf8).*", RegexOption.IGNORE_CASE).containsMatchIn(u)
                && !u.contains("UTF8")) {
                out.add(Triple(i + 1, "语义不同", "非 utf8mb4 字符集：本系统只支持 utf8mb4"))
            }
            if (u.contains("COLLATE") && !u.contains("_BIN")) {
                out.add(Triple(i + 1, "语义不同", "大小写不敏感排序规则：本系统一律按二进制序比较"))
            }
            if (Regex("\\bAUTO_INCREMENT\\s*=\\s*\\d+").containsMatchIn(u)) {
                out.add(Triple(i + 1, "语义不同", "自增值不保证连续（与 innodb_autoinc_lock_mode=2 一致）"))
            }
        }
        return out
    }

    /** 按设计文档 §13 的成本模型估算。让「贵不贵」变成一个可算的数字。 */
    private suspend fun cost(o: Map<String, String>) {
        val store = openStore(o)
        val cls = (o["class"] ?: "standard").lowercase()

        // ★ 一次 LIST 扫描，**不逐对象 head**（见 [summarize] 的说明）。
        // 这条命令是给人做容量与账单评估用的，它有责任自己别成为账单的一部分
        val summary = summarize(store)
        val objects = summary.totalObjects.toLong()
        val totalBytes = summary.totalBytes

        // 单价（美元/GB/月，美元/万次请求）。这是**可替换的输入**，不是信仰——
        // 各家云厂商差异很大，用户应当按自己的账单改这几个数。
        val (gbPrice, putPer10k, getPer10k) = when (cls) {
            "ia", "infrequent" -> Triple(0.0125, 0.01, 0.01)
            "archive" -> Triple(0.004, 0.05, 0.05)
            else -> Triple(0.023, 0.005, 0.0004)
        }
        val gb = totalBytes / 1024.0 / 1024 / 1024
        println("存储类别: $cls")
        println("对象数: $objects")
        println("总字节: $totalBytes (${"%.3f".format(gb)} GB)${if (summary.sizesComplete) "" else "  ← 估算值"}")
        println("存储费用: ${"%.2f".format(gb * gbPrice)} 美元/月")
        println()
        println("请求费用取决于负载，按设计文档 §14.2 的预算估算：")
        println("  稳态点查 1 次 GET/查询 → 每百万次查询 ${"%.2f".format(1_000_000.0 / 10_000 * getPer10k)} 美元")
        println("  小事务提交 ≤1 次 PUT/事务（组提交摊薄后）→ 每百万次写入 ${"%.2f".format(1_000_000.0 / 10_000 * putPer10k)} 美元")
        println()
        println("提示：单价因厂商与区域而异，请按自己的账单核对；本命令的价值在于把")
        println("      「有多少对象、多大」摊开——这两个数字是成本的全部输入。")
    }
}
