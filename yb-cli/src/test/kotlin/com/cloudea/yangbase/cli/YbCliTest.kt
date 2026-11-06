package com.cloudea.yangbase.cli

import com.cloudea.yangbase.objectstore.LocalFileObjectStore
import com.cloudea.yangbase.server.ServerConfig
import com.cloudea.yangbase.server.YangBaseServer
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `yb-cli` 测试。
 *
 * 造数据用的是**真实的 CN**（同一个 `LocalFileObjectStore` 目录），
 * 然后用 CLI 去读它。这样测的是「运维工具能不能看懂数据库真正写下的东西」——
 * 如果只有 CLI 自己造的假数据，测的就是它自己。
 */
class YbCliTest {

    private lateinit var storeDir: java.nio.file.Path
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        storeDir = Files.createTempDirectory("yangbase-cli")
        // 直接注入 LocalFileObjectStore，让 CLI 事后能打开同一个目录
        server = YangBaseServer.forTesting(store = LocalFileObjectStore(storeDir), port = 0)
        server.start()
        port = server.boundPort
        Class.forName("com.mysql.cj.jdbc.Driver")
    }

    @AfterTest
    fun tearDown() {
        runCatching { server.stop() }
        runCatching { storeDir.toFile().deleteRecursively() }
    }

    private fun connect(): Connection = DriverManager.getConnection(
        "jdbc:mysql://127.0.0.1:$port/clidb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        "root", "",
    )

    private fun Connection.st(block: (Statement) -> Unit) = createStatement().use(block)

    /**
     * 跑一次 CLI 并捕获它的输出。
     *
     * 调的是 [YbCli.run] 而不是 `main`：后者在参数错误时会 `exitProcess`，
     * 那会把整个测试 JVM 杀掉。命令与进程退出解耦之后，两边都能测。
     */
    private fun cli(vararg args: String): String {
        val buf = ByteArrayOutputStream()
        val oldOut = System.out
        System.setOut(PrintStream(buf, true, Charsets.UTF_8))
        try {
            YbCli.run(args[0], args.drop(1) + listOf("--fs", storeDir.toString()))
        } finally {
            System.setOut(oldOut)
        }
        return buf.toString(Charsets.UTF_8)
    }

    /**
     * 与 [cli] 相同的捕获输出，但**不追加 `--fs`**。
     *
     * `--fs` 在 `openStore` 里是**优先匹配**的，所以指向真实 S3 端点的用例必须绕开它，
     * 否则会被静默地拽回本地目录（第一次写这条用例就踩了）。
     */
    private fun cliRaw(vararg args: String): String {
        val buf = ByteArrayOutputStream()
        val oldOut = System.out
        System.setOut(PrintStream(buf, true, Charsets.UTF_8))
        try {
            YbCli.run(args[0], args.drop(1))
        } finally {
            System.setOut(oldOut)
        }
        return buf.toString(Charsets.UTF_8)
    }

    private fun seed(rows: Int = 200) {
        connect().use { c ->
            c.st { s ->
                s.execute("CREATE DATABASE IF NOT EXISTS clidb")
                s.execute("USE clidb")
                s.execute("DROP TABLE IF EXISTS t")
                s.execute(
                    """
                    CREATE TABLE t (
                        id BIGINT NOT NULL, v VARCHAR(32), n INT,
                        PRIMARY KEY (id), KEY idx_n (n)
                    )
                    """.trimIndent()
                )
                (1..rows step 100).forEach { start ->
                    val values = (start until minOf(start + 100, rows + 1))
                        .joinToString(",") { "($it,'v$it',${it % 17})" }
                    s.execute("INSERT INTO t VALUES $values")
                }
                // 落盘，否则数据还在 WAL 里，CLI 看到的 SST 会很少
                s.execute("SELECT COUNT(*) FROM t")
            }
        }
        kotlinx.coroutines.runBlocking { server.engine.flushAll() }
    }

    // ── ls ────────────────────────────────────────────────────────

    @Test
    fun `ls 按前缀归组并报告对象数与字节`() {
        seed()
        val out = cli("ls")
        assertTrue(out.contains("合计"), "应输出合计行：\n$out")
        assertTrue(out.contains("shard/*/*/sst/*.dat"), "应能看到数据对象：\n$out")
        assertTrue(out.contains("shard/*/*/manifest/"), "应能看到 manifest：\n$out")
        assertTrue(out.contains("catalog/"), "应能看到 catalog：\n$out")
    }

    /**
     * ★ **容量盘点必须是 0 次额外请求。**
     *
     * `ls` / `cost` 曾经对每个对象发一次 `head`：十万对象的桶就是十万次计费请求，
     * **做一次容量评估比一整天的业务请求还多**。大小现在来自 LIST 响应本身
     * （[com.cloudea.yangbase.objectstore.ListPage.sizes]）。
     *
     * 这条断言守护的正是那个数字——比"输出看起来对"可靠得多。
     */
    @Test
    fun `ls 与 cost 不逐对象发 head`() = kotlinx.coroutines.runBlocking {
        seed()
        val counting = com.cloudea.yangbase.objectstore.CountingObjectStore(
            LocalFileObjectStore(storeDir),
        )
        val summary = YbCli.summarize(counting)
        assertTrue(summary.totalObjects > 10, "应当数到对象：${summary.totalObjects}")
        assertTrue(summary.sizesComplete, "本地 FS 的 LIST 应当直接返回大小")
        assertEquals(0L, counting.count("head"), "容量盘点绝不能逐对象 head")
        assertEquals(0L, counting.count("get"), "容量盘点只需要 LIST")
    }

    /**
     * ★ **`--s3-oss` 与 `--path-style` 必须真的接进 store 的构造**。
     *
     * 这两个开关不是"顺手加的别名"：它们决定客户端**用哪套原语**跟后端说话
     * （原子创建发 `If-None-Match` 还是 `x-oss-forbid-overwrite`、寻址用路径式还是
     * 三级域名）。运维命令里 `gc --delete`、`restore`、`pitr` 都会**写**，
     * 声明错了会被后端直接拒绝（400）——而只读命令不会暴露这个问题，
     * 所以这里用一个真实的 S3 端点把整条构造路径跑一遍。
     */
    @Test
    fun `能用 --s3-oss 与 --path-style 打开 S3 端点`() {
        val s3 = com.cloudea.yangbase.objectstore.EmbeddedS3Server("cli-oss").apply { start() }
        try {
            kotlinx.coroutines.runBlocking {
                val store = com.cloudea.yangbase.objectstore.S3ObjectStore.create(
                    bucket = "cli-oss", endpoint = s3.endpoint, region = "us-east-1",
                    accessKey = "t", secretKey = "t",
                )
                store.put("shard/1001/0/wal/e1/000000000001.log", ByteArray(64))
                store.close()
            }
            val out = cliRaw(
                "ls",
                "--endpoint", s3.endpoint, "--bucket", "cli-oss",
                "--access-key", "t", "--secret-key", "t",
                "--path-style", "true", "--s3-oss",
            )
            assertTrue(out.contains("shard/*/*/wal/"), "应当列出刚才写进去的对象：\n$out")
            assertTrue(out.contains("合计"), "应当有合计行：\n$out")
        } finally {
            runCatching { s3.stop() }
        }
    }

    /**
     * ★ **`restore` 必须在没有 ETag CAS 的后端上真的能跑通**（2026-09-27，评审指出）。
     *
     * 这条用例的来历值得写清楚：上一版只断言了"开关被认出来并告警"——
     * 而开关当时**只传给了 `DatabaseEngine`**，真正切 `manifest/CURRENT` 指针的是
     * `SnapshotManager` 内部的 `ManifestStore`，它有自己的一份开关（默认关）。
     * 于是 `restore --yes` 在 OSS 上照样报"需要条件写能力"，
     * 而测试因为没走到真正切指针那一步（没传 `--yes`）**照样是绿的**。
     *
     * 所以这里做一次**真实的备份 → 恢复往返**：先种一张表与一个分片，
     * 用 CLI 备份，再用 CLI 恢复，断言真的恢复了分片（而不是只看告警文本）。
     */
    @Test
    fun `restore 在没有 CAS 的后端上带上开关才真的能恢复`() {
        val s3 = com.cloudea.yangbase.objectstore.EmbeddedS3Server("cli-restore").apply {
            ossStyle = true
            start()
        }
        val store = com.cloudea.yangbase.objectstore.S3ObjectStore.create(
            bucket = "cli-restore", endpoint = s3.endpoint, region = "us-east-1",
            accessKey = "t", secretKey = "t", pathStyle = true,
            caps = com.cloudea.yangbase.objectstore.ObjectStoreCapabilities.OSS,
        )
        try {
            // ── 种数据：一张表 + 一个已 flush 的分片（不经过 CLI，CLI 没有写 SQL 的命令）
            val engine = com.cloudea.yangbase.txn.DatabaseEngine(
                store,
                kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.Default),
                nodeId = "seed",
            )
            kotlinx.coroutines.runBlocking {
                engine.open()
                val db = engine.catalog.createDatabase("rdb")
                val t = engine.catalog.createTable(
                    db.databaseId, "t",
                    listOf(com.cloudea.yangbase.common.ColumnDef("id", com.cloudea.yangbase.common.ColumnType.BIGINT, false)),
                    listOf("id"),
                )
                // flush 一次 → 写出该分片的 manifest（整份备份只需要这一个引用）
                val shard = engine.allShards(t).single()
                engine.shardStore(shard, t).flush()
                engine.close()
            }

            val base = arrayOf(
                "--endpoint", s3.endpoint, "--bucket", "cli-restore",
                "--access-key", "t", "--secret-key", "t",
                "--path-style", "true", "--s3-oss",
            )
            // ① 备份（只读操作，用不上条件写）
            val backupOut = cliRaw("backup", "--note", "e2e", *base)
            val key = Regex("备份已创建: (\\S+)").find(backupOut)?.groupValues?.get(1)
            assertTrue(key != null, "备份应当成功并打印 key，实际：\n$backupOut")

            // ② 不带开关恢复：必须**明确失败**（切指针要用 If-Match）
            val refused = runCatching {
                cliRaw("restore", "--from", key!!, "--yes", *base)
            }.exceptionOrNull()
            assertTrue(
                refused is com.cloudea.yangbase.common.CapabilityNotSupportedException,
                "没有开关时必须在切指针处明确拒绝，实际：$refused",
            )

            // ③ 带上开关：真的恢复（这一步才是本条用例的重点）
            val restored = cliRaw("restore", "--from", key, "--yes", "--i-accept-nonatomic-pointer-switch", *base)
            assertTrue(
                restored.contains("已恢复 1 个分片"),
                "带开关必须真的恢复那个分片，实际：\n$restored",
            )
        } finally {
            runCatching { store.close() }
            runCatching { s3.stop() }
        }
    }

    @Test
    fun `ls 在空存储上不崩`() {
        val out = cli("ls")
        assertTrue(out.contains("空"), "空存储应明确说「空」而不是报错：\n$out")
    }

    // ── shards ────────────────────────────────────────────────────

    @Test
    fun `shards 报告每张表的分片与行数`() {
        seed()
        val out = cli("shards")
        assertTrue(out.contains("t"), "应列出表名：\n$out")
        assertTrue(out.contains("合计行数: 200"), "应给出总行数：\n$out")
    }

    /**
     * ★ `shards --owner` 必须报得出**当前写者属主**（issue #14）。
     *
     * 前提得是真的：`forTesting` 默认是 `coord-mode=single`，那种部署**不开租约**、
     * 对象存储上一个租约对象也没有。所以这里把 CN 重新装配成 `coord-mode=objectstore`
     * （用本地目录的条件写做 CAS）——否则这条用例测的只是“表格多了一列”，
     * 而不是“运维工具能不能真的告诉使用者该把写请求发给谁”。
     */
    @Test
    fun `shards --owner 报出当前写者属主`() {
        runCatching { server.stop() }
        server = YangBaseServer(
            config = ServerConfig(
                host = "127.0.0.1", port = 0,
                nodeId = "cn-owner-a",
                coordMode = "objectstore",
            ),
            injectedStore = LocalFileObjectStore(storeDir),
        )
        server.start()
        port = server.boundPort

        seed()
        val out = cli("shards", "--owner")
        assertTrue(out.contains("属主"), "--owner 必须多出属主列：\n$out")
        assertTrue(out.contains("cn-owner-a"), "应报出真正持有写者租约的节点：\n$out")
        assertTrue(
            Regex("[1-9]\\d* 个分片有活着的租约").containsMatchIn(out),
            "汇总行应说清楚有多少分片真的持有租约：\n$out",
        )
    }

    /** `--owner` 在**没有租约对象**的存储上要解释“为什么查不到”，而不是让人误以为“没人写”。 */
    @Test
    fun `shards --owner 在没有租约对象时解释原因`() {
        seed()   // 默认 single 模式：不开租约，不会写租约对象
        val out = cli("shards", "--owner")
        assertTrue(out.contains("—"), "没有租约对象时列应为空：\n$out")
        assertTrue(out.contains("coord-mode=single"), "必须解释“查不到”的原因（而不是留白）：\n$out")
    }

    // ── gc ────────────────────────────────────────────────────────

    @Test
    fun `gc 默认只标记不删除`() {
        seed()
        val before = Files.walk(storeDir).filter { Files.isRegularFile(it) }.count()

        val out = cli("gc")
        assertTrue(out.contains("标记"), "默认必须明确说是标记阶段：\n$out")

        val after = Files.walk(storeDir).filter { Files.isRegularFile(it) }.count()
        assertTrue(
            after >= before,
            "标记阶段绝不能减少文件数（之前 $before，之后 $after）——它是只读的",
        )

        // 数据必须完好
        connect().use { c ->
            c.st { s ->
                s.execute("USE clidb")
                assertTrue(s.executeQuery("SELECT COUNT(*) FROM t").use { it.next(); it.getLong(1) } == 200L)
            }
        }
    }

    @Test
    fun `gc 把被 fence 的 WAL 与普通垃圾分开报`() {
        seed()
        val out = cli("gc")
        // 这两个数字混在一起会让「泄漏」看起来比实际严重（设计文档 §6.9）
        assertTrue(
            out.contains("fence") || out.contains("没有可回收"),
            "GC 报告应区分被 fence 的旧写者 WAL：\n$out",
        )
    }

    @Test
    fun `gc 在对象存储上留下可核查的审计记录`() {
        // 默认的 24 小时安全窗口会让**刚写下的对象全部受保护**——那正是它的作用。
        // 因此这里把窗口压到 0，否则「本次标记出了 0 个对象」，也就没什么可审计的。
        // 走的是 `--delete`：它才是真正销毁数据的动作（§12.5 里最需要留痕的一条）。
        //
        // ⚠️ 窗口 < 1 小时需要显式加 `--i-know-what-im-doing`：那是唯一防止
        // 「删掉刚写出、还没进 manifest 的 SST/WAL」的机制（见 YbCli.gc 的说明）
        seed()
        val out = cli("gc", "--delete", "--window-hours", "0", "--i-know-what-im-doing")

        // 审计落点由 CLI 装：只在单元测试里构造过 ObjectStoreAuditLog 是不够的，
        // 线上跑 `yb-cli gc --delete` 时同样要写得出来——这条断言守的是那个接线本身。
        val lines = Files.walk(storeDir)
            .filter { Files.isRegularFile(it) && it.toString().contains("ops/audit") }
            .flatMap { Files.readAllLines(it).stream() }
            .filter { it.isNotBlank() }
            .toList()
        val all = lines.joinToString("\n")
        assertTrue(lines.isNotEmpty(), "gc 必须留下审计记录，命令输出：\n$out")
        for (action in listOf("gc.mark", "gc.advance_watermark", "gc.sweep_delete")) {
            assertTrue(
                lines.any { it.contains("\"action\":\"$action\"") },
                "缺少 $action 记录——「删了什么」必须能从对象存储里查回来：\n$all",
            )
        }
        // 「一行一条 JSON」是 jsonl 能被外部工具直接读的前提，也是这里的存储格式约定
        assertTrue(
            lines.all { it.trimStart().startsWith("{") && it.trimEnd().endsWith("}") },
            "每条记录必须自成一行合法 JSON：\n$all",
        )
        // 删掉的必须只是**不可达**的对象：表本身完好无损
        connect().use { c ->
            c.st { s ->
                s.execute("USE clidb")
                assertTrue(
                    s.executeQuery("SELECT COUNT(*) FROM t").use { it.next(); it.getLong(1) } == 200L,
                    "GC 只应删掉不可达对象，表数据必须完好。命令输出：\n$out",
                )
            }
        }
    }

    // ── recover-check ─────────────────────────────────────────────

    @Test
    fun `recover-check 在健康数据上通过`() {
        seed()
        val out = cli("recover-check")
        assertTrue(
            out.contains("健康") || out.contains("✓"),
            "健康的数据不该报问题：\n$out",
        )
    }

    @Test
    fun `recover-check 能发现被删掉的 SST 数据对象`() {
        seed()
        // 手动制造「manifest 引用了但对象不存在」——这是最危险的一类损坏，
        // 因为它不会在启动时报错，只会在读那一块数据时才炸。
        //
        // ⚠️ 这里必须删**全部** `.dat`，不能只删 `Files.walk` 找到的第一个：
        // 一个分片可能已经被 flush/compaction 多轮，目录里同时存在**旧版本**的 SST，
        // 而旧 SST 不再被当前 manifest 引用 —— 全部删掉的话，旧的自然无害，
        // 被删掉的“当前” SST 一定会被报出来。
        // 只删一个会因文件系统 walk 顺序而随机失败（在 CI 的 overlayfs 上就翻车过）。
        val victims = Files.walk(storeDir).use { s ->
            s.filter { it.toString().endsWith(".dat") }.toList()
        }
        require(victims.isNotEmpty()) { "测试需要至少一个 .dat 对象" }
        victims.forEach { Files.delete(it) }

        val out = cli("recover-check")
        assertTrue(
            out.contains("缺失对象") || out.contains("✗"),
            "被 manifest 引用但缺失的对象必须被报出来（已删 ${victims.size} 个 .dat）：\n$out",
        )
    }

    // ── cost ──────────────────────────────────────────────────────

    @Test
    fun `cost 给出可核对的数字`() {
        seed()
        val out = cli("cost")
        assertTrue(out.contains("对象数"), "应报告对象数：\n$out")
        assertTrue(out.contains("总字节"), "应报告总字节：\n$out")
        assertTrue(out.contains("美元/月"), "应给出月度存储费用：\n$out")
    }

    // ── 未知命令 ──────────────────────────────────────────────────

    @Test
    fun `未知命令抛出明确错误而不是静默成功`() {
        // 静默成功是最糟的失败方式：运维脚本会以为清理成功了
        val e = assertFailsWith<CliError> { cli("no-such-command") }
        assertTrue(e.message!!.contains("未知命令"), "错误信息应说清是什么问题：${e.message}")
    }
}
