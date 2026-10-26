package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.CapabilityNotSupportedException
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.PreconditionFailedException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import java.nio.file.Files
import java.util.UUID
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * **后端一致性（conformance）套件**：同一套用例跑遍所有 `ObjectStore` 后端。
 *
 * ## 为什么需要它
 *
 * 本系统把「后端行为」当作正确性的地基：写者选举、manifest 指针切换、fencing
 * 全都建立在条件写语义上。而条件写是**各厂商实现差异最大**的地方——
 * OSS 直接拒绝 `If-None-Match`、GCS 的互操作层会**静默忽略**条件头、
 * R2 的 LIST 是最终一致的。这些差异只要有一个没被识别出来，
 * 表现就不是「某个功能不好用」，而是**并发下静默损坏数据**。
 *
 * 所以这里不问「它实现了什么」，只问两件事：
 *
 * 1. **声明的能力与真实行为是否一致**——声明支持却做不到的，必须当场炸；
 * 2. **声明的能力之外，语义是否仍然是同一个数据库能依赖的语义**（键的字符集、
 *    分页不重不漏、删除幂等、ETag 变化……）。
 *
 * ## 怎么跑不同的后端
 *
 * | 后端 | 触发方式 | 说明 |
 * |---|---|---|
 * | 内存 | 总是 | 语义基准 |
 * | 本地文件 | 总是 | 落盘与 ETag 语义 |
 * | 内嵌 S3（标准风格） | 总是 | 走真实 HTTP + SDK |
 * | 内嵌 S3（OSS 风格） | 总是 | 拒绝 `If-None-Match`、只认 `x-oss-forbid-overwrite` |
 * | 真实后端 | 设 `YANGBASE_CONFORMANCE_ENDPOINT` 等环境变量 | MinIO / OSS / COS / OBS / R2 / B2 |
 *
 * 真实后端的环境变量：
 *
 * ```text
 * YANGBASE_CONFORMANCE_ENDPOINT   例 https://oss-cn-hangzhou.aliyuncs.com
 * YANGBASE_CONFORMANCE_BUCKET     必填
 * YANGBASE_CONFORMANCE_ACCESS_KEY 必填
 * YANGBASE_CONFORMANCE_SECRET_KEY 必填
 * YANGBASE_CONFORMANCE_REGION     默认 us-east-1
 * YANGBASE_CONFORMANCE_STYLE      `oss` = 阿里云 OSS 能力预设；默认 = S3 全能力
 * YANGBASE_CONFORMANCE_PATH_STYLE `true` / `false`；默认按 endpoint 自动
 * YANGBASE_CONFORMANCE_NAME       矩阵里显示的名字，默认 endpoint 的 host
 * ```
 *
 * ## 产出
 *
 * 跑完写一份 Markdown 矩阵到 `build/reports/conformance/matrix.md`，
 * 行是后端、列是用例，格子是 `✅ / ⊘（跳过 + 原因）/ ❌`。
 * CI 的 job summary 里直接贴这一份——「兼容矩阵」于是**不是手写的文档**，
 * 而是每次 CI 的真实运行结果。
 */
class ObjectStoreConformanceTest {

    // ── 后端 ──────────────────────────────────────────────────────

    /**
     * 一个被测后端。
     *
     * [declared] 是后端**自己声明**的能力。套件用它决定「跳过」还是「必须通过」：
     * 声明了不支持的能力，用例跳过并记下原因（显式降级）；
     * 声明了支持的能力，用例必须通过——否则就是「声称支持却没做到」，
     * 而那正是会在生产上静默损坏数据的那一类。
     */
    private class Rig(
        val name: String,
        val declared: ObjectStoreCapabilities,
        val open: () -> ObjectStore,
        val close: (ObjectStore) -> Unit = {},
        /** 真实后端要额外说明自己是怎么配的，矩阵里带上，便于复现。 */
        val note: String = "",
    )

    // ── 用例 ──────────────────────────────────────────────────────

    private class Check(
        val name: String,
        /**
         * 返回 null 表示这个能力后端没声明、用例跳过；返回字符串表示跳过原因。
         * 用「跳过原因」而不是布尔值，是因为矩阵里需要写出**为什么**跳过——
         * 「⊘ 没有 ETag CAS」与「⊘ 不知道」是两件不同的事。
         */
        val skip: (ObjectStoreCapabilities) -> String? = { null },
        val body: suspend (ObjectStore, String) -> Unit,
    )

    private val checks: List<Check> = listOf(
        Check("put/get roundtrip") { s, p ->
            s.put("$p/a", "hello 世界".toByteArray())
            assertEquals("hello 世界", String(s.get("$p/a")))
        },
        Check("empty object") { s, p ->
            s.put("$p/empty", ByteArray(0))
            assertEquals(0, s.get("$p/empty").size)
        },
        Check("1 MiB object") { s, p ->
            val big = Random(7).nextBytes(1024 * 1024)
            s.put("$p/big", big)
            assertTrue(big.contentEquals(s.get("$p/big")), "大对象内容必须逐字节一致")
        },
        Check("get missing object") { s, p ->
            try {
                s.get("$p/missing-$p")
                fail("读取不存在的对象必须抛 ObjectNotFoundException，而不是返回空内容")
            } catch (_: ObjectNotFoundException) {
                // 期望
            }
        },
        Check("HEAD metadata") { s, p ->
            val etag = s.put("$p/h", ByteArray(42))
            val m = s.head("$p/h")
            assertEquals(42L, m.size)
            assertEquals(etag.value, m.etag.value, "HEAD 的 ETag 必须与 PUT 返回的一致，否则 CAS 无法用")
            assertTrue(m.lastModifiedMillis > 0, "必须给出修改时间")
        },
        Check("overwrite changes ETag") { s, p ->
            val e1 = s.put("$p/ow", "a".toByteArray())
            val e2 = s.put("$p/ow", "b".toByteArray())
            assertTrue(e1 != e2, "覆盖写必须改变 ETag，否则 CAS 形同虚设")
            assertEquals("b", String(s.get("$p/ow")))
        },
        Check("delete is idempotent") { s, p ->
            s.put("$p/d", "x".toByteArray())
            assertTrue(s.delete("$p/d"), "第一次删除应返回 true")
            assertFalse(s.delete("$p/d"), "删不存在的对象必须返回 false，而不是抛异常")
        },
        Check("range read") { s, p ->
            val data = "0123456789".toByteArray()
            s.put("$p/r", data)
            assertEquals("0", String(s.get("$p/r", ByteRange(0, 0))), "首字节")
            assertEquals("456", String(s.get("$p/r", ByteRange(4, 6))), "中间")
            assertEquals("9", String(s.get("$p/r", ByteRange(9, 9))), "尾字节")
            assertEquals("0123456789", String(s.get("$p/r", ByteRange(0, 9))), "整对象")
        },
        Check("LIST prefix + lexicographic order") { s, p ->
            val keys = listOf("$p/l/b", "$p/l/a", "$p/l/c").sorted()
            keys.forEach { s.put(it, it.toByteArray()) }
            s.put("$p/other/x", "x".toByteArray())
            val page = s.list("$p/l/", null, 1000)
            assertEquals(keys, page.keys, "LIST 必须按键字典序返回，且只返回前缀匹配的键")
        },
        Check("LIST pagination (no dup, no gap)") { s, p ->
            val n = 7
            val expected = (0 until n).map { "$p/pg/$it" }.sorted()
            expected.forEach { s.put(it, it.toByteArray()) }
            val seen = ArrayList<String>()
            var token: String? = null
            var guard = 0
            do {
                val page = s.list("$p/pg/", token, 2)
                seen += page.keys
                token = page.nextToken
                assertTrue(++guard <= n + 2, "分页没有终止：LIMIT 被忽略或 token 不前进")
            } while (token != null)
            assertEquals(expected, seen, "分页必须不重不漏，且保持字典序")
        },
        Check("LIST sizes correct or absent") { s, p ->
            s.put("$p/sz/a", ByteArray(10))
            s.put("$p/sz/b", ByteArray(20))
            val sizes = s.list("$p/sz/", null, 100).sizes
            // 契约：能免费拿到就填，拿不到就留空；填了就必须对
            sizes.forEach { (k, v) ->
                assertEquals(s.head(k).size, v, "LIST 里给出的大小必须与 HEAD 一致：$k")
            }
        },
        Check("key charset roundtrip") { s, p ->
            val keys = listOf(
                "$p/cn/中文键",
                "$p/cn/with space",
                "$p/cn/plus+percent%",
                "$p/cn/a/b/c",
                "$p/cn/tilde~dash-under_score",
            )
            keys.forEach { s.put(it, it.toByteArray()) }
            keys.forEach { assertEquals(it, String(s.get(it)), "键必须原样往返：$it") }
        },
        Check("read-your-writes",
            skip = { if (it.strongReadAfterWrite) null else "后端声明读己之写最终一致" },
        ) { s, p ->
            s.put("$p/ryw", "v".toByteArray())
            assertEquals(listOf("$p/ryw"), s.list("$p/ryw", null, 10).keys, "写完立刻 LIST 必须看得到")
        },
        Check("delete visible in LIST immediately",
            skip = { if (it.strongListConsistency) null else "后端声明 LIST 最终一致" },
        ) { s, p ->
            s.put("$p/del", "v".toByteArray())
            s.delete("$p/del")
            assertTrue(s.list("$p/del", null, 10).keys.isEmpty(), "删除后 LIST 必须立刻看不到")
        },
        Check("atomic create: exactly one winner",
            skip = { if (it.conditionalCreate) null else "后端声明不支持原子创建" },
        ) { s, p ->
            val key = "$p/cas"
            val wins = coroutineScope {
                (1..8).map { i ->
                    async {
                        runCatching { s.put(key, "writer-$i".toByteArray(), Precondition.MustNotExist) }.isSuccess
                    }
                }.awaitAll()
            }
            assertEquals(1, wins.count { it }, "MustNotExist 并发写必须有且只有一个成功（实际成功 ${wins.count { it }} 个）")
        },
        Check("conditional update: stale ETag fails",
            skip = { if (it.conditionalUpdate) null else "后端声明没有 ETag CAS（如阿里云 OSS）" },
        ) { s, p ->
            val e1 = s.put("$p/cu", "v1".toByteArray())
            s.put("$p/cu", "v2".toByteArray())
            // 断言的是**不变式**：写入必须失败、内容必须不变。
            // 至于抛哪一种异常，各后端目前并不统一（见 issue #8：内存/本地 FS 抛
            // ConcurrentModificationException，S3 把 412 映射成 PreconditionFailedException），
            // 所以这里两者都接受，但必须抛其中之一——静默成功是最危险的那种"兼容"。
            val thrown = runCatching {
                s.put("$p/cu", "v3".toByteArray(), Precondition.MustMatch(e1))
            }.exceptionOrNull()
            assertTrue(
                thrown is PreconditionFailedException || thrown is com.cloudea.yangbase.common.ConcurrentModificationException,
                "用过期 ETag 写入必须被拒绝，实际：${thrown ?: "成功（静默忽略前置条件！）"}",
            )
            assertEquals("v2", String(s.get("$p/cu")), "被拒绝的写入不能改动内容")
            val e2 = s.head("$p/cu").etag
            s.put("$p/cu", "v4".toByteArray(), Precondition.MustMatch(e2))
            assertEquals("v4", String(s.get("$p/cu")), "用当前 ETag 写入必须成功")
        },
        Check("undeclared precondition rejected") { s, p ->
            // 这条**不跳过**：声明了不支持的后端，遇到条件写必须抛
            // CapabilityNotSupportedException。静默忽略比报错危险得多——
            // 它会让系统在并发下破坏线性化却不报任何错。
            val caps = s.caps
            if (!caps.conditionalCreate) {
                try {
                    s.put("$p/nogo", "v".toByteArray(), Precondition.MustNotExist)
                    fail("后端声明不支持原子创建，却让条件写成功了——这类静默忽略会损坏数据")
                } catch (_: CapabilityNotSupportedException) {
                    // 期望
                }
            } else if (!caps.conditionalUpdate) {
                val e = s.put("$p/nogo2", "v".toByteArray())
                try {
                    s.put("$p/nogo2", "v2".toByteArray(), Precondition.MustMatch(e))
                    fail("后端声明不支持 ETag CAS，却让条件更新成功了——必须显式拒绝")
                } catch (_: CapabilityNotSupportedException) {
                    // 期望
                }
            }
        },
    )

    // ── 跑起来 ────────────────────────────────────────────────────

    @TestFactory
    fun conformance(): List<DynamicTest> = rigs().map { rig ->
        DynamicTest.dynamicTest("conformance · ${rig.name}") {
            if (rig.note.isNotBlank()) note(rig.name, rig.note)
            val store = rig.open()
            // 声明的东西必须与实例上的一致：后端若在装配时"偷偷"降级或升级能力，
            // 上层按声明做的判断就全错了，而这种错不会以异常的形式出现。
            assertEquals(rig.declared, store.caps, "后端声明的能力与实例暴露的能力不一致")
            val prefix = "conf/${UUID.randomUUID().toString().take(8)}"
            try {
                val failures = ArrayList<String>()
                for (check in checks) {
                    val skip = check.skip(store.caps)
                    if (skip != null) {
                        record(rig.name, check.name, Outcome.SKIP, skip)
                        continue
                    }
                    try {
                        runBlocking { check.body(store, prefix) }
                        record(rig.name, check.name, Outcome.PASS, "")
                    } catch (e: Throwable) {
                        record(rig.name, check.name, Outcome.FAIL, e.message ?: e::class.simpleName ?: "?")
                        failures += "  ✗ ${check.name}：${e.message ?: e::class.simpleName}\n${e.stackTraceToString().lines().take(6).joinToString("\n") { "      $it" }}"
                    }
                }
                if (failures.isNotEmpty()) {
                    fail("后端「${rig.name}」有 ${failures.size} 项不符合契约：\n${failures.joinToString("\n")}")
                }
            } finally {
                cleanup(store, prefix)
                rig.close(store)
            }
        }
    }

    /** 把自己写进去的键删干净。真实后端上这一步是**省钱**，不是洁癖。 */
    private fun cleanup(store: ObjectStore, prefix: String) {
        runCatching {
            runBlocking {
                var token: String? = null
                do {
                    val page = store.list(prefix, token, 1000)
                    page.keys.forEach { runCatching { store.delete(it) } }
                    token = page.nextToken
                } while (token != null)
            }
        }
    }

    // ── 后端清单 ──────────────────────────────────────────────────

    private fun rigs(): List<Rig> {
        val list = ArrayList<Rig>()

        list += Rig(
            name = "memory",
            declared = MemoryObjectStore().caps,
            open = { MemoryObjectStore() },
        )

        list += Rig(
            name = "local-fs",
            declared = LocalFileObjectStore(Files.createTempDirectory("conf-localfs")).caps,
            open = { LocalFileObjectStore(Files.createTempDirectory("conf-localfs")) },
        )

        list += Rig(
            name = "embedded-s3",
            declared = ObjectStoreCapabilities.FULL,
            open = {
                val s3 = EmbeddedS3Server("conf-s3").apply { start() }
                S3ObjectStore.create(
                    bucket = "conf-s3",
                    endpoint = s3.endpoint,
                    region = "us-east-1",
                    accessKey = "t",
                    secretKey = "t",
                    caps = ObjectStoreCapabilities.FULL,
                ).also { store -> holder[store] = s3 }
            },
            close = { store ->
                runCatching { (store as S3ObjectStore).close() }
                runCatching { holder.remove(store)?.stop() }
            },
        )

        // OSS 风格的内嵌后端：拒绝 If-None-Match、只认 x-oss-forbid-overwrite。
        // 它证明「声明式降级」这条路真的走得通，而不是纸上谈兵。
        list += Rig(
            name = "embedded-s3(oss-style)",
            declared = ObjectStoreCapabilities.OSS,
            open = {
                val s3 = EmbeddedS3Server("conf-oss").apply { ossStyle = true; start() }
                S3ObjectStore.create(
                    bucket = "conf-oss",
                    endpoint = s3.endpoint,
                    region = "us-east-1",
                    accessKey = "t",
                    secretKey = "t",
                    pathStyle = true,
                    caps = ObjectStoreCapabilities.OSS,
                ).also { store -> holder[store] = s3 }
            },
            close = { store ->
                runCatching { (store as S3ObjectStore).close() }
                runCatching { holder.remove(store)?.stop() }
            },
        )

        realRig()?.let { list += it }
        return list
    }

    /** 内嵌 S3 服务器与它产出的 store 的配对（close 时要一起收）。 */
    private val holder = HashMap<ObjectStore, EmbeddedS3Server>()

    /**
     * 真实后端：只由环境变量触发。
     *
     * 默认**不跑**——CI 上没有云凭据，也不该有。但同一套用例
     * 在本地/带凭据的环境里必须能一键跑起来，否则「多云端兼容」
     * 就只是文档上的一句话。
     */
    private fun realRig(): Rig? {
        val endpoint = System.getenv("YANGBASE_CONFORMANCE_ENDPOINT") ?: return null
        val bucket = System.getenv("YANGBASE_CONFORMANCE_BUCKET") ?: return null
        val ak = System.getenv("YANGBASE_CONFORMANCE_ACCESS_KEY") ?: return null
        val sk = System.getenv("YANGBASE_CONFORMANCE_SECRET_KEY") ?: return null
        val region = System.getenv("YANGBASE_CONFORMANCE_REGION") ?: "us-east-1"
        val style = System.getenv("YANGBASE_CONFORMANCE_STYLE")?.lowercase()
        val pathStyle = System.getenv("YANGBASE_CONFORMANCE_PATH_STYLE")?.toBooleanStrictOrNull()
        val name = System.getenv("YANGBASE_CONFORMANCE_NAME")
            ?: URIish(endpoint).host
            ?: "real-backend"
        val caps = if (style == "oss") ObjectStoreCapabilities.OSS else ObjectStoreCapabilities.FULL

        return Rig(
            name = "real:$name",
            declared = caps,
            note = "endpoint=$endpoint bucket=$bucket region=$region style=${style ?: "s3"} " +
                "pathStyle=${pathStyle ?: "auto"}",
            open = {
                S3ObjectStore.create(
                    bucket = bucket,
                    endpoint = endpoint,
                    region = region,
                    accessKey = ak,
                    secretKey = sk,
                    pathStyle = pathStyle,
                    caps = caps,
                )
            },
            close = { store -> runCatching { (store as S3ObjectStore).close() } },
        )
    }

    /** 只为从 endpoint 里取 host，不引入更多依赖。 */
    private class URIish(val raw: String) {
        val host: String? get() = runCatching { java.net.URI.create(raw).host }.getOrNull()
    }

    // ── 结果与矩阵 ────────────────────────────────────────────────

    private enum class Outcome(val mark: String) {
        PASS("✅"),
        SKIP("⊘"),
        FAIL("❌"),
    }

    private data class Cell(val outcome: Outcome, val detail: String)

    companion object {
        private val results = LinkedHashMap<String, LinkedHashMap<String, Cell>>()
        private val checkOrder = ArrayList<String>()
        private val rigNotes = LinkedHashMap<String, String>()

        @Synchronized
        private fun record(rig: String, check: String, outcome: Outcome, detail: String) {
            results.getOrPut(rig) { LinkedHashMap() }[check] = Cell(outcome, detail)
            if (check !in checkOrder) checkOrder += check
        }

        @Synchronized
        private fun note(rig: String, text: String) {
            rigNotes[rig] = text
        }

        @JvmStatic
        @AfterAll
        fun writeMatrix() {
            if (results.isEmpty()) return
            val sb = StringBuilder()
            sb.appendLine("# Backend conformance matrix")
            sb.appendLine()
            sb.appendLine("Generated by `ObjectStoreConformanceTest` at ${java.time.Instant.now()}.")
            sb.appendLine("Rows are backends, columns are checks. `⊘` = the backend declares it does not")
            sb.appendLine("support this capability (explicit degradation), `❌` = it declares support but the")
            sb.appendLine("behaviour does not match — that is the class of bug that silently corrupts data.")
            sb.appendLine()
            sb.append("| backend |")
            checkOrder.forEach { sb.append(" ${it.replace("|", "\\|")} |") }
            sb.appendLine()
            sb.append("|---|")
            checkOrder.forEach { sb.append("---|") }
            sb.appendLine()
            for ((rig, cells) in results) {
                sb.append("| **$rig** |")
                checkOrder.forEach { c ->
                    val cell = cells[c]
                    sb.append(
                        when {
                            cell == null -> " · |"
                            cell.outcome == Outcome.SKIP -> " ${cell.outcome.mark} ${cell.detail.replace("|", "\\|")} |"
                            cell.outcome == Outcome.FAIL -> " ${cell.outcome.mark} |"
                            else -> " ${cell.outcome.mark} |"
                        },
                    )
                }
                sb.appendLine()
            }
            rigNotes.forEach { (rig, note) -> if (note.isNotBlank()) sb.appendLine("> `$rig`: $note") }

            val out = java.io.File(
                System.getProperty("yb.conformance.matrix") ?: "build/reports/conformance/matrix.md",
            )
            out.parentFile?.mkdirs()
            out.writeText(sb.toString())
            println(sb)
        }
    }
}
