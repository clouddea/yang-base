package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.PreconditionFailedException
import java.util.UUID

/**
 * 运行时能力探测。
 *
 * **为什么必须实测而不是相信厂商文档**：本系统的正确性建立在条件写之上
 * （写者选举、manifest 指针切换、fencing）。而不同 S3 兼容后端在这上面的
 * 行为差异很大，且**故障模式是静默的**——典型例子是 GCS 的 S3 互操作层会
 * **直接忽略**写请求上的条件头：请求成功返回，但前置条件根本没生效。
 * 这种后端上跑 YangBase 会在并发下损坏数据，却不报任何错。
 *
 * 因此 [probe] 用**真实的冲突写请求**去问后端，而不是读它的文档
 * （设计文档 §3.3.1）。
 *
 * 探测在隔离的键前缀下进行，结束后清理，不在数据面上留下痕迹。
 */
object CapabilityProbe {

    data class Result(
        val capabilities: ObjectStoreCapabilities,
        /** 逐项的实测结论，用于日志与诊断。 */
        val findings: List<Finding>,
        val probeMillis: Long,
    ) {
        val allCriticalPassed: Boolean
            get() = findings.filter { it.critical }.all { it.passed }

        fun report(): String = buildString {
            append("能力探测 (${probeMillis}ms): ")
            append(findings.joinToString(", ") { "${it.name}=${if (it.passed) "✓" else "✗"}" })
            val failed = findings.filter { !it.passed }
            if (failed.isNotEmpty()) {
                append("\n  ⚠️ 未通过: ")
                append(failed.joinToString("; ") { "${it.name} — ${it.detail}" })
            }
        }
    }

    data class Finding(
        val name: String,
        val passed: Boolean,
        val detail: String,
        /** 关键能力缺失会导致数据损坏，而不只是性能下降。 */
        val critical: Boolean,
        /**
         * 这一项失败**允许被显式接受**（用部署开关换取可用性）。
         *
         * 与 [critical] 的区别：critical 说的是"缺了它会静默损坏数据"，
         * degradable 说的是"它确实缺，但可以知情的降级"。
         * 目前只有一项：后端**如实声明**不支持 ETag CAS（阿里云 OSS）——
         * 在那种后端上跑的前提是显式打开
         * `--i-accept-nonatomic-pointer-switch`，而且**绝不**能对
         * "声称支持却静默忽略"（GCS 互操作层）那一类放行。
         */
        val degradable: Boolean = false,
    )

    /**
     * 实测后端能力。
     *
     * @param base 已构建好的 store；探测只用它，不改它的配置
     * @param probePrefix 探测用的键前缀。生产环境应当给一个可被 GC 忽略的前缀
     */
    suspend fun probe(
        store: ObjectStore,
        probePrefix: String = ".yb-probe/${UUID.randomUUID()}",
    ): Result {
        val started = System.currentTimeMillis()
        val findings = ArrayList<Finding>()
        val k = "$probePrefix/probe"

        // ── 1. 基本读写 ────────────────────────────────────────────
        val payload = "yangbase-probe".toByteArray()
        var basicOk = true
        try {
            store.put(k, payload)
            val back = store.get(k)
            basicOk = back.contentEquals(payload)
        } catch (_: Exception) {
            basicOk = false
        }
        findings += Finding("读写", basicOk, "PUT 后 GET 应返回相同内容", critical = true)

        // ── 2. 读己之写（强一致读）────────────────────────────────
        // 弱一致的后端上，刚写完立刻读可能读不到——那会让 MVCC 的可见性论证失效
        var rawOk = false
        if (basicOk) {
            try {
                // 连续覆盖写并立刻读，弱一致后端有一定概率读到旧值
                var consistent = true
                for (i in 1..5) {
                    val v = "raw-$i".toByteArray()
                    store.put(k, v)
                    if (!store.get(k).contentEquals(v)) { consistent = false; break }
                }
                rawOk = consistent
            } catch (_: Exception) {
                rawOk = false
            }
        }
        findings += Finding("读己之写", rawOk, "覆盖写后立刻读必须返回新值", critical = true)

        // ── 3. 条件创建（If-None-Match: *）────────────────────────
        // 关键判定：对**已存在**的对象做条件创建，必须失败。
        // 若成功返回，说明后端忽略了条件头 —— 这就是 GCS 互操作层的故障模式。
        var condCreate = false
        var condCreateDetail = "前置步骤失败"
        if (basicOk) {
            try {
                store.put(k, "should-not-land".toByteArray(), Precondition.MustNotExist)
                // 到这里说明写入"成功"了 —— 但对象本来就存在，所以条件被忽略了
                condCreate = false
                condCreateDetail = "对已存在的对象，MustNotExist 竟然成功了 —— 后端**忽略了条件头**，" +
                    "在这种后端上运行会导致并发写损坏数据"
            } catch (_: PreconditionFailedException) {
                condCreate = true
                condCreateDetail = "返回 PreconditionFailed，条件生效"
            } catch (_: ConcurrentModificationException) {
                condCreate = true
                condCreateDetail = "返回 Conflict，条件生效"
            } catch (e: Exception) {
                // 其他错误码（如 404/501）说明未实现条件写
                condCreate = false
                condCreateDetail = "非预期错误: ${e::class.simpleName} ${e.message}"
            }
        }
        findings += Finding("条件创建 If-None-Match", condCreate, condCreateDetail, critical = true)

        // ── 4. 比较交换（If-Match: <etag>）────────────────────────
        var condUpdate = false
        var condUpdateDetail = "前置步骤失败"
        var honestNoCas = false
        if (basicOk && condCreate) {
            try {
                store.put(k, "v1".toByteArray())
                val etag = store.head(k).etag
                // 正确的 etag 应能交换成功
                store.put(k, "v2".toByteArray(), Precondition.MustMatch(etag))
                val nowV2 = store.get(k).contentEquals("v2".toByteArray())
                // 陈旧的 etag 必须被拒绝
                var staleRejected = false
                try {
                    store.put(k, "v3".toByteArray(), Precondition.MustMatch(etag))
                } catch (_: PreconditionFailedException) {
                    staleRejected = true
                } catch (_: ConcurrentModificationException) {
                    staleRejected = true
                }
                condUpdate = nowV2 && staleRejected
                condUpdateDetail = when {
                    !nowV2 -> "etag 匹配时写入未生效"
                    !staleRejected -> "**陈旧 etag 的写入被接受了** —— fencing 在这种后端上失效"
                    else -> "etag 匹配时成功、陈旧时被拒，CAS 语义完整"
                }
                honestNoCas = false
            } catch (e: com.cloudea.yangbase.common.CapabilityNotSupportedException) {
                // ★ **如实声明**不支持（阿里云 OSS 就是这样：`If-Match` 直接 400
                // `NotImplemented`）。这与"声称支持却静默忽略"是**两回事**：
                // 后者必须拒绝启动（GCS 互操作层），前者可以在知情的前提下降级
                honestNoCas = true
                condUpdateDetail = "后端如实声明不支持 If-Match（无 ETag CAS）——" +
                    "manifest 指针切换需要显式接受非原子降级（--i-accept-nonatomic-pointer-switch）"
            } catch (e: Exception) {
                condUpdateDetail = "非预期错误: ${e::class.simpleName} ${e.message}"
            }
        }
        findings += Finding(
            "比较交换 If-Match", condUpdate, condUpdateDetail,
            critical = true,
            // 只有"后端自己说没有"才可降级；探测到的其它失败一律视为致命
            degradable = honestNoCas,
        )

        // ── 5. Range 读 ───────────────────────────────────────────
        var rangeOk = false
        var rangeDetail = "前置步骤失败"
        if (basicOk) {
            try {
                val data = ByteArray(1000) { (it % 256).toByte() }
                store.put(k, data)
                val slice = store.get(k, ByteRange(100, 199))
                rangeOk = slice.size == 100 &&
                    (0 until 100).all { slice[it] == data[100 + it] }
                rangeDetail = if (rangeOk) "返回精确的字节区间" else "返回的字节不匹配"
            } catch (e: Exception) {
                // 带上 message 与最内层原因：只写一个异常类名会让排查
                // 「这个后端到底哪里不支持」变成猜谜（真实的例子：
                // 阿里云 OSS 上报的是 `UncheckedIOException`，而 OSS 本身是支持 Range 的）
                rangeDetail = "不支持: ${e::class.simpleName} ${e.message}" +
                    (e.cause?.let { " ← ${it::class.simpleName}: ${it.message}" } ?: "")
            }
        }
        findings += Finding("Range 读", rangeOk, rangeDetail, critical = false)

        // ── 6. LIST 一致性与分页 ──────────────────────────────────
        var listOk = false
        var listDetail = "前置步骤失败"
        if (basicOk) {
            try {
                for (i in 1..12) store.put("$probePrefix/pg/${"%03d".format(i)}", ByteArray(0))
                // 刚写完就列举：弱一致的 LIST 可能看不到
                val all = store.listAll("$probePrefix/pg/")
                val sortedOk = all == all.sorted()
                // 分页：每页 5 个，应能跨页取全
                var token: String? = null
                val paged = ArrayList<String>()
                do {
                    val page = store.list("$probePrefix/pg/", token, limit = 5)
                    paged.addAll(page.keys)
                    token = page.nextToken
                } while (token != null)
                listOk = all.size == 12 && sortedOk && paged.size == 12
                listDetail = when {
                    all.size != 12 -> "刚写入的对象在 LIST 中不可见（弱一致 LIST）"
                    !sortedOk -> "LIST 未按字典序返回"
                    else -> "强一致且有序，分页正确"
                }
            } catch (e: Exception) {
                listDetail = "不支持: ${e::class.simpleName} ${e.message}"
            }
        }
        findings += Finding("LIST", listOk, listDetail, critical = true)

        // NOTE：这里曾经还有第 7 项「服务端拼接」（`compose`）。它已从 SPI 中移除——
        // 探测一个没有任何数据路径会用的能力，只会让启动报告多一行噪音。
        // 移除的理由（两层布局没有可原样保留的区间、S3 的 5 MiB 最小片长、
        // 没有整对象复制的调用点）记在 docs/yangbase-design.md §3.2 的决策记录里。

        // ── 清理 ──────────────────────────────────────────────────
        runCatching { store.listAll(probePrefix).forEach { store.delete(it) } }

        val caps = ObjectStoreCapabilities(
            conditionalCreate = condCreate,
            conditionalUpdate = condUpdate,
            strongReadAfterWrite = rawOk,
            strongListConsistency = listOk,
            // 低延迟层的能力无法在通用后端上探测，保持保守
            lowLatencyTier = false,
            lowLatencyTierSupportsConditionalWrite = false,
            // ★ 这一项现在是真的测出来的，不是抄文档
            conditionalWriteVerified = condCreate && condUpdate,
        )
        return Result(caps, findings, System.currentTimeMillis() - started)
    }

    /** 在已有能力声明的基础上，用实测结果覆盖可探测的几项。 */
    suspend fun probeAndMerge(store: ObjectStore, declared: ObjectStoreCapabilities): Result {
        val r = probe(store)
        return r.copy(
            capabilities = declared.copy(
                conditionalCreate = declared.conditionalCreate && r.capabilities.conditionalCreate,
                conditionalUpdate = declared.conditionalUpdate && r.capabilities.conditionalUpdate,
                strongReadAfterWrite = declared.strongReadAfterWrite && r.capabilities.strongReadAfterWrite,
                strongListConsistency = declared.strongListConsistency && r.capabilities.strongListConsistency,
                conditionalWriteVerified = r.capabilities.conditionalWriteVerified,
            )
        )
    }
}
