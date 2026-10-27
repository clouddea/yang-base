package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.CapabilityNotSupportedException

/**
 * 「仅当对象不存在时创建」这个原语**怎么表达**。
 *
 * 语义相同、协议不同，而差异是**服务端强加的**：
 *
 *  - [IF_NONE_MATCH]：`If-None-Match: *`（AWS S3 / MinIO / R2 / 本地 FS）。
 *    条件不成立时返回 **412**。
 *  - [FORBID_OVERWRITE]：`x-oss-forbid-overwrite: true`（阿里云 OSS）。
 *    OSS **不认** `If-None-Match`——实测返回 400（是拒绝，不是忽略）；
 *    它自己的原语是请求头 + **409 FileAlreadyExists**。
 *
 * 两条路的错误码不一样，所以映射必须跟着风格走：把 409 一律当成
 * 「并发冲突、可重试」会让"对象已存在"这个**确定结论**被反复重发。
 */
enum class AtomicCreateStyle {
    IF_NONE_MATCH,
    FORBID_OVERWRITE,
}

/**
 * 后端能力的声明式描述。连接建立时探测一次，之后全局只读。
 *
 * **任何依赖某项能力的代码路径都必须先检查，否则视为 bug。**
 * 见设计文档 §3.1 与 §3.3.1。
 */
data class ObjectStoreCapabilities(
    /** 支持原子创建（具体协议见 [atomicCreateStyle]） */
    val conditionalCreate: Boolean,
    /** 原子创建的协议形式。只有 [conditionalCreate] 为真时才有意义 */
    val atomicCreateStyle: AtomicCreateStyle = AtomicCreateStyle.IF_NONE_MATCH,
    /** 支持 `If-Match: <etag>` 的原子比较交换 */
    val conditionalUpdate: Boolean,
    /** 读己之写强一致（S3 自 2020-12 起成立） */
    val strongReadAfterWrite: Boolean,
    /** LIST 的强一致性与有序性。R2 为 false（最终一致） */
    val strongListConsistency: Boolean,
    val maxObjectSize: Long = 5L * 1024 * 1024 * 1024,
    val lowLatencyTier: Boolean = false,
    /**
     * 低延迟层是否同样支持条件写。
     * **S3 Express One Zone 为 false**——它的 PutObject 不支持条件写，
     * 因此不能把 manifest 指针放在低延迟层上（设计文档 §3.3.1）。
     */
    val lowLatencyTierSupportsConditionalWrite: Boolean = false,
    /**
     * 条件写是否被**实测验证**过，而非依据厂商文档假设。
     * GCS 的 S3 互操作层会静默忽略条件头——这一类后端必须被识别出来。
     */
    val conditionalWriteVerified: Boolean = false,
    val requestsAreBilled: Boolean = true,
    val serverSideCopy: Boolean = true,
    val eventNotification: Boolean = false,
    /**
     * 原生批量删除的单次请求最多能带多少个键；**0 表示没有原生批量删除**。
     *
     * S3 / OSS / COS / OBS / R2 / B2 的 `DeleteObjects`（`POST /?delete`）都是 1000，
     * 但这里不写死：真实上限属于后端，用错的代价是 400 而不是慢。
     *
     * 为什么它是能力而不仅是性能优化：清桶路径（GC、`wipe()`、`DROP TABLE`）
     * 快照里可能有上万键，逐个删就是上万次计费请求（原则 P4）。
     * 单位是所有调用方共用的**请求数**，所以这个数字也直接决定了
     * `CountingObjectStore` 的计数口径（见那里的注释）。
     */
    val batchDeleteMaxKeys: Int = 0,
) {
    init {
        require(!conditionalUpdate || conditionalCreate) {
            "conditionalUpdate 蕴含 conditionalCreate"
        }
    }

    companion object {
        /** 全能力的参考配置（AWS S3 标准层）。 */
        val FULL = ObjectStoreCapabilities(
            conditionalCreate = true,
            conditionalUpdate = true,
            strongReadAfterWrite = true,
            strongListConsistency = true,
            lowLatencyTier = true,
            lowLatencyTierSupportsConditionalWrite = false,
            conditionalWriteVerified = true,
            serverSideCopy = true,
            eventNotification = true,
            batchDeleteMaxKeys = 1000,
        )

        /**
         * **阿里云 OSS 的实测能力**（2026-09-25，杭州地域，`CapabilityProbe` + 原始 HTTP 复验）。
         *
         * | 原语 | 结果 |
         * |---|---|
         * | `If-None-Match: *` | ❌ 400（拒绝该头） |
         * | `If-Match: <etag>` | ❌ 400 `NotImplemented` |
         * | `x-oss-forbid-overwrite: true` | ✅ 二次写入 409 `FileAlreadyExists` |
         * | Range 读 / LIST / 读己之写 | ✅ |
         *
         * ⚠️ 还有一条不属于「能力」但同样必须记住的：**OSS 拒绝路径式寻址**
         * （`SecondLevelDomainForbidden`），必须用三级域名——见 `S3ObjectStore.create`
         * 的 `pathStyle`。
         *
         * ⚠️ `conditionalUpdate = false` 的直接后果：**manifest 指针无法做 CAS**。
         * 每次 flush/compaction 都要切指针，所以这个后端上必须显式接受
         * 一个弱化的指针切换（见 `ManifestStore.allowNonAtomicPointerSwitch`），
         * 否则服务端会在能力探测阶段就拒绝启动。
         */
        val OSS = ObjectStoreCapabilities(
            conditionalCreate = true,
            atomicCreateStyle = AtomicCreateStyle.FORBID_OVERWRITE,
            conditionalUpdate = false,
            strongReadAfterWrite = true,
            strongListConsistency = true,
            lowLatencyTier = true,
            lowLatencyTierSupportsConditionalWrite = false,
            serverSideCopy = true,
            // OSS 的 DeleteMultipleObjects 同样一次 1000 键，但**要求 Content-MD5**。
            // 实测确认（2026-10-27，杭州，真实桶）：SDK 自己带上了这个头，
            // conformance 的批量删除用例在真实 OSS 上通过，所以这里可以开。
            batchDeleteMaxKeys = 1000,
        )
    }
}

@JvmInline
value class ETag(val value: String) {
    override fun toString(): String = value
}

/**
 * 原子性前置条件。
 *
 * 当后端不支持而调用方传入了非空值时，实现**必须抛出
 * [CapabilityNotSupportedException]**，绝不能静默忽略——
 * 静默忽略会让一个「看起来正常」的系统在并发下破坏线性化。
 */
sealed interface Precondition {
    /** `If-None-Match: *` —— 仅当对象不存在时写入成功 */
    data object MustNotExist : Precondition

    /** `If-Match: <etag>` —— 仅当 etag 匹配时写入成功 */
    data class MustMatch(val etag: ETag) : Precondition
}

/** 闭区间字节范围，语义同 HTTP `Range: bytes=start-end`。 */
data class ByteRange(val start: Long, val endInclusive: Long) {
    init {
        require(start >= 0) { "range start 必须非负: $start" }
        require(endInclusive >= start) { "range 上界必须 >= 下界: $start-$endInclusive" }
    }

    val length: Long get() = endInclusive - start + 1

    fun toHeader(): String = "bytes=$start-$endInclusive"

    companion object {
        /** 从 offset 起读 length 字节。 */
        fun ofLength(offset: Long, length: Int): ByteRange =
            ByteRange(offset, offset + length - 1)
    }
}

data class ObjectMeta(
    val key: String,
    val size: Long,
    val etag: ETag,
    val lastModifiedMillis: Long,
)

/**
 * 一页列举结果。
 *
 * [sizes] 是**同一份响应里顺手带回来的对象大小**，不是额外请求换来的。
 * 这不是可有可无的装饰：LIST 的响应里本来就带着每个对象的字节数
 * （S3 的 `Contents[].Size`、本地 FS 的一次 stat），而"容量盘点"这类命令
 * 若逐个 `head` 去问，一次评估就能发出成千上万次计费请求——
 * 比一整天的业务请求还多（P4：请求数是一等成本）。
 *
 * 契约：**后端能免费拿到就填**；拿不到就留空（默认空 map），调用方据此降级。
 * 允许只填一部分键（例如列举过程中对象消失了）——调用方必须自己判断
 * `sizes[k]` 是否存在，而不是假设 `sizes.size == keys.size`。
 * 见 `yb-cli cost` / `ls` 的用法。
 */
data class ListPage(
    val keys: List<String>,
    val nextToken: String?,
    val sizes: Map<String, Long> = emptyMap(),
) {
    val hasMore: Boolean get() = nextToken != null
}

/**
 * 对象存储抽象。刻意保持**窄**——只有 5 个操作，避免各后端在行为细节上分叉。
 *
 * 所有上层逻辑（LSM、索引、GC）都构建在这 5 个操作之上。
 * 见设计文档 §3.2（其中记录了「`compose` 服务端拼接为什么不在接口里」这条决策）。
 */
interface ObjectStore {
    val caps: ObjectStoreCapabilities

    /** 读取整个对象，或对象的某个字节区间。 */
    suspend fun get(key: String, range: ByteRange? = null): ByteArray

    /** 写入对象。[precondition] 为 null 时是普通覆盖写。 */
    suspend fun put(key: String, data: ByteArray, precondition: Precondition? = null): ETag

    /** 删除对象（幂等）。对象不存在时返回 false。 */
    suspend fun delete(key: String): Boolean

    /**
     * 批量删除。返回**后端报告的已删除键数**。
     *
     * 默认实现逐个 [delete]，即 N 个键 = N 次请求（外加每次的 HEAD）。
     * 声明了 [ObjectStoreCapabilities.batchDeleteMaxKeys] 的后端必须覆写成一次请求删多键，
     * 把清桶路径的请求数从 `2N` 降到 `ceil(N/maxKeys)`——这是 GC / `DROP TABLE` /
     * 恢复清理在真实对象存储上能不能跑得动的前提（原则 P4）。
     *
     * ⚠️ 计数语义**不等于**逐个 `delete`：原生批量 API（S3/OSS 的 `DeleteObjects`）
     * 对**不存在的键**也报「已删除」，所以返回值不能用来判断「这些键原来存不存在」。
     * 需要那个结论请用 [delete] 或 [exists]——这正是默认实现与原生实现唯一允许的差异，
     * 且被 conformance 套件固定成「至少包含实际存在的键数」。
     */
    suspend fun deleteBatch(keys: Collection<String>): Int {
        var deleted = 0
        for (k in keys) if (delete(k)) deleted++
        return deleted
    }

    /** 按键字典序分页列举。 */
    suspend fun list(prefix: String, pageToken: String? = null, limit: Int = 1000): ListPage

    /** 获取元信息（不传输内容），用于条件校验。 */
    suspend fun head(key: String): ObjectMeta

    /** 对象是否存在。默认实现基于 [head]，后端可覆写为更廉价的方式。 */
    suspend fun exists(key: String): Boolean = try {
        head(key); true
    } catch (_: com.cloudea.yangbase.common.ObjectNotFoundException) {
        false
    }

    /** 列举全部（自动翻页）。**仅限非热路径使用**——LIST 是慢操作。 */
    suspend fun listAll(prefix: String): List<String> {
        val out = ArrayList<String>()
        var token: String? = null
        do {
            val page = list(prefix, token)
            out.addAll(page.keys)
            token = page.nextToken
        } while (token != null)
        return out
    }
}
