package com.cloudea.yangbase.objectstore

/**
 * 常见 S3 兼容后端的**接入预设**。
 *
 * ## 这个东西解决什么问题
 *
 * 接一个新后端过去要手工填四样东西：端点、区域、寻址风格、能力声明。
 * 其中两样**很容易填错、且错了不报错**：
 *
 * - **寻址风格**：MinIO 要路径式，阿里云 OSS **拒绝**路径式（`SecondLevelDomainForbidden`）；
 * - **能力**：OSS 没有 ETag CAS、原子创建要走 `x-oss-forbid-overwrite`。
 *   用 S3 的标准语义去打它，条件头会被直接拒绝（400），而不是被忽略。
 *
 * 于是把它们收进预设：`--s3-provider oss` 一句话就够，不必再记四个参数。
 *
 * ## 预设里的能力声明是**上限假设**，不是承诺
 *
 * 声明只影响「启动时用什么语义去打」和「探测失败时怎么解释」。
 * 真正生效的能力一律来自 [CapabilityProbe] 的**实测**：
 * `probeAndMerge` 取「声明 ∩ 实测」，所以预设写得再乐观，
 * 真实后端做不到的项也会在启动阶段被降下来（或直接拒绝启动）。
 *
 * 由此得到一条使用建议：**没实测过的后端，先跑 conformance 套件再上生产**
 * （见 `docs/en/backends.md`）。这里的 `verified` 字段就是用来标这件事的。
 */
enum class S3Provider(
    /** CLI 上用的 id。 */
    val id: String,
    /**
     * 端点模板，`{region}` 会被替换；null 表示「没有可推导的端点，必须显式给
     * `--s3-endpoint`」（AWS 用 SDK 内置端点，MinIO/R2 的端点各自带账号或主机名）。
     */
    val endpointTemplate: String?,
    /** 未显式指定 region 时的默认值。 */
    val defaultRegion: String,
    /** 未显式指定寻址风格时的默认值；null = 交给 `S3ObjectStore.create` 的自动规则。 */
    val defaultPathStyle: Boolean?,
    /** 能力声明的**上限**（启动时会被实测收敛）。 */
    val caps: ObjectStoreCapabilities,
    /**
     * 是否由本项目在真实服务上实测过。
     * `false` 的含义很具体：**它只是"讲 S3 协议"，不是"验证过能用"**。
     */
    val verified: Boolean,
    /** 一句话说明，出现在报错与文档里。 */
    val note: String,
) {
    /** AWS S3：交给 SDK 内置端点解析，region 必须给对。 */
    AWS(
        id = "aws",
        endpointTemplate = null,
        defaultRegion = "us-east-1",
        defaultPathStyle = null,
        caps = ObjectStoreCapabilities.FULL,
        verified = true,
        note = "AWS S3 原生端点：不需要 --s3-endpoint",
    ),

    /** MinIO / 自建网关：端点必须显式给，寻址用路径式。 */
    MINIO(
        id = "minio",
        endpointTemplate = null,
        defaultRegion = "us-east-1",
        defaultPathStyle = true,
        caps = ObjectStoreCapabilities.FULL,
        verified = true,
        note = "MinIO 或自建 S3 网关：必须给 --s3-endpoint，默认路径式寻址",
    ),

    /**
     * 阿里云 OSS：**唯一一个能力与 S3 不同的预设**。
     *
     * 实测（2026-09-25，杭州）：`If-None-Match` 被拒绝（400）、没有 `If-Match`，
     * 原子创建要用 `x-oss-forbid-overwrite`（冲突时 409）。寻址**必须**三级域名。
     */
    OSS(
        id = "oss",
        endpointTemplate = "https://oss-{region}.aliyuncs.com",
        defaultRegion = "cn-hangzhou",
        defaultPathStyle = false,
        caps = ObjectStoreCapabilities.OSS,
        verified = true,
        note = "阿里云 OSS：用 x-oss-forbid-overwrite 做原子创建，没有 ETag CAS，必须虚拟主机式寻址",
    ),

    /** 腾讯云 COS：S3 兼容，未实测。 */
    COS(
        id = "cos",
        endpointTemplate = "https://cos.{region}.myqcloud.com",
        defaultRegion = "ap-guangzhou",
        defaultPathStyle = false,
        caps = ObjectStoreCapabilities.FULL,
        verified = false,
        note = "腾讯云 COS：S3 兼容，未实测；先跑 conformance 套件",
    ),

    /** 华为云 OBS：S3 兼容，未实测。 */
    OBS(
        id = "obs",
        endpointTemplate = "https://obs.{region}.myhuaweicloud.com",
        defaultRegion = "cn-north-4",
        defaultPathStyle = false,
        caps = ObjectStoreCapabilities.FULL,
        verified = false,
        note = "华为云 OBS：S3 兼容，未实测；先跑 conformance 套件",
    ),

    /** Cloudflare R2：端点带账号 id，必须显式给。 */
    R2(
        id = "r2",
        endpointTemplate = null,
        defaultRegion = "auto",
        defaultPathStyle = true,
        caps = ObjectStoreCapabilities.FULL,
        verified = false,
        note = "Cloudflare R2：endpoint 形如 https://<account>.r2.cloudflarestorage.com，region 用 auto",
    ),

    /** Backblaze B2 的 S3 API：端点按区域拼。 */
    B2(
        id = "b2",
        endpointTemplate = "https://s3.{region}.backblazeb2.com",
        defaultRegion = "us-west-004",
        defaultPathStyle = true,
        caps = ObjectStoreCapabilities.FULL,
        verified = false,
        note = "Backblaze B2（S3 API）：region 形如 us-west-004",
    ),
    ;

    /** 用 [region] 展开端点模板；没有模板时返回 null（必须显式给 endpoint）。 */
    fun endpointFor(region: String): String? = endpointTemplate?.replace("{region}", region)

    companion object {
        /** 全部合法 id，用在报错信息里——报错必须告诉用户**可以填什么**。 */
        val ids: String get() = entries.joinToString(", ") { it.id }

        /** 按 id 找预设（大小写不敏感）；未知 id 返回 null，由调用方给出带 [ids] 的错误。 */
        fun of(id: String?): S3Provider? {
            if (id == null) return AWS
            return entries.firstOrNull { it.id.equals(id.trim(), ignoreCase = true) }
        }
    }
}

/**
 * 一次「后端怎么连」的最终结论：[S3Provider] + 用户显式指定的覆盖项。
 *
 * 单独抽出来，是为了让**服务端**与 **CLI** 用同一套解析——
 * 两处各写一遍必然漂移，而漂移的症状是"CLI 能连、服务端连不上"这种最难查的问题。
 */
data class S3Target(
    val provider: S3Provider,
    val endpoint: String?,
    val region: String,
    val pathStyle: Boolean?,
    val caps: ObjectStoreCapabilities,
) {
    companion object {
        /**
         * 解析顺序：**显式参数 > 预设默认值**。
         *
         * @param providerId `--s3-provider` 的值；null 时按 [legacyOssFlag] 决定
         *   （兼容 2026-10 之前的 `--s3-oss`：它等价于 `--s3-provider=oss`）。
         * @throws IllegalArgumentException 未知的 provider id（消息里列出全部合法值）
         */
        fun resolve(
            providerId: String?,
            endpoint: String?,
            region: String?,
            pathStyle: Boolean?,
            legacyOssFlag: Boolean = false,
        ): S3Target {
            val effectiveId = providerId ?: if (legacyOssFlag) S3Provider.OSS.id else null
            val provider = S3Provider.of(effectiveId)
                ?: throw IllegalArgumentException(
                    "未知的 --s3-provider『$providerId』。可选值：${S3Provider.ids}。" +
                        "其中 oss 是阿里云 OSS（它的原子创建原语与 S3 不同），" +
                        "minio 表示自建网关（路径式寻址）。其他 S3 兼容后端用对应的 id，" +
                        "或用 --s3-provider aws 并按标准 S3 语义连接。",
                )
            // region 为 null 时才用预设默认值；显式给了空串同样视为"没给"
            val effectiveRegion = region?.takeIf { it.isNotBlank() } ?: provider.defaultRegion
            return S3Target(
                provider = provider,
                endpoint = endpoint ?: provider.endpointFor(effectiveRegion),
                region = effectiveRegion,
                pathStyle = pathStyle ?: provider.defaultPathStyle,
                caps = provider.caps,
            )
        }
    }
}
