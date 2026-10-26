# 对象存储后端

[English](../en/backends.md) | [简体中文](backends.md)

YangBase 把**全部持久状态**放在对象存储里，并把它当作唯一真相源。于是后端不属于「部署细节」，它属于**正确性面**：写者选举、manifest 指针切换、fencing 全都建立在后端的条件写语义上。

这一页回答三个问题：哪些后端能用、**「能用」到底指什么**、以及怎么**实测**一个新后端而不是相信厂商文档。

---

## 1. 支持的后端

| 后端 | 状态 | 说明 |
|---|---|---|
| 内存 | ✅ 仅测试 | 语义基准；不持久，不能真用 |
| 本地文件系统 | ✅ 仅测试/开发 | 必须显式带 `--i-know-this-is-not-durable`；测试与单机实验用 |
| AWS S3 | ✅ 支持 | 原子创建走 `If-None-Match: *`，CAS 走 `If-Match` |
| MinIO | ✅ **已实测** | 路径式寻址；集成测试直接跑在它上面 |
| 阿里云 OSS | ✅ **真实服务上实测过** | 拒绝 `If-None-Match`，用 `x-oss-forbid-overwrite`（409）；**没有 ETag CAS** → 指针切换需 `--i-accept-nonatomic-pointer-switch` |
| 腾讯云 COS | ⚠️ 预期可用，**未实测** | S3 兼容；上生产前先跑 conformance 套件 |
| 华为云 OBS | ⚠️ 预期可用，**未实测** | S3 兼容；同上 |
| Cloudflare R2 | ⚠️ 预期可用，**未实测** | LIST 最终一致 —— 这项能力必须声明为 `false` |
| Backblaze B2（S3 API） | ⚠️ 预期可用，**未实测** | endpoint/region 取自桶的 S3 endpoint |

「已实测」的含义很具体：有人拿 conformance 套件**加**完整集成测试跑过真实服务，结果留在项目历史里。「预期可用」只表示它讲 S3 协议——那是一个假设，不是承诺。

---

## 2. 怎么配一个后端：`--s3-provider` 预设

过去接一个后端要手工填四样：端点、区域、寻址风格、能力声明。其中两样**填错不报错**：用路径式打阿里云 OSS 会得到 `SecondLevelDomainForbidden`；用标准 S3 条件头打 OSS 会被直接拒绝（400），而不是被忽略。

`--s3-provider` 把这四样收进一个预设：

```bash
# 阿里云 OSS：端点按 region 推导、三级域名寻址、
# 原子创建走 x-oss-forbid-overwrite、没有 ETag CAS
yb-server --s3-provider oss --s3-region cn-hangzhou \
          --s3-bucket my-bucket --s3-access-key "$AK" --s3-secret-key "$SK"

# MinIO / 自建网关：必须给端点，路径式寻址
yb-server --s3-provider minio --s3-endpoint http://127.0.0.1:9000 \
          --s3-bucket yangbase --s3-access-key "$AK" --s3-secret-key "$SK"
```

| provider | 端点 | 默认区域 | 寻址 | 能力声明 | 已实测 |
|---|---|---|---|---|---|
| `aws`（默认） | SDK 内置 | `us-east-1` | 自动 | 标准 S3 全能力 | ✅ |
| `minio` | 必须显式给 | `us-east-1` | 路径式 | 标准 S3 全能力 | ✅ |
| `oss` | `https://oss-{region}.aliyuncs.com` | `cn-hangzhou` | 三级域名 | **无 ETag CAS**、`FORBID_OVERWRITE` | ✅ 真实服务 |
| `cos` | `https://cos.{region}.myqcloud.com` | `ap-guangzhou` | 三级域名 | 标准 S3（假设） | ❌ 未实测 |
| `obs` | `https://obs.{region}.myhuaweicloud.com` | `cn-north-4` | 三级域名 | 标准 S3（假设） | ❌ 未实测 |
| `r2` | 必须显式给（`https://<account>.r2.cloudflarestorage.com`） | `auto` | 路径式 | 标准 S3（假设） | ❌ 未实测 |
| `b2` | `https://s3.{region}.backblazeb2.com` | `us-west-004` | 路径式 | 标准 S3（假设） | ❌ 未实测 |

显式参数永远优先（`--s3-endpoint`、`--s3-region`、`--s3-path-style`）。`--s3-oss` 保留为 `--s3-provider oss` 的兼容别名。

**预设里声明的能力是"上限"，不是承诺。** 启动时的能力探测会实测并按「声明 ∩ 实测」收敛——所以预设写得再乐观也会被纠正，纠正不了就拒绝启动。这也是 `cos`/`obs`/`r2`/`b2` 可以声明"标准 S3 全能力"却仍标成**未实测**的原因：声明的意思是"先按标准 S3 语义试试"，不是"我们验过了"。

---

## 3. 后端契约到底是什么

SPI 刻意只有五个操作：

```kotlin
suspend fun get(key: String, range: ByteRange? = null): ByteArray
suspend fun put(key: String, data: ByteArray, precondition: Precondition? = null): ETag
suspend fun delete(key: String): Boolean      // 幂等；不存在时返回 false
suspend fun list(prefix: String, pageToken: String? = null, limit: Int = 1000): ListPage
suspend fun head(key: String): ObjectMeta
```

`Precondition` 只有两种，而它们的**失败语义不同**：

| 前置条件 | 含义 | 冲突时 |
|---|---|---|
| `MustNotExist` | 仅当不存在时创建 | 确定的失败——**不要重试** |
| `MustMatch(etag)` | 比较并交换 | 确定的失败——**不要重试** |

各后端表达这两件事的协议不同，而这个差异是**服务端强加的**，不是我们选的。所以有了能力声明：

```kotlin
data class ObjectStoreCapabilities(
    val conditionalCreate: Boolean,
    val atomicCreateStyle: AtomicCreateStyle,   // IF_NONE_MATCH 或 FORBID_OVERWRITE
    val conditionalUpdate: Boolean,             // ETag CAS
    val strongReadAfterWrite: Boolean,
    val strongListConsistency: Boolean,
    val lowLatencyTier: Boolean,
    val lowLatencyTierSupportsConditionalWrite: Boolean,
    val conditionalWriteVerified: Boolean,
    val requestsAreBilled: Boolean,
    val serverSideCopy: Boolean,
    val eventNotification: Boolean,
)
```

由此有两条**硬规则**（违反它们算 bug，不算风格问题）：

1. **依赖某项能力的代码路径必须先检查它。** 在没有 `conditionalUpdate` 的后端上做 CAS，要么拒绝启动、要么走显式降级路径——绝不允许"悄悄落空"。
2. **不支持的前置条件必须抛 `CapabilityNotSupportedException`。** 静默忽略 `If-Match` 是最危险的后端行为：系统照常运行，然后并发下悄悄丢更新。（Google Cloud Storage 的 S3 互操作层就是已知的例子。）

---

## 4. 两类失败，两种应对

| 情形 | 怎么发现 | 怎么应对 |
|---|---|---|
| 后端**如实声明**自己缺某项能力 | 能力探测，或声明的预设 | **显式**降级：要么拒绝启动并给出可执行的建议，要么要求一个"我接受这个弱点"的开关（`--i-accept-nonatomic-pointer-switch`） |
| 后端**声称支持却没做到** | conformance 套件 / 能力探测 | **拒绝启动。** 这种情形没有安全降级 |

两者必须在启动阶段就分开：后期的症状一模一样（并发下状态损坏），但**只有一种能活下来**。

---

## 5. 跑 conformance 套件

套件在 `yb-objectstore`，`./gradlew build` 里就会跑：

```bash
./gradlew :yb-objectstore:test --tests '*ObjectStoreConformanceTest*'
```

默认总跑四个本地后端：内存、本地文件、内嵌 S3（标准风格）、内嵌 S3（OSS 风格）。要打真实后端，设环境变量：

```bash
export YANGBASE_CONFORMANCE_ENDPOINT=https://oss-cn-hangzhou.aliyuncs.com
export YANGBASE_CONFORMANCE_BUCKET=my-bucket
export YANGBASE_CONFORMANCE_ACCESS_KEY=...
export YANGBASE_CONFORMANCE_SECRET_KEY=...
export YANGBASE_CONFORMANCE_REGION=cn-hangzhou
export YANGBASE_CONFORMANCE_STYLE=oss          # 不设 = 按标准 S3 语义
export YANGBASE_CONFORMANCE_PATH_STYLE=false   # OSS 必须三级域名
export YANGBASE_CONFORMANCE_NAME=aliyun-oss-hz
./gradlew :yb-objectstore:test --tests '*ObjectStoreConformanceTest*'
```

跑完写出一份 Markdown 矩阵：`yb-objectstore/build/reports/conformance/matrix.md`。

- ✅ 通过
- ⊘ 跳过，因为后端**声明**不支持这项能力（格子里写明原因）
- ❌ 后端声明支持、行为却不一样——这一类正是会静默损坏数据的 bug

CI 会把矩阵贴进 job summary 并作为 artifact 上传。**兼容矩阵是真实运行的结果，不是手维护的表格。**

### 用例覆盖什么

基础语义（读写往返、空对象、1 MiB 对象、不存在的键、HEAD 元信息、覆盖写改变 ETag、删除幂等、Range 读）；列举（前缀过滤、字典序、分页不重不漏、大小可留空）；键的字符集（中文、空格、`+`/`%`、斜杠）；一致性（读己之写、删除后立刻不可见）；以及条件写的不变式：

- 并发 `MustNotExist`：**有且只有一个**写者成功
- 用过期 ETag 的 `MustMatch`：写入**失败**且内容不变
- 未声明的条件写：**被拒绝**，绝不静默放行

---

## 6. 验证一个新后端

1. 用上面的环境变量跑 conformance 套件。❌ 要么修好，要么诚实地把能力声明成 `false`。
2. 用**真实后端**跑一遍集成测试（`./gradlew integrationTest`，把 store 指过去）。这一步才会压到 LSM/事务层，而不只是 SPI。
3. 把你观察到的东西写进本页表格（endpoint 的怪癖、哪些前置条件被拒、LIST 是否强一致）。
4. 这之后才可以标「已实测」。

如果一个后端**连关键能力都满足不了**（比如完全没有原子创建），它不能承载 manifest，YangBase 应当拒绝在它上面启动——而不是假装可以。
