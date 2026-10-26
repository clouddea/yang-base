package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.ObjectStoreException
import com.cloudea.yangbase.common.PreconditionFailedException
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.core.exception.SdkServiceException
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest
import java.net.URI
import java.util.concurrent.Executors

/**
 * S3 兼容后端（AWS S3 / MinIO / OSS S3 兼容层 / COS / R2 …）。
 *
 * 并发模型遵循设计文档 §15.1：底层 SDK 是**阻塞**的，所以所有调用都跑在一个
 * **固定大小的平台线程池**上（而不是无界放大并发）——存储引擎作为稀缺资源
 * 应当有显式背压。
 */
class S3ObjectStore(
    private val client: S3Client,
    private val bucket: String,
    override val caps: ObjectStoreCapabilities,
    private val ioParallelism: Int = 64,
) : ObjectStore {

    /**
     * 固定大小的 IO 调度器：限制对对象存储的并发请求数，形成天然背压。
     * 这是设计文档 §15.1 的并发模型——**不用虚拟线程**，因为 JNI/阻塞式原生调用
     * 会钉住载体线程；用一个有界的平台线程池反而是更正确的形态。
     */
    private val ioDispatcher: kotlinx.coroutines.CoroutineDispatcher =
        Executors.newFixedThreadPool(ioParallelism) { r ->
            Thread(r, "yb-s3-io").apply { isDaemon = true }
        }.asCoroutineDispatcher()

    override suspend fun get(key: String, range: ByteRange?): ByteArray = withContext(ioDispatcher) {
        val req = GetObjectRequest.builder()
            .bucket(bucket).key(key)
            .apply { range?.let { this.range(it.toHeader()) } }
            .build()
        try {
            val resp: ResponseBytes<*> = client.getObjectAsBytes(req)
            resp.asByteArray()
        } catch (e: NoSuchKeyException) {
            // ⚠️ GET 的 404 被 SDK **一律**建成 `NoSuchKeyException`（它按状态码建模），
            // 桶不存在也是 404——所以这里不能直接当成"对象不存在"，
            // 必须再看一眼错误码（见 [isNoSuchBucket]）
            if (isNoSuchBucket(e)) noSuchBucket(key, e)
            throw ObjectNotFoundException(key)
        } catch (e: S3Exception) {
            if (isNoSuchBucket(e)) noSuchBucket(key, e)
            if (e.statusCode() == 404) throw ObjectNotFoundException(key)
            throw ObjectStoreException(
                "GET $key 失败: ${e.statusCode()} ${e.awsErrorDetails()?.errorMessage()}",
                e, retryable = isTransient(e),
            )
        } catch (e: SdkException) {
            throw ObjectStoreException("GET $key 失败: ${e.message}", e, retryable = isTransient(e))
        } catch (e: java.io.UncheckedIOException) {
            ioFailure("GET", key, e)
        }
    }

    override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag =
        withContext(ioDispatcher) {
            // ★ 按**实际用到的那个能力**检查，不是只看 `conditionalCreate`。
            //
            // `MustMatch`（`If-Match`）用的是 `conditionalUpdate`——它是**独立**的能力位：
            // 有后端支持"仅当不存在时创建"却不支持"仅当 etag 匹配时覆盖"
            // （gcs 的 S3 互操作层就是这一类：它会**静默忽略**条件头）。
            // 只查 conditionalCreate 会让 `If-Match` 在这些后端上照样发出去，
            // 而 CAS 退化成 last-writer-wins —— manifest 指针、租约、行锁全都失去互斥，
            // 且没有任何一处报错
            val needed = when (precondition) {
                null -> false
                Precondition.MustNotExist -> !caps.conditionalCreate
                is Precondition.MustMatch -> !caps.conditionalUpdate
            }
            if (needed) {
                // 绝不静默忽略前置条件
                throw com.cloudea.yangbase.common.CapabilityNotSupportedException(
                    "后端不支持所需的条件写能力（precondition=$precondition）：" +
                        "conditionalCreate=${caps.conditionalCreate} " +
                        "conditionalUpdate=${caps.conditionalUpdate} key=$key",
                )
            }
            val ossStyle = caps.atomicCreateStyle == AtomicCreateStyle.FORBID_OVERWRITE
            val req = PutObjectRequest.builder()
                .bucket(bucket).key(key)
                .apply {
                    when (precondition) {
                        null -> {}
                        // 「仅当不存在时创建」有两种协议形式，由后端声明决定（见 AtomicCreateStyle）
                        Precondition.MustNotExist -> if (ossStyle) {
                            // OSS 不认 `If-None-Match`（实测 400），它自己的原语是这个头。
                            // 必须用 overrideConfiguration：这是**自定义头**，
                            // 不是 SDK 认识的请求字段，加不进去就不会被发送
                            overrideConfiguration(
                                software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration.builder()
                                    .putHeader("x-oss-forbid-overwrite", "true")
                                    .build(),
                            )
                        } else {
                            ifNoneMatch("*")
                        }
                        is Precondition.MustMatch -> ifMatch(precondition.etag.value)
                    }
                }
                .build()
            try {
                val resp = client.putObject(req, RequestBody.fromBytes(data))
                ETag(resp.eTag() ?: "")
            } catch (e: S3Exception) {
                // ★ 错误码的**语义跟着协议走**，不能只看数字。
                //
                // `If-None-Match` 失败是 412；而 OSS 的 `x-oss-forbid-overwrite`
                // 失败是 **409 FileAlreadyExists**。同样是 409，在 S3 语义下
                // 是「并发冲突、结论未知、可以重试」，在 OSS 语义下却是
                // 「对象已经存在」这个**确定结论**——按前者处理会对一个
                // 永远不会成功的请求反复重发（并把它记成"后端在抖"）
                val ossCreateConflict = e.statusCode() == 409 &&
                    precondition is Precondition.MustNotExist &&
                    caps.atomicCreateStyle == AtomicCreateStyle.FORBID_OVERWRITE
                when {
                    ossCreateConflict ->
                        throw PreconditionFailedException("前置条件不成立（对象已存在）: key=$key", e)
                    e.statusCode() == 412 ->
                        throw PreconditionFailedException("前置条件不成立: key=$key", e)
                    // 409 = 并发冲突 → 条件结论未知，可退避重试（§3.3.1）
                    e.statusCode() == 409 ->
                        throw ConcurrentModificationException("条件写并发冲突: key=$key", e)
                    // 404 在 PUT 上**几乎总是**桶不存在（对象不存在不会让 PUT 失败）
                    e.statusCode() == 404 ->
                        if (isNoSuchBucket(e)) noSuchBucket(key, e) else throw ObjectNotFoundException(key)
                    // 5xx / 429 / 408 是瞬时故障，重发可能成功 → 标成可重试
                    else -> throw ObjectStoreException(
                        "PUT $key 失败: ${e.statusCode()} ${e.awsErrorDetails()?.errorMessage()}".trim(),
                        e, retryable = isTransient(e),
                    )
                }
            } catch (e: SdkException) {
                // 连不上 / 超时 / 连接被重置。**这是最常见的一类瞬时故障**，
                // 不标成可重试就等于把网络抖动直接变成用户可见的提交失败。
                throw ObjectStoreException("PUT $key 失败: ${e.message}", e, retryable = isTransient(e))
            } catch (e: java.io.UncheckedIOException) {
                ioFailure("PUT", key, e)
            }
        }

    override suspend fun delete(key: String): Boolean = withContext(ioDispatcher) {
        val existed = try {
            client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build())
            true
        } catch (_: NoSuchKeyException) {
            false
        } catch (e: S3Exception) {
            // ⚠️ 这里返回 false 的语义是"对象不存在，没什么可删的"。
            // 桶不存在时若也返回 false，GC / DROP TABLE / 清理路径会把
            // **一次配置错误**读成"本来就没有东西"，然后一声不响地继续
            if (isNoSuchBucket(e)) noSuchBucket(key, e)
            if (e.statusCode() == 404) false
            else throw ObjectStoreException("HEAD $key 失败: ${e.statusCode()}", e, retryable = isTransient(e))
        } catch (e: SdkException) {
            throw ObjectStoreException("HEAD $key 失败: ${e.message}", e, retryable = isTransient(e))
        } catch (e: java.io.UncheckedIOException) {
            ioFailure("HEAD", key, e)
        }
        if (!existed) return@withContext false
        try {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build())
        } catch (e: S3Exception) {
            // DELETE 是幂等的：重发最多是「又删了一次不存在的对象」，代价为零。
            // 这也是 GC 删除路径能容忍瞬时故障的前提（§6.9）。
            throw ObjectStoreException("DELETE $key 失败: ${e.statusCode()}", e, retryable = isTransient(e))
        } catch (e: SdkException) {
            throw ObjectStoreException("DELETE $key 失败: ${e.message}", e, retryable = isTransient(e))
        } catch (e: java.io.UncheckedIOException) {
            ioFailure("DELETE", key, e)
        }
        true
    }

    override suspend fun list(prefix: String, pageToken: String?, limit: Int): ListPage =
        withContext(ioDispatcher) {
            val req = ListObjectsV2Request.builder()
                .bucket(bucket).prefix(prefix).maxKeys(limit)
                .apply { pageToken?.let { continuationToken(it) } }
                .build()
            try {
                val resp = client.listObjectsV2(req)
                // ⚠️ `Size` 就在这份响应里，**不要**为了拿它再逐对象 `head`：
                // 容量盘点类命令（yb-cli ls/cost）会因此把一次评估变成上万次计费请求
                // 只收**确实给了大小**的那些：把拿不到的记成 0 会让调用方
                // 以为"这个对象是空的"，而不是"这个后端没告诉我"
                ListPage(
                    resp.contents().map { it.key() },
                    if (resp.isTruncated == true) resp.nextContinuationToken() else null,
                    resp.contents().mapNotNull { o -> o.size()?.let { o.key() to it } }.toMap(),
                )
            } catch (e: S3Exception) {
                if (isNoSuchBucket(e)) noSuchBucket(prefix, e)
                throw ObjectStoreException("LIST $prefix 失败: ${e.statusCode()}", e, retryable = isTransient(e))
            } catch (e: SdkException) {
                throw ObjectStoreException("LIST $prefix 失败: ${e.message}", e, retryable = isTransient(e))
            } catch (e: java.io.UncheckedIOException) {
                ioFailure("LIST", prefix, e)
            }
        }

    override suspend fun head(key: String): ObjectMeta = withContext(ioDispatcher) {
        try {
            val r = client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build())
            ObjectMeta(key, r.contentLength(), ETag(r.eTag() ?: ""), r.lastModified().toEpochMilli())
        } catch (_: NoSuchKeyException) {
            throw ObjectNotFoundException(key)
        } catch (e: S3Exception) {
            if (isNoSuchBucket(e)) noSuchBucket(key, e)
            if (e.statusCode() == 404) throw ObjectNotFoundException(key)
            throw ObjectStoreException("HEAD $key 失败: ${e.statusCode()}", e, retryable = isTransient(e))
        } catch (e: SdkException) {
            throw ObjectStoreException("HEAD $key 失败: ${e.message}", e, retryable = isTransient(e))
        } catch (e: java.io.UncheckedIOException) {
            ioFailure("HEAD", key, e)
        }
    }


    /**
     * 404 有**两种**，含义完全不同，绝不能混成一个：
     *
     * | 错误码 | 含义 | 属于 |
     * |---|---|---|
     * | `NoSuchKey` | 这个**对象**不存在 | **正常状态**——恢复路径正是靠它判断"这个 WAL 对象没有"、目录靠它判断"这张表没有" |
     * | `NoSuchBucket` | 这个**桶**不存在 | **配置错误**——桶名写错、桶没建、地域不对 |
     *
     * 混起来的后果是静默的，而且正是本项目最不能接受的那一类：把"桶不存在"当成
     * "对象不存在"返回，目录会读成"这个库一张表都没有"、GC 会读成"没有东西要删"、
     * 崩溃恢复会读成"没有 WAL 要重放"——系统带着一个**空世界**继续跑，
     * 直到某个写路径才报错，而那时错误离原因已经很远了。
     *
     * （真事，2026-09-27：`--s3-bucket` 填了一个不存在的桶，启动时报的是
     * `ObjectNotFoundException: 对象不存在: .yb-probe/…/probe`——它指着探测用的那个
     * 临时对象，而真正的问题在桶名上。SDK 的 `NoSuchBucketException` 是"建模过的"
     * 异常，但某些 S3 兼容后端（含 OSS）不一定会走到那个类型，所以错误码也要看。）
     *
     * ⚠️ 判据依赖**响应体**里的 `<Code>`，而 `HEAD` 没有响应体（S3/OSS 都不返回）——
     * 所以对 [head] 与 [delete] 里的那次 HEAD，它在真实后端上多半不会生效，
     * 那里仍然按"对象不存在"处理。真正兜住"桶写错了"的是有响应体的
     * [get] / [put] / [list]（目录加载、写入、恢复列举都要走它们）。
     */
    private fun isNoSuchBucket(e: S3Exception): Boolean =
        e is NoSuchBucketException || e.awsErrorDetails()?.errorCode() == "NoSuchBucket"

    private fun noSuchBucket(key: String, e: S3Exception): Nothing = throw ObjectStoreException(
        "桶不存在: '$bucket'（操作的是 key=$key）。请检查：--s3-bucket 是否写对 · " +
            "桶是否已在控制台/ossutil 里创建（**本服务不会替你建桶**）· " +
            "--s3-region 是否与桶所在地域一致",
        e,
        // 配置错误重试一万次也是同样的结果，标成可重试只会把它伪装成"后端在抖"
        retryable = false,
    )

    /**
     * 把「响应体读取期间的 IO 失败」翻译成本层的错误类型。
     *
     * 它既不是 [S3Exception] 也不是 [SdkException]（JDK 的 `UncheckedIOException`
     * 直接包着 `IOException` 抛出来），所以上面那些 catch 一个都接不住——
     * 它会以原始类型穿透到调用方，而 [RetryingObjectStore] 只认
     * `ObjectStoreException(retryable = true)`，于是**一次连接抖动变成了永久失败**。
     */
    private fun ioFailure(op: String, key: String, e: java.io.UncheckedIOException): Nothing =
        throw ObjectStoreException("$op $key 失败: ${e.message}", e, retryable = isTransient(e))

    fun close() = client.close()

    companion object {
        /**
         * 「这个失败是瞬时的吗」——即**重发一次可能成功**。
         *
         * 分类必须发生在这一层（而不是让重试层去读 message）：只有这里能同时看到
         * 传输层的异常类型与 HTTP 状态码（设计文档 §3.3.1）。
         *
         *  - `5xx` / `429 TooManyRequests` / `408 RequestTimeout`：服务端抖了或让我们慢一点
         *  - `SdkClientException`（连不上、超时、连接被重置）：**最常见的瞬时故障**，
         *    SDK 不会把它包装成 S3Exception，所以必须在异常类型上单独接住
         *  - `412` / `409` / `404` 都不是瞬时故障，它们各有专门的语义（见 [put]）
         *
         * `409` 虽然也是"重发可能成功"，但它由
         * [com.cloudea.yangbase.common.ConcurrentModificationException] 专门表达——
         * 调用方要靠这个类型做冲突处理，不能混进 [ObjectStoreException]。
         *
         * ⚠️ `501 NotImplemented` 是个**明确的永久结论**，尽管它在 5xx 区间里：
         * 它说的是"这个操作我不支持"，重发一万次也还是这句话。把它算作瞬时故障会让
         * 调用方**永远学不到"这条路走不通"**，只会一次次重发同一份注定失败的请求。
         * 5xx 的直觉在这里失效，是因为状态码的区间划分按的是"谁错了"，而不是"能不能重试"。
         */
        internal fun isTransient(e: Throwable): Boolean {
            // ★ **必须沿 cause 链判定，不能只看最外层。**
            //
            // 真实的传输层失败很少以它原本的面目到达这里。两个已见过的例子：
            //  - 响应体读了一半连接被掐断 → JDK 抛 `UncheckedIOException`（不是
            //    `IOException`，也不是 SDK 自己的异常），`SdkException` 那一支接不住它；
            //  - SDK 把底层 `SocketException` 包进自己的客户端异常再抛。
            // 只看最外层的结果是：**一次纯粹的网络抖动被当成永久故障**，
            // L1 不重发、调用方拿到一个本该自愈的错误。在阿里云 OSS 上，
            // 这表现为能力探测里「Range 读 = ✗」——而 OSS 的 Range 读完全正常，
            // 只是那一次请求的 keep-alive 连接已经作废了。
            var cur: Throwable? = e
            var depth = 0
            while (cur != null && depth < 8) {
                when {
                    // 服务端**明确表过态**的，按状态码判（4xx 一律不重发）
                    cur is S3Exception -> return isTransientStatus(cur.statusCode())
                    cur is SdkServiceException -> return isTransientStatus(cur.statusCode())
                    // SdkClientException 一类：请求根本没到达服务端（DNS、连接池、
                    // 读写超时）。重发是安全的，因为服务端**没有**做出任何决定
                    cur is SdkException -> return true
                    // SocketException / EOFException / SSLException 都在这一支下
                    cur is java.io.IOException -> return true
                }
                cur = cur.cause
                depth++
            }
            return false
        }

        private fun isTransientStatus(code: Int): Boolean =
            (code >= 500 && code != 501) || code == 429 || code == 408

        /**
         * 构建一个指向 S3 兼容端点的 store。
         *
         * @param endpoint 非空时使用路径寻址（MinIO、自建网关需要）；为 null 时用 AWS 原生虚拟主机寻址
         */
        fun create(
            bucket: String,
            endpoint: String? = null,
            region: String = "us-east-1",
            accessKey: String? = null,
            secretKey: String? = null,
            caps: ObjectStoreCapabilities = ObjectStoreCapabilities.FULL,
            ioParallelism: Int = 64,
            /**
             * 寻址风格：`true` = 路径式（`endpoint/bucket/key`），
             * `false` = 虚拟主机式（`bucket.endpoint/key`），`null` = 自动。
             *
             * **自动规则：给了 endpoint 就用路径式**——这不是偏好，是本项目踩过的坑：
             * MinIO 与各种自建网关默认只认路径式。
             * 但**阿里云 OSS 恰好相反**：它要求三级域名，路径式会被直接拒绝
             * （`SecondLevelDomainForbidden`）。所以接 OSS 要显式传 `false`。
             */
            pathStyle: Boolean? = null,
        ): S3ObjectStore =
            S3ObjectStore(
                buildClient(endpoint, region, accessKey, secretKey, pathStyle), bucket, caps, ioParallelism,
            )

        private fun buildClient(
            endpoint: String?,
            region: String,
            accessKey: String?,
            secretKey: String?,
            pathStyle: Boolean? = null,
        ): S3Client {
            val builder = S3Client.builder()
                .region(Region.of(region))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .serviceConfiguration(
                    S3Configuration.builder()
                        // 见 [create] 的 pathStyle 说明：给了 endpoint 默认走路径式（MinIO 需要），
                        // 而 OSS 必须显式关掉它（它要求三级域名）
                        .pathStyleAccessEnabled(pathStyle ?: (endpoint != null))
                        .chunkedEncodingEnabled(false) // MinIO 对 chunked 签名更挑剔
                        .build()
                )
                // ⚠️ **关掉 SDK 自带的重试**（标准模式默认重试 3 次）。
                //
                // 重试的**唯一主人是 L1 适配层的 [RetryingObjectStore]**（设计文档 §2.2），
                // 让 SDK 在下面再叠一层会产生三个后果，而且都很隐蔽：
                //  1. 预算相乘：4 次尝试 × 3 次 SDK 尝试 = 最多 12 次真实请求，
                //     而 `yb_objectstore_retries_total` 只看得见层与层之间那几次，
                //     运维侧「后端在抖」的图景直接是假的
                //  2. 时限失效：SDK 不看我们的总时限，它有自己的超时与退避，
                //     一次调用可以在「已超时」之后继续重发几秒
                //  3. 分类被绕过：409/412 这类**明确结论**不该重发，SDK 不认识这两个
                //     语义（它只按状态码是否可重试来判），会有它自己的判断
                //
                // 代价是单独使用这个后端（不套 [RetryingObjectStore]）时不再有重试。
                // 这是对的：重试策略属于适配层，后端只负责**如实报告**发生了什么。
                .overrideConfiguration(
                    ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.none())
                        .build()
                )
            if (endpoint != null) builder.endpointOverride(URI.create(endpoint))
            // ★ 密钥的**空串**要在这里挡住，不能等到签名失败。
            //
            // `--s3-access-key $AK` 里的 `$AK` 没在当前 shell 里导出时，shell 会
            // **安安静静地传一个空参数**：命令行看起来完全正确，错误却要到能力探测
            // 阶段才以「403 / InvalidAccessKeyId」的形式冒出来——那条错误指向密钥
            // 内容不对，而不是"你的变量是空的"，排查会从错误的方向开始。
            // （`null` 不一样：那表示"走默认凭据链"，在 ECS 上配合实例角色是正常用法。）
            if (accessKey?.isBlank() == true || secretKey?.isBlank() == true) {
                throw IllegalArgumentException(
                    "S3 密钥是空串：--s3-access-key / --s3-secret-key 收到了空值。" +
                        "最常见的原因是它们来自环境变量而该变量没有导出" +
                        "（`--s3-access-key \$AK` 在 \$AK 为空时会传一个**空参数**，命令行看起来完全正常）。" +
                        "先确认一下：echo \"len(\$AK)=\${#AK}\"。若本意是用实例角色/默认凭据链，请两个都不传。",
                )
            }
            if ((accessKey == null) != (secretKey == null)) {
                throw IllegalArgumentException(
                    "S3 密钥只给了一半：accessKey=${if (accessKey != null) "已给" else "未给"} " +
                        "secretKey=${if (secretKey != null) "已给" else "未给"}。" +
                        "两者要么都给，要么都不给（都不给 = 走实例角色/环境变量等默认凭据链）",
                )
            }
            if (accessKey != null && secretKey != null) {
                builder.credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
                )
            }
            return builder.build()
        }

        /**
         * 确保 bucket 存在。
         *
         * **真实对象存储不会自动建桶**——AWS S3 与 MinIO 都要求桶预先存在，
         * 否则首次 PUT 直接报 NoSuchBucket。AWS 上通常由运维预先创建，
         * 但自建 MinIO 与测试环境需要这一步。
         *
         * @return true 表示本次真的创建了（而不是已存在）
         */
        fun ensureBucket(
            bucket: String,
            endpoint: String? = null,
            region: String = "us-east-1",
            accessKey: String? = null,
            secretKey: String? = null,
            pathStyle: Boolean? = null,
        ): Boolean {
            val client = buildClient(endpoint, region, accessKey, secretKey, pathStyle)
            return try {
                client.use {
                    // ★ **先看它是否已经存在**，再决定要不要创建。
                    //
                    // 直接 createBucket 会踩到两个后端差异：
                    //  1. 阿里云 OSS 的 CreateBucket 要求 `location` 用**它自己的**
                    //     地域写法（`oss-cn-hangzhou`），而 AWS SDK 会按 region
                    //     拼一个 S3 风格的值 → 400 "location-constraint is not valid"。
                    //     桶明明已经存在、也明明能读写，却因为"创建"这一步失败而开不了机。
                    //  2. 不同后端在"已存在"上的状态码并不一致。
                    //
                    // 探测存在性是 headBucket：它对已存在的桶返回 200，成本一次请求，
                    // 而"确保桶存在"本来就是启动阶段的一次性动作（不在热路径上，P4 无碍）
                    if (exists(it, bucket)) return false
                    it.createBucket(
                        software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder()
                            .bucket(bucket)
                            // ★ 地域约束**必须按后端自己的写法给**。
                            //
                            // 2026-10-26 在 OSS 上第一次真正建桶时撞到：不传或传 S3 风格的值
                            // 都是 400 `The specified location-constraint is not valid`。
                            // 之前的代码没传，于是“建桶”这条路对 OSS 从来没成功过——
                            // 能跑只是因为桶是事先在控制台建好的。于是任何“换一个桶试试”
                            // （包括 CI 的分片桶、本地的一次性桶）都在启动阶段就死掉，
                            // 而错误信息看起来像地域写错了。
                            .apply {
                                bucketLocationConstraint(endpoint, region)?.let { lc ->
                                    createBucketConfiguration(
                                        software.amazon.awssdk.services.s3.model.CreateBucketConfiguration.builder()
                                            .locationConstraint(lc)
                                            .build()
                                    )
                                }
                            }
                            .build()
                    )
                    true
                }
            } catch (e: S3Exception) {
                // 409 = BucketAlreadyOwnedByYou / BucketAlreadyExists —— 已存在就是成功
                if (e.statusCode() == 409) false
                else throw ObjectStoreException("创建 bucket '$bucket' 失败: ${e.statusCode()} ${e.awsErrorDetails()?.errorMessage()}", e)
            }
        }

        /**
         * 建桶时必须给的地域约束，`null` 表示不给（后端不接受）。
         *
         * 三种情形各不相同，而且都只能靠实测得知：
         * - **阿里云 OSS**：要它自己的写法 `oss-cn-hangzhou`（S3 风格的 `cn-hangzhou` 会被拒）；
         * - **AWS**：`us-east-1` 是特例，**不能**传（传了反而 400），其他地域要传；
         * - **MinIO / 自建网关**：通常不认这个字段，不给最安全。
         */
        private fun bucketLocationConstraint(endpoint: String?, region: String): String? = when {
            endpoint?.contains("aliyuncs.com", ignoreCase = true) == true ->
                if (region.startsWith("oss-")) region else "oss-$region"
            endpoint == null || endpoint.contains("amazonaws.com", ignoreCase = true) ->
                if (region == "us-east-1") null else region
            else -> null
        }

        /** 桶存在且可访问。用 HEAD 探测（不返回内容，最便宜的存在性判断）。 */
        private fun exists(client: S3Client, bucket: String): Boolean = try {
            client.headBucket(
                software.amazon.awssdk.services.s3.model.HeadBucketRequest.builder().bucket(bucket).build()
            )
            true
        } catch (_: software.amazon.awssdk.services.s3.model.NoSuchBucketException) {
            false
        } catch (e: S3Exception) {
            // 403 = 桶存在但我们无权访问 —— 那不该被当成"不存在"然后去创建它
            if (e.statusCode() == 403) true else false
        }
    }
}
