package com.cloudea.yangbase.objectstore

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

/**
 * 内嵌的 S3 兼容服务器，用于**集成测试**。
 *
 * 为什么需要它而不只用 [MemoryObjectStore]：
 *  - 它走**真实的 HTTP**，因此能覆盖签名、路径编码、Range 请求、头部编解码
 *    这些只有在网络边界上才会暴露的问题
 *  - 它实现了**真实的 S3 条件写语义**（`If-None-Match: *` 与 `If-Match`），
 *    而本系统的正确性完全建立在条件写之上（设计文档 §3.3.1）
 *
 * 它**不是** S3 的完整实现：没有鉴权校验、没有版本控制、没有生命周期规则。
 * 但**有 multipart 与服务端拼接**（`UploadPartCopy`）——因为拼接路径不测就等于没实现，
 * 而它恰恰是跨区时不产生下行流量的唯一手段（设计文档 §6.6）。
 * 生产验证仍需真实对象存储（MinIO / S3 / OSS）。
 */
class EmbeddedS3Server(bucket: String = "yangbase-test") {

    private class Stored(val data: ByteArray, val etag: String, val modified: Long)

    private val objects = ConcurrentHashMap<String, Stored>()
    private val version = AtomicLong(1)
    private val defaultBucket = bucket

    @Volatile
    private var server: HttpServer? = null

    var port: Int = -1
        private set

    /** 记录收到的请求，供测试断言「服务端真的收到了 N 次 PUT」。 */
    private val counters = ConcurrentHashMap<String, AtomicLong>()

    val endpoint: String get() = "http://127.0.0.1:$port"

    fun start() {
        val s = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 128)
        s.executor = Executors.newFixedThreadPool(16) { r ->
            Thread(r, "yb-fake-s3").apply { isDaemon = true }
        }
        s.createContext("/") { ex -> handle(ex) }
        s.start()
        server = s
        port = s.address.port
    }

    // ── 故障注入（重试层的测试用）──────────────────────────────────
    //
    // 为什么必须有它：重试/退避的**分类**只有走真实 HTTP 才测得准——自己 new 一个
    // S3Exception 只能测到「我们写的 when 分支」，测不到 SDK 在真实状态码上的行为，
    // 也测不到「重发一次到底会不会成功」。注入的是状态码，不是我们自己的异常。

    private val pendingFailures = AtomicLong(0)

    /** 让接下来的 [count] 个请求以 [status] 失败（默认 503，即设计文档 §3.3.1 里可重试的那类）。 */
    fun injectFailures(count: Int, status: Int = 503) {
        failStatus = status
        pendingFailures.set(count.toLong())
    }

    /** 还剩多少次注入的失败没被消费。用来断言「重试确实发生了 N 次」。 */
    fun pendingInjections(): Int = pendingFailures.get().toInt()

    @Volatile
    private var failStatus: Int = 503

    fun stop() {
        server?.stop(0)
        server = null
    }

    fun requestCount(op: String): Long = counters[op]?.get() ?: 0L
    fun resetCounters() = counters.clear()
    fun objectCount(): Int = objects.size

    private fun bump(op: String) {
        counters.computeIfAbsent(op) { AtomicLong() }.incrementAndGet()
    }

    private fun handle(ex: HttpExchange) {
        try {
            val left = pendingFailures.getAndUpdate { if (it > 0) it - 1 else 0 }
            if (left > 0) {
                val status = failStatus
                bump("Injected$status")
                // ⚠️ **必须先把请求体读干净**，再回响应。
                //
                // 注入的失败是在「解析请求」之前就返回的，于是请求体（PUT 的负载）
                // 原封不动留在连接里。HTTP/1.1 是长连接：服务端以为下一个请求从
                // 响应字节之后开始，客户端发的却是新请求行——两边对不齐，
                // 服务端就在等它永远等不到的那些字节，**客户端表现为 Read timed out**。
                //
                // 症状是间歇性的（取决于连接是否被复用），而它会让重试层的用例
                // 随机变红：一次超时本身就吃掉了整个总时限预算，
                // 于是"重试 3 次后成功"变成"第 1 次就超时"。这一条是实测踩出来的。
                ex.requestBody.transferTo(java.io.OutputStream.nullOutputStream())
                val code = if (status == 409) "ConditionalRequestConflict" else "ServiceUnavailable"
                respond(ex, status, s3Error(code, "injected failure (测试注入)"), "application/xml")
                return
            }
            val path = URLDecoder.decode(ex.requestURI.path, Charsets.UTF_8)
            // 路径式寻址：/{bucket}/{key}
            val trimmed = path.removePrefix("/")
            val slash = trimmed.indexOf('/')
            val bucket = if (slash < 0) trimmed else trimmed.substring(0, slash)
            val key = if (slash < 0) "" else trimmed.substring(slash + 1)
            if (bucket != defaultBucket) {
                respond(ex, 404, "NoSuchBucket".toByteArray(), "application/xml")
                return
            }
            val query = parseQuery(ex.requestURI.query)

            when (ex.requestMethod.uppercase()) {
                // multipart 的创建与完成都是 POST 到同一个 key 上，
                // 靠 query 参数区分：`?uploads` 创建，`?uploadId=...` 完成
                "POST" -> when {
                    query.containsKey("uploads") -> createMultipart(ex, key)
                    query["uploadId"] != null -> completeMultipart(ex, key, query["uploadId"]!!)
                    else -> respond(ex, 400, "BadRequest".toByteArray(), "text/plain")
                }
                "PUT" -> if (query["uploadId"] != null && query["partNumber"] != null) {
                    uploadPart(ex, key, query["uploadId"]!!, query["partNumber"]!!.toInt())
                } else {
                    put(ex, key)
                }
                "GET" -> if (query["list-type"] == "2") list(ex, query) else get(ex, key)
                "HEAD" -> head(ex, key)
                "DELETE" -> if (query["uploadId"] != null) {
                    uploads.remove(query["uploadId"])
                    respond(ex, 204, ByteArray(0), "text/plain")
                } else {
                    delete(ex, key)
                }
                else -> respond(ex, 405, "MethodNotAllowed".toByteArray(), "text/plain")
            }
        } catch (t: Throwable) {
            runCatching {
                respond(ex, 500, "InternalError: ${t.message}".toByteArray(), "text/plain")
            }
        } finally {
            runCatching { ex.close() }
        }
    }

    private fun put(ex: HttpExchange, key: String) {
        bump("PUT")
        val body = ex.requestBody.readBytes()
        val ifNoneMatch = ex.requestHeaders.getFirst("If-None-Match")
        val ifMatch = ex.requestHeaders.getFirst("If-Match")
        val forbidOverwrite = ex.requestHeaders.getFirst("x-oss-forbid-overwrite")
        val existing = objects[key]

        // ── 阿里云 OSS 的原子创建原语（`--s3-oss` 走这条路）──────────
        //
        // 语义一样、协议与错误码都不同：OSS **不认** `If-None-Match`（实测 400），
        // 它自己的头是 `x-oss-forbid-overwrite`，冲突时返回 **409 FileAlreadyExists**。
        // 内嵌服务器必须如实复现这个差异——否则「OSS 适配」只有在真实云上才测得到，
        // 而按项目规矩，集成测试不许打真实云
        if (forbidOverwrite == "true") {
            if (ossStyle && existing != null) {
                respond(
                    ex, 409,
                    s3Error("FileAlreadyExists", "The object you specified already exists and can not be overwritten."),
                    "application/xml",
                )
                return
            }
            // 对象不存在（或本服务器没被要求按 OSS 语义行事）：正常写入
            val etag0 = "\"${java.util.UUID.randomUUID()}\""
            objects[key] = Stored(body, etag0, System.currentTimeMillis())
            ex.responseHeaders.add("ETag", etag0)
            respond(ex, 200, ByteArray(0), "application/octet-stream")
            return
        }
        if (ossStyle && ifNoneMatch == "*") {
            // OSS 上 `If-None-Match` 不是"忽略"，而是**拒绝**（实测 400）
            respond(
                ex, 400,
                s3Error("InvalidArgument", "A header you provided implies functionality that is not implemented."),
                "application/xml",
            )
            return
        }

        // 条件写语义必须与 S3 一致，否则测不出 fencing 的真实行为
        if (ifNoneMatch == "*" && existing != null) {
            respond(ex, 412, s3Error("PreconditionFailed", "At least one of the pre-conditions you specified did not hold"), "application/xml")
            return
        }
        if (ifMatch != null && ifMatch != "*") {
            if (existing == null) {
                respond(ex, 404, s3Error("NoSuchKey", "The specified key does not exist."), "application/xml")
                return
            }
            if (existing.etag != ifMatch) {
                respond(ex, 412, s3Error("PreconditionFailed", "etag mismatch"), "application/xml")
                return
            }
        }
        if (ifMatch == "*" && existing == null) {
            respond(ex, 412, s3Error("PreconditionFailed", "object does not exist"), "application/xml")
            return
        }

        val etag = "\"${java.util.UUID.randomUUID()}\""
        objects[key] = Stored(body, etag, System.currentTimeMillis())
        ex.responseHeaders.add("ETag", etag)
        respond(ex, 200, ByteArray(0), "application/octet-stream")
    }

    // ── multipart ─────────────────────────────────────────────────
    // 存在的理由：服务端拼接（UploadPartCopy）是跨区时不产生下行流量的唯一手段，
    // 没有它，那条路径在测试里永远跑不到——等于没实现。

    /** 进行中的 multipart 上传：uploadId → (bucket 内的 key, 分片表)。 */
    private val uploads = java.util.concurrent.ConcurrentHashMap<String, Upload>()

    private class Upload(val key: String) {
        val parts = java.util.concurrent.ConcurrentHashMap<Int, ByteArray>()
    }

    /** 设为 true 后拒绝一切 multipart 请求——用来测「后端能力声明错误」时的降级。 */
    @Volatile
    var refuseMultipart: Boolean = false

    /**
     * 按**阿里云 OSS** 的语义行事（`--s3-oss` 那一条路径）：
     * 拒绝 `If-None-Match`、认 `x-oss-forbid-overwrite` 与它的 409。
     *
     * 这样「OSS 适配」就能在**内嵌服务器**上被完整测到，而不是只能靠打真实云
     * （项目规矩：集成测试不许打真实云）。
     */
    @Volatile
    var ossStyle: Boolean = false

    private fun createMultipart(ex: HttpExchange, key: String) {
        if (refuseMultipart) {
            respond(ex, 501, s3Error("NotImplemented", "multipart not supported"), "application/xml")
            return
        }
        bump("CreateMultipart")
        val id = java.util.UUID.randomUUID().toString()
        uploads[id] = Upload(key)
        val xml = """<?xml version="1.0" encoding="UTF-8"?>
            <InitiateMultipartUploadResult><Bucket>$defaultBucket</Bucket><Key>$key</Key>
            <UploadId>$id</UploadId></InitiateMultipartUploadResult>""".trimIndent()
        respond(ex, 200, xml.toByteArray(), "application/xml")
    }

    /**
     * `PUT /{bucket}/{key}?partNumber=N&uploadId=X`
     *
     * 带 `x-amz-copy-source` 时是 **UploadPartCopy**——服务端直接把源区间拷过来，
     * 数据不经过客户端。这正是我们要测的那条路径。
     */
    private fun uploadPart(ex: HttpExchange, key: String, uploadId: String, partNumber: Int) {
        val up = uploads[uploadId]
        if (up == null) {
            respond(ex, 404, s3Error("NoSuchUpload", "The specified upload does not exist."), "application/xml")
            return
        }
        val copySource = ex.requestHeaders.getFirst("x-amz-copy-source")
        if (copySource == null) {
            bump("UploadPart")
            up.parts[partNumber] = ex.requestBody.readBytes()
            ex.responseHeaders.add("ETag", "\"part-$partNumber\"")
            respond(ex, 200, ByteArray(0), "application/octet-stream")
            return
        }

        bump("UploadPartCopy")
        // copy-source 形如 "/bucket/key" 或 "bucket/key"，可能被 URL 编码
        val src = URLDecoder.decode(copySource, Charsets.UTF_8).removePrefix("/")
        val srcKey = src.substringAfter('/', "")
        val source = objects[srcKey]
        if (source == null) {
            respond(ex, 404, s3Error("NoSuchKey", "The specified key does not exist."), "application/xml")
            return
        }
        val rangeHeader = ex.requestHeaders.getFirst("x-amz-copy-source-range")
        val slice = if (rangeHeader != null) {
            val m = Regex("bytes=(\\d+)-(\\d+)").find(rangeHeader)
                ?: run {
                    respond(ex, 400, "BadRequest: bad range".toByteArray(), "text/plain")
                    return
                }
            val a = m.groupValues[1].toInt()
            val b = m.groupValues[2].toInt()
            if (a > b || b >= source.data.size) {
                // 越界的源区间必须报错，而不是悄悄截断——
                // 截断会产出一个「看起来成功」的坏对象
                respond(ex, 416, s3Error("InvalidRange", "The requested range is not satisfiable"), "application/xml")
                return
            }
            source.data.copyOfRange(a, b + 1)
        } else {
            source.data
        }
        up.parts[partNumber] = slice
        val etag = "\"copy-$partNumber\""
        ex.responseHeaders.add("ETag", etag)
        val xml = """<?xml version="1.0" encoding="UTF-8"?>
            <CopyPartResult><ETag>$etag</ETag></CopyPartResult>""".trimIndent()
        respond(ex, 200, xml.toByteArray(), "application/xml")
    }

    private fun completeMultipart(ex: HttpExchange, key: String, uploadId: String) {
        val up = uploads.remove(uploadId)
        if (up == null) {
            respond(ex, 404, s3Error("NoSuchUpload", "The specified upload does not exist."), "application/xml")
            return
        }
        // 按分片号顺序拼接——**不能按到达顺序**，分片是可能乱序到达的
        val out = java.io.ByteArrayOutputStream()
        for (n in up.parts.keys.sorted()) out.write(up.parts[n]!!)
        bump("CompleteMultipart")
        val etag = "\"${java.util.UUID.randomUUID()}\""
        objects[key] = Stored(out.toByteArray(), etag, System.currentTimeMillis())
        ex.responseHeaders.add("ETag", etag)
        val xml = """<?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUploadResult><Key>$key</Key><ETag>$etag</ETag>
            </CompleteMultipartUploadResult>""".trimIndent()
        respond(ex, 200, xml.toByteArray(), "application/xml")
    }

    private fun get(ex: HttpExchange, key: String) {
        bump("GET")
        val o = objects[key] ?: run {
            respond(ex, 404, s3Error("NoSuchKey", "The specified key does not exist."), "application/xml")
            return
        }
        ex.responseHeaders.add("ETag", o.etag)
        ex.responseHeaders.add("Last-Modified", httpDate(o.modified))

        val rangeHeader = ex.requestHeaders.getFirst("Range")
        if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {
            val spec = rangeHeader.removePrefix("bytes=")
            val parts = spec.split("-")
            val start = parts[0].toLongOrNull() ?: 0L
            val end = if (parts.size > 1 && parts[1].isNotEmpty()) parts[1].toLong() else o.data.size - 1L
            val from = start.toInt().coerceIn(0, o.data.size)
            val to = (end + 1).toInt().coerceIn(from, o.data.size)
            val slice = o.data.copyOfRange(from, to)
            ex.responseHeaders.add("Content-Range", "bytes $from-${to - 1}/${o.data.size}")
            respond(ex, 206, slice, "application/octet-stream")
        } else {
            respond(ex, 200, o.data, "application/octet-stream")
        }
    }

    private fun head(ex: HttpExchange, key: String) {
        bump("HEAD")
        val o = objects[key]
        if (o == null) {
            ex.sendResponseHeaders(404, -1)
            return
        }
        ex.responseHeaders.add("ETag", o.etag)
        ex.responseHeaders.add("Last-Modified", httpDate(o.modified))
        ex.responseHeaders.add("Content-Length", o.data.size.toString())
        ex.sendResponseHeaders(200, -1)
    }

    private fun delete(ex: HttpExchange, key: String) {
        bump("DELETE")
        objects.remove(key)
        ex.sendResponseHeaders(204, -1)
    }

    /** ListObjectsV2。返回与 S3 一致的 XML 形状，客户端才能正确解析分页。 */
    private fun list(ex: HttpExchange, q: Map<String, String>) {
        bump("LIST")
        val prefix = q["prefix"] ?: ""
        val maxKeys = (q["max-keys"]?.toIntOrNull() ?: 1000).coerceIn(1, 1000)
        val token = q["continuation-token"]
        val all = objects.keys.filter { it.startsWith(prefix) }.sorted()
        val after = if (token == null) all else all.filter { it > token }
        val page = after.take(maxKeys)
        val truncated = after.size > maxKeys

        val xml = buildString {
            append("""<?xml version="1.0" encoding="UTF-8"?>""")
            append("""<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">""")
            append("<Name>$defaultBucket</Name>")
            append("<Prefix>${escapeXml(prefix)}</Prefix>")
            append("<KeyCount>${page.size}</KeyCount>")
            append("<MaxKeys>$maxKeys</MaxKeys>")
            append("<IsTruncated>$truncated</IsTruncated>")
            if (truncated && page.isNotEmpty()) {
                append("<NextContinuationToken>${escapeXml(page.last())}</NextContinuationToken>")
            }
            for (k in page) {
                val o = objects[k]!!
                append("<Contents>")
                append("<Key>${escapeXml(k)}</Key>")
                // ⚠️ XML 里的 LastModified 必须是 ISO-8601，而不是 HTTP 头的 RFC-1123 格式——
                // 用错的话 SDK 会在反序列化时报 "could not be parsed at index 0"。
                append("<LastModified>${isoDate(o.modified)}</LastModified>")
                append("<ETag>${escapeXml(o.etag)}</ETag>")
                append("<Size>${o.data.size}</Size>")
                append("<StorageClass>STANDARD</StorageClass>")
                append("</Contents>")
            }
            append("</ListBucketResult>")
        }
        respond(ex, 200, xml.toByteArray(Charsets.UTF_8), "application/xml")
    }

    private fun respond(ex: HttpExchange, code: Int, body: ByteArray, contentType: String) {
        ex.responseHeaders.add("Content-Type", contentType)
        ex.responseHeaders.add("x-amz-request-id", java.util.UUID.randomUUID().toString())
        if (body.isEmpty()) {
            ex.sendResponseHeaders(code, -1)
        } else {
            ex.sendResponseHeaders(code, body.size.toLong())
            ex.responseBody.use { it.write(body) }
        }
    }

    private fun parseQuery(q: String?): Map<String, String> {
        if (q.isNullOrEmpty()) return emptyMap()
        return q.split("&").mapNotNull {
            val i = it.indexOf('=')
            if (i < 0) it to "" else it.substring(0, i) to URLDecoder.decode(it.substring(i + 1), Charsets.UTF_8)
        }.toMap()
    }

    private fun s3Error(code: String, message: String) =
        """<?xml version="1.0" encoding="UTF-8"?><Error><Code>$code</Code><Message>$message</Message></Error>"""
            .toByteArray(Charsets.UTF_8)

    private fun escapeXml(s: String) = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    /** HTTP 头的日期格式（RFC 1123）。 */
    private fun httpDate(millis: Long): String =
        java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(
            java.time.Instant.ofEpochMilli(millis).atZone(java.time.ZoneOffset.UTC)
        )

    /** XML 载荷里的日期格式（ISO-8601）。两者不能混用。 */
    private fun isoDate(millis: Long): String =
        java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.ofEpochMilli(millis))
}
