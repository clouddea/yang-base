package com.cloudea.yangbase.bench

import com.cloudea.yangbase.objectstore.*
import kotlinx.coroutines.runBlocking
import java.util.UUID

/** 独立命名空间；清理只遍历本次运行的前缀，绝不清空用户桶。 */
internal class IndexBenchStore : AutoCloseable {
    private fun cfg(key: String) = System.getProperty(key) ?: System.getenv(key)
    val oss = cfg("YB_S3_OSS")?.toBooleanStrict() ?: false
    val endpoint = cfg("YB_S3_ENDPOINT")
    val bucket = cfg("YB_S3_BUCKET") ?: if (endpoint == null) "index-bench" else
        error("外部后端必须指定 YB_S3_BUCKET")
    val prefix = "index-bench/${UUID.randomUUID()}/"
    private val embedded = if (endpoint == null) EmbeddedS3Server(bucket).also { it.start() } else null
    init {
        if (endpoint != null && !oss) S3ObjectStore.ensureBucket(
            bucket, endpoint, cfg("YB_S3_REGION") ?: "us-east-1",
            cfg("YB_S3_ACCESS_KEY"), cfg("YB_S3_SECRET_KEY"),
            cfg("YB_S3_PATH_STYLE")?.toBooleanStrict() ?: true,
        )
    }
    private val raw = S3ObjectStore.create(
        bucket, endpoint ?: embedded!!.endpoint, cfg("YB_S3_REGION") ?: "us-east-1",
        cfg("YB_S3_ACCESS_KEY") ?: if (embedded != null) "test" else null,
        cfg("YB_S3_SECRET_KEY") ?: if (embedded != null) "test" else null,
        pathStyle = cfg("YB_S3_PATH_STYLE")?.toBooleanStrict() ?: !oss,
        caps = if (oss) ObjectStoreCapabilities.OSS else ObjectStoreCapabilities.FULL,
    )
    val store: ObjectStore = object : ObjectStore {
        override val caps get() = raw.caps
        override suspend fun get(key: String, range: ByteRange?) = raw.get(prefix + key, range)
        override suspend fun put(key: String, data: ByteArray, precondition: Precondition?) =
            raw.put(prefix + key, data, precondition)
        override suspend fun delete(key: String) = raw.delete(prefix + key)
        override suspend fun head(key: String) = raw.head(prefix + key).copy(key = key)
        override suspend fun list(prefix: String, pageToken: String?, limit: Int): ListPage {
            val page = raw.list(this@IndexBenchStore.prefix + prefix, pageToken, limit)
            fun strip(key: String): String {
                check(key.startsWith(this@IndexBenchStore.prefix))
                return key.removePrefix(this@IndexBenchStore.prefix)
            }
            return ListPage(page.keys.map(::strip), page.nextToken, page.sizes.mapKeys { strip(it.key) })
        }
    }
    override fun close() {
        try {
            runBlocking { store.listAll("").forEach { store.delete(it) } }
            println("Cleaned namespace: $prefix")
        } finally {
            raw.close()
            embedded?.stop()
        }
    }
}
