package com.cloudea.yangbase.server

import com.cloudea.yangbase.objectstore.*
import kotlinx.coroutines.runBlocking

/** 默认内嵌 S3；显式指定外部配置时复用同一套语义断言。外部桶必须是专用测试桶。 */
internal class IntegrationStore(defaultBucket: String) : AutoCloseable {
    private fun cfg(key: String) = System.getProperty(key) ?: System.getenv(key)
    val oss = cfg("YB_S3_OSS")?.toBooleanStrictOrNull() ?: false
    private val embedded: EmbeddedS3Server?
    val store: S3ObjectStore
    init {
        val external = cfg("YB_S3_ENDPOINT")
        if (external == null) {
            embedded = EmbeddedS3Server(defaultBucket).also { it.start() }
            store = S3ObjectStore.create(defaultBucket, embedded.endpoint, "us-east-1", "test", "test")
        } else {
            embedded = null
            val bucket = requireNotNull(cfg("YB_S3_BUCKET")) { "外部回归必须显式指定专用 YB_S3_BUCKET" }
            val region = cfg("YB_S3_REGION") ?: "us-east-1"
            val pathStyle = cfg("YB_S3_PATH_STYLE")?.toBooleanStrictOrNull()
            S3ObjectStore.ensureBucket(bucket, external, region, cfg("YB_S3_ACCESS_KEY"), cfg("YB_S3_SECRET_KEY"), pathStyle)
            store = S3ObjectStore.create(bucket, external, region, cfg("YB_S3_ACCESS_KEY"), cfg("YB_S3_SECRET_KEY"),
                pathStyle = pathStyle, caps = if (oss) ObjectStoreCapabilities.OSS else ObjectStoreCapabilities.FULL)
            runBlocking { for (key in store.listAll("")) store.delete(key) }
        }
        println("[IntegrationStore] backend=${external ?: "embedded S3"}, oss=$oss")
    }
    override fun close() { store.close(); embedded?.stop() }
}
