dependencies {
    api(project(":yb-common"))
    implementation("org.slf4j:slf4j-api:2.0.16")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")

    // S3 客户端：AWS SDK v2。用 url-connection-client 而非 Netty，
    // 因为本层的并发模型是「固定平台线程池 + 协程」（见设计文档 §15.1）——
    // 阻塞式 HTTP 客户端配 Dispatchers.IO 比引入 Netty 更可控。
    // 需要 2.30+ ：PutObject 的 If-Match（ETag CAS）是在该版本之后加入的
    implementation("software.amazon.awssdk:s3:2.39.6")
    implementation("software.amazon.awssdk:url-connection-client:2.39.6")
    implementation("software.amazon.awssdk:apache-client:2.39.6")
}
