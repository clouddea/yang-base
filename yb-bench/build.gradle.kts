dependencies {
    implementation(project(":yb-server"))
    implementation(project(":yb-common"))
    implementation("org.slf4j:slf4j-api:2.0.16")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
    // 协调模式基准的端到端那一段走 JDBC（与集成测试同一条路径：真实的
    // MySQL 线协议），所以驱动必须在运行时类路径上——少了它，
    // `DriverManager` 只会报 "No suitable driver found"，与基准本身无关
    runtimeOnly("com.mysql:mysql-connector-j:9.1.0")
}

// 基准以 main 函数形式运行（避免引入 JMH 的注解处理器开销），
// 通过 ./gradlew :yb-bench:bench 触发
tasks.register<JavaExec>("bench") {
    description = "运行 YangBase 基准测试（默认内嵌 S3；-PYB_S3_ENDPOINT=... 打真实 MinIO/S3）"
    group = "verification"
    mainClass.set("com.cloudea.yangbase.bench.BenchMainKt")
    classpath = sourceSets["main"].runtimeClasspath
    jvmArgs("-Xmx2g")
    // 与集成测试同样用 -P 属性透传（Gradle 守护进程不继承 shell 的新环境变量）
    listOf("YB_S3_ENDPOINT", "YB_S3_BUCKET", "YB_S3_ACCESS_KEY", "YB_S3_SECRET_KEY").forEach { k ->
        (project.findProperty(k) as String?)?.let { systemProperty(k, it) }
    }
}

// 协调模式对比基准（single / objectstore / ms）
tasks.register<JavaExec>("benchCoordination") {
    description = "对比三种协调模式：协调原语延迟 + 端到端 SQL + 对象存储请求数"
    group = "verification"
    mainClass.set("com.cloudea.yangbase.bench.CoordinationBenchKt")
    classpath = sourceSets["main"].runtimeClasspath
    jvmArgs("-Xmx2g")
}

// 基准测试模块不含生产代码，因此不纳入覆盖率门禁。
// 它由 ./gradlew :yb-bench:bench 直接运行验证。
tasks.matching { it.name == "koverVerify" }.configureEach { enabled = false }
