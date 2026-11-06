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

// 独立 opt-in 性能门禁：任何索引没有节省 IO 或返回错误结果都会以非零码退出。
tasks.register<JavaExec>("benchIndexes") {
    description = "验收全部索引的 SQL 读路径、请求数、字节数（默认内嵌 S3）"
    group = "verification"
    mainClass.set("com.cloudea.yangbase.bench.IndexBenchKt")
    classpath = sourceSets["main"].runtimeClasspath
    jvmArgs("-Xmx2g")
    systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")
    listOf("YB_S3_ENDPOINT", "YB_S3_BUCKET", "YB_S3_REGION", "YB_S3_PATH_STYLE", "YB_S3_OSS",
        "YB_INDEX_ROWS", "YB_INDEX_SAMPLES", "YB_INDEX_REPORT").forEach { key ->
        providers.gradleProperty(key).orNull?.let { systemProperty(key, it) }
    }
    // 密钥仅由环境传入子进程，不放在 JVM 参数或 Gradle -P 命令行中。
    listOf("YB_S3_ACCESS_KEY", "YB_S3_SECRET_KEY").forEach { key ->
        providers.environmentVariable(key).orNull?.let { environment(key, it) }
    }
}

// 公开基准：输出机器可比的 JSON（bench/current.json），供 scripts/bench-compare.py
// 与基线逐项对比。请求数是确定性指标，用于 PR 硬门禁；延迟只做告警。
tasks.register<JavaExec>("benchPublic") {
    description = "运行公开基准并输出 JSON（-PYB_BENCH_PROFILE=default|ci -PYB_BENCH_OUT=bench/current.json）"
    group = "verification"
    mainClass.set("com.cloudea.yangbase.bench.PublicBenchKt")
    classpath = sourceSets["main"].runtimeClasspath
    // 输出路径按仓库根解析（`bench/current.json`），不随 Gradle 的模块工作目录漂移
    workingDir = rootProject.projectDir
    jvmArgs("-Xmx2g")
    systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")
    listOf("YB_BENCH_PROFILE", "YB_BENCH_OUT", "YB_BENCH_LABEL", "YB_BENCH_DEBUG").forEach { key ->
        providers.gradleProperty(key).orNull?.let { systemProperty(key, it) }
    }
    listOf("YB_S3_ENDPOINT", "YB_S3_BUCKET").forEach { key ->
        providers.gradleProperty(key).orNull?.let { systemProperty(key, it) }
    }
    listOf("YB_S3_ACCESS_KEY", "YB_S3_SECRET_KEY").forEach { key ->
        providers.environmentVariable(key).orNull?.let { environment(key, it) }
    }
}
