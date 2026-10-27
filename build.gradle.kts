import org.gradle.api.tasks.SourceSetContainer
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension
import kotlinx.kover.gradle.plugin.dsl.CoverageUnit
import kotlinx.kover.gradle.plugin.dsl.KoverProjectExtension

plugins {
    kotlin("jvm") version "2.1.0" apply false
    kotlin("plugin.serialization") version "2.1.0" apply false
    id("org.jetbrains.kotlinx.kover") version "0.8.3" apply false
}

allprojects {
    group = "com.cloudea.yangbase"
    // Single source of truth for the version number: `version.txt` at the repo root.
    // A release tag must equal it; `.github/workflows/release.yml` and
    // `scripts/release-preflight.sh` both verify that mechanically, so a build
    // can never claim a version that was never released.
    version = rootProject.file("version.txt").readText().trim()
    repositories { mavenCentral() }
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.serialization")
    apply(plugin = "org.jetbrains.kotlinx.kover")

    extensions.configure<KotlinJvmProjectExtension> {
        jvmToolchain(21)
    }

    dependencies {
        add("testImplementation", "org.jetbrains.kotlin:kotlin-test")
        add("testImplementation", "org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
    }

    val integrationTag = "integration"

    // 把外部对象存储的配置透传给测试 JVM。
    // ⚠️ 必须用 -P 属性而不是环境变量：Gradle 守护进程是长期存活的，
    // 它在启动时就固定了环境，之后 shell 里 export 的变量**不会**传导进来
    // （这是实现期间踩到的坑——设了 YB_S3_ENDPOINT 却仍跑在内嵌后端上）。
    val s3Props = listOf(
        "YB_S3_ENDPOINT", "YB_S3_BUCKET", "YB_S3_REGION",
        "YB_S3_ACCESS_KEY", "YB_S3_SECRET_KEY",
        // 后端是阿里云 OSS 时要多传两个：它拒绝路径式寻址（必须 false），
        // 而且没有 ETag CAS（`YB_S3_OSS=true` 声明这一点并接受指针切换的非原子降级）
        "YB_S3_PATH_STYLE", "YB_S3_OSS",
    )
    tasks.withType<Test>().configureEach {
        s3Props.forEach { k ->
            (project.findProperty(k) as String?)?.let { systemProperty(k, it) }
        }
    }

    // 单元测试：只跑 src/test（集成用例在独立源集里，天然不会混进来）
    tasks.named<Test>("test") {
        useJUnitPlatform { excludeTags(integrationTag) }
        testLogging {
            events("passed", "skipped", "failed")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }

    // ── 集成测试源集 ──────────────────────────────────────────────
    // 单独一套 src/integrationTest，而不是把集成用例混在 src/test 里靠 tag 过滤。
    // 理由是集成测试要**真的连对象存储**：共用一套类路径会让「误把集成用例
    // 当作单元测试跑」变成一个随时可能发生的意外，而代价是 CI 忽然需要
    // 一个外部服务。物理隔离比 tag 过滤可靠。
    val testSourceSet = extensions.getByType<SourceSetContainer>().getByName("test")
    val mainOutput = extensions.getByType<SourceSetContainer>().getByName("main").output
    val integrationSourceSet = extensions.getByType<SourceSetContainer>().create("integrationTest") {
        // ⚠️ `main` 的输出必须显式加上：新建的源集不会自动继承任何东西，
        // 只加 test 的输出会得到一堆「Unresolved reference」——被引用的类明明就在隔壁。
        compileClasspath += mainOutput
        runtimeClasspath += mainOutput
    }
    configurations["integrationTestImplementation"].extendsFrom(configurations["testImplementation"])
    configurations["integrationTestRuntimeOnly"].extendsFrom(configurations["testRuntimeOnly"])

    tasks.register<Test>("integrationTest") {
        description = "运行 src/integrationTest 下需要真实对象存储语义的用例"
        group = "verification"
        testClassesDirs = integrationSourceSet.output.classesDirs
        classpath = integrationSourceSet.runtimeClasspath
        useJUnitPlatform { includeTags(integrationTag) }
        shouldRunAfter(tasks.named("test"))
        // 集成测试跑的是外部对象存储，Gradle 的构建缓存看不到它的状态变化，
        // 会把「上一次针对另一个后端的结果」当成可复用的输出（踩过这个坑）。
        outputs.upToDateWhen { false }
        testLogging {
            events("passed", "skipped", "failed")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }

    extensions.configure<KoverProjectExtension> {
        useJacoco()
    }

    // 逐模块的 verify 关掉：门禁在 yb-server 上按**聚合测量**判定（见 yb-server/build.gradle.kts）
    tasks.matching { it.name == "koverVerify" }.configureEach { enabled = false }
}


// ── 覆盖率门禁 ─────────────────────────────────────────────────────
// 见 yb-server/build.gradle.kts：门禁按 yb-server 的**聚合测量**判定。
// 理由：端到端测试挂在 yb-server 上，但它真正跑的是 yb-sql / yb-mysql / yb-txn
// 的代码。逐模块统计会把这些模块记成 0%——数字很低，代码却是被覆盖的。
