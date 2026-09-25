dependencies {
    api(project(":yb-common"))
    api(project(":yb-storage"))
    api(project(":yb-txn"))
    api(project(":yb-ms"))
    api(project(":yb-index"))
    api(project(":yb-sql"))
    api(project(":yb-mysql"))
    implementation("org.slf4j:slf4j-api:2.0.16")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
    testImplementation("com.mysql:mysql-connector-j:9.1.0")
}


// ── 覆盖率门禁（聚合测量）──────────────────────────────────────────
// 端到端测试通过真实 MySQL 协议跑通了 yb-sql / yb-mysql / yb-txn / yb-storage
// 的全部主要路径，因此覆盖率必须把这些模块合并起来看才准确。
dependencies {
    kover(project(":yb-common"))
    kover(project(":yb-objectstore"))
    kover(project(":yb-storage"))
    kover(project(":yb-txn"))
    kover(project(":yb-ms"))
    kover(project(":yb-index"))
    kover(project(":yb-sql"))
    kover(project(":yb-mysql"))
}

// 本模块承担门禁，因此重新启用 verify
tasks.matching { it.name == "koverVerify" }.configureEach { enabled = true }

extensions.configure<kotlinx.kover.gradle.plugin.dsl.KoverProjectExtension> {
    reports {
        verify {
            rule {
                bound {
                    minValue = 50
                    coverageUnits = kotlinx.kover.gradle.plugin.dsl.CoverageUnit.BRANCH
                }
            }
        }
    }
}

// ── 可执行入口 ─────────────────────────────────────────────────────
// README 里写的是 `./gradlew :yb-server:run --args="..."`，
// 而 Kotlin JVM 插件不提供 run 任务——必须应用 application 插件。
apply(plugin = "application")
extensions.configure<JavaApplication> {
    mainClass.set("com.cloudea.yangbase.server.YangBaseServerKt")
    applicationName = "yangbase-server"
}
