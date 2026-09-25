dependencies {
    implementation(project(":yb-common"))
    implementation(project(":yb-objectstore"))
    implementation(project(":yb-storage"))
    implementation(project(":yb-txn"))
    implementation("org.slf4j:slf4j-api:2.0.16")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

// CLI 与 bench 一样以 main 函数形式运行。
apply(plugin = "application")
extensions.configure<JavaApplication> {
    mainClass.set("com.cloudea.yangbase.cli.YbCli")
    applicationName = "yb-cli"
}
tasks.register<JavaExec>("cli") {
    description = "运行 yb-cli（参数用 -Pargs=\"ls\" 传入）"
    group = "application"
    mainClass.set("com.cloudea.yangbase.cli.YbCli")
    classpath = sourceSets["main"].runtimeClasspath
    (project.findProperty("args") as String?)?.split(" ")?.let { args(it) }
}

// CLI 不含生产代码路径（它只消费其它模块），不纳入覆盖率门禁
tasks.matching { it.name == "koverVerify" }.configureEach { enabled = false }

// 测试需要能起一个真实的 CN 来造数据，再让 CLI 去读同一个对象存储
dependencies {
    testImplementation(project(":yb-server"))
    testImplementation("com.mysql:mysql-connector-j:9.1.0")
}
