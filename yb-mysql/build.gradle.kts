dependencies {
    api(project(":yb-common"))
    api(project(":yb-sql"))
    implementation("io.netty:netty-all:4.1.115.Final")
    implementation("org.slf4j:slf4j-api:2.0.16")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
    // 集成测试用真实 JDBC 驱动验证协议兼容性
    testImplementation("com.mysql:mysql-connector-j:9.1.0")
}
