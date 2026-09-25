dependencies {
    // MS 只依赖「协调接口 + 对象存储」——它**不**依赖 SQL、存储引擎、MySQL 协议。
    // 这条依赖边界是刻意的：MS 必须能被单独部署到 3 台小规格实例上，
    // 把整个数据库的依赖拖过去会让它无法"随时被抢占/迁移"（§4.6 的冷启动目标）
    api(project(":yb-common"))
    api(project(":yb-objectstore"))
    api(project(":yb-txn"))
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")
}
