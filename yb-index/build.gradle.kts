dependencies {
    api(project(":yb-common"))
    api(project(":yb-storage"))
    // 索引文件必须落在对象存储上（P1），且复用 §6.7 的三层缓存与在途去重
    api(project(":yb-objectstore"))
    implementation("org.slf4j:slf4j-api:2.0.16")
    // 段元信息 / 跳数索引 / 删除位图用 CBOR（只在加载时解析一次，之后常驻内存）；
    // postings / positions / docmap 用手写二进制（热路径，要精确控制偏移与分块）
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-cbor:1.7.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
