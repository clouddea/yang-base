dependencies {
    api(project(":yb-common"))
    api(project(":yb-objectstore"))
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("org.lz4:lz4-java:1.8.0")
    // 元数据用 CBOR（可读性优先、代码量小）；SST 数据块与 WAL 用手写二进制（见 CLAUDE.md 编码约定）
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-cbor:1.7.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
