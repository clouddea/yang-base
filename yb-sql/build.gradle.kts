dependencies {
    api(project(":yb-common"))
    api(project(":yb-storage"))
    api(project(":yb-txn"))
    api(project(":yb-index"))
    implementation("org.slf4j:slf4j-api:2.0.16")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
