dependencies {
    api(project(":yb-common"))
    api(project(":yb-storage"))
    implementation("org.slf4j:slf4j-api:2.0.16")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
