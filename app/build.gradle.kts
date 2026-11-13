// The only Spring Boot module (DESIGN.md D17, D18). It wires the core's ports explicitly; no scanning of the core.
plugins {
    id("driftline.java-conventions")
    alias(libs.plugins.spring.boot)
}

dependencies {
    implementation(platform(libs.spring.boot.bom))
    implementation(project(":core"))
    implementation(libs.spring.boot.starter)

    testImplementation(platform(libs.spring.boot.bom))
    testImplementation(libs.spring.boot.starter.test)
    // The shared ArchUnit rules (canary confinement). ArchUnit itself has no JUnit dependency.
    testImplementation(testFixtures(project(":core")))
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    // Proves the app starts inside the laptop memory budget (DESIGN.md §15.1).
    maxHeapSize = "256m"
    jvmArgs("-XX:+UseSerialGC", "-Xss512k")
}
