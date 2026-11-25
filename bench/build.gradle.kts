// The benchmarks of Shift 10 (DESIGN §17): the hot-account benchmark against real PostgreSQL, and the open-model load
// generator against a running stack. Plain Java, no Spring (D18).
plugins {
    id("driftline.plain-java")
    application
}

// The load generator runs from bench/build/install/bench/bin/bench with no Gradle daemon alive (DESIGN §15.1, §17).
application {
    mainClass = "io.github.vanish073.driftline.bench.LoadGenerator"
    applicationDefaultJvmArgs = listOf("-Xmx192m", "-XX:+UseSerialGC")
}

dependencies {
    implementation(project(":core"))
    implementation(project(":wire"))
    implementation(project(":store-postgres"))
    // the benchmark migrates its own scratch database with the same migrations production runs
    implementation(libs.flyway.core)
    runtimeOnly(libs.flyway.database.postgresql)
    runtimeOnly(libs.postgresql)
    testImplementation(testFixtures(project(":core")))
}

// ./gradlew :bench:hotAccounts --args="--jdbc jdbc:postgresql://127.0.0.1:55432/driftline ..."   (DESIGN §7.5, §17)
tasks.register<JavaExec>("hotAccounts") {
    group = "benchmark"
    description = "ROW_LOCK vs APPEND_SNAPSHOT at several writer counts on one merchant's hot accounts."
    mainClass = "io.github.vanish073.driftline.bench.HotAccountBenchmark"
    classpath = sourceSets["main"].runtimeClasspath
    maxHeapSize = "384m"
}
