// The only Spring Boot module (DESIGN.md D17, D18). It wires the core's ports explicitly; no scanning of the core.
plugins {
    id("driftline.java-conventions")
    alias(libs.plugins.spring.boot)
}

dependencies {
    implementation(platform(libs.spring.boot.bom))
    implementation(project(":core"))
    implementation(project(":wire"))
    implementation(project(":store-postgres"))
    implementation(libs.spring.boot.starter)
    implementation(libs.spring.boot.starter.webmvc)
    implementation(libs.spring.boot.starter.security)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.spring.boot.starter.jdbc)
    implementation(libs.spring.boot.starter.flyway)
    implementation(libs.kafka.clients)
    implementation(platform(libs.opentelemetry.bom))
    implementation(libs.opentelemetry.api)
    constraints {
        // D79: patch releases that fix high-severity advisories in what kafka-clients brings (the producer uses lz4)
        implementation(libs.lz4.java) { because("GHSA-mcr4-qmvw-px4g and two lower ones, fixed in 1.11.4") }
        implementation(libs.snappy.java) { because("GHSA-gwq9-9ffj-mvrv and GHSA-q233-g92q-qxp6, fixed in 1.1.10.9") }
    }
    runtimeOnly(libs.micrometer.registry.prometheus)
    runtimeOnly(libs.boot.flyway.database.postgresql)
    runtimeOnly(libs.boot.postgresql)

    testImplementation(platform(libs.spring.boot.bom))
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.spring.boot.starter.webmvc.test)
    testImplementation(libs.spring.security.test)
    testImplementation(platform(libs.opentelemetry.bom))
    testImplementation(libs.opentelemetry.sdk.testing)
    // the names the Grafana board and the alert rules use are checked against a real Prometheus scrape (D78)
    testImplementation(libs.micrometer.registry.prometheus)
    // The shared ArchUnit rules (canary confinement). ArchUnit itself has no JUnit dependency.
    testImplementation(testFixtures(project(":core")))
    testRuntimeOnly(libs.junit.platform.launcher)
}

testing {
    suites {
        // Real PostgreSQL and Kafka in Testcontainers (DESIGN §11.9): the start-up smoke test under the production heap
        // cap, the partition driver (rebalance, zombie fencing, crash mid-batch, replay), ingest through Kafka, and the
        // outbox crash points CP1-CP5 against the sandbox gateway.
        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter()
            dependencies {
                implementation(project())
                implementation(platform(libs.spring.boot.bom))
                implementation(project(":core"))
                implementation(project(":wire"))
                implementation(project(":store-postgres"))
                // OutboxCrashIT runs the sandbox's gateway in-process (the simulator's world models over HTTP, D65)
                implementation(project(":sandbox"))
                implementation(libs.spring.boot.starter.test)
                implementation(platform(libs.opentelemetry.bom))
                implementation(libs.opentelemetry.sdk.testing)
                implementation(libs.kafka.clients)
                implementation(libs.boot.testcontainers.kafka)
                implementation(libs.boot.testcontainers.postgresql)
                implementation(libs.boot.postgresql)
                implementation(libs.spring.boot.starter.jdbc)
                implementation(libs.flyway.core)
                runtimeOnly(libs.boot.flyway.database.postgresql)
                implementation(libs.hikaricp)
                runtimeOnly(libs.junit.platform.launcher)
            }
            targets.all {
                testTask.configure {
                    shouldRunAfter(tasks.test)
                    // Everything here runs inside the laptop budget (DESIGN §15.1): the app's production heap cap.
                    maxHeapSize = "256m"
                    jvmArgs("-XX:+UseSerialGC", "-Xss512k")
                }
            }
        }
    }
}

tasks.named("check") {
    dependsOn(testing.suites.named("integrationTest"))
}
