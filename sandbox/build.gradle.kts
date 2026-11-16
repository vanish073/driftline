// The simulator's world models served over the JDK HttpServer (no Spring, DESIGN.md D20, D51): the merchant pool, the
// scenario injector CLI, and the Compose e2e suite.
plugins {
    id("driftline.plain-java")
    application
}

application {
    mainClass = "io.github.vanish073.driftline.sandbox.SandboxMain"
    applicationDefaultJvmArgs = listOf("-Xmx128m", "-XX:+UseSerialGC")
}

dependencies {
    implementation(project(":core"))
    implementation(project(":wire"))
    implementation(project(":sim"))
    testImplementation(testFixtures(project(":core")))
}

testing {
    suites {
        // The end-to-end suite against a running Compose stack (scripts/e2e.sh). Never part of `check`: it needs the
        // stack. It runs from the JUnit console launcher with Gradle stopped, to fit the laptop budget (DESIGN §15.1).
        register<JvmTestSuite>("e2eTest") {
            useJUnitJupiter(libs.versions.junit5.get())
            dependencies {
                implementation(project())
                implementation(project(":core"))
                implementation(project(":wire"))
                implementation(project(":sim"))
                implementation(libs.assertj)
            }
        }
    }
}

val e2eLauncher by configurations.creating
dependencies {
    e2eLauncher(libs.junit.platform.console.standalone)
}

// build/e2e/{classes,lib,launcher}: everything scripts/e2e.sh needs to run the suite without Gradle.
tasks.register<Sync>("e2eDist") {
    group = "verification"
    description = "Packages the e2e suite to run from the JUnit console launcher."
    val e2e = sourceSets["e2eTest"]
    from(e2e.output) { into("classes") }
    from(e2e.runtimeClasspath.filter { it.isFile && !it.name.startsWith("junit-") && !it.name.startsWith("opentest4j")
            && !it.name.startsWith("apiguardian") }) { into("lib") }
    from(e2eLauncher) { into("launcher") }
    into(layout.buildDirectory.dir("e2e"))
}
