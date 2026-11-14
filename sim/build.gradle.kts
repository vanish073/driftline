// Deterministic simulator: kernel, world models, faults, invariants, canaries. The `sim.runner` package is the
// driver (CLI, campaign threads, file output) and sits outside the determinism rules (DESIGN.md D22).
plugins {
    id("driftline.plain-java")
    application
}

application {
    mainClass = "io.github.vanish073.driftline.sim.runner.SimMain"
}

tasks.named<JavaExec>("run") {
    maxHeapSize = "384m"
}

dependencies {
    implementation(project(":core"))
    implementation(project(":wire"))
    testImplementation(testFixtures(project(":core")))
}
