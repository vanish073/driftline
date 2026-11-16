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

// ./gradlew :sim:campaign --args="--from 1 --count 10000 --threads 8"   (DESIGN §11.12)
tasks.register<JavaExec>("campaign") {
    group = "simulation"
    description = "Runs the regression corpus, then many simulator seeds on several threads."
    mainClass = "io.github.vanish073.driftline.sim.runner.CampaignMain"
    classpath = sourceSets["main"].runtimeClasspath
    maxHeapSize = "768m"
    jvmArgs("-XX:+UseParallelGC")
}

// ./gradlew :sim:canaries   (seeds-to-detection per planted bug, DESIGN §11.10)
tasks.register<JavaExec>("canaries") {
    group = "simulation"
    description = "Plants each active canary and counts the pr seeds until the simulator catches it."
    mainClass = "io.github.vanish073.driftline.sim.runner.CanaryMain"
    classpath = sourceSets["main"].runtimeClasspath
    maxHeapSize = "512m"
    jvmArgs("-XX:+UseParallelGC")
}

// ./gradlew :sim:determinism   (INV-DETERMINISTIC: 100 seeds run twice in one JVM, digests written for a
// cross-JVM diff; CI runs it twice and diffs the two files)
tasks.register<JavaExec>("determinism") {
    group = "simulation"
    description = "Runs 100 pr seeds twice, compares their trace digests and writes them to a file."
    mainClass = "io.github.vanish073.driftline.sim.runner.CampaignMain"
    classpath = sourceSets["main"].runtimeClasspath
    maxHeapSize = "512m"
    jvmArgs("-XX:+UseParallelGC")
    args("--from", "1", "--count", "100", "--double-run", "100", "--regression", "none",
        "--digests", "build/sim-results/digests.txt")
}

dependencies {
    implementation(project(":core"))
    implementation(project(":wire"))
    testImplementation(testFixtures(project(":core")))
    // Shift 7: the settlement case corpus is YAML, read with Jackson's YAML module on the line the BOM pins.
    testImplementation(platform(libs.jackson.bom))
    testImplementation(libs.jackson.dataformat.yaml)
}

// The settlement case corpus lives at core/src/test/resources/settlement-cases/*.yaml (SHIFTS Shift 7). The cases run
// through the real Matcher and InMemoryStore, which live here, so the corpus is on this module's test classpath.
tasks.named<ProcessResources>("processTestResources") {
    from(rootProject.file("core/src/test/resources")) {
        include("settlement-cases/**")
    }
}
