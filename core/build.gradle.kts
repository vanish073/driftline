// JDK only: no third-party runtime dependencies, ever (DESIGN.md §3.1, §18).
plugins {
    id("driftline.plain-java")
    `java-test-fixtures`
}

dependencies {
    // Shared ArchUnit rule set (determinism, Spring confinement, money types) used by core, wire and sim tests.
    testFixturesApi(libs.archunit)
}
