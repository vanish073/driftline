// JDK only: no third-party runtime dependencies, ever (DESIGN.md §3.1, §18).
plugins {
    id("driftline.plain-java")
    `java-test-fixtures`
}

dependencies {
    // Shared ArchUnit rule set (determinism, Spring confinement, money types) used by core, wire and sim tests.
    testFixturesApi(libs.archunit)
    // The Store contract suite. Implementation scope, so app (which runs JUnit 6, D23) never gets JUnit 5 from here;
    // every module that runs the suite brings its own JUnit 5 through the plain-java convention.
    testFixturesImplementation(platform(libs.junit5.bom))
    testFixturesImplementation(libs.junit.jupiter.api)
    testFixturesImplementation(libs.assertj)
}
