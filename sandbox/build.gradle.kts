// The simulator's world models served over the JDK HttpServer (no Spring, DESIGN.md D20). Arrives in Shift 6.
plugins {
    id("driftline.plain-java")
}

dependencies {
    implementation(project(":core"))
    implementation(project(":wire"))
    implementation(project(":sim"))
    testImplementation(testFixtures(project(":core")))
}
