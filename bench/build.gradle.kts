// Load generator and hot-account benchmark. Arrives in Shift 10.
plugins {
    id("driftline.plain-java")
}

dependencies {
    implementation(project(":core"))
    implementation(project(":wire"))
    testImplementation(testFixtures(project(":core")))
}
