// Wire formats: envelope and webhook codecs, HMAC, settlement CSV parser. Deterministic; used by the simulator.
plugins {
    id("driftline.plain-java")
}

dependencies {
    api(project(":core"))
    testImplementation(testFixtures(project(":core")))
}
