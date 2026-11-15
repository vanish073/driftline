// Wire formats: envelope and webhook codecs, HMAC, settlement CSV parser. Deterministic; used by the simulator.
plugins {
    id("driftline.plain-java")
}

dependencies {
    api(project(":core"))
    // StrictJson.write hands callers a JsonGenerator, so Jackson's streaming core is part of wire's API.
    api(platform(libs.jackson.bom))
    api(libs.jackson.core)
    testImplementation(testFixtures(project(":core")))
}
