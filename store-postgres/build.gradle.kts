// PostgresStore (plain JDBC), Flyway migrations, balance strategies. Arrives in Shift 2.
plugins {
    id("driftline.plain-java")
}

dependencies {
    implementation(project(":core"))
    testImplementation(testFixtures(project(":core")))
}
