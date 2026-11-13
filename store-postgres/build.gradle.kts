// PostgresStore (plain JDBC, HikariCP), the Flyway migrations and both balance strategies (DESIGN §7, §12). No Spring
// (D18): its contract tests run without a Spring context. Flyway itself is only a test dependency here; in production
// Spring Boot runs these migrations from the app module (DESIGN §18).
plugins {
    id("driftline.plain-java")
}

dependencies {
    implementation(project(":core"))
    implementation(libs.hikaricp)
    runtimeOnly(libs.postgresql)
    testImplementation(testFixtures(project(":core")))
}

testing {
    suites {
        // Real PostgreSQL in Testcontainers: the contract suite on both balance strategies, the database-level ledger
        // guards (raw SQL as the app role), concurrency and the snapshot horizon (DESIGN §7.3, §7.5, §11.9).
        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter(libs.versions.junit5.get())
            dependencies {
                implementation(project())
                implementation(project(":core"))
                implementation(testFixtures(project(":core")))
                implementation(libs.postgresql)
                implementation(libs.hikaricp)
                implementation(libs.flyway.core)
                runtimeOnly(libs.flyway.database.postgresql)
                implementation(libs.testcontainers.postgresql)
                implementation(libs.jqwik)
                implementation(libs.assertj)
            }
            targets.all {
                testTask.configure {
                    shouldRunAfter(tasks.test)
                }
            }
        }
    }
}

tasks.named("check") {
    dependsOn(testing.suites.named("integrationTest"))
}
