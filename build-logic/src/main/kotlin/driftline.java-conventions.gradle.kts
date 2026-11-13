// Shared by every module: Java 21, strict compiler, test runner limits sized for a small machine.
import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    `java-library`
}

group = "io.github.vanish073.driftline"
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 21
    options.encoding = "UTF-8"
    // -Xpkginfo:always emits package-info.class even without annotations, so a module with no code yet still has a
    // class that its ArchitectureTest can find (the test fails if its import is empty).
    options.compilerArgs.addAll(listOf("-Xlint:all,-serial,-processing", "-Werror", "-parameters", "-Xpkginfo:always"))
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    maxHeapSize = "384m"
    maxParallelForks = 1
    testLogging {
        events("failed", "skipped")
        exceptionFormat = TestExceptionFormat.FULL
    }
}
