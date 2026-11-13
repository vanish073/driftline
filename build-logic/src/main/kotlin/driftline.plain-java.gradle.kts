// Every module except `app`: no Spring allowed (DESIGN.md D18); tests use JUnit 5 + jqwik + AssertJ + ArchUnit.
plugins {
    id("driftline.java-conventions")
}

val libs = the<VersionCatalogsExtension>().named("libs")

dependencies {
    testImplementation(platform(libs.findLibrary("junit5-bom").get()))
    testImplementation(libs.findLibrary("junit-jupiter").get())
    testImplementation(libs.findLibrary("jqwik").get())
    testImplementation(libs.findLibrary("assertj").get())
    testImplementation(libs.findLibrary("archunit").get())
    testRuntimeOnly(libs.findLibrary("junit-platform-launcher").get())
}

fun componentsOf(configurationName: String): Provider<List<String>> =
    configurations.named(configurationName).flatMap { configuration ->
        configuration.incoming.artifacts.resolvedArtifacts.map { artifacts ->
            artifacts.map { it.id.componentIdentifier.displayName }
        }
    }

val verifyNoSpring = tasks.register<VerifyNoSpring>("verifyNoSpring") {
    description = "Checks that no org.springframework artifact is on the compile or runtime classpath."
    group = "verification"
    components.addAll(componentsOf("compileClasspath"))
    components.addAll(componentsOf("runtimeClasspath"))
    report = layout.buildDirectory.file("verify-no-spring.txt")
}

tasks.named("check") {
    dependsOn(verifyNoSpring)
}
