plugins {
    `kotlin-dsl`
}

// Pin the convention plugins' bytecode to Java 21, whatever JVM the daemon runs on.
kotlin {
    jvmToolchain(21)
}
