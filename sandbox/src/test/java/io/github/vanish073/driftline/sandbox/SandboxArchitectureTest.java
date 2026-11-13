package io.github.vanish073.driftline.sandbox;

import com.tngtech.archunit.core.domain.JavaClasses;
import io.github.vanish073.driftline.testkit.arch.DriftlineRules;
import org.junit.jupiter.api.Test;

/** The sandbox is served by the JDK HttpServer, not Spring (D20). */
class SandboxArchitectureTest {

    private static final JavaClasses CLASSES = DriftlineRules.importMainClasses("io.github.vanish073.driftline.sandbox");

    @Test
    void hasNoSpring() {
        DriftlineRules.NO_SPRING.check(CLASSES);
    }

    @Test
    void neverBuildsCanaries() {
        DriftlineRules.CANARIES_BUILT_ONLY_IN_SIM.check(CLASSES);
    }
}
