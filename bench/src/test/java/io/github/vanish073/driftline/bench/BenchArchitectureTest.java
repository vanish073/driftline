package io.github.vanish073.driftline.bench;

import com.tngtech.archunit.core.domain.JavaClasses;
import io.github.vanish073.driftline.testkit.arch.DriftlineRules;
import org.junit.jupiter.api.Test;

/** The benchmark tools carry no Spring (D18). */
class BenchArchitectureTest {

    private static final JavaClasses CLASSES = DriftlineRules.importMainClasses("io.github.vanish073.driftline.bench");

    @Test
    void hasNoSpring() {
        DriftlineRules.NO_SPRING.check(CLASSES);
    }

    @Test
    void neverBuildsCanaries() {
        DriftlineRules.CANARIES_BUILT_ONLY_IN_SIM.check(CLASSES);
    }
}
