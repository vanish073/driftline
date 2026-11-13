package io.github.vanish073.driftline.wire;

import com.tngtech.archunit.core.domain.JavaClasses;
import io.github.vanish073.driftline.testkit.arch.DriftlineRules;
import org.junit.jupiter.api.Test;

/** The wire codecs run inside the simulator, so they follow the determinism rules (DESIGN §11.3, §11.11). */
class WireArchitectureTest {

    private static final JavaClasses WIRE = DriftlineRules.importMainClasses(DriftlineRules.ROOT + ".wire");

    @Test
    void isDeterministic() {
        DriftlineRules.DETERMINISM.check(WIRE);
    }

    @Test
    void hasNoFloatingPoint() {
        DriftlineRules.NO_FLOATING_POINT.check(WIRE);
    }

    @Test
    void hasNoSpring() {
        DriftlineRules.NO_SPRING.check(WIRE);
    }

    @Test
    void neverBuildsSimulatorOnlyObjects() {
        DriftlineRules.SIM_ONLY_CONSTRUCTION.check(WIRE);
    }
}
