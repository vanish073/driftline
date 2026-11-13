package io.github.vanish073.driftline.app;

import com.tngtech.archunit.core.domain.JavaClasses;
import io.github.vanish073.driftline.testkit.arch.DriftlineRules;
import org.junit.jupiter.api.Test;

/** Production must never switch a canary on (DESIGN §11.10). From Shift 6 the app also checks this at startup. */
class AppArchitectureTest {

    private static final JavaClasses APP = DriftlineRules.importMainClasses(DriftlineRules.ROOT + ".app");

    @Test
    void neverBuildsSimulatorOnlyObjects() {
        DriftlineRules.SIM_ONLY_CONSTRUCTION.check(APP);
    }
}
