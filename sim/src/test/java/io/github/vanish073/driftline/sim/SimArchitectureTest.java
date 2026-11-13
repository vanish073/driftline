package io.github.vanish073.driftline.sim;

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideOutsideOfPackage;

import com.tngtech.archunit.core.domain.JavaClasses;
import io.github.vanish073.driftline.testkit.arch.DriftlineRules;
import org.junit.jupiter.api.Test;

/**
 * Everything in the simulator except its driver ({@code sim.runner}: CLI, campaign threads, files) must be
 * deterministic, and nothing else in it may depend on the driver (D22).
 */
class SimArchitectureTest {

    private static final JavaClasses SIM = DriftlineRules.importMainClasses(DriftlineRules.SIM);
    private static final JavaClasses DETERMINISTIC_PART = SIM.that(resideOutsideOfPackage(DriftlineRules.SIM_RUNNER + ".."));

    @Test
    void isDeterministicOutsideTheRunner() {
        DriftlineRules.DETERMINISM.check(DETERMINISTIC_PART);
    }

    @Test
    void nothingDependsOnTheRunner() {
        DriftlineRules.NOTHING_DEPENDS_ON_SIM_RUNNER.check(SIM);
    }

    @Test
    void hasNoSpring() {
        DriftlineRules.NO_SPRING.check(SIM);
    }
}
