package io.github.vanish073.driftline.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.domain.JavaClasses;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.payment.PaymentStateMachine;
import io.github.vanish073.driftline.testkit.arch.DriftlineRules;
import org.junit.jupiter.api.Test;

/** The core runs unchanged in production and in the simulator, so every rule applies to it (DESIGN §3.1, §11.11). */
class CoreArchitectureTest {

    private static final JavaClasses CORE = DriftlineRules.importMainClasses(DriftlineRules.ROOT + ".core");

    @Test
    void theImportSeesProductionClassesOnly() {
        assertThat(CORE.contain(Money.class)).isTrue();
        assertThat(CORE.contain(PaymentStateMachine.class)).isTrue();
        assertThat(CORE.contain(CoreArchitectureTest.class)).isFalse();
    }

    @Test
    void isDeterministic() {
        DriftlineRules.DETERMINISM.check(CORE);
    }

    @Test
    void hasNoFloatingPoint() {
        DriftlineRules.NO_FLOATING_POINT.check(CORE);
    }

    @Test
    void hasNoSpring() {
        DriftlineRules.NO_SPRING.check(CORE);
    }

    @Test
    void neverBuildsCanaries() {
        DriftlineRules.CANARIES_BUILT_ONLY_IN_SIM.check(CORE);
    }
}
