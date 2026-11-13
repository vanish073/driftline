package io.github.vanish073.driftline.store.postgres;

import com.tngtech.archunit.core.domain.JavaClasses;
import io.github.vanish073.driftline.testkit.arch.DriftlineRules;
import org.junit.jupiter.api.Test;

/** PostgresStore uses plain JDBC: no Spring, so its contract tests run without a Spring context (D18). */
class PostgresStoreArchitectureTest {

    private static final JavaClasses CLASSES = DriftlineRules.importMainClasses("io.github.vanish073.driftline.store.postgres");

    @Test
    void hasNoSpring() {
        DriftlineRules.NO_SPRING.check(CLASSES);
    }

    @Test
    void neverBuildsCanaries() {
        DriftlineRules.CANARIES_BUILT_ONLY_IN_SIM.check(CLASSES);
    }
}
