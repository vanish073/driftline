package io.github.vanish073.driftline.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Shift 4 Prove, inside {@code ./gradlew check}: each active canary (DESIGN §11.10) is caught within
 * {@value CanaryCatalog#SEED_BUDGET} {@code pr} seeds, by one of the invariants it is meant to trip. The same search,
 * with its table, is {@code ./gradlew :sim:canaries}.
 */
class CanaryDetectionTest {

    @TestFactory
    Stream<DynamicTest> everyActiveCanaryIsCaughtWithinTheBudget() {
        return CanaryCatalog.active().stream().map(canary -> DynamicTest.dynamicTest(canary.code() + " " + canary.name(),
                () -> assertCaught(canary)));
    }

    private static void assertCaught(Canary canary) {
        for (long seed = 1; seed <= CanaryCatalog.SEED_BUDGET; seed++) {
            RunResult r = Simulation.run(seed, Profile.PR, Canaries.of(canary), false);
            if (!r.passed()) {
                assertThat(r.violation().orElseThrow().invariant()).as(canary.code() + " at seed " + seed)
                        .isIn(CanaryCatalog.expected(canary));
                return;
            }
        }
        throw new AssertionError(canary.code() + " survived " + CanaryCatalog.SEED_BUDGET + " pr seeds");
    }
}
