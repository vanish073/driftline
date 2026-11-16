package io.github.vanish073.driftline.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Shift 8 Prove: each of the resolver's and dispatcher's crash points (DESIGN §9.4) is forced on purpose in dedicated
 * seeds, on top of the {@code pr} swarm, and every invariant still holds (one refund per payment, no decided refund
 * lost, books matching the world). A forced dispatcher point strikes a REFUND call. CP6–CP8 are armed by NODE_CRASH
 * faults in every campaign.
 */
class ForcedCrashPointTest {

    static final int SEEDS = 10;

    @ParameterizedTest
    @EnumSource(value = CrashPoint.class, names = {"CP1", "CP2", "CP3", "CP4", "CP5"})
    void everyInvariantHoldsWhenTheCrashPointStrikes(CrashPoint point) {
        long struck = 0;
        long refunds = 0;
        for (long seed = 1; seed <= SEEDS; seed++) {
            RunResult r = Simulation.run(seed, Profile.PR, Canaries.NONE, false, Optional.of(point));
            assertThat(r.violation()).as(r.repro()).isEmpty();
            struck += r.stats().getOrDefault("forced.crashes", 0L);
            refunds += r.stats().getOrDefault("gateway.refunds", 0L);
        }
        assertThat(struck).as("%s struck in %d seeds", point, SEEDS).isGreaterThanOrEqualTo(SEEDS);
        assertThat(refunds).as("the seeds refund something").isPositive();
    }
}
