package io.github.vanish073.driftline.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.sim.world.Scenario;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * Shift 3 Prove: 1,000 {@code calm} seeds pass every invariant, and each one, run twice, gives the same trace digest
 * (INV-DETERMINISTIC).
 */
class CalmCampaignTest {

    private static final int SEEDS = 1_000;

    @Test
    void aThousandCalmSeedsPassAndReproduceExactly() {
        List<String> failures = new ArrayList<>();
        TreeSet<String> digests = new TreeSet<>();
        TreeMap<String, Long> totals = new TreeMap<>();
        for (long seed = 1; seed <= SEEDS; seed++) {
            RunResult first = Simulation.run(seed, Profile.CALM, false);
            RunResult second = Simulation.run(seed, Profile.CALM, false);
            if (!first.passed()) {
                failures.add(first.violation().orElseThrow() + "  REPRO: " + first.repro());
            }
            if (!first.digest().equals(second.digest())) {
                failures.add("seed " + seed + " is not deterministic: " + first.digest() + " vs " + second.digest());
            }
            digests.add(first.digest());
            first.stats().forEach((k, v) -> totals.merge(k, v, Long::sum));
        }
        assertThat(failures).as("failing seeds").isEmpty();
        assertThat(digests).as("different seeds must give different runs").hasSize(SEEDS);
        for (Scenario s : Scenario.values()) {
            assertThat(totals.getOrDefault("scenario." + s.name(), 0L)).as(s.name()).isPositive();
        }
        assertThat(totals.get("ledger.transactions")).isPositive();
        assertThat(totals.get("ingest.rejected")).isZero();
    }
}
