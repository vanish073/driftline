package io.github.vanish073.driftline.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.sim.fault.Fault;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * A slice of the Shift 4 campaign inside {@code ./gradlew check}: 200 {@code pr} seeds pass every invariant, the
 * first 50 give the same digest when run again, and every fault of the swarm really fires somewhere in the slice
 * (a fault that never fires would make the 10,000-seed job prove nothing about it). The full 10,000 seeds run in
 * CI's {@code simulation} job.
 */
class PrSampleTest {

    private static final int SEEDS = 200;
    private static final int DOUBLE_RUNS = 50;

    /** For each fault, the counters that only move when the fault actually struck. */
    private static EnumMap<Fault, List<String>> evidence() {
        EnumMap<Fault, List<String>> e = new EnumMap<>(Fault.class);
        e.put(Fault.NET_DROP, List.of("fault.net.dropped"));
        e.put(Fault.NET_DUP, List.of("fault.net.duplicated"));
        e.put(Fault.NET_DELAY, List.of("fault.net.delayed"));
        e.put(Fault.NODE_CRASH, List.of("fault.matcher.crashes", "fault.matcher.crashes.mid_batch", "fault.ingest.crashes"));
        e.put(Fault.NODE_PAUSE, List.of("fault.matcher.pauses", "fault.matcher.fenced", "coordinator.reassignments"));
        e.put(Fault.CLOCK_JUMP, List.of("fault.clock.jumps"));
        e.put(Fault.STORE_DOWN, List.of("fault.store.down.windows", "fault.store.retries"));
        e.put(Fault.PRODUCE_AMBIGUOUS, List.of("fault.ingest.ambiguous"));
        e.put(Fault.MERCHANT_KEY_REUSE, List.of("fault.merchant.keys.reused", "quarantined"));
        return e;
    }

    @Test
    void twoHundredPrSeedsPassWithEveryFaultFiring() {
        List<String> failures = new ArrayList<>();
        TreeMap<String, Long> totals = new TreeMap<>();
        int skewed = 0;
        for (long seed = 1; seed <= SEEDS; seed++) {
            RunResult first = Simulation.run(seed, Profile.PR, false);
            if (!first.passed()) {
                failures.add(first.violation().orElseThrow() + "  REPRO: " + first.repro());
            }
            if (seed <= DOUBLE_RUNS) {
                RunResult second = Simulation.run(seed, Profile.PR, false);
                if (!first.digest().equals(second.digest())) {
                    failures.add("seed " + seed + " is not deterministic: " + first.digest() + " vs " + second.digest());
                }
            }
            first.stats().forEach((k, v) -> totals.merge(k, v, Long::sum));
            skewed += first.swarm().contains(Fault.CLOCK_SKEW.name()) ? 1 : 0;
        }
        assertThat(failures).isEmpty();
        EnumMap<Fault, List<String>> evidence = evidence();
        evidence.forEach((fault, counters) -> {
            for (String counter : counters) {
                assertThat(totals.getOrDefault(counter, 0L)).as(fault + " evidence " + counter).isPositive();
            }
        });
        assertThat(skewed).as("seeds with CLOCK_SKEW").isPositive();
        assertThat(evidence.size() + 1).as("every fault is covered").isEqualTo(Fault.values().length);
    }
}
