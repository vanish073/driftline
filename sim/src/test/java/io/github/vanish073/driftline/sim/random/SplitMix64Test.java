package io.github.vanish073.driftline.sim.random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class SplitMix64Test {

    /** Reference values from an independent Python implementation (2026-11-13); seed 0 matches the published ones. */
    @Test
    void matchesTheReferenceOutputs() {
        SplitMix64 zero = new SplitMix64(0);
        assertThat(zero.nextLong()).isEqualTo(0xe220a8397b1dcdafL);
        assertThat(zero.nextLong()).isEqualTo(0x6e789e6aa1b965f4L);
        assertThat(zero.nextLong()).isEqualTo(0x06c45d188009454fL);
        SplitMix64 fortyTwo = new SplitMix64(42);
        assertThat(List.of(fortyTwo.nextLong(), fortyTwo.nextLong(), fortyTwo.nextLong()))
                .containsExactly(-4767286540954276203L, 2949826092126892291L, 5139283748462763858L);
    }

    @Test
    void streamsAreDerivedFromSeedAndLabel() {
        SplitMix64 workload = SplitMix64.stream(42, "workload");
        assertThat(workload.nextLong()).isEqualTo(207912568947532439L);
        assertThat(workload.nextLong()).isEqualTo(5469215545810665914L);
        assertThat(SplitMix64.stream(42, "network").nextLong()).isNotEqualTo(SplitMix64.stream(42, "workload").nextLong());
        assertThat(SplitMix64.stream(42, "faults").nextLong()).isEqualTo(SplitMix64.stream(42, "faults").nextLong());
    }

    @Test
    void boundedValuesStayInRangeAndCoverIt() {
        SplitMix64 rng = new SplitMix64(7);
        boolean[] seen = new boolean[7];
        for (int i = 0; i < 10_000; i++) {
            long v = rng.nextLong(7);
            assertThat(v).isBetween(0L, 6L);
            seen[(int) v] = true;
            assertThat(rng.between(-3L, 3L)).isBetween(-3L, 3L);
        }
        for (boolean b : seen) {
            assertThat(b).isTrue();
        }
        assertThat(rng.between(Long.MIN_VALUE + 1, Long.MIN_VALUE + 1)).isEqualTo(Long.MIN_VALUE + 1);
        assertThatThrownBy(() -> rng.nextLong(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> rng.chance(3, 2)).isInstanceOf(IllegalArgumentException.class);
        assertThat(rng.chance(0, 5)).isFalse();
        assertThat(rng.chance(5, 5)).isTrue();
    }
}
