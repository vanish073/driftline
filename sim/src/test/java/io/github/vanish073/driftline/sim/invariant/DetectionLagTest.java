package io.github.vanish073.driftline.sim.invariant;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Stream breaks are counted by how long after the world's capture they were raised (R2). */
class DetectionLagTest {

    @Test
    void aLagFallsInTheFirstBucketItIsWithinAndTheBoundIsInclusive() {
        assertThat(Invariants.lagBucket(0L)).isEqualTo("le_1m");
        assertThat(Invariants.lagBucket(60_000L)).isEqualTo("le_1m");
        assertThat(Invariants.lagBucket(60_001L)).isEqualTo("le_5m");
        assertThat(Invariants.lagBucket(12 * 60_000L)).isEqualTo("le_15m");
        assertThat(Invariants.lagBucket(24 * 3_600_000L)).isEqualTo("le_1440m");
        assertThat(Invariants.lagBucket(24 * 3_600_000L + 1)).isEqualTo("gt_1d");
    }

    @Test
    void theBucketsRiseAndEndAtOneDay() {
        assertThat(Invariants.LAG_BUCKETS).isSorted().doesNotHaveDuplicates().last().isEqualTo(1440L);
    }
}
