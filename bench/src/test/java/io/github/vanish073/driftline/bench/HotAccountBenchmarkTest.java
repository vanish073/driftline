package io.github.vanish073.driftline.bench;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HotAccountBenchmarkTest {

    @Test
    void percentilesAreNearestRankInMilliseconds() {
        long[] nanos = new long[100];
        for (int i = 0; i < 100; i++) {
            nanos[i] = (i + 1) * 1_000_000L;   // 1 ms .. 100 ms, sorted
        }
        assertThat(HotAccountBenchmark.millis(nanos, 0.50)).isEqualTo(50.0);
        assertThat(HotAccountBenchmark.millis(nanos, 0.99)).isEqualTo(99.0);
        assertThat(HotAccountBenchmark.millis(new long[] {7_000_000L}, 0.99)).isEqualTo(7.0);
        assertThat(HotAccountBenchmark.millis(new long[0], 0.5)).isNaN();
    }
}
