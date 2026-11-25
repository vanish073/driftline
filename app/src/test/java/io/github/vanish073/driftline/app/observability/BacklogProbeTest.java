package io.github.vanish073.driftline.app.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class BacklogProbeTest {

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    final AtomicReference<List<Integer>> owned = new AtomicReference<>(List.of(0, 1));
    final TreeMap<Integer, Long> ends = new TreeMap<>(Map.of(0, 120L, 1, 50L, 2, 999L));
    final TreeMap<Integer, Long> stored = new TreeMap<>(Map.of(0, 100L, 1, 50L, 2, 0L));
    final AtomicReference<RuntimeException> endsFail = new AtomicReference<>();

    final BacklogProbe probe = new BacklogProbe(registry, owned::get, partitions -> {
        RuntimeException e = endsFail.get();
        if (e != null) {
            throw e;
        }
        TreeMap<Integer, Long> out = new TreeMap<>(ends);
        out.keySet().retainAll(partitions);
        return out;
    }, partitions -> {
        TreeMap<Integer, Long> out = new TreeMap<>(stored);
        out.keySet().retainAll(partitions);
        return out;
    }, Duration.ofSeconds(5));

    double gauge(int partition) {
        Gauge g = registry.find("driftline.consumer.lag.records").tag("partition", Integer.toString(partition))
                .gauge();
        return g == null ? Double.NaN : g.value();
    }

    @Test
    void theBacklogOfEachOwnedPartitionIsItsEndOffsetMinusTheStoredOffset() {
        assertThat(probe.refresh()).containsExactlyEntriesOf(new TreeMap<>(Map.of(0, 20L, 1, 0L)));
        assertThat(gauge(0)).isEqualTo(20);
        assertThat(gauge(1)).isEqualTo(0);
        assertThat(gauge(2)).as("not owned here: another instance reports it").isNaN();
    }

    @Test
    void theBacklogGrowsWhileNothingIsProcessedAndAPartitionLostDropsOut() {
        probe.refresh();
        ends.put(0, 400L);   // records keep arriving, the stored offset stays: a stalled matcher
        assertThat(probe.refresh()).containsEntry(0, 300L);
        assertThat(gauge(0)).isEqualTo(300);

        owned.set(List.of(1));
        assertThat(probe.refresh()).containsOnlyKeys(1);
        assertThat(gauge(0)).as("a partition no longer owned drops out").isNaN();
        assertThat(gauge(1)).isEqualTo(0);
    }

    @Test
    void aStoredOffsetPastTheEndOffsetCountsAsNoBacklog() {
        stored.put(1, 60L);   // the end offset was read a moment earlier than the stored one moved
        assertThat(probe.refresh()).containsEntry(1, 0L);
    }

    @Test
    void aFailedReadLeavesThePreviousValues() {
        probe.refresh();
        endsFail.set(new IllegalStateException("cannot list the end offsets"));
        assertThatThrownBy(probe::refresh).isInstanceOf(IllegalStateException.class);
        assertThat(gauge(0)).isEqualTo(20);
    }

    @Test
    void noOwnedPartitionReportsNothingAndReadsNothing() {
        owned.set(List.of());
        endsFail.set(new IllegalStateException("must not be called"));
        assertThat(probe.refresh()).isEmpty();
        assertThat(registry.find("driftline.consumer.lag.records").gauges()).isEmpty();
    }

    @Test
    void theThreadStartsAndStops() {
        probe.start();
        assertThat(probe.isRunning()).isTrue();
        probe.stop();
        assertThat(probe.isRunning()).isFalse();
    }
}
