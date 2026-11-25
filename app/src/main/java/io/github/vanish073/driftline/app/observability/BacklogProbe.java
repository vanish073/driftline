package io.github.vanish073.driftline.app.observability;

import io.github.vanish073.driftline.core.port.Store;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * The matcher's backlog, {@code driftline_consumer_lag_records{partition}} (DESIGN §13; D83): for each partition this
 * instance owns, the topic's end offset minus the offset stored as processed in PostgreSQL ({@code partition_leases},
 * committed in the same transaction as the records' effects). Measured every {@code interval} on its own thread, so a
 * matcher that stalls inside a chunk shows a growing backlog instead of the value it last reported; the Kafka client's
 * own estimate, which only the driver loop updates, stays available as {@code driftline_consumer_client_lag_records}.
 * A partition no longer owned here drops out; another instance reports it. A failed read is logged and the previous
 * values stay.
 */
public final class BacklogProbe implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(BacklogProbe.class);

    /** The topic's end offsets (the offset the next record will get) for the given partitions. */
    @FunctionalInterface
    public interface EndOffsets {
        Map<Integer, Long> latest(Collection<Integer> partitions);
    }

    /** The offsets stored as processed (the next offset to process) for the given partitions. */
    @FunctionalInterface
    public interface StoredOffsets {
        Map<Integer, Long> next(Collection<Integer> partitions);
    }

    /** Stored offsets read from the Store in one transaction. */
    public static StoredOffsets storedOffsets(Store store) {
        Objects.requireNonNull(store, "store");
        return partitions -> store.transact("backlog-probe", tx -> {
            TreeMap<Integer, Long> out = new TreeMap<>();
            for (Integer p : partitions) {
                out.put(p, tx.partitions().nextOffset(p));
            }
            return out;
        });
    }

    private final Supplier<? extends Collection<Integer>> owned;
    private final EndOffsets ends;
    private final StoredOffsets stored;
    private final Duration interval;
    private final GaugeRows backlog;

    private volatile boolean running;
    private volatile Thread thread;

    public BacklogProbe(MeterRegistry registry, Supplier<? extends Collection<Integer>> owned, EndOffsets ends,
                        StoredOffsets stored, Duration interval) {
        Objects.requireNonNull(registry, "registry");
        this.owned = Objects.requireNonNull(owned, "owned");
        this.ends = Objects.requireNonNull(ends, "ends");
        this.stored = Objects.requireNonNull(stored, "stored");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.backlog = new GaugeRows(MultiGauge.builder("driftline.consumer.lag.records")
                .description("Records in each owned partition not yet processed: end offset minus the stored offset")
                .register(registry));
    }

    /**
     * The backlog of each of {@code owned} that both sides report: end offset minus stored offset, never below 0 (the
     * stored offset is read first, so a commit in between can only make the end offset look older). Public for tests.
     */
    public static SortedMap<Integer, Long> measure(Collection<Integer> owned, EndOffsets ends, StoredOffsets stored) {
        List<Integer> partitions = List.copyOf(new TreeSet<>(owned));
        TreeMap<Integer, Long> lag = new TreeMap<>();
        if (partitions.isEmpty()) {
            return lag;
        }
        Map<Integer, Long> next = stored.next(partitions);
        Map<Integer, Long> end = ends.latest(partitions);
        for (Integer p : partitions) {
            Long n = next.get(p);
            Long e = end.get(p);
            if (n != null && e != null) {
                lag.put(p, Math.max(0L, Math.subtractExact(e, n)));
            }
        }
        return lag;
    }

    /** One measurement into the gauge; returns it by partition. Public for tests. */
    public SortedMap<Integer, Long> refresh() {
        SortedMap<Integer, Long> lag = measure(owned.get(), ends, stored);
        LinkedHashMap<Tags, Long> rows = new LinkedHashMap<>();
        lag.forEach((p, n) -> rows.put(Tags.of("partition", Integer.toString(p)), n));
        backlog.set(rows);
        return lag;
    }

    @Override
    public synchronized void start() {
        if (!running) {
            running = true;
            thread = Thread.ofPlatform().name("backlog-probe").daemon().start(this::run);
        }
    }

    private void run() {
        while (running) {
            try {
                refresh();
            } catch (RuntimeException e) {
                LOG.warn("matcher backlog not measured: {}", e.toString());
            }
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    @Override
    public void stop() {
        running = false;
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
