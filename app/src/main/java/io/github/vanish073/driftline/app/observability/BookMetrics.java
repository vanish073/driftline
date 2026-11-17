package io.github.vanish073.driftline.app.observability;

import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.store.postgres.BookStats;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * The metrics of DESIGN §13 that describe the books rather than one process's work (D78): breaks detected, closed,
 * escalated and open; deadline headroom and compensation exposure; the outbox; settlement lines and pricing drift;
 * watermark lag. Read from PostgreSQL ({@link BookStats}) every {@code interval} by the resolver role, so they survive
 * restarts and count every process's work once. Counters are cumulative counts of append-only facts (breaks are never
 * deleted, transitions never change), so they only grow. Settlement lines in a status that can still change are a
 * gauge ({@code driftline_settlement_lines_unresolved}), so {@code driftline_settlement_lines_total} only grows.
 * A failed read is logged and the previous values stay.
 */
public final class BookMetrics implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(BookMetrics.class);

    /** Line statuses that may still change: re-solved, or attributed later (§6.3, §6.4). */
    static final Set<String> UNRESOLVED_LINE_STATUSES = Set.copyOf(List.of("AWAITING_BATCH", "UNATTRIBUTED",
            "AMBIGUOUS", "NO_SOLUTION", "BUDGET_EXCEEDED"));

    private final MeterRegistry registry;
    private final BookStats stats;
    private final Clock clock;
    private final Duration interval;
    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final MultiGauge open;
    private final MultiGauge headroom;
    private final MultiGauge pending;
    private final MultiGauge unresolvedLines;
    private final MultiGauge watermarkLag;
    private final AtomicLong compensation = new AtomicLong();
    private final AtomicReference<Double> oldestPending = new AtomicReference<>(0.0);

    private volatile boolean running;
    private volatile Thread thread;

    public BookMetrics(MeterRegistry registry, BookStats stats, Clock clock, Duration interval) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.stats = Objects.requireNonNull(stats, "stats");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.open = MultiGauge.builder("driftline.breaks.open").description("Open breaks (not final, not linked)")
                .register(registry);
        this.headroom = MultiGauge.builder("driftline.deadline.headroom.seconds")
                .description("Time left before the earliest deadline among open breaks; negative once missed")
                .register(registry);
        this.pending = MultiGauge.builder("driftline.outbox.pending").description("Outbox commands not done")
                .register(registry);
        this.unresolvedLines = MultiGauge.builder("driftline.settlement.lines.unresolved")
                .description("Settlement lines in a status that can still change").register(registry);
        this.watermarkLag = MultiGauge.builder("driftline.watermark.lag.seconds")
                .description("Processing time minus each stream's watermark").register(registry);
        Gauge.builder("driftline.compensation.exposure.rupees", compensation, AtomicLong::get)
                .description("What the open breaks would owe under the RBI rules if resolved now").register(registry);
        Gauge.builder("driftline.outbox.oldest.pending.seconds", oldestPending, AtomicReference::get)
                .description("Age of the oldest outbox command not done").register(registry);
    }

    private void count(String name, String description, Tags tags, long value) {
        String key = name + tags;
        counters.computeIfAbsent(key, k -> {
            AtomicLong v = new AtomicLong();
            FunctionCounter.builder(name, v, AtomicLong::get).description(description).tags(tags).register(registry);
            return v;
        }).set(value);
    }

    /** One read of the books into the meters. Public for tests. */
    public void refresh() {
        Instant now = clock.now();
        BookStats.Snapshot s = stats.snapshot(now);
        s.detected().forEach((type, n) -> count("driftline.breaks.detected", "Breaks raised", Tags.of("type", type), n));
        s.closed().forEach((type, byHow) -> byHow.forEach((how, n) -> count("driftline.breaks.closed",
                "Breaks closed", Tags.of("type", type, "how", how), n)));
        s.escalated().forEach((type, byReason) -> byReason.forEach((reason, n) -> count("driftline.breaks.escalated",
                "Escalations", Tags.of("type", type, "reason", reason), n)));
        List<MultiGauge.Row<?>> openRows = new ArrayList<>();
        s.open().forEach((type, byState) -> byState.forEach((state, n) ->
                openRows.add(MultiGauge.Row.of(Tags.of("type", type, "state", state), n))));
        open.register(openRows, true);
        List<MultiGauge.Row<?>> headroomRows = new ArrayList<>();
        s.earliestDeadline().forEach((type, deadline) -> headroomRows.add(MultiGauge.Row.of(Tags.of("type", type),
                Duration.between(now, deadline).toMillis() / 1000.0)));
        headroom.register(headroomRows, true);
        compensation.set(s.compensationRupees());
        List<MultiGauge.Row<?>> pendingRows = new ArrayList<>();
        s.outboxPending().forEach((kind, n) -> pendingRows.add(MultiGauge.Row.of(Tags.of("kind", kind), n)));
        pending.register(pendingRows, true);
        oldestPending.set(s.oldestPendingSince().map(t -> Math.max(0, Duration.between(t, now).toMillis()) / 1000.0)
                .orElse(0.0));
        List<MultiGauge.Row<?>> unresolved = new ArrayList<>();
        for (Map.Entry<String, Long> e : s.lines().entrySet()) {
            if (UNRESOLVED_LINE_STATUSES.contains(e.getKey())) {
                unresolved.add(MultiGauge.Row.of(Tags.of("status", e.getKey().toLowerCase(Locale.ROOT)), e.getValue()));
            } else {
                count("driftline.settlement.lines", "Settlement lines received, and in each final status",
                        Tags.of("result", e.getKey().toLowerCase(Locale.ROOT)), e.getValue());
            }
        }
        unresolvedLines.register(unresolved, true);
        count("driftline.pricing.drift", "Short lines explained by fees that differ from the schedule", Tags.empty(),
                s.pricingDrift());
        List<MultiGauge.Row<?>> lagRows = new ArrayList<>();
        for (BookStats.Watermark w : s.watermarks()) {
            lagRows.add(MultiGauge.Row.of(Tags.of("partition", Integer.toString(w.partition()), "source",
                    w.source().toLowerCase(Locale.ROOT)), Duration.between(w.value(), now).toMillis() / 1000.0));
        }
        watermarkLag.register(lagRows, true);
    }

    @Override
    public synchronized void start() {
        if (!running) {
            running = true;
            thread = Thread.ofPlatform().name("book-metrics").daemon().start(this::run);
        }
    }

    private void run() {
        while (running) {
            try {
                refresh();
            } catch (RuntimeException e) {
                LOG.warn("book metrics not refreshed: {}", e.toString());
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
