package io.github.vanish073.driftline.app.observability;

import io.github.vanish073.driftline.app.kafka.PartitionDriver;
import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.matcher.BatchOutcome;
import io.github.vanish073.driftline.store.postgres.TraceContexts;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The matcher's metrics and spans (DESIGN §13, D78), from what the partition driver reports after each commit.
 *
 * <ul>
 *   <li><b>Events</b>: consumed, duplicate and late, by source, from each record's fate (a record already processed,
 *       replayed after a crash, is not counted again).</li>
 *   <li><b>Latency</b>: event-to-match, from the record's ingest {@code receivedAt} to the commit that applied it;
 *       detection lag by break type, from the arrival of the record that raised the break (or, for a break raised by
 *       a timer, of the newest record in that transaction) to its commit. A break a tick raises with no record is not
 *       measured.</li>
 *   <li><b>Progress</b>: the Kafka client's own lag estimate by partition (the backlog itself is
 *       {@link BacklogProbe}'s, D83), fenced commits; subset-sum search costs.</li>
 *   <li><b>Spans</b>: {@code matcher.batch} for each chunk, linked to its records' traces; {@code matcher.detect}
 *       as a child of the trace of the record that raised a break. Each new break's trace context is stored, so the
 *       resolver continues that trace.</li>
 * </ul>
 * Nothing here may throw into the driver: a failure is logged and the chunk's telemetry is skipped.
 */
public final class MatcherObserver implements PartitionDriver.Listener {

    private static final Logger LOG = LoggerFactory.getLogger(MatcherObserver.class);

    private final MeterRegistry registry;
    private final Tracing tracing;
    private final TraceContexts traces;
    private final Timer eventToMatch;
    private final DistributionSummary subsetSumOps;
    private final Counter fenced;
    private final ConcurrentHashMap<Integer, AtomicLong> lag = new ConcurrentHashMap<>();

    public MatcherObserver(MeterRegistry registry, Tracing tracing, TraceContexts traces) {
        this.registry = registry;
        this.tracing = tracing;
        this.traces = traces;
        this.eventToMatch = Timer.builder("driftline.event.to.match")
                .description("From ingest receivedAt to the matcher commit that applied the event")
                .publishPercentileHistogram().minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofHours(1)).register(registry);
        this.subsetSumOps = DistributionSummary.builder("driftline.subset.sum.ops")
                .description("Cost in operations of each subset-sum search run").publishPercentileHistogram()
                .minimumExpectedValue(1.0).maximumExpectedValue(1e9).register(registry);
        this.fenced = Counter.builder("driftline.partition.fenced").description("Commits refused under a stale epoch")
                .register(registry);
    }

    @Override
    public void fenced(int partition) {
        fenced.increment();
    }

    @Override
    public void lag(int partition, long records) {
        lag.computeIfAbsent(partition, p -> {
            AtomicLong value = new AtomicLong();
            // only the driver loop updates this, so it freezes while the loop is stuck; BacklogProbe does not (D83)
            registry.gauge("driftline.consumer.client.lag.records",
                    List.of(Tag.of("partition", Integer.toString(p))), value);
            return value;
        }).set(records);
    }

    @Override
    public void chunk(int partition, SortedMap<Long, EventEnvelope> records, List<BatchOutcome> outcomes,
                      Instant began, Instant committed) {
        try {
            count(records, outcomes, committed);
            if (tracing.enabled()) {
                trace(partition, records, outcomes, began, committed);
            }
        } catch (RuntimeException e) {
            LOG.warn("p{}: telemetry for a committed chunk skipped: {}", partition, e.toString());
        }
    }

    private static String source(EventEnvelope e) {
        return e.source().name().toLowerCase(Locale.ROOT);
    }

    private void count(SortedMap<Long, EventEnvelope> records, List<BatchOutcome> outcomes, Instant committed) {
        Optional<Instant> newest = records.values().stream().map(EventEnvelope::receivedAt).max(Instant::compareTo);
        for (BatchOutcome o : outcomes) {
            for (Map.Entry<Long, BatchOutcome.RecordFate> f : o.fates().entrySet()) {
                EventEnvelope e = records.get(f.getKey());
                if (e == null) {
                    continue;
                }
                registry.counter("driftline.events.consumed", "source", source(e)).increment();
                switch (f.getValue()) {
                    case DUPLICATE -> registry.counter("driftline.events.duplicate", "source", source(e)).increment();
                    case APPLIED_LATE -> {
                        registry.counter("driftline.events.late", "source", source(e)).increment();
                        eventToMatch.record(nonNegative(Duration.between(e.receivedAt(), committed)));
                    }
                    case APPLIED -> eventToMatch.record(nonNegative(Duration.between(e.receivedAt(), committed)));
                    case KEY_REUSED -> { }
                }
            }
            for (Break b : o.raised()) {
                Optional<Instant> arrived = Optional.ofNullable(o.raisedBy().get(b.breakId()))
                        .map(records::get).map(EventEnvelope::receivedAt).or(() -> newest);
                arrived.ifPresent(at -> Timer.builder("driftline.detection.lag")
                        .description("From the arrival of the evidence that completed a break to its commit")
                        .tag("type", b.type().name()).publishPercentileHistogram()
                        .minimumExpectedValue(Duration.ofMillis(1)).maximumExpectedValue(Duration.ofDays(7))
                        .register(registry).record(nonNegative(Duration.between(at, committed))));
            }
            o.solveOps().forEach(subsetSumOps::record);
        }
    }

    private static Duration nonNegative(Duration d) {
        return d.isNegative() ? Duration.ZERO : d;
    }

    private void trace(int partition, SortedMap<Long, EventEnvelope> records, List<BatchOutcome> outcomes,
                       Instant began, Instant committed) {
        List<Break> raised = outcomes.stream().flatMap(o -> o.raised().stream()).toList();
        if (records.isEmpty() && raised.isEmpty()) {
            return;   // an idle tick: no span
        }
        List<SpanContext> links = new ArrayList<>();
        for (EventEnvelope e : records.values()) {
            e.traceparent().flatMap(Tracing::parse).ifPresent(links::add);
        }
        Span batch = tracing.start("matcher.batch", Optional.empty(), began, Attributes.builder()
                .put("driftline.partition", partition).put("driftline.records", records.size())
                .put("driftline.breaks_raised", raised.size()).build(), links);
        TreeMap<String, String> contexts = new TreeMap<>();
        for (BatchOutcome o : outcomes) {
            for (Break b : o.raised()) {
                Optional<SpanContext> cause = Optional.ofNullable(o.raisedBy().get(b.breakId())).map(records::get)
                        .flatMap(EventEnvelope::traceparent).flatMap(Tracing::parse);
                if (cause.isPresent()) {
                    Span detect = tracing.start("matcher.detect", cause, began, Attributes.builder()
                            .put("driftline.break_id", b.breakId()).put("driftline.break_type", b.type().name())
                            .put("driftline.partition", partition).build(), List.of(batch.getSpanContext()));
                    contexts.put(b.breakId(), Tracing.traceparent(detect.getSpanContext()));
                    Tracing.end(detect, committed);
                } else {
                    contexts.put(b.breakId(), Tracing.traceparent(batch.getSpanContext()));
                }
            }
        }
        Tracing.end(batch, committed);
        if (!contexts.isEmpty()) {
            traces.save(contexts, committed);
        }
    }
}
