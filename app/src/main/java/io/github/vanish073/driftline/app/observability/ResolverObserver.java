package io.github.vanish073.driftline.app.observability;

import io.github.vanish073.driftline.app.resolver.ResolverDriver;
import io.github.vanish073.driftline.core.resolver.Resolver;
import io.github.vanish073.driftline.store.postgres.TraceContexts;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The resolver's spans (DESIGN §13, D78): a {@code resolver.decide} span for each reviewed break that has a stored trace
 * context, as a child of it, and the context of each command that review enqueued, so the dispatcher continues the
 * trace. A break raised while tracing was off has no context and gets no span. Never throws into the driver.
 */
public final class ResolverObserver implements ResolverDriver.Listener {

    private static final Logger LOG = LoggerFactory.getLogger(ResolverObserver.class);

    private final Tracing tracing;
    private final TraceContexts traces;

    public ResolverObserver(Tracing tracing, TraceContexts traces) {
        this.tracing = tracing;
        this.traces = traces;
    }

    @Override
    public void decided(Resolver.Outcome outcome, Instant began, Instant committed) {
        if (!tracing.enabled() || outcome.reviewed().isEmpty()) {
            return;
        }
        try {
            SortedMap<String, String> parents = traces.find(
                    outcome.reviewed().stream().map(Resolver.Decision::breakId).toList());
            TreeMap<String, String> contexts = new TreeMap<>();
            for (Resolver.Decision d : outcome.reviewed()) {
                Optional<SpanContext> parent = Optional.ofNullable(parents.get(d.breakId())).flatMap(Tracing::parse);
                if (parent.isEmpty()) {
                    continue;
                }
                Span span = tracing.start("resolver.decide", parent, began, Attributes.builder()
                        .put("driftline.break_id", d.breakId()).put("driftline.break_state", d.state().name())
                        .put("driftline.commands", d.enqueued().size()).build(), List.of());
                String traceparent = Tracing.traceparent(span.getSpanContext());
                d.enqueued().forEach(c -> contexts.put(c, traceparent));
                Tracing.end(span, committed);
            }
            traces.save(contexts, committed);
        } catch (RuntimeException e) {
            LOG.warn("resolver telemetry skipped: {}", e.toString());
        }
    }
}
