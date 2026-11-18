package io.github.vanish073.driftline.app.observability;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Manual spans (DESIGN §13, D78) on the OpenTelemetry API. In production the Java agent supplies the implementation
 * (exporting over OTLP); without it the API is a no-op, {@link #enabled()} is false, and nothing traced costs anything,
 * including the trace-context reads and writes the drivers make only when tracing is on. A trace crosses Kafka in the
 * {@code traceparent} header and the envelope (D46), and crosses from the matcher to the resolver to the dispatcher
 * through the trace contexts stored per break and per command (V11).
 */
public final class Tracing {

    /** At most this many records are linked from one {@code matcher.batch} span. */
    public static final int MAX_LINKS = 128;

    /** The carrier is the {@code traceparent} value itself; nothing else is read from it. */
    private static final TextMapGetter<String> TRACEPARENT =
            (carrier, key) -> "traceparent".equals(key) ? carrier : null;

    private final Tracer tracer;
    private final boolean enabled;

    public Tracing(OpenTelemetry openTelemetry) {
        this.tracer = openTelemetry.getTracer("io.github.vanish073.driftline");
        Span probe = tracer.spanBuilder("driftline.probe").setNoParent().startSpan();
        this.enabled = probe.getSpanContext().isValid();
        // the probe is never ended, so it is never exported
    }

    /** False with the no-op API (no agent): then no span is started and no trace context is read or stored. */
    public boolean enabled() {
        return enabled;
    }

    /** The W3C {@code traceparent} of a span context (version 00). */
    public static String traceparent(SpanContext c) {
        return "00-" + c.getTraceId() + "-" + c.getSpanId() + "-" + c.getTraceFlags().asHex();
    }

    /** A remote span context from a {@code traceparent}, if it is a valid one. */
    public static Optional<SpanContext> parse(String traceparent) {
        if (traceparent == null) {
            return Optional.empty();
        }
        Context extracted = W3CTraceContextPropagator.getInstance().extract(Context.root(), traceparent, TRACEPARENT);
        SpanContext c = Span.fromContext(extracted).getSpanContext();
        return c.isValid() ? Optional.of(c) : Optional.empty();
    }

    /**
     * Starts a span at {@code start}; with a parent, as its child, else as a new trace. The caller ends it.
     *
     * @param links span contexts to link (a batch's records); at most {@link #MAX_LINKS} are kept
     */
    public Span start(String name, Optional<SpanContext> parent, Instant start, Attributes attributes,
                      List<SpanContext> links) {
        Objects.requireNonNull(parent, "parent");
        SpanBuilder b = tracer.spanBuilder(name).setStartTimestamp(start.toEpochMilli(), TimeUnit.MILLISECONDS)
                .setAllAttributes(attributes);
        if (parent.isPresent()) {
            b.setParent(Context.root().with(Span.wrap(parent.get())));
        } else {
            b.setNoParent();
        }
        links.stream().limit(MAX_LINKS).forEach(b::addLink);
        return b.startSpan();
    }

    /**
     * A span for an incoming request, started now: a child of the current span (the agent's HTTP server span, which
     * already continues the caller's {@code traceparent}), or else of the caller's {@code traceparent} header, or else
     * a new trace.
     */
    public Span startForRequest(String name, Optional<String> traceparentHeader, Attributes attributes) {
        SpanBuilder b = tracer.spanBuilder(name).setAllAttributes(attributes);
        if (Span.current().getSpanContext().isValid()) {
            b.setParent(Context.current());
        } else {
            Optional<SpanContext> caller = traceparentHeader.flatMap(Tracing::parse);
            if (caller.isPresent()) {
                b.setParent(Context.root().with(Span.wrap(caller.get())));
            } else {
                b.setNoParent();
            }
        }
        return b.startSpan();
    }

    public static void end(Span span, Instant at) {
        span.end(at.toEpochMilli(), TimeUnit.MILLISECONDS);
    }

    public static AttributeKey<String> key(String name) {
        return AttributeKey.stringKey(name);
    }
}
