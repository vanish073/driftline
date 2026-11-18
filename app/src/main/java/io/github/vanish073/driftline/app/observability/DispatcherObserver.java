package io.github.vanish073.driftline.app.observability;

import io.github.vanish073.driftline.app.dispatcher.DispatcherDriver;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.OutboundCall;
import io.github.vanish073.driftline.store.postgres.TraceContexts;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Scope;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The dispatcher's metrics and spans (DESIGN §13, D78). {@code driftline_outbox_attempts_total{kind,result}} counts how
 * each attempt ended. A call whose command has a stored trace context is sent inside a {@code dispatcher.call} span,
 * a child of the resolver's, and current while the call is handed to the HTTP client, so the agent's client span
 * (and the {@code traceparent} it sends) joins the same trace. Never throws into the driver; the call is always sent.
 */
public final class DispatcherObserver implements DispatcherDriver.Listener {

    private static final Logger LOG = LoggerFactory.getLogger(DispatcherObserver.class);

    private final MeterRegistry registry;
    private final Tracing tracing;
    private final TraceContexts traces;

    public DispatcherObserver(MeterRegistry registry, Tracing tracing, TraceContexts traces) {
        this.registry = registry;
        this.tracing = tracing;
        this.traces = traces;
    }

    /** The outbox command kind a call serves (§9.1), for the {@code kind} label. */
    static String kind(OutboundCall call) {
        return switch (call.request()) {
            case CallRequest.StatusQuery q -> "STATUS_QUERY_PAYMENT";
            case CallRequest.StatusQueryOrder q -> "STATUS_QUERY_ORDER";
            case CallRequest.PaymentSweep q -> "PAYMENT_SWEEP";
            case CallRequest.Refund r -> "REFUND";
            case CallRequest.RefundLookup r -> "REFUND_LOOKUP";
            case CallRequest.MerchantConfirm c -> "MERCHANT_CONFIRM";
            case CallRequest.Observation o -> "PUBLISH_OBSERVATION";
            case CallRequest.EscalationNotify n -> "ESCALATION_NOTIFY";
        };
    }

    /** The command a call belongs to: its id up to the first dot ({@code commandId.attempt[.lookup]}). */
    static String commandId(OutboundCall call) {
        int dot = call.callId().indexOf('.');
        return dot < 0 ? call.callId() : call.callId().substring(0, dot);
    }

    @Override
    public void attempt(OutboundCall call, String result) {
        try {
            registry.counter("driftline.outbox.attempts", "kind", kind(call), "result", result).increment();
        } catch (RuntimeException e) {
            LOG.warn("dispatcher metrics skipped: {}", e.toString());
        }
    }

    @Override
    public void send(OutboundCall call, Runnable send) {
        Optional<Span> span = Optional.empty();
        if (tracing.enabled()) {
            try {
                String command = commandId(call);
                Optional<SpanContext> parent = Optional.ofNullable(traces.find(List.of(command)).get(command))
                        .flatMap(Tracing::parse);
                span = parent.map(p -> tracing.start("dispatcher.call", Optional.of(p), Instant.now(),
                        Attributes.builder().put("driftline.call_id", call.callId())
                                .put("driftline.command_kind", kind(call))
                                .put("driftline.target", call.target().name()).build(), List.of()));
            } catch (RuntimeException e) {
                LOG.warn("dispatcher trace skipped for {}: {}", call.callId(), e.toString());
            }
        }
        if (span.isEmpty()) {
            send.run();
            return;
        }
        Scope scope = span.get().makeCurrent();
        try {
            send.run();
        } finally {
            scope.close();
            span.get().end();
        }
    }
}
