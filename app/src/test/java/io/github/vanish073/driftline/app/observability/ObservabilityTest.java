package io.github.vanish073.driftline.app.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventPayload;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.matcher.BatchOutcome;
import io.github.vanish073.driftline.core.matcher.KeyedState;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.core.port.OutboundCall;
import io.github.vanish073.driftline.core.resolver.Resolver;
import io.github.vanish073.driftline.store.postgres.BookStats;
import io.github.vanish073.driftline.store.postgres.TraceContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/** The metrics and spans of DESIGN §13 (D78), from what the drivers report, with spans captured in memory. */
class ObservabilityTest {

    static final MerchantId M = MerchantId.of("m_1");
    static final Instant RECEIVED = Instant.parse("2026-11-17T06:00:00Z");
    static final Instant BEGAN = RECEIVED.plusMillis(400);
    static final Instant COMMITTED = RECEIVED.plusMillis(500);
    static final String INGEST_TRACE = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    SimpleMeterRegistry registry;
    InMemorySpanExporter spans;
    Tracing tracing;
    TraceContexts traces;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        spans = InMemorySpanExporter.create();
        OpenTelemetry sdk = OpenTelemetrySdk.builder().setTracerProvider(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spans)).build()).build();
        tracing = new Tracing(sdk);
        traces = mock(TraceContexts.class);
    }

    static EventEnvelope envelope(String key, Source source, Optional<String> traceparent) {
        boolean gateway = source == Source.GATEWAY;
        EventPayload payload = gateway
                ? new PaymentPayload(PaymentId.of("pay_1"), OrderId.of("ord_1"), Money.ofPaise(100))
                : new OrderPayload(OrderId.of("ord_1"), 2, Money.ofPaise(100), RECEIVED.plusSeconds(900),
                        Optional.of(PaymentId.of("pay_1")));
        return new EventEnvelope(EventEnvelope.SCHEMA_VERSION, M, 0, source, key,
                gateway ? EventType.PAYMENT_SUCCEEDED : EventType.ORDER_PAID, RECEIVED.minusSeconds(5), RECEIVED,
                Sha256.of(key.getBytes(StandardCharsets.UTF_8)), payload, traceparent);
    }

    static BatchOutcome outcome(SortedMap<Long, BatchOutcome.RecordFate> fates, List<Break> raised,
                                SortedMap<String, Long> raisedBy, List<Long> solveOps) {
        return new BatchOutcome(0, 0, 0, 0, 0, 0, List.of(), raised, 0, 0, Optional.empty(),
                mock(KeyedState.Delta.class), fates, raisedBy, solveOps);
    }

    @Test
    void theMatcherCountsEachRecordOnceBySourceAndTimesEventToMatchAndDetection() {
        Break lateSuccess = Break.detected(M, BreakType.LATE_SUCCESS, "pay_1", COMMITTED, Map.of());
        Break missing = Break.detected(M, BreakType.MISSING_SETTLEMENT, "pay_2", COMMITTED, Map.of());
        TreeMap<Long, EventEnvelope> records = new TreeMap<>(Map.of(
                10L, envelope("e10", Source.GATEWAY, Optional.of(INGEST_TRACE)),
                11L, envelope("e11", Source.GATEWAY, Optional.empty()),
                12L, envelope("e12", Source.MERCHANT, Optional.empty())));
        BatchOutcome o = outcome(new TreeMap<>(Map.of(10L, BatchOutcome.RecordFate.APPLIED,
                        11L, BatchOutcome.RecordFate.DUPLICATE, 12L, BatchOutcome.RecordFate.APPLIED_LATE)),
                List.of(lateSuccess, missing), new TreeMap<>(Map.of(lateSuccess.breakId(), 10L)), List.of(1_200L));
        MatcherObserver observer = new MatcherObserver(registry, tracing, traces);
        observer.chunk(0, records, List.of(o), BEGAN, COMMITTED);
        observer.lag(0, 42);
        observer.fenced(0);

        assertThat(registry.get("driftline.events.consumed").tag("source", "gateway").counter().count()).isEqualTo(2);
        assertThat(registry.get("driftline.events.consumed").tag("source", "merchant").counter().count()).isEqualTo(1);
        assertThat(registry.get("driftline.events.duplicate").tag("source", "gateway").counter().count()).isEqualTo(1);
        assertThat(registry.get("driftline.events.late").tag("source", "merchant").counter().count()).isEqualTo(1);
        assertThat(registry.get("driftline.event.to.match").timer().count()).as("applied records only").isEqualTo(2);
        assertThat(registry.get("driftline.event.to.match").timer().max(TimeUnit.MILLISECONDS))
                .isEqualTo(500);
        assertThat(registry.get("driftline.detection.lag").tag("type", "LATE_SUCCESS").timer().count()).isEqualTo(1);
        assertThat(registry.get("driftline.detection.lag").tag("type", "MISSING_SETTLEMENT").timer().count())
                .as("a timer-raised break is measured from the newest record").isEqualTo(1);
        assertThat(registry.get("driftline.subset.sum.ops").summary().totalAmount()).isEqualTo(1_200);
        assertThat(registry.get("driftline.consumer.client.lag.records").tag("partition", "0").gauge().value())
                .isEqualTo(42);
        assertThat(registry.get("driftline.partition.fenced").counter().count()).isEqualTo(1);
    }

    @SuppressWarnings("unchecked")
    @Test
    void aBreakRaisedByATracedRecordContinuesThatTraceAndATimerBreakTheBatchs() {
        Break lateSuccess = Break.detected(M, BreakType.LATE_SUCCESS, "pay_1", COMMITTED, Map.of());
        Break missing = Break.detected(M, BreakType.MISSING_SETTLEMENT, "pay_2", COMMITTED, Map.of());
        TreeMap<Long, EventEnvelope> records = new TreeMap<>(Map.of(10L,
                envelope("e10", Source.GATEWAY, Optional.of(INGEST_TRACE))));
        BatchOutcome o = outcome(new TreeMap<>(Map.of(10L, BatchOutcome.RecordFate.APPLIED)),
                List.of(lateSuccess, missing), new TreeMap<>(Map.of(lateSuccess.breakId(), 10L)), List.of());
        new MatcherObserver(registry, tracing, traces).chunk(0, records, List.of(o), BEGAN, COMMITTED);

        SpanData batch = span("matcher.batch");
        SpanData detect = span("matcher.detect");
        assertThat(batch.getLinks()).singleElement().satisfies(l ->
                assertThat(l.getSpanContext().getTraceId()).isEqualTo("0af7651916cd43dd8448eb211c80319c"));
        assertThat(detect.getTraceId()).as("the ingest trace continues").isEqualTo("0af7651916cd43dd8448eb211c80319c");
        assertThat(detect.getParentSpanId()).isEqualTo("b7ad6b7169203331");
        ArgumentCaptor<SortedMap<String, String>> saved = ArgumentCaptor.forClass(SortedMap.class);
        verify(traces).save(saved.capture(), any());
        assertThat(saved.getValue()).containsEntry(lateSuccess.breakId(), Tracing.traceparent(detect.getSpanContext()))
                .containsEntry(missing.breakId(), Tracing.traceparent(batch.getSpanContext()));
    }

    @Test
    void anIdleTickMakesNoSpanAndTracingOffTouchesNoTraceContext() {
        new MatcherObserver(registry, tracing, traces).chunk(0, new TreeMap<>(),
                List.of(outcome(new TreeMap<>(), List.of(), new TreeMap<>(), List.of())), BEGAN, COMMITTED);
        assertThat(spans.getFinishedSpanItems()).isEmpty();

        Tracing off = new Tracing(OpenTelemetry.noop());
        assertThat(off.enabled()).isFalse();
        Break b = Break.detected(M, BreakType.LATE_SUCCESS, "pay_1", COMMITTED, Map.of());
        new MatcherObserver(registry, off, traces).chunk(0, new TreeMap<>(Map.of(10L,
                envelope("e10", Source.GATEWAY, Optional.of(INGEST_TRACE)))), List.of(outcome(new TreeMap<>(Map.of(
                10L, BatchOutcome.RecordFate.APPLIED)), List.of(b), new TreeMap<>(Map.of(b.breakId(), 10L)), List.of())),
                BEGAN, COMMITTED);
        new ResolverObserver(off, traces).decided(new Resolver.Outcome(List.of(new Resolver.Decision(b.breakId(),
                BreakState.ACTION_PENDING, List.of("cmd_1")))), BEGAN, COMMITTED);
        verify(traces, never()).save(any(), any());
        verify(traces, never()).find(anyCollection());
    }

    @SuppressWarnings("unchecked")
    @Test
    void theResolverAndTheDispatcherCarryTheBreaksTraceToTheCall() {
        String breakTrace = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01";
        when(traces.find(List.of("brk_1", "brk_untraced"))).thenReturn(new TreeMap<>(Map.of("brk_1", breakTrace)));
        new ResolverObserver(tracing, traces).decided(new Resolver.Outcome(List.of(
                new Resolver.Decision("brk_1", BreakState.ACTION_PENDING, List.of("cmd_r")),
                new Resolver.Decision("brk_untraced", BreakState.ESCALATED, List.of("cmd_x")))), BEGAN, COMMITTED);
        SpanData decide = span("resolver.decide");
        assertThat(decide.getParentSpanId()).isEqualTo("00f067aa0ba902b7");
        assertThat(spans.getFinishedSpanItems()).as("no span for a break without a context").hasSize(1);
        ArgumentCaptor<SortedMap<String, String>> saved = ArgumentCaptor.forClass(SortedMap.class);
        verify(traces).save(saved.capture(), any());
        String commandTrace = Tracing.traceparent(decide.getSpanContext());
        assertThat(saved.getValue()).containsExactly(Map.entry("cmd_r", commandTrace));

        when(traces.find(List.of("cmd_r"))).thenReturn(new TreeMap<>(Map.of("cmd_r", commandTrace)));
        OutboundCall call = new OutboundCall("cmd_r.1", M, CallTarget.GATEWAY, Optional.of("rfd_1"),
                new CallRequest.Refund(PaymentId.of("pay_1"), OrderId.of("ord_1"), Money.ofPaise(100)));
        String[] currentDuringSend = new String[1];
        DispatcherObserver dispatcher = new DispatcherObserver(registry, tracing, traces);
        dispatcher.send(call, () -> currentDuringSend[0] = Span.current().getSpanContext().getSpanId());
        SpanData sent = span("dispatcher.call");
        assertThat(sent.getTraceId()).as("one trace from ingest to the call").isEqualTo(decide.getTraceId());
        assertThat(sent.getParentSpanId()).isEqualTo(decide.getSpanId());
        assertThat(currentDuringSend[0]).as("current while the HTTP client sends").isEqualTo(sent.getSpanId());

        dispatcher.attempt(call, "answered");
        assertThat(registry.get("driftline.outbox.attempts").tag("kind", "REFUND").tag("result", "answered")
                .counter().count()).isEqualTo(1);
        assertThat(DispatcherObserver.commandId(new OutboundCall("cmd_r.2.lookup", M, CallTarget.GATEWAY,
                Optional.empty(), new CallRequest.RefundLookup(PaymentId.of("pay_1"))))).isEqualTo("cmd_r");
    }

    @Test
    void aCallWithoutAStoredTraceIsStillSent() {
        boolean[] sent = {false};
        new DispatcherObserver(registry, tracing, traces).send(new OutboundCall("cmd_q.1", M, CallTarget.GATEWAY,
                Optional.empty(), new CallRequest.StatusQuery(PaymentId.of("pay_1"))), () -> sent[0] = true);
        assertThat(sent[0]).isTrue();
        assertThat(spans.getFinishedSpanItems()).isEmpty();
    }

    @Test
    void ingestCountsEveryRequestToItsRoutesAndBoundsTheRejectionReason() throws Exception {
        IngestMetrics metrics = new IngestMetrics(registry);
        MockHttpServletResponse response = new MockHttpServletResponse();
        response.setStatus(401);
        metrics.filter().doFilter(new MockHttpServletRequest("POST", "/v1/webhooks/gateway/m_1"), response,
                new MockFilterChain());
        metrics.filter().doFilter(new MockHttpServletRequest("GET", "/v1/breaks"), new MockHttpServletResponse(),
                new MockFilterChain());
        assertThat(registry.get("driftline.ingest.requests").tag("route", "webhook").tag("status", "401").counter()
                .count()).isEqualTo(1);
        assertThat(registry.find("driftline.ingest.requests").tag("route", "webhook").counters()).hasSize(1);
        assertThat(IngestMetrics.route("/v1/merchants/m_1/order-events")).contains("order_event");
        assertThat(IngestMetrics.route("/v1/merchants/m_1/settlement-files")).contains("settlement_file");
        assertThat(IngestMetrics.route("/v1/merchants/m_1/ledger/adjustments")).isEmpty();
        assertThat(IngestMetrics.rejectionReason(401, "signature OUTSIDE_REPLAY_WINDOW"))
                .isEqualTo("signature_outside_replay_window");
        assertThat(IngestMetrics.rejectionReason(400, "payment.id: not an id \"<script>\"")).isEqualTo("malformed");
        assertThat(IngestMetrics.rejectionReason(413, "signature TOO_LARGE")).isEqualTo("signature_too_large");
    }

    @Test
    void theBooksMetricsMirrorTheSnapshotAndLinesThatCanStillChangeAreAGauge() {
        BookStats stats = mock(BookStats.class);
        Instant now = Instant.parse("2026-11-20T06:00:00Z");
        when(stats.snapshot(now)).thenReturn(new BookStats.Snapshot(
                new TreeMap<>(Map.of("DEBITED_NO_ORDER", 3L)),
                new TreeMap<>(Map.of("DEBITED_NO_ORDER", new TreeMap<>(Map.of("auto", 1L)))),
                new TreeMap<>(Map.of("STATUS_CONFLICT", new TreeMap<>(Map.of("for_a_person", 2L)))),
                new TreeMap<>(Map.of("DEBITED_NO_ORDER", new TreeMap<>(Map.of("ACTION_PENDING", 2L)))),
                new TreeMap<>(Map.of("DEBITED_NO_ORDER", now.plus(Duration.ofHours(5)))), 300,
                new TreeMap<>(Map.of("REFUND", 1L)), Optional.of(now.minusSeconds(90)),
                new TreeMap<>(Map.of("received", 5L, "MATCHED", 3L, "AWAITING_BATCH", 2L)), 1,
                List.of(new BookStats.Watermark(3, "GATEWAY", now.minusSeconds(60)))));
        BookMetrics book = new BookMetrics(registry, stats, () -> now, Duration.ofSeconds(15));
        book.refresh();
        assertThat(registry.get("driftline.breaks.detected").tag("type", "DEBITED_NO_ORDER").functionCounter().count())
                .isEqualTo(3);
        assertThat(registry.get("driftline.breaks.closed").tag("how", "auto").functionCounter().count()).isEqualTo(1);
        assertThat(registry.get("driftline.breaks.escalated").tag("reason", "for_a_person").functionCounter().count())
                .isEqualTo(2);
        assertThat(registry.get("driftline.breaks.open").tag("state", "ACTION_PENDING").gauge().value()).isEqualTo(2);
        assertThat(registry.get("driftline.deadline.headroom.seconds").tag("type", "DEBITED_NO_ORDER").gauge().value())
                .isEqualTo(18_000);
        assertThat(registry.get("driftline.compensation.exposure.rupees").gauge().value()).isEqualTo(300);
        assertThat(registry.get("driftline.outbox.pending").tag("kind", "REFUND").gauge().value()).isEqualTo(1);
        assertThat(registry.get("driftline.outbox.oldest.pending.seconds").gauge().value()).isEqualTo(90);
        assertThat(registry.get("driftline.settlement.lines").tag("result", "received").functionCounter().count())
                .isEqualTo(5);
        assertThat(registry.get("driftline.settlement.lines").tag("result", "matched").functionCounter().count())
                .isEqualTo(3);
        assertThat(registry.find("driftline.settlement.lines").tag("result", "awaiting_batch").functionCounter())
                .as("a status that can change is not a counter").isNull();
        assertThat(registry.get("driftline.settlement.lines.unresolved").tag("status", "awaiting_batch").gauge()
                .value()).isEqualTo(2);
        assertThat(registry.get("driftline.pricing.drift").functionCounter().count()).isEqualTo(1);
        assertThat(registry.get("driftline.watermark.lag.seconds").tag("partition", "3").tag("source", "gateway")
                .gauge().value()).isEqualTo(60);
    }

    @Test
    void aTraceparentRoundTrips() {
        assertThat(Tracing.parse(INGEST_TRACE).map(Tracing::traceparent)).contains(INGEST_TRACE);
        assertThat(Tracing.parse("00-00000000000000000000000000000000-b7ad6b7169203331-01")).isEmpty();
        assertThat(Tracing.parse("garbage")).isEmpty();
        assertThat(Tracing.parse(null)).isEmpty();
    }

    SpanData span(String name) {
        return spans.getFinishedSpanItems().stream().filter(s -> s.getName().equals(name)).findFirst().orElseThrow();
    }

    /**
     * Every metric the Grafana board and the alert rules query is one Driftline exports, under that exact Prometheus
     * name: each observer is driven once through every path, then a real Prometheus registry is scraped.
     */
    @Test
    void everyMetricTheBoardAndTheAlertsQueryIsExported() throws Exception {
        PrometheusMeterRegistry prometheus = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Break b = Break.detected(M, BreakType.LATE_SUCCESS, "pay_1", COMMITTED, Map.of());
        MatcherObserver matcher = new MatcherObserver(prometheus, tracing, traces);
        matcher.chunk(0, new TreeMap<>(Map.of(1L, envelope("e1", Source.GATEWAY, Optional.empty()),
                        2L, envelope("e2", Source.GATEWAY, Optional.empty()),
                        3L, envelope("e3", Source.MERCHANT, Optional.empty()))),
                List.of(outcome(new TreeMap<>(Map.of(1L, BatchOutcome.RecordFate.APPLIED,
                                2L, BatchOutcome.RecordFate.DUPLICATE, 3L, BatchOutcome.RecordFate.APPLIED_LATE)),
                        List.of(b), new TreeMap<>(Map.of(b.breakId(), 1L)), List.of(10L))), BEGAN, COMMITTED);
        matcher.lag(0, 1);
        matcher.fenced(0);
        IngestMetrics ingest = new IngestMetrics(prometheus);
        ingest.filter().doFilter(new MockHttpServletRequest("POST", "/v1/webhooks/gateway/m_1"),
                new MockHttpServletResponse(), new MockFilterChain());
        ingest.webhookRejected("signature_no_match");
        new DispatcherObserver(prometheus, tracing, traces).attempt(new OutboundCall("cmd_1.1", M, CallTarget.GATEWAY,
                Optional.empty(), new CallRequest.StatusQuery(PaymentId.of("pay_1"))), "no_reply");
        BookStats stats = mock(BookStats.class);
        Instant now = Instant.parse("2026-11-20T06:00:00Z");
        when(stats.snapshot(now)).thenReturn(new BookStats.Snapshot(new TreeMap<>(Map.of("FEED_STALL", 1L)),
                new TreeMap<>(Map.of("FEED_STALL", new TreeMap<>(Map.of("manual", 1L)))),
                new TreeMap<>(Map.of("FEED_STALL", new TreeMap<>(Map.of("deadline", 1L)))),
                new TreeMap<>(Map.of("FEED_STALL", new TreeMap<>(Map.of("ESCALATED", 1L)))),
                new TreeMap<>(Map.of("FEED_STALL", now)), 0, new TreeMap<>(Map.of("REFUND", 1L)), Optional.of(now),
                new TreeMap<>(Map.of("received", 1L, "AMBIGUOUS", 1L)), 0,
                List.of(new BookStats.Watermark(0, "GATEWAY", now))));
        new BookMetrics(prometheus, stats, () -> now, Duration.ofSeconds(15)).refresh();
        new BacklogProbe(prometheus, () -> List.of(0), partitions -> Map.of(0, 5L), partitions -> Map.of(0, 3L),
                Duration.ofSeconds(5)).refresh();
        String scrape = prometheus.scrape();

        Path deploy = Path.of("..", "deploy");
        String queried = Files.readString(deploy.resolve("grafana/dashboards/driftline.json"))
                + Files.readString(deploy.resolve("prometheus/alerts.yml"));
        Set<String> names = new TreeSet<>();
        Matcher m = Pattern.compile("driftline_[a-z_]+").matcher(queried);
        while (m.find()) {
            names.add(m.group());
        }
        assertThat(names).as("the board and the alerts query the §13 metrics").hasSizeGreaterThan(20);
        assertThat(names).allSatisfy(name -> assertThat(scrape).as(name).containsPattern("(?m)^" + name + "[{ ]"));
    }
}
