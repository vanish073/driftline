package io.github.vanish073.driftline.app;

import static io.github.vanish073.driftline.app.Containers.await;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Metrics and traces end to end (DESIGN §13, D78), with real PostgreSQL and Kafka and every role in one process, and
 * an in-memory OpenTelemetry SDK in place of the Java agent. A payment that fails and then succeeds raises a
 * LATE_SUCCESS; the resolver decides and the dispatcher calls the (unreachable) gateway. Every span from the webhook
 * that raised the break to the dispatcher's call belongs to one trace, and Prometheus shows the §13 metrics.
 */
class ObservabilityIT {

    /**
     * Two days ahead, whatever today is: a first settlement date already past makes a settlement break due on the
     * first record, which this test does not expect (it passed until 2026-11-18, then failed).
     */
    static final String FIRST_SETTLEMENT = LocalDate.now(ZoneOffset.UTC).plusDays(2).toString();

    static final String MERCHANT = "m_obs";
    static final String SECRET = "test-only-webhook-secret-obs-not-a-real-key";
    static final String OPS_TOKEN = "test-token-obs_ops";
    static final String SERVICE_TOKEN = "test-token-obs_service";
    static final HttpClient HTTP = HttpClient.newHttpClient();

    @TempDir
    Path dir;
    String api;

    HttpResponse<String> send(HttpRequest.Builder request) {
        try {
            return HTTP.send(request.timeout(Duration.ofSeconds(20)).build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    int webhook(String eventId, EventType type, Instant at) {
        byte[] body = GatewayWebhookCodec.encode(new GatewayWebhook(eventId, type, at,
                new PaymentPayload(PaymentId.of("pay_obs"), OrderId.of("ord_obs"), Money.ofPaise(49_900))));
        return send(HttpRequest.newBuilder(URI.create(api + "/v1/webhooks/gateway/" + MERCHANT))
                .header(WebhookSignature.HEADER, WebhookSignature.sign(SECRET.getBytes(StandardCharsets.UTF_8),
                        Instant.now(), body)).POST(HttpRequest.BodyPublishers.ofByteArray(body))).statusCode();
    }

    static Optional<SpanData> span(InMemorySpanExporter spans, String name) {
        return spans.getFinishedSpanItems().stream().filter(s -> s.getName().equals(name)).findFirst();
    }

    @Test
    void oneTraceRunsFromTheWebhookToTheDispatchersCallAndPrometheusShowsTheMetrics() throws Exception {
        String url = Containers.newDatabase("observability_it");
        Containers.createTopic("obs_it.events", 12);
        Path bootstrap = dir.resolve("bootstrap.json");
        Files.writeString(bootstrap, """
                {"format":"driftline-bootstrap-v1",
                 "merchants":{"%s":{"partition":5,"fee_bps":200,"gst_bps":1800,"cycle_days":1,
                                    "first_settlement_date":"%s","webhook_secret":"%s","token":"test-token-obs_m"}},
                 "ops_token":"%s","service_token":"%s"}
                """.formatted(MERCHANT, FIRST_SETTLEMENT, SECRET, OPS_TOKEN, SERVICE_TOKEN));
        InMemorySpanExporter spans = InMemorySpanExporter.create();
        OpenTelemetry sdk = OpenTelemetrySdk.builder().setTracerProvider(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spans)).build()).build();
        SpringApplication application = DriftlineApplication.application("all");
        application.addInitializers(context -> context.getBeanFactory().registerSingleton("openTelemetry", sdk));
        try (ConfigurableApplicationContext app = application.run(
                "--spring.datasource.url=" + url,
                "--spring.datasource.username=" + Containers.APP_USER,
                "--spring.datasource.password=" + Containers.APP_PASSWORD,
                "--driftline.kafka.bootstrap-servers=" + Containers.KAFKA.getBootstrapServers(),
                "--driftline.kafka.topic=obs_it.events",
                "--driftline.kafka.group-id=obs-it",
                "--driftline.kafka.session-timeout=6s",
                "--driftline.secrets.active-kid=it",
                "--driftline.secrets.keys.it=" + Base64.getEncoder().encodeToString(new byte[32]),
                "--driftline.bootstrap-file=" + bootstrap, "--driftline.environment=test",
                "--driftline.metrics.token=test-only-metrics-token",
                "--driftline.instance-id=obs",
                "--driftline.resolver.metrics-interval=1s",
                // port 9 refuses at once: the call fails fast and is retried after its lease
                "--driftline.dispatcher.gateway-url=http://127.0.0.1:9/gateway",
                "--driftline.dispatcher.merchant-url=http://127.0.0.1:9/merchant",
                "--driftline.dispatcher.ops-url=http://127.0.0.1:9/ops",
                "--driftline.dispatcher.observations-url=http://127.0.0.1:9",
                "--driftline.dispatcher.service-token=" + SERVICE_TOKEN,
                "--server.port=0",
                "--management.server.port=0")) {
            api = "http://127.0.0.1:" + app.getEnvironment().getProperty("local.server.port");
            String management = "http://127.0.0.1:" + app.getEnvironment().getProperty("local.management.port");
            Instant t = Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(Duration.ofMinutes(10));
            assertThat(webhook("evt_obs_failed", EventType.PAYMENT_FAILED, t)).isEqualTo(202);
            assertThat(webhook("evt_obs_succeeded", EventType.PAYMENT_SUCCEEDED, t.plusSeconds(60))).isEqualTo(202);

            await("the dispatcher sent a call for the break", Duration.ofSeconds(90),
                    () -> span(spans, "dispatcher.call").isPresent());
            SpanData call = span(spans, "dispatcher.call").orElseThrow();
            SpanData decide = spans.getFinishedSpanItems().stream()
                    .filter(s -> s.getName().equals("resolver.decide") && s.getSpanId().equals(call.getParentSpanId()))
                    .findFirst().orElseThrow();
            SpanData detect = spans.getFinishedSpanItems().stream()
                    .filter(s -> s.getName().equals("matcher.detect") && s.getSpanId().equals(decide.getParentSpanId()))
                    .findFirst().orElseThrow();
            SpanData ingest = spans.getFinishedSpanItems().stream()
                    .filter(s -> s.getName().equals("ingest.webhook") && s.getSpanId().equals(detect.getParentSpanId()))
                    .findFirst().orElseThrow();
            assertThat(List.of(ingest, detect, decide, call)).extracting(SpanData::getTraceId)
                    .as("ingest → Kafka → matcher → resolver → dispatcher: one trace").containsOnly(ingest.getTraceId());
            assertThat(detect.getAttributes().asMap().toString()).contains("LATE_SUCCESS");
            assertThat(span(spans, "matcher.batch").orElseThrow().getLinks()).as("a batch links its records' traces")
                    .isNotEmpty();

            List<String> required = List.of(
                    "driftline_ingest_requests_total{route=\"webhook\",status=\"202\"} 2.0",
                    "driftline_events_consumed_total{source=\"gateway\"} 2.0",
                    "driftline_event_to_match_seconds_bucket",
                    "driftline_detection_lag_seconds_bucket{type=\"LATE_SUCCESS\"",
                    "driftline_breaks_detected_total{type=\"LATE_SUCCESS\"} 1.0",
                    "driftline_breaks_open{state=",
                    "driftline_deadline_headroom_seconds{type=\"LATE_SUCCESS\"}",
                    "driftline_compensation_exposure_rupees",
                    "driftline_outbox_pending{kind=",
                    "driftline_outbox_oldest_pending_seconds",
                    "driftline_outbox_attempts_total{kind=",
                    "driftline_watermark_lag_seconds{partition=\"5\",source=\"gateway\"}",
                    "driftline_consumer_lag_records{partition=",
                    "driftline_consumer_client_lag_records{partition=",
                    "driftline_settlement_lines_total{result=\"received\"} 0.0",
                    "driftline_pricing_drift_total 0.0");
            URI prometheus = URI.create(management + "/actuator/prometheus");
            try {
                await("every §13 metric is exported", Duration.ofSeconds(30), () -> {
                    String body = send(HttpRequest.newBuilder(prometheus)
                            .header("Authorization", "Bearer test-only-metrics-token").GET()).body();
                    return required.stream().allMatch(body::contains);
                });
            } catch (AssertionError timedOut) {
                // the assertion below names each metric still missing
            }
            String scrape = send(HttpRequest.newBuilder(prometheus)
                    .header("Authorization", "Bearer test-only-metrics-token").GET()).body();
            assertThat(required).allSatisfy(m -> assertThat(scrape).as(m).contains(m));
        }
    }
}
