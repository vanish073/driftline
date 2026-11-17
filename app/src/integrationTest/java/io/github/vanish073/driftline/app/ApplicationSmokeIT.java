package io.github.vanish073.driftline.app;

import static io.github.vanish073.driftline.app.Containers.await;
import static io.github.vanish073.driftline.app.Containers.count;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.order.OrderEventCodec;
import io.github.vanish073.driftline.wire.order.OrderEventMessage;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
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
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The whole application, role {@code all}, under the production heap cap (-Xmx256m, SerialGC; DESIGN §15.1), against
 * real PostgreSQL and Kafka: it starts, reports live and ready on the management port, takes signed webhooks and order
 * events through HTTP into Kafka, the matcher detects a DUPLICATE_CAPTURE that the ops API then shows, a replayed
 * webhook changes nothing, a settlement upload goes through the separate worker into the matcher, and it stops.
 */
class ApplicationSmokeIT {

    static final long HEAP_BUDGET_BYTES = 256L * 1024 * 1024;
    static final String MERCHANT = "m_smoke";
    static final String SECRET = "test-only-webhook-secret-smoke-not-a-real-key";
    static final String TOKEN = "test-token-smoke_merchant";
    static final String OPS_TOKEN = "test-token-smoke_ops";
    static final String SERVICE_TOKEN = "test-token-smoke_service";
    static final Instant T = Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(Duration.ofHours(1));
    static final LocalDate FIRST_SETTLEMENT = LocalDate.parse("2026-11-10");

    @TempDir
    static Path dir;
    static String url;
    static ConfigurableApplicationContext app;
    static String api;
    static String management;
    static final HttpClient HTTP = HttpClient.newHttpClient();

    @BeforeAll
    static void start() throws IOException {
        assertThat(Runtime.getRuntime().maxMemory()).as("the test JVM runs with the production heap cap")
                .isLessThanOrEqualTo(HEAP_BUDGET_BYTES);
        url = Containers.newDatabase("smoke_it");
        Containers.createTopic("driftline.events.v1", 12);
        Path bootstrap = dir.resolve("bootstrap.json");
        Files.writeString(bootstrap, """
                {"format":"driftline-bootstrap-v1",
                 "merchants":{"%s":{"partition":3,"fee_bps":200,"gst_bps":1800,"cycle_days":1,
                                    "first_settlement_date":"%s","webhook_secret":"%s","token":"%s"}},
                 "ops_token":"%s","service_token":"%s"}
                """.formatted(MERCHANT, FIRST_SETTLEMENT, SECRET, TOKEN, OPS_TOKEN, SERVICE_TOKEN));
        app = DriftlineApplication.application("all").run(
                "--spring.datasource.url=" + url,
                "--spring.datasource.username=" + Containers.APP_USER,
                "--spring.datasource.password=" + Containers.APP_PASSWORD,
                "--driftline.kafka.bootstrap-servers=" + Containers.KAFKA.getBootstrapServers(),
                "--driftline.kafka.session-timeout=6s",
                "--driftline.secrets.active-kid=it",
                "--driftline.secrets.keys.it=" + Base64.getEncoder().encodeToString(new byte[32]),
                "--driftline.bootstrap-file=" + bootstrap,
                "--driftline.instance-id=smoke",
                // role `all` includes the dispatcher; port 9 refuses at once, so any call fails fast and is retried later
                "--driftline.dispatcher.gateway-url=http://127.0.0.1:9/gateway",
                "--driftline.dispatcher.merchant-url=http://127.0.0.1:9/merchant",
                "--driftline.dispatcher.ops-url=http://127.0.0.1:9/ops",
                "--driftline.dispatcher.observations-url=http://127.0.0.1:9",
                "--driftline.dispatcher.service-token=" + SERVICE_TOKEN,
                "--server.port=0",
                "--management.server.port=0");
        api = "http://127.0.0.1:" + app.getEnvironment().getProperty("local.server.port");
        management = "http://127.0.0.1:" + app.getEnvironment().getProperty("local.management.port");
    }

    @AfterAll
    static void stop() {
        if (app != null) {
            app.close();
            assertThat(app.isActive()).isFalse();
        }
    }

    static HttpResponse<String> send(HttpRequest.Builder request) {
        try {
            return HTTP.send(request.timeout(Duration.ofSeconds(20)).build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    static HttpResponse<String> webhook(String eventId, String paymentId, String orderId, Instant at) {
        byte[] body = GatewayWebhookCodec.encode(new GatewayWebhook(eventId, EventType.PAYMENT_SUCCEEDED, at,
                new PaymentPayload(PaymentId.of(paymentId), OrderId.of(orderId), Money.ofPaise(25_000))));
        String signature = WebhookSignature.sign(SECRET.getBytes(StandardCharsets.UTF_8), Instant.now(), body);
        return send(HttpRequest.newBuilder(URI.create(api + "/v1/webhooks/gateway/" + MERCHANT))
                .header(WebhookSignature.HEADER, signature).POST(HttpRequest.BodyPublishers.ofByteArray(body)));
    }

    static HttpResponse<String> get(String path, String token) {
        return send(HttpRequest.newBuilder(URI.create(api + path)).header("Authorization", "Bearer " + token).GET());
    }

    @Test
    void itIsLiveAndReadyOnTheManagementPortOnly() {
        HttpResponse<String> live = send(HttpRequest.newBuilder(URI.create(management + "/actuator/health/liveness")).GET());
        assertThat(live.statusCode()).isEqualTo(200);
        assertThat(live.body()).contains("UP");
        HttpResponse<String> ready = send(HttpRequest.newBuilder(URI.create(management + "/actuator/health/readiness")).GET());
        assertThat(ready.statusCode()).isEqualTo(200);
        assertThat(send(HttpRequest.newBuilder(URI.create(api + "/actuator/health")).GET()).statusCode())
                .as("actuator is not on the API port").isIn(401, 404);
    }

    @Test
    void aDuplicateCaptureTravelsFromHttpThroughKafkaToTheOpsApi() {
        byte[] created = OrderEventCodec.encode(new OrderEventMessage(EventType.ORDER_CREATED, T,
                new OrderPayload(OrderId.of("ord_1"), 1, Money.ofPaise(25_000), T.plusSeconds(900), Optional.empty())));
        HttpResponse<String> order = send(HttpRequest.newBuilder(URI.create(api + "/v1/merchants/" + MERCHANT
                        + "/order-events")).header("Authorization", "Bearer " + TOKEN).header("Idempotency-Key", "ord_1-v1")
                .POST(HttpRequest.BodyPublishers.ofByteArray(created)));
        assertThat(order.statusCode()).isEqualTo(202);
        assertThat(webhook("evt_1", "pay_1", "ord_1", T.plusSeconds(10)).statusCode()).isEqualTo(202);
        assertThat(webhook("evt_2", "pay_2", "ord_1", T.plusSeconds(11)).statusCode()).isEqualTo(202);
        // A later gateway event moves the gateway watermark past DUP_CHECK (max success + 2 min, DESIGN §5.5).
        assertThat(webhook("evt_9", "pay_9", "ord_9", T.plusSeconds(600)).statusCode()).isEqualTo(202);

        await("the duplicate capture is detected", Duration.ofSeconds(90),
                () -> get("/v1/breaks?type=DUPLICATE_CAPTURE", TOKEN).body().contains("\"count\":1"));
        String breaks = get("/v1/breaks?type=DUPLICATE_CAPTURE", OPS_TOKEN).body();
        assertThat(breaks).contains("\"subject_key\":\"pay_2\"").contains("\"merchant_id\":\"" + MERCHANT + "\"");

        // A replayed webhook (fresh signature, same event id) is accepted and changes nothing.
        assertThat(webhook("evt_2", "pay_2", "ord_1", T.plusSeconds(11)).statusCode()).isEqualTo(202);
        // The partition is ordered: once a marker sent after the replay is applied, the replay was consumed too.
        assertThat(webhook("evt_10", "pay_10", "ord_10", T.plusSeconds(601)).statusCode()).isEqualTo(202);
        await("the replay was consumed", Duration.ofSeconds(30), () -> count(url,
                "SELECT count(*) FROM processed_events WHERE event_key = 'evt_10'") == 1);
        assertThat(count(url, "SELECT count(*) FROM payment_transitions WHERE payment_id = 'pay_2'")).isEqualTo(1);
        assertThat(count(url, "SELECT count(*) FROM processed_events WHERE event_key = 'evt_2'")).isEqualTo(1);
        String timeline = get("/v1/payments/pay_2/timeline", TOKEN).body();
        assertThat(timeline).contains("\"status\":\"SUCCEEDED\"").contains("DUPLICATE_CAPTURE");
    }

    @Test
    void aSettlementUploadGoesThroughTheSeparateWorkerIntoTheMatcher() {
        String file = "H,DLSF,1,stl_smoke_1,%s,%s,%s\nT,0,0.00\n".formatted(MERCHANT, FIRST_SETTLEMENT,
                FIRST_SETTLEMENT + "T04:30:00Z");
        HttpResponse<String> upload = send(HttpRequest.newBuilder(URI.create(api + "/v1/merchants/" + MERCHANT
                        + "/settlement-files")).header("Authorization", "Bearer " + TOKEN)
                .POST(HttpRequest.BodyPublishers.ofString(file)));
        assertThat(upload.statusCode()).isEqualTo(202);
        await("the worker emitted it", Duration.ofSeconds(60), () -> count(url,
                "SELECT count(*) FROM settlement_uploads WHERE state = 'EMITTED'") == 1);
        await("the matcher completed it", Duration.ofSeconds(60), () -> count(url,
                "SELECT count(*) FROM settlement_file_ends WHERE file_id = 'stl_smoke_1' AND complete") == 1);
        HttpResponse<String> again = send(HttpRequest.newBuilder(URI.create(api + "/v1/merchants/" + MERCHANT
                        + "/settlement-files")).header("Authorization", "Bearer " + TOKEN)
                .POST(HttpRequest.BodyPublishers.ofString(file)));
        assertThat(again.body()).as("the same answer for the same file").isEqualTo(upload.body());
        assertThat(count(url, "SELECT count(*) FROM settlement_files")).isEqualTo(1);
    }
}
