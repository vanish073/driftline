package io.github.vanish073.driftline.app;

import static io.github.vanish073.driftline.app.Containers.await;
import static io.github.vanish073.driftline.app.Containers.count;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
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
import java.time.ZoneOffset;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Webhook secret rotation in a running app (§10.2, §14, D76): after a rotation through the ops API, webhooks signed
 * with either secret are accepted during the overlap and the old one is refused once it ends, with no restart; the
 * rotation leaves one audit row and the secret appears nowhere in it.
 */
class SecretRotationIT {

    /**
     * Two days ahead, whatever today is: a first settlement date already past makes a settlement break due on the
     * first record, which this test does not expect (it passed until 2026-11-18, then failed).
     */
    static final String FIRST_SETTLEMENT = LocalDate.now(ZoneOffset.UTC).plusDays(2).toString();

    static final String MERCHANT = "m_rotate";
    static final String OLD = "test-only-webhook-secret-before-rotation";
    static final String NEW = "test-only-webhook-secret-after-the-rotation";
    static final String OPS_TOKEN = "test-token-rotation_ops";
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

    int webhook(String secret, String eventId) {
        byte[] body = GatewayWebhookCodec.encode(new GatewayWebhook(eventId, EventType.PAYMENT_PENDING,
                Instant.parse("2026-11-17T06:00:00Z"), new PaymentPayload(PaymentId.of("pay_" + eventId),
                OrderId.of("ord_" + eventId), Money.ofPaise(10_000))));
        String signature = WebhookSignature.sign(secret.getBytes(StandardCharsets.UTF_8), Instant.now(), body);
        return send(HttpRequest.newBuilder(URI.create(api + "/v1/webhooks/gateway/" + MERCHANT))
                .header(WebhookSignature.HEADER, signature).POST(HttpRequest.BodyPublishers.ofByteArray(body)))
                .statusCode();
    }

    @Test
    void aRotatedSecretIsAcceptedAtOnceAndTheOldOneIsRefusedAfterTheOverlapWithNoRestart() throws IOException {
        String url = Containers.newDatabase("secret_rotation_app_it");
        Containers.createTopic("rotation_it.events", 12);
        Path bootstrap = dir.resolve("bootstrap.json");
        Files.writeString(bootstrap, """
                {"format":"driftline-bootstrap-v1",
                 "merchants":{"%s":{"partition":2,"fee_bps":200,"gst_bps":1800,"cycle_days":1,
                                    "first_settlement_date":"%s","webhook_secret":"%s","token":"test-token-rotation_m"}},
                 "ops_token":"%s","service_token":"test-token-rotation_service"}
                """.formatted(MERCHANT, FIRST_SETTLEMENT, OLD, OPS_TOKEN));
        try (ConfigurableApplicationContext app = DriftlineApplication.application("ingest").run(
                "--spring.datasource.url=" + url,
                "--spring.datasource.username=" + Containers.APP_USER,
                "--spring.datasource.password=" + Containers.APP_PASSWORD,
                "--driftline.kafka.bootstrap-servers=" + Containers.KAFKA.getBootstrapServers(),
                "--driftline.kafka.topic=rotation_it.events",
                "--driftline.secrets.active-kid=it",
                "--driftline.secrets.keys.it=" + Base64.getEncoder().encodeToString(new byte[32]),
                "--driftline.secrets.refresh-interval=1s",
                "--driftline.bootstrap-file=" + bootstrap,
                "--driftline.instance-id=rotation",
                "--server.port=0",
                "--management.server.port=0")) {
            api = "http://127.0.0.1:" + app.getEnvironment().getProperty("local.server.port");
            assertThat(webhook(OLD, "e1")).isEqualTo(202);
            assertThat(webhook(NEW, "e2")).as("not yet").isEqualTo(401);

            HttpResponse<String> rotated = send(HttpRequest.newBuilder(URI.create(api + "/v1/merchants/" + MERCHANT
                            + "/webhook-secrets/rotate")).header("Authorization", "Bearer " + OPS_TOKEN)
                    .POST(HttpRequest.BodyPublishers.ofString("{\"kid\":\"k2\",\"secret\":\"" + NEW
                            + "\",\"overlap_seconds\":60}")));
            assertThat(rotated.statusCode()).as(rotated.body()).isEqualTo(201);
            assertThat(rotated.body()).doesNotContain(NEW);
            Instant retireAt = Instant.parse(rotated.body().replaceAll(".*\"previous_secrets_retire_at\":\"([^\"]+)\".*",
                    "$1").trim());

            assertThat(webhook(NEW, "e3")).as("the new secret, at once on this instance").isEqualTo(202);
            assertThat(webhook(OLD, "e4")).as("the old secret, during the overlap").isEqualTo(202);
            await("the old secret is refused after the overlap", Duration.ofSeconds(90),
                    () -> Instant.now().isAfter(retireAt.plusSeconds(2)) && webhook(OLD, "e5") == 401);
            assertThat(webhook(NEW, "e6")).isEqualTo(202);

            assertThat(count(url, "SELECT count(*) FROM audit_log WHERE action = 'WEBHOOK_SECRET_ROTATE'")).isEqualTo(1);
            assertThat(count(url, "SELECT count(*) FROM audit_log WHERE details::text LIKE '%" + NEW + "%'"))
                    .as("the secret is nowhere in the audit trail").isZero();
            assertThat(count(url, "SELECT count(*) FROM merchant_webhook_secrets WHERE position(convert_to('" + NEW
                    + "', 'UTF8') IN secret_enc) > 0")).as("stored only as ciphertext").isZero();
            assertThat(count(url, "SELECT count(*) FROM merchant_webhook_secrets WHERE kid = 'k2'")).isEqualTo(1);
        }
    }
}
