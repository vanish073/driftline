package io.github.vanish073.driftline.app;

import static io.github.vanish073.driftline.app.Containers.await;
import static io.github.vanish073.driftline.app.Containers.count;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.webhook.CustomerContact;
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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Personal data end to end (DESIGN §14, D77): a webhook's contact is stored as ciphertext only, with a blind index,
 * append-only; the ops API finds the payment by VPA and shows the contact masked; and the running app's log output,
 * captured whole, holds no raw contact, even when a line deliberately includes one.
 */
@ExtendWith(OutputCaptureExtension.class)
class CustomerVaultIT {

    /**
     * Two days ahead, whatever today is: a first settlement date already past makes a settlement break due on the
     * first record, which this test does not expect (it passed until 2026-11-18, then failed).
     */
    static final String FIRST_SETTLEMENT = LocalDate.now(ZoneOffset.UTC).plusDays(2).toString();

    static final String MERCHANT = "m_vault";
    static final String SECRET = "test-only-webhook-secret-vault-not-a-real-key";
    static final String OPS_TOKEN = "test-token-vault_ops";
    static final String VPA = "vault.buyer@okaxis";
    static final String PHONE = "9812345678";
    static final String EMAIL = "vault.buyer@example.com";
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

    static String key(int fill) {
        byte[] b = new byte[32];
        Arrays.fill(b, (byte) fill);
        return Base64.getEncoder().encodeToString(b);
    }

    @Test
    void aContactIsStoredEncryptedFoundByVpaShownMaskedAndNeverLogged(CapturedOutput output) throws Exception {
        String url = Containers.newDatabase("customer_vault_it");
        Containers.createTopic("vault_it.events", 12);
        Path bootstrap = dir.resolve("bootstrap.json");
        Files.writeString(bootstrap, """
                {"format":"driftline-bootstrap-v1",
                 "merchants":{"%s":{"partition":4,"fee_bps":200,"gst_bps":1800,"cycle_days":1,
                                    "first_settlement_date":"%s","webhook_secret":"%s","token":"test-token-vault_m"}},
                 "ops_token":"%s","service_token":"test-token-vault_service"}
                """.formatted(MERCHANT, FIRST_SETTLEMENT, SECRET, OPS_TOKEN));
        try (ConfigurableApplicationContext app = DriftlineApplication.application("ingest,matcher").run(
                "--spring.datasource.url=" + url,
                "--spring.datasource.username=" + Containers.APP_USER,
                "--spring.datasource.password=" + Containers.APP_PASSWORD,
                "--driftline.kafka.bootstrap-servers=" + Containers.KAFKA.getBootstrapServers(),
                "--driftline.kafka.topic=vault_it.events",
                "--driftline.kafka.group-id=vault-it",
                "--driftline.kafka.session-timeout=6s",
                "--driftline.secrets.active-kid=it",
                "--driftline.secrets.keys.it=" + key(0),
                "--driftline.pii.active-kid=p1",
                "--driftline.pii.keys.p1=" + key(5),
                "--driftline.pii.index-key=" + key(6),
                "--driftline.bootstrap-file=" + bootstrap,
                "--driftline.instance-id=vault",
                "--server.port=0",
                "--management.server.port=0")) {
            api = "http://127.0.0.1:" + app.getEnvironment().getProperty("local.server.port");
            byte[] body = GatewayWebhookCodec.encode(new GatewayWebhook("evt_v1", EventType.PAYMENT_SUCCEEDED,
                    Instant.parse("2026-11-17T06:00:00Z"), new PaymentPayload(PaymentId.of("pay_v1"),
                    OrderId.of("ord_v1"), Money.ofPaise(49_900))),
                    Optional.of(CustomerContact.of(Optional.of(VPA), Optional.of(PHONE), Optional.of(EMAIL))));
            HttpResponse<String> accepted = send(HttpRequest.newBuilder(URI.create(api + "/v1/webhooks/gateway/"
                    + MERCHANT)).header(WebhookSignature.HEADER, WebhookSignature.sign(SECRET.getBytes(
                    StandardCharsets.UTF_8), Instant.now(), body)).POST(HttpRequest.BodyPublishers.ofByteArray(body)));
            assertThat(accepted.statusCode()).as(accepted.body()).isEqualTo(202);

            assertThat(count(url, "SELECT count(*) FROM payment_customers WHERE payment_id = 'pay_v1'"
                    + " AND vpa_index IS NOT NULL")).isEqualTo(1);
            for (String plain : new String[] {VPA, PHONE, EMAIL}) {
                assertThat(count(url, "SELECT count(*) FROM payment_customers WHERE position(convert_to('" + plain
                        + "', 'UTF8') IN coalesce(vpa_enc, '') || coalesce(phone_enc, '') || coalesce(email_enc, '')) > 0"))
                        .as("no plaintext in the vault: " + plain.length() + " characters").isZero();
            }
            try (Connection c = Containers.superuser(url); Statement st = c.createStatement()) {
                assertThatThrownBy(() -> st.execute("UPDATE payment_customers SET vpa_index = NULL"))
                        .as("append-only, even for the owner").isInstanceOf(SQLException.class);
            }

            HttpResponse<String> search = send(HttpRequest.newBuilder(URI.create(api + "/v1/payments/search?vpa="
                    + "Vault.Buyer@OKAXIS&merchantId=" + MERCHANT)).header("Authorization", "Bearer " + OPS_TOKEN).GET());
            assertThat(search.statusCode()).isEqualTo(200);
            assertThat(search.body()).contains("\"payment_id\":\"pay_v1\"");

            await("the matcher applied the payment", Duration.ofSeconds(60), () -> send(HttpRequest.newBuilder(
                    URI.create(api + "/v1/payments/pay_v1/timeline?merchantId=" + MERCHANT))
                    .header("Authorization", "Bearer " + OPS_TOKEN).GET()).statusCode() == 200);
            String timeline = send(HttpRequest.newBuilder(URI.create(api + "/v1/payments/pay_v1/timeline?merchantId="
                    + MERCHANT)).header("Authorization", "Bearer " + OPS_TOKEN).GET()).body();
            assertThat(timeline).contains("\"vpa\":\"va***@okaxis\"").contains("\"phone\":\"98******78\"")
                    .contains("\"email\":\"va***@example.com\"");

            LoggerFactory.getLogger(CustomerVaultIT.class).info("a careless line: {} {} {}", VPA, PHONE, EMAIL);
        }
        String logs = output.getAll();
        assertThat(logs).as("the careless line was written, masked").contains("a careless line: va***@okaxis");
        assertThat(logs).as("no raw contact anywhere in the app's output").doesNotContain(VPA).doesNotContain(PHONE)
                .doesNotContain(EMAIL);
    }
}
