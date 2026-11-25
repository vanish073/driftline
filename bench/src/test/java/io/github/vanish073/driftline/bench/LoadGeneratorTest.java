package io.github.vanish073.driftline.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.order.OrderEventCodec;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The generator's requests are what a real gateway and merchant send, three events per order, merchants in turn. */
class LoadGeneratorTest {

    static final Instant NOW = Instant.parse("2026-11-18T06:00:00Z");
    static final byte[] BOOTSTRAP = """
            {"format":"driftline-bootstrap-v1",
             "merchants":{"m_b":{"partition":1,"fee_bps":200,"gst_bps":1800,"cycle_days":1,
                                 "first_settlement_date":"2026-11-20","webhook_secret":"test-only-secret-b","token":"tb"},
                          "m_a":{"partition":0,"fee_bps":200,"gst_bps":1800,"cycle_days":1,
                                 "first_settlement_date":"2026-11-20","webhook_secret":"test-only-secret-a","token":"ta"}},
             "ops_token":"o","service_token":"s"}
            """.getBytes(StandardCharsets.UTF_8);

    @Test
    void eachOrderIsCreatedThenCapturedThenPaidAndOrdersRotateOverMerchants() {
        List<LoadGenerator.Merchant> merchants = LoadGenerator.merchants(BOOTSTRAP);
        assertThat(merchants).extracting(LoadGenerator.Merchant::id).containsExactly("m_a", "m_b");
        URI base = URI.create("http://127.0.0.1:8080");

        LoadGenerator.Arrival created = LoadGenerator.arrival(base, merchants, "s10", 0, NOW);
        assertThat(created.uri().toString()).isEqualTo("http://127.0.0.1:8080/v1/merchants/m_a/order-events");
        assertThat(created.headers()).containsEntry("Authorization", "Bearer ta").containsEntry("Idempotency-Key",
                "s10-0-0");
        assertThat(OrderEventCodec.decode(created.body()).type()).isEqualTo(EventType.ORDER_CREATED);

        LoadGenerator.Arrival webhook = LoadGenerator.arrival(base, merchants, "s10", 1, NOW);
        assertThat(webhook.uri().getPath()).isEqualTo("/v1/webhooks/gateway/m_a");
        assertThat(GatewayWebhookCodec.decode(webhook.body()).payload().toString()).contains("pay_s10_0", "ord_s10_0");
        assertThat(GatewayWebhookCodec.customer(webhook.body()).flatMap(c -> c.vpa()))
                .as("a valid fake contact in the scanned pattern").contains("buyer000000@okaxis");
        assertThat(WebhookSignature.verify(webhook.headers().get(WebhookSignature.HEADER), webhook.body(),
                List.of("test-only-secret-a".getBytes(StandardCharsets.US_ASCII)), NOW).valid())
                .as("signed with the merchant's secret").isTrue();

        LoadGenerator.Arrival paid = LoadGenerator.arrival(base, merchants, "s10", 2, NOW);
        assertThat(OrderEventCodec.decode(paid.body()).order().paidWith()).isPresent();
        assertThat(LoadGenerator.arrival(base, merchants, "s10", 3, NOW).merchant()).as("the next order").isEqualTo("m_b");
        assertThat(LoadGenerator.arrival(base, merchants, "s20", 0, NOW).headers().get("Idempotency-Key"))
                .as("a new label never reuses a key").isEqualTo("s20-0-0");
    }

    /** The generator stamps events with the wall clock, which has sub-millisecond digits the codecs refuse. */
    @Test
    void aWallClockTimeWithNanosecondsStillMakesValidEvents() {
        Instant precise = Instant.parse("2026-11-18T06:00:00.123456789Z");
        List<LoadGenerator.Merchant> merchants = LoadGenerator.merchants(BOOTSTRAP);
        URI base = URI.create("http://127.0.0.1:8080");
        for (long slot = 0; slot < 3; slot++) {
            LoadGenerator.Arrival a = LoadGenerator.arrival(base, merchants, "s10", slot, precise);
            if (slot == 1) {
                assertThat(GatewayWebhookCodec.decode(a.body()).occurredAt())
                        .isEqualTo(Instant.parse("2026-11-18T06:00:00.123Z"));
            } else {
                assertThat(OrderEventCodec.decode(a.body()).occurredAt())
                        .isEqualTo(Instant.parse("2026-11-18T06:00:00.123Z"));
            }
        }
    }
}
