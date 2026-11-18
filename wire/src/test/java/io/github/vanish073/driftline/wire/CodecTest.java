package io.github.vanish073.driftline.wire;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.json.MalformedJson;
import io.github.vanish073.driftline.wire.json.StrictJson;
import io.github.vanish073.driftline.wire.order.OrderEventCodec;
import io.github.vanish073.driftline.wire.order.OrderEventMessage;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CodecTest {

    private static final Instant AT = Instant.parse("2026-11-13T06:00:00.123Z");

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void aPaymentWebhookRoundTripsToItsCanonicalForm() {
        GatewayWebhook webhook = new GatewayWebhook("evt_1", EventType.PAYMENT_SUCCEEDED, AT,
                new PaymentPayload(PaymentId.of("pay_1"), OrderId.of("ord_1"), Money.ofPaise(49_900)));
        byte[] encoded = GatewayWebhookCodec.encode(webhook);
        assertThat(new String(encoded, StandardCharsets.UTF_8)).isEqualTo(
                "{\"id\":\"evt_1\",\"type\":\"payment.succeeded\",\"occurred_at\":\"2026-11-13T06:00:00.123Z\","
                        + "\"payment\":{\"id\":\"pay_1\",\"order_id\":\"ord_1\",\"amount_paise\":49900,\"currency\":\"INR\"}}");
        assertThat(GatewayWebhookCodec.decode(encoded)).isEqualTo(webhook);
    }

    @Test
    void whitespaceFieldOrderAndUnknownFieldsDoNotChangeTheCanonicalForm() {
        String reordered = """
                { "payment": {"currency":"INR", "amount_paise": 49900, "order_id":"ord_1", "id":"pay_1", "method":"upi"},
                  "occurred_at": "2026-11-13T06:00:00.123Z", "type": "payment.succeeded", "id": "evt_1", "livemode": true }
                """;
        GatewayWebhook decoded = GatewayWebhookCodec.decode(utf8(reordered));
        assertThat(GatewayWebhookCodec.encode(decoded)).isEqualTo(GatewayWebhookCodec.encode(GatewayWebhookCodec.decode(
                GatewayWebhookCodec.encode(decoded))));
        assertThat(decoded.eventId()).isEqualTo("evt_1");
    }

    @Test
    void aRefundWebhookRoundTrips() {
        GatewayWebhook refund = new GatewayWebhook("evt_r", EventType.REFUND_PROCESSED, AT,
                new RefundPayload(PaymentId.of("pay_1"), Money.ofPaise(100)));
        assertThat(GatewayWebhookCodec.decode(GatewayWebhookCodec.encode(refund))).isEqualTo(refund);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",                                                                       // not JSON
        "[]",                                                                     // not an object
        "{\"id\":\"e\",\"id\":\"f\",\"type\":\"payment.pending\"}",              // duplicate key
        "{\"id\":\"e\",\"type\":\"payment.pending\",\"occurred_at\":\"2026-11-13T06:00:00Z\","
                + "\"payment\":{\"id\":\"p\",\"order_id\":\"o\",\"amount_paise\":499.00,\"currency\":\"INR\"}}",   // fractional amount
        "{\"id\":\"e\",\"type\":\"payment.pending\",\"occurred_at\":\"2026-11-13T06:00:00Z\","
                + "\"payment\":{\"id\":\"p\",\"order_id\":\"o\",\"amount_paise\":\"49900\",\"currency\":\"INR\"}}", // amount as string
        "{\"id\":\"e\",\"type\":\"payment.pending\",\"occurred_at\":\"2026-11-13T06:00:00Z\","
                + "\"payment\":{\"id\":\"p\",\"order_id\":\"o\",\"amount_paise\":49900,\"currency\":\"USD\"}}",    // currency
        "{\"id\":\"e\",\"type\":\"payment.pending\",\"occurred_at\":\"2026-11-13T06:00:00+05:30\","
                + "\"payment\":{\"id\":\"p\",\"order_id\":\"o\",\"amount_paise\":49900,\"currency\":\"INR\"}}",    // not UTC Z
        "{\"id\":\"e\",\"type\":\"payment.pending\",\"occurred_at\":\"2026-11-13T06:00:00.123456789Z\","
                + "\"payment\":{\"id\":\"p\",\"order_id\":\"o\",\"amount_paise\":49900,\"currency\":\"INR\"}}",    // nanoseconds
        "{\"id\":\"e\",\"type\":\"payment.pending\",\"occurred_at\":\"2026-11-13T06:00:00Z\","
                + "\"payment\":{\"id\":\"p\",\"order_id\":\"o\",\"amount_paise\":0,\"currency\":\"INR\"}}",        // zero amount
        "{\"id\":\"e\",\"type\":\"order.paid\",\"occurred_at\":\"2026-11-13T06:00:00Z\"}",                         // not a gateway type
        "{\"id\":\"e\",\"type\":\"payment.teleported\",\"occurred_at\":\"2026-11-13T06:00:00Z\"}",                 // unknown type
        "{\"id\":\"e\",\"type\":\"payment.pending\",\"occurred_at\":\"2026-11-13T06:00:00Z\"} {}",                // trailing content
        "{\"id\":\"e\",\"type\":\"payment.pending\",\"occurred_at\":\"2026-11-13T06:00:00Z\",\"x\":[1]}",          // an array
        "{\"id\":\"e\",\"type\":\"payment.pending\",\"x\":{\"y\":{\"z\":{}}}}",                         // depth 4 > 3
        "{\"id\":\"e\",\"type\":\"payment.pending\",\"occurred_at\":\"2026-11-13T06:00:00Z\","
                + "\"payment\":{\"id\":\"p\",\"order_id\":\"o\",\"amount_paise\":99999999999999999999,\"currency\":\"INR\"}}", // too big
    })
    void rejectsMalformedWebhooks(String body) {
        assertThatThrownBy(() -> GatewayWebhookCodec.decode(utf8(body))).isInstanceOf(MalformedJson.class);
    }

    /** StrictJson's own limits (docs/THREAT_MODEL.md, denial of service): depth at most 3, no arrays, no duplicates. */
    @Test
    void strictJsonRefusesDepthBeyondThreeArraysAndDuplicateKeys() {
        assertThat(StrictJson.parse(utf8("{\"a\":{\"b\":{\"c\":1}}}"))).as("depth 3 is allowed").isNotNull();
        assertThatThrownBy(() -> StrictJson.parse(utf8("{\"a\":{\"b\":{\"c\":{\"d\":1}}}}")))
                .isInstanceOf(MalformedJson.class).hasMessageContaining("nested too deeply");
        assertThatThrownBy(() -> StrictJson.parse(utf8("{\"a\":[1]}")))
                .isInstanceOf(MalformedJson.class).hasMessageContaining("unsupported JSON type");
        assertThatThrownBy(() -> StrictJson.parse(utf8("{\"a\":1,\"a\":2}")))
                .isInstanceOf(MalformedJson.class).hasMessageContaining("not valid JSON");
    }

    @Test
    void anOrderEventRoundTripsAndPaidWithFollowsTheType() {
        OrderEventMessage paid = new OrderEventMessage(EventType.ORDER_PAID, AT, new OrderPayload(OrderId.of("ord_1"), 3,
                Money.ofPaise(49_900), AT.plusSeconds(900), Optional.of(PaymentId.of("pay_1"))));
        assertThat(OrderEventCodec.decode(OrderEventCodec.encode(paid))).isEqualTo(paid);

        String createdWithPayment = "{\"type\":\"order.created\",\"occurred_at\":\"2026-11-13T06:00:00Z\",\"order\":"
                + "{\"id\":\"o\",\"version\":1,\"amount_paise\":100,\"currency\":\"INR\",\"expires_at\":\"2026-11-13T07:00:00Z\","
                + "\"paid_with\":\"pay_1\"}}";
        assertThatThrownBy(() -> OrderEventCodec.decode(utf8(createdWithPayment))).isInstanceOf(MalformedJson.class);
        String paidWithout = createdWithPayment.replace("order.created", "order.paid").replace(",\"paid_with\":\"pay_1\"", "");
        assertThatThrownBy(() -> OrderEventCodec.decode(utf8(paidWithout))).isInstanceOf(MalformedJson.class);
    }
}
