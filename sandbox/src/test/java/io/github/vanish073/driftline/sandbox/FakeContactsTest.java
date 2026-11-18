package io.github.vanish073.driftline.sandbox;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.webhook.CustomerContact;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class FakeContactsTest {

    static final Instant T = Instant.parse("2026-11-17T06:00:00Z");

    @Test
    void aPaymentWebhookGainsAValidContactThatDependsOnlyOnThePayment() {
        GatewayWebhook w = new GatewayWebhook("evt_1", EventType.PAYMENT_SUCCEEDED, T,
                new PaymentPayload(PaymentId.of("pay_1"), OrderId.of("ord_1"), Money.ofPaise(100)));
        byte[] body = FakeContacts.addTo(GatewayWebhookCodec.encode(w));
        CustomerContact c = GatewayWebhookCodec.customer(body).orElseThrow();
        assertThat(c.dropped()).as("every field validates").isZero();
        assertThat(c.vpa().orElseThrow()).matches("buyer[0-9]{6}@okaxis");
        assertThat(c.phone().orElseThrow()).matches("6000[0-9]{6}");
        assertThat(c.email().orElseThrow()).matches("buyer[0-9]{6}@example\\.com");
        assertThat(FakeContacts.addTo(GatewayWebhookCodec.encode(w))).as("a retry carries the same contact")
                .isEqualTo(body);
        assertThat(GatewayWebhookCodec.decode(body)).as("the event itself is unchanged").isEqualTo(w);
        assertThat(FakeContacts.of(PaymentId.of("pay_2"))).isNotEqualTo(c);
    }

    @Test
    void aRefundWebhookIsLeftAsItIs() {
        byte[] body = GatewayWebhookCodec.encode(new GatewayWebhook("evt_2", EventType.REFUND_PROCESSED, T,
                new RefundPayload(PaymentId.of("pay_1"), Money.ofPaise(100))));
        assertThat(FakeContacts.addTo(body)).isEqualTo(body);
        assertThat(GatewayWebhookCodec.customer(body)).isEqualTo(Optional.empty());
    }
}
