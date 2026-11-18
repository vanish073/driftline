package io.github.vanish073.driftline.wire;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.ingest.IngestNormaliser;
import io.github.vanish073.driftline.wire.ingest.IngestResult;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import io.github.vanish073.driftline.wire.webhook.CustomerContact;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The customer's contact details on a webhook (D77): validated, never in the event or its hash, never printed. */
class CustomerContactTest {

    static final Instant NOW = Instant.parse("2026-11-17T06:00:00Z");
    static final byte[] SECRET = "test-only-webhook-secret-contact-not-a-key".getBytes(StandardCharsets.US_ASCII);
    static final MerchantContext M = new MerchantContext(MerchantId.of("m_c"), 1, List.of(SECRET));

    static CustomerContact contact(String vpa, String phone, String email) {
        return CustomerContact.of(Optional.ofNullable(vpa), Optional.ofNullable(phone), Optional.ofNullable(email));
    }

    @Test
    void fieldsAreNormalisedAndAnInvalidOneIsDroppedNotFatal() {
        CustomerContact c = contact(" Buyer.One@OKAXIS ", "+91 98765-43210", "Buyer.One@Example.COM");
        assertThat(c.vpa()).contains("buyer.one@okaxis");
        assertThat(c.phone()).contains("9876543210");
        assertThat(c.email()).contains("buyer.one@example.com");
        assertThat(c.dropped()).isZero();

        CustomerContact bad = contact("no-at-sign", "12345", "buyer@example.com");
        assertThat(bad.vpa()).isEmpty();
        assertThat(bad.phone()).as("not a ten-digit mobile").isEmpty();
        assertThat(bad.email()).contains("buyer@example.com");
        assertThat(bad.dropped()).isEqualTo(2);
    }

    @Test
    void itNeverPrintsAValue() {
        String text = contact("buyer@okaxis", "9876543210", "buyer@example.com").toString();
        assertThat(text).doesNotContain("buyer").doesNotContain("9876543210").contains("present");
    }

    @Test
    void theContactTravelsBesideTheEventNeverInItOrItsHash() {
        GatewayWebhook w = new GatewayWebhook("evt_c1", EventType.PAYMENT_SUCCEEDED, NOW.minusSeconds(30),
                new PaymentPayload(PaymentId.of("pay_c1"), OrderId.of("ord_c1"), Money.ofPaise(49_900)));
        CustomerContact c = contact("buyer@okaxis", "9876543210", "buyer@example.com");
        byte[] withContact = GatewayWebhookCodec.encode(w, Optional.of(c));
        byte[] without = GatewayWebhookCodec.encode(w);
        assertThat(new String(withContact, StandardCharsets.UTF_8)).contains("\"customer\":{\"vpa\":\"buyer@okaxis\"");
        assertThat(GatewayWebhookCodec.customer(withContact)).contains(c);
        assertThat(GatewayWebhookCodec.customer(without)).isEmpty();

        IngestResult.Accepted a = (IngestResult.Accepted) IngestNormaliser.webhook(M, withContact,
                Optional.of(WebhookSignature.sign(SECRET, NOW, withContact)), NOW, Optional.empty());
        IngestResult.Accepted b = (IngestResult.Accepted) IngestNormaliser.webhook(M, without,
                Optional.of(WebhookSignature.sign(SECRET, NOW, without)), NOW, Optional.empty());
        assertThat(a.customer()).contains(c);
        assertThat(b.customer()).isEmpty();
        assertThat(a.envelope()).as("the same event, the same hash: identity and dedupe ignore personal data")
                .isEqualTo(b.envelope());
    }

    @Test
    void onlyAPaymentEventCarriesAContactAndAMalformedOneCountsAsNone() {
        GatewayWebhook refund = new GatewayWebhook("evt_r1", EventType.REFUND_PROCESSED, NOW.minusSeconds(30),
                new RefundPayload(PaymentId.of("pay_c1"), Money.ofPaise(49_900)));
        byte[] body = GatewayWebhookCodec.encode(refund, Optional.of(contact("buyer@okaxis", null, null)));
        assertThat(new String(body, StandardCharsets.UTF_8)).doesNotContain("customer");
        byte[] odd = ("{\"id\":\"evt_c2\",\"type\":\"payment.pending\",\"occurred_at\":\"2026-11-17T05:59:00Z\","
                + "\"payment\":{\"id\":\"pay_c2\",\"order_id\":\"ord_c2\",\"amount_paise\":100,\"currency\":\"INR\"},"
                + "\"customer\":\"buyer@okaxis\"}").getBytes(StandardCharsets.UTF_8);
        assertThat(GatewayWebhookCodec.customer(odd)).isEmpty();
        assertThat(GatewayWebhookCodec.decode(odd).eventId()).as("the webhook itself is still valid").isEqualTo("evt_c2");
    }
}
