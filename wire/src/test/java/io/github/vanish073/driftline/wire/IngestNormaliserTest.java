package io.github.vanish073.driftline.wire;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.ingest.IngestNormaliser;
import io.github.vanish073.driftline.wire.ingest.IngestResult;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import io.github.vanish073.driftline.wire.order.OrderEventCodec;
import io.github.vanish073.driftline.wire.order.OrderEventMessage;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class IngestNormaliserTest {

    private static final byte[] SECRET = "k".repeat(32).getBytes(StandardCharsets.UTF_8);
    private static final MerchantContext M = new MerchantContext(MerchantId.of("m_001"), 3, List.of(SECRET));
    private static final Instant AT = Instant.parse("2026-11-13T06:00:00.250Z");
    private static final Instant NOW = AT.plusSeconds(2);
    private static final String TRACE = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private static byte[] webhook() {
        return GatewayWebhookCodec.encode(new GatewayWebhook("evt_1", EventType.PAYMENT_SUCCEEDED, AT,
                new PaymentPayload(PaymentId.of("pay_1"), OrderId.of("ord_1"), Money.ofPaise(49_900))));
    }

    @Test
    void aSignedWebhookBecomesAnEnvelope() {
        byte[] body = webhook();
        IngestResult result = IngestNormaliser.webhook(M, body, Optional.of(WebhookSignature.sign(SECRET, NOW, body)), NOW,
                Optional.of(TRACE));
        assertThat(result).isInstanceOf(IngestResult.Accepted.class);
        EventEnvelope e = ((IngestResult.Accepted) result).envelope();
        assertThat(e.merchantId()).isEqualTo(M.merchantId());
        assertThat(e.partition()).isEqualTo(3);
        assertThat(e.source()).isEqualTo(Source.GATEWAY);
        assertThat(e.eventKey()).isEqualTo("evt_1");
        assertThat(e.eventType()).isEqualTo(EventType.PAYMENT_SUCCEEDED);
        assertThat(e.eventTime()).isEqualTo(AT);
        assertThat(e.receivedAt()).isEqualTo(NOW);
        assertThat(e.traceparent()).contains(TRACE);
    }

    @Test
    void theSignatureIsCheckedBeforeTheBodyIsParsed() {
        byte[] garbage = "not json at all".getBytes(StandardCharsets.UTF_8);
        IngestResult unsigned = IngestNormaliser.webhook(M, garbage, Optional.empty(), NOW, Optional.empty());
        assertThat(unsigned).isEqualTo(new IngestResult.Rejected(401, "signature MISSING"));
        IngestResult signedGarbage = IngestNormaliser.webhook(M, garbage,
                Optional.of(WebhookSignature.sign(SECRET, NOW, garbage)), NOW, Optional.empty());
        assertThat(signedGarbage).isInstanceOfSatisfying(IngestResult.Rejected.class, r -> assertThat(r.status()).isEqualTo(400));
    }

    @Test
    void aBadOrStaleSignatureIs401() {
        byte[] body = webhook();
        String stale = WebhookSignature.sign(SECRET, NOW.minusSeconds(600), body);
        assertThat(IngestNormaliser.webhook(M, body, Optional.of(stale), NOW, Optional.empty()))
                .isEqualTo(new IngestResult.Rejected(401, "signature OUTSIDE_REPLAY_WINDOW"));
        String wrong = WebhookSignature.sign("x".repeat(32).getBytes(StandardCharsets.UTF_8), NOW, body);
        assertThat(IngestNormaliser.webhook(M, body, Optional.of(wrong), NOW, Optional.empty()))
                .isEqualTo(new IngestResult.Rejected(401, "signature NO_MATCH"));
    }

    @Test
    void retriesSignedAtDifferentTimesGiveTheSameEnvelopeContent() {
        byte[] body = webhook();
        EventEnvelope first = ((IngestResult.Accepted) IngestNormaliser.webhook(M, body,
                Optional.of(WebhookSignature.sign(SECRET, NOW, body)), NOW, Optional.empty())).envelope();
        EventEnvelope retry = ((IngestResult.Accepted) IngestNormaliser.webhook(M, body,
                Optional.of(WebhookSignature.sign(SECRET, NOW.plusSeconds(30), body)), NOW.plusSeconds(30),
                Optional.empty())).envelope();
        assertThat(retry.eventKey()).isEqualTo(first.eventKey());
        assertThat(retry.payloadSha256()).isEqualTo(first.payloadSha256());
    }

    @Test
    void orderEventsNeedAnIdempotencyKeyAndDropAnInvalidTraceparent() {
        byte[] body = OrderEventCodec.encode(new OrderEventMessage(EventType.ORDER_CREATED, AT,
                new OrderPayload(OrderId.of("ord_1"), 1, Money.ofPaise(49_900), AT.plusSeconds(900), Optional.empty())));
        assertThat(IngestNormaliser.orderEvent(M, body, Optional.empty(), NOW, Optional.empty()))
                .isEqualTo(new IngestResult.Rejected(400, "the Idempotency-Key header is required"));
        IngestResult ok = IngestNormaliser.orderEvent(M, body, Optional.of("idem-1"), NOW, Optional.of("garbage"));
        EventEnvelope e = ((IngestResult.Accepted) ok).envelope();
        assertThat(e.source()).isEqualTo(Source.MERCHANT);
        assertThat(e.eventKey()).isEqualTo("idem-1");
        assertThat(e.traceparent()).isEmpty();
        assertThat(IngestNormaliser.orderEvent(M, body, Optional.of("bad key"), NOW, Optional.empty()))
                .isInstanceOfSatisfying(IngestResult.Rejected.class, r -> assertThat(r.status()).isEqualTo(400));
    }
}
