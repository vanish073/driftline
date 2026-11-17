package io.github.vanish073.driftline.wire.ingest;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.order.OrderEventCodec;
import io.github.vanish073.driftline.wire.order.OrderEventMessage;
import io.github.vanish073.driftline.wire.webhook.CustomerContact;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;

/**
 * The ingest normaliser (DESIGN §3.1, §10): a pure function from a raw request to an {@link EventEnvelope} or a
 * rejection. The Spring controllers (Shift 6) and the simulator's IngestNode run this same code.
 *
 * <p>Ingest is stateless (D3): it never dedupes. {@code payloadSha256} is the hash of the canonical re-encoding, so
 * whitespace or field order cannot make one event look like two, while a key reused with a different body can be
 * told apart downstream.
 */
public final class IngestNormaliser {

    private IngestNormaliser() {
    }

    /** {@code POST /v1/webhooks/gateway/{merchantId}}: verify the signature on the raw bytes, then parse. */
    public static IngestResult webhook(MerchantContext merchant, byte[] body, Optional<String> signatureHeader,
                                       Instant receivedAt, Optional<String> traceparent) {
        WebhookSignature.Verification verification = WebhookSignature.verify(
                signatureHeader.orElse(null), body, merchant.webhookSecrets(), receivedAt);
        if (!verification.valid()) {
            WebhookSignature.Failure failure = verification.failure().orElseThrow();
            return new IngestResult.Rejected(failure == WebhookSignature.Failure.TOO_LARGE ? 413 : 401,
                    "signature " + failure);
        }
        try {
            GatewayWebhook webhook = GatewayWebhookCodec.decode(body);
            byte[] canonical = GatewayWebhookCodec.encode(webhook);   // without the customer: never hashed (D77)
            Optional<CustomerContact> customer = webhook.payload() instanceof PaymentPayload
                    ? GatewayWebhookCodec.customer(body) : Optional.empty();
            return new IngestResult.Accepted(new EventEnvelope(EventEnvelope.SCHEMA_VERSION, merchant.merchantId(),
                    merchant.partition(), Source.GATEWAY, webhook.eventId(), webhook.type(), webhook.occurredAt(),
                    receivedAt, Sha256.of(canonical), webhook.payload(), validTraceparent(traceparent)), customer);
        } catch (IllegalArgumentException e) {   // includes MalformedJson
            return new IngestResult.Rejected(400, e.getMessage());
        }
    }

    /** {@code POST /v1/merchants/{merchantId}/order-events}: the {@code Idempotency-Key} header is required (§10.3). */
    public static IngestResult orderEvent(MerchantContext merchant, byte[] body, Optional<String> idempotencyKey,
                                          Instant receivedAt, Optional<String> traceparent) {
        if (body.length > WebhookSignature.MAX_BODY_BYTES) {
            return new IngestResult.Rejected(413, "body larger than " + WebhookSignature.MAX_BODY_BYTES + " bytes");
        }
        if (idempotencyKey.isEmpty()) {
            return new IngestResult.Rejected(400, "the Idempotency-Key header is required");
        }
        try {
            String key = IdRules.requireKey("Idempotency-Key", idempotencyKey.get());
            OrderEventMessage message = OrderEventCodec.decode(body);
            byte[] canonical = OrderEventCodec.encode(message);
            return new IngestResult.Accepted(new EventEnvelope(EventEnvelope.SCHEMA_VERSION, merchant.merchantId(),
                    merchant.partition(), Source.MERCHANT, key, message.type(), message.occurredAt(), receivedAt,
                    Sha256.of(canonical), message.order(), validTraceparent(traceparent)));
        } catch (IllegalArgumentException e) {   // includes MalformedJson
            return new IngestResult.Rejected(400, e.getMessage());
        }
    }

    /**
     * {@code POST /internal/v1/observations}: a fact the dispatcher learned by asking (D10), already typed, so there is
     * no signature to check; it enters as a GATEWAY event with its derived key. {@code payloadSha256} hashes a canonical
     * text of the observation, so a repeat is a DUPLICATE and never a reused key.
     */
    public static IngestResult observation(MerchantContext merchant, CallRequest.Observation o, Instant receivedAt) {
        StringBuilder canonical = new StringBuilder().append(o.eventType().wireName()).append('|').append(o.eventKey())
                .append('|').append(o.eventTime()).append('|');
        switch (o.payload()) {
            case PaymentPayload p -> canonical.append(p.paymentId().value()).append('|').append(p.orderId().value())
                    .append('|').append(p.amount().paise());
            case RefundPayload r -> canonical.append(r.paymentId().value()).append('|').append(r.amount().paise());
            default -> {
                return new IngestResult.Rejected(400, "an observation carries a payment or a refund");
            }
        }
        if (!o.eventType().allows(Source.GATEWAY)) {
            return new IngestResult.Rejected(400, o.eventType().wireName() + " is not a gateway event");
        }
        return new IngestResult.Accepted(new EventEnvelope(EventEnvelope.SCHEMA_VERSION, merchant.merchantId(),
                merchant.partition(), Source.GATEWAY, o.eventKey(), o.eventType(), o.eventTime(), receivedAt,
                Sha256.of(canonical.toString().getBytes(StandardCharsets.UTF_8)), o.payload(), Optional.empty()));
    }

    private static Optional<String> validTraceparent(Optional<String> traceparent) {
        return traceparent.filter(EventEnvelope::isValidTraceparent);
    }
}
