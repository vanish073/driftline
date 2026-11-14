package io.github.vanish073.driftline.wire.ingest;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.order.OrderEventCodec;
import io.github.vanish073.driftline.wire.order.OrderEventMessage;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
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
            byte[] canonical = GatewayWebhookCodec.encode(webhook);
            return new IngestResult.Accepted(new EventEnvelope(EventEnvelope.SCHEMA_VERSION, merchant.merchantId(),
                    merchant.partition(), Source.GATEWAY, webhook.eventId(), webhook.type(), webhook.occurredAt(),
                    receivedAt, Sha256.of(canonical), webhook.payload(), validTraceparent(traceparent)));
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

    private static Optional<String> validTraceparent(Optional<String> traceparent) {
        return traceparent.filter(EventEnvelope::isValidTraceparent);
    }
}
