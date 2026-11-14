package io.github.vanish073.driftline.wire.webhook;

import io.github.vanish073.driftline.core.event.EventPayload;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.id.IdRules;
import java.time.Instant;
import java.util.Objects;

/**
 * One gateway webhook, Driftline Gateway Webhook v1 (cited against public gateway documentation in
 * {@code docs/formats.md}, Shift 5). The event id is the idempotency key; every retry of an event carries the same id.
 */
public record GatewayWebhook(String eventId, EventType type, Instant occurredAt, EventPayload payload) {

    public GatewayWebhook {
        IdRules.requireKey("eventId", eventId);
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(payload, "payload");
        boolean payment = type == EventType.PAYMENT_PENDING || type == EventType.PAYMENT_SUCCEEDED
                || type == EventType.PAYMENT_FAILED;
        boolean refund = type == EventType.REFUND_PROCESSED || type == EventType.REFUND_FAILED;
        if (!(payment && payload instanceof PaymentPayload) && !(refund && payload instanceof RefundPayload)) {
            throw new IllegalArgumentException(type.wireName() + " is not a gateway webhook with this payload");
        }
    }
}
