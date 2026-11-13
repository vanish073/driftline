package io.github.vanish073.driftline.core.event;

/** The typed payload of an {@link EventEnvelope}. Each {@link EventType} names the record it carries. */
public sealed interface EventPayload
        permits PaymentPayload, RefundPayload, OrderPayload, SettlementLinePayload, SettlementFileEndPayload {
}
