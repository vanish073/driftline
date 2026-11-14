package io.github.vanish073.driftline.wire.order;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import java.time.Instant;
import java.util.Objects;

/** One merchant order event: the order's snapshot at a version (DESIGN §4.4). The key travels in a header. */
public record OrderEventMessage(EventType type, Instant occurredAt, OrderPayload order) {

    public OrderEventMessage {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(order, "order");
        if (type.payloadType() != OrderPayload.class) {
            throw new IllegalArgumentException(type.wireName() + " is not an order event");
        }
        if (order.paidWith().isPresent() != type.carriesPaidPayment()) {
            throw new IllegalArgumentException("paid_with is present exactly for order.paid and order.fulfilled");
        }
    }
}
