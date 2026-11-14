package io.github.vanish073.driftline.core.order;

import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.OrderId;
import java.time.Instant;
import java.util.Objects;

/** One recorded decision of the order state machine: a row of {@code order_transitions} (DESIGN §4.4). */
public record OrderTransition(
        OrderId orderId, OrderStatus from, OrderStatus to, long version, OrderRule rule, String eventKey, Instant eventTime) {

    public OrderTransition {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(rule, "rule");
        IdRules.requireKey("eventKey", eventKey);
        Objects.requireNonNull(eventTime, "eventTime");
        if (version < 1) {
            throw new IllegalArgumentException("order versions start at 1, got " + version);
        }
    }
}
