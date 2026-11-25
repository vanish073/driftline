package io.github.vanish073.driftline.core.event;

import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.AmountLimits;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Payload of every {@code order.*} event: a full snapshot of the order at {@code version}. Because each event is
 * a snapshot, an order first seen through a later event (for example {@code order.paid} before
 * {@code order.created}) is still complete. {@code paidWith} is present exactly for {@code order.paid} and
 * {@code order.fulfilled}; {@link EventEnvelope} checks that against the event type.
 */
public record OrderPayload(OrderId orderId, long version, Money amount, Instant expiresAt, Optional<PaymentId> paidWith)
        implements EventPayload {

    public OrderPayload {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(expiresAt, "expiresAt");
        Objects.requireNonNull(paidWith, "paidWith");
        if (version < 1) {
            throw new IllegalArgumentException("order versions start at 1, got " + version);
        }
        if (!amount.isPositive()) {
            throw new IllegalArgumentException("an order amount must be positive");
        }
        AmountLimits.requirePayment("an order amount", amount);
    }
}
