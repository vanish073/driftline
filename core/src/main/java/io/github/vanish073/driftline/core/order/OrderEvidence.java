package io.github.vanish073.driftline.core.order;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** One merchant order event: the order's full snapshot at {@code version}. */
public record OrderEvidence(
        OrderId orderId,
        OrderStatus reported,
        long version,
        Money amount,
        Instant expiresAt,
        Optional<PaymentId> paidWith) {

    public OrderEvidence {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(reported, "reported");
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(expiresAt, "expiresAt");
        Objects.requireNonNull(paidWith, "paidWith");
        if (reported == OrderStatus.NONE) {
            throw new IllegalArgumentException("an order event reports a real state, never NONE");
        }
        if (version < 1) {
            throw new IllegalArgumentException("order versions start at 1, got " + version);
        }
        if (!amount.isPositive()) {
            throw new IllegalArgumentException("an order amount must be positive");
        }
        if (paidWith.isPresent() != reported.carriesPayment()) {
            throw new IllegalArgumentException("paidWith is present exactly for PAID and FULFILLED, not " + reported);
        }
    }

    /** Reads an {@code order.*} envelope. */
    public static OrderEvidence from(EventEnvelope envelope) {
        OrderStatus reported = switch (envelope.eventType()) {
            case ORDER_CREATED -> OrderStatus.CREATED;
            case ORDER_PENDING_PAYMENT -> OrderStatus.PENDING_PAYMENT;
            case ORDER_PAID -> OrderStatus.PAID;
            case ORDER_FULFILLED -> OrderStatus.FULFILLED;
            case ORDER_EXPIRED -> OrderStatus.EXPIRED;
            case ORDER_CANCELLED -> OrderStatus.CANCELLED;
            default -> throw new IllegalArgumentException(envelope.eventType().wireName() + " is not an order event");
        };
        OrderPayload payload = (OrderPayload) envelope.payload();
        return new OrderEvidence(payload.orderId(), reported, payload.version(), payload.amount(),
                payload.expiresAt(), payload.paidWith());
    }

    /** The order state this snapshot describes. */
    public OrderState toState() {
        return new OrderState(orderId, reported, version, Optional.of(amount), Optional.of(expiresAt), paidWith);
    }
}
