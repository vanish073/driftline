package io.github.vanish073.driftline.core.order;

import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * The latest snapshot of a merchant order that Driftline has applied (DESIGN §4.4).
 *
 * @param version  the merchant's monotonic version; 0 only for {@link OrderStatus#NONE}
 * @param paidWith the payment the order was paid with; present exactly for PAID and FULFILLED
 */
public record OrderState(
        OrderId orderId,
        OrderStatus status,
        long version,
        Optional<Money> amount,
        Optional<Instant> expiresAt,
        Optional<PaymentId> paidWith) {

    public OrderState {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(expiresAt, "expiresAt");
        Objects.requireNonNull(paidWith, "paidWith");
        if (status == OrderStatus.NONE) {
            if (version != 0 || amount.isPresent() || expiresAt.isPresent() || paidWith.isPresent()) {
                throw new IllegalArgumentException("an order with no events has version 0 and no snapshot");
            }
        } else {
            if (version < 1) {
                throw new IllegalArgumentException("order versions start at 1, got " + version);
            }
            if (amount.isEmpty() || !amount.get().isPositive() || expiresAt.isEmpty()) {
                throw new IllegalArgumentException("a seen order has a positive amount and an expiry");
            }
            if (paidWith.isPresent() != status.carriesPayment()) {
                throw new IllegalArgumentException("paidWith is present exactly for PAID and FULFILLED, not " + status);
            }
        }
    }

    /** The state before any event: {@link OrderStatus#NONE}. */
    public static OrderState unknown(OrderId orderId) {
        return new OrderState(orderId, OrderStatus.NONE, 0, Optional.empty(), Optional.empty(), Optional.empty());
    }
}
