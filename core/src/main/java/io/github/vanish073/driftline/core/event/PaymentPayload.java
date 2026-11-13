package io.github.vanish073.driftline.core.event;

import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import java.util.Objects;

/** Payload of {@code payment.pending}, {@code payment.succeeded} and {@code payment.failed}. */
public record PaymentPayload(PaymentId paymentId, OrderId orderId, Money amount) implements EventPayload {

    public PaymentPayload {
        Objects.requireNonNull(paymentId, "paymentId");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(amount, "amount");
        if (!amount.isPositive()) {
            throw new IllegalArgumentException("a payment amount must be positive");
        }
    }
}
