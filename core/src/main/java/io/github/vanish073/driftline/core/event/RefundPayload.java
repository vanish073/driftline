package io.github.vanish073.driftline.core.event;

import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import java.util.Objects;

/** Payload of {@code refund.processed} and {@code refund.failed}. v1 has full refunds only (DESIGN §1). */
public record RefundPayload(PaymentId paymentId, Money amount) implements EventPayload {

    public RefundPayload {
        Objects.requireNonNull(paymentId, "paymentId");
        Objects.requireNonNull(amount, "amount");
        if (!amount.isPositive()) {
            throw new IllegalArgumentException("a refund amount must be positive");
        }
    }
}
