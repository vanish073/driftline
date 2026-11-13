package io.github.vanish073.driftline.core.payment;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.util.Objects;

/** One payment status event, reduced to what the payment state machine needs. */
public record PaymentEvidence(
        PaymentId paymentId, PaymentStatus reported, Instant eventTime, OrderId orderId, Money amount) {

    public PaymentEvidence {
        Objects.requireNonNull(paymentId, "paymentId");
        Objects.requireNonNull(reported, "reported");
        Objects.requireNonNull(eventTime, "eventTime");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(amount, "amount");
        if (reported == PaymentStatus.NONE) {
            throw new IllegalArgumentException("an event reports PENDING, SUCCEEDED or FAILED, never NONE");
        }
        if (!amount.isPositive()) {
            throw new IllegalArgumentException("a payment amount must be positive");
        }
    }

    /** Reads a {@code payment.pending}, {@code payment.succeeded} or {@code payment.failed} envelope. */
    public static PaymentEvidence from(EventEnvelope envelope) {
        PaymentStatus reported = switch (envelope.eventType()) {
            case PAYMENT_PENDING -> PaymentStatus.PENDING;
            case PAYMENT_SUCCEEDED -> PaymentStatus.SUCCEEDED;
            case PAYMENT_FAILED -> PaymentStatus.FAILED;
            default -> throw new IllegalArgumentException(envelope.eventType().wireName() + " is not a payment status event");
        };
        PaymentPayload payload = (PaymentPayload) envelope.payload();
        return new PaymentEvidence(payload.paymentId(), reported, envelope.eventTime(), payload.orderId(), payload.amount());
    }
}
