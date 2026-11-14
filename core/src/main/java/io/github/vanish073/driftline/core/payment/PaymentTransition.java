package io.github.vanish073.driftline.core.payment;

import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.PaymentId;
import java.time.Instant;
import java.util.Objects;

/** One recorded decision of the payment state machine: a row of {@code payment_transitions} (DESIGN §4.3). */
public record PaymentTransition(
        PaymentId paymentId, PaymentStatus from, PaymentStatus to, PaymentRule rule, String eventKey, Instant eventTime) {

    public PaymentTransition {
        Objects.requireNonNull(paymentId, "paymentId");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(rule, "rule");
        IdRules.requireKey("eventKey", eventKey);
        Objects.requireNonNull(eventTime, "eventTime");
    }
}
