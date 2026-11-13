package io.github.vanish073.driftline.core.payment;

import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * What Driftline knows about one payment's gateway status (DESIGN §4.3, D24).
 *
 * <p>Outside a conflict, the state is a function of the <em>set</em> of events seen, not of their order:
 * {@code successAt} is the earliest success instant seen, {@code failedAt} the latest failure instant, and
 * {@link #lateSuccess()} holds when a failure preceded the success. The constructor enforces the shape of each
 * status, including {@code failedAt < successAt}.
 *
 * @param orderId   the order the payment was made for; empty only for {@link PaymentStatus#NONE}
 * @param amount    the payment amount; empty only for {@link PaymentStatus#NONE}
 * @param failedAt  the latest failure instant seen, if any
 * @param successAt the earliest success instant seen, if any; the first debit evidence that fixes T
 * @param conflict  set once the records contradicted each other; the payment is then frozen (D14, D24)
 */
public record PaymentState(
        PaymentId paymentId,
        PaymentStatus status,
        Optional<OrderId> orderId,
        Optional<Money> amount,
        Optional<Instant> failedAt,
        Optional<Instant> successAt,
        boolean conflict) {

    public PaymentState {
        Objects.requireNonNull(paymentId, "paymentId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(failedAt, "failedAt");
        Objects.requireNonNull(successAt, "successAt");
        switch (status) {
            case NONE -> require(orderId.isEmpty() && amount.isEmpty() && failedAt.isEmpty() && successAt.isEmpty()
                    && !conflict, "a payment with no events has no facts and no conflict");
            case PENDING -> require(failedAt.isEmpty() && successAt.isEmpty(),
                    "a PENDING payment has no success or failure recorded");
            case FAILED -> require(failedAt.isPresent() && successAt.isEmpty(),
                    "a FAILED payment has a failure and no success recorded");
            case SUCCEEDED -> require(successAt.isPresent(), "a SUCCEEDED payment has a success recorded");
        }
        if (status != PaymentStatus.NONE) {
            require(orderId.isPresent() && amount.isPresent() && amount.get().isPositive(),
                    "a seen payment has an order id and a positive amount");
        }
        if (failedAt.isPresent() && successAt.isPresent()) {
            require(failedAt.get().isBefore(successAt.get()), "a recorded failure must come before the success");
        }
    }

    /** The state before any event: {@link PaymentStatus#NONE}. */
    public static PaymentState unknown(PaymentId paymentId) {
        return new PaymentState(paymentId, PaymentStatus.NONE, Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), false);
    }

    /** True when a failure preceded the success: the LATE_SUCCESS fact, whichever order the events arrived in. */
    public boolean lateSuccess() {
        return failedAt.isPresent() && successAt.isPresent();
    }

    PaymentState withConflict() {
        return new PaymentState(paymentId, status, orderId, amount, failedAt, successAt, true);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
