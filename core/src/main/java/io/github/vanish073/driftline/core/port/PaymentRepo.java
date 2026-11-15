package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import java.util.List;
import java.util.Optional;

/** Payments and their append-only transition log. Mirrors {@code payments} and {@code payment_transitions} (§12). */
public interface PaymentRepo {

    Optional<PaymentState> find(MerchantId merchantId, PaymentId paymentId);

    /** Inserts or replaces the state of {@code state.paymentId()}. */
    void save(MerchantId merchantId, PaymentState state);

    /** Appends one decision of the payment state machine, in order (§4.3: every decision is recorded). */
    void appendTransition(MerchantId merchantId, PaymentTransition transition);

    /** The transitions of one payment, oldest first. */
    List<PaymentTransition> transitions(MerchantId merchantId, PaymentId paymentId);

    /** Every payment made for {@code orderId}, by payment id (the duplicate-capture join, §5.3, §5.6). */
    List<PaymentState> forOrder(MerchantId merchantId, OrderId orderId);

    /** The merchant's payments that have no settlement match yet: the open ones (§5.2), by payment id. */
    List<PaymentState> unsettled(MerchantId merchantId);

    /** The break that owns automatic actions for this payment, if one has claimed it (D11, §5.6). */
    Optional<String> actionOwner(MerchantId merchantId, PaymentId paymentId);

    /**
     * Makes {@code breakId} the payment's action owner if it has none yet.
     *
     * @return empty if the claim succeeded (or {@code breakId} already owned it); otherwise the existing owner
     * @throws IllegalArgumentException if the payment is not stored
     */
    Optional<String> claimActionOwner(MerchantId merchantId, PaymentId paymentId, String breakId);
}
