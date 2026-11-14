package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.id.MerchantId;
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
}
