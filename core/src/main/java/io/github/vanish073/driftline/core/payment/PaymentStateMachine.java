package io.github.vanish073.driftline.core.payment;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.core.state.Decision;
import java.time.Instant;
import java.util.Optional;

/**
 * Decides what one payment status event does to a payment (DESIGN §4.3, D24). Pure and stateless: the caller loads
 * the state, offers the event, and stores the returned state with the rule in {@code payment_transitions}.
 *
 * <p>Duplicates by event key never get here; the dedupe row stops them first (D3). What can get here is the same fact
 * reported again under a different key, which the table classifies as IGNORED_DUPLICATE.
 *
 * <p>Canary C06 ({@link Canary#SUCCESS_TO_FAILED}, DESIGN §11.10) plants "last event wins": a failure that arrives
 * after the success it precedes moves the payment back to FAILED. Only the simulator can switch it on.
 */
public final class PaymentStateMachine {

    private final boolean successToFailed;

    /** The production machine: no canary. */
    public PaymentStateMachine() {
        this(Canaries.NONE);
    }

    public PaymentStateMachine(Canaries canaries) {
        this.successToFailed = canaries.isEnabled(Canary.SUCCESS_TO_FAILED);
    }

    public Decision<PaymentState> decide(PaymentState current, PaymentEvidence incoming) {
        if (!current.paymentId().equals(incoming.paymentId())) {
            throw new IllegalArgumentException("evidence for payment " + incoming.paymentId()
                    + " offered to payment " + current.paymentId());
        }
        if (current.conflict()) {
            return Decision.of(PaymentRule.ALREADY_IN_CONFLICT, current);
        }
        if (current.status() != PaymentStatus.NONE
                && (!current.amount().orElseThrow().equals(incoming.amount())
                    || !current.orderId().orElseThrow().equals(incoming.orderId()))) {
            return Decision.of(PaymentRule.FACTS_MISMATCH, current.withConflict());
        }

        PaymentTransitionTable.Row row = PaymentTransitionTable.lookup(
                current.status(),
                TimeOrder.of(incoming.eventTime(), current.failedAt()),
                TimeOrder.of(incoming.eventTime(), current.successAt()),
                incoming.reported());

        if (successToFailed && row.rule() == PaymentRule.FAILURE_BEFORE_SUCCESS) {
            return Decision.of(row.rule(), new PaymentState(current.paymentId(), PaymentStatus.FAILED,
                    current.orderId(), current.amount(), Optional.of(incoming.eventTime()), Optional.empty(), false));
        }
        return switch (row.rule().kind()) {
            case APPLIED -> Decision.of(row.rule(), applied(current, incoming, row.to()));
            case IGNORED_STALE, IGNORED_DUPLICATE -> Decision.of(row.rule(), current);
            case CONFLICT -> Decision.of(row.rule(), current.withConflict());
        };
    }

    /**
     * An applied row records the incoming instant for its status. The table only applies a success that is earlier
     * than any recorded success and a failure that is later than any recorded failure, so this keeps
     * {@code successAt} the earliest success and {@code failedAt} the latest failure.
     */
    private static PaymentState applied(PaymentState current, PaymentEvidence incoming, PaymentStatus next) {
        Optional<Instant> at = Optional.of(incoming.eventTime());
        return new PaymentState(
                current.paymentId(),
                next,
                Optional.of(incoming.orderId()),
                Optional.of(incoming.amount()),
                incoming.reported() == PaymentStatus.FAILED ? at : current.failedAt(),
                incoming.reported() == PaymentStatus.SUCCEEDED ? at : current.successAt(),
                false);
    }
}
