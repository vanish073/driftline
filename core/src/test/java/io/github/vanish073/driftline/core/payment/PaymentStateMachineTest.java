package io.github.vanish073.driftline.core.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.state.Decision;
import io.github.vanish073.driftline.core.state.DecisionKind;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The guards that run before the table, the evidence mapping, and the worked examples behind D24. */
class PaymentStateMachineTest {

    private static final Instant T0 = Instant.parse("2026-11-13T06:00:00Z");
    private static final PaymentId PAYMENT = PaymentId.of("pay_1");
    private static final OrderId ORDER = OrderId.of("ord_1");
    private static final Money AMOUNT = Money.ofPaise(49_900);

    private final PaymentStateMachine machine = new PaymentStateMachine();

    private static PaymentEvidence event(PaymentStatus status, long seconds) {
        return new PaymentEvidence(PAYMENT, status, T0.plusSeconds(seconds), ORDER, AMOUNT);
    }

    private PaymentState play(PaymentEvidence... events) {
        PaymentState state = PaymentState.unknown(PAYMENT);
        for (PaymentEvidence e : events) {
            state = machine.decide(state, e).state();
        }
        return state;
    }

    @Test
    void aDifferentAmountIsAConflictThatFreezesThePayment() {
        PaymentState succeeded = play(event(PaymentStatus.SUCCEEDED, 20));
        Decision<PaymentState> mismatch = machine.decide(succeeded,
                new PaymentEvidence(PAYMENT, PaymentStatus.SUCCEEDED, T0.plusSeconds(20), ORDER, Money.ofPaise(50_000)));
        assertThat(mismatch.rule()).isEqualTo(PaymentRule.FACTS_MISMATCH);
        assertThat(mismatch.state().conflict()).isTrue();
        assertThat(mismatch.state().status()).isEqualTo(PaymentStatus.SUCCEEDED);

        Decision<PaymentState> after = machine.decide(mismatch.state(), event(PaymentStatus.FAILED, 5));
        assertThat(after.kind()).isEqualTo(DecisionKind.CONFLICT);
        assertThat(after.rule()).isEqualTo(PaymentRule.ALREADY_IN_CONFLICT);
        assertThat(after.state()).isEqualTo(mismatch.state());
    }

    @Test
    void aDifferentOrderIsAConflictToo() {
        PaymentState pending = play(event(PaymentStatus.PENDING, 1));
        Decision<PaymentState> mismatch = machine.decide(pending,
                new PaymentEvidence(PAYMENT, PaymentStatus.SUCCEEDED, T0.plusSeconds(2), OrderId.of("ord_2"), AMOUNT));
        assertThat(mismatch.rule()).isEqualTo(PaymentRule.FACTS_MISMATCH);
        assertThat(mismatch.state().status()).isEqualTo(PaymentStatus.PENDING);
    }

    @Test
    void rejectsEvidenceForAnotherPayment() {
        assertThatThrownBy(() -> machine.decide(PaymentState.unknown(PaymentId.of("pay_2")), event(PaymentStatus.PENDING, 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** D24 (a): the §4.3 table, read literally, sets lateSuccess only when the failure arrives first. */
    @Test
    void lateSuccessDoesNotDependOnArrivalOrder() {
        PaymentState inTimeOrder = play(event(PaymentStatus.FAILED, 1), event(PaymentStatus.SUCCEEDED, 3));
        PaymentState reversed = play(event(PaymentStatus.SUCCEEDED, 3), event(PaymentStatus.FAILED, 1));
        assertThat(inTimeOrder.lateSuccess()).isTrue();
        assertThat(reversed).isEqualTo(inTimeOrder);
    }

    /** D24 (b): with failedAt kept as the first failure seen, this contradiction slipped through in one order. */
    @Test
    void aFailureAfterTheSuccessIsCaughtInEveryOrder() {
        PaymentEvidence f1 = event(PaymentStatus.FAILED, 1);
        PaymentEvidence f5 = event(PaymentStatus.FAILED, 5);
        PaymentEvidence s3 = event(PaymentStatus.SUCCEEDED, 3);
        assertThat(play(f1, f5, s3).conflict()).isTrue();
        assertThat(play(f1, s3, f5).conflict()).isTrue();
        assertThat(play(f5, f1, s3).conflict()).isTrue();
        assertThat(play(s3, f1, f5).conflict()).isTrue();
    }

    /** Canary C06 (DESIGN §11.10): "last event wins" turns a reordered late success back into a failure. */
    @Test
    void canaryC06MovesASucceededPaymentBackToFailed() {
        PaymentStateMachine planted = new PaymentStateMachine(Canaries.of(Canary.SUCCESS_TO_FAILED));
        PaymentState succeeded = planted.decide(PaymentState.unknown(PAYMENT), event(PaymentStatus.SUCCEEDED, 3)).state();
        Decision<PaymentState> d = planted.decide(succeeded, event(PaymentStatus.FAILED, 1));
        assertThat(d.rule()).isEqualTo(PaymentRule.FAILURE_BEFORE_SUCCESS);
        assertThat(d.state().status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(PaymentTransitionTable.rows()).noneMatch(r -> r.from() == PaymentStatus.SUCCEEDED
                && r.to() == PaymentStatus.FAILED);
        assertThat(machine.decide(succeeded, event(PaymentStatus.FAILED, 1)).state().status())
                .as("without the canary").isEqualTo(PaymentStatus.SUCCEEDED);
    }

    /** D24 (c): T is fixed by the earliest success evidence, whichever report arrives first. */
    @Test
    void theEarliestSuccessFixesT() {
        PaymentState a = play(event(PaymentStatus.SUCCEEDED, 9), event(PaymentStatus.SUCCEEDED, 4));
        PaymentState b = play(event(PaymentStatus.SUCCEEDED, 4), event(PaymentStatus.SUCCEEDED, 9));
        assertThat(a).isEqualTo(b);
        assertThat(a.successAt()).contains(T0.plusSeconds(4));
    }

    @Test
    void stateShapesAreEnforced() {
        assertThatThrownBy(() -> new PaymentState(PAYMENT, PaymentStatus.SUCCEEDED, Optional.of(ORDER), Optional.of(AMOUNT),
                Optional.of(T0.plusSeconds(20)), Optional.of(T0.plusSeconds(20)), false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PaymentState(PAYMENT, PaymentStatus.FAILED, Optional.of(ORDER), Optional.of(AMOUNT),
                Optional.empty(), Optional.empty(), false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PaymentEvidence(PAYMENT, PaymentStatus.NONE, T0, ORDER, AMOUNT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readsEvidenceFromAnEnvelope() {
        EventEnvelope envelope = new EventEnvelope(1, MerchantId.of("m_001"), 0, Source.GATEWAY, "evt_1",
                EventType.PAYMENT_FAILED, T0, T0, Sha256.of("x".getBytes(StandardCharsets.UTF_8)),
                new PaymentPayload(PAYMENT, ORDER, AMOUNT), Optional.empty());
        assertThat(PaymentEvidence.from(envelope)).isEqualTo(new PaymentEvidence(PAYMENT, PaymentStatus.FAILED, T0, ORDER, AMOUNT));

        EventEnvelope order = new EventEnvelope(1, MerchantId.of("m_001"), 0, Source.MERCHANT, "idem_1",
                EventType.ORDER_CREATED, T0, T0, Sha256.of("y".getBytes(StandardCharsets.UTF_8)),
                new OrderPayload(ORDER, 1, AMOUNT, T0.plusSeconds(900), Optional.empty()), Optional.empty());
        assertThatThrownBy(() -> PaymentEvidence.from(order)).isInstanceOf(IllegalArgumentException.class);
    }
}
