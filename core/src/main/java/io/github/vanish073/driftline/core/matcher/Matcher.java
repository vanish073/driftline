package io.github.vanish073.driftline.core.matcher;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.ledger.PostingRules;
import io.github.vanish073.driftline.core.order.OrderEvidence;
import io.github.vanish073.driftline.core.order.OrderRule;
import io.github.vanish073.driftline.core.order.OrderState;
import io.github.vanish073.driftline.core.order.OrderStateMachine;
import io.github.vanish073.driftline.core.order.OrderTransition;
import io.github.vanish073.driftline.core.payment.PaymentEvidence;
import io.github.vanish073.driftline.core.payment.PaymentRule;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentStateMachine;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import io.github.vanish073.driftline.core.port.DedupeRepo;
import io.github.vanish073.driftline.core.port.QuarantineRepo;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.state.Decision;
import io.github.vanish073.driftline.core.state.DecisionKind;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The matcher, first cut (Shift 3; DESIGN §5). Sans-I/O: the driver hands it one {@link PartitionBatch} and the
 * transaction to run it in. For each record it claims the dedupe row, applies the payment or order state machine,
 * records the decision, and posts CAPTURE and ORDER_MATCH; then it commits the offset under the batch's partition
 * epoch, all in the same transaction, so a crash before commit replays the batch with no effect (D2, D3) and a
 * fenced commit rolls the whole batch back (§5.1). A key reused with a different payload is quarantined in the same
 * transaction (§10.3).
 *
 * <p>Canary C01 ({@link Canary#DEDUPE_OFF}) skips the dedupe claim; canary C06 lives in {@link PaymentStateMachine}.
 *
 * <p>Not here yet: watermarks, timers, break detection (Shift 5), settlement (Shifts 5 and 7), refunds (Shift 8).
 */
public final class Matcher {

    private final PaymentStateMachine payments;
    private final OrderStateMachine orders = new OrderStateMachine();
    private final boolean dedupeOff;

    /** The production matcher: no canary. */
    public Matcher() {
        this(Canaries.NONE);
    }

    public Matcher(Canaries canaries) {
        this.payments = new PaymentStateMachine(canaries);
        this.dedupeOff = canaries.isEnabled(Canary.DEDUPE_OFF);
    }

    public BatchOutcome process(PartitionBatch batch, Tx tx) {
        int applied = 0;
        int alreadyProcessed = 0;
        int duplicates = 0;
        int keyReused = 0;
        int notYetHandled = 0;
        Postings postings = new Postings(tx);
        long next = tx.partitions().nextOffset(batch.partition());
        long committed = next;

        for (LogRecord record : batch.records()) {
            if (record.offset() < next) {
                alreadyProcessed++;
                continue;
            }
            if (record.offset() != committed) {
                throw new IllegalArgumentException("batch skips offsets: expected " + committed + ", got " + record.offset());
            }
            committed = record.offset() + 1;
            EventEnvelope e = record.envelope();
            DedupeRepo.Claim claim = dedupeOff ? DedupeRepo.Claim.FIRST : tx.dedupe().claim(
                    e.merchantId(), e.source(), e.eventKey(), e.payloadSha256(), record.partition(), record.offset());
            switch (claim) {
                case DUPLICATE -> duplicates++;
                case KEY_REUSED -> {
                    tx.quarantine().add(new QuarantineRepo.Entry(record.partition(), record.offset(), e.merchantId(),
                            e.source(), e.eventKey(), e.payloadSha256(), QuarantineRepo.Reason.IDEMPOTENCY_CONFLICT));
                    keyReused++;
                }
                case FIRST -> {
                    switch (e.eventType()) {
                        case PAYMENT_PENDING, PAYMENT_SUCCEEDED, PAYMENT_FAILED -> {
                            applyPayment(tx, e, postings);
                            applied++;
                        }
                        case ORDER_CREATED, ORDER_PENDING_PAYMENT, ORDER_PAID, ORDER_FULFILLED, ORDER_EXPIRED,
                             ORDER_CANCELLED -> {
                            applyOrder(tx, e, postings);
                            applied++;
                        }
                        case REFUND_PROCESSED, REFUND_FAILED, SETTLEMENT_LINE, SETTLEMENT_FILE_END -> notYetHandled++;
                    }
                }
            }
        }
        if (committed > next) {
            tx.partitions().commitOffset(batch.partition(), batch.epoch(), committed);
        }
        return new BatchOutcome(applied, alreadyProcessed, duplicates, keyReused, notYetHandled,
                postings.posted, postings.conflicts);
    }

    private void applyPayment(Tx tx, EventEnvelope e, Postings postings) {
        MerchantId m = e.merchantId();
        PaymentEvidence evidence = PaymentEvidence.from(e);
        PaymentState before = tx.payments().find(m, evidence.paymentId()).orElse(PaymentState.unknown(evidence.paymentId()));
        Decision<PaymentState> d = payments.decide(before, evidence);
        PaymentState after = d.state();
        tx.payments().save(m, after);
        tx.payments().appendTransition(m, new PaymentTransition(evidence.paymentId(), before.status(), after.status(),
                (PaymentRule) d.rule(), e.eventKey(), e.eventTime()));
        if (d.kind() == DecisionKind.APPLIED && after.status() == PaymentStatus.SUCCEEDED) {
            postings.post(PostingRules.capture(m, after.paymentId(), after.amount().orElseThrow(), after.successAt().orElseThrow()));
            Optional<OrderState> order = tx.orders().find(m, after.orderId().orElseThrow());
            if (order.isPresent()) {
                matchIfPaid(m, order.get(), after, postings, e.eventTime());
            }
        }
    }

    private void applyOrder(Tx tx, EventEnvelope e, Postings postings) {
        MerchantId m = e.merchantId();
        OrderEvidence evidence = OrderEvidence.from(e);
        OrderState before = tx.orders().find(m, evidence.orderId()).orElse(OrderState.unknown(evidence.orderId()));
        Decision<OrderState> d = orders.decide(before, evidence);
        OrderState after = d.state();
        tx.orders().save(m, after);
        tx.orders().appendTransition(m, new OrderTransition(evidence.orderId(), before.status(), after.status(),
                evidence.version(), (OrderRule) d.rule(), e.eventKey(),
                e.eventTime()));
        if (d.kind() == DecisionKind.APPLIED && after.paidWith().isPresent()) {
            Optional<PaymentState> payment = tx.payments().find(m, after.paidWith().get());
            if (payment.isPresent()) {
                matchIfPaid(m, after, payment.get(), postings, e.eventTime());
            }
        }
    }

    /**
     * ORDER_MATCH once both sides are known: the order is paid with this payment, the payment succeeded without a
     * conflict, and the amounts agree. Whichever side arrives second triggers it; the ledger's cause uniqueness
     * absorbs the repeat when a later event re-checks (§7.4).
     */
    private static void matchIfPaid(MerchantId m, OrderState order, PaymentState payment, Postings postings, Instant at) {
        PaymentId paymentId = payment.paymentId();
        boolean paidWithThis = order.paidWith().filter(paymentId::equals).isPresent();
        boolean succeeded = payment.status() == PaymentStatus.SUCCEEDED && !payment.conflict();
        boolean sameAmount = order.amount().equals(payment.amount());
        if (paidWithThis && succeeded && sameAmount) {
            postings.post(PostingRules.orderMatch(m, paymentId, payment.amount().orElseThrow(), at));
        }
    }

    /** Collects posting results for the outcome. */
    private static final class Postings {
        private final Tx tx;
        private int posted;
        private final List<PostResult.LedgerConflict> conflicts = new ArrayList<>();

        Postings(Tx tx) {
            this.tx = tx;
        }

        void post(LedgerTxn txn) {
            PostResult result = tx.ledger().post(txn);
            if (result instanceof PostResult.Posted) {
                posted++;
            } else if (result instanceof PostResult.LedgerConflict conflict) {
                conflicts.add(conflict);
            }
        }
    }
}
