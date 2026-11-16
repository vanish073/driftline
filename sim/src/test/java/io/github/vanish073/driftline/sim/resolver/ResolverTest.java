package io.github.vanish073.driftline.sim.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.breaks.DeadlineRule;
import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventPayload;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.matcher.BatchOutcome;
import io.github.vanish073.driftline.core.matcher.LogRecord;
import io.github.vanish073.driftline.core.matcher.Matcher;
import io.github.vanish073.driftline.core.matcher.MerchantDirectory;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.matcher.PartitionBatch;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.BreakRepo.Plan;
import io.github.vanish073.driftline.core.port.OutboxRepo.Command;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandKind;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandStatus;
import io.github.vanish073.driftline.core.resolver.Resolver;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The resolver's playbooks (DESIGN §8.2) against the real matcher and the in-memory store. The test plays the
 * dispatcher: it answers the commands the resolver writes, as the dispatcher records results (as facts), and wakes the
 * break.
 */
class ResolverTest {

    static final MerchantId M = MerchantId.of("m_0");
    static final Money AMOUNT = Money.ofPaise(49_900);
    /** 11:30 IST on 2026-11-13. */
    static final Instant T0 = Instant.parse("2026-11-13T06:00:00Z");
    static final PaymentId P1 = PaymentId.of("pay_1");
    static final PaymentId P2 = PaymentId.of("pay_2");
    static final OrderId O1 = OrderId.of("ord_1");
    static final MerchantDirectory DIRECTORY = new MerchantDirectory(List.of(
            new MerchantTerms(M, 0, new FeeSchedule(200, 1_800), 1, LocalDate.parse("2026-11-14"))));

    final InMemoryStore store = new InMemoryStore();
    final List<LogRecord> log = new ArrayList<>();
    final Matcher matcher = new Matcher(DIRECTORY);
    final long epoch = store.transact("assign", tx -> tx.partitions().acquire(0, "matcher-a.1")).epoch();
    Resolver resolver = new Resolver();
    int pendingFrom;

    // ---- the world, through the matcher ----

    void append(Source source, EventType type, Instant at, EventPayload payload) {
        String key = "k" + log.size();
        log.add(new LogRecord(0, log.size(), new EventEnvelope(1, M, 0, source, key, type, at, at,
                Sha256.of(key.getBytes(StandardCharsets.UTF_8)), payload, Optional.empty())));
    }

    void pay(PaymentId p, EventType type, Instant at) {
        append(Source.GATEWAY, type, at, new PaymentPayload(p, O1, AMOUNT));
    }

    void order(EventType type, long version, Instant at, PaymentId paidWith) {
        Optional<PaymentId> paid = type.carriesPaidPayment() ? Optional.of(paidWith) : Optional.empty();
        append(Source.MERCHANT, type, at, new OrderPayload(O1, version, AMOUNT, T0.plusSeconds(900), paid));
    }

    void batch(Instant now) {
        List<LogRecord> records = List.copyOf(log.subList(pendingFrom, log.size()));
        pendingFrom = log.size();
        BatchOutcome o = store.transact("batch", tx -> matcher.process(new PartitionBatch(0, epoch, now, records), tx));
        matcher.committed(o);
    }

    void tick(Instant now) {
        BatchOutcome o = store.transact("tick", tx -> matcher.tick(0, epoch, now, tx));
        matcher.committed(o);
    }

    // ---- the resolver and a dispatcher played by the test ----

    Resolver.Outcome resolve(Instant now) {
        return store.transact("resolve", tx -> resolver.step(tx, now, 50));
    }

    List<Command> commands(CommandKind kind) {
        return store.committedCommands().stream().filter(c -> c.kind() == kind).toList();
    }

    /** Records {@code result} for every not-yet-DONE command of {@code kind}, and wakes its break, at {@code now}. */
    void answer(CommandKind kind, Map<String, String> result, Instant now) {
        store.transact("dispatch", tx -> {
            for (Command c : tx.outbox().forBreak(M, c(kind).breakId().orElseThrow())) {
                if (c.kind() == kind && c.status() != CommandStatus.DONE) {
                    tx.outbox().complete(c.commandId(), result);
                    c.breakId().ifPresent(b -> tx.breaks().wake(M, b, now));
                }
            }
            return null;
        });
    }

    Command c(CommandKind kind) {
        List<Command> all = commands(kind);
        assertThat(all).as("a %s command", kind).isNotEmpty();
        return all.get(all.size() - 1);
    }

    Break brk(BreakType type, String subject) {
        return store.transact("read", tx -> tx.breaks().find(M, type, subject)).orElseThrow();
    }

    Plan plan(Break b) {
        return store.committedPlan(M, b.breakId()).orElseThrow();
    }

    /** A success whose merchant never heard: DEBITED_NO_ORDER at ORDER_CHECK (T0 + 15 min). */
    Break debitedNoOrder() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        order(EventType.ORDER_PENDING_PAYMENT, 2, T0, null);
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        batch(T0.plusSeconds(11));
        tick(T0.plus(Duration.ofMinutes(15)));
        return brk(BreakType.DEBITED_NO_ORDER, "pay_1");
    }

    // ---- DEBITED_NO_ORDER ----

    @Test
    void anExpiredOrderIsRefundedOnceTheGatewayConfirmsAndClosesWhenTheRefundIsBooked() {
        Break b = debitedNoOrder();
        order(EventType.ORDER_EXPIRED, 3, T0.plus(Duration.ofMinutes(16)), null);
        batch(T0.plus(Duration.ofMinutes(16)));
        Instant now = T0.plus(Duration.ofMinutes(17));

        resolve(now);
        assertThat(brk(BreakType.DEBITED_NO_ORDER, "pay_1").state()).isEqualTo(BreakState.AWAITING_EVIDENCE);
        assertThat(c(CommandKind.STATUS_QUERY_PAYMENT).args()).containsEntry("payment_id", "pay_1");
        Plan p = plan(b);
        assertThat(p.rule()).isEqualTo(DeadlineRule.RBI_UPI_P2M_T5);
        assertThat(p.deadlineAt()).isEqualTo(Instant.parse("2026-11-18T18:30:00Z"));

        answer(CommandKind.STATUS_QUERY_PAYMENT, Map.of("status", "SUCCEEDED"), now.plusSeconds(1));
        resolve(now.plusSeconds(2));
        assertThat(brk(BreakType.DEBITED_NO_ORDER, "pay_1").state()).isEqualTo(BreakState.ACTION_PENDING);
        Command refund = c(CommandKind.REFUND);
        assertThat(refund.idempotencyKey()).as("payment-scoped (D11)").contains(Resolver.refundKey(M, P1));
        assertThat(refund.args()).containsEntry("amount", "499.00").containsEntry("order_id", "ord_1");
        assertThat(plan(b).action()).contains("REFUND");

        resolve(now.plus(Duration.ofMinutes(30)));
        assertThat(commands(CommandKind.REFUND)).as("decided once, whatever the later reviews").hasSize(1);

        append(Source.GATEWAY, EventType.REFUND_PROCESSED, now.plus(Duration.ofMinutes(31)), new RefundPayload(P1, AMOUNT));
        batch(now.plus(Duration.ofMinutes(31)));
        resolve(now.plus(Duration.ofMinutes(31)));
        assertThat(brk(BreakType.DEBITED_NO_ORDER, "pay_1").state()).isEqualTo(BreakState.RESOLVED_AUTO);
        assertThat(plan(b).resolvedAt()).contains(now.plus(Duration.ofMinutes(31)));
        assertThat(plan(b).compensation()).isEqualTo(Money.ZERO);
    }

    @Test
    void anOrderPaidWithThePaymentBeforeAnyRefundHealsTheBreak() {
        Break b = debitedNoOrder();
        resolve(T0.plus(Duration.ofMinutes(16)));
        order(EventType.ORDER_PAID, 3, T0.plus(Duration.ofMinutes(17)), P1);
        batch(T0.plus(Duration.ofMinutes(17)));
        assertThat(plan(b).reviewAt()).as("order.paid woke the break").isEqualTo(T0.plus(Duration.ofMinutes(17)));
        resolve(T0.plus(Duration.ofMinutes(17)));
        assertThat(brk(BreakType.DEBITED_NO_ORDER, "pay_1").state()).isEqualTo(BreakState.SELF_HEALED);
        assertThat(commands(CommandKind.REFUND)).isEmpty();
    }

    @Test
    void anOrderTheMerchantNeverReportedIsEscalatedNotRefunded() {
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        batch(T0.plusSeconds(11));
        tick(T0.plus(Duration.ofMinutes(15)));
        resolve(T0.plus(Duration.ofMinutes(16)));
        answer(CommandKind.STATUS_QUERY_PAYMENT, Map.of("status", "SUCCEEDED"), T0.plus(Duration.ofMinutes(17)));
        resolve(T0.plus(Duration.ofMinutes(17)));
        assertThat(brk(BreakType.DEBITED_NO_ORDER, "pay_1").state()).isEqualTo(BreakState.ESCALATED);
        assertThat(commands(CommandKind.REFUND)).as("D15").isEmpty();
        assertThat(c(CommandKind.ESCALATION_NOTIFY).args()).containsEntry("type", "DEBITED_NO_ORDER");
    }

    @Test
    void aGatewayThatKeepsDenyingTheSuccessRaisesStatusConflictAndEscalates() {
        debitedNoOrder();
        Instant now = T0.plus(Duration.ofMinutes(16));
        for (int i = 0; i < 3; i++) {
            resolve(now);
            answer(CommandKind.STATUS_QUERY_PAYMENT, Map.of("status", "PENDING"), now.plusSeconds(1));
            now = now.plus(Duration.ofMinutes(3));
        }
        resolve(now);
        assertThat(commands(CommandKind.STATUS_QUERY_PAYMENT)).hasSize(3);
        assertThat(brk(BreakType.DEBITED_NO_ORDER, "pay_1").state()).isEqualTo(BreakState.ESCALATED);
        assertThat(brk(BreakType.STATUS_CONFLICT, "pay_1").state()).isEqualTo(BreakState.DETECTED);
        assertThat(commands(CommandKind.REFUND)).isEmpty();
    }

    @Test
    void escalateByIsHonouredUnlessCanaryC10TurnsItOff() {
        Break b = debitedNoOrder();
        resolve(T0.plus(Duration.ofMinutes(16)));
        Instant late = plan(b).escalateBy();
        resolve(late);
        assertThat(brk(BreakType.DEBITED_NO_ORDER, "pay_1").state()).isEqualTo(BreakState.ESCALATED);
        assertThat(plan(b).resolvedAt()).as("escalation does not stop the clock").isEmpty();
    }

    @Test
    void canaryC10LeavesABreakOpenPastEscalateBy() {
        resolver = new Resolver(Resolver.Config.DEFAULT, Canaries.of(Canary.IGNORE_ESCALATE_BY), CrashPoints.NONE);
        Break b = debitedNoOrder();
        resolve(T0.plus(Duration.ofMinutes(16)));
        Instant past = plan(b).escalateBy().plusSeconds(1);
        resolve(past);
        assertThat(brk(BreakType.DEBITED_NO_ORDER, "pay_1").state()).isNotEqualTo(BreakState.ESCALATED);
        assertThat(plan(b).reviewAt()).as("the deadline is forgotten, not spun on").isAfter(past);
    }

    @Test
    void aCrashAtCp1LeavesNothingAndTheNextReviewDecidesTheSameCommand() {
        debitedNoOrder();
        resolver = new Resolver(Resolver.Config.DEFAULT, Canaries.NONE, point -> {
            if (point == CrashPoint.CP1) {
                throw new IllegalStateException("crash at CP1");
            }
        });
        assertThatThrownBy(() -> resolve(T0.plus(Duration.ofMinutes(16)))).hasMessage("crash at CP1");
        assertThat(store.committedCommands()).isEmpty();
        assertThat(store.committedPlan(M, brk(BreakType.DEBITED_NO_ORDER, "pay_1").breakId())).isEmpty();
        resolver = new Resolver();
        resolve(T0.plus(Duration.ofMinutes(16)));
        assertThat(commands(CommandKind.STATUS_QUERY_PAYMENT)).hasSize(1);
    }

    @Test
    void canaryC02SendsTheRefundWithNoKey() {
        resolver = new Resolver(Resolver.Config.DEFAULT, Canaries.of(Canary.NO_REFUND_KEY), CrashPoints.NONE);
        debitedNoOrder();
        order(EventType.ORDER_EXPIRED, 3, T0.plus(Duration.ofMinutes(16)), null);
        batch(T0.plus(Duration.ofMinutes(16)));
        resolve(T0.plus(Duration.ofMinutes(17)));
        answer(CommandKind.STATUS_QUERY_PAYMENT, Map.of("status", "SUCCEEDED"), T0.plus(Duration.ofMinutes(18)));
        resolve(T0.plus(Duration.ofMinutes(18)));
        assertThat(c(CommandKind.REFUND).idempotencyKey()).as("C02").isEmpty();
    }

    // ---- LATE_SUCCESS ----

    /** A failure, then a success: LATE_SUCCESS while the order is still pending. */
    Break lateSuccess() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        order(EventType.ORDER_PENDING_PAYMENT, 2, T0, null);
        pay(P1, EventType.PAYMENT_FAILED, T0.plusSeconds(5));
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(60));
        batch(T0.plusSeconds(61));
        return brk(BreakType.LATE_SUCCESS, "pay_1");
    }

    @Test
    void aLateSuccessOnAPendingOrderAsksTheMerchantAndARejectionIsRefunded() {
        lateSuccess();
        Instant now = T0.plus(Duration.ofMinutes(2));
        resolve(now);
        answer(CommandKind.STATUS_QUERY_PAYMENT, Map.of("status", "SUCCEEDED"), now.plusSeconds(1));
        resolve(now.plusSeconds(1));
        Command confirm = c(CommandKind.MERCHANT_CONFIRM);
        assertThat(confirm.idempotencyKey()).contains(Resolver.confirmKey(M, O1, P1));
        assertThat(brk(BreakType.LATE_SUCCESS, "pay_1").state()).isEqualTo(BreakState.ACTION_PENDING);

        answer(CommandKind.MERCHANT_CONFIRM, Map.of("status", "REJECTED"), now.plusSeconds(2));
        resolve(now.plusSeconds(2));
        assertThat(commands(CommandKind.REFUND)).hasSize(1);
    }

    @Test
    void anAcceptedLateSuccessResolvesWhenTheOrderIsPaid() {
        lateSuccess();
        Instant now = T0.plus(Duration.ofMinutes(2));
        resolve(now);
        answer(CommandKind.STATUS_QUERY_PAYMENT, Map.of("status", "SUCCEEDED"), now.plusSeconds(1));
        resolve(now.plusSeconds(1));
        answer(CommandKind.MERCHANT_CONFIRM, Map.of("status", "ACCEPTED"), now.plusSeconds(2));
        resolve(now.plusSeconds(2));
        assertThat(brk(BreakType.LATE_SUCCESS, "pay_1").state()).isEqualTo(BreakState.ACTION_PENDING);
        order(EventType.ORDER_PAID, 3, now.plusSeconds(3), P1);
        batch(now.plusSeconds(3));
        resolve(now.plusSeconds(3));
        assertThat(brk(BreakType.LATE_SUCCESS, "pay_1").state()).isEqualTo(BreakState.RESOLVED_AUTO);
        assertThat(commands(CommandKind.REFUND)).isEmpty();
    }

    @Test
    void aLateSuccessPaidWhileTheMerchantConfirmIsPendingResolvesAutomatically() {
        // BUG-004: ACTION_PENDING -> SELF_HEALED is not in the lifecycle; Driftline had already acted
        lateSuccess();
        Instant now = T0.plus(Duration.ofMinutes(2));
        resolve(now);
        answer(CommandKind.STATUS_QUERY_PAYMENT, Map.of("status", "SUCCEEDED"), now.plusSeconds(1));
        resolve(now.plusSeconds(1));
        assertThat(brk(BreakType.LATE_SUCCESS, "pay_1").state()).isEqualTo(BreakState.ACTION_PENDING);
        order(EventType.ORDER_PAID, 3, now.plusSeconds(2), P1);   // the merchant took it on its own; no answer yet
        batch(now.plusSeconds(2));
        resolve(now.plusSeconds(2));
        assertThat(brk(BreakType.LATE_SUCCESS, "pay_1").state()).isEqualTo(BreakState.RESOLVED_AUTO);
        assertThat(commands(CommandKind.REFUND)).isEmpty();
    }

    // ---- DUPLICATE_CAPTURE ----

    @Test
    void aDuplicateCaptureIsRefundedAfterBothPaymentsAreConfirmed() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        order(EventType.ORDER_PAID, 2, T0.plusSeconds(11), P1);
        pay(P2, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(20));
        batch(T0.plusSeconds(21));
        tick(T0.plus(Duration.ofMinutes(10)));   // idle: wm_G = now - 4 min, past the last capture + bound_G
        Break dup = brk(BreakType.DUPLICATE_CAPTURE, "pay_2");
        assertThat(dup.evidence()).containsEntry("kept", "pay_1");
        Instant now = T0.plus(Duration.ofMinutes(11));
        resolve(now);
        answer(CommandKind.STATUS_QUERY_PAYMENT, Map.of("status", "SUCCEEDED"), now.plusSeconds(1));
        resolve(now.plusSeconds(1));
        answer(CommandKind.STATUS_QUERY_PAYMENT, Map.of("status", "SUCCEEDED"), now.plusSeconds(2));
        resolve(now.plusSeconds(2));
        assertThat(commands(CommandKind.STATUS_QUERY_PAYMENT)).extracting(cmd -> cmd.args().get("payment_id"))
                .as("both payments queried").containsExactlyInAnyOrder("pay_2", "pay_1");
        assertThat(c(CommandKind.REFUND).args()).containsEntry("payment_id", "pay_2");
    }

    // ---- settlement breaks and anomalies ----

    @Test
    void aMissingSettlementIsEscalatedAtOnceWithAFourHourDeadline() {
        Break b = store.transact("raise", tx -> {
            Break x = Break.detected(M, BreakType.MISSING_SETTLEMENT, "pay_9", T0, Map.of("due_date", "2026-11-14"));
            tx.breaks().raise(x, "test");
            return x;
        });
        resolve(T0.plusSeconds(1));
        assertThat(brk(BreakType.MISSING_SETTLEMENT, "pay_9").state()).isEqualTo(BreakState.ESCALATED);
        assertThat(plan(b).deadlineAt()).isEqualTo(T0.plus(Duration.ofHours(4)));
    }

    @Test
    void anOrphanWaitsForAttributionUntilEscalateByThenEscalates() {
        Break b = store.transact("raise", tx -> {
            Break x = Break.detected(M, BreakType.ORPHAN_SETTLEMENT, "stl_1:2", T0, Map.of("reason", "AMBIGUOUS"));
            tx.breaks().raise(x, "test");
            return x;
        });
        resolve(T0.plusSeconds(1));
        assertThat(brk(BreakType.ORPHAN_SETTLEMENT, "stl_1:2").state()).isEqualTo(BreakState.AWAITING_EVIDENCE);
        assertThat(plan(b).reviewAt()).isEqualTo(T0.plus(Duration.ofHours(4)));
        assertThat(resolve(T0.plus(Duration.ofHours(1))).decisions()).as("not due before escalate_by").isEmpty();
        resolve(T0.plus(Duration.ofHours(4)));
        assertThat(brk(BreakType.ORPHAN_SETTLEMENT, "stl_1:2").state()).isEqualTo(BreakState.ESCALATED);
    }

    @Test
    void anOrphanNamingAPaymentAsksTheGatewayAboutItAndAsksAgainWhileTheAnswerIsNotFinal() {
        // D64: the reference may be a capture whose webhooks were all lost
        Break b = store.transact("raise", tx -> {
            Break x = Break.detected(M, BreakType.ORPHAN_SETTLEMENT, "stl_1:4", T0,
                    Map.of("reason", "UNKNOWN_REFERENCE", "reference", "pay_9", "amount", "499.00"));
            tx.breaks().raise(x, "test");
            return x;
        });
        resolve(T0.plusSeconds(1));
        assertThat(commands(CommandKind.STATUS_QUERY_PAYMENT)).singleElement()
                .satisfies(c -> assertThat(c.args()).containsEntry("payment_id", "pay_9"));
        assertThat(brk(BreakType.ORPHAN_SETTLEMENT, "stl_1:4").state()).isEqualTo(BreakState.AWAITING_EVIDENCE);

        answer(CommandKind.STATUS_QUERY_PAYMENT, Map.of("status", "PENDING"), T0.plusSeconds(2));
        resolve(T0.plusSeconds(2));
        assertThat(commands(CommandKind.STATUS_QUERY_PAYMENT)).as("a stale answer is asked again").hasSize(2);
        assertThat(plan(b).reviewAt()).isEqualTo(T0.plusSeconds(2).plus(Resolver.Config.DEFAULT.requeryAfter()));

        answer(CommandKind.STATUS_QUERY_PAYMENT, Map.of("status", "SUCCEEDED"), T0.plus(Duration.ofMinutes(3)));
        resolve(T0.plus(Duration.ofMinutes(3)));
        assertThat(commands(CommandKind.STATUS_QUERY_PAYMENT)).as("a final answer is not asked again").hasSize(2);
        assertThat(plan(b).reviewAt()).as("then it waits for attribution").isEqualTo(plan(b).escalateBy());
    }

    @Test
    void anAttributedOrphanHeals() {
        Break b = store.transact("raise", tx -> {
            Break x = Break.detected(M, BreakType.ORPHAN_SETTLEMENT, "stl_1:3", T0, Map.of("reason", "UNKNOWN_REFERENCE"));
            tx.breaks().raise(x, "test");
            return x;
        });
        resolve(T0.plusSeconds(1));
        store.transact("attribute", tx -> {
            tx.breaks().addEvidence(M, b.breakId(), "attributed_to", "pay_5");
            tx.breaks().wake(M, b.breakId(), T0.plusSeconds(2));
            return null;
        });
        resolve(T0.plusSeconds(2));
        assertThat(brk(BreakType.ORPHAN_SETTLEMENT, "stl_1:3").state()).isEqualTo(BreakState.SELF_HEALED);
    }

    @Test
    void anOpenIdempotencyConflictOnTheOrderBlocksAnyAutomaticAction() {
        debitedNoOrder();
        store.transact("conflict", tx -> tx.breaks().raise(Break.detected(M, BreakType.IDEMPOTENCY_CONFLICT,
                "MERCHANT:k9@9", T0, Map.of("order_id", "ord_1")), "test"));
        resolve(T0.plus(Duration.ofMinutes(16)));
        assertThat(brk(BreakType.DEBITED_NO_ORDER, "pay_1").state()).as("D41").isEqualTo(BreakState.ESCALATED);
        assertThat(commands(CommandKind.STATUS_QUERY_PAYMENT)).isEmpty();
    }
}
