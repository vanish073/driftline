package io.github.vanish073.driftline.testkit.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.breaks.DeadlineRule;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.port.BreakRepo.Plan;
import io.github.vanish073.driftline.core.port.OutboxRepo.Command;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandKind;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandStatus;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.port.TxBody;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The resolver's part of the Store contract (DESIGN §8, §9, §7.6; Shift 8): break plans and the review queue, the
 * outbox (enqueue, claim with leases, results as facts, retries) and the hash-chained audit log. Runs against
 * {@code InMemoryStore} and {@code PostgresStore}. The review queue and the outbox claim span every merchant, so each
 * test gets an empty store: {@link #store()} must return a fresh one on every call.
 */
public abstract class ResolverStoreContract {

    static final Instant T0 = Instant.parse("2026-11-14T06:00:00.123456Z");
    static final MerchantId M = MerchantId.of("m_r");

    private Store store;

    /** A new, empty store. */
    protected abstract Store store();

    @BeforeEach
    void fresh() {
        store = store();
    }

    private <R> R tx(TxBody<R> body) {
        return store.transact("contract", body);
    }

    private <T> Optional<T> one(TxBody<Optional<T>> body) {
        return store.transact("contract", body);
    }

    private <T> List<T> many(TxBody<List<T>> body) {
        return store.transact("contract", body);
    }

    private boolean yes(TxBody<Boolean> body) {
        return store.transact("contract", body);
    }

    private void run(Consumer<Tx> body) {
        store.transact("contract", t -> {
            body.accept(t);
            return null;
        });
    }

    private Break raise(BreakType type, String subject) {
        Break b = Break.detected(M, type, subject, T0, Map.of());
        run(t -> t.breaks().raise(b, "contract"));
        return b;
    }

    private static Plan plan(Instant reviewAt, Optional<String> action) {
        Instant deadline = T0.plus(Duration.ofHours(4));
        return new Plan(DeadlineRule.INTERNAL_ESCALATE_4H, deadline, deadline, reviewAt, action, Optional.empty(),
                Money.ZERO);
    }

    // ---- plans and the review queue ---------------------------------------------------------------------------------

    @Test
    void aPlanRoundTripsAndItsActionIsSetAtMostOnce() {
        Break b = raise(BreakType.ORPHAN_SETTLEMENT, "stl_1:1");
        assertThat(one(t -> t.breaks().planOf(M, b.breakId()))).isEmpty();
        Plan first = plan(T0, Optional.empty());
        run(t -> t.breaks().savePlan(M, b.breakId(), first));
        assertThat(one(t -> t.breaks().planOf(M, b.breakId()))).contains(first);

        Plan acted = new Plan(first.rule(), first.deadlineAt(), first.escalateBy(), T0.plusSeconds(60),
                Optional.of("ESCALATE"), Optional.of(T0.plusSeconds(90)), Money.ofPaise(10_000));
        run(t -> t.breaks().savePlan(M, b.breakId(), acted));
        assertThat(one(t -> t.breaks().planOf(M, b.breakId()))).contains(acted);
        run(t -> t.breaks().savePlan(M, b.breakId(), acted.withReviewAt(T0.plusSeconds(120))));

        assertThatThrownBy(() -> run(t -> t.breaks().savePlan(M, b.breakId(), new Plan(first.rule(), first.deadlineAt(),
                first.escalateBy(), T0, Optional.of("REFUND"), Optional.empty(), Money.ZERO))))
                .as("an action, once set, never changes").isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> run(t -> t.breaks().savePlan(M, "brk_none", first)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theReviewQueueHoldsOpenBreaksUnplannedFirstThenByReviewTime() {
        Break unplanned = raise(BreakType.MISSING_SETTLEMENT, "pay_u");
        Break later = raise(BreakType.MISSING_SETTLEMENT, "pay_l");
        Break sooner = raise(BreakType.MISSING_SETTLEMENT, "pay_s");
        Break future = raise(BreakType.MISSING_SETTLEMENT, "pay_f");
        Break closed = raise(BreakType.MISSING_SETTLEMENT, "pay_c");
        Break escalated = raise(BreakType.MISSING_SETTLEMENT, "pay_e");
        run(t -> {
            t.breaks().savePlan(M, later.breakId(), plan(T0.plusSeconds(20), Optional.empty()));
            t.breaks().savePlan(M, sooner.breakId(), plan(T0.plusSeconds(10), Optional.empty()));
            t.breaks().savePlan(M, future.breakId(), plan(T0.plusSeconds(3_600), Optional.empty()));
            t.breaks().transition(M, closed.breakId(), BreakState.SELF_HEALED, T0, "contract");
            t.breaks().transition(M, escalated.breakId(), BreakState.ESCALATED, T0, "contract");
        });
        List<Break> due = tx(t -> t.breaks().dueForReview(T0.plusSeconds(30), 10));
        assertThat(due).extracting(Break::breakId)
                .containsExactly(unplanned.breakId(), sooner.breakId(), later.breakId());
        assertThat(many(t -> t.breaks().dueForReview(T0.plusSeconds(30), 2))).hasSize(2);

        run(t -> t.breaks().wake(M, future.breakId(), T0.plusSeconds(5)));
        assertThat(one(t -> t.breaks().planOf(M, future.breakId())).orElseThrow().reviewAt())
                .isEqualTo(T0.plusSeconds(5));
        run(t -> t.breaks().wake(M, future.breakId(), T0.plusSeconds(900)));
        assertThat(one(t -> t.breaks().planOf(M, future.breakId())).orElseThrow().reviewAt())
                .as("waking never postpones a review").isEqualTo(T0.plusSeconds(5));
    }

    // ---- the outbox -------------------------------------------------------------------------------------------------

    private Command command(Break b, String id, Instant at) {
        return Command.pending(id, M, Optional.of(b.breakId()), CommandKind.REFUND, Map.of("payment_id", "pay_1", "amount", "499.00"),
                Optional.of("rfd-key-1"), at);
    }

    @Test
    void enqueueIsIdempotentAndACommandNeedsItsBreak() {
        Break b = raise(BreakType.DEBITED_NO_ORDER, "pay_1");
        Command c = command(b, "cmd_1", T0);
        assertThat(yes(t -> t.outbox().enqueue(c))).isTrue();
        assertThat(yes(t -> t.outbox().enqueue(c))).isFalse();
        assertThat(one(t -> t.outbox().find("cmd_1"))).contains(c);
        assertThat(one(t -> t.outbox().find("cmd_none"))).isEmpty();
        assertThatThrownBy(() -> run(t -> t.outbox().enqueue(Command.pending("cmd_2", M, Optional.of("brk_none"),
                CommandKind.REFUND, Map.of(), Optional.empty(), T0)))).isInstanceOf(IllegalArgumentException.class);
        Command probe = Command.pending("cmd_probe", M, Optional.empty(), CommandKind.STATUS_QUERY_PAYMENT,
                Map.of("payment_id", "pay_9"), Optional.empty(), T0);
        assertThat(yes(t -> t.outbox().enqueue(probe))).as("a pending-order probe has no break").isTrue();
        assertThat(one(t -> t.outbox().find("cmd_probe"))).contains(probe);
    }

    @Test
    void aClaimLeasesDueCommandsOnceAndAnExpiredLeaseMakesThemDueAgain() {
        Break b = raise(BreakType.DEBITED_NO_ORDER, "pay_1");
        run(t -> {
            t.outbox().enqueue(command(b, "cmd_b", T0.plusSeconds(1)));
            t.outbox().enqueue(command(b, "cmd_a", T0.plusSeconds(1)));
            t.outbox().enqueue(command(b, "cmd_0", T0));
            t.outbox().enqueue(command(b, "cmd_later", T0.plusSeconds(3_600)));
        });
        Instant now = T0.plusSeconds(10);
        List<Command> claimed = tx(t -> t.outbox().claim(now, "d1", now.plusSeconds(30), now.plusSeconds(40), 10));
        assertThat(claimed).extracting(Command::commandId).containsExactly("cmd_0", "cmd_a", "cmd_b");
        Command first = claimed.get(0);
        assertThat(first.status()).isEqualTo(CommandStatus.IN_FLIGHT);
        assertThat(first.attempt()).isEqualTo(1);
        assertThat(first.leaseOwner()).contains("d1");
        assertThat(first.leaseUntil()).contains(now.plusSeconds(30));
        assertThat(first.firstSentAt()).contains(now);
        assertThat(first.nextAttemptAt()).isEqualTo(now.plusSeconds(40));
        assertThat(one(t -> t.outbox().find("cmd_0"))).contains(first);

        assertThat(many(t -> t.outbox().claim(now.plusSeconds(5), "d2", now.plusSeconds(60), now.plusSeconds(70), 10)))
                .as("leased commands are not claimed again").isEmpty();
        Instant afterLease = now.plusSeconds(41);
        List<Command> again = tx(t -> t.outbox().claim(afterLease, "d2", afterLease.plusSeconds(30),
                afterLease.plusSeconds(40), 1));
        assertThat(again).extracting(Command::commandId).containsExactly("cmd_0");
        assertThat(again.get(0).attempt()).isEqualTo(2);
        assertThat(again.get(0).leaseOwner()).contains("d2");
        assertThat(again.get(0).firstSentAt()).as("the first send time is kept").contains(now);
        assertThat(many(t -> t.outbox().forBreak(M, b.breakId()))).extracting(Command::commandId)
                .containsExactly("cmd_0", "cmd_a", "cmd_b", "cmd_later");
    }

    @Test
    void theFirstResultWinsAndADoneCommandIsNeverClaimedOrRetried() {
        Break b = raise(BreakType.DEBITED_NO_ORDER, "pay_1");
        run(t -> t.outbox().enqueue(command(b, "cmd_1", T0)));
        run(t -> t.outbox().claim(T0, "d1", T0.plusSeconds(30), T0.plusSeconds(40), 10));
        assertThat(yes(t -> t.outbox().complete("cmd_1", Map.of("status", "PROCESSED", "refund_id", "rfd_9")))).isTrue();
        assertThat(yes(t -> t.outbox().complete("cmd_1", Map.of("status", "FAILED")))).as("a later result").isFalse();
        Command done = tx(t -> t.outbox().find("cmd_1")).orElseThrow();
        assertThat(done.status()).isEqualTo(CommandStatus.DONE);
        assertThat(done.result().orElseThrow()).containsEntry("status", "PROCESSED").containsEntry("refund_id", "rfd_9");
        assertThat(done.leaseOwner()).isEmpty();
        run(t -> t.outbox().retryAt("cmd_1", T0));
        assertThat(one(t -> t.outbox().find("cmd_1"))).contains(done);
        assertThat(many(t -> t.outbox().claim(T0.plusSeconds(3_600), "d2", T0.plusSeconds(3_630), T0.plusSeconds(3_640),
                10))).isEmpty();
        assertThatThrownBy(() -> run(t -> t.outbox().complete("cmd_none", Map.of())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anExplicitRetryReleasesTheLeaseAndReschedules() {
        Break b = raise(BreakType.DEBITED_NO_ORDER, "pay_1");
        run(t -> t.outbox().enqueue(command(b, "cmd_1", T0)));
        run(t -> t.outbox().claim(T0, "d1", T0.plusSeconds(30), T0.plusSeconds(40), 10));
        run(t -> t.outbox().retryAt("cmd_1", T0.plusSeconds(5)));
        Command c = tx(t -> t.outbox().find("cmd_1")).orElseThrow();
        assertThat(c.status()).isEqualTo(CommandStatus.PENDING);
        assertThat(c.leaseOwner()).isEmpty();
        assertThat(c.nextAttemptAt()).isEqualTo(T0.plusSeconds(5));
        assertThat(c.attempt()).isEqualTo(1);
        assertThat(many(t -> t.outbox().claim(T0.plusSeconds(6), "d2", T0.plusSeconds(36), T0.plusSeconds(46), 10)))
                .extracting(Command::commandId).containsExactly("cmd_1");
    }

    // ---- the audit log ----------------------------------------------------------------------------------------------

    @Test
    void auditRowsChainEachToThePreviousHash() {
        AuditRepo.Entry e1 = AuditRepo.Entry.of(T0, "ops:alice", "RESOLVE_MANUAL", "brk_1", Map.of("note", "called the bank"));
        AuditRepo.Entry e2 = AuditRepo.Entry.of(T0.plusSeconds(1), "ops:bob", "RESOLVE_MANUAL", "brk_2", Map.of());
        AuditRepo.Row r1 = tx(t -> t.audit().append(e1));
        AuditRepo.Row r2 = tx(t -> t.audit().append(e2));
        assertThat(r1.seq()).isLessThan(r2.seq());
        assertThat(r1.prevHash()).isEqualTo(AuditRepo.GENESIS);
        assertThat(r1.hash()).isEqualTo(AuditRepo.hash(AuditRepo.GENESIS, e1));
        assertThat(r2.prevHash()).isEqualTo(r1.hash());
        assertThat(r2.hash()).isEqualTo(AuditRepo.hash(r1.hash(), e2));
        assertThat(many(t -> t.audit().rows())).containsExactly(r1, r2);
    }

    @Test
    void rolledBackPlansCommandsAndAuditRowsAreForgotten() {
        Break b = raise(BreakType.DEBITED_NO_ORDER, "pay_1");
        assertThatThrownBy(() -> store.transact("contract-abort", t -> {
            t.breaks().savePlan(M, b.breakId(), plan(T0, Optional.of("REFUND")));
            t.outbox().enqueue(command(b, "cmd_1", T0));
            t.audit().append(AuditRepo.Entry.of(T0, "ops:alice", "RESOLVE_MANUAL", b.breakId(), Map.of()));
            throw new IllegalStateException("abort");
        })).hasMessage("abort");
        assertThat(one(t -> t.breaks().planOf(M, b.breakId()))).isEmpty();
        assertThat(one(t -> t.outbox().find("cmd_1"))).isEmpty();
        assertThat(many(t -> t.audit().rows())).isEmpty();
    }
}
