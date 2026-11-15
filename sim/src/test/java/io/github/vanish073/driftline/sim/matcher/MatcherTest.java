package io.github.vanish073.driftline.sim.matcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventPayload;
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
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.CauseType;
import io.github.vanish073.driftline.core.matcher.BatchOutcome;
import io.github.vanish073.driftline.core.matcher.LogRecord;
import io.github.vanish073.driftline.core.matcher.Matcher;
import io.github.vanish073.driftline.core.matcher.PartitionBatch;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.port.QuarantineRepo;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The core matcher, first cut, against the in-memory store. */
class MatcherTest {

    private static final MerchantId M = MerchantId.of("m_0");
    private static final PaymentId P = PaymentId.of("pay_1");
    private static final OrderId O = OrderId.of("ord_1");
    private static final Money AMOUNT = Money.ofPaise(49_900);
    private static final Instant T0 = Instant.parse("2026-11-13T06:00:00Z");

    private final InMemoryStore store = new InMemoryStore();
    private final Matcher matcher = new Matcher();
    private final List<LogRecord> log = new ArrayList<>();
    private final long epoch = store.transact("assign", tx -> tx.partitions().acquire(0, "matcher-a.1")).epoch();

    private LogRecord append(Source source, String key, EventType type, int second, EventPayload payload, String body) {
        EventEnvelope e = new EventEnvelope(1, M, 0, source, key, type, T0.plusSeconds(second), T0.plusSeconds(second),
                Sha256.of(body.getBytes(StandardCharsets.UTF_8)), payload, Optional.empty());
        LogRecord r = new LogRecord(0, log.size(), e);
        log.add(r);
        return r;
    }

    private LogRecord payment(String key, EventType type, int second) {
        return append(Source.GATEWAY, key, type, second, new PaymentPayload(P, O, AMOUNT), key);
    }

    private LogRecord order(String key, EventType type, long version, int second) {
        Optional<PaymentId> paid = type.carriesPaidPayment() ? Optional.of(P) : Optional.empty();
        return append(Source.MERCHANT, key, type, second, new OrderPayload(O, version, AMOUNT, T0.plusSeconds(900), paid), key);
    }

    private BatchOutcome process(List<LogRecord> records) {
        return process(matcher, epoch, records);
    }

    private BatchOutcome process(Matcher with, long underEpoch, List<LogRecord> records) {
        return store.transact("batch", tx -> with.process(new PartitionBatch(0, underEpoch, records), tx));
    }

    private long balance(AccountCode code) {
        return store.transact("read", tx -> tx.ledger().balance(M, code)).paise();
    }

    @Test
    void aPaidOrderAndASuccessfulPaymentPostCaptureAndOrderMatchInEitherOrder() {
        process(List.of(order("o1", EventType.ORDER_CREATED, 1, 0), order("o3", EventType.ORDER_PAID, 3, 20)));
        assertThat(store.committedLedger()).isEmpty();
        BatchOutcome outcome = process(List.of(payment("e1", EventType.PAYMENT_PENDING, 1),
                payment("e2", EventType.PAYMENT_SUCCEEDED, 10)));
        assertThat(outcome.postings()).isEqualTo(2);
        assertThat(store.committedLedger()).extracting(t -> t.cause().type())
                .containsExactly(CauseType.CAPTURE, CauseType.ORDER_MATCH);
        assertThat(balance(AccountCode.GATEWAY_RECEIVABLE)).isEqualTo(49_900);
        assertThat(balance(AccountCode.PAYMENTS_CLEARING)).isZero();
        assertThat(balance(AccountCode.SALES)).isEqualTo(-49_900);
        assertThat(store.committedOffset(0)).isEqualTo(4);
    }

    @Test
    void aReplayedBatchAndARepeatedKeyChangeNothing() {
        List<LogRecord> batch = List.of(payment("e1", EventType.PAYMENT_SUCCEEDED, 1));
        process(batch);
        BatchOutcome replay = process(batch);
        assertThat(replay.alreadyProcessed()).isEqualTo(1);
        assertThat(replay.applied()).isZero();

        BatchOutcome redelivered = process(List.of(payment("e1", EventType.PAYMENT_SUCCEEDED, 1)));
        assertThat(redelivered.duplicates()).isEqualTo(1);
        assertThat(store.committedLedger()).hasSize(1);
        assertThat(store.committedPaymentTransitions()).hasSize(1);
    }

    @Test
    void aKeyReusedWithADifferentBodyIsQuarantinedAndNotApplied() {
        process(List.of(payment("e1", EventType.PAYMENT_PENDING, 1)));
        LogRecord second = append(Source.GATEWAY, "e1", EventType.PAYMENT_SUCCEEDED, 5,
                new PaymentPayload(P, O, AMOUNT), "a different body");
        BatchOutcome reused = process(List.of(second));
        assertThat(reused.keyReused()).isEqualTo(1);
        assertThat(store.transact("read", tx -> tx.payments().find(M, P)).orElseThrow().status())
                .isEqualTo(PaymentStatus.PENDING);
        assertThat(store.committedQuarantine()).containsExactly(new QuarantineRepo.Entry(0, 1, M, Source.GATEWAY, "e1",
                second.envelope().payloadSha256(), QuarantineRepo.Reason.IDEMPOTENCY_CONFLICT));
    }

    @Test
    void aZombieBatchIsFencedAndLeavesNothingBehind() {
        process(List.of(payment("e1", EventType.PAYMENT_PENDING, 1)));
        store.transact("reassign", tx -> tx.partitions().acquire(0, "matcher-b.1"));
        List<LogRecord> zombieBatch = List.of(payment("e2", EventType.PAYMENT_SUCCEEDED, 10));
        assertThatThrownBy(() -> process(zombieBatch)).isInstanceOf(StoreConflict.class);
        assertThat(store.committedLedger()).isEmpty();
        assertThat(store.committedPaymentTransitions()).hasSize(1);
        assertThat(store.committedOffset(0)).isEqualTo(1);

        BatchOutcome owner = process(matcher, epoch + 1, zombieBatch);
        assertThat(owner.applied()).as("the new owner applies what the zombie could not").isEqualTo(1);
        assertThat(store.committedOffsetCommits()).allSatisfy(c -> assertThat(c.epochUsed()).isEqualTo(c.leaseEpoch()));
    }

    @Test
    void canaryC09LetsTheZombieCommitAndTheCommitLogShowsIt() {
        InMemoryStore unfenced = new InMemoryStore(Canaries.of(Canary.NO_FENCING));
        long stale = unfenced.transact("a", tx -> tx.partitions().acquire(0, "matcher-a.1")).epoch();
        unfenced.transact("b", tx -> tx.partitions().acquire(0, "matcher-b.1"));
        LogRecord r = payment("e1", EventType.PAYMENT_PENDING, 1);
        unfenced.transact("zombie", tx -> matcher.process(new PartitionBatch(0, stale, List.of(r)), tx));
        assertThat(unfenced.committedOffsetCommits()).singleElement()
                .satisfies(c -> assertThat(c.epochUsed()).isLessThan(c.leaseEpoch()));
    }

    @Test
    void canaryC01SkipsDedupeSoARedeliveryReachesTheStateMachineAgain() {
        Matcher noDedupe = new Matcher(Canaries.of(Canary.DEDUPE_OFF));
        process(noDedupe, epoch, List.of(payment("e1", EventType.PAYMENT_SUCCEEDED, 1)));
        process(noDedupe, epoch, List.of(payment("e1", EventType.PAYMENT_SUCCEEDED, 1)));
        assertThat(store.committedPaymentTransitions()).extracting(t -> t.row().eventKey()).containsExactly("e1", "e1");
    }

    @Test
    void aLateSuccessIsCapturedOnce() {
        BatchOutcome outcome = process(List.of(payment("f", EventType.PAYMENT_FAILED, 5),
                payment("s", EventType.PAYMENT_SUCCEEDED, 9)));
        assertThat(outcome.applied()).isEqualTo(2);
        assertThat(store.committedLedger()).hasSize(1);
        assertThat(store.transact("read", tx -> tx.payments().find(M, P)).orElseThrow().lateSuccess()).isTrue();
    }
}
