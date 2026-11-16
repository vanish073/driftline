package io.github.vanish073.driftline.testkit.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakTransition;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.order.OrderState;
import io.github.vanish073.driftline.core.order.OrderStatus;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.port.SettlementRepo.FileEnd;
import io.github.vanish073.driftline.core.port.SettlementRepo.Line;
import io.github.vanish073.driftline.core.port.SettlementRepo.LineStatus;
import io.github.vanish073.driftline.core.port.SettlementRepo.Match;
import io.github.vanish073.driftline.core.port.SettlementRepo.MatchHow;
import io.github.vanish073.driftline.core.port.SettlementRepo.Receipt;
import io.github.vanish073.driftline.core.port.SettlementRepo.ReceiptStatus;
import io.github.vanish073.driftline.core.port.SettlementRepo.ReceivedFile;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.TimerRepo.Kind;
import io.github.vanish073.driftline.core.port.TimerRepo.Timer;
import io.github.vanish073.driftline.core.port.TimerRepo.TimerClock;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.port.TxBody;
import io.github.vanish073.driftline.core.port.WatermarkRepo.StreamWatermark;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * The detection part of the Store contract (DESIGN §5.4–§5.6, §6.2, §6.3, §8.1): watermarks, timers, breaks,
 * settlement receipts, lines, trailers and matches, and the payment and order queries the matcher joins on. Runs
 * against {@code InMemoryStore} and {@code PostgresStore}. Every test works on its own merchant and partition.
 */
public abstract class DetectionStoreContract {

    private static final Instant T0 = Instant.parse("2026-11-13T06:00:00.123456Z");
    private static final LocalDate D0 = LocalDate.parse("2026-11-14");

    private Store store;
    private MerchantId m;
    private MerchantId other;
    private int partition;
    private String salt;

    protected abstract Store store();

    @BeforeEach
    void fresh(TestInfo test) {
        store = store();
        salt = getClass().getName() + "#" + test.getDisplayName();
        m = MerchantId.of("m-" + DerivedId.of("detection-contract", salt));
        other = MerchantId.of("o-" + DerivedId.of("detection-contract", salt));
        partition = 2_000_000 + Math.floorMod(salt.hashCode(), 1_000_000);
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

    private Receipt receipt(TxBody<Receipt> body) {
        return store.transact("contract", body);
    }

    private void run(Consumer<Tx> body) {
        store.transact("contract", t -> {
            body.accept(t);
            return null;
        });
    }

    private void rolledBack(Consumer<Tx> body) {
        assertThatThrownBy(() -> store.transact("contract-abort", t -> {
            body.accept(t);
            throw new IllegalStateException("abort");
        })).hasMessage("abort");
    }

    private Sha256 sha(String s) {
        return Sha256.of((salt + "/" + s).getBytes(StandardCharsets.UTF_8));
    }

    // ---- watermarks ---------------------------------------------------------------------------------------------

    @Test
    void aStreamWatermarkRoundTripsAndNeverMovesBack() {
        assertThat(one(t -> t.watermarks().stream(partition, Source.GATEWAY))).isEmpty();
        StreamWatermark w = new StreamWatermark(partition, Source.GATEWAY, T0, Optional.of(T0.plusSeconds(120)),
                Optional.of(T0.plusSeconds(121)));
        run(t -> t.watermarks().saveStream(w));
        assertThat(one(t -> t.watermarks().stream(partition, Source.GATEWAY))).contains(w);
        assertThat(one(t -> t.watermarks().stream(partition, Source.MERCHANT))).isEmpty();

        StreamWatermark idle = new StreamWatermark(partition, Source.GATEWAY, T0.plusSeconds(60),
                w.maxEventTime(), w.lastEventAt());
        run(t -> t.watermarks().saveStream(idle));
        assertThat(one(t -> t.watermarks().stream(partition, Source.GATEWAY))).contains(idle);

        assertThatThrownBy(() -> run(t -> t.watermarks().saveStream(new StreamWatermark(partition, Source.GATEWAY,
                T0, w.maxEventTime(), w.lastEventAt())))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> run(t -> t.watermarks().saveStream(new StreamWatermark(partition, Source.GATEWAY,
                idle.value(), Optional.of(T0), w.lastEventAt())))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> run(t -> t.watermarks().saveStream(new StreamWatermark(partition, Source.GATEWAY,
                idle.value(), Optional.empty(), w.lastEventAt())))).isInstanceOf(IllegalArgumentException.class);
        assertThat(one(t -> t.watermarks().stream(partition, Source.GATEWAY))).contains(idle);
    }

    @Test
    void aWatermarkWithNoEventsYetKeepsItsEmptyFields() {
        StreamWatermark w = new StreamWatermark(partition, Source.MERCHANT, T0, Optional.empty(), Optional.empty());
        run(t -> t.watermarks().saveStream(w));
        assertThat(one(t -> t.watermarks().stream(partition, Source.MERCHANT))).contains(w);
    }

    @Test
    void theSettlementWatermarkRoundTripsAndNeverMovesBack() {
        assertThat(one(t -> t.watermarks().settledThrough(m))).isEmpty();
        run(t -> t.watermarks().saveSettledThrough(m, D0));
        run(t -> t.watermarks().saveSettledThrough(m, D0));
        run(t -> t.watermarks().saveSettledThrough(m, D0.plusDays(2)));
        assertThat(one(t -> t.watermarks().settledThrough(m))).contains(D0.plusDays(2));
        assertThat(one(t -> t.watermarks().settledThrough(other))).isEmpty();
        assertThatThrownBy(() -> run(t -> t.watermarks().saveSettledThrough(m, D0.plusDays(1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(one(t -> t.watermarks().settledThrough(m))).contains(D0.plusDays(2));
    }

    @Test
    void aRolledBackWatermarkIsForgotten() {
        rolledBack(t -> {
            t.watermarks().saveStream(new StreamWatermark(partition, Source.GATEWAY, T0, Optional.empty(), Optional.empty()));
            t.watermarks().saveSettledThrough(m, D0);
        });
        assertThat(one(t -> t.watermarks().stream(partition, Source.GATEWAY))).isEmpty();
        assertThat(one(t -> t.watermarks().settledThrough(m))).isEmpty();
    }

    // ---- timers -------------------------------------------------------------------------------------------------

    private Timer timer(Kind kind, String subject, TimerClock clock, Instant at) {
        return new Timer(m, kind, subject, partition, clock, at);
    }

    @Test
    void reRegisteringATimerMovesIt() {
        Timer first = timer(Kind.ORDER_CHECK, "pay_1", TimerClock.STREAM_GM, T0.plusSeconds(600));
        run(t -> t.timers().upsert(first));
        assertThat(one(t -> t.timers().find(m, Kind.ORDER_CHECK, "pay_1"))).contains(first);
        Timer moved = timer(Kind.ORDER_CHECK, "pay_1", TimerClock.STREAM_GM, T0.plusSeconds(60));
        run(t -> t.timers().upsert(moved));
        assertThat(one(t -> t.timers().find(m, Kind.ORDER_CHECK, "pay_1"))).contains(moved);
        assertThat(many(t -> t.timers().due(partition, TimerClock.STREAM_GM, T0.plusSeconds(3600)))).containsExactly(moved);
    }

    @Test
    void dueTimersComeInFiringOrderAndOnlyForTheirPartitionAndClock() {
        Timer late = timer(Kind.ORDER_CHECK, "pay_a", TimerClock.STREAM_GM, T0.plusSeconds(30));
        Timer dupSameInstant = timer(Kind.DUP_CHECK, "ord_z", TimerClock.STREAM_GM, T0.plusSeconds(10));
        Timer orderSameInstantB = timer(Kind.ORDER_CHECK, "pay_b", TimerClock.STREAM_GM, T0.plusSeconds(10));
        Timer orderSameInstantA = timer(Kind.ORDER_CHECK, "pay_a2", TimerClock.STREAM_GM, T0.plusSeconds(10));
        Timer notYet = timer(Kind.ORDER_CHECK, "pay_c", TimerClock.STREAM_GM, T0.plusSeconds(31));
        Timer otherClock = timer(Kind.DUP_CHECK, "ord_y", TimerClock.STREAM_G, T0);
        Timer otherPartition = new Timer(m, Kind.ORDER_CHECK, "pay_d", partition + 1, TimerClock.STREAM_GM, T0);
        run(t -> List.of(late, dupSameInstant, orderSameInstantB, orderSameInstantA, notYet, otherClock, otherPartition)
                .forEach(t.timers()::upsert));

        assertThat(many(t -> t.timers().due(partition, TimerClock.STREAM_GM, T0.plusSeconds(30))))
                .as("fireAt, then kind (DUP_CHECK < ORDER_CHECK by name), then subject key; upTo is inclusive")
                .containsExactly(dupSameInstant, orderSameInstantA, orderSameInstantB, late);
        assertThat(many(t -> t.timers().due(partition, TimerClock.STREAM_G, T0.plusSeconds(30)))).containsExactly(otherClock);
        assertThat(one(t -> t.timers().earliest(partition, TimerClock.STREAM_GM))).contains(T0.plusSeconds(10));
        assertThat(one(t -> t.timers().earliest(partition, TimerClock.PROCESSING))).isEmpty();
    }

    @Test
    void dueForMerchantSeesOnlyThatMerchantsTimers() {
        Timer mine = timer(Kind.SETTLEMENT_DUE, "pay_1", TimerClock.SETTLEMENT, T0);
        Timer theirs = new Timer(other, Kind.SETTLEMENT_DUE, "pay_1", partition, TimerClock.SETTLEMENT, T0);
        run(t -> {
            t.timers().upsert(mine);
            t.timers().upsert(theirs);
        });
        assertThat(many(t -> t.timers().dueForMerchant(m, TimerClock.SETTLEMENT, T0))).containsExactly(mine);
        assertThat(many(t -> t.timers().dueForMerchant(other, TimerClock.SETTLEMENT, T0))).containsExactly(theirs);
        assertThat(many(t -> t.timers().dueForMerchant(m, TimerClock.SETTLEMENT, T0.minusMillis(1)))).isEmpty();
    }

    @Test
    void aDeletedTimerIsGoneAndDeletingNothingIsHarmless() {
        Timer t1 = timer(Kind.FEED_STALL, D0.toString(), TimerClock.PROCESSING, T0);
        run(t -> t.timers().upsert(t1));
        run(t -> t.timers().delete(m, Kind.FEED_STALL, D0.toString()));
        run(t -> t.timers().delete(m, Kind.FEED_STALL, D0.toString()));
        assertThat(one(t -> t.timers().find(m, Kind.FEED_STALL, D0.toString()))).isEmpty();
        assertThat(many(t -> t.timers().due(partition, TimerClock.PROCESSING, T0))).isEmpty();
    }

    @Test
    void theEarliestTimerSkipsOneDeletedInTheSameTransaction() {
        Timer early = timer(Kind.ORDER_CHECK, "pay_1", TimerClock.STREAM_GM, T0);
        Timer late = timer(Kind.ORDER_CHECK, "pay_2", TimerClock.STREAM_GM, T0.plusSeconds(60));
        run(t -> {
            t.timers().upsert(early);
            t.timers().upsert(late);
        });
        assertThat(one(t -> {
            t.timers().delete(m, Kind.ORDER_CHECK, "pay_1");
            return t.timers().earliest(partition, TimerClock.STREAM_GM);
        })).contains(T0.plusSeconds(60));
        assertThat(one(t -> {
            t.timers().upsert(timer(Kind.ORDER_CHECK, "pay_2", TimerClock.STREAM_GM, T0.minusSeconds(5)));
            return t.timers().earliest(partition, TimerClock.STREAM_GM);
        })).as("a timer moved earlier in this transaction").contains(T0.minusSeconds(5));
        assertThat(one(t -> t.timers().earliest(partition, TimerClock.STREAM_GM))).contains(T0.minusSeconds(5));
    }

    @Test
    void aRolledBackTimerIsForgotten() {
        rolledBack(t -> t.timers().upsert(timer(Kind.ORDER_CHECK, "pay_1", TimerClock.STREAM_GM, T0)));
        assertThat(one(t -> t.timers().find(m, Kind.ORDER_CHECK, "pay_1"))).isEmpty();
    }

    // ---- breaks -------------------------------------------------------------------------------------------------

    @Test
    void raisingABreakTwiceKeepsTheFirstOne() {
        Break b = Break.detected(m, BreakType.DEBITED_NO_ORDER, "pay_1", T0, Map.of("order", "ord_1"));
        var first = tx(t -> t.breaks().raise(b, "ORDER_CHECK"));
        assertThat(first.created()).isTrue();
        assertThat(first.current()).isEqualTo(b);

        Break again = Break.detected(m, BreakType.DEBITED_NO_ORDER, "pay_1", T0.plusSeconds(5), Map.of("order", "ord_9"));
        var second = tx(t -> t.breaks().raise(again, "ORDER_CHECK"));
        assertThat(second.created()).isFalse();
        assertThat(second.current()).isEqualTo(b);
        assertThat(one(t -> t.breaks().find(m, BreakType.DEBITED_NO_ORDER, "pay_1"))).contains(b);
        assertThat(one(t -> t.breaks().find(m, BreakType.LATE_SUCCESS, "pay_1"))).isEmpty();
        assertThat(one(t -> t.breaks().find(other, BreakType.DEBITED_NO_ORDER, "pay_1"))).isEmpty();
        assertThat(many(t -> t.breaks().transitions(m, b.breakId()))).containsExactly(
                new BreakTransition(b.breakId(), 1, Optional.empty(), BreakState.DETECTED, T0, "ORDER_CHECK"));
    }

    @Test
    void evidenceBumpsTheVersionOnlyWhenItChangesSomething() {
        Break b = Break.detected(m, BreakType.ORPHAN_SETTLEMENT, "stl_1:3", T0, Map.of("reason", "UNKNOWN_REFERENCE"));
        run(t -> t.breaks().raise(b, "line"));
        run(t -> t.breaks().addEvidence(m, b.breakId(), "attributed_to", "pay_7"));
        run(t -> t.breaks().addEvidence(m, b.breakId(), "attributed_to", "pay_7"));
        Break after = tx(t -> t.breaks().find(m, BreakType.ORPHAN_SETTLEMENT, "stl_1:3")).orElseThrow();
        assertThat(after.version()).isEqualTo(2);
        assertThat(after.evidence()).containsExactlyEntriesOf(
                new TreeMap<>(Map.of("attributed_to", "pay_7", "reason", "UNKNOWN_REFERENCE")));
        run(t -> t.breaks().addEvidence(m, b.breakId(), "attributed_to", "pay_8"));
        assertThat(one(t -> t.breaks().find(m, BreakType.ORPHAN_SETTLEMENT, "stl_1:3")).orElseThrow().version())
                .isEqualTo(3);
        assertThatThrownBy(() -> run(t -> t.breaks().addEvidence(m, Break.idOf(m, BreakType.LATE_SUCCESS, "x"), "k", "v")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aTransitionIsAppendedAndBumpsTheVersion() {
        Break b = Break.detected(m, BreakType.LATE_SUCCESS, "pay_2", T0, Map.of());
        run(t -> t.breaks().raise(b, "late"));
        run(t -> t.breaks().transition(m, b.breakId(), BreakState.LINKED, T0.plusSeconds(1), "owner brk_x"));
        Break after = tx(t -> t.breaks().find(m, BreakType.LATE_SUCCESS, "pay_2")).orElseThrow();
        assertThat(after.state()).isEqualTo(BreakState.LINKED);
        assertThat(after.version()).isEqualTo(2);
        assertThat(after.detectedAt()).isEqualTo(T0);
        assertThat(many(t -> t.breaks().transitions(m, b.breakId()))).containsExactly(
                new BreakTransition(b.breakId(), 1, Optional.empty(), BreakState.DETECTED, T0, "late"),
                new BreakTransition(b.breakId(), 2, Optional.of(BreakState.DETECTED), BreakState.LINKED,
                        T0.plusSeconds(1), "owner brk_x"));
        assertThatThrownBy(() -> run(t -> t.breaks().transition(m, b.breakId(), BreakState.LINKED, T0, "again")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aMerchantsBreaksComeByTypeThenSubject() {
        Break z = Break.detected(m, BreakType.MISSING_SETTLEMENT, "pay_1", T0, Map.of());
        Break a2 = Break.detected(m, BreakType.DEBITED_NO_ORDER, "pay_2", T0, Map.of());
        Break a1 = Break.detected(m, BreakType.DEBITED_NO_ORDER, "pay_10", T0, Map.of());
        Break theirs = Break.detected(other, BreakType.DEBITED_NO_ORDER, "pay_1", T0, Map.of());
        run(t -> List.of(z, a2, a1, theirs).forEach(b -> t.breaks().raise(b, "test")));
        assertThat(many(t -> t.breaks().forMerchant(m))).containsExactly(a1, a2, z);
    }

    @Test
    void aBreakIsFoundByItsIdAloneWhateverItsMerchant() {
        Break mine = Break.detected(m, BreakType.DEBITED_NO_ORDER, "pay_1", T0, Map.of("k", "v"));
        Break theirs = Break.detected(other, BreakType.DEBITED_NO_ORDER, "pay_1", T0, Map.of());
        run(t -> {
            t.breaks().raise(mine, "test");
            t.breaks().raise(theirs, "test");
            assertThat(t.breaks().byId(mine.breakId())).as("visible inside the transaction").contains(mine);
        });
        assertThat(one(t -> t.breaks().byId(mine.breakId()))).contains(mine);
        assertThat(one(t -> t.breaks().byId(theirs.breakId()))).contains(theirs);
        assertThat(one(t -> t.breaks().byId("no-such-break"))).isEmpty();
        rolledBack(t -> t.breaks().addEvidence(m, mine.breakId(), "k", "changed"));
        assertThat(one(t -> t.breaks().byId(mine.breakId()))).contains(mine);
    }

    @Test
    void aRolledBackBreakIsForgotten() {
        Break b = Break.detected(m, BreakType.DUPLICATE_CAPTURE, "pay_3", T0, Map.of());
        rolledBack(t -> t.breaks().raise(b, "dup"));
        assertThat(many(t -> t.breaks().forMerchant(m))).isEmpty();
        assertThat(many(t -> t.breaks().transitions(m, b.breakId()))).isEmpty();
    }

    // ---- settlement: the worker's receipts ----------------------------------------------------------------------

    private ReceivedFile received(String fileId, LocalDate date, Sha256 sha) {
        return new ReceivedFile(m, fileId, date, sha, 2, Money.ofPaise(1_000), T0, ReceiptStatus.ACCEPTED);
    }

    @Test
    void sameBytesAreADuplicateAndADifferentFileForTheSameDateIsAConflict() {
        Sha256 a = sha("a");
        assertThat(receipt(t -> t.settlement().receive(received("stl_1", D0, a)))).isEqualTo(Receipt.NEW);
        assertThat(receipt(t -> t.settlement().receive(received("stl_1", D0, a)))).isEqualTo(Receipt.DUPLICATE);
        assertThat(one(t -> t.settlement().receipt(a))).contains(received("stl_1", D0, a));

        Sha256 b = sha("b");
        assertThat(receipt(t -> t.settlement().receive(received("stl_2", D0, b)))).isEqualTo(Receipt.CONFLICT);
        assertThat(one(t -> t.settlement().receipt(b)).orElseThrow().status()).isEqualTo(ReceiptStatus.REJECTED_CONFLICT);
        assertThat(receipt(t -> t.settlement().receive(received("stl_2", D0, b))))
                .as("the rejected bytes again are a duplicate of the rejected receipt").isEqualTo(Receipt.DUPLICATE);

        Sha256 c = sha("c");
        assertThat(receipt(t -> t.settlement().receive(received("stl_1", D0.plusDays(1), c))))
                .as("the same file id with other bytes").isEqualTo(Receipt.CONFLICT);
        Sha256 d = sha("d");
        assertThat(receipt(t -> t.settlement().receive(received("stl_3", D0.plusDays(1), d)))).isEqualTo(Receipt.NEW);
        assertThat(receipt(t -> t.settlement().receive(new ReceivedFile(other, "stl_1", D0, sha("e"), 0, Money.ZERO, T0,
                ReceiptStatus.ACCEPTED)))).as("another merchant's file").isEqualTo(Receipt.NEW);
    }

    // ---- settlement: the matcher's view -------------------------------------------------------------------------

    private Line line(String fileId, int lineNo, String reference, LineStatus status) {
        return new Line(m, fileId, lineNo, D0, D0, Money.ofPaise(49_000 + lineNo), Optional.of(Money.ofPaise(980)),
                Optional.empty(), Optional.ofNullable(reference), Optional.of("UTR1"), status);
    }

    @Test
    void linesRoundTripInLineNumberOrderAndCanBeUpdated() {
        Line l10 = line("stl_1", 10, "pay_1", LineStatus.MATCHED);
        Line l2 = line("stl_1", 2, null, LineStatus.AWAITING_BATCH);
        Line elsewhere = line("stl_2", 1, "pay_1", LineStatus.CONFLICT);
        run(t -> List.of(l10, l2, elsewhere).forEach(t.settlement()::saveLine));
        assertThat(many(t -> t.settlement().lines(m, "stl_1"))).containsExactly(l2, l10);
        assertThat(one(t -> t.settlement().line(m, "stl_1", 2))).contains(l2);
        assertThat(one(t -> t.settlement().line(m, "stl_1", 3))).isEmpty();
        assertThat(many(t -> t.settlement().linesReferencing(m, "pay_1"))).containsExactly(l10, elsewhere);
        assertThat(many(t -> t.settlement().linesReferencing(other, "pay_1"))).isEmpty();

        run(t -> t.settlement().saveLine(l2.withStatus(LineStatus.UNATTRIBUTED)));
        assertThat(one(t -> t.settlement().line(m, "stl_1", 2)).orElseThrow().status()).isEqualTo(LineStatus.UNATTRIBUTED);
    }

    /** Shift 7 (§6.4): every batch outcome round-trips, and an INFERRED line keeps its candidate hash. */
    @Test
    void batchOutcomesRoundTripAndAnInferredLineKeepsItsCandidateHash() {
        Line waiting = line("stl_9", 1, null, LineStatus.AWAITING_BATCH);
        Line inferred = waiting.inferred(sha("c"));
        run(t -> t.settlement().saveLine(waiting));
        run(t -> t.settlement().saveLine(inferred));
        assertThat(one(t -> t.settlement().line(m, "stl_9", 1))).contains(inferred);
        assertThat(one(t -> t.settlement().line(m, "stl_9", 1)).orElseThrow().candidateHash()).contains(sha("c"));

        Line ambiguous = line("stl_9", 7, null, LineStatus.AWAITING_BATCH)
                .ambiguous(List.of(PaymentId.of("pay_a"), PaymentId.of("pay_b")), List.of(PaymentId.of("pay_c")));
        run(t -> t.settlement().saveLine(ambiguous));
        assertThat(one(t -> t.settlement().line(m, "stl_9", 7))).contains(ambiguous);
        assertThat(one(t -> t.settlement().line(m, "stl_9", 7)).orElseThrow().witnesses())
                .containsExactly(List.of(PaymentId.of("pay_a"), PaymentId.of("pay_b")), List.of(PaymentId.of("pay_c")));

        List<LineStatus> unresolved = List.of(LineStatus.NO_SOLUTION,
                LineStatus.BUDGET_EXCEEDED, LineStatus.ZERO_AMOUNT, LineStatus.OUT_OF_RANGE);
        for (int i = 0; i < unresolved.size(); i++) {
            Line l = line("stl_9", i + 2, null, unresolved.get(i));
            run(t -> t.settlement().saveLine(l));
            assertThat(one(t -> t.settlement().line(m, "stl_9", l.lineNo()))).contains(l);
        }
        assertThat(many(t -> t.settlement().lines(m, "stl_9"))).extracting(Line::status)
                .containsExactly(LineStatus.INFERRED, LineStatus.NO_SOLUTION, LineStatus.BUDGET_EXCEEDED,
                        LineStatus.ZERO_AMOUNT, LineStatus.OUT_OF_RANGE, LineStatus.AMBIGUOUS);

        Match byInference = new Match(m, PaymentId.of("pay_9"), "stl_9", 1, MatchHow.INFERRED);
        assertThat(one(t -> t.settlement().match(byInference))).isEmpty();
        assertThat(one(t -> t.settlement().matchOf(m, PaymentId.of("pay_9")))).contains(byInference);
    }

    @Test
    void onlyACompleteTrailerAnswersForItsDate() {
        FileEnd open = new FileEnd(m, "stl_1", D0, 2, Money.ofPaise(98_003), sha("x"), false);
        run(t -> t.settlement().saveFileEnd(open));
        assertThat(one(t -> t.settlement().fileEnd(m, "stl_1"))).contains(open);
        assertThat(one(t -> t.settlement().completeFile(m, D0))).isEmpty();
        FileEnd done = new FileEnd(m, "stl_1", D0, 2, Money.ofPaise(98_003), sha("x"), true);
        run(t -> t.settlement().saveFileEnd(done));
        assertThat(one(t -> t.settlement().completeFile(m, D0))).contains(done);
        assertThat(one(t -> t.settlement().completeFile(m, D0.plusDays(1)))).isEmpty();
        assertThat(one(t -> t.settlement().completeFile(other, D0))).isEmpty();
    }

    @Test
    void aPaymentIsMatchedAtMostOnce() {
        PaymentId p = PaymentId.of("pay_1");
        Match first = new Match(m, p, "stl_1", 1, MatchHow.REFERENCED);
        assertThat(one(t -> t.settlement().match(first))).isEmpty();
        Match second = new Match(m, p, "stl_2", 4, MatchHow.SHORT_PARTIAL);
        assertThat(one(t -> t.settlement().match(second))).contains(first);
        assertThat(one(t -> t.settlement().matchOf(m, p))).contains(first);
        assertThat(one(t -> t.settlement().matchOf(other, p))).isEmpty();
    }

    @Test
    void rolledBackSettlementStateIsForgotten() {
        rolledBack(t -> {
            t.settlement().receive(received("stl_1", D0, sha("a")));
            t.settlement().saveLine(line("stl_1", 1, "pay_1", LineStatus.MATCHED));
            t.settlement().saveFileEnd(new FileEnd(m, "stl_1", D0, 1, Money.ofPaise(49_001), sha("a"), true));
            t.settlement().match(new Match(m, PaymentId.of("pay_1"), "stl_1", 1, MatchHow.REFERENCED));
        });
        assertThat(one(t -> t.settlement().receipt(sha("a")))).isEmpty();
        assertThat(many(t -> t.settlement().lines(m, "stl_1"))).isEmpty();
        assertThat(one(t -> t.settlement().completeFile(m, D0))).isEmpty();
        assertThat(one(t -> t.settlement().matchOf(m, PaymentId.of("pay_1")))).isEmpty();
    }

    // ---- payment and order queries ------------------------------------------------------------------------------

    private static PaymentState succeeded(String id, String order, Instant at) {
        return new PaymentState(PaymentId.of(id), PaymentStatus.SUCCEEDED, Optional.of(OrderId.of(order)),
                Optional.of(Money.ofPaise(49_900)), Optional.empty(), Optional.of(at), false);
    }

    @Test
    void paymentsAreFoundByOrderAndOpenUntilMatched() {
        PaymentState a = succeeded("pay_a", "ord_1", T0);
        PaymentState b = succeeded("pay_b", "ord_1", T0.plusSeconds(5));
        PaymentState c = succeeded("pay_c", "ord_2", T0);
        run(t -> List.of(b, a, c).forEach(p -> t.payments().save(m, p)));
        assertThat(many(t -> t.payments().forOrder(m, OrderId.of("ord_1")))).containsExactly(a, b);
        assertThat(many(t -> t.payments().forOrder(other, OrderId.of("ord_1")))).isEmpty();
        assertThat(many(t -> t.payments().unsettled(m))).containsExactly(a, b, c);
        run(t -> t.settlement().match(new Match(m, PaymentId.of("pay_b"), "stl_1", 1, MatchHow.REFERENCED)));
        assertThat(many(t -> t.payments().unsettled(m))).containsExactly(a, c);
    }

    @Test
    void theFirstBreakToClaimAPaymentOwnsItsActions() {
        PaymentId p = PaymentId.of("pay_a");
        run(t -> t.payments().save(m, succeeded("pay_a", "ord_1", T0)));
        assertThat(one(t -> t.payments().actionOwner(m, p))).isEmpty();
        assertThat(one(t -> t.payments().claimActionOwner(m, p, "brk_first"))).isEmpty();
        assertThat(one(t -> t.payments().claimActionOwner(m, p, "brk_first"))).as("idempotent").isEmpty();
        assertThat(one(t -> t.payments().claimActionOwner(m, p, "brk_second"))).contains("brk_first");
        assertThat(one(t -> t.payments().actionOwner(m, p))).contains("brk_first");
        assertThatThrownBy(() -> run(t -> t.payments().claimActionOwner(m, PaymentId.of("pay_unknown"), "brk_x")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void openOrdersAreTheOnesStillWaitingOrPaid() {
        Instant expiry = T0.plusSeconds(900);
        Money amount = Money.ofPaise(49_900);
        OrderState created = new OrderState(OrderId.of("ord_1"), OrderStatus.CREATED, 1, Optional.of(amount),
                Optional.of(expiry), Optional.empty());
        OrderState paid = new OrderState(OrderId.of("ord_2"), OrderStatus.PAID, 3, Optional.of(amount),
                Optional.of(expiry), Optional.of(PaymentId.of("pay_2")));
        OrderState fulfilled = new OrderState(OrderId.of("ord_3"), OrderStatus.FULFILLED, 4, Optional.of(amount),
                Optional.of(expiry), Optional.of(PaymentId.of("pay_3")));
        OrderState expired = new OrderState(OrderId.of("ord_4"), OrderStatus.EXPIRED, 3, Optional.of(amount),
                Optional.of(expiry), Optional.empty());
        run(t -> List.of(expired, fulfilled, paid, created).forEach(o -> t.orders().save(m, o)));
        assertThat(many(t -> t.orders().open(m))).containsExactly(created, paid);
        assertThat(many(t -> t.orders().open(other))).isEmpty();
    }
}
