package io.github.vanish073.driftline.sim.matcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventPayload;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.event.SettlementFileEndPayload;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
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
import io.github.vanish073.driftline.core.matcher.MatcherConfig;
import io.github.vanish073.driftline.core.matcher.MerchantDirectory;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.matcher.PartitionBatch;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.SettlementRepo.LineStatus;
import io.github.vanish073.driftline.core.port.SettlementRepo.MatchHow;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.WatermarkRepo.StreamWatermark;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.core.time.IstCalendar;
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
 * The detection side of the matcher (DESIGN §5.4–§5.6, §6.2–§6.5) against the in-memory store: one test per rule, the
 * late-evidence paths, the action-owner rule, settlement outcomes, the watermark cases Shift 5 must prove, the keyed
 * state's safety under rollback, and canary C07. Every batch and tick names its processing time explicitly.
 */
class DetectionTest {

    private static final MerchantId M = MerchantId.of("m_0");
    private static final Money AMOUNT = Money.ofPaise(49_900);
    /** 2% fee = 998, 18% GST on it = 180 (rounded half up), so the expected net is 48,722 paise. */
    private static final long NET = 48_722;
    /** 11:30 IST on 2026-11-13: T = 2026-11-13, due 2026-11-14, missing only once 2026-11-15 is settled. */
    private static final Instant T0 = Instant.parse("2026-11-13T06:00:00Z");
    private static final LocalDate D14 = LocalDate.parse("2026-11-14");
    private static final LocalDate D15 = LocalDate.parse("2026-11-15");
    private static final PaymentId P1 = PaymentId.of("pay_1");
    private static final PaymentId P2 = PaymentId.of("pay_2");
    private static final OrderId O1 = OrderId.of("ord_1");
    private static final MerchantDirectory DIRECTORY = new MerchantDirectory(List.of(
            new MerchantTerms(M, 0, new FeeSchedule(200, 1_800), 1, D14)));

    private final InMemoryStore store = new InMemoryStore();
    private final List<LogRecord> log = new ArrayList<>();
    private Matcher matcher = new Matcher(DIRECTORY);
    private final long epoch = store.transact("assign", tx -> tx.partitions().acquire(0, "matcher-a.1")).epoch();
    private int pendingFrom;

    // ---- building the log ------------------------------------------------------------------------------------------

    private LogRecord append(Source source, EventType type, Instant at, EventPayload payload) {
        String key = "k" + log.size();
        EventEnvelope e = new EventEnvelope(1, M, 0, source, key, type, at, at,
                Sha256.of(key.getBytes(StandardCharsets.UTF_8)), payload, Optional.empty());
        LogRecord r = new LogRecord(0, log.size(), e);
        log.add(r);
        return r;
    }

    private void pay(PaymentId p, EventType type, Instant at) {
        append(Source.GATEWAY, type, at, new PaymentPayload(p, O1, AMOUNT));
    }

    private void order(EventType type, long version, Instant at, PaymentId paidWith) {
        Optional<PaymentId> paid = type.carriesPaidPayment() ? Optional.of(paidWith) : Optional.empty();
        append(Source.MERCHANT, type, at, new OrderPayload(O1, version, AMOUNT, T0.plusSeconds(900), paid));
    }

    private void line(String file, int no, LocalDate date, long paise, Long fee, Long tax, String reference) {
        append(Source.BANK, EventType.SETTLEMENT_LINE, IstCalendar.startOfIstDay(date).plusSeconds(36_000),
                new SettlementLinePayload(file, no, date, date, Money.ofPaise(paise),
                        Optional.ofNullable(fee).map(Money::ofPaise), Optional.ofNullable(tax).map(Money::ofPaise),
                        Optional.ofNullable(reference), Optional.of("UTR1"), "settlement"));
    }

    private void end(String file, LocalDate date, int count, long totalPaise) {
        append(Source.BANK, EventType.SETTLEMENT_FILE_END, IstCalendar.startOfIstDay(date).plusSeconds(36_000),
                new SettlementFileEndPayload(file, date, count, Money.ofPaise(totalPaise),
                        Sha256.of(file.getBytes(StandardCharsets.UTF_8))));
    }

    /** A complete file for {@code date} with no lines. */
    private void emptyFile(LocalDate date) {
        end("stl_" + date, date, 0, 0);
    }

    // ---- running the matcher ---------------------------------------------------------------------------------------

    /** Processes every record appended since the last batch, at processing time {@code now}. */
    private BatchOutcome batch(Instant now) {
        List<LogRecord> records = List.copyOf(log.subList(pendingFrom, log.size()));
        pendingFrom = log.size();
        BatchOutcome outcome = store.transact("batch",
                tx -> matcher.process(new PartitionBatch(0, epoch, now, records), tx));
        matcher.committed(outcome);
        return outcome;
    }

    private BatchOutcome tick(Instant now) {
        BatchOutcome outcome = store.transact("tick", tx -> matcher.tick(0, epoch, now, tx));
        matcher.committed(outcome);
        return outcome;
    }

    private Optional<Break> found(BreakType type, String subject) {
        return store.transact("read", tx -> tx.breaks().find(M, type, subject));
    }

    private Optional<String> owner(PaymentId p) {
        return store.transact("read", tx -> tx.payments().actionOwner(M, p));
    }

    private List<BreakType> typesRaised() {
        return store.committedBreaks().stream().map(Break::type).toList();
    }

    private long balance(AccountCode code) {
        return store.transact("read", tx -> tx.ledger().balance(M, code)).paise();
    }

    /** A paid order and its successful payment, settled by nothing yet. */
    private void paidPayment() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        order(EventType.ORDER_PAID, 2, T0.plusSeconds(11), P1);
        batch(T0.plusSeconds(12));
    }

    // ---- refund outcomes (Shift 8) --------------------------------------------------------------------------------

    @Test
    void aProcessedRefundPostsOnceAgainstClearingAndIsRecordedOnTheActionOwner() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        order(EventType.ORDER_PENDING_PAYMENT, 2, T0, null);
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        batch(T0.plusSeconds(11));
        tick(T0.plus(Duration.ofMinutes(15)));
        Break owner = found(BreakType.DEBITED_NO_ORDER, "pay_1").orElseThrow();
        assertThat(owner(P1)).contains(owner.breakId());
        assertThat(balance(AccountCode.GATEWAY_RECEIVABLE)).isEqualTo(AMOUNT.paise());

        RefundPayload refund = new RefundPayload(P1, AMOUNT);
        append(Source.GATEWAY, EventType.REFUND_PROCESSED, T0.plus(Duration.ofMinutes(20)), refund);
        append(Source.GATEWAY, EventType.REFUND_PROCESSED, T0.plus(Duration.ofMinutes(21)), refund);   // the observation
        batch(T0.plus(Duration.ofMinutes(22)));

        assertThat(store.committedLedger().stream().filter(t -> t.cause().type() == CauseType.REFUND).count())
                .as("one REFUND transaction, however many times the outcome arrives").isEqualTo(1);
        assertThat(balance(AccountCode.GATEWAY_RECEIVABLE)).isZero();
        assertThat(balance(AccountCode.PAYMENTS_CLEARING)).as("never matched to an order: reversed from clearing")
                .isZero();
        assertThat(balance(AccountCode.SALES)).isZero();
        assertThat(found(BreakType.DEBITED_NO_ORDER, "pay_1").orElseThrow().evidence()).containsKey("refund_processed");
    }

    // ---- DEBITED_NO_ORDER (ORDER_CHECK) ----------------------------------------------------------------------------

    @Test
    void debitedNoOrderWaitsForTheWatermarkToPassSuccessPlusGrace() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        order(EventType.ORDER_PENDING_PAYMENT, 2, T0, null);
        pay(P1, EventType.PAYMENT_PENDING, T0.plusSeconds(1));
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        batch(T0.plusSeconds(11));
        assertThat(found(BreakType.DEBITED_NO_ORDER, "pay_1")).isEmpty();

        tick(T0.plus(Duration.ofMinutes(13)));   // idle: wm = now - bound - skew = T0+9m < T0+10m10s
        assertThat(found(BreakType.DEBITED_NO_ORDER, "pay_1")).isEmpty();

        BatchOutcome due = tick(T0.plus(Duration.ofMinutes(15)));   // wm = T0+11m
        Break b = found(BreakType.DEBITED_NO_ORDER, "pay_1").orElseThrow();
        assertThat(due.raised()).containsExactly(b);
        assertThat(b.state()).isEqualTo(BreakState.DETECTED);
        assertThat(b.evidence()).containsEntry("order_status", "PENDING_PAYMENT").containsEntry("order_id", "ord_1");
        assertThat(b.detectedAt()).isEqualTo(T0.plus(Duration.ofMinutes(15)));
        assertThat(owner(P1)).contains(b.breakId());
    }

    @Test
    void anOrderPaidWithThePaymentBeforeTheCheckIsNoBreak() {
        paidPayment();
        tick(T0.plus(Duration.ofHours(1)));
        assertThat(typesRaised()).doesNotContain(BreakType.DEBITED_NO_ORDER);
    }

    @Test
    void anOrderPaidAfterTheBreakIsLateEvidenceOnIt() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        batch(T0.plusSeconds(11));
        tick(T0.plus(Duration.ofMinutes(15)));
        order(EventType.ORDER_PAID, 2, T0.plus(Duration.ofMinutes(16)), P1);
        batch(T0.plus(Duration.ofMinutes(16)));
        Break b = found(BreakType.DEBITED_NO_ORDER, "pay_1").orElseThrow();
        assertThat(b.evidence()).containsKey("order_paid_later");
        assertThat(b.version()).isEqualTo(2);
    }

    // ---- LATE_SUCCESS ----------------------------------------------------------------------------------------------

    @Test
    void aSuccessAfterAFailureIsALateSuccessInEitherArrivalOrder() {
        pay(P1, EventType.PAYMENT_FAILED, T0.plusSeconds(5));
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(90));
        BatchOutcome inOrder = batch(T0.plusSeconds(91));
        assertThat(inOrder.raised()).extracting(Break::type).containsExactly(BreakType.LATE_SUCCESS);
        // D78: the record that raised it is named, so its trace can carry on to the resolver
        long successOffset = inOrder.fates().lastKey();
        assertThat(inOrder.raisedBy()).containsExactly(Map.entry(inOrder.raised().get(0).breakId(), successOffset));

        pay(P2, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(90));
        pay(P2, EventType.PAYMENT_FAILED, T0.plusSeconds(5));
        batch(T0.plusSeconds(92));
        assertThat(found(BreakType.LATE_SUCCESS, "pay_2")).isPresent();
    }

    // ---- DUPLICATE_CAPTURE (DUP_CHECK) -----------------------------------------------------------------------------

    @Test
    void twoCapturesForOneOrderFlagTheLaterOneWhenTheOrderIsUnpaid() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        pay(P2, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(20));
        batch(T0.plusSeconds(21));
        assertThat(found(BreakType.DUPLICATE_CAPTURE, "pay_2")).isEmpty();
        tick(T0.plus(Duration.ofMinutes(7)));   // wm_G = T0+3m >= T0+20s+2m
        assertThat(found(BreakType.DUPLICATE_CAPTURE, "pay_2")).isPresent();
        assertThat(found(BreakType.DUPLICATE_CAPTURE, "pay_1")).as("the earliest capture is kept").isEmpty();
    }

    @Test
    void theOrdersOwnPaymentIsKeptEvenWhenItIsTheLaterCapture() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        pay(P2, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(20));
        order(EventType.ORDER_PAID, 2, T0.plusSeconds(25), P2);
        batch(T0.plusSeconds(26));
        tick(T0.plus(Duration.ofMinutes(7)));
        assertThat(found(BreakType.DUPLICATE_CAPTURE, "pay_1")).isPresent();
        assertThat(found(BreakType.DUPLICATE_CAPTURE, "pay_2")).isEmpty();
    }

    @Test
    void anOrderPaidAfterTheCheckRaisesTheOtherPaymentAndMarksTheKeptOne() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        pay(P2, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(20));
        batch(T0.plusSeconds(21));
        tick(T0.plus(Duration.ofMinutes(7)));
        assertThat(found(BreakType.DUPLICATE_CAPTURE, "pay_2")).isPresent();

        order(EventType.ORDER_PAID, 2, T0.plusSeconds(25), P2);
        batch(T0.plus(Duration.ofMinutes(8)));
        assertThat(found(BreakType.DUPLICATE_CAPTURE, "pay_1")).as("the payment the order was not paid with").isPresent();
        assertThat(found(BreakType.DUPLICATE_CAPTURE, "pay_2").orElseThrow().evidence()).containsKey("kept_by_order");
    }

    @Test
    void aDuplicateOwnsThePaymentOverALateSuccessRaisedInTheSameTransaction() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        pay(P2, EventType.PAYMENT_FAILED, T0.plusSeconds(15));
        batch(T0.plusSeconds(16));
        tick(T0.plus(Duration.ofMinutes(9)));   // wm_G = T0+5m, before the second capture is known

        pay(P2, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(20));   // behind the watermark: DUP_CHECK is due at once
        BatchOutcome both = batch(T0.plus(Duration.ofMinutes(9)).plusSeconds(1));
        assertThat(both.raised()).extracting(Break::type)
                .containsExactly(BreakType.LATE_SUCCESS, BreakType.DUPLICATE_CAPTURE);
        assertThat(both.lateEvents()).isEqualTo(1);
        Break dup = found(BreakType.DUPLICATE_CAPTURE, "pay_2").orElseThrow();
        Break late = found(BreakType.LATE_SUCCESS, "pay_2").orElseThrow();
        assertThat(owner(P2)).contains(dup.breakId());
        assertThat(late.state()).isEqualTo(BreakState.LINKED);
        assertThat(late.evidence()).containsEntry("linked_to", dup.breakId());
    }

    // ---- settlement lines (§6.3, §6.5) -----------------------------------------------------------------------------

    @Test
    void aLinePayingTheExpectedNetMatchesAndPostsTheSettlement() {
        paidPayment();
        line("stl_a", 1, D14, NET, null, null, "pay_1");
        end("stl_a", D14, 1, NET);
        BatchOutcome outcome = batch(T0.plus(Duration.ofDays(1)));
        assertThat(outcome.raised()).isEmpty();
        assertThat(store.committedMatches()).singleElement()
                .satisfies(m -> assertThat(m.how()).isEqualTo(MatchHow.REFERENCED));
        assertThat(store.committedLines()).singleElement()
                .satisfies(l -> assertThat(l.status()).isEqualTo(LineStatus.MATCHED));
        assertThat(store.committedLedger()).extracting(t -> t.cause().type()).contains(CauseType.SETTLEMENT);
        assertThat(balance(AccountCode.GATEWAY_RECEIVABLE)).isZero();
        assertThat(balance(AccountCode.BANK)).isEqualTo(NET);
        assertThat(balance(AccountCode.GATEWAY_FEES)).isEqualTo(998);
        assertThat(balance(AccountCode.GST_INPUT_CREDIT)).isEqualTo(180);
        assertThat(store.committedSettledThrough(M)).contains(D14);
    }

    @Test
    void aShortLineWhoseItemisedFeeAndTaxExplainTheGapIsResolvedAutomatically() {
        paidPayment();
        line("stl_a", 1, D14, NET - 500, 998L + 500, 180L, "pay_1");
        end("stl_a", D14, 1, NET - 500);
        batch(T0.plus(Duration.ofDays(1)));
        Break b = found(BreakType.SHORT_SETTLEMENT, "stl_a:1").orElseThrow();
        assertThat(b.state()).isEqualTo(BreakState.RESOLVED_AUTO);
        assertThat(b.evidence()).containsEntry("pricing_drift", "5.00").containsEntry("shortfall", "5.00");
        assertThat(balance(AccountCode.GATEWAY_RECEIVABLE)).as("fully settled").isZero();
        assertThat(balance(AccountCode.GATEWAY_FEES)).isEqualTo(1_498);
    }

    @Test
    void aShortLineWithoutItemisationIsPostedPartiallyAndLeftForEscalation() {
        paidPayment();
        line("stl_a", 1, D14, NET - 500, null, null, "pay_1");
        end("stl_a", D14, 1, NET - 500);
        batch(T0.plus(Duration.ofDays(1)));
        Break b = found(BreakType.SHORT_SETTLEMENT, "stl_a:1").orElseThrow();
        assertThat(b.state()).isEqualTo(BreakState.DETECTED);
        assertThat(b.evidence()).containsEntry("itemised", "no").containsEntry("expected_net", "487.22");
        assertThat(store.committedLedger()).extracting(t -> t.cause().type()).contains(CauseType.SETTLEMENT_PARTIAL);
        assertThat(balance(AccountCode.GATEWAY_RECEIVABLE)).as("the rest stays open").isEqualTo(49_900 - (NET - 500));
        assertThat(store.committedMatches()).singleElement()
                .satisfies(m -> assertThat(m.how()).isEqualTo(MatchHow.SHORT_PARTIAL));
    }

    @Test
    void itemisationThatDoesNotAddUpIsStillShortAndUnexplained() {
        paidPayment();
        line("stl_a", 1, D14, NET - 500, 998L, 180L, "pay_1");
        end("stl_a", D14, 1, NET - 500);
        batch(T0.plus(Duration.ofDays(1)));
        Break b = found(BreakType.SHORT_SETTLEMENT, "stl_a:1").orElseThrow();
        assertThat(b.state()).isEqualTo(BreakState.DETECTED);
        assertThat(b.evidence()).containsEntry("itemised", "yes");
    }

    @Test
    void aLinePayingMoreThanTheExpectedNetIsAnAnomalyHeldInSuspense() {
        paidPayment();
        line("stl_a", 1, D14, NET + 100, null, null, "pay_1");
        end("stl_a", D14, 1, NET + 100);
        batch(T0.plus(Duration.ofDays(1)));
        assertThat(found(BreakType.OVER_SETTLEMENT, "stl_a:1")).isPresent();
        assertThat(balance(AccountCode.SUSPENSE)).isEqualTo(-(NET + 100));
        assertThat(store.committedMatches()).singleElement()
                .satisfies(m -> assertThat(m.how()).isEqualTo(MatchHow.OVER_HELD));
    }

    @Test
    void aLineWithAnUnknownReferenceIsAnOrphanHeldInSuspense() {
        line("stl_a", 1, D14, 12_345, null, null, "ext_9");
        end("stl_a", D14, 1, 12_345);
        batch(T0.plus(Duration.ofDays(1)));
        Break b = found(BreakType.ORPHAN_SETTLEMENT, "stl_a:1").orElseThrow();
        assertThat(b.evidence()).containsEntry("reason", "UNKNOWN_REFERENCE").containsEntry("reference", "ext_9");
        assertThat(balance(AccountCode.SUSPENSE)).isEqualTo(-12_345);
        assertThat(balance(AccountCode.BANK)).isEqualTo(12_345);
    }

    @Test
    void aLineThatArrivesBeforeItsPaymentIsAttributedWhenThePaymentSucceeds() {
        line("stl_a", 1, D14, NET, null, null, "pay_1");
        end("stl_a", D14, 1, NET);
        batch(T0.plus(Duration.ofDays(1)));
        assertThat(found(BreakType.ORPHAN_SETTLEMENT, "stl_a:1")).isPresent();

        order(EventType.ORDER_CREATED, 1, T0, null);
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        order(EventType.ORDER_PAID, 2, T0.plusSeconds(11), P1);
        batch(T0.plus(Duration.ofDays(1)).plusSeconds(1));

        assertThat(found(BreakType.ORPHAN_SETTLEMENT, "stl_a:1").orElseThrow().evidence())
                .containsEntry("attributed_to", "pay_1");
        assertThat(store.committedLedger()).extracting(t -> t.cause().type()).contains(CauseType.SUSPENSE_RELEASE);
        assertThat(balance(AccountCode.SUSPENSE)).isZero();
        assertThat(balance(AccountCode.GATEWAY_RECEIVABLE)).isZero();
        assertThat(store.committedLines()).singleElement()
                .satisfies(l -> assertThat(l.status()).isEqualTo(LineStatus.MATCHED));
    }

    @Test
    void aSecondLineForAnAlreadySettledPaymentIsASettlementConflict() {
        paidPayment();
        line("stl_a", 1, D14, NET, null, null, "pay_1");
        line("stl_a", 2, D14, NET, null, null, "pay_1");
        end("stl_a", D14, 2, 2 * NET);
        batch(T0.plus(Duration.ofDays(1)));
        Break b = found(BreakType.SETTLEMENT_CONFLICT, "stl_a:2").orElseThrow();
        assertThat(b.evidence()).containsEntry("settled_by", "stl_a:1");
        assertThat(balance(AccountCode.SUSPENSE)).isEqualTo(-NET);
    }

    // ---- watermarks: late versus missing (§5.4) --------------------------------------------------------------------

    @Test
    void aLateFileNeverProducesAFalseMissingButAStalledFeedRaisesAnAlert() {
        paidPayment();
        emptyFile(D15);                      // the 2026-11-14 file is late; 2026-11-15 arrived first
        batch(T0.plus(Duration.ofDays(2)));
        for (int day = 2; day <= 6; day++) {
            tick(T0.plus(Duration.ofDays(day)));
        }
        assertThat(store.committedSettledThrough(M)).as("swm cannot pass the missing date").contains(D14.minusDays(1));
        assertThat(found(BreakType.MISSING_SETTLEMENT, "pay_1")).isEmpty();
        assertThat(found(BreakType.FEED_STALL, D14.toString())).as("an operational alert").isPresent();

        line("stl_late", 1, D14, NET, null, null, "pay_1");
        end("stl_late", D14, 1, NET);
        batch(T0.plus(Duration.ofDays(6)).plusSeconds(1));
        assertThat(store.committedSettledThrough(M)).contains(D15);
        assertThat(found(BreakType.MISSING_SETTLEMENT, "pay_1")).isEmpty();
        assertThat(typesRaised()).containsOnly(BreakType.FEED_STALL);
    }

    @Test
    void aPaymentAbsentFromEveryFileUpToItsDueDatePlusGraceIsMissing() {
        paidPayment();
        emptyFile(D14);
        batch(T0.plus(Duration.ofDays(1)));
        assertThat(found(BreakType.MISSING_SETTLEMENT, "pay_1")).as("the grace day is still open").isEmpty();
        emptyFile(D15);
        BatchOutcome outcome = batch(T0.plus(Duration.ofDays(2)));
        Break b = found(BreakType.MISSING_SETTLEMENT, "pay_1").orElseThrow();
        assertThat(outcome.raised()).containsExactly(b);
        assertThat(b.evidence()).containsEntry("due_date", D14.toString()).containsEntry("settled_through", D15.toString());
    }

    @Test
    void anIdleSourceStillAdvancesItsWatermark() {
        Instant now = T0.plus(Duration.ofHours(3));
        tick(now);
        StreamWatermark g = store.transact("read", tx -> tx.watermarks().stream(0, Source.GATEWAY)).orElseThrow();
        assertThat(g.value()).isEqualTo(now.minus(Duration.ofMinutes(4)));
        assertThat(g.maxEventTime()).isEmpty();

        pay(P1, EventType.PAYMENT_PENDING, T0);   // far behind the watermark now
        BatchOutcome late = batch(now.plusSeconds(1));
        assertThat(late.lateEvents()).isEqualTo(1);
        assertThat(late.applied()).as("a late event is still processed").isEqualTo(1);
        assertThat(store.transact("read", tx -> tx.watermarks().stream(0, Source.GATEWAY)).orElseThrow().value())
                .as("never moves back").isEqualTo(now.minus(Duration.ofMinutes(4)));
    }

    @Test
    void aBusySourceDoesNotUseTheIdleRule() {
        pay(P1, EventType.PAYMENT_PENDING, T0);
        batch(T0.plus(Duration.ofHours(1)));
        StreamWatermark g = store.transact("read", tx -> tx.watermarks().stream(0, Source.GATEWAY)).orElseThrow();
        assertThat(g.value()).as("max event time minus the bound").isEqualTo(T0.minus(Duration.ofMinutes(2)));
    }

    @Test
    void theNextWakeUpPointsAtTheEarliestTimerThatIsNotDueYet() {
        order(EventType.ORDER_CREATED, 1, T0, null);
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(10));
        BatchOutcome outcome = batch(T0.plusSeconds(11));
        assertThat(outcome.nextWakeAt()).as("ORDER_CHECK at T0+10m10s, plus bound, skew and idle allowances")
                .contains(T0.plusSeconds(10).plus(Duration.ofMinutes(10 + 2 + 2 + 1)));
    }

    // ---- fencing, keyed state, canary ------------------------------------------------------------------------------

    @Test
    void aZombiesTickIsFencedLikeItsBatches() {
        store.transact("reassign", tx -> tx.partitions().acquire(0, "matcher-b.1"));
        assertThatThrownBy(() -> tick(T0)).isInstanceOf(StoreConflict.class);
        assertThat(store.committedOffsetCommits()).isEmpty();
    }

    @Test
    void aRolledBackBatchLeavesNothingStaleInTheCache() {
        pay(P1, EventType.PAYMENT_PENDING, T0);
        List<LogRecord> first = List.copyOf(log.subList(pendingFrom, log.size()));
        assertThatThrownBy(() -> store.transact("crash", tx -> {
            matcher.process(new PartitionBatch(0, epoch, T0, first), tx);
            throw new IllegalStateException("crash before commit");
        })).hasMessage("crash before commit");
        assertThat(matcher.keyedState().size()).as("a write is uncached until its batch commits").isZero();

        BatchOutcome replay = batch(T0.plusSeconds(1));   // the same records again, now committed
        assertThat(replay.applied()).isEqualTo(1);
        assertThat(matcher.keyedState().size()).isEqualTo(1);

        long hits = matcher.keyedState().hits();
        pay(P1, EventType.PAYMENT_SUCCEEDED, T0.plusSeconds(5));
        batch(T0.plusSeconds(6));
        assertThat(matcher.keyedState().hits()).isGreaterThan(hits);
        matcher.ownershipChanged(0);
        assertThat(matcher.keyedState().size()).isZero();
    }

    @Test
    void canaryC07DecidesMissingByTheProcessingClockAndIgnoresALateFile() {
        matcher = new Matcher(DIRECTORY, MatcherConfig.DEFAULT, Canaries.of(Canary.PROCESSING_TIME_WATERMARK));
        paidPayment();
        tick(IstCalendar.endOfIstDay(D14).plusSeconds(1));   // no file has arrived at all
        assertThat(found(BreakType.MISSING_SETTLEMENT, "pay_1")).as("the planted bug").isPresent();
    }
}
