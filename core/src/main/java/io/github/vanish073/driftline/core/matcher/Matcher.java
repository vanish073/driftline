package io.github.vanish073.driftline.core.matcher;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.SettlementFileEndPayload;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.ledger.PostingRules;
import io.github.vanish073.driftline.core.money.Money;
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
import io.github.vanish073.driftline.core.port.BreakRepo;
import io.github.vanish073.driftline.core.port.DedupeRepo;
import io.github.vanish073.driftline.core.port.QuarantineRepo;
import io.github.vanish073.driftline.core.port.SettlementRepo.FileEnd;
import io.github.vanish073.driftline.core.port.SettlementRepo.Line;
import io.github.vanish073.driftline.core.port.SettlementRepo.LineStatus;
import io.github.vanish073.driftline.core.port.SettlementRepo.Match;
import io.github.vanish073.driftline.core.port.SettlementRepo.MatchHow;
import io.github.vanish073.driftline.core.port.TimerRepo.Kind;
import io.github.vanish073.driftline.core.port.TimerRepo.Timer;
import io.github.vanish073.driftline.core.port.TimerRepo.TimerClock;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.port.WatermarkRepo.StreamWatermark;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.core.state.Decision;
import io.github.vanish073.driftline.core.state.DecisionKind;
import io.github.vanish073.driftline.core.time.IstCalendar;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The matcher (DESIGN §5). Sans-I/O: the driver hands it one {@link PartitionBatch} (or a tick) and the transaction to
 * run it in, and everything the batch does happens in that one transaction (§5.7):
 * <ol>
 *   <li>per record: skip it if below the stored offset (a replay); claim its dedupe row (D3), quarantining a key reused
 *       with a different payload (§10.3); apply it: the payment or order state machine with CAPTURE and ORDER_MATCH
 *       postings, or a settlement line or trailer (§6.2, §6.3, §6.5); note its event time for the stream watermark;</li>
 *   <li>advance the stream watermarks (bounded lateness, idle advance; §5.4) and the settlement watermarks (file
 *       completeness), then fire every due timer in firing order (§5.5), which runs the detection rules (§5.6);</li>
 *   <li>give each new refund-capable break its payment's action owner, in precedence order (D11);</li>
 *   <li>commit the offset under the batch's epoch: a fenced instance rolls everything back (§5.1). A tick commits
 *       the unchanged offset for the same reason.</li>
 * </ol>
 *
 * <p>Canaries: C01 ({@link Canary#DEDUPE_OFF}) skips the dedupe claim; C07
 * ({@link Canary#PROCESSING_TIME_WATERMARK}) decides "missing" by the processing clock at the end of the due date
 * instead of by the settlement watermark; C06 lives in {@link PaymentStateMachine}.
 *
 * <p>Not here yet: refunds and the resolver (Shift 8), batch matching of unreferenced lines (Shift 7), the pending-order
 * probe (Shift 8).
 */
public final class Matcher {

    static final int MAX_TIMER_ROUNDS = 10_000;
    private static final Comparator<Timer> FIRING_ORDER = Comparator.comparing(Timer::fireAt)
            .thenComparing(t -> t.kind().name())
            .thenComparing(Timer::subjectKey)
            .thenComparing(t -> t.merchantId().value());

    private final MerchantDirectory merchants;
    private final MatcherConfig config;
    private final PaymentStateMachine payments;
    private final OrderStateMachine orders = new OrderStateMachine();
    private final boolean dedupeOff;
    private final boolean processingTimeMissing;
    private final KeyedState state = new KeyedState();

    public Matcher(MerchantDirectory merchants, MatcherConfig config, Canaries canaries) {
        this.merchants = merchants;
        this.config = config;
        this.payments = new PaymentStateMachine(canaries);
        this.dedupeOff = canaries.isEnabled(Canary.DEDUPE_OFF);
        this.processingTimeMissing = canaries.isEnabled(Canary.PROCESSING_TIME_WATERMARK);
    }

    /** The production matcher: default constants, no canary. */
    public Matcher(MerchantDirectory merchants) {
        this(merchants, MatcherConfig.DEFAULT, Canaries.NONE);
    }

    public KeyedState keyedState() {
        return state;
    }

    // ---- the driver's API -----------------------------------------------------------------------------------------

    public BatchOutcome process(PartitionBatch batch, Tx tx) {
        Run run = new Run(tx, batch.partition(), batch.now());
        int applied = 0;
        int alreadyProcessed = 0;
        int duplicates = 0;
        int keyReused = 0;
        int notYetHandled = 0;
        long next = tx.partitions().nextOffset(batch.partition());
        long committed = next;

        for (var record : batch.records()) {
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
                    run.idempotencyConflict(e, record.partition(), record.offset());
                    keyReused++;
                }
                case FIRST -> {
                    run.observe(e);
                    switch (e.eventType()) {
                        case PAYMENT_PENDING, PAYMENT_SUCCEEDED, PAYMENT_FAILED -> {
                            run.applyPayment(e);
                            applied++;
                        }
                        case ORDER_CREATED, ORDER_PENDING_PAYMENT, ORDER_PAID, ORDER_FULFILLED, ORDER_EXPIRED,
                             ORDER_CANCELLED -> {
                            run.applyOrder(e);
                            applied++;
                        }
                        case SETTLEMENT_LINE -> {
                            run.settlementLine(e);
                            applied++;
                        }
                        case SETTLEMENT_FILE_END -> {
                            run.fileEnd(e);
                            applied++;
                        }
                        case REFUND_PROCESSED, REFUND_FAILED -> notYetHandled++;
                    }
                }
            }
        }
        run.finish();
        tx.partitions().commitOffset(batch.partition(), batch.epoch(), committed);
        return run.outcome(applied, alreadyProcessed, duplicates, keyReused, notYetHandled);
    }

    /**
     * Advances idle watermarks and fires due timers with no new records (§5.4: "if idle on a tick"). Fenced like a
     * batch: it commits the unchanged offset under {@code epoch}.
     */
    public BatchOutcome tick(int partition, long epoch, Instant now, Tx tx) {
        Run run = new Run(tx, partition, now);
        run.finish();
        tx.partitions().commitOffset(partition, epoch, tx.partitions().nextOffset(partition));
        return run.outcome(0, 0, 0, 0, 0);
    }

    /** Loads the partition's open payments and orders (§5.2). Apply the result with {@link #committed(KeyedState.Delta)}. */
    public KeyedState.Delta warm(int partition, Tx tx) {
        KeyedState.Recorder recorder = new KeyedState.Recorder();
        for (MerchantTerms t : merchants.inPartition(partition)) {
            for (PaymentState p : tx.payments().unsettled(t.merchantId())) {
                recorder.payment(partition, KeyedState.paymentKey(t.merchantId(), p.paymentId()), p, true);
            }
            for (OrderState o : tx.orders().open(t.merchantId())) {
                recorder.order(partition, KeyedState.orderKey(t.merchantId(), o.orderId()), o);
            }
        }
        return recorder.delta();
    }

    /** The batch's transaction committed: the cache may learn its writes. */
    public void committed(BatchOutcome outcome) {
        state.apply(outcome.delta());
    }

    public void committed(KeyedState.Delta delta) {
        state.apply(delta);
    }

    /** The partition was acquired or lost: whatever is cached for it may be stale. */
    public void ownershipChanged(int partition) {
        state.dropPartition(partition);
    }

    /** The process restarted: nothing in memory survives. */
    public void forgetAll() {
        state.clear();
    }

    // ---- one batch or tick ----------------------------------------------------------------------------------------

    /** A stream watermark being advanced in this run. */
    private static final class Stream {
        Instant value;
        Optional<Instant> maxEventTime;
        Optional<Instant> lastEventAt;
        boolean changed;

        Stream(Instant value, Optional<Instant> maxEventTime, Optional<Instant> lastEventAt) {
            this.value = value;
            this.maxEventTime = maxEventTime;
            this.lastEventAt = lastEventAt;
        }
    }

    /** A new refund-capable break waiting for its payment's action owner (D11). */
    private record Claim(MerchantId merchantId, PaymentId paymentId, BreakType type, String breakId) {
    }

    private static Instant later(Instant a, Instant b) {
        return a.isAfter(b) ? a : b;
    }

    private static TreeMap<String, String> evidence(String... keyValues) {
        TreeMap<String, String> out = new TreeMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            out.put(keyValues[i], keyValues[i + 1]);
        }
        return out;
    }

    private final class Run {
        private final Tx tx;
        private final int partition;
        private final Instant now;
        private final List<MerchantTerms> partitionMerchants;
        private final TreeMap<Source, Stream> streams = new TreeMap<>();
        private final TreeSet<String> dirty = new TreeSet<>();
        private final KeyedState.Recorder recorder = new KeyedState.Recorder();
        private final List<Break> raised = new ArrayList<>();
        private final List<Claim> claims = new ArrayList<>();
        private final List<PostResult.LedgerConflict> conflicts = new ArrayList<>();
        private int posted;
        private int timersFired;
        private int lateEvents;

        Run(Tx tx, int partition, Instant now) {
            this.tx = tx;
            this.partition = partition;
            this.now = now;
            this.partitionMerchants = merchants.inPartition(partition);
        }

        BatchOutcome outcome(int applied, int alreadyProcessed, int duplicates, int keyReused, int notYetHandled) {
            return new BatchOutcome(applied, alreadyProcessed, duplicates, keyReused, notYetHandled, posted, conflicts,
                    raised, timersFired, lateEvents, nextWakeAt(), recorder.delta());
        }

        // ---- reads and writes through the keyed-state cache ----

        Optional<PaymentState> payment(MerchantId m, PaymentId id) {
            String key = KeyedState.paymentKey(m, id);
            if (!dirty.contains(key)) {
                Optional<PaymentState> cached = state.payment(key);
                if (cached.isPresent()) {
                    return cached;
                }
            }
            return tx.payments().find(m, id);
        }

        Optional<OrderState> order(MerchantId m, OrderId id) {
            String key = KeyedState.orderKey(m, id);
            if (!dirty.contains(key)) {
                Optional<OrderState> cached = state.order(key);
                if (cached.isPresent()) {
                    return cached;
                }
            }
            return tx.orders().find(m, id);
        }

        void savePayment(MerchantId m, PaymentState p) {
            String key = KeyedState.paymentKey(m, p.paymentId());
            state.invalidate(key);
            dirty.add(key);
            tx.payments().save(m, p);
            recorder.payment(partition, key, p, true);
        }

        /** The payment is settled: evict it from the cache once this batch commits (§5.2). */
        void closePayment(MerchantId m, PaymentId id) {
            String key = KeyedState.paymentKey(m, id);
            state.invalidate(key);
            dirty.add(key);
            recorder.close(partition, key);
        }

        void saveOrder(MerchantId m, OrderState o) {
            String key = KeyedState.orderKey(m, o.orderId());
            state.invalidate(key);
            dirty.add(key);
            tx.orders().save(m, o);
            recorder.order(partition, key, o);
        }

        void post(LedgerTxn txn) {
            PostResult result = tx.ledger().post(txn);
            if (result instanceof PostResult.Posted) {
                posted++;
            } else if (result instanceof PostResult.LedgerConflict conflict) {
                conflicts.add(conflict);
            }
        }

        void upsertTimer(MerchantId m, Kind kind, String subject, TimerClock clock, Instant fireAt) {
            tx.timers().upsert(new Timer(m, kind, subject, merchants.terms(m).partition(), clock, fireAt));
        }

        BreakRepo.Raised raise(MerchantId m, BreakType type, String subject, TreeMap<String, String> facts,
                               String reason) {
            BreakRepo.Raised r = tx.breaks().raise(Break.detected(m, type, subject, now, facts), reason);
            if (r.created()) {
                raised.add(r.current());
            }
            return r;
        }

        void raiseRefundCapable(MerchantId m, PaymentId p, BreakType type, TreeMap<String, String> facts,
                                String reason) {
            BreakRepo.Raised r = raise(m, type, p.value(), facts, reason);
            if (r.created()) {
                claims.add(new Claim(m, p, type, r.current().breakId()));
            }
        }

        /**
         * A record refused because its key was already used with another body (§10.3) is quarantined, and escalated as
         * an IDEMPOTENCY_CONFLICT anomaly naming the order or payment it was about: Driftline's view of that order may
         * now be incomplete (it may never learn which payment the order was paid with), so no automatic action on it
         * can be trusted (D14). Raised once per refused record.
         */
        void idempotencyConflict(EventEnvelope e, int recordPartition, long offset) {
            TreeMap<String, String> facts = evidence("event_type", e.eventType().wireName(),
                    "partition", Integer.toString(recordPartition), "offset", Long.toString(offset));
            if (e.payload() instanceof OrderPayload o) {
                facts.put("order_id", o.orderId().value());
            } else if (e.payload() instanceof PaymentPayload p) {
                facts.put("payment_id", p.paymentId().value());
                facts.put("order_id", p.orderId().value());
            }
            raise(e.merchantId(), BreakType.IDEMPOTENCY_CONFLICT, e.source() + ":" + e.eventKey() + "@" + offset, facts,
                    "key reused with a different body");
        }

        // ---- payments (§4.3, §5.6) ----

        void applyPayment(EventEnvelope e) {
            MerchantId m = e.merchantId();
            PaymentEvidence ev = PaymentEvidence.from(e);
            PaymentState before = payment(m, ev.paymentId()).orElse(PaymentState.unknown(ev.paymentId()));
            Decision<PaymentState> d = payments.decide(before, ev);
            PaymentState after = d.state();
            savePayment(m, after);
            tx.payments().appendTransition(m, new PaymentTransition(ev.paymentId(), before.status(), after.status(),
                    (PaymentRule) d.rule(), e.eventKey(), e.eventTime()));
            if (d.kind() == DecisionKind.CONFLICT) {
                raise(m, BreakType.STATUS_CONFLICT, ev.paymentId().value(),
                        evidence("rule", ((PaymentRule) d.rule()).name(), "event_key", e.eventKey()), "conflicting records");
                return;
            }
            if (d.kind() != DecisionKind.APPLIED) {
                return;
            }
            if (after.status() == PaymentStatus.SUCCEEDED) {
                post(PostingRules.capture(m, after.paymentId(), after.amount().orElseThrow(), after.successAt().orElseThrow()));
                order(m, after.orderId().orElseThrow()).ifPresent(o -> matchIfPaid(m, o, after, e.eventTime()));
                boolean newSuccess = before.status() != PaymentStatus.SUCCEEDED
                        || !before.successAt().equals(after.successAt());
                if (newSuccess && !after.conflict()) {
                    onSuccess(m, after, e.eventTime());
                }
            }
            if (!before.lateSuccess() && after.lateSuccess() && !after.conflict()) {
                raiseRefundCapable(m, after.paymentId(), BreakType.LATE_SUCCESS,
                        evidence("failed_at", after.failedAt().orElseThrow().toString(),
                                "success_at", after.successAt().orElseThrow().toString(),
                                "order_id", after.orderId().orElseThrow().value()), "success after a failure");
            }
        }

        /** First success (or an earlier one, D24 d): register the checks that hang off it, attribute early lines. */
        void onSuccess(MerchantId m, PaymentState p, Instant at) {
            MerchantTerms terms = merchants.terms(m);
            Instant success = p.successAt().orElseThrow();
            String id = p.paymentId().value();
            upsertTimer(m, Kind.ORDER_CHECK, id, TimerClock.STREAM_GM, success.plus(config.orderGrace()));
            LocalDate due = terms.dueDate(IstCalendar.tDate(success));
            if (processingTimeMissing) {
                upsertTimer(m, Kind.SETTLEMENT_DUE, id, TimerClock.PROCESSING, IstCalendar.endOfIstDay(due));
            } else {
                upsertTimer(m, Kind.SETTLEMENT_DUE, id, TimerClock.SETTLEMENT,
                        IstCalendar.startOfIstDay(due.plusDays(config.graceDays())));
            }
            List<PaymentState> succeeded = succeededFor(m, p.orderId().orElseThrow());
            if (succeeded.size() >= 2) {
                Instant latest = succeeded.stream().map(x -> x.successAt().orElseThrow()).max(Comparator.naturalOrder())
                        .orElseThrow();
                upsertTimer(m, Kind.DUP_CHECK, p.orderId().orElseThrow().value(), TimerClock.STREAM_G,
                        latest.plus(config.boundGateway()));
            }
            for (Line line : tx.settlement().linesReferencing(m, id)) {
                if (line.status() == LineStatus.UNATTRIBUTED) {
                    settleReferenced(line, true, at);
                }
            }
        }

        List<PaymentState> succeededFor(MerchantId m, OrderId orderId) {
            List<PaymentState> out = new ArrayList<>();
            for (PaymentState p : tx.payments().forOrder(m, orderId)) {
                if (p.status() == PaymentStatus.SUCCEEDED && !p.conflict()) {
                    out.add(p);
                }
            }
            return out;
        }

        // ---- orders (§4.4) ----

        void applyOrder(EventEnvelope e) {
            MerchantId m = e.merchantId();
            OrderEvidence ev = OrderEvidence.from(e);
            OrderState before = order(m, ev.orderId()).orElse(OrderState.unknown(ev.orderId()));
            Decision<OrderState> d = orders.decide(before, ev);
            OrderState after = d.state();
            saveOrder(m, after);
            tx.orders().appendTransition(m, new OrderTransition(ev.orderId(), before.status(), after.status(),
                    ev.version(), (OrderRule) d.rule(), e.eventKey(), e.eventTime()));
            if (d.kind() == DecisionKind.CONFLICT) {
                raise(m, BreakType.ORDER_ANOMALY, ev.orderId().value(),
                        evidence("rule", ((OrderRule) d.rule()).name(), "version", Long.toString(ev.version())),
                        "contradictory order history");
                return;
            }
            if (d.kind() != DecisionKind.APPLIED || after.paidWith().isEmpty()) {
                return;
            }
            PaymentId paidWith = after.paidWith().get();
            payment(m, paidWith).ifPresent(p -> matchIfPaid(m, after, p, e.eventTime()));
            tx.breaks().find(m, BreakType.DEBITED_NO_ORDER, paidWith.value()).ifPresent(b -> tx.breaks()
                    .addEvidence(m, b.breakId(), "order_paid_later", "v" + after.version() + " at " + e.eventTime()));
            if (tx.timers().find(m, Kind.DUP_CHECK, ev.orderId().value()).isEmpty()) {
                evaluateDuplicates(m, ev.orderId());
            }
        }

        /**
         * ORDER_MATCH once both sides are known: the order is paid with this payment, the payment succeeded without a
         * conflict, and the amounts agree. Whichever side arrives second triggers it; the ledger's cause uniqueness
         * absorbs the repeat when a later event re-checks (§7.4).
         */
        void matchIfPaid(MerchantId m, OrderState order, PaymentState payment, Instant at) {
            PaymentId paymentId = payment.paymentId();
            boolean paidWithThis = order.paidWith().filter(paymentId::equals).isPresent();
            boolean succeeded = payment.status() == PaymentStatus.SUCCEEDED && !payment.conflict();
            boolean sameAmount = order.amount().equals(payment.amount());
            if (paidWithThis && succeeded && sameAmount) {
                post(PostingRules.orderMatch(m, paymentId, payment.amount().orElseThrow(), at));
            }
        }

        // ---- detection rules (§5.6) ----

        /** ORDER_CHECK: debited, but the order is not paid with this payment nor with another one. */
        void evaluateDebitedNoOrder(MerchantId m, PaymentId id) {
            Optional<PaymentState> found = payment(m, id);
            if (found.isEmpty() || found.get().status() != PaymentStatus.SUCCEEDED || found.get().conflict()) {
                return;
            }
            PaymentState p = found.get();
            OrderId orderId = p.orderId().orElseThrow();
            OrderState o = order(m, orderId).orElse(OrderState.unknown(orderId));
            if (o.paidWith().isPresent()) {
                return;   // paid with this payment, or with another one (a duplicate, DUP_CHECK's job)
            }
            raiseRefundCapable(m, id, BreakType.DEBITED_NO_ORDER,
                    evidence("order_id", orderId.value(), "order_status", o.status().name(),
                            "success_at", p.successAt().orElseThrow().toString(),
                            "wm_gm", streamGM().toString()), "ORDER_CHECK");
        }

        /** DUP_CHECK, or an order paid after it: every successful payment of the order except the kept one. */
        void evaluateDuplicates(MerchantId m, OrderId orderId) {
            List<PaymentState> succeeded = succeededFor(m, orderId);
            if (succeeded.size() < 2) {
                return;
            }
            Optional<PaymentId> paidWith = order(m, orderId).flatMap(OrderState::paidWith);
            PaymentState kept = succeeded.stream()
                    .filter(p -> paidWith.filter(p.paymentId()::equals).isPresent())
                    .findFirst()
                    .orElseGet(() -> succeeded.stream()
                            .min(Comparator.comparing((PaymentState p) -> p.successAt().orElseThrow())
                                    .thenComparing(p -> p.paymentId().value()))
                            .orElseThrow());
            List<String> all = new ArrayList<>();
            succeeded.forEach(p -> all.add(p.paymentId().value()));
            for (PaymentState p : succeeded) {
                if (p.paymentId().equals(kept.paymentId())) {
                    continue;
                }
                raiseRefundCapable(m, p.paymentId(), BreakType.DUPLICATE_CAPTURE,
                        evidence("order_id", orderId.value(), "kept", kept.paymentId().value(),
                                "payments", String.join(" ", all)), "DUP_CHECK");
            }
            tx.breaks().find(m, BreakType.DUPLICATE_CAPTURE, kept.paymentId().value()).ifPresent(b -> tx.breaks()
                    .addEvidence(m, b.breakId(), "kept_by_order", "order paid with this payment"));
        }

        /** SETTLEMENT_DUE: succeeded, and still not settled once every file up to its due date plus grace is complete. */
        void evaluateMissing(MerchantId m, PaymentId id) {
            Optional<PaymentState> found = payment(m, id);
            if (found.isEmpty() || found.get().status() != PaymentStatus.SUCCEEDED || found.get().conflict()
                    || tx.settlement().matchOf(m, id).isPresent()) {
                return;
            }
            LocalDate tDate = IstCalendar.tDate(found.get().successAt().orElseThrow());
            raise(m, BreakType.MISSING_SETTLEMENT, id.value(),
                    evidence("t_date", tDate.toString(), "due_date", merchants.terms(m).dueDate(tDate).toString(),
                            "settled_through", settledThrough(merchants.terms(m)).toString()), "SETTLEMENT_DUE");
        }

        /** FEED_STALL: an operational alert; no payment is flagged (§5.4). */
        void evaluateStall(MerchantId m, LocalDate date) {
            if (tx.settlement().completeFile(m, date).isEmpty()) {
                raise(m, BreakType.FEED_STALL, date.toString(),
                        evidence("expected_by", stallAt(date).toString()), "no complete file");
            }
        }

        // ---- settlement (§6.2, §6.3, §6.5) ----

        void settlementLine(EventEnvelope e) {
            SettlementLinePayload l = (SettlementLinePayload) e.payload();
            MerchantId m = e.merchantId();
            if (tx.settlement().line(m, l.fileId(), l.lineNo()).isPresent()) {
                raise(m, BreakType.FILE_CONFLICT, l.fileId(), evidence("line", Integer.toString(l.lineNo()),
                        "event_key", e.eventKey()), "a second file with the same file id");
                return;
            }
            Line line = new Line(m, l.fileId(), l.lineNo(), l.settlementDate(), l.valueDate(), l.amount(), l.fee(),
                    l.tax(), l.reference(), l.batchId(), LineStatus.AWAITING_BATCH);
            if (l.reference().isEmpty()) {
                suspenseIn(line, e.eventTime());
                tx.settlement().saveLine(line);
            } else {
                settleReferenced(line, false, e.eventTime());
            }
            checkComplete(m, l.fileId());
        }

        /**
         * A line that names a payment. {@code late} means the line was held as UNATTRIBUTED (its money is already in
         * SUSPENSE) and the payment it names has just become known as captured.
         */
        void settleReferenced(Line line, boolean late, Instant at) {
            MerchantId m = line.merchantId();
            PaymentId id = PaymentId.of(line.reference().orElseThrow());
            Optional<PaymentState> found = payment(m, id);
            if (found.isEmpty() || found.get().status() != PaymentStatus.SUCCEEDED || found.get().conflict()) {
                if (!late) {
                    suspenseIn(line, at);
                    tx.settlement().saveLine(line.withStatus(LineStatus.UNATTRIBUTED));
                    raise(m, BreakType.ORPHAN_SETTLEMENT, line.key(),
                            evidence("reason", found.isEmpty() ? "UNKNOWN_REFERENCE" : "REFERENCE_NOT_CAPTURED",
                                    "reference", id.value(), "amount", line.amount().toString()), "line");
                }
                return;
            }
            Optional<Match> existing = tx.settlement().matchOf(m, id);
            if (existing.isPresent()) {
                if (!late) {
                    suspenseIn(line, at);
                }
                tx.settlement().saveLine(line.withStatus(LineStatus.CONFLICT));
                raise(m, BreakType.SETTLEMENT_CONFLICT, line.key(), evidence("payment_id", id.value(),
                        "settled_by", existing.get().fileId() + ":" + existing.get().lineNo()), "already settled");
                attributed(line, late, id);
                return;
            }
            PaymentState p = found.get();
            Money gross = p.amount().orElseThrow();
            FeeSchedule.Breakdown expected = merchants.terms(m).fees().expected(gross);
            Money received = line.amount();
            LineStatus status;
            MatchHow how;
            int cmp = received.compareTo(expected.net());
            if (cmp == 0) {
                status = LineStatus.MATCHED;
                how = MatchHow.REFERENCED;
                post(late
                        ? PostingRules.suspenseRelease(m, line.fileId(), line.lineNo(), received, expected.fee(),
                                expected.gst(), gross, at)
                        : PostingRules.settlement(m, line.fileId(), line.lineNo(), received, expected.fee(),
                                expected.gst(), gross, at));
            } else if (cmp < 0) {
                boolean explained = line.fee().isPresent() && line.tax().isPresent()
                        && gross.minus(received).equals(line.fee().get().plus(line.tax().get()));
                TreeMap<String, String> facts = evidence("payment_id", id.value(), "gross", gross.toString(),
                        "expected_net", expected.net().toString(), "received", received.toString(),
                        "shortfall", expected.net().minus(received).toString(),
                        "itemised", line.fee().isPresent() && line.tax().isPresent() ? "yes" : "no");
                if (explained) {
                    status = LineStatus.SHORT_EXPLAINED;
                    how = MatchHow.SHORT_EXPLAINED;
                    Money fee = line.fee().get();
                    Money tax = line.tax().get();
                    facts.put("pricing_drift", fee.plus(tax).minus(expected.fee().plus(expected.gst())).toString());
                    post(late
                            ? PostingRules.suspenseRelease(m, line.fileId(), line.lineNo(), received, fee, tax, gross, at)
                            : PostingRules.settlement(m, line.fileId(), line.lineNo(), received, fee, tax, gross, at));
                    BreakRepo.Raised r = raise(m, BreakType.SHORT_SETTLEMENT, line.key(), facts, "short line");
                    if (r.created()) {
                        tx.breaks().transition(m, r.current().breakId(), BreakState.RESOLVED_AUTO, now,
                                "itemised fee and tax explain the gap");
                    }
                } else {
                    status = LineStatus.SHORT_UNEXPLAINED;
                    how = MatchHow.SHORT_PARTIAL;
                    if (received.isPositive()) {
                        post(late
                                ? PostingRules.suspenseRelease(m, line.fileId(), line.lineNo(), received, Money.ZERO,
                                        Money.ZERO, received, at)
                                : PostingRules.settlementPartial(m, line.fileId(), line.lineNo(), received, at));
                    }
                    raise(m, BreakType.SHORT_SETTLEMENT, line.key(), facts, "short line");
                }
            } else {
                status = LineStatus.OVER;
                how = MatchHow.OVER_HELD;
                if (!late) {
                    suspenseIn(line, at);
                }
                raise(m, BreakType.OVER_SETTLEMENT, line.key(), evidence("payment_id", id.value(),
                        "expected_net", expected.net().toString(), "received", received.toString()), "over-settled");
            }
            tx.settlement().match(new Match(m, id, line.fileId(), line.lineNo(), how));
            tx.settlement().saveLine(line.withStatus(status));
            closePayment(m, id);
            attributed(line, late, id);
        }

        /** Late evidence on the orphan break of a line that has now been attributed. */
        void attributed(Line line, boolean late, PaymentId id) {
            if (late) {
                tx.breaks().find(line.merchantId(), BreakType.ORPHAN_SETTLEMENT, line.key()).ifPresent(b ->
                        tx.breaks().addEvidence(line.merchantId(), b.breakId(), "attributed_to", id.value()));
            }
        }

        void suspenseIn(Line line, Instant at) {
            if (line.amount().isPositive()) {
                post(PostingRules.suspenseIn(line.merchantId(), line.fileId(), line.lineNo(), line.amount(), at));
            }
        }

        void fileEnd(EventEnvelope e) {
            SettlementFileEndPayload f = (SettlementFileEndPayload) e.payload();
            MerchantId m = e.merchantId();
            Optional<FileEnd> sameDate = tx.settlement().completeFile(m, f.settlementDate())
                    .filter(x -> !x.fileId().equals(f.fileId()));
            Optional<FileEnd> sameId = tx.settlement().fileEnd(m, f.fileId())
                    .filter(x -> !x.sha256().equals(f.fileSha256()));
            if (sameDate.isPresent() || sameId.isPresent()) {
                raise(m, BreakType.FILE_CONFLICT, f.fileId(), evidence("settlement_date", f.settlementDate().toString(),
                        "sha256", f.fileSha256().hex()), "a second file for a date or file id");
                return;
            }
            if (tx.settlement().fileEnd(m, f.fileId()).isEmpty()) {
                tx.settlement().saveFileEnd(new FileEnd(m, f.fileId(), f.settlementDate(), f.lineCount(),
                        f.totalAmount(), f.fileSha256(), false));
            }
            checkComplete(m, f.fileId());
        }

        /** COMPLETE once the trailer has been read and lines 1..N are all there with the trailer's total (§6.2). */
        void checkComplete(MerchantId m, String fileId) {
            Optional<FileEnd> end = tx.settlement().fileEnd(m, fileId);
            if (end.isEmpty() || end.get().complete()) {
                return;
            }
            List<Line> lines = tx.settlement().lines(m, fileId);
            Money sum = Money.ZERO;
            for (Line l : lines) {
                sum = sum.plus(l.amount());
            }
            boolean all = lines.size() == end.get().lineCount()
                    && (lines.isEmpty() || lines.get(lines.size() - 1).lineNo() == lines.size());
            if (all && sum.equals(end.get().totalAmount())) {
                FileEnd e = end.get();
                tx.settlement().saveFileEnd(new FileEnd(m, fileId, e.settlementDate(), e.lineCount(), e.totalAmount(),
                        e.sha256(), true));
                advanceSettlement(merchants.terms(m));
            }
        }

        LocalDate settledThrough(MerchantTerms t) {
            return tx.watermarks().settledThrough(t.merchantId()).orElse(t.firstSettlementDate().minusDays(1));
        }

        /** {@code swm}: the greatest date up to which every expected file is COMPLETE (calendar DAILY, §5.4). */
        void advanceSettlement(MerchantTerms t) {
            LocalDate swm = settledThrough(t);
            LocalDate start = swm;
            while (tx.settlement().completeFile(t.merchantId(), swm.plusDays(1)).isPresent()) {
                swm = swm.plusDays(1);
            }
            if (!swm.equals(start)) {
                tx.watermarks().saveSettledThrough(t.merchantId(), swm);
                registerStall(t.merchantId(), swm.plusDays(1));
            }
        }

        Instant stallAt(LocalDate date) {
            return IstCalendar.startOfIstDay(date).plus(config.expectedArrival()).plus(config.stallTimeout());
        }

        void registerStall(MerchantId m, LocalDate date) {
            upsertTimer(m, Kind.FEED_STALL, date.toString(), TimerClock.PROCESSING, stallAt(date));
        }

        // ---- watermarks and timers (§5.4, §5.5) ----

        Stream stream(Source source) {
            return streams.computeIfAbsent(source, s -> tx.watermarks().stream(partition, s)
                    .map(w -> new Stream(w.value(), w.maxEventTime(), w.lastEventAt()))
                    .orElseGet(() -> new Stream(Instant.EPOCH, Optional.empty(), Optional.empty())));
        }

        Instant streamGM() {
            Instant g = stream(Source.GATEWAY).value;
            Instant mm = stream(Source.MERCHANT).value;
            return g.isBefore(mm) ? g : mm;
        }

        void observe(EventEnvelope e) {
            if (e.source() != Source.GATEWAY && e.source() != Source.MERCHANT) {
                return;
            }
            Stream s = stream(e.source());
            if (e.eventTime().isBefore(s.value)) {
                lateEvents++;
            }
            s.maxEventTime = Optional.of(s.maxEventTime.map(t -> later(t, e.eventTime())).orElse(e.eventTime()));
            s.lastEventAt = Optional.of(now);
            s.changed = true;
        }

        Duration bound(Source source) {
            return source == Source.GATEWAY ? config.boundGateway() : config.boundMerchant();
        }

        void advanceWatermarks() {
            for (Source source : List.of(Source.GATEWAY, Source.MERCHANT)) {
                Stream s = stream(source);
                Instant value = s.value;
                if (s.maxEventTime.isPresent()) {
                    value = later(value, s.maxEventTime.get().minus(bound(source)));
                }
                boolean idle = s.lastEventAt.isEmpty() || !now.isBefore(s.lastEventAt.get().plus(config.idleTimeout()));
                if (idle) {
                    value = later(value, now.minus(bound(source)).minus(config.skewAllowance()));
                }
                if (!value.equals(s.value) || s.changed) {
                    s.value = value;
                    s.changed = false;
                    tx.watermarks().saveStream(new StreamWatermark(partition, source, value, s.maxEventTime,
                            s.lastEventAt));
                }
            }
        }

        void finish() {
            for (MerchantTerms t : partitionMerchants) {
                if (tx.watermarks().settledThrough(t.merchantId()).isEmpty()) {
                    tx.watermarks().saveSettledThrough(t.merchantId(), t.firstSettlementDate().minusDays(1));
                    registerStall(t.merchantId(), t.firstSettlementDate());
                }
            }
            advanceWatermarks();
            fireDueTimers();
            claimActionOwners();
        }

        void fireDueTimers() {
            for (int round = 0; ; round++) {
                if (round > MAX_TIMER_ROUNDS) {
                    throw new IllegalStateException("timers keep falling due in partition " + partition);
                }
                List<Timer> due = new ArrayList<>();
                due.addAll(tx.timers().due(partition, TimerClock.STREAM_GM, streamGM()));
                due.addAll(tx.timers().due(partition, TimerClock.STREAM_G, stream(Source.GATEWAY).value));
                due.addAll(tx.timers().due(partition, TimerClock.PROCESSING, now));
                for (MerchantTerms t : partitionMerchants) {
                    due.addAll(tx.timers().dueForMerchant(t.merchantId(), TimerClock.SETTLEMENT,
                            IstCalendar.startOfIstDay(settledThrough(t))));
                }
                if (due.isEmpty()) {
                    return;
                }
                due.sort(FIRING_ORDER);
                for (Timer t : due) {
                    tx.timers().delete(t.merchantId(), t.kind(), t.subjectKey());
                    timersFired++;
                    switch (t.kind()) {
                        case ORDER_CHECK -> evaluateDebitedNoOrder(t.merchantId(), PaymentId.of(t.subjectKey()));
                        case DUP_CHECK -> evaluateDuplicates(t.merchantId(), OrderId.of(t.subjectKey()));
                        case SETTLEMENT_DUE -> evaluateMissing(t.merchantId(), PaymentId.of(t.subjectKey()));
                        case FEED_STALL -> evaluateStall(t.merchantId(), LocalDate.parse(t.subjectKey()));
                        case PENDING_PROBE, BATCH_SOLVE -> {
                            // never registered before Shifts 8 and 7
                        }
                    }
                }
            }
        }

        /** D11: the first refund-capable break of a payment owns its actions; later ones follow it as LINKED. */
        void claimActionOwners() {
            claims.sort(Comparator.comparingInt((Claim c) -> BreakType.REFUND_CAPABLE.indexOf(c.type()))
                    .thenComparing(c -> c.merchantId().value())
                    .thenComparing(c -> c.paymentId().value()));
            for (Claim c : claims) {
                Optional<String> owner = tx.payments().claimActionOwner(c.merchantId(), c.paymentId(), c.breakId());
                if (owner.isPresent()) {
                    tx.breaks().transition(c.merchantId(), c.breakId(), BreakState.LINKED, now,
                            "follows the action owner of " + c.paymentId());
                    tx.breaks().addEvidence(c.merchantId(), c.breakId(), "linked_to", owner.get());
                }
            }
        }

        /**
         * When a tick could fire a timer that is not due yet. A stream timer at F falls due once the watermark passes
         * F; if the sources go quiet that happens by the idle rule at about F + bound + skewAllowance + idleTimeout.
         */
        Optional<Instant> nextWakeAt() {
            Duration slack = config.skewAllowance().plus(config.idleTimeout());
            Duration maxBound = config.boundGateway().compareTo(config.boundMerchant()) >= 0
                    ? config.boundGateway() : config.boundMerchant();
            List<Instant> candidates = new ArrayList<>();
            tx.timers().earliest(partition, TimerClock.PROCESSING).ifPresent(candidates::add);
            tx.timers().earliest(partition, TimerClock.STREAM_GM).ifPresent(f -> candidates.add(f.plus(maxBound).plus(slack)));
            tx.timers().earliest(partition, TimerClock.STREAM_G)
                    .ifPresent(f -> candidates.add(f.plus(config.boundGateway()).plus(slack)));
            return candidates.stream().min(Comparator.naturalOrder()).map(t -> later(t, now.plusSeconds(1)));
        }
    }
}
