package io.github.vanish073.driftline.core.resolver;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakLifecycle;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.breaks.DeadlineRule;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.Cause;
import io.github.vanish073.driftline.core.ledger.CauseType;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.order.OrderState;
import io.github.vanish073.driftline.core.order.OrderStatus;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.port.BreakRepo.Plan;
import io.github.vanish073.driftline.core.port.OutboxRepo.Command;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandKind;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandStatus;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.time.IstCalendar;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The resolver (DESIGN §8): it reviews due breaks and, for each, chooses the next step of its playbook (§8.2) in the
 * caller's transaction: read the break and the current payment and order state, decide, then write the break's
 * transition, its plan and any outbox command (§8.1). It never calls anything itself; the dispatcher sends what it
 * writes (§9), and results come back as facts that wake the break again.
 *
 * <p>Safety rules it keeps:
 * <ul>
 *   <li>Only a payment's action-owner break acts (D11); the refund key is derived from the payment, so two breaks or
 *       two decisions can never refund it twice. A refund is decided at most once ({@code breaks.action}).</li>
 *   <li>It never guesses (D14): contradictory gateway records, an order it does not know (D15), an order with an open
 *       IDEMPOTENCY_CONFLICT (D41), or a gateway that still disagrees after {@code maxStatusQueries} queries are
 *       escalated. No external action rests on an inferred settlement match (D56): the refund playbooks read only
 *       payment and order state.</li>
 *   <li>Every break is resolved or escalated by its {@code escalate_by} (§8.3).</li>
 * </ul>
 *
 * <p>Crash point CP1 sits after the decisions and before the caller commits: a crash there leaves nothing behind, and
 * the next review decides again from scratch.
 */
public final class Resolver {

    /**
     * @param maxStatusQueries how many status queries may disagree with Driftline before STATUS_CONFLICT (§8.2 step 3)
     * @param recheckAfter     the latest a waiting break is looked at again, even if nothing wakes it
     * @param requeryAfter     the gap before a status query is repeated (the status API may be stale)
     */
    public record Config(int maxStatusQueries, Duration recheckAfter, Duration requeryAfter) {
        public static final Config DEFAULT = new Config(3, Duration.ofMinutes(10), Duration.ofMinutes(2));

        public Config {
            if (maxStatusQueries < 1) {
                throw new IllegalArgumentException("at least one status query");
            }
            Objects.requireNonNull(recheckAfter, "recheckAfter");
            Objects.requireNonNull(requeryAfter, "requeryAfter");
        }
    }

    /** What one review step decided, in review order, as {@code breakId → state after}. */
    public record Outcome(List<String> decisions) {
        public Outcome {
            decisions = List.copyOf(decisions);
        }
    }

    private final Config config;
    private final CrashPoints crashPoints;
    private final boolean ignoreEscalateBy;
    private final boolean noRefundKey;
    private final boolean breakScopedRefundKey;

    public Resolver(Config config, Canaries canaries, CrashPoints crashPoints) {
        this.config = Objects.requireNonNull(config, "config");
        this.crashPoints = Objects.requireNonNull(crashPoints, "crashPoints");
        this.ignoreEscalateBy = canaries.isEnabled(Canary.IGNORE_ESCALATE_BY);
        this.noRefundKey = canaries.isEnabled(Canary.NO_REFUND_KEY);
        this.breakScopedRefundKey = canaries.isEnabled(Canary.BREAK_SCOPED_REFUND_KEY);
    }

    /** The production resolver: default constants, no canary, no crash point. */
    public Resolver() {
        this(Config.DEFAULT, Canaries.NONE, CrashPoints.NONE);
    }

    /** The refund idempotency key of a payment (§9.1): {@code H("refund", merchant, paymentId)}. */
    public static String refundKey(MerchantId m, PaymentId p) {
        return "rfd_" + DerivedId.of("refund", m.value(), p.value()).value();
    }

    /** The merchant-confirm idempotency key (§9.1): {@code H("confirm", merchant, orderId, paymentId)}. */
    public static String confirmKey(MerchantId m, OrderId o, PaymentId p) {
        return "cnf_" + DerivedId.of("confirm", m.value(), o.value(), p.value()).value();
    }

    /** Reviews up to {@code limit} due breaks in {@code tx}, at processing time {@code now}. */
    public Outcome step(Tx tx, Instant now, int limit) {
        List<String> decisions = new ArrayList<>();
        for (Break b : tx.breaks().dueForReview(now, limit)) {
            Review r = new Review(tx, b, now);
            r.decide();
            decisions.add(b.breakId() + "->" + r.state);
        }
        if (!decisions.isEmpty()) {
            crashPoints.reached(CrashPoint.CP1);
        }
        return new Outcome(decisions);
    }

    // ---------------------------------------------------------------------------------------------------------------

    /** One break's review. */
    private final class Review {
        final Tx tx;
        final Break b;
        final MerchantId m;
        final Instant now;
        Plan plan;
        BreakState state;
        final List<Command> commands;

        Review(Tx tx, Break b, Instant now) {
            this.tx = tx;
            this.b = b;
            this.m = b.merchantId();
            this.now = now;
            this.state = b.state();
            this.plan = tx.breaks().planOf(m, b.breakId()).orElseGet(this::newPlan);
            this.commands = new ArrayList<>(tx.outbox().forBreak(m, b.breakId()));
        }

        Plan newPlan() {
            DeadlineRule rule = DeadlineRule.forType(b.type());
            LocalDate tDate = paymentSubject()
                    .flatMap(p -> tx.payments().find(m, p))
                    .flatMap(PaymentState::successAt)
                    .map(IstCalendar::tDate)
                    .orElse(IstCalendar.istDate(b.detectedAt()));
            Instant deadline = rule.deadlineAt(tDate, b.detectedAt());
            Instant escalateBy = rule.escalateBy(deadline);
            return new Plan(rule, deadline, escalateBy, now, Optional.empty(), Optional.empty(), Money.ZERO);
        }

        Optional<PaymentId> paymentSubject() {
            return switch (b.type()) {
                case DEBITED_NO_ORDER, LATE_SUCCESS, DUPLICATE_CAPTURE, MISSING_SETTLEMENT ->
                        Optional.of(PaymentId.of(b.subjectKey()));
                default -> Optional.empty();
            };
        }

        void decide() {
            if (!ignoreEscalateBy && !now.isBefore(plan.escalateBy())) {
                escalate("escalate_by " + plan.escalateBy() + " has passed");
                return;
            }
            switch (b.type()) {
                case DEBITED_NO_ORDER, LATE_SUCCESS, DUPLICATE_CAPTURE -> refundPlaybook();
                case ORPHAN_SETTLEMENT -> {
                    if (b.evidence().containsKey("attributed_to")) {
                        close(BreakState.SELF_HEALED, "attributed to " + b.evidence().get("attributed_to"));
                    } else {
                        // D64: a line naming a payment Driftline does not know may be a capture whose webhooks were
                        // lost; ask the gateway, whose answer is published as an observation and attributes the line
                        Optional<PaymentId> ref = Optional.ofNullable(b.evidence().get("reference")).map(PaymentId::of);
                        moveTo(BreakState.AWAITING_EVIDENCE, "waiting for attribution until escalate_by");
                        waitUntil(plan.escalateBy());
                        ref.ifPresent(this::askAboutReference);   // may look again sooner, for a re-query
                    }
                }
                default -> escalate("evidence gathered; " + b.type() + " is for a person (§8.2)");
            }
        }

        // ---- the refund playbooks (§8.2): DEBITED_NO_ORDER, LATE_SUCCESS, DUPLICATE_CAPTURE ----

        void refundPlaybook() {
            PaymentId p = PaymentId.of(b.subjectKey());
            Optional<String> owner = tx.payments().actionOwner(m, p);
            // C11 plants both halves of D11 off: a break-scoped key, and every break acting for itself
            if (!breakScopedRefundKey && owner.isPresent() && !owner.get().equals(b.breakId())) {
                if (BreakLifecycle.allowed(state, BreakState.LINKED)) {
                    close(BreakState.LINKED, "follows " + owner.get());   // D11: the owner acts for the payment
                } else {
                    escalate("another break owns this payment's actions but this one already acted");
                }
                return;
            }
            if (tx.ledger().find(m, new Cause(CauseType.REFUND, p.value())).isPresent()) {
                close(BreakState.RESOLVED_AUTO, "refund confirmed");
                return;
            }
            if (b.evidence().containsKey("refund_failed")) {
                escalate("the gateway reported the refund failed");
                return;
            }
            boolean refundDecided = plan.action().filter("REFUND"::equals).isPresent();
            Optional<PaymentState> payment = tx.payments().find(m, p);
            if (payment.isEmpty() || payment.get().status() != PaymentStatus.SUCCEEDED || payment.get().conflict()) {
                escalate("the payment is no longer a clean success (D14)");
                return;
            }
            OrderId orderId = payment.get().orderId().orElseThrow();
            OrderState order = tx.orders().find(m, orderId).orElse(OrderState.unknown(orderId));
            boolean paidWithThis = order.paidWith().filter(p::equals).isPresent()
                    && (order.status() == OrderStatus.PAID || order.status() == OrderStatus.FULFILLED);
            boolean keptByOrder = b.type() == BreakType.DUPLICATE_CAPTURE && b.evidence().containsKey("kept_by_order");
            if (paidWithThis || keptByOrder) {
                if (refundDecided) {
                    escalate("the order was paid with this payment after its refund was decided");
                } else if (b.type() == BreakType.LATE_SUCCESS && confirmAccepted()) {
                    close(BreakState.RESOLVED_AUTO, "the merchant accepted the late success and the order is paid");
                } else if (!BreakLifecycle.allowed(state, BreakState.SELF_HEALED)) {
                    // BUG-004: Driftline already asked the merchant to confirm, so the fix is not the world's alone
                    close(BreakState.RESOLVED_AUTO, "the order is paid with this payment while a confirmation was pending");
                } else {
                    close(BreakState.SELF_HEALED, "the order is paid with this payment");
                }
                return;
            }
            if (refundDecided) {
                moveTo(BreakState.ACTION_PENDING, "refund in flight");
                waitUntil(now.plus(config.recheckAfter()));
                return;
            }
            if (openIdempotencyConflict(orderId)) {
                escalate("the order has an open IDEMPOTENCY_CONFLICT (D41)");
                return;
            }
            Confirmation confirmed = confirmSucceeded(p);
            if (b.type() == BreakType.DUPLICATE_CAPTURE && confirmed == Confirmation.YES) {
                Optional<PaymentId> kept = Optional.ofNullable(b.evidence().get("kept")).map(PaymentId::of);
                if (kept.isEmpty()) {
                    escalate("the duplicate names no kept payment");
                    return;
                }
                confirmed = confirmSucceeded(kept.get());
                if (confirmed == Confirmation.CONFLICT) {
                    escalate("the kept payment is not confirmed as succeeded, so this one may be the only real one");
                    return;
                }
            }
            switch (confirmed) {
                case WAITING -> {
                    moveTo(BreakState.AWAITING_EVIDENCE, "status query sent");
                    waitUntil(now.plus(config.recheckAfter()));
                    return;
                }
                case CONFLICT -> {
                    tx.breaks().raise(Break.detected(m, BreakType.STATUS_CONFLICT, p.value(), now,
                            kv("payment_id", p.value(), "queries", Integer.toString(config.maxStatusQueries()))),
                            "the gateway does not confirm the success");
                    escalate("the gateway does not confirm the success after " + config.maxStatusQueries()
                            + " queries");
                    return;
                }
                case YES -> {
                    // fall through to the order
                }
            }
            if (b.type() == BreakType.DUPLICATE_CAPTURE) {
                refund(p, payment.get(), orderId, "a duplicate capture: refund the payment the order is not paid with");
                return;
            }
            if (order.status() == OrderStatus.NONE) {
                escalate("the merchant has never reported this order (D15)");
                return;
            }
            boolean paidWithOther = order.paidWith().isPresent() && !paidWithThis;
            if (order.status() == OrderStatus.EXPIRED || order.status() == OrderStatus.CANCELLED || paidWithOther) {
                refund(p, payment.get(), orderId, "the order is " + order.status()
                        + (paidWithOther ? " with another payment" : ""));
                return;
            }
            // CREATED or PENDING_PAYMENT: the order may still take this payment
            if (b.type() == BreakType.LATE_SUCCESS) {
                Optional<Command> confirm = latest(CommandKind.MERCHANT_CONFIRM);
                if (confirm.isEmpty()) {
                    enqueue(CommandKind.MERCHANT_CONFIRM, kv("order_id", orderId.value(), "payment_id", p.value(),
                            "amount", payment.get().amount().orElseThrow().toString()),
                            Optional.of(confirmKey(m, orderId, p)), now);
                    moveTo(BreakState.ACTION_PENDING, "asked the merchant to confirm the late success");
                    waitUntil(now.plus(config.recheckAfter()));
                } else if (confirm.get().status() != CommandStatus.DONE) {
                    waitUntil(now.plus(config.recheckAfter()));
                } else if ("ACCEPTED".equals(confirm.get().result().orElseThrow().get("status"))) {
                    waitUntil(now.plus(config.recheckAfter()));   // order.paid with this payment will follow
                } else {
                    refund(p, payment.get(), orderId, "the merchant rejected the late success");
                }
                return;
            }
            moveTo(BreakState.AWAITING_EVIDENCE, "the order is still " + order.status());
            waitUntil(order.expiresAt().filter(t -> t.isAfter(now)).orElse(now.plus(config.recheckAfter())));
        }

        boolean confirmAccepted() {
            return latest(CommandKind.MERCHANT_CONFIRM).flatMap(Command::result)
                    .filter(r -> "ACCEPTED".equals(r.get("status"))).isPresent();
        }

        boolean openIdempotencyConflict(OrderId orderId) {
            for (Break other : tx.breaks().forMerchant(m)) {
                if (other.type() == BreakType.IDEMPOTENCY_CONFLICT && orderId.value().equals(other.evidence().get("order_id"))
                        && !BreakLifecycle.isFinal(other.state())) {
                    return true;
                }
            }
            return false;
        }

        /** Step 1 of the refund playbooks: is the success confirmed by the gateway's status API? */
        Confirmation confirmSucceeded(PaymentId p) {
            List<Command> queries = commands.stream()
                    .filter(c -> c.kind() == CommandKind.STATUS_QUERY_PAYMENT && p.value().equals(c.args().get("payment_id")))
                    .sorted(Comparator.comparingInt(Review::seq)).toList();
            if (queries.isEmpty()) {
                enqueue(CommandKind.STATUS_QUERY_PAYMENT, kv("payment_id", p.value()), Optional.empty(), now);
                return Confirmation.WAITING;
            }
            Command last = queries.get(queries.size() - 1);
            if (last.status() != CommandStatus.DONE) {
                return Confirmation.WAITING;
            }
            if ("SUCCEEDED".equals(last.result().orElseThrow().get("status"))) {
                return Confirmation.YES;
            }
            if (queries.size() >= config.maxStatusQueries()) {
                return Confirmation.CONFLICT;
            }
            enqueue(CommandKind.STATUS_QUERY_PAYMENT, kv("payment_id", p.value()), Optional.empty(),
                    now.plus(config.requeryAfter()));
            return Confirmation.WAITING;
        }

        /**
         * Asks the gateway about an orphan's reference: once, and again after {@code requeryAfter} while the answer is
         * not final (a stale status API may still say pending), up to {@code maxStatusQueries}. The answer itself is
         * not used here: the dispatcher publishes a new outcome, the matcher attributes the line, and this break
         * heals.
         */
        void askAboutReference(PaymentId p) {
            List<Command> queries = commands.stream()
                    .filter(c -> c.kind() == CommandKind.STATUS_QUERY_PAYMENT && p.value().equals(c.args().get("payment_id")))
                    .sorted(Comparator.comparingInt(Review::seq)).toList();
            if (queries.isEmpty()) {
                enqueue(CommandKind.STATUS_QUERY_PAYMENT, kv("payment_id", p.value()), Optional.empty(), now);
                return;
            }
            Command last = queries.get(queries.size() - 1);
            String status = last.result().map(r -> r.getOrDefault("status", "")).orElse("");
            boolean finalAnswer = status.equals("SUCCEEDED") || status.equals("FAILED");
            if (last.status() == CommandStatus.DONE && !finalAnswer && queries.size() < config.maxStatusQueries()) {
                enqueue(CommandKind.STATUS_QUERY_PAYMENT, kv("payment_id", p.value()), Optional.empty(),
                        now.plus(config.requeryAfter()));
                waitUntil(now.plus(config.requeryAfter()));
            }
        }

        void refund(PaymentId p, PaymentState payment, OrderId orderId, String why) {
            Optional<String> key = noRefundKey ? Optional.empty()
                    : Optional.of(breakScopedRefundKey ? "rfd_" + DerivedId.of("refund", b.breakId()).value()
                            : refundKey(m, p));
            enqueue(CommandKind.REFUND, kv("payment_id", p.value(), "order_id", orderId.value(),
                    "amount", payment.amount().orElseThrow().toString()), key, now);
            plan = new Plan(plan.rule(), plan.deadlineAt(), plan.escalateBy(), plan.reviewAt(), Optional.of("REFUND"),
                    plan.resolvedAt(), plan.compensation());
            moveTo(BreakState.ACTION_PENDING, "refund decided: " + why);
            waitUntil(now.plus(config.recheckAfter()));
        }

        // ---- commands ----

        static int seq(Command c) {
            return Integer.parseInt(c.args().get("seq"));
        }

        Optional<Command> latest(CommandKind kind) {
            return commands.stream().filter(c -> c.kind() == kind).max(Comparator.comparingInt(Review::seq));
        }

        /** A command with an id derived from the break, its kind and its sequence among that kind (D4). */
        void enqueue(CommandKind kind, Map<String, String> args, Optional<String> key, Instant at) {
            int seq = (int) commands.stream().filter(c -> c.kind() == kind).count() + 1;
            TreeMap<String, String> all = new TreeMap<>(args);
            all.put("seq", Integer.toString(seq));
            String id = "cmd_" + DerivedId.of("command", b.breakId(), kind.name(), Integer.toString(seq)).value();
            Command c = Command.pending(id, m, Optional.of(b.breakId()), kind, all, key, at);
            tx.outbox().enqueue(c);
            commands.add(c);
        }

        // ---- moves ----

        void escalate(String why) {
            enqueue(CommandKind.ESCALATION_NOTIFY, kv("type", b.type().name(), "subject", b.subjectKey(),
                    "reason", truncate(why)), Optional.empty(), now);
            moveTo(BreakState.ESCALATED, why);
            plan = new Plan(plan.rule(), plan.deadlineAt(), plan.escalateBy(), plan.reviewAt(), plan.action(),
                    Optional.empty(), plan.rule().compensation(plan.deadlineAt(), now));
            save();
        }

        void close(BreakState to, String why) {
            moveTo(to, why);
            plan = new Plan(plan.rule(), plan.deadlineAt(), plan.escalateBy(), plan.reviewAt(), plan.action(),
                    Optional.of(now), plan.rule().compensation(plan.deadlineAt(), now));
            save();
        }

        void moveTo(BreakState to, String why) {
            if (state == to) {
                return;
            }
            if (!BreakLifecycle.allowed(state, to)) {
                throw new IllegalStateException(b.breakId() + ": " + state + " -> " + to + " is not in the lifecycle");
            }
            tx.breaks().transition(m, b.breakId(), to, now, truncate(why));
            state = to;
        }

        /** Look again at {@code at}, but never after escalate_by. */
        void waitUntil(Instant at) {
            Instant review = at.isAfter(plan.escalateBy()) ? plan.escalateBy() : at;
            if (ignoreEscalateBy) {
                // C10: the deadline is forgotten, so a break keeps being looked at, never escalated on time
                review = at.isAfter(now) ? at : now.plus(config.recheckAfter());
            }
            plan = plan.withReviewAt(review);
            save();
        }

        void save() {
            tx.breaks().savePlan(m, b.breakId(), plan);
        }
    }

    private enum Confirmation {
        YES,
        WAITING,
        CONFLICT
    }

    /** Key-value pairs as a sorted map (the core never uses Map.of, whose iteration order is salted per JVM). */
    private static TreeMap<String, String> kv(String... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("pairs come in twos");
        }
        TreeMap<String, String> out = new TreeMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put(pairs[i], pairs[i + 1]);
        }
        return out;
    }

    private static String truncate(String s) {
        return s.length() <= Break.MAX_EVIDENCE_VALUE ? s : s.substring(0, Break.MAX_EVIDENCE_VALUE);
    }
}
