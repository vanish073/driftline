package io.github.vanish073.driftline.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.order.OrderEvidence;
import io.github.vanish073.driftline.core.order.OrderState;
import io.github.vanish073.driftline.core.order.OrderStateMachine;
import io.github.vanish073.driftline.core.order.OrderStatus;
import io.github.vanish073.driftline.core.order.OrderTransitionTable;
import io.github.vanish073.driftline.core.payment.PaymentEvidence;
import io.github.vanish073.driftline.core.payment.PaymentRule;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentStateMachine;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.state.Decision;
import io.github.vanish073.driftline.core.state.DecisionKind;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.statistics.Statistics;

/**
 * DESIGN §4.5: take any multiset of events with no contradictions; deliver it in any permutation, with any
 * duplicates; the final payment and order states must equal those of delivery in event-time order.
 *
 * <p>The second property covers what §4.5 leaves out: for any multiset, contradictory or not, the payment conflict
 * flag is raised in every delivery order exactly when the records contradict each other (D24).
 */
class OrderInsensitivityProperties {

    private static final Instant T0 = Instant.parse("2026-11-13T06:00:00Z");
    private static final OrderId ORDER = OrderId.of("ord_1");
    private static final PaymentId PAY_1 = PaymentId.of("pay_1");
    private static final Comparator<Delivery> EVENT_TIME_ORDER =
            Comparator.comparing(Delivery::eventTime).thenComparing(Delivery::key);

    /** One delivery. The same object delivered twice is an exact re-delivery (same event key). */
    record Delivery(String key, Instant eventTime, Object evidence) {
    }

    /** Applies deliveries the way the matcher will: dedupe on the event key first (D3), then the state machine. */
    static final class Replay {
        private final PaymentStateMachine paymentMachine = new PaymentStateMachine();
        private final OrderStateMachine orderMachine = new OrderStateMachine();
        final Set<String> applied = new TreeSet<>();
        final Map<PaymentId, PaymentState> payments = new TreeMap<>();
        final Map<OrderId, OrderState> orders = new TreeMap<>();
        final List<Decision<?>> decisions = new ArrayList<>();

        static Replay of(List<Delivery> deliveries) {
            Replay replay = new Replay();
            deliveries.forEach(replay::deliver);
            return replay;
        }

        void deliver(Delivery delivery) {
            if (!applied.add(delivery.key())) {
                return;
            }
            switch (delivery.evidence()) {
                case PaymentEvidence e -> {
                    Decision<PaymentState> d = paymentMachine.decide(
                            payments.getOrDefault(e.paymentId(), PaymentState.unknown(e.paymentId())), e);
                    payments.put(e.paymentId(), d.state());
                    decisions.add(d);
                }
                case OrderEvidence e -> {
                    Decision<OrderState> d = orderMachine.decide(
                            orders.getOrDefault(e.orderId(), OrderState.unknown(e.orderId())), e);
                    orders.put(e.orderId(), d.state());
                    decisions.add(d);
                }
                default -> throw new IllegalArgumentException("unknown evidence " + delivery.evidence());
            }
        }

        boolean any(DecisionKind kind) {
            return decisions.stream().anyMatch(d -> d.kind() == kind);
        }

        boolean any(PaymentRule rule) {
            return decisions.stream().anyMatch(d -> d.rule() == rule);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Property 1: §4.5
    // ---------------------------------------------------------------------------------------------------------------

    @Property(tries = 10_000)
    void anyPermutationWithDuplicatesEndsInTheEventTimeOrderState(
            @ForAll("contradictionFreeDeliveries") List<Delivery> delivered) {
        List<Delivery> inEventTimeOrder = delivered.stream().sorted(EVENT_TIME_ORDER).toList();
        Replay actual = Replay.of(delivered);
        Replay expected = Replay.of(inEventTimeOrder);

        assertThat(actual.payments).isEqualTo(expected.payments);
        assertThat(actual.orders).isEqualTo(expected.orders);
        assertThat(actual.any(DecisionKind.CONFLICT)).as("a contradiction-free multiset raised a conflict").isFalse();

        // The property must not pass vacuously: each interesting situation has to occur often enough.
        cover("delivered out of event-time order", !delivered.equals(inEventTimeOrder), 50);
        cover("an exact re-delivery absorbed by dedupe", delivered.size() > actual.applied.size(), 20);
        cover("the same fact again under a new key", actual.any(DecisionKind.IGNORED_DUPLICATE), 20);
        cover("a stale event", actual.any(DecisionKind.IGNORED_STALE), 20);
        cover("a late success", actual.payments.values().stream().anyMatch(PaymentState::lateSuccess), 20);
        cover("a late success found out of order", actual.any(PaymentRule.FAILURE_BEFORE_SUCCESS), 5);
        cover("an order that reached PAID or later",
                actual.orders.values().stream().anyMatch(o -> o.status().carriesPayment()), 10);
    }

    private static void cover(String label, boolean happened, double minimumPercent) {
        Statistics.label(label).collect(happened);
        Statistics.label(label).coverage(c -> c.check(true).percentage(p -> p >= minimumPercent));
    }

    @Provide
    Arbitrary<List<Delivery>> contradictionFreeDeliveries() {
        return Combinators.combine(
                        consistentPayment("pay_1"), consistentPayment("pay_2"), consistentPayment("pay_3"), consistentOrder())
                .as((a, b, c, order) -> {
                    List<Delivery> all = new ArrayList<>(a);
                    all.addAll(b);
                    all.addAll(c);
                    all.addAll(order);
                    return List.copyOf(all);
                })
                .flatMap(OrderInsensitivityProperties::redeliveredAndShuffled);
    }

    /**
     * Pendings, failures and successes for one payment, with one amount and one order. When both failures and
     * successes are present, every failure is strictly before every success, so the multiset has no contradiction.
     * Times are whole seconds in a small window, so equal instants are common.
     */
    private static Arbitrary<List<Delivery>> consistentPayment(String id) {
        PaymentId paymentId = PaymentId.of(id);
        return Combinators.combine(
                        Arbitraries.longs().between(1, 10_000_000).map(Money::ofPaise),
                        Arbitraries.integers().between(0, 2),
                        Arbitraries.integers().between(0, 3),
                        Arbitraries.integers().between(0, 3),
                        Arbitraries.integers().between(1, 20))
                .flatAs((amount, pendings, failures, successes, split) -> {
                    Arbitrary<List<Integer>> pendingAt = seconds(0, 20).list().ofSize(pendings);
                    Arbitrary<List<Integer>> failedAt = seconds(0, successes > 0 ? split - 1 : 20).list().ofSize(failures);
                    Arbitrary<List<Integer>> succeededAt = seconds(failures > 0 ? split : 0, 20).list().ofSize(successes);
                    return Combinators.combine(pendingAt, failedAt, succeededAt).as((p, f, s) -> {
                        List<Delivery> out = new ArrayList<>();
                        addPayment(out, paymentId, PaymentStatus.PENDING, p, amount);
                        addPayment(out, paymentId, PaymentStatus.FAILED, f, amount);
                        addPayment(out, paymentId, PaymentStatus.SUCCEEDED, s, amount);
                        return List.copyOf(out);
                    });
                });
    }

    private static void addPayment(List<Delivery> out, PaymentId id, PaymentStatus status, List<Integer> seconds,
                                   Money amount) {
        for (int i = 0; i < seconds.size(); i++) {
            Instant at = T0.plusSeconds(seconds.get(i));
            out.add(new Delivery(id + "/" + status + "/" + i, at, new PaymentEvidence(id, status, at, ORDER, amount)));
        }
    }

    /**
     * A legal walk through the order graph from CREATED, with skipped versions and same-state updates, of which a
     * non-empty sub-multiset is delivered (some snapshots twice under different keys). Event time grows with version.
     */
    private static Arbitrary<List<Delivery>> consistentOrder() {
        return Combinators.combine(
                        Arbitraries.longs().between(1, 10_000_000).map(Money::ofPaise),
                        Arbitraries.integers().between(0, 999).list().ofMinSize(1).ofMaxSize(6),
                        Arbitraries.integers().between(0, 2).list().ofSize(6))
                .as((amount, steps, copies) -> {
                    List<OrderEvidence> path = legalPath(amount, steps);
                    List<Delivery> out = new ArrayList<>();
                    for (int i = 0; i < path.size(); i++) {
                        OrderEvidence snapshot = path.get(i);
                        int times = (i == path.size() - 1 && out.isEmpty()) ? Math.max(1, copies.get(i)) : copies.get(i);
                        for (int copy = 0; copy < times; copy++) {
                            out.add(new Delivery("ord_1/v" + snapshot.version() + "/" + copy,
                                    T0.plusSeconds(60 * snapshot.version()), snapshot));
                        }
                    }
                    return List.copyOf(out);
                });
    }

    private static List<OrderEvidence> legalPath(Money amount, List<Integer> steps) {
        List<OrderEvidence> path = new ArrayList<>();
        OrderStatus state = OrderStatus.CREATED;
        long version = 1;
        Instant expiresAt = T0.plusSeconds(3_600);
        path.add(orderSnapshot(state, version, amount, expiresAt));
        for (int step : steps.subList(1, steps.size())) {
            if (state.isTerminal()) {
                break;
            }
            List<OrderStatus> successors = new ArrayList<>();
            for (OrderTransitionTable.Edge edge : OrderTransitionTable.edges()) {
                if (edge.from() == state) {
                    successors.add(edge.to());
                }
            }
            // One step in six is a same-state update; half follow the first edge (towards PAID and FULFILLED);
            // the rest take the other edges (EXPIRED, CANCELLED). Uniform choice reached PAID in only ~3% of runs.
            int pick = step % 6;
            OrderStatus chosen = pick == 0 ? state
                    : successors.get(pick <= 3 ? 0 : (pick - 3) % successors.size());
            if (chosen == state) {
                expiresAt = expiresAt.plusSeconds(60);
            }
            version += 1 + step % 3;                           // versions may be skipped
            state = chosen;
            path.add(orderSnapshot(state, version, amount, expiresAt));
        }
        return path;
    }

    private static OrderEvidence orderSnapshot(OrderStatus status, long version, Money amount, Instant expiresAt) {
        return new OrderEvidence(ORDER, status, version, amount, expiresAt,
                status.carriesPayment() ? Optional.of(PAY_1) : Optional.empty());
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Property 2: conflicts are detected in every order (D24)
    // ---------------------------------------------------------------------------------------------------------------

    @Property(tries = 10_000)
    void aConflictIsRaisedInEveryOrderExactlyWhenTheRecordsContradict(
            @ForAll("anyPaymentDeliveries") List<Delivery> delivered) {
        List<PaymentEvidence> facts = delivered.stream().map(d -> (PaymentEvidence) d.evidence()).distinct().toList();
        boolean contradictory = contradicts(facts);
        Replay actual = Replay.of(delivered);

        assertThat(actual.payments.get(PAY_1).conflict()).isEqualTo(contradictory);
        if (!contradictory) {
            assertThat(actual.payments).isEqualTo(Replay.of(delivered.stream().sorted(EVENT_TIME_ORDER).toList()).payments);
        }

        cover("records contradict", contradictory, 15);
        Statistics.label("records agree").collect(!contradictory);
        Statistics.label("records agree").coverage(c -> c.check(true).percentage(p -> p >= 15));
    }

    /** The oracle, written from the definition rather than the table: a fact mismatch, or a failure at or after a success. */
    private static boolean contradicts(List<PaymentEvidence> facts) {
        boolean mismatch = facts.stream().map(f -> f.amount() + "/" + f.orderId()).distinct().count() > 1;
        Optional<Instant> latestFailure = facts.stream().filter(f -> f.reported() == PaymentStatus.FAILED)
                .map(PaymentEvidence::eventTime).max(Comparator.naturalOrder());
        Optional<Instant> earliestSuccess = facts.stream().filter(f -> f.reported() == PaymentStatus.SUCCEEDED)
                .map(PaymentEvidence::eventTime).min(Comparator.naturalOrder());
        boolean failureNotBeforeSuccess = latestFailure.isPresent() && earliestSuccess.isPresent()
                && !latestFailure.get().isBefore(earliestSuccess.get());
        return mismatch || failureNotBeforeSuccess;
    }

    @Provide
    Arbitrary<List<Delivery>> anyPaymentDeliveries() {
        Arbitrary<PaymentEvidence> fact = Combinators.combine(
                        Arbitraries.of(PaymentStatus.PENDING, PaymentStatus.SUCCEEDED, PaymentStatus.FAILED),
                        seconds(0, 6),
                        Arbitraries.integers().between(1, 12).map(i -> Money.ofPaise(i == 1 ? 50_000 : 49_900)),
                        Arbitraries.integers().between(1, 12).map(i -> OrderId.of(i == 1 ? "ord_2" : "ord_1")))
                .as((status, second, amount, order) ->
                        new PaymentEvidence(PAY_1, status, T0.plusSeconds(second), order, amount));
        return fact.list().ofMinSize(1).ofMaxSize(6)
                .map(list -> {
                    List<Delivery> out = new ArrayList<>();
                    for (int i = 0; i < list.size(); i++) {
                        out.add(new Delivery("pay_1/" + i, list.get(i).eventTime(), list.get(i)));
                    }
                    return List.copyOf(out);
                })
                .flatMap(OrderInsensitivityProperties::redeliveredAndShuffled);
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static Arbitrary<Integer> seconds(int from, int to) {
        return Arbitraries.integers().between(from, to);
    }

    /** Adds up to four exact re-deliveries, then shuffles everything into an arbitrary permutation. */
    private static Arbitrary<List<Delivery>> redeliveredAndShuffled(List<Delivery> facts) {
        return Arbitraries.integers().between(0, facts.size() - 1).list().ofMaxSize(4).flatMap(again -> {
            List<Delivery> all = new ArrayList<>(facts);
            again.forEach(i -> all.add(facts.get(i)));
            return Arbitraries.shuffle(all);
        });
    }
}
