package io.github.vanish073.driftline.core.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.state.Decision;
import io.github.vanish073.driftline.core.state.DecisionKind;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * The exhaustive order test: every state × incoming state × version order (older, equal, newer), against a
 * reachability matrix written by hand from the legal graph in DESIGN §4.4 (D25: zero steps counts).
 */
class OrderTransitionTableTest {

    private static final String REACHABLE = """
            from\\to          CREATED  PENDING_PAYMENT  PAID  FULFILLED  EXPIRED  CANCELLED
            CREATED          yes      yes              yes   yes        yes      yes
            PENDING_PAYMENT  no       yes              yes   yes        yes      yes
            PAID             no       no               yes   yes        no       no
            FULFILLED        no       no               no    yes        no       no
            EXPIRED          no       no               no    no         yes      no
            CANCELLED        no       no               no    no         no       yes
            """;

    private static final Instant T0 = Instant.parse("2026-11-13T06:00:00Z");
    private static final OrderId ORDER = OrderId.of("ord_1");
    private static final Money AMOUNT = Money.ofPaise(49_900);
    private static final PaymentId P1 = PaymentId.of("pay_1");
    private static final PaymentId P2 = PaymentId.of("pay_2");
    private static final long CURRENT_VERSION = 5;

    private final OrderStateMachine machine = new OrderStateMachine();

    private static OrderEvidence snapshot(OrderStatus status, long version, PaymentId paidWith) {
        return new OrderEvidence(ORDER, status, version, AMOUNT, T0.plusSeconds(900),
                status.carriesPayment() ? Optional.of(paidWith) : Optional.empty());
    }

    private static List<OrderStatus> states() {
        return Stream.of(OrderStatus.values()).filter(s -> s != OrderStatus.NONE).toList();
    }

    private static boolean expectedReachable(OrderStatus from, OrderStatus to) {
        List<String> lines = REACHABLE.lines().toList();
        List<String> header = List.of(lines.get(0).trim().split("\\s+"));
        for (String line : lines.subList(1, lines.size())) {
            String[] cells = line.trim().split("\\s+");
            if (cells[0].equals(from.name())) {
                return cells[header.indexOf(to.name())].equals("yes");
            }
        }
        throw new IllegalArgumentException("no row for " + from);
    }

    @Test
    void reachabilityMatchesTheHandWrittenMatrix() {
        for (OrderStatus from : states()) {
            for (OrderStatus to : states()) {
                assertThat(OrderTransitionTable.reachable(from, to)).as(from + " -> " + to).isEqualTo(expectedReachable(from, to));
            }
        }
    }

    @TestFactory
    Stream<DynamicTest> everyStateTargetAndVersionOrderGivesTheDesignedResult() {
        List<DynamicTest> tests = new ArrayList<>();
        for (OrderStatus to : states()) {
            OrderEvidence first = snapshot(to, 1, P1);
            tests.add(DynamicTest.dynamicTest("NONE x " + to + " -> FIRST_EVIDENCE",
                    () -> expect(OrderState.unknown(ORDER), first, OrderRule.FIRST_EVIDENCE, first.toState())));
        }
        for (OrderStatus from : states()) {
            OrderState current = snapshot(from, CURRENT_VERSION, P1).toState();
            for (OrderStatus to : states()) {
                OrderEvidence older = snapshot(to, CURRENT_VERSION - 2, P1);
                OrderEvidence same = snapshot(to, CURRENT_VERSION, P1);
                OrderEvidence newer = snapshot(to, CURRENT_VERSION + 2, P1);
                OrderRule sameRule = to == from ? OrderRule.SAME_VERSION_REPEATED : OrderRule.SAME_VERSION_DIFFERENT;
                boolean reachable = expectedReachable(from, to);
                OrderRule newerRule = reachable ? OrderRule.NEWER_VERSION : OrderRule.ILLEGAL_TARGET;
                OrderState afterNewer = reachable ? newer.toState() : current;
                tests.add(DynamicTest.dynamicTest(from + " x " + to + " older -> STALE_VERSION",
                        () -> expect(current, older, OrderRule.STALE_VERSION, current)));
                tests.add(DynamicTest.dynamicTest(from + " x " + to + " same -> " + sameRule,
                        () -> expect(current, same, sameRule, current)));
                tests.add(DynamicTest.dynamicTest(from + " x " + to + " newer -> " + newerRule,
                        () -> expect(current, newer, newerRule, afterNewer)));
            }
        }
        assertThat(tests).hasSize(6 + 6 * 6 * 3);
        return tests.stream();
    }

    @Test
    void theSameVersionWithAnyOtherFieldChangedIsAnAnomaly() {
        OrderState current = snapshot(OrderStatus.PENDING_PAYMENT, CURRENT_VERSION, P1).toState();
        OrderEvidence otherAmount = new OrderEvidence(ORDER, OrderStatus.PENDING_PAYMENT, CURRENT_VERSION,
                Money.ofPaise(50_000), T0.plusSeconds(900), Optional.empty());
        OrderEvidence otherExpiry = new OrderEvidence(ORDER, OrderStatus.PENDING_PAYMENT, CURRENT_VERSION,
                AMOUNT, T0.plusSeconds(901), Optional.empty());
        expect(current, otherAmount, OrderRule.SAME_VERSION_DIFFERENT, current);
        expect(current, otherExpiry, OrderRule.SAME_VERSION_DIFFERENT, current);
        expect(snapshot(OrderStatus.PAID, CURRENT_VERSION, P1).toState(), snapshot(OrderStatus.PAID, CURRENT_VERSION, P2),
                OrderRule.SAME_VERSION_DIFFERENT, snapshot(OrderStatus.PAID, CURRENT_VERSION, P1).toState());
    }

    /** D25: once paid with p1, a newer snapshot that names p2 is an anomaly even though the state is reachable. */
    @Test
    void aPaidOrderCannotSwitchPayments() {
        for (OrderStatus from : List.of(OrderStatus.PAID, OrderStatus.FULFILLED)) {
            OrderState current = snapshot(from, CURRENT_VERSION, P1).toState();
            for (OrderStatus to : List.of(OrderStatus.PAID, OrderStatus.FULFILLED)) {
                if (expectedReachable(from, to)) {
                    expect(current, snapshot(to, CURRENT_VERSION + 1, P2), OrderRule.PAID_WITH_CHANGED, current);
                    expect(current, snapshot(to, CURRENT_VERSION + 1, P1), OrderRule.NEWER_VERSION,
                            snapshot(to, CURRENT_VERSION + 1, P1).toState());
                }
            }
        }
    }

    @Test
    void skippedVersionsAreAllowedWhenTheTargetIsReachable() {
        OrderState created = snapshot(OrderStatus.CREATED, 1, P1).toState();
        OrderEvidence fulfilled = snapshot(OrderStatus.FULFILLED, 9, P1);
        expect(created, fulfilled, OrderRule.NEWER_VERSION, fulfilled.toState());
        OrderState expired = snapshot(OrderStatus.EXPIRED, 3, P1).toState();
        expect(expired, snapshot(OrderStatus.PAID, 4, P1), OrderRule.ILLEGAL_TARGET, expired);
    }

    @Test
    void readsEvidenceFromAnEnvelope() {
        EventEnvelope envelope = new EventEnvelope(1, MerchantId.of("m_001"), 0, Source.MERCHANT, "idem_1",
                EventType.ORDER_PAID, T0, T0, Sha256.of("x".getBytes(StandardCharsets.UTF_8)),
                new OrderPayload(ORDER, 3, AMOUNT, T0.plusSeconds(900), Optional.of(P1)), Optional.empty());
        assertThat(OrderEvidence.from(envelope)).isEqualTo(snapshot(OrderStatus.PAID, 3, P1));
    }

    @Test
    void rejectsEvidenceForAnotherOrderAndSnapshotsWithTheWrongShape() {
        assertThatThrownBy(() -> machine.decide(OrderState.unknown(OrderId.of("ord_2")), snapshot(OrderStatus.CREATED, 1, P1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrderEvidence(ORDER, OrderStatus.PAID, 1, AMOUNT, T0, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrderEvidence(ORDER, OrderStatus.CREATED, 0, AMOUNT, T0, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void expect(OrderState current, OrderEvidence incoming, OrderRule rule, OrderState next) {
        Decision<OrderState> decision = machine.decide(current, incoming);
        assertThat(decision.rule()).isEqualTo(rule);
        assertThat(decision.kind()).isEqualTo(rule.kind());
        assertThat(decision.state()).isEqualTo(next);
        if (rule.kind() != DecisionKind.APPLIED) {
            assertThat(decision.state()).isEqualTo(current);
        }
    }
}
