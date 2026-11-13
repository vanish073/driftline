package io.github.vanish073.driftline.core.payment;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.state.Decision;
import io.github.vanish073.driftline.core.state.DecisionKind;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * The exhaustive table test: every state × incoming event × time order (before, equal, after), against expectations
 * written by hand from DESIGN §4.3 and D24. It is independent of {@link PaymentTransitionTable}: a second encoding
 * of the same rules, in a form that reads like the design.
 */
class PaymentTransitionTableTest {

    private static final Instant T0 = Instant.parse("2026-11-13T06:00:00Z");
    private static final PaymentId PAYMENT = PaymentId.of("pay_1");
    private static final OrderId ORDER = OrderId.of("ord_1");
    private static final Money AMOUNT = Money.ofPaise(49_900);

    /**
     * Times are seconds after T0; '-' means not recorded. A failure is recorded at 10 and a success at 20, so
     * 5 is before both, 15 is between them and 25 is after both.
     */
    private static final String EXPECTED = """
            # current: status  failedAt  successAt | incoming  at | kind               rule                        | next: status  failedAt  successAt
            NONE       -   -   | PENDING    7  | APPLIED            FIRST_EVIDENCE              | PENDING    -   -
            NONE       -   -   | SUCCEEDED  7  | APPLIED            FIRST_EVIDENCE              | SUCCEEDED  -   7
            NONE       -   -   | FAILED     7  | APPLIED            FIRST_EVIDENCE              | FAILED     7   -
            PENDING    -   -   | PENDING    7  | IGNORED_DUPLICATE  PENDING_REPEATED            | PENDING    -   -
            PENDING    -   -   | SUCCEEDED  7  | APPLIED            PENDING_RESOLVED            | SUCCEEDED  -   7
            PENDING    -   -   | FAILED     7  | APPLIED            PENDING_RESOLVED            | FAILED     7   -
            FAILED     10  -   | PENDING    5  | IGNORED_STALE      PENDING_AFTER_OUTCOME       | FAILED     10  -
            FAILED     10  -   | PENDING    10 | IGNORED_STALE      PENDING_AFTER_OUTCOME       | FAILED     10  -
            FAILED     10  -   | PENDING    15 | IGNORED_STALE      PENDING_AFTER_OUTCOME       | FAILED     10  -
            FAILED     10  -   | SUCCEEDED  5  | CONFLICT           SUCCESS_NOT_AFTER_FAILURE   | FAILED     10  -
            FAILED     10  -   | SUCCEEDED  10 | CONFLICT           SUCCESS_NOT_AFTER_FAILURE   | FAILED     10  -
            FAILED     10  -   | SUCCEEDED  15 | APPLIED            LATE_SUCCESS                | SUCCEEDED  10  15
            FAILED     10  -   | FAILED     5  | IGNORED_DUPLICATE  FAILURE_REPEATED            | FAILED     10  -
            FAILED     10  -   | FAILED     10 | IGNORED_DUPLICATE  FAILURE_REPEATED            | FAILED     10  -
            FAILED     10  -   | FAILED     15 | APPLIED            FAILURE_LATER               | FAILED     15  -
            SUCCEEDED  -   20  | PENDING    15 | IGNORED_STALE      PENDING_AFTER_OUTCOME       | SUCCEEDED  -   20
            SUCCEEDED  -   20  | PENDING    20 | IGNORED_STALE      PENDING_AFTER_OUTCOME       | SUCCEEDED  -   20
            SUCCEEDED  -   20  | PENDING    25 | IGNORED_STALE      PENDING_AFTER_OUTCOME       | SUCCEEDED  -   20
            SUCCEEDED  -   20  | SUCCEEDED  15 | APPLIED            SUCCESS_EARLIER             | SUCCEEDED  -   15
            SUCCEEDED  -   20  | SUCCEEDED  20 | IGNORED_DUPLICATE  SUCCESS_REPEATED            | SUCCEEDED  -   20
            SUCCEEDED  -   20  | SUCCEEDED  25 | IGNORED_DUPLICATE  SUCCESS_REPEATED            | SUCCEEDED  -   20
            SUCCEEDED  -   20  | FAILED     15 | APPLIED            FAILURE_BEFORE_SUCCESS      | SUCCEEDED  15  20
            SUCCEEDED  -   20  | FAILED     20 | CONFLICT           FAILURE_NOT_BEFORE_SUCCESS  | SUCCEEDED  -   20
            SUCCEEDED  -   20  | FAILED     25 | CONFLICT           FAILURE_NOT_BEFORE_SUCCESS  | SUCCEEDED  -   20
            SUCCEEDED  10  20  | PENDING    5  | IGNORED_STALE      PENDING_AFTER_OUTCOME       | SUCCEEDED  10  20
            SUCCEEDED  10  20  | PENDING    10 | IGNORED_STALE      PENDING_AFTER_OUTCOME       | SUCCEEDED  10  20
            SUCCEEDED  10  20  | PENDING    15 | IGNORED_STALE      PENDING_AFTER_OUTCOME       | SUCCEEDED  10  20
            SUCCEEDED  10  20  | PENDING    20 | IGNORED_STALE      PENDING_AFTER_OUTCOME       | SUCCEEDED  10  20
            SUCCEEDED  10  20  | PENDING    25 | IGNORED_STALE      PENDING_AFTER_OUTCOME       | SUCCEEDED  10  20
            SUCCEEDED  10  20  | SUCCEEDED  5  | CONFLICT           SUCCESS_NOT_AFTER_FAILURE   | SUCCEEDED  10  20
            SUCCEEDED  10  20  | SUCCEEDED  10 | CONFLICT           SUCCESS_NOT_AFTER_FAILURE   | SUCCEEDED  10  20
            SUCCEEDED  10  20  | SUCCEEDED  15 | APPLIED            SUCCESS_EARLIER             | SUCCEEDED  10  15
            SUCCEEDED  10  20  | SUCCEEDED  20 | IGNORED_DUPLICATE  SUCCESS_REPEATED            | SUCCEEDED  10  20
            SUCCEEDED  10  20  | SUCCEEDED  25 | IGNORED_DUPLICATE  SUCCESS_REPEATED            | SUCCEEDED  10  20
            SUCCEEDED  10  20  | FAILED     5  | IGNORED_DUPLICATE  FAILURE_REPEATED            | SUCCEEDED  10  20
            SUCCEEDED  10  20  | FAILED     10 | IGNORED_DUPLICATE  FAILURE_REPEATED            | SUCCEEDED  10  20
            SUCCEEDED  10  20  | FAILED     15 | APPLIED            FAILURE_LATER               | SUCCEEDED  15  20
            SUCCEEDED  10  20  | FAILED     20 | CONFLICT           FAILURE_NOT_BEFORE_SUCCESS  | SUCCEEDED  10  20
            SUCCEEDED  10  20  | FAILED     25 | CONFLICT           FAILURE_NOT_BEFORE_SUCCESS  | SUCCEEDED  10  20
            """;

    private record Case(PaymentState current, PaymentEvidence incoming, DecisionKind kind, PaymentRule rule,
                        PaymentState next, String line) {
    }

    private final PaymentStateMachine machine = new PaymentStateMachine();

    @TestFactory
    Stream<DynamicTest> everyStateEventAndTimeOrderGivesTheDesignedResult() {
        return cases().stream().map(c -> DynamicTest.dynamicTest(c.line(), () -> {
            Decision<PaymentState> decision = machine.decide(c.current(), c.incoming());
            assertThat(decision.kind()).isEqualTo(c.kind());
            assertThat(decision.rule()).isEqualTo(c.rule());
            assertThat(decision.state()).isEqualTo(c.next());
        }));
    }

    /** The hand-written expectations and the machine's table cover exactly the same keys, each once. */
    @Test
    void theExpectationsEnumerateEveryKeyOfTheTable() {
        List<String> expectedKeys = cases().stream()
                .map(c -> key(c.current().status(),
                        TimeOrder.of(c.incoming().eventTime(), c.current().failedAt()),
                        TimeOrder.of(c.incoming().eventTime(), c.current().successAt()),
                        c.incoming().reported()))
                .sorted().toList();
        List<String> tableKeys = PaymentTransitionTable.rows().stream()
                .map(r -> key(r.from(), r.vsFailure(), r.vsSuccess(), r.incoming()))
                .sorted().toList();
        assertThat(expectedKeys).doesNotHaveDuplicates().hasSize(39).isEqualTo(tableKeys);
    }

    @Test
    void everyRowAgreesWithTheKindOfItsRule() {
        for (PaymentTransitionTable.Row row : PaymentTransitionTable.rows()) {
            if (row.rule().kind() != DecisionKind.APPLIED) {
                assertThat(row.to()).as(row.toString()).isEqualTo(row.from());
            }
        }
    }

    private static String key(PaymentStatus from, TimeOrder vsFailure, TimeOrder vsSuccess, PaymentStatus incoming) {
        return from + "/" + vsFailure + "/" + vsSuccess + "/" + incoming;
    }

    private static List<Case> cases() {
        List<Case> cases = new ArrayList<>();
        for (String line : EXPECTED.lines().toList()) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] f = line.replace("|", " ").trim().split("\\s+");
            PaymentStatus status = PaymentStatus.valueOf(f[0]);
            PaymentState current = status == PaymentStatus.NONE
                    ? PaymentState.unknown(PAYMENT)
                    : state(status, f[1], f[2], false);
            PaymentEvidence incoming = new PaymentEvidence(PAYMENT, PaymentStatus.valueOf(f[3]), at(f[4]), ORDER, AMOUNT);
            DecisionKind kind = DecisionKind.valueOf(f[5]);
            PaymentRule rule = PaymentRule.valueOf(f[6]);
            PaymentState next = state(PaymentStatus.valueOf(f[7]), f[8], f[9], kind == DecisionKind.CONFLICT);
            cases.add(new Case(current, incoming, kind, rule, next, line.trim()));
        }
        return cases;
    }

    private static PaymentState state(PaymentStatus status, String failedAt, String successAt, boolean conflict) {
        return new PaymentState(PAYMENT, status, Optional.of(ORDER), Optional.of(AMOUNT),
                optionalAt(failedAt), optionalAt(successAt), conflict);
    }

    private static Optional<Instant> optionalAt(String seconds) {
        return seconds.equals("-") ? Optional.empty() : Optional.of(at(seconds));
    }

    private static Instant at(String seconds) {
        return T0.plusSeconds(Long.parseLong(seconds));
    }
}
