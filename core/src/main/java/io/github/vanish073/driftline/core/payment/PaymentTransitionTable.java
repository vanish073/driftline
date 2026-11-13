package io.github.vanish073.driftline.core.payment;

import static io.github.vanish073.driftline.core.payment.PaymentRule.FAILURE_BEFORE_SUCCESS;
import static io.github.vanish073.driftline.core.payment.PaymentRule.FAILURE_LATER;
import static io.github.vanish073.driftline.core.payment.PaymentRule.FAILURE_NOT_BEFORE_SUCCESS;
import static io.github.vanish073.driftline.core.payment.PaymentRule.FAILURE_REPEATED;
import static io.github.vanish073.driftline.core.payment.PaymentRule.FIRST_EVIDENCE;
import static io.github.vanish073.driftline.core.payment.PaymentRule.LATE_SUCCESS;
import static io.github.vanish073.driftline.core.payment.PaymentRule.PENDING_AFTER_OUTCOME;
import static io.github.vanish073.driftline.core.payment.PaymentRule.PENDING_REPEATED;
import static io.github.vanish073.driftline.core.payment.PaymentRule.PENDING_RESOLVED;
import static io.github.vanish073.driftline.core.payment.PaymentRule.SUCCESS_EARLIER;
import static io.github.vanish073.driftline.core.payment.PaymentRule.SUCCESS_NOT_AFTER_FAILURE;
import static io.github.vanish073.driftline.core.payment.PaymentRule.SUCCESS_REPEATED;
import static io.github.vanish073.driftline.core.payment.PaymentStatus.FAILED;
import static io.github.vanish073.driftline.core.payment.PaymentStatus.NONE;
import static io.github.vanish073.driftline.core.payment.PaymentStatus.PENDING;
import static io.github.vanish073.driftline.core.payment.PaymentStatus.SUCCEEDED;
import static io.github.vanish073.driftline.core.payment.TimeOrder.AFTER;
import static io.github.vanish073.driftline.core.payment.TimeOrder.BEFORE;
import static io.github.vanish073.driftline.core.payment.TimeOrder.EQUAL;
import static io.github.vanish073.driftline.core.payment.TimeOrder.NO_RECORD;

import io.github.vanish073.driftline.core.state.DecisionKind;
import java.util.List;
import java.util.Objects;

/**
 * The payment transition table: DESIGN §4.3, refined by D24, as data. {@link PaymentStateMachine} only computes
 * the key and looks the row up.
 *
 * <p>A row is keyed by the current status, where the incoming event time falls relative to the recorded failure
 * ({@code failedAt}) and success ({@code successAt}) instants, and the incoming status. Two guards run before the
 * table: a frozen payment ({@link PaymentRule#ALREADY_IN_CONFLICT}) and a fact mismatch
 * ({@link PaymentRule#FACTS_MISMATCH}).
 *
 * <p>Reading guide, against the cells of DESIGN §4.3:
 * <ul>
 *   <li>NONE and PENDING rows: as in §4.3.</li>
 *   <li>FAILED (f₀) × succeeded: s &gt; f₀ is LATE_SUCCESS; s &le; f₀ is CONFLICT (D24: an equal instant cannot be
 *       ordered, and breaking the tie by event key would be a guess).</li>
 *   <li>SUCCEEDED (s₀) × failed: f &ge; s₀ is CONFLICT; f &lt; s₀ is recorded as late-success evidence instead of
 *       being ignored (D24), so LATE_SUCCESS does not depend on arrival order.</li>
 *   <li>Repeats of a known status update the recorded instant only when that makes it more conservative: the
 *       earliest success and the latest failure (D24).</li>
 * </ul>
 */
public final class PaymentTransitionTable {

    /**
     * One cell of the table.
     *
     * @param vsFailure where the incoming time falls relative to {@code failedAt}
     * @param vsSuccess where the incoming time falls relative to {@code successAt}
     * @param to        the status after the decision; equal to {@code from} unless the rule is APPLIED
     */
    public record Row(PaymentStatus from, TimeOrder vsFailure, TimeOrder vsSuccess, PaymentStatus incoming,
                      PaymentRule rule, PaymentStatus to) {

        public Row {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(vsFailure, "vsFailure");
            Objects.requireNonNull(vsSuccess, "vsSuccess");
            Objects.requireNonNull(incoming, "incoming");
            Objects.requireNonNull(rule, "rule");
            Objects.requireNonNull(to, "to");
        }

        boolean matches(PaymentStatus status, TimeOrder failure, TimeOrder success, PaymentStatus reported) {
            return from == status && vsFailure == failure && vsSuccess == success && incoming == reported;
        }
    }

    private static final List<Row> ROWS = List.of(
            //  current         vs failedAt  vs successAt  incoming      rule                         next
            // NONE: nothing recorded yet.
            row(NONE,           NO_RECORD,   NO_RECORD,    PENDING,      FIRST_EVIDENCE,              PENDING),
            row(NONE,           NO_RECORD,   NO_RECORD,    SUCCEEDED,    FIRST_EVIDENCE,              SUCCEEDED),
            row(NONE,           NO_RECORD,   NO_RECORD,    FAILED,       FIRST_EVIDENCE,              FAILED),
            // PENDING: still nothing recorded.
            row(PENDING,        NO_RECORD,   NO_RECORD,    PENDING,      PENDING_REPEATED,            PENDING),
            row(PENDING,        NO_RECORD,   NO_RECORD,    SUCCEEDED,    PENDING_RESOLVED,            SUCCEEDED),
            row(PENDING,        NO_RECORD,   NO_RECORD,    FAILED,       PENDING_RESOLVED,            FAILED),
            // FAILED at f0.
            row(FAILED,         BEFORE,      NO_RECORD,    PENDING,      PENDING_AFTER_OUTCOME,       FAILED),
            row(FAILED,         EQUAL,       NO_RECORD,    PENDING,      PENDING_AFTER_OUTCOME,       FAILED),
            row(FAILED,         AFTER,       NO_RECORD,    PENDING,      PENDING_AFTER_OUTCOME,       FAILED),
            row(FAILED,         BEFORE,      NO_RECORD,    SUCCEEDED,    SUCCESS_NOT_AFTER_FAILURE,   FAILED),
            row(FAILED,         EQUAL,       NO_RECORD,    SUCCEEDED,    SUCCESS_NOT_AFTER_FAILURE,   FAILED),
            row(FAILED,         AFTER,       NO_RECORD,    SUCCEEDED,    LATE_SUCCESS,                SUCCEEDED),
            row(FAILED,         BEFORE,      NO_RECORD,    FAILED,       FAILURE_REPEATED,            FAILED),
            row(FAILED,         EQUAL,       NO_RECORD,    FAILED,       FAILURE_REPEATED,            FAILED),
            row(FAILED,         AFTER,       NO_RECORD,    FAILED,       FAILURE_LATER,               FAILED),
            // SUCCEEDED at s0, no failure recorded.
            row(SUCCEEDED,      NO_RECORD,   BEFORE,       PENDING,      PENDING_AFTER_OUTCOME,       SUCCEEDED),
            row(SUCCEEDED,      NO_RECORD,   EQUAL,        PENDING,      PENDING_AFTER_OUTCOME,       SUCCEEDED),
            row(SUCCEEDED,      NO_RECORD,   AFTER,        PENDING,      PENDING_AFTER_OUTCOME,       SUCCEEDED),
            row(SUCCEEDED,      NO_RECORD,   BEFORE,       SUCCEEDED,    SUCCESS_EARLIER,             SUCCEEDED),
            row(SUCCEEDED,      NO_RECORD,   EQUAL,        SUCCEEDED,    SUCCESS_REPEATED,            SUCCEEDED),
            row(SUCCEEDED,      NO_RECORD,   AFTER,        SUCCEEDED,    SUCCESS_REPEATED,            SUCCEEDED),
            row(SUCCEEDED,      NO_RECORD,   BEFORE,       FAILED,       FAILURE_BEFORE_SUCCESS,      SUCCEEDED),
            row(SUCCEEDED,      NO_RECORD,   EQUAL,        FAILED,       FAILURE_NOT_BEFORE_SUCCESS,  SUCCEEDED),
            row(SUCCEEDED,      NO_RECORD,   AFTER,        FAILED,       FAILURE_NOT_BEFORE_SUCCESS,  SUCCEEDED),
            // SUCCEEDED at s0 after a failure at f0 < s0 (late success). The incoming time falls in one of five places.
            row(SUCCEEDED,      BEFORE,      BEFORE,       PENDING,      PENDING_AFTER_OUTCOME,       SUCCEEDED),
            row(SUCCEEDED,      EQUAL,       BEFORE,       PENDING,      PENDING_AFTER_OUTCOME,       SUCCEEDED),
            row(SUCCEEDED,      AFTER,       BEFORE,       PENDING,      PENDING_AFTER_OUTCOME,       SUCCEEDED),
            row(SUCCEEDED,      AFTER,       EQUAL,        PENDING,      PENDING_AFTER_OUTCOME,       SUCCEEDED),
            row(SUCCEEDED,      AFTER,       AFTER,        PENDING,      PENDING_AFTER_OUTCOME,       SUCCEEDED),
            row(SUCCEEDED,      BEFORE,      BEFORE,       SUCCEEDED,    SUCCESS_NOT_AFTER_FAILURE,   SUCCEEDED),
            row(SUCCEEDED,      EQUAL,       BEFORE,       SUCCEEDED,    SUCCESS_NOT_AFTER_FAILURE,   SUCCEEDED),
            row(SUCCEEDED,      AFTER,       BEFORE,       SUCCEEDED,    SUCCESS_EARLIER,             SUCCEEDED),
            row(SUCCEEDED,      AFTER,       EQUAL,        SUCCEEDED,    SUCCESS_REPEATED,            SUCCEEDED),
            row(SUCCEEDED,      AFTER,       AFTER,        SUCCEEDED,    SUCCESS_REPEATED,            SUCCEEDED),
            row(SUCCEEDED,      BEFORE,      BEFORE,       FAILED,       FAILURE_REPEATED,            SUCCEEDED),
            row(SUCCEEDED,      EQUAL,       BEFORE,       FAILED,       FAILURE_REPEATED,            SUCCEEDED),
            row(SUCCEEDED,      AFTER,       BEFORE,       FAILED,       FAILURE_LATER,               SUCCEEDED),
            row(SUCCEEDED,      AFTER,       EQUAL,        FAILED,       FAILURE_NOT_BEFORE_SUCCESS,  SUCCEEDED),
            row(SUCCEEDED,      AFTER,       AFTER,        FAILED,       FAILURE_NOT_BEFORE_SUCCESS,  SUCCEEDED));

    static {
        for (int i = 0; i < ROWS.size(); i++) {
            Row a = ROWS.get(i);
            if (a.rule().kind() != DecisionKind.APPLIED && a.to() != a.from()) {
                throw new IllegalStateException("row " + i + ": only an APPLIED rule may change the status");
            }
            for (int j = i + 1; j < ROWS.size(); j++) {
                if (ROWS.get(j).matches(a.from(), a.vsFailure(), a.vsSuccess(), a.incoming())) {
                    throw new IllegalStateException("rows " + i + " and " + j + " have the same key");
                }
            }
        }
    }

    private PaymentTransitionTable() {
    }

    /** Every row, in table order. */
    public static List<Row> rows() {
        return ROWS;
    }

    /** The unique row for this key. A valid {@link PaymentState} always has one. */
    public static Row lookup(PaymentStatus from, TimeOrder vsFailure, TimeOrder vsSuccess, PaymentStatus incoming) {
        for (Row row : ROWS) {
            if (row.matches(from, vsFailure, vsSuccess, incoming)) {
                return row;
            }
        }
        throw new IllegalStateException("no payment transition for " + from + " x " + incoming
                + " (vs failedAt " + vsFailure + ", vs successAt " + vsSuccess + ")");
    }

    private static Row row(PaymentStatus from, TimeOrder vsFailure, TimeOrder vsSuccess, PaymentStatus incoming,
                           PaymentRule rule, PaymentStatus to) {
        return new Row(from, vsFailure, vsSuccess, incoming, rule, to);
    }
}
