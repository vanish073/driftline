package io.github.vanish073.driftline.core.payment;

import io.github.vanish073.driftline.core.state.DecisionKind;
import io.github.vanish073.driftline.core.state.Rule;

/** The rules of the payment transition table (DESIGN §4.3 as refined by D24). */
public enum PaymentRule implements Rule {
    FIRST_EVIDENCE("P01", DecisionKind.APPLIED, "first event for a payment Driftline has not seen"),
    PENDING_REPEATED("P02", DecisionKind.IGNORED_DUPLICATE, "pending again while pending"),
    PENDING_RESOLVED("P03", DecisionKind.APPLIED, "a pending payment succeeded or failed"),
    PENDING_AFTER_OUTCOME("P04", DecisionKind.IGNORED_STALE, "pending after the payment succeeded or failed"),
    LATE_SUCCESS("P05", DecisionKind.APPLIED, "success after the recorded failure: LATE_SUCCESS"),
    SUCCESS_NOT_AFTER_FAILURE("P06", DecisionKind.CONFLICT, "success at or before the recorded failure"),
    FAILURE_REPEATED("P07", DecisionKind.IGNORED_DUPLICATE, "failure at or before the recorded failure"),
    FAILURE_LATER("P08", DecisionKind.APPLIED,
            "failure after the recorded failure and before any success: the failure instant moves later"),
    SUCCESS_REPEATED("P09", DecisionKind.IGNORED_DUPLICATE, "success at or after the recorded success"),
    SUCCESS_EARLIER("P10", DecisionKind.APPLIED,
            "success before the recorded success and after any failure: the success instant moves earlier"),
    FAILURE_BEFORE_SUCCESS("P11", DecisionKind.APPLIED,
            "failure before the recorded success, arriving after it: LATE_SUCCESS found out of order"),
    FAILURE_NOT_BEFORE_SUCCESS("P12", DecisionKind.CONFLICT, "failure at or after the recorded success"),
    FACTS_MISMATCH("P13", DecisionKind.CONFLICT, "amount or order id differs from the payment's"),
    ALREADY_IN_CONFLICT("P14", DecisionKind.CONFLICT, "the payment is frozen by an earlier conflict");

    private final String code;
    private final DecisionKind kind;
    private final String summary;

    PaymentRule(String code, DecisionKind kind, String summary) {
        this.code = code;
        this.kind = kind;
        this.summary = summary;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public DecisionKind kind() {
        return kind;
    }

    @Override
    public String summary() {
        return summary;
    }
}
