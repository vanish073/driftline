package io.github.vanish073.driftline.core.breaks;

import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.time.IstCalendar;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;

/**
 * The deadline rules of DESIGN §8.3, the same table as {@code config/deadlines.yaml} (a test fails if they differ).
 * A T-date rule's deadline is the end of IST day T + days (00:00 IST on day T + days + 1, exclusive); a detection rule's
 * is the detection time plus hours. {@code escalate_by = deadline − margin}. Compensation accrues only on the RBI rules:
 * ₹100 for each day the resolution date (IST) is after the deadline date, and escalation does not stop the clock.
 */
public enum DeadlineRule {

    RBI_UPI_P2M_T5(Anchor.T_DATE, 5, 0, Duration.ofHours(24), 100,
            List.of(BreakType.DEBITED_NO_ORDER, BreakType.LATE_SUCCESS)),
    RBI_UPI_P2P_T1(Anchor.T_DATE, 1, 0, Duration.ofHours(24), 100, List.of()),
    INTERNAL_DUP_T1(Anchor.T_DATE, 1, 0, Duration.ofHours(24), 0, List.of(BreakType.DUPLICATE_CAPTURE)),
    INTERNAL_ESCALATE_4H(Anchor.DETECTION, 0, 4, Duration.ZERO, 0, List.of(BreakType.SHORT_SETTLEMENT,
            BreakType.MISSING_SETTLEMENT, BreakType.ORPHAN_SETTLEMENT, BreakType.STATUS_CONFLICT,
            BreakType.ORDER_ANOMALY, BreakType.OVER_SETTLEMENT, BreakType.FILE_CONFLICT, BreakType.SETTLEMENT_CONFLICT,
            BreakType.INFERRED_MATCH_CHALLENGED, BreakType.IDEMPOTENCY_CONFLICT, BreakType.LEDGER_CONFLICT,
            BreakType.FEED_STALL));

    /** What a rule's deadline counts from. */
    public enum Anchor {
        /** The IST date of the payment's first debit evidence. */
        T_DATE,
        /** The break's detection time. */
        DETECTION
    }

    private final Anchor anchor;
    private final int days;
    private final int hours;
    private final Duration margin;
    private final long compensationRupeesPerDay;
    private final List<BreakType> appliesTo;

    DeadlineRule(Anchor anchor, int days, int hours, Duration margin, long compensationRupeesPerDay,
                 List<BreakType> appliesTo) {
        this.anchor = anchor;
        this.days = days;
        this.hours = hours;
        this.margin = margin;
        this.compensationRupeesPerDay = compensationRupeesPerDay;
        this.appliesTo = appliesTo;
    }

    public Anchor anchor() {
        return anchor;
    }

    public int days() {
        return days;
    }

    public int hours() {
        return hours;
    }

    public Duration margin() {
        return margin;
    }

    public long compensationRupeesPerDay() {
        return compensationRupeesPerDay;
    }

    public List<BreakType> appliesTo() {
        return appliesTo;
    }

    /** The rule for a break type; every type has exactly one. */
    public static DeadlineRule forType(BreakType type) {
        DeadlineRule found = null;
        for (DeadlineRule r : values()) {
            if (r.appliesTo.contains(type)) {
                if (found != null) {
                    throw new IllegalStateException(type + " has two deadline rules");
                }
                found = r;
            }
        }
        if (found == null) {
            throw new IllegalStateException(type + " has no deadline rule");
        }
        return found;
    }

    /**
     * @param tDate      the payment's T date, for a T-date rule (ignored otherwise)
     * @param detectedAt the break's detection time, for a detection rule (ignored otherwise)
     */
    public Instant deadlineAt(LocalDate tDate, Instant detectedAt) {
        return switch (anchor) {
            case T_DATE -> IstCalendar.deadlineAt(Objects.requireNonNull(tDate, "tDate"), days);
            case DETECTION -> Objects.requireNonNull(detectedAt, "detectedAt").plus(Duration.ofHours(hours));
        };
    }

    public Instant escalateBy(Instant deadlineAt) {
        return deadlineAt.minus(margin);
    }

    /**
     * The compensation exposure of a break with this deadline, resolved (or still open) at {@code at}: ₹100 for each IST
     * day after the deadline date, on the RBI rules only; zero otherwise. The deadline date is the last IST day still
     * within the deadline (T + days).
     */
    public Money compensation(Instant deadlineAt, Instant at) {
        if (compensationRupeesPerDay == 0 || at.isBefore(deadlineAt)) {
            return Money.ZERO;
        }
        LocalDate deadlineDate = IstCalendar.istDate(deadlineAt.minusNanos(1));
        long lateDays = ChronoUnit.DAYS.between(deadlineDate, IstCalendar.istDate(at));
        return Money.ofPaise(Math.multiplyExact(Math.multiplyExact(lateDays, compensationRupeesPerDay), 100L));
    }
}
