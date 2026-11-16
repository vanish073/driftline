package io.github.vanish073.driftline.core.breaks;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

/** DESIGN §8.1 and §8.3: the lifecycle table, deadlines, escalate_by and compensation. */
class DeadlineRuleTest {

    static final LocalDate T = LocalDate.parse("2026-11-13");

    @Test
    void everyBreakTypeHasExactlyOneRule() {
        for (BreakType type : BreakType.values()) {
            assertThat(DeadlineRule.forType(type)).as("%s", type).isNotNull();
        }
        assertThat(DeadlineRule.forType(BreakType.DEBITED_NO_ORDER)).isEqualTo(DeadlineRule.RBI_UPI_P2M_T5);
        assertThat(DeadlineRule.forType(BreakType.LATE_SUCCESS)).isEqualTo(DeadlineRule.RBI_UPI_P2M_T5);
        assertThat(DeadlineRule.forType(BreakType.DUPLICATE_CAPTURE)).isEqualTo(DeadlineRule.INTERNAL_DUP_T1);
        assertThat(DeadlineRule.forType(BreakType.ORPHAN_SETTLEMENT)).isEqualTo(DeadlineRule.INTERNAL_ESCALATE_4H);
    }

    @Test
    void aT5DeadlineIsTheEndOfIstDayTPlus5AndEscalationComesADayEarlier() {
        Instant deadline = DeadlineRule.RBI_UPI_P2M_T5.deadlineAt(T, null);
        assertThat(deadline).isEqualTo(Instant.parse("2026-11-18T18:30:00Z"));   // 00:00 IST on 19 Nov
        assertThat(DeadlineRule.RBI_UPI_P2M_T5.escalateBy(deadline)).isEqualTo(Instant.parse("2026-11-17T18:30:00Z"));
    }

    @Test
    void aDetectionRuleCountsFourHoursFromDetectionWithNoMargin() {
        Instant detected = Instant.parse("2026-11-14T05:00:00Z");
        Instant deadline = DeadlineRule.INTERNAL_ESCALATE_4H.deadlineAt(null, detected);
        assertThat(deadline).isEqualTo(Instant.parse("2026-11-14T09:00:00Z"));
        assertThat(DeadlineRule.INTERNAL_ESCALATE_4H.escalateBy(deadline)).isEqualTo(deadline);
    }

    @Test
    void compensationIs100RupeesPerIstDayAfterTheDeadlineDateOnRbiRulesOnly() {
        Instant deadline = DeadlineRule.RBI_UPI_P2M_T5.deadlineAt(T, null);
        assertThat(DeadlineRule.RBI_UPI_P2M_T5.compensation(deadline, deadline.minusSeconds(1))).isEqualTo(Money.ZERO);
        assertThat(DeadlineRule.RBI_UPI_P2M_T5.compensation(deadline, deadline))
                .as("00:00 IST on T+6 is one day after the deadline date T+5").isEqualTo(Money.ofPaise(10_000));
        assertThat(DeadlineRule.RBI_UPI_P2M_T5.compensation(deadline, Instant.parse("2026-11-21T18:29:59Z")))
                .as("23:59:59 IST on T+8").isEqualTo(Money.ofPaise(30_000));
        assertThat(DeadlineRule.INTERNAL_DUP_T1.compensation(DeadlineRule.INTERNAL_DUP_T1.deadlineAt(T, null),
                Instant.parse("2026-12-01T00:00:00Z"))).as("internal rules carry no compensation").isEqualTo(Money.ZERO);
    }

    @Test
    void theLifecycleTableKeepsFinalStatesFinalAndEscalationReachableFromEveryOpenState() {
        for (BreakState from : BreakState.values()) {
            if (BreakLifecycle.isFinal(from)) {
                for (BreakState to : BreakState.values()) {
                    assertThat(BreakLifecycle.allowed(from, to)).as("%s -> %s", from, to).isFalse();
                }
            } else if (from != BreakState.ESCALATED) {
                assertThat(BreakLifecycle.allowed(from, BreakState.ESCALATED)).as("%s -> ESCALATED", from).isTrue();
            }
        }
        assertThat(BreakLifecycle.allowed(BreakState.ACTION_PENDING, BreakState.SELF_HEALED))
                .as("a refund may be in flight: never heal quietly").isFalse();
        assertThat(BreakLifecycle.allowed(BreakState.ESCALATED, BreakState.RESOLVED_MANUAL)).isTrue();
        assertThat(BreakLifecycle.isHandled(BreakState.ESCALATED)).isTrue();
        assertThat(BreakLifecycle.isHandled(BreakState.ACTION_PENDING)).isFalse();
    }
}
