package io.github.vanish073.driftline.core.settlement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.settlement.BatchMatching.Candidate;
import io.github.vanish073.driftline.core.settlement.BatchMatching.Kind;
import io.github.vanish073.driftline.core.settlement.BatchMatching.Line;
import io.github.vanish073.driftline.core.settlement.BatchMatching.Outcome;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The pure batch-matching layer (DESIGN §6.4); the settlement case corpus covers it through the matcher too. */
class BatchMatchingTest {

    static final LocalDate D = LocalDate.parse("2026-11-14");
    static final Instant T = Instant.parse("2026-11-13T05:00:00Z");   // 10:30 IST on 13 Nov: batch date 14 Nov
    static final long BUDGET = 20_000_000L;

    static Candidate c(String id, long netPaise, int minutes) {
        return new Candidate(PaymentId.of(id), T.plusSeconds(60L * minutes), D, Money.ofPaise(netPaise));
    }

    static Line line(int no, long paise) {
        return new Line("stl_1:" + no, D, Money.ofPaise(paise));
    }

    @Test
    void aBatchDateIsTheIstCaptureDatePlusTheCycle() {
        assertThat(BatchMatching.batchDate(Instant.parse("2026-11-13T18:29:59Z"), 1)).isEqualTo(D);
        assertThat(BatchMatching.batchDate(Instant.parse("2026-11-13T18:30:00Z"), 1)).isEqualTo(D.plusDays(1));
    }

    @Test
    void amountsBeyondTheExactRangeAreRefusedNotWrapped() {
        long big = Long.MAX_VALUE / 2 + 1;
        Outcome o = BatchMatching.match(List.of(line(1, 5)), List.of(c("pay_a", big, 0), c("pay_b", big, 1)), 1,
                BUDGET, false).get(0);
        assertThat(o.kind()).isEqualTo(Kind.OUT_OF_RANGE);
        assertThat(o.candidates()).hasSize(2);
        assertThatThrownBy(() -> SubsetSum.solve(new long[] {big, big}, 5, BUDGET))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void candidatesAreInCanonicalOrderAndTheHashCoversThem() {
        List<Candidate> pool = List.of(c("pay_b", 300, 5), c("pay_a", 200, 5), c("pay_c", 100, 1));
        Outcome o = BatchMatching.match(List.of(line(1, 500)), pool, 1, BUDGET, false).get(0);
        assertThat(o.kind()).isEqualTo(Kind.INFERRED);
        assertThat(o.candidates()).extracting(PaymentId::value).containsExactly("pay_c", "pay_a", "pay_b");
        assertThat(o.matched()).extracting(PaymentId::value).containsExactly("pay_a", "pay_b");
        assertThat(o.candidateHash()).isEqualTo(BatchMatching.hash(List.of(pool.get(2), pool.get(1), pool.get(0))));
        Outcome reordered = BatchMatching.match(List.of(line(1, 500)), List.of(pool.get(2), pool.get(0), pool.get(1)),
                1, BUDGET, false).get(0);
        assertThat(reordered).as("the pool's order does not matter").isEqualTo(o);
    }

    @Test
    void theLookbackBoundsTheWindowOnBothSides() {
        Candidate tooOld = new Candidate(PaymentId.of("pay_old"), T, D.minusDays(2), Money.ofPaise(100));
        Candidate tooNew = new Candidate(PaymentId.of("pay_new"), T, D.plusDays(1), Money.ofPaise(100));
        Candidate inside = new Candidate(PaymentId.of("pay_in"), T, D.minusDays(1), Money.ofPaise(100));
        Outcome o = BatchMatching.match(List.of(line(1, 100)), List.of(tooOld, tooNew, inside), 1, BUDGET, false).get(0);
        assertThat(o.matched()).extracting(PaymentId::value).containsExactly("pay_in");
        assertThat(o.candidates()).hasSize(1);
    }

    @Test
    void anAmbiguousLineCarriesTwoVerifiedWitnessesAndTheCanaryHidesIt() {
        List<Candidate> pool = List.of(c("pay_1", 499, 0), c("pay_2", 499, 1));
        Outcome honest = BatchMatching.match(List.of(line(1, 499)), pool, 1, BUDGET, false).get(0);
        assertThat(honest.kind()).isEqualTo(Kind.AMBIGUOUS);
        assertThat(honest.witnesses()).containsExactlyInAnyOrder(List.of(PaymentId.of("pay_1")),
                List.of(PaymentId.of("pay_2")));
        Outcome canary = BatchMatching.match(List.of(line(1, 499)), pool, 1, BUDGET, true).get(0);
        assertThat(canary.kind()).as("C08 reports the first fit as unique").isEqualTo(Kind.INFERRED);
    }

    @Test
    void aPaymentCannotBeInThePoolTwice() {
        assertThatThrownBy(() -> BatchMatching.match(List.of(line(1, 1)), List.of(c("pay_1", 1, 0), c("pay_1", 1, 1)),
                1, BUDGET, false)).isInstanceOf(IllegalArgumentException.class);
    }
}
