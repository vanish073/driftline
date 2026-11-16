package io.github.vanish073.driftline.core.settlement;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.settlement.SubsetSum.Ambiguous;
import io.github.vanish073.driftline.core.settlement.SubsetSum.BudgetExceeded;
import io.github.vanish073.driftline.core.settlement.SubsetSum.None;
import io.github.vanish073.driftline.core.settlement.SubsetSum.Result;
import io.github.vanish073.driftline.core.settlement.SubsetSum.Unique;
import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

/**
 * Properties P1–P6 of the batch solver (SHIFTS Shift 7, DESIGN §6.4, §11.8: the solver oracle is brute force for
 * n ≤ 18). Small value ranges force collisions, so ambiguity is common; negative values exercise meet-in-the-middle.
 */
class SubsetSumProperties {

    static final long BIG = 1_000_000_000L;

    /** A problem: values and a target. */
    record Problem(long[] values, long target) {
    }

    /** Brute force: up to two solutions, in ascending mask order. */
    static List<List<Integer>> bruteForce(long[] values, long target) {
        int n = values.length;
        List<List<Integer>> found = new ArrayList<>();
        for (int mask = 1; mask < (1 << n) && found.size() < 3; mask++) {
            long sum = 0;
            for (int i = 0; i < n; i++) {
                if ((mask & (1 << i)) != 0) {
                    sum += values[i];
                }
            }
            if (sum == target) {
                List<Integer> subset = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    if ((mask & (1 << i)) != 0) {
                        subset.add(i);
                    }
                }
                found.add(subset);
            }
        }
        return found;
    }

    /** The answer agrees with brute force: class, the unique subset, and verified distinct witnesses. */
    static void assertAgreesWithBruteForce(long[] values, long target, Result r) {
        List<List<Integer>> truth = bruteForce(values, target);
        switch (truth.size()) {
            case 0 -> assertThat(r).isInstanceOf(None.class);
            case 1 -> assertThat(r).isEqualTo(new Unique(truth.get(0)));
            default -> {
                assertThat(r).isInstanceOf(Ambiguous.class);
                assertWitnessed(values, target, (Ambiguous) r);
            }
        }
    }

    static void assertWitnessed(long[] values, long target, Ambiguous a) {
        assertThat(SubsetSum.verifies(values, target, a.first())).as("first witness %s", a.first()).isTrue();
        assertThat(SubsetSum.verifies(values, target, a.second())).as("second witness %s", a.second()).isTrue();
        assertThat(a.first()).isNotEqualTo(a.second());
    }

    @Provide
    Arbitrary<Problem> small() {
        Arbitrary<long[]> values = Arbitraries.longs().between(-12, 25).array(long[].class).ofMinSize(0).ofMaxSize(18);
        return Combinators.combine(values, Arbitraries.longs().between(-40, 120)).as(Problem::new);
    }

    @Provide
    Arbitrary<Problem> smallPositive() {
        Arbitrary<long[]> values = Arbitraries.longs().between(1, 30).array(long[].class).ofMinSize(1).ofMaxSize(18);
        return Combinators.combine(values, Arbitraries.longs().between(1, 250)).as(Problem::new);
    }

    // P1: the solver equals a brute-force oracle

    @Property(tries = 1500)
    void p1SolverEqualsBruteForce(@ForAll("small") Problem p) {
        assertAgreesWithBruteForce(p.values(), p.target(), SubsetSum.solve(p.values(), p.target(), BIG));
    }

    @Property(tries = 800)
    void p1MeetInTheMiddleEqualsBruteForce(@ForAll("small") Problem p) {
        assertAgreesWithBruteForce(p.values(), p.target(), SubsetSum.mitm(p.values(), p.target(), false));
    }

    @Property(tries = 800)
    void p1DynamicProgrammeEqualsBruteForce(@ForAll("smallPositive") Problem p) {
        assertAgreesWithBruteForce(p.values(), p.target(), SubsetSum.dp(p.values(), (int) p.target(), false));
    }

    // P2: permutation invariance

    @Property(tries = 800)
    void p2PermutationInvariance(@ForAll("small") Problem p, @ForAll long shuffleSeed) {
        int n = p.values().length;
        int[] perm = permutation(n, shuffleSeed);
        long[] shuffled = new long[n];
        for (int i = 0; i < n; i++) {
            shuffled[i] = p.values()[perm[i]];
        }
        Result original = SubsetSum.solve(p.values(), p.target(), BIG);
        Result permuted = SubsetSum.solve(shuffled, p.target(), BIG);
        assertThat(permuted.getClass()).isEqualTo(original.getClass());
        if (original instanceof Unique u) {
            List<Integer> mapped = ((Unique) permuted).subset().stream().map(i -> perm[i]).sorted().toList();
            assertThat(mapped).as("the same payments, whatever their order").isEqualTo(u.subset());
        }
        if (permuted instanceof Ambiguous a) {
            assertWitnessed(shuffled, p.target(), a);
        }
    }

    // P3: scaling invariance

    @Property(tries = 800)
    void p3ScalingInvariance(@ForAll("small") Problem p, @ForAll @IntRange(min = 2, max = 1000) int k) {
        long[] scaled = new long[p.values().length];
        for (int i = 0; i < scaled.length; i++) {
            scaled[i] = p.values()[i] * k;
        }
        Result original = SubsetSum.solve(p.values(), p.target(), BIG);
        Result scaledResult = SubsetSum.solve(scaled, p.target() * k, BIG);
        assertThat(scaledResult).as("dividing by the gcd makes the scaled problem the same problem")
                .isEqualTo(original);
        assertThat(SubsetSum.solve(scaled, p.target() * k + 1, BIG)).as("a target off the gcd lattice")
                .isInstanceOf(None.class);
    }

    // P4: a planted solution never gives NONE

    @Property(tries = 1000)
    void p4PlantedSolutionIsNeverNone(@ForAll("small") Problem p, @ForAll long plantSeed) {
        long[] v = p.values();
        if (v.length == 0) {
            return;
        }
        long seed = plantSeed;
        long target = 0;
        boolean any = false;
        for (int i = 0; i < v.length; i++) {
            seed = seed * 6364136223846793005L + 1442695040888963407L;
            if ((seed >>> 63) == 1 || (!any && i == v.length - 1)) {
                target += v[i];
                any = true;
            }
        }
        Result r = SubsetSum.solve(v, target, BIG);
        assertThat(r).isNotInstanceOf(None.class).isNotInstanceOf(BudgetExceeded.class);
    }

    // P5: witnesses always verify

    @Property(tries = 1500)
    void p5WitnessesAlwaysVerify(@ForAll("small") Problem p) {
        Result r = SubsetSum.solve(p.values(), p.target(), BIG);
        if (r instanceof Unique u) {
            assertThat(SubsetSum.verifies(p.values(), p.target(), u.subset())).isTrue();
        } else if (r instanceof Ambiguous a) {
            assertWitnessed(p.values(), p.target(), a);
        }
    }

    // P6: a bigger budget never changes a non-BUDGET answer

    @Property(tries = 1000)
    void p6BiggerBudgetNeverChangesAnAnswer(@ForAll("small") Problem p,
                                            @ForAll @IntRange(min = 0, max = 5000) int smallBudget) {
        Result constrained = SubsetSum.solve(p.values(), p.target(), smallBudget);
        Result generous = SubsetSum.solve(p.values(), p.target(), BIG);
        if (!(constrained instanceof BudgetExceeded)) {
            assertThat(generous).isEqualTo(constrained);
        }
        assertThat(generous).isNotInstanceOf(BudgetExceeded.class);
    }

    /** C08: with the canary on, a fit is reported as unique even when brute force finds two. */
    @Property(tries = 300)
    void theFirstSolutionWinsCanaryHidesAmbiguity(@ForAll("small") Problem p) {
        Result honest = SubsetSum.solve(p.values(), p.target(), BIG, false);
        Result canary = SubsetSum.solve(p.values(), p.target(), BIG, true);
        if (honest instanceof Ambiguous) {
            assertThat(canary).isInstanceOf(Unique.class);
        } else {
            assertThat(canary).isEqualTo(honest);
        }
    }

    private static int[] permutation(int n, long seed) {
        int[] perm = new int[n];
        for (int i = 0; i < n; i++) {
            perm[i] = i;
        }
        long s = seed;
        for (int i = n - 1; i > 0; i--) {
            s = s * 6364136223846793005L + 1442695040888963407L;
            int j = (int) Long.remainderUnsigned(s >>> 1, i + 1);
            int tmp = perm[i];
            perm[i] = perm[j];
            perm[j] = tmp;
        }
        return perm;
    }
}
