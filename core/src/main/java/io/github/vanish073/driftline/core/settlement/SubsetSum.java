package io.github.vanish073.driftline.core.settlement;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Exact subset-sum for batch settlement lines (DESIGN §6.4): which non-empty subset of {@code values} sums to
 * {@code target}, and is it the only one? It never guesses. It answers {@link None}, {@link Unique},
 * {@link Ambiguous} (with two distinct, verified witnesses) or {@link BudgetExceeded}.
 *
 * <ul>
 *   <li><b>Deterministic.</b> The answer depends only on the inputs and their order; the budget counts operations,
 *       never wall-clock time (D7). No hash maps: sorted {@code long[]} arrays and flat tables.</li>
 *   <li><b>Budget-independent answers.</b> The engine is chosen by estimated cost alone, and the budget only decides
 *       whether that engine may run. A bigger budget therefore never changes an answer that is not
 *       {@code BudgetExceeded} (property P6).</li>
 *   <li><b>Exact.</b> Every sum uses {@code Math.addExact}. Every partial sum lies within {@code ±Σ|values|}, and the
 *       complement {@code sum − target} is formed only when {@code sum ≥ target > 0}, so the caller must keep
 *       {@code Σ|values|} within a {@code long} ({@link #fitsExactly}); otherwise the solver refuses rather than risk a
 *       wrapped sum.</li>
 * </ul>
 *
 * <p>Steps: (1) divide by the gcd, or answer NONE if the target is not a multiple of it; (2) when every value is
 * positive, the whole set is the only solution if it sums to the target, nothing is if the target is larger, and the
 * complement is solved when it is the smaller target; (3) the cheaper of meet-in-the-middle (n ≤ 40, any signs) and a
 * capped-count dynamic programme (positive values) runs, if it fits the budget. Both count solutions up to two.
 *
 * <p>Witnesses are indices into {@code values}, ascending.
 */
public final class SubsetSum {

    /** Meet-in-the-middle handles at most this many values (2 × 2²⁰ partial sums). */
    public static final int MITM_MAX_N = 40;

    private SubsetSum() {
    }

    /** The answer. */
    public sealed interface Result permits None, Unique, Ambiguous, BudgetExceeded {
    }

    /** No non-empty subset sums to the target. */
    public record None() implements Result {
    }

    /** Exactly one non-empty subset sums to the target. */
    public record Unique(List<Integer> subset) implements Result {
        public Unique {
            subset = List.copyOf(subset);
        }
    }

    /** At least two distinct non-empty subsets sum to the target; two of them, verified. */
    public record Ambiguous(List<Integer> first, List<Integer> second) implements Result {
        public Ambiguous {
            first = List.copyOf(first);
            second = List.copyOf(second);
            if (first.equals(second)) {
                throw new IllegalArgumentException("the two witnesses must differ");
            }
        }
    }

    /** The cheapest engine needs more operations than the budget allows. */
    public record BudgetExceeded(long estimatedOps) implements Result {
    }

    /**
     * Whether {@code Σ|values|} fits in a {@code long} and no input is {@code Long.MIN_VALUE}, which {@link #solve}
     * requires: then no partial sum, comparison or complement can overflow.
     */
    public static boolean fitsExactly(long[] values, long target) {
        try {
            Math.absExact(target);
            long total = 0;
            for (long v : values) {
                total = Math.addExact(total, Math.absExact(v));
            }
            return true;
        } catch (ArithmeticException e) {
            return false;
        }
    }

    public static Result solve(long[] values, long target, long budgetOps) {
        return solve(values, target, budgetOps, false);
    }

    /**
     * @param firstSolutionWins canary C08 ({@code FIRST_SOLUTION_WINS}): report the first fit found as unique, with no
     *                          check for a second one. Never true outside the simulator.
     */
    public static Result solve(long[] values, long target, long budgetOps, boolean firstSolutionWins) {
        return solveCounted(values, target, budgetOps, firstSolutionWins).result();
    }

    /**
     * An answer with the cost of the engine that produced it, in the budget's operations (§13 {@code
     * driftline_subset_sum_ops}): 0 for the fast paths, and 0 for {@link BudgetExceeded}, since no engine ran.
     */
    public record Counted(Result result, long ops) {
        public Counted {
            Objects.requireNonNull(result, "result");
            if (ops < 0) {
                throw new IllegalArgumentException("ops must not be negative");
            }
        }
    }

    /** {@link #solve(long[], long, long, boolean)}, with the operations the chosen engine cost. */
    public static Counted solveCounted(long[] values, long target, long budgetOps, boolean firstSolutionWins) {
        Objects.requireNonNull(values, "values");
        if (budgetOps < 0) {
            throw new IllegalArgumentException("the budget must not be negative");
        }
        if (!fitsExactly(values, target)) {
            throw new IllegalArgumentException("Σ|values| exceeds the exact range of a long");
        }
        int n = values.length;
        if (n == 0) {
            return new Counted(new None(), 0);
        }
        long[] v = values.clone();
        long t = target;

        // 1. gcd reduction
        long g = 0;
        for (long x : v) {
            g = gcd(g, Math.abs(x));
        }
        if (g == 0) {   // every value is zero: any non-empty subset sums to zero
            if (t != 0) {
                return new Counted(new None(), 0);
            }
            return new Counted(n == 1 ? new Unique(List.of(0)) : pick(List.of(0), List.of(1), firstSolutionWins), 0);
        }
        if (t % g != 0) {
            return new Counted(new None(), 0);
        }
        if (g > 1) {
            for (int i = 0; i < n; i++) {
                v[i] /= g;
            }
            t /= g;
        }

        // 2. all positive: fast path and complement
        boolean allPositive = true;
        long sum = 0;
        for (long x : v) {
            allPositive &= x > 0;
            sum = Math.addExact(sum, x);
        }
        if (allPositive) {
            if (t <= 0 || sum < t) {
                return new Counted(new None(), 0);
            }
            if (sum == t) {
                return new Counted(new Unique(all(n)), 0);
            }
            long complement = sum - t;
            if (complement < t) {
                Counted r = engines(v, complement, budgetOps, true, firstSolutionWins);
                return new Counted(complementOf(r.result(), n), r.ops());
            }
        }

        // 3. the cheaper engine
        return engines(v, t, budgetOps, allPositive, firstSolutionWins);
    }

    private static Counted engines(long[] v, long t, long budgetOps, boolean allPositive, boolean firstSolutionWins) {
        int n = v.length;
        long mitmCost = n <= MITM_MAX_N ? mitmCost(n) : Long.MAX_VALUE;
        long dpCost = allPositive && t > 0 ? dpCost(n, t) : Long.MAX_VALUE;
        long cheapest = Math.min(mitmCost, dpCost);
        if (cheapest == Long.MAX_VALUE) {
            return new Counted(new BudgetExceeded(Long.MAX_VALUE), 0);
        }
        if (cheapest > budgetOps) {
            return new Counted(new BudgetExceeded(cheapest), 0);
        }
        return new Counted(mitmCost <= dpCost ? mitm(v, t, firstSolutionWins) : dp(v, (int) t, firstSolutionWins),
                cheapest);
    }

    // ---- meet-in-the-middle --------------------------------------------------------------------------------------

    /** Partial sums generated plus merge steps, for both halves. */
    static long mitmCost(int n) {
        int h = n / 2;
        return 3L * ((1L << h) + (1L << (n - h)));
    }

    static Result mitm(long[] v, long t, boolean firstSolutionWins) {
        int n = v.length;
        int h = n / 2;
        Half left = half(v, 0, h);
        Half right = half(v, h, n);
        List<List<Integer>> found = new ArrayList<>(2);
        int i = 0;
        int j = right.size() - 1;
        while (i < left.size() && j >= 0 && found.size() < 2) {
            long s = Math.addExact(left.sums[i], right.sums[j]);
            if (s < t) {
                i++;
            } else if (s > t) {
                j--;
            } else {
                int iEnd = i;
                while (iEnd < left.size() && left.sums[iEnd] == left.sums[i]) {
                    iEnd++;
                }
                int jStart = j;
                while (jStart >= 0 && right.sums[jStart] == right.sums[j]) {
                    jStart--;
                }
                for (int a = i; a < iEnd && found.size() < 2; a++) {
                    for (int b = jStart + 1; b <= j && found.size() < 2; b++) {
                        if (left.masks[a] == 0 && right.masks[b] == 0) {
                            continue;   // the empty subset is not a settlement
                        }
                        found.add(indices(left.masks[a], 0, right.masks[b], h));
                        if (firstSolutionWins) {
                            return new Unique(found.get(0));
                        }
                    }
                }
                i = iEnd;
                j = jStart;
            }
        }
        return outcome(found);
    }

    /** Every subset sum of {@code v[from, to)}, ascending, with the subset as a bit mask (bit k = value from + k). */
    private record Half(long[] sums, int[] masks) {
        int size() {
            return sums.length;
        }
    }

    private static Half half(long[] v, int from, int to) {
        int size = 1 << (to - from);
        long[] sums = new long[size];
        int[] masks = new int[size];
        long[] outSums = new long[size];
        int[] outMasks = new int[size];
        int m = 1;   // sums[0] = 0, masks[0] = 0: the empty subset
        for (int k = 0; k < to - from; k++) {
            long add = v[from + k];
            int bit = 1 << k;
            int a = 0;
            int b = 0;
            int o = 0;
            // merge (sums) and (sums + add), both ascending; ties keep the subset without the new value first
            while (a < m || b < m) {
                long withNew = b < m ? Math.addExact(sums[b], add) : 0;
                if (b >= m || (a < m && sums[a] <= withNew)) {
                    outSums[o] = sums[a];
                    outMasks[o++] = masks[a++];
                } else {
                    outSums[o] = withNew;
                    outMasks[o++] = masks[b++] | bit;
                }
            }
            long[] swapSums = sums;
            sums = outSums;
            outSums = swapSums;
            int[] swapMasks = masks;
            masks = outMasks;
            outMasks = swapMasks;
            m *= 2;
        }
        return new Half(sums, masks);
    }

    private static List<Integer> indices(int leftMask, int leftFrom, int rightMask, int rightFrom) {
        List<Integer> out = new ArrayList<>();
        for (int k = 0; k < 32; k++) {
            if ((leftMask & (1 << k)) != 0) {
                out.add(leftFrom + k);
            }
        }
        for (int k = 0; k < 32; k++) {
            if ((rightMask & (1 << k)) != 0) {
                out.add(rightFrom + k);
            }
        }
        return out;
    }

    // ---- capped-count dynamic programme ---------------------------------------------------------------------------

    /** One table cell per (value, partial sum), plus the walk back. Long.MAX_VALUE if the table cannot exist. */
    static long dpCost(int n, long t) {
        if (t >= Integer.MAX_VALUE - 1) {
            return Long.MAX_VALUE;
        }
        try {
            return Math.multiplyExact((long) n + 1, t + 1);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * {@code ways[i][s]}: how many subsets of {@code v[i..n)} sum to {@code s}, counting the empty one, capped at 2.
     * The two walks back from {@code (0, t)}, one preferring to skip a value and one preferring to take it, give the
     * same subset exactly when the solution is unique.
     */
    static Result dp(long[] v, int t, boolean firstSolutionWins) {
        int n = v.length;
        int width = t + 1;
        byte[] ways = new byte[Math.multiplyExact(n + 1, width)];
        ways[n * width] = 1;   // the empty subset of no values sums to 0
        for (int i = n - 1; i >= 0; i--) {
            int row = i * width;
            int next = (i + 1) * width;
            long x = v[i];
            for (int s = 0; s <= t; s++) {
                int count = ways[next + s];
                if (s >= x) {
                    count += ways[next + s - (int) x];
                }
                ways[row + s] = (byte) Math.min(2, count);
            }
        }
        if (ways[t] == 0) {
            return new None();
        }
        List<Integer> skipFirst = walk(v, t, ways, width, false);
        if (firstSolutionWins) {
            return new Unique(skipFirst);
        }
        if (ways[t] == 1) {
            return new Unique(skipFirst);
        }
        return new Ambiguous(skipFirst, walk(v, t, ways, width, true));
    }

    private static List<Integer> walk(long[] v, int t, byte[] ways, int width, boolean preferTake) {
        int n = v.length;
        List<Integer> out = new ArrayList<>();
        int s = t;
        for (int i = 0; i < n; i++) {
            int next = (i + 1) * width;
            boolean canSkip = ways[next + s] > 0;
            boolean canTake = s >= v[i] && ways[next + s - (int) v[i]] > 0;
            if (canTake && (preferTake || !canSkip)) {
                out.add(i);
                s -= (int) v[i];
            }
        }
        return out;
    }

    // ---- helpers ----------------------------------------------------------------------------------------------------

    private static Result outcome(List<List<Integer>> found) {
        return switch (found.size()) {
            case 0 -> new None();
            case 1 -> new Unique(found.get(0));
            default -> new Ambiguous(found.get(0), found.get(1));
        };
    }

    private static Result pick(List<Integer> first, List<Integer> second, boolean firstSolutionWins) {
        return firstSolutionWins ? new Unique(first) : new Ambiguous(first, second);
    }

    private static Result complementOf(Result r, int n) {
        return switch (r) {
            case Unique u -> new Unique(complement(u.subset(), n));
            case Ambiguous a -> new Ambiguous(complement(a.first(), n), complement(a.second(), n));
            case None none -> none;
            case BudgetExceeded b -> b;
        };
    }

    private static List<Integer> complement(List<Integer> subset, int n) {
        boolean[] in = new boolean[n];
        subset.forEach(i -> in[i] = true);
        List<Integer> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (!in[i]) {
                out.add(i);
            }
        }
        return out;
    }

    private static List<Integer> all(int n) {
        List<Integer> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(i);
        }
        return out;
    }

    private static long gcd(long a, long b) {
        while (b != 0) {
            long r = a % b;
            a = b;
            b = r;
        }
        return a;
    }

    /** Whether {@code subset} is non-empty, ascending, within bounds, and sums to {@code target} (exactly). */
    public static boolean verifies(long[] values, long target, List<Integer> subset) {
        if (subset.isEmpty()) {
            return false;
        }
        long sum = 0;
        int previous = -1;
        for (int i : subset) {
            if (i <= previous || i >= values.length) {
                return false;
            }
            previous = i;
            sum = Math.addExact(sum, values[i]);
        }
        return sum == target;
    }
}
