package io.github.vanish073.driftline.core.settlement;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.time.IstCalendar;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.LongConsumer;

/**
 * Batch matching of the unreferenced lines of one settlement file (DESIGN §6.4): conservative narrowing, the exact
 * solver, and forced-assignment propagation to a fixpoint. A pure function: the matcher gathers the inputs and
 * records the outcomes.
 *
 * <p><b>Narrowing.</b> The caller passes the pool: the merchant's payments that SUCCEEDED, are not yet settled, and
 * are not referenced by any line. Each line then keeps the payments whose batch date lies in
 * {@code [settlementDate − lookbackDays, settlementDate]}, in the canonical order (capture time, payment id).
 *
 * <p><b>Propagation.</b> A line with a unique solution over every payment still open must take exactly that subset in
 * any joint assignment, so committing it and re-solving the rest is sound. After each commit the pass starts again,
 * so a later line never keeps a stale answer. Lines still ambiguous at the fixpoint are reported as ambiguous, even
 * when they would be jointly unique: cautious, never wrong.
 */
public final class BatchMatching {

    private BatchMatching() {
    }

    /** A payment that could be in a batch line. */
    public record Candidate(PaymentId paymentId, Instant capturedAt, LocalDate batchDate, Money net) {
        public Candidate {
            Objects.requireNonNull(paymentId, "paymentId");
            Objects.requireNonNull(capturedAt, "capturedAt");
            Objects.requireNonNull(batchDate, "batchDate");
            Objects.requireNonNull(net, "net");
        }

        /** The canonical order of §6.4: capture time, then payment id. */
        static final Comparator<Candidate> ORDER = Comparator.comparing(Candidate::capturedAt)
                .thenComparing(c -> c.paymentId().value());
    }

    /**
     * The batch date of a payment captured at {@code capturedAt}: its IST capture date plus the merchant's cycle. The
     * cutoff is 00:00 IST; v1 stores no other cutoff (D48).
     */
    public static LocalDate batchDate(Instant capturedAt, int cycleDays) {
        return IstCalendar.istDate(capturedAt).plusDays(cycleDays);
    }

    /** An unreferenced line: its key ({@code fileId:lineNo}), its file's settlement date and the amount received. */
    public record Line(String key, LocalDate settlementDate, Money amount) {
        public Line {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(settlementDate, "settlementDate");
            Objects.requireNonNull(amount, "amount");
        }
    }

    /** What happened to one line. */
    public enum Kind {
        /** A unique subset of the candidates sums to the line: MATCHED_INFERRED. */
        INFERRED,
        /** Two or more subsets fit; two are given as witnesses. */
        AMBIGUOUS,
        /** No subset fits (this includes a line with no candidates). */
        NO_SOLUTION,
        /** The cheapest exact search needs more operations than the budget. */
        BUDGET_EXCEEDED,
        /** A zero amount settles nothing: the line is rejected and flagged. */
        ZERO_AMOUNT,
        /** The amounts are too large to sum exactly in a {@code long}. */
        OUT_OF_RANGE
    }

    /**
     * @param matched       the payments of an INFERRED line, in canonical order; empty otherwise
     * @param witnesses     two distinct subsets for an AMBIGUOUS line, each in canonical order; empty otherwise
     * @param candidates    the candidate set the final answer was computed over, in canonical order
     * @param candidateHash SHA-256 of that candidate set ({@code paymentId:netPaise} joined by {@code ;}), recorded with
     *                      an inferred match so the proof can be re-checked
     */
    public record Outcome(Line line, Kind kind, List<PaymentId> matched, List<List<PaymentId>> witnesses,
                          List<PaymentId> candidates, Sha256 candidateHash, long estimatedOps) {
        public Outcome {
            Objects.requireNonNull(line, "line");
            Objects.requireNonNull(kind, "kind");
            matched = List.copyOf(matched);
            witnesses = witnesses.stream().map(List::copyOf).toList();
            candidates = List.copyOf(candidates);
            Objects.requireNonNull(candidateHash, "candidateHash");
            if ((kind == Kind.INFERRED) == matched.isEmpty()) {
                throw new IllegalArgumentException("exactly the INFERRED outcomes carry the matched payments");
            }
            if ((kind == Kind.AMBIGUOUS) != (witnesses.size() == 2)) {
                throw new IllegalArgumentException("exactly the AMBIGUOUS outcomes carry two witnesses");
            }
        }
    }

    /**
     * Matches the unreferenced lines of one file.
     *
     * @param lines        in line-number order
     * @param pool         every payment that may be in any of the lines (see the class comment), in any order
     * @param lookbackDays how many days before the settlement date a batch may reach back
     * @return one outcome per line, in the order of {@code lines}
     */
    public static List<Outcome> match(List<Line> lines, List<Candidate> pool, int lookbackDays, long budgetOps,
                                      boolean firstSolutionWins) {
        return match(lines, pool, lookbackDays, budgetOps, firstSolutionWins, ops -> { });
    }

    /**
     * {@link #match(List, List, int, long, boolean)}, telling {@code solved} the cost in operations of every subset-sum
     * search actually run ({@link SubsetSum.Counted}; a cached answer runs none), for §13 {@code driftline_subset_sum_ops}.
     */
    public static List<Outcome> match(List<Line> lines, List<Candidate> pool, int lookbackDays, long budgetOps,
                                      boolean firstSolutionWins, LongConsumer solved) {
        if (lookbackDays < 0) {
            throw new IllegalArgumentException("lookbackDays must not be negative");
        }
        TreeSet<String> open = new TreeSet<>();
        pool.forEach(c -> open.add(c.paymentId().value()));
        if (open.size() != pool.size()) {
            throw new IllegalArgumentException("a payment is in the pool twice");
        }
        TreeMap<String, Outcome> decided = new TreeMap<>();
        TreeMap<String, Outcome> lastSolved = new TreeMap<>();   // re-solve a line only when its candidates change
        boolean committed = true;
        while (committed) {
            committed = false;
            for (Line line : lines) {
                if (decided.containsKey(line.key())) {
                    continue;
                }
                Outcome o = solveOne(line, pool, open, lookbackDays, budgetOps, firstSolutionWins, lastSolved, solved);
                if (o.kind() == Kind.INFERRED) {
                    decided.put(line.key(), o);
                    o.matched().forEach(p -> open.remove(p.value()));
                    committed = true;
                    break;   // the open set changed: start the pass again
                }
                if (o.kind() == Kind.ZERO_AMOUNT || o.kind() == Kind.OUT_OF_RANGE) {
                    decided.put(line.key(), o);   // final whatever else happens
                }
            }
        }
        List<Outcome> out = new ArrayList<>(lines.size());
        for (Line line : lines) {
            Outcome o = decided.get(line.key());
            out.add(o != null ? o
                    : solveOne(line, pool, open, lookbackDays, budgetOps, firstSolutionWins, lastSolved, solved));
        }
        return out;
    }

    private static Outcome solveOne(Line line, List<Candidate> pool, TreeSet<String> open, int lookbackDays,
                                    long budgetOps, boolean firstSolutionWins, TreeMap<String, Outcome> lastSolved,
                                    LongConsumer solved) {
        LocalDate from = line.settlementDate().minusDays(lookbackDays);
        List<Candidate> candidates = pool.stream()
                .filter(c -> open.contains(c.paymentId().value()))
                .filter(c -> !c.batchDate().isBefore(from) && !c.batchDate().isAfter(line.settlementDate()))
                .sorted(Candidate.ORDER)
                .toList();
        List<PaymentId> ids = candidates.stream().map(Candidate::paymentId).toList();
        Outcome previous = lastSolved.get(line.key());
        if (previous != null && previous.candidates().equals(ids)) {
            return previous;   // the same candidates give the same answer: the solver is a pure function
        }
        Outcome o = solveFresh(line, candidates, ids, budgetOps, firstSolutionWins, solved);
        lastSolved.put(line.key(), o);
        return o;
    }

    private static Outcome solveFresh(Line line, List<Candidate> candidates, List<PaymentId> ids, long budgetOps,
                                      boolean firstSolutionWins, LongConsumer solved) {
        Sha256 hash = hash(candidates);
        if (line.amount().isZero()) {
            return new Outcome(line, Kind.ZERO_AMOUNT, List.of(), List.of(), ids, hash, 0);
        }
        long[] values = candidates.stream().mapToLong(c -> c.net().paise()).toArray();
        long target = line.amount().paise();
        if (!SubsetSum.fitsExactly(values, target)) {
            return new Outcome(line, Kind.OUT_OF_RANGE, List.of(), List.of(), ids, hash, 0);
        }
        SubsetSum.Counted counted = SubsetSum.solveCounted(values, target, budgetOps, firstSolutionWins);
        solved.accept(counted.ops());
        return switch (counted.result()) {
            case SubsetSum.Unique u ->
                    new Outcome(line, Kind.INFERRED, pick(candidates, u.subset()), List.of(), ids, hash, 0);
            case SubsetSum.Ambiguous a -> new Outcome(line, Kind.AMBIGUOUS, List.of(),
                    List.of(pick(candidates, a.first()), pick(candidates, a.second())), ids, hash, 0);
            case SubsetSum.None none -> new Outcome(line, Kind.NO_SOLUTION, List.of(), List.of(), ids, hash, 0);
            case SubsetSum.BudgetExceeded b ->
                    new Outcome(line, Kind.BUDGET_EXCEEDED, List.of(), List.of(), ids, hash, b.estimatedOps());
        };
    }

    private static List<PaymentId> pick(List<Candidate> candidates, List<Integer> indices) {
        return indices.stream().map(i -> candidates.get(i).paymentId()).toList();
    }

    /** SHA-256 of {@code paymentId:netPaise} for each candidate, joined by {@code ;}, in canonical order. */
    public static Sha256 hash(List<Candidate> candidates) {
        StringBuilder text = new StringBuilder();
        for (Candidate c : candidates) {
            if (!text.isEmpty()) {
                text.append(';');
            }
            text.append(c.paymentId().value()).append(':').append(c.net().paise());
        }
        return Sha256.of(text.toString().getBytes(StandardCharsets.UTF_8));
    }
}
