package io.github.vanish073.driftline.core.ledger;

import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.MerchantId;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * A double-entry ledger transaction (DESIGN §7). {@link #of} is layer 1 of "rejects any unbalanced transaction"
 * (§7.3): it refuses fewer than 2 entries, a zero entry, mixed merchants, overflow and a non-zero sum.
 *
 * <p>Entries are kept in a canonical order (account, then amount), so two transactions with the same entries are
 * equal whatever order they were built in. {@code postedAt} is kept at microsecond precision, as PostgreSQL stores
 * it. The id is derived from the merchant and the cause (D4), so a replay produces the same id.
 */
public final class LedgerTxn {

    private static final Comparator<Entry> CANONICAL =
            Comparator.comparing(Entry::account).thenComparing(Entry::amount);

    private final MerchantId merchantId;
    private final Cause cause;
    private final Instant postedAt;
    private final List<Entry> entries;

    private LedgerTxn(MerchantId merchantId, Cause cause, Instant postedAt, List<Entry> entries) {
        this.merchantId = Objects.requireNonNull(merchantId, "merchantId");
        this.cause = Objects.requireNonNull(cause, "cause");
        this.postedAt = Objects.requireNonNull(postedAt, "postedAt").truncatedTo(ChronoUnit.MICROS);
        this.entries = Objects.requireNonNull(entries, "entries").stream().sorted(CANONICAL).toList();
    }

    /** A validated transaction. @throws LedgerRejected if the entries break a double-entry rule */
    public static LedgerTxn of(MerchantId merchantId, Cause cause, Instant postedAt, List<Entry> entries) {
        LedgerChecks.requireValid(merchantId, entries);
        return new LedgerTxn(merchantId, cause, postedAt, entries);
    }

    /**
     * A transaction that skips layer 1, so tests can prove that the stores (layer 2) and PostgreSQL (layer 3) reject
     * it on their own. ArchUnit allows only the simulator and test code to call this (D28).
     */
    public static LedgerTxn unchecked(MerchantId merchantId, Cause cause, Instant postedAt, List<Entry> entries) {
        return new LedgerTxn(merchantId, cause, postedAt, entries);
    }

    public DerivedId id() {
        return DerivedId.of("ledger", merchantId.value(), cause.type().name(), cause.id());
    }

    public MerchantId merchantId() {
        return merchantId;
    }

    public Cause cause() {
        return cause;
    }

    public Instant postedAt() {
        return postedAt;
    }

    /** The entries in canonical order. */
    public List<Entry> entries() {
        return entries;
    }

    /**
     * True if {@code other} records the same economic event the same way: the same merchant, cause and entries.
     * {@code postedAt} is not compared; a replay posted later is still the same transaction (DESIGN §7.4).
     */
    public boolean sameEntriesAs(LedgerTxn other) {
        return merchantId.equals(other.merchantId) && cause.equals(other.cause) && entries.equals(other.entries);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof LedgerTxn other && sameEntriesAs(other) && postedAt.equals(other.postedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(merchantId, cause, postedAt, entries);
    }

    @Override
    public String toString() {
        return "LedgerTxn[" + merchantId + " " + cause + " at " + postedAt + " " + entries + "]";
    }
}
