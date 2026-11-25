package io.github.vanish073.driftline.core.resolver;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.Entry;
import io.github.vanish073.driftline.core.ledger.LedgerChecks;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.ledger.PostingRules;
import io.github.vanish073.driftline.core.money.AmountLimits;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.port.Tx;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * {@code POST /v1/merchants/{m}/ledger/adjustments} (DESIGN §7, D8, D74), sans-I/O: a person posts a balanced
 * correcting entry to a merchant's books. The matcher is the only automatic ledger writer; this is the one manual
 * way in, and it is audited. In one transaction it appends the audit row, then posts the ledger transaction whose cause
 * is that row's sequence number ({@code MANUAL_ADJUSTMENT:<seq>}), so the two exist together or not at all, and every
 * manual entry in the books names the audit row that explains it. Everything is checked before anything is written:
 * the entries must balance (§7.3 layer 1), a note is required, and a named break must be the merchant's.
 *
 * <p>Repeat-safe (Shift 11, D94): the caller's {@code Idempotency-Key} and the request's hash go into the audit row
 * ({@link ManualRequests}). A repeat of the same request posts nothing and gets the first posting back; the same key
 * with other content is refused. Before D94 a retried request (say, after a timeout) posted the correction twice.
 */
public final class ManualAdjustment {

    public static final String ACTION = "LEDGER_ADJUST_MANUAL";

    /** What happened. */
    public sealed interface Result {
    }

    /** The named break does not exist, or belongs to another merchant. */
    public record BreakNotFound() implements Result {
    }

    /** Posted now, or, if {@code replayed}, by an earlier request with the same key and content. */
    public record Posted(long auditSeq, LedgerTxn txn, boolean replayed) implements Result {
    }

    /** The {@code Idempotency-Key} was used before, on this merchant, for a different adjustment. */
    public record KeyReused() implements Result {
    }

    private ManualAdjustment() {
    }

    /**
     * @param amounts signed amounts per account, debit positive (§7.2); at least two, summing to zero
     * @param breakId the break this corrects, if any
     * @param requestKey the caller's {@code Idempotency-Key} (D94)
     * @throws IllegalArgumentException if the note is invalid ({@link ManualResolution#requireNote}), the key is
     *                                  malformed, an amount is above {@code AmountLimits}, or the entries break a
     *                                  double-entry rule ({@code LedgerRejected})
     */
    public static Result post(Tx tx, MerchantId m, String actor, String note, Optional<String> breakId,
                              SortedMap<AccountCode, Money> amounts, String requestKey, Instant now) {
        ManualResolution.requireNote(note);
        Objects.requireNonNull(amounts, "amounts");
        List<Entry> entries = new ArrayList<>();
        amounts.forEach((account, amount) -> entries.add(new Entry(m, account,
                AmountLimits.requireSettlementField("entry " + account.name(), amount))));
        LedgerChecks.requireValid(m, entries);
        String canonicalEntries = amounts.entrySet().stream().map(e -> e.getKey().name() + "=" + e.getValue())
                .collect(Collectors.joining(";"));
        io.github.vanish073.driftline.core.event.Sha256 request = ManualRequests.hash(ACTION, m.value(), note,
                breakId.orElse(""), canonicalEntries);
        switch (ManualRequests.prior(tx.audit(), ACTION, m.value(), requestKey, request)) {
            case ManualRequests.Different d -> {
                return new KeyReused();
            }
            case ManualRequests.Same same -> {
                LedgerTxn first = tx.ledger().find(m, PostingRules.manualAdjustment(m, same.row().seq(), entries,
                        same.row().entry().at()).cause()).orElseThrow(() -> new IllegalStateException(
                                "audit row " + same.row().seq() + " has no ledger transaction"));
                return new Posted(same.row().seq(), first, true);
            }
            case ManualRequests.None none -> {
                // act below
            }
        }
        if (breakId.isPresent()) {
            Optional<Break> b = tx.breaks().byId(breakId.get()).filter(x -> x.merchantId().equals(m));
            if (b.isEmpty()) {
                return new BreakNotFound();
            }
        }
        TreeMap<String, String> details = new TreeMap<>();
        details.put("merchant_id", m.value());
        details.put("entries", canonicalEntries);
        details.put("note", note);
        breakId.ifPresent(b -> details.put("break_id", b));
        ManualRequests.stamp(details, requestKey, request);
        AuditRepo.Row row = tx.audit().append(AuditRepo.Entry.of(now, actor, ACTION, m.value(), details));
        LedgerTxn txn = PostingRules.manualAdjustment(m, row.seq(), entries, now);
        if (!(tx.ledger().post(txn) instanceof PostResult.Posted)) {
            // the audit sequence number is new, so its cause cannot exist; if it does, write nothing at all
            throw new IllegalStateException("ledger cause " + txn.cause() + " already exists");
        }
        return new Posted(row.seq(), txn, false);
    }
}
