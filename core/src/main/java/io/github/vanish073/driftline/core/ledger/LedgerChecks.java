package io.github.vanish073.driftline.core.ledger;

import io.github.vanish073.driftline.core.id.MerchantId;
import java.util.List;
import java.util.Objects;

/**
 * The double-entry rules, shared by layer 1 ({@link LedgerTxn#of}) and layer 2 (both stores) of DESIGN §7.3.
 *
 * <p>Debits and credits are summed separately, each with exact arithmetic, and must be equal. Summing them
 * separately makes the overflow check independent of entry order: {@code [+MAX, +1, −MAX, −1]} is rejected
 * whatever order the entries come in, where a running signed total would overflow in one order and not another.
 */
public final class LedgerChecks {

    private LedgerChecks() {
    }

    /** @throws LedgerRejected if the entries do not form a valid transaction for {@code merchantId} */
    public static void requireValid(MerchantId merchantId, List<Entry> entries) {
        Objects.requireNonNull(merchantId, "merchantId");
        Objects.requireNonNull(entries, "entries");
        if (entries.size() < 2) {
            throw new LedgerRejected(LedgerRejected.Reason.TOO_FEW_ENTRIES,
                    "a transaction needs at least 2 entries, got " + entries.size());
        }
        long debits = 0;
        long credits = 0;
        for (Entry entry : entries) {
            Objects.requireNonNull(entry, "entry");
            if (!entry.merchantId().equals(merchantId)) {
                throw new LedgerRejected(LedgerRejected.Reason.MIXED_MERCHANTS,
                        "an entry for merchant " + entry.merchantId() + " in a transaction of merchant " + merchantId);
            }
            long paise = entry.amount().paise();
            if (paise == 0) {
                throw new LedgerRejected(LedgerRejected.Reason.ZERO_ENTRY, "an entry of zero on " + entry.account());
            }
            try {
                if (paise > 0) {
                    debits = Math.addExact(debits, paise);
                } else {
                    credits = Math.addExact(credits, -paise);
                }
            } catch (ArithmeticException overflow) {
                throw new LedgerRejected(LedgerRejected.Reason.OVERFLOW, "the debits or the credits exceed the long range");
            }
        }
        if (debits != credits) {
            throw new LedgerRejected(LedgerRejected.Reason.UNBALANCED,
                    "debits " + debits + " and credits " + credits + " paise differ by " + (debits - credits));
        }
    }
}
