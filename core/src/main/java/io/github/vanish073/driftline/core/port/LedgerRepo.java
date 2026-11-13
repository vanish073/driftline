package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.Cause;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.money.Money;
import java.util.Optional;

/**
 * The double-entry ledger: append-only, every transaction sums to zero (DESIGN §7). Mirrors
 * {@code ledger_accounts}, {@code ledger_transactions}, {@code ledger_entries} and the balance tables in §12.
 *
 * <p>Every method exists in {@code InMemoryStore} and {@code PostgresStore}, with the contract suite in
 * {@code core} test fixtures (DESIGN §3.2, CLAUDE.md "Store parity").
 */
public interface LedgerRepo {

    /**
     * Records {@code txn} once per {@code (merchant, cause)} (§7.4). The store validates the entries again (layer 2
     * of §7.3) before writing.
     *
     * @return {@code Posted} for a new cause, {@code Duplicate} if the same entries were already recorded, and
     *     {@code LedgerConflict} if different entries were; nothing is written in the last two cases
     * @throws io.github.vanish073.driftline.core.ledger.LedgerRejected if the entries break a double-entry rule
     */
    PostResult post(LedgerTxn txn);

    Optional<LedgerTxn> find(MerchantId merchantId, Cause cause);

    /** The sum of every entry on the account; zero for an account with no entries. */
    Money balance(MerchantId merchantId, AccountCode account);
}
