package io.github.vanish073.driftline.core.ledger;

import io.github.vanish073.driftline.core.id.DerivedId;
import java.util.Objects;

/** What an idempotent {@code post} did (DESIGN §7.4). */
public sealed interface PostResult {

    /** The cause was new; the transaction was recorded. */
    record Posted(DerivedId txnId) implements PostResult {
        public Posted {
            Objects.requireNonNull(txnId, "txnId");
        }
    }

    /** The cause was already recorded with the same entries: a harmless replay, counted as a duplicate. */
    record Duplicate(DerivedId txnId) implements PostResult {
        public Duplicate {
            Objects.requireNonNull(txnId, "txnId");
        }
    }

    /**
     * The cause was already recorded with <em>different</em> entries. Nothing was written. This is a bug: the matcher
     * raises a LEDGER_CONFLICT anomaly and the simulator fails the seed.
     */
    record LedgerConflict(LedgerTxn existing, LedgerTxn attempted) implements PostResult {
        public LedgerConflict {
            Objects.requireNonNull(existing, "existing");
            Objects.requireNonNull(attempted, "attempted");
        }
    }
}
