package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;

/**
 * Dedupe rows keyed by {@code (merchant, source, eventKey)}, written in the same transaction as the state change
 * (D3). Mirrors {@code processed_events} in DESIGN §12. The rows are append-only.
 */
public interface DedupeRepo {

    /** What {@link #claim} found. */
    enum Claim {
        /** The key is new; the row was written and the event should be applied. */
        FIRST,
        /** The key was seen with the same payload: a harmless replay. */
        DUPLICATE,
        /** The key was seen with a different payload: IDEMPOTENCY_CONFLICT (DESIGN §10.3). Nothing was written. */
        KEY_REUSED
    }

    Claim claim(MerchantId merchantId, Source source, String eventKey, Sha256 payloadSha256, int partition, long offset);
}
