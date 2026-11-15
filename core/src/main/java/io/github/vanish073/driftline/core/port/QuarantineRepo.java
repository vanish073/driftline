package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import java.util.List;
import java.util.Objects;

/**
 * Log records the matcher refused to apply, written in the same transaction as the batch that met them (DESIGN §5.7,
 * §10.3). Mirrors {@code quarantine} (§12). Append-only; one row per log record.
 */
public interface QuarantineRepo {

    /** Why a record was quarantined. */
    enum Reason {
        /** The event key was already applied with a different payload hash (§10.3). */
        IDEMPOTENCY_CONFLICT
    }

    /**
     * One quarantined log record. Its id is derived from its log position (D4), so a replayed batch writes the same
     * row again rather than a second one.
     */
    record Entry(int partition, long offset, MerchantId merchantId, Source source, String eventKey,
                 Sha256 payloadSha256, Reason reason) {

        public Entry {
            if (partition < 0 || offset < 0) {
                throw new IllegalArgumentException("partition and offset must not be negative");
            }
            Objects.requireNonNull(merchantId, "merchantId");
            Objects.requireNonNull(source, "source");
            IdRules.requireKey("eventKey", eventKey);
            Objects.requireNonNull(payloadSha256, "payloadSha256");
            Objects.requireNonNull(reason, "reason");
        }

        public String id() {
            return DerivedId.of("quarantine", Integer.toString(partition), Long.toString(offset)).value();
        }
    }

    /** Records {@code entry}; a second add for the same log position changes nothing. */
    void add(Entry entry);

    /** The merchant's quarantined records, by partition and offset. */
    List<Entry> forMerchant(MerchantId merchantId);
}
