package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import java.util.Arrays;
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
        IDEMPOTENCY_CONFLICT,
        /** The log record could not be decoded into an envelope at all (§5.7); its raw bytes are kept. */
        UNDECODABLE
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
            if (reason == Reason.UNDECODABLE) {
                throw new IllegalArgumentException("an undecodable record is an Undecodable, not an Entry");
            }
        }

        public String id() {
            return idOf(partition, offset);
        }
    }

    /**
     * A log record that did not decode (§5.7, D47). Nothing in it can be trusted, so it keeps only its position, a
     * short reason that never echoes the bytes, and the raw bytes themselves (at most {@link #MAX_RAW_BYTES}, which is
     * far above anything ingest writes).
     */
    record Undecodable(int partition, long offset, String error, byte[] raw) {

        public static final int MAX_RAW_BYTES = 1 << 20;
        public static final int MAX_ERROR_LENGTH = 300;

        public Undecodable {
            if (partition < 0 || offset < 0) {
                throw new IllegalArgumentException("partition and offset must not be negative");
            }
            Objects.requireNonNull(error, "error");
            Objects.requireNonNull(raw, "raw");
            if (error.isEmpty() || error.length() > MAX_ERROR_LENGTH) {
                throw new IllegalArgumentException("error must be 1 to " + MAX_ERROR_LENGTH + " characters");
            }
            if (raw.length > MAX_RAW_BYTES) {
                throw new IllegalArgumentException("raw is longer than " + MAX_RAW_BYTES + " bytes");
            }
            raw = raw.clone();
        }

        @Override
        public byte[] raw() {
            return raw.clone();
        }

        public String id() {
            return idOf(partition, offset);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Undecodable u && u.partition == partition && u.offset == offset
                    && u.error.equals(error) && Arrays.equals(u.raw, raw);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, offset, error, Arrays.hashCode(raw));
        }

        @Override
        public String toString() {
            return "Undecodable[" + partition + "@" + offset + ", " + error + ", " + raw.length + " bytes]";
        }
    }

    /** One quarantine row per log position, whatever its reason (D4: replays rewrite the same row). */
    static String idOf(int partition, long offset) {
        return DerivedId.of("quarantine", Integer.toString(partition), Long.toString(offset)).value();
    }

    /** Records {@code entry}; a second add for the same log position changes nothing. */
    void add(Entry entry);

    /** The merchant's quarantined records, by partition and offset. */
    List<Entry> forMerchant(MerchantId merchantId);

    /** Records {@code record}; a second add for the same log position changes nothing. */
    void addUndecodable(Undecodable record);

    /** The undecodable records of one partition, by offset. */
    List<Undecodable> undecodable(int partition);
}
