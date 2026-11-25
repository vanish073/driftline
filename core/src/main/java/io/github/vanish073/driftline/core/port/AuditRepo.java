package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.IdRules;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The append-only, hash-chained audit trail (DESIGN §7.6, §12): every manual action (a manual resolution, a manual
 * ledger adjustment) writes a row. Each row's hash covers the previous row's hash and its own content, so editing or
 * removing a row breaks every hash after it. The chain is computed here, so both stores build the same one.
 */
public interface AuditRepo {

    /** The hash before the first row. */
    String GENESIS = "0".repeat(64);

    /**
     * What happened. {@code at} is kept to the microsecond, as PostgreSQL stores it, so a stored row hashes the same
     * when read back.
     */
    record Entry(Instant at, String actor, String action, String subject, SortedMap<String, String> details) {
        public Entry {
            at = Objects.requireNonNull(at, "at").truncatedTo(ChronoUnit.MICROS);
            IdRules.requireKey("actor", actor);
            IdRules.requireToken("action", action);
            IdRules.requireKey("subject", subject);
            TreeMap<String, String> copy = new TreeMap<>();
            Objects.requireNonNull(details, "details").forEach((k, v) -> {
                IdRules.requireToken("detail key", k);
                copy.put(k, Objects.requireNonNull(v, "detail value"));
            });
            details = Collections.unmodifiableSortedMap(copy);
        }

        public static Entry of(Instant at, String actor, String action, String subject, Map<String, String> details) {
            return new Entry(at, actor, action, subject, new TreeMap<>(details));
        }
    }

    /** A stored row. */
    record Row(long seq, Entry entry, String prevHash, String hash) {
        public Row {
            if (seq < 1) {
                throw new IllegalArgumentException("audit sequence numbers start at 1");
            }
            Objects.requireNonNull(entry, "entry");
            Objects.requireNonNull(prevHash, "prevHash");
            Objects.requireNonNull(hash, "hash");
        }
    }

    /**
     * The hash of a row: SHA-256 over the previous hash and a canonical text of the entry, each field length-prefixed so
     * no two different entries share a text.
     */
    static String hash(String prevHash, Entry e) {
        StringBuilder text = new StringBuilder();
        field(text, prevHash);
        field(text, e.at().toString());
        field(text, e.actor());
        field(text, e.action());
        field(text, e.subject());
        e.details().forEach((k, v) -> {
            field(text, k);
            field(text, v);
        });
        return Sha256.of(text.toString().getBytes(StandardCharsets.UTF_8)).hex();
    }

    private static void field(StringBuilder text, String value) {
        text.append(value.length()).append(':').append(value).append(';');
    }

    /** The detail key under which a manual request's {@code Idempotency-Key} is recorded (Shift 11, D94). */
    String REQUEST_KEY = "request_key";

    /** The detail key under which the SHA-256 of a manual request's canonical content is recorded (D94). */
    String REQUEST_SHA256 = "request_sha256";

    /**
     * Appends a row after the last one, chained to its hash ({@link #GENESIS} for the first).
     *
     * @throws StoreConflict if the entry carries a {@link #REQUEST_KEY} that a row with the same action and subject
     *                       already carries (D94): two concurrent repeats of one request cannot both commit
     */
    Row append(Entry entry);

    /** The row of {@code action} on {@code subject} that carries {@code requestKey} as its {@link #REQUEST_KEY}. */
    Optional<Row> byRequestKey(String action, String subject, String requestKey);

    /** Every row, by sequence number. */
    List<Row> rows();
}
