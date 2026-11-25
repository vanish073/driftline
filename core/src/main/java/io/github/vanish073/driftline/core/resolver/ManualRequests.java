package io.github.vanish073.driftline.core.resolver;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.port.AuditRepo;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;

/**
 * Repeat-safe manual actions (Shift 11, D94). Every manual action carries the caller's {@code Idempotency-Key}; the
 * key and the SHA-256 of the request's canonical content go into the action's own audit row, which commits in the
 * same transaction as the action. A unique index on (action, subject, key) means a repeat finds that row instead of
 * acting again: with the same content it gets the first answer back, with other content it is refused. Keys are scoped
 * to the action and its subject (a merchant, a break), so two staff members reusing a key on different merchants do not
 * collide.
 */
public final class ManualRequests {

    /** What an earlier request with this key did. */
    public sealed interface Prior {
    }

    /** No earlier request used this key: act. */
    public record None() implements Prior {
    }

    /** The same request was made before: answer as then, from its audit row, and do nothing. */
    public record Same(AuditRepo.Row row) implements Prior {
    }

    /** The key was used for a different request: refuse. */
    public record Different() implements Prior {
    }

    private ManualRequests() {
    }

    /** Checks the key ({@link IdRules#requireExternalKey}) and looks for an earlier request with it. */
    public static Prior prior(AuditRepo audit, String action, String subject, String key, Sha256 request) {
        IdRules.requireExternalKey("Idempotency-Key", key);
        Objects.requireNonNull(request, "request");
        Optional<AuditRepo.Row> row = audit.byRequestKey(action, subject, key);
        if (row.isEmpty()) {
            return new None();
        }
        String recorded = row.get().entry().details().get(AuditRepo.REQUEST_SHA256);
        return request.hex().equals(recorded) ? new Same(row.get()) : new Different();
    }

    /** Records the key and the request hash in an audit row's details. */
    public static void stamp(SortedMap<String, String> details, String key, Sha256 request) {
        details.put(AuditRepo.REQUEST_KEY, IdRules.requireExternalKey("Idempotency-Key", key));
        details.put(AuditRepo.REQUEST_SHA256, request.hex());
    }

    /**
     * The SHA-256 of a request's fields in order, each length-prefixed so that no two different requests share a text.
     * Callers pass the parsed, canonical values (amounts as {@code Money.toString()}, maps in sorted order), so spacing
     * or field order in the body does not make one request look like two.
     */
    public static Sha256 hash(String... fields) {
        StringBuilder text = new StringBuilder();
        for (String f : fields) {
            String v = Objects.requireNonNull(f, "field");
            text.append(v.length()).append(':').append(v).append(';');
        }
        return Sha256.of(text.toString().getBytes(StandardCharsets.UTF_8));
    }
}
