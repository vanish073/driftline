package io.github.vanish073.driftline.app.ops;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.ingest.BoundedBody;
import io.github.vanish073.driftline.app.merchant.DevBootstrap;
import io.github.vanish073.driftline.app.security.MerchantAccess;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.core.resolver.ManualRequests;
import io.github.vanish073.driftline.core.resolver.ManualResolution;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.StrictJson;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * The token API (Shift 11, D95), scope {@code tokens:write}, ops tokens only:
 * <ul>
 *   <li>{@code POST /v1/tokens} with {@code {"merchant_id":"m_1","scopes":"events:write,breaks:read",
 *       "ttl_seconds":2592000,"note":"…"}} creates a token and shows it once (201). The caller can grant only scopes it
 *       holds itself, so no token can make a stronger one; a merchant token gets merchant scopes only; every token
 *       expires (at most {@link #MAX_OPS_LIFETIME} without a merchant, {@link #MAX_MERCHANT_LIFETIME} with one).</li>
 *   <li>{@code GET /v1/tokens} lists every token: its id, merchant, scopes and dates, never the token.</li>
 *   <li>{@code POST /v1/tokens/{tokenId}/revoke} with {@code {"note":"…"}} revokes one at once. A caller may revoke
 *       only a token no stronger than itself.</li>
 * </ul>
 * Each POST needs an {@code Idempotency-Key} (D94) and writes an audit row naming the caller; the token itself is never
 * stored, logged or audited, only its SHA-256. A repeated create cannot show the token again, since it was never
 * kept: it answers 409 with the id of the token the first request made.
 */
@RestController
@Profile(Roles.INGEST)
public class TokenController {

    static final Duration MIN_LIFETIME = Duration.ofMinutes(5);
    static final Duration MAX_OPS_LIFETIME = Duration.ofDays(90);
    static final Duration MAX_MERCHANT_LIFETIME = Duration.ofDays(365);
    /** The scopes a merchant-bound token may hold; the rest are for Driftline's own staff and services. */
    static final Set<String> MERCHANT_SCOPES = Set.of("events:write", "settlement:write", "breaks:read",
            "breaks:resolve");
    static final int MAX_BODY = 4_096;
    private static final HexFormat HEX = HexFormat.of();

    private final PostgresDirectory directory;
    private final Clock clock;
    private final SecureRandom random = new SecureRandom();

    public TokenController(PostgresDirectory directory, Clock clock) {
        this.directory = directory;
        this.clock = clock;
    }

    private static List<String> scopesOf(Authentication auth) {
        List<String> out = new ArrayList<>();
        for (GrantedAuthority a : auth.getAuthorities()) {
            String name = a.getAuthority();
            if (name != null && name.startsWith("SCOPE_")) {
                out.add(name.substring("SCOPE_".length()));
            }
        }
        return out;
    }

    @PostMapping("/v1/tokens")
    @PreAuthorize("hasAuthority('SCOPE_tokens:write')")
    public ResponseEntity<?> create(Authentication auth, HttpServletRequest request,
                                    @RequestHeader(name = OpsController.IDEMPOTENCY_KEY, required = false)
                                    Optional<String> key) throws IOException {
        if (MerchantAccess.boundMerchant(auth).isPresent()) {
            return OpsController.error(403, "only an ops token may manage tokens");
        }
        Optional<ResponseEntity<?>> badKey = OpsController.requireKey(key);
        if (badKey.isPresent()) {
            return badKey.get();
        }
        Optional<byte[]> body = BoundedBody.read(request, MAX_BODY);
        if (body.isEmpty()) {
            return OpsController.error(413, "body larger than " + MAX_BODY + " bytes");
        }
        Optional<MerchantId> merchant;
        TreeSet<String> scopes = new TreeSet<>();
        Duration ttl;
        String note;
        try {
            JsonObject root = StrictJson.parse(body.get());
            for (String field : root.fields().keySet()) {
                if (!Set.of("merchant_id", "scopes", "ttl_seconds", "note").contains(field)) {
                    // "role":"admin" and the like: a field the API does not know is refused, never ignored
                    return OpsController.error(400, "unknown field: only merchant_id, scopes, ttl_seconds and note");
                }
            }
            merchant = Optional.ofNullable(root.optionalString("merchant_id")).map(MerchantId::of);
            for (String s : root.string("scopes").split(",", -1)) {
                if (!PostgresDirectory.SCOPES.contains(s) || !scopes.add(s)) {
                    return OpsController.error(400, "scopes is a comma-separated list of known scopes, each once");
                }
            }
            ttl = Duration.ofSeconds(root.integer("ttl_seconds"));
            note = ManualResolution.requireNote(root.string("note"));
        } catch (IllegalArgumentException | ArithmeticException e) {   // MalformedJson, a bad merchant id
            return OpsController.error(400, e.getMessage() == null ? "malformed token request" : e.getMessage());
        }
        Duration max = merchant.isPresent() ? MAX_MERCHANT_LIFETIME : MAX_OPS_LIFETIME;
        if (ttl.compareTo(MIN_LIFETIME) < 0 || ttl.compareTo(max) > 0) {
            return OpsController.error(400, "ttl_seconds must be between " + MIN_LIFETIME.toSeconds() + " and "
                    + max.toSeconds() + (merchant.isPresent() ? " for a merchant token" : " for an ops token"));
        }
        List<String> held = scopesOf(auth);
        if (!held.containsAll(scopes)) {
            return OpsController.error(403, "a token can grant only scopes it holds itself");
        }
        if (merchant.isPresent() && !MERCHANT_SCOPES.containsAll(scopes)) {
            return OpsController.error(400, "a merchant token may hold only " + new TreeSet<>(MERCHANT_SCOPES));
        }

        Instant now = clock.now().truncatedTo(ChronoUnit.MICROS);
        byte[] raw = new byte[32];
        random.nextBytes(raw);
        String token = (merchant.isPresent() ? "dl_mer_" : "dl_ops_") + HEX.formatHex(raw);
        Sha256 hash = DevBootstrap.tokenHash(token);
        PostgresDirectory.ApiToken stored = new PostgresDirectory.ApiToken(hash, merchant, List.copyOf(scopes), now,
                now.plus(ttl), Optional.empty());
        Sha256 asked = ManualRequests.hash(PostgresDirectory.TOKEN_CREATE_ACTION,
                merchant.map(MerchantId::value).orElse(""), String.join(",", scopes), Long.toString(ttl.toSeconds()),
                note);
        PostgresDirectory.TokenCreation result;
        try {
            result = directory.createToken(stored, String.valueOf(auth.getPrincipal()), note, key.get(), asked);
        } catch (StoreUnavailable e) {
            return OpsController.error(503, "the store is unavailable; retry");
        } catch (StoreConflict e) {
            return OpsController.error(409, OpsController.KEY_IN_FLIGHT);
        }
        return switch (result) {
            case PostgresDirectory.TokenUnknownMerchant u -> OpsController.error(404, "unknown merchant");
            case PostgresDirectory.TokenKeyReused r -> OpsController.error(409, OpsController.KEY_REUSED);
            case PostgresDirectory.TokenAlreadyCreated a -> {
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("error", "a token was already created with this Idempotency-Key; it is shown only once, so "
                        + "revoke it and create another if it was lost");
                v.put("token_id", a.tokenId());
                yield ResponseEntity.status(409).body(v);
            }
            case PostgresDirectory.TokenCreated c -> {
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("token_id", c.tokenId());
                v.put("token", token);
                v.put("merchant_id", merchant.map(MerchantId::value).orElse(null));
                v.put("scopes", String.join(",", scopes));
                v.put("expires_at", stored.expiresAt().toString());
                yield ResponseEntity.status(201).body(v);
            }
        };
    }

    @GetMapping("/v1/tokens")
    @PreAuthorize("hasAuthority('SCOPE_tokens:write')")
    public ResponseEntity<?> list(Authentication auth) {
        if (MerchantAccess.boundMerchant(auth).isPresent()) {
            return OpsController.error(403, "only an ops token may manage tokens");
        }
        Instant now = clock.now();
        List<Map<String, Object>> out = new ArrayList<>();
        try {
            for (PostgresDirectory.TokenInfo t : directory.tokens()) {
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("token_id", t.tokenId());
                v.put("merchant_id", t.merchantId().map(MerchantId::value).orElse(null));
                v.put("scopes", String.join(",", t.scopes()));
                v.put("created_at", t.createdAt().toString());
                v.put("expires_at", t.expiresAt().toString());
                v.put("revoked_at", t.revokedAt().map(Instant::toString).orElse(null));
                v.put("created_by", t.createdBy());
                v.put("active", now.isBefore(t.expiresAt()) && t.revokedAt().map(now::isBefore).orElse(true));
                out.add(v);
            }
        } catch (StoreUnavailable e) {
            return OpsController.error(503, "the store is unavailable; retry");
        }
        return ResponseEntity.ok(Map.of("tokens", out));
    }

    @PostMapping("/v1/tokens/{tokenId}/revoke")
    @PreAuthorize("hasAuthority('SCOPE_tokens:write')")
    public ResponseEntity<?> revoke(Authentication auth, @PathVariable String tokenId, HttpServletRequest request,
                                    @RequestHeader(name = OpsController.IDEMPOTENCY_KEY, required = false)
                                    Optional<String> key) throws IOException {
        if (MerchantAccess.boundMerchant(auth).isPresent()) {
            return OpsController.error(403, "only an ops token may manage tokens");
        }
        Optional<ResponseEntity<?>> badKey = OpsController.requireKey(key);
        if (badKey.isPresent()) {
            return badKey.get();
        }
        if (!tokenId.matches("tok_[0-9a-f]{16}")) {
            return OpsController.error(404, "no such token");
        }
        Optional<byte[]> body = BoundedBody.read(request, MAX_BODY);
        if (body.isEmpty()) {
            return OpsController.error(413, "body larger than " + MAX_BODY + " bytes");
        }
        String note;
        try {
            note = ManualResolution.requireNote(StrictJson.parse(body.get()).string("note"));
        } catch (IllegalArgumentException e) {
            return OpsController.error(400, e.getMessage());
        }
        PostgresDirectory.RevocationResult result;
        try {
            result = directory.revokeToken(tokenId, scopesOf(auth), clock.now(), String.valueOf(auth.getPrincipal()),
                    note, key.get(), ManualRequests.hash(PostgresDirectory.TOKEN_REVOKE_ACTION, tokenId, note));
        } catch (StoreUnavailable e) {
            return OpsController.error(503, "the store is unavailable; retry");
        } catch (StoreConflict e) {
            return OpsController.error(409, OpsController.KEY_IN_FLIGHT);
        }
        return switch (result.status()) {
            case NOT_FOUND -> OpsController.error(404, "no such token");
            case FORBIDDEN -> OpsController.error(403, "a token may revoke only tokens no stronger than itself");
            case ALREADY_REVOKED -> OpsController.error(409, "the token is already revoked");
            case KEY_REUSED -> OpsController.error(409, OpsController.KEY_REUSED);
            case REVOKED, REPLAYED -> {
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("token_id", tokenId);
                v.put("revoked_at", result.revokedAt().orElseThrow().toString());
                yield OpsController.replayed(ResponseEntity.ok(),
                        result.status() == PostgresDirectory.Revocation.REPLAYED).body(v);
            }
        };
    }
}
