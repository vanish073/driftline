package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.resolver.ManualRequests;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.sql.DataSource;

/**
 * Merchants, their webhook secrets and API tokens (DESIGN §10, §12, §14; migration V5). This is production
 * configuration, not matcher state: the simulator builds its merchant directory from the run's configuration and has
 * no tokens, so these are {@code PostgresStore}-side methods and not {@code Store} methods, as with the snapshotter
 * (D29, D48). Each call runs in its own short transaction.
 *
 * <p>Onboarding is idempotent: inserting a merchant, secret or token that exists with the same values changes nothing,
 * and one that exists with different values is refused, because a merchant's partition and terms never change (D5).
 */
public final class PostgresDirectory {

    /** The scopes a token may carry (§10.1). */
    public static final List<String> SCOPES = List.of("events:write", "settlement:write", "observations:write",
            "breaks:read", "breaks:resolve", "ledger:adjust", "secrets:write", "tokens:write", "pii:erase");

    /** One webhook secret as stored: {@code secretEnc} is ciphertext; the caller encrypts and decrypts (D48). */
    public record WebhookSecret(MerchantId merchantId, String kid, byte[] secretEnc, Instant activeFrom,
                                Optional<Instant> activeTo) {
        public WebhookSecret {
            Objects.requireNonNull(merchantId, "merchantId");
            IdRules.requireToken("kid", kid);
            secretEnc = secretEnc.clone();
            Objects.requireNonNull(activeFrom, "activeFrom");
            Objects.requireNonNull(activeTo, "activeTo");
        }

        @Override
        public byte[] secretEnc() {
            return secretEnc.clone();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof WebhookSecret w && w.merchantId.equals(merchantId) && w.kid.equals(kid)
                    && Arrays.equals(w.secretEnc, secretEnc) && w.activeFrom.equals(activeFrom)
                    && w.activeTo.equals(activeTo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(merchantId, kid, Arrays.hashCode(secretEnc), activeFrom, activeTo);
        }

        @Override
        public String toString() {
            return "WebhookSecret[" + merchantId + ", " + kid + "]";
        }
    }

    /**
     * One API token as stored: only its SHA-256. An empty merchant is an ops token. Every token expires (D95); one
     * authenticates only before both its expiry and its revocation.
     */
    public record ApiToken(Sha256 tokenSha256, Optional<MerchantId> merchantId, List<String> scopes, Instant createdAt,
                           Instant expiresAt, Optional<Instant> revokedAt) {
        public ApiToken {
            Objects.requireNonNull(tokenSha256, "tokenSha256");
            Objects.requireNonNull(merchantId, "merchantId");
            scopes = List.copyOf(scopes);
            if (scopes.isEmpty() || !SCOPES.containsAll(scopes) || scopes.stream().distinct().count() != scopes.size()) {
                throw new IllegalArgumentException("scopes must be a non-empty subset of " + SCOPES + ", each once");
            }
            Objects.requireNonNull(createdAt, "createdAt");
            Objects.requireNonNull(expiresAt, "expiresAt");
            if (!expiresAt.isAfter(createdAt)) {
                throw new IllegalArgumentException("a token must expire after it is created");
            }
            Objects.requireNonNull(revokedAt, "revokedAt");
        }

        /** The token's public name (D95): {@code tok_} and the first 16 hex digits of its SHA-256. */
        public String tokenId() {
            return tokenId(tokenSha256);
        }

        public static String tokenId(Sha256 tokenSha256) {
            return "tok_" + tokenSha256.hex().substring(0, 16);
        }

        public boolean activeAt(Instant now) {
            return now.isBefore(expiresAt) && (revokedAt.isEmpty() || now.isBefore(revokedAt.get()));
        }
    }

    /** A token as the token API lists it (D95): never the token or its full hash. */
    public record TokenInfo(String tokenId, Optional<MerchantId> merchantId, List<String> scopes, Instant createdAt,
                            Instant expiresAt, Optional<Instant> revokedAt, String createdBy) {
        public TokenInfo {
            scopes = List.copyOf(scopes);
        }
    }

    /** The audit action of creating a token through the API (D95); its subject is {@link #TOKENS_SUBJECT}. */
    public static final String TOKEN_CREATE_ACTION = "TOKEN_CREATE";
    /** The audit action of revoking a token through the API (D95); its subject is the token id. */
    public static final String TOKEN_REVOKE_ACTION = "TOKEN_REVOKE";
    /** The audit subject of every token creation, so a request key is unique across them (D94). */
    public static final String TOKENS_SUBJECT = "api_tokens";

    private final DataSource dataSource;

    private static OffsetDateTime ts(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Optional<Instant> optionalInstant(ResultSet rs, int column) throws SQLException {
        return Optional.ofNullable(rs.getObject(column, OffsetDateTime.class)).map(OffsetDateTime::toInstant);
    }

    public PostgresDirectory(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @FunctionalInterface
    private interface Work<R> {
        R run(Connection c) throws SQLException;
    }

    private <R> R inTransaction(String what, Work<R> work) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            try {
                R result = work.run(c);
                c.commit();
                return result;
            } catch (SQLException | RuntimeException failure) {
                try {
                    c.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } catch (SQLException e) {
            throw SqlErrors.translate(what, e);
        }
    }

    // ---- merchants ------------------------------------------------------------------------------------------------

    /**
     * Onboards a merchant with calendar {@code DAILY} (the only v1 calendar).
     *
     * @return true if it was inserted now, false if it already existed with exactly these terms
     * @throws IllegalStateException if it exists with different terms
     */
    public boolean onboard(MerchantTerms terms, Instant at) {
        return inTransaction("onboard " + terms.merchantId(), c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO merchants (merchant_id, partition, fee_bps, gst_bps, cycle_days, calendar_id,
                                           first_settlement_date, onboarded_at)
                    VALUES (?, ?, ?, ?, ?, 'DAILY', ?, ?)
                    ON CONFLICT (merchant_id) DO NOTHING
                    """)) {
                ps.setString(1, terms.merchantId().value());
                ps.setInt(2, terms.partition());
                ps.setLong(3, terms.fees().feeBps());
                ps.setLong(4, terms.fees().gstBps());
                ps.setInt(5, terms.cycleDays());
                ps.setObject(6, terms.firstSettlementDate());
                ps.setObject(7, ts(at));
                if (ps.executeUpdate() == 1) {
                    return true;
                }
            }
            MerchantTerms stored = merchant(c, terms.merchantId()).orElseThrow();
            if (!stored.equals(terms)) {
                throw new IllegalStateException("merchant " + terms.merchantId()
                        + " is already onboarded with different terms; partitions and terms never change (D5)");
            }
            return false;
        });
    }

    /** Every merchant, by id: the input of the matcher's {@code MerchantDirectory}. */
    public List<MerchantTerms> merchants() {
        return inTransaction("merchants", c -> {
            try (PreparedStatement ps = c.prepareStatement(MERCHANT_SELECT + " ORDER BY merchant_id COLLATE \"C\"");
                 ResultSet rs = ps.executeQuery()) {
                List<MerchantTerms> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(readMerchant(rs));
                }
                return List.copyOf(out);
            }
        });
    }

    private static final String MERCHANT_SELECT = """
            SELECT merchant_id, partition, fee_bps, gst_bps, cycle_days, first_settlement_date FROM merchants
            """;

    private static Optional<MerchantTerms> merchant(Connection c, MerchantId id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(MERCHANT_SELECT + " WHERE merchant_id = ?")) {
            ps.setString(1, id.value());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(readMerchant(rs)) : Optional.empty();
            }
        }
    }

    private static MerchantTerms readMerchant(ResultSet rs) throws SQLException {
        return new MerchantTerms(MerchantId.of(rs.getString(1)), rs.getInt(2), new FeeSchedule(rs.getLong(3),
                rs.getLong(4)), rs.getInt(5), rs.getObject(6, LocalDate.class));
    }

    // ---- webhook secrets ------------------------------------------------------------------------------------------

    /**
     * @return true if inserted now, false if the merchant already has a secret with this key id. The stored value is
     *         not compared: ciphertext carries a fresh nonce on every encryption, so equal secrets differ here.
     */
    public boolean addWebhookSecret(WebhookSecret secret) {
        return inTransaction("webhook secret " + secret.merchantId() + "/" + secret.kid(), c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO merchant_webhook_secrets (merchant_id, kid, secret_enc, active_from, active_to)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (merchant_id, kid) DO NOTHING
                    """)) {
                ps.setString(1, secret.merchantId().value());
                ps.setString(2, secret.kid());
                ps.setBytes(3, secret.secretEnc());
                ps.setObject(4, ts(secret.activeFrom()));
                ps.setObject(5, secret.activeTo().map(PostgresDirectory::ts).orElse(null));
                return ps.executeUpdate() == 1;
            }
        });
    }

    /** The audit action of a webhook secret rotation (D76). */
    public static final String ROTATE_ACTION = "WEBHOOK_SECRET_ROTATE";

    /** What a rotation did (D76). */
    public enum Rotation {
        /** The new secret is active now; the ones it replaces stay valid until the overlap ends. */
        ROTATED,
        /** Two secrets are already active: a rotation is in progress, and a third would exceed §10.2's limit. */
        IN_PROGRESS,
        /** The merchant already has (or had) a secret with this key id. */
        KID_TAKEN,
        UNKNOWN_MERCHANT,
        /** The same request was made before with this {@code Idempotency-Key}; nothing changed now (D94). */
        REPLAYED,
        /** The {@code Idempotency-Key} was used before on this merchant for a different rotation (D94). */
        KEY_REUSED
    }

    /** A rotation's outcome; for ROTATED and REPLAYED, when the new secret became active and the old ones retire. */
    public record RotationResult(Rotation status, Optional<Instant> activeFrom, Optional<Instant> retireAt) {
        public RotationResult {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(activeFrom, "activeFrom");
            Objects.requireNonNull(retireAt, "retireAt");
        }

        static RotationResult of(Rotation status) {
            return new RotationResult(status, Optional.empty(), Optional.empty());
        }
    }

    /**
     * Rotates a merchant's webhook secret (§10.2, §14, D76) in one transaction: under a per-merchant lock, every secret
     * active at {@code now} is given {@code retireAt} as the end of its window (two are accepted until then), the new
     * secret becomes active at {@code now}, and an audit row ({@code WEBHOOK_SECRET_ROTATE}; never the secret) is
     * appended with the store's own hash-chained code, so the change and its record commit together. The new ciphertext
     * is the caller's (D48), already encrypted.
     *
     * @param retireAt after {@code now}: the overlap in which the gateway may still sign with the old secret
     * @param requestKey the caller's {@code Idempotency-Key}, and {@code request} the hash of what was asked (D94): a
     *                   repeat changes nothing and gets the first times back; the key with another request is refused
     */
    public RotationResult rotateWebhookSecret(MerchantId merchantId, String kid, byte[] secretEnc, Instant now,
                                              Instant retireAt, String actor, String requestKey, Sha256 request) {
        IdRules.requireToken("kid", kid);
        if (!retireAt.isAfter(now)) {
            throw new IllegalArgumentException("the old secret must stay valid for a while after the rotation");
        }
        return inTransaction("rotate webhook secret " + merchantId, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT pg_advisory_xact_lock(hashtext(?))")) {
                ps.setString(1, "driftline_webhook_secrets/" + merchantId.value());
                ps.execute();
            }
            try (PreparedStatement ps = c.prepareStatement("SELECT 1 FROM merchants WHERE merchant_id = ?")) {
                ps.setString(1, merchantId.value());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return RotationResult.of(Rotation.UNKNOWN_MERCHANT);
                    }
                }
            }
            JdbcResolverRepos.Audit audit = new JdbcResolverRepos.Audit(new JdbcTx(c, BalanceStrategy.APPEND_SNAPSHOT));
            switch (ManualRequests.prior(audit, ROTATE_ACTION, merchantId.value(), requestKey, request)) {
                case ManualRequests.Different d -> {
                    return RotationResult.of(Rotation.KEY_REUSED);
                }
                case ManualRequests.Same same -> {
                    return new RotationResult(Rotation.REPLAYED, Optional.of(same.row().entry().at()),
                            Optional.of(Instant.parse(same.row().entry().details().get("retire_at"))));
                }
                case ManualRequests.None none -> {
                    // rotate below
                }
            }
            OffsetDateTime at = ts(now);
            List<WebhookSecret> active = secrets(c, "WHERE merchant_id = ? AND active_from <= ? AND (active_to IS NULL "
                    + "OR active_to > ?)", merchantId.value(), at, at);
            if (active.size() >= 2) {
                return RotationResult.of(Rotation.IN_PROGRESS);
            }
            if (!secrets(c, "WHERE merchant_id = ? AND kid = ?", merchantId.value(), kid).isEmpty()) {
                return RotationResult.of(Rotation.KID_TAKEN);
            }
            try (PreparedStatement ps = c.prepareStatement("""
                    UPDATE merchant_webhook_secrets SET active_to = ?
                     WHERE merchant_id = ? AND active_from <= ? AND (active_to IS NULL OR active_to > ?)
                    """)) {
                ps.setObject(1, ts(retireAt));
                ps.setString(2, merchantId.value());
                ps.setObject(3, at);
                ps.setObject(4, ts(retireAt));
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO merchant_webhook_secrets (merchant_id, kid, secret_enc, active_from, active_to)
                    VALUES (?, ?, ?, ?, NULL)
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, kid);
                ps.setBytes(3, secretEnc.clone());
                ps.setObject(4, at);
                ps.executeUpdate();
            }
            TreeMap<String, String> details = new TreeMap<>();
            details.put("merchant_id", merchantId.value());
            details.put("new_kid", kid);
            details.put("retired_kids", active.stream().map(WebhookSecret::kid).collect(Collectors.joining(",")));
            details.put("retire_at", retireAt.toString());
            ManualRequests.stamp(details, requestKey, request);
            AuditRepo.Row row = audit.append(AuditRepo.Entry.of(now, actor, ROTATE_ACTION, merchantId.value(), details));
            // the stored instant (microseconds), so a replay's answer is the same as this one (D94)
            return new RotationResult(Rotation.ROTATED, Optional.of(row.entry().at()), Optional.of(retireAt));
        });
    }

    /** The secrets active at {@code now}, by merchant then key id. */
    public List<WebhookSecret> activeSecrets(Instant now) {
        OffsetDateTime at = ts(now);
        return inTransaction("active webhook secrets", c -> secrets(c,
                "WHERE active_from <= ? AND (active_to IS NULL OR active_to > ?)", at, at));
    }

    private static List<WebhookSecret> secrets(Connection c, String where, Object... params) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT merchant_id, kid, secret_enc, active_from, active_to FROM merchant_webhook_secrets
                """ + where + " ORDER BY merchant_id COLLATE \"C\", kid COLLATE \"C\"")) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            List<WebhookSecret> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new WebhookSecret(MerchantId.of(rs.getString(1)), rs.getString(2), rs.getBytes(3),
                            rs.getObject(4, OffsetDateTime.class).toInstant(), optionalInstant(rs, 5)));
                }
            }
            return List.copyOf(out);
        }
    }

    // ---- API tokens -----------------------------------------------------------------------------------------------

    /**
     * @return true if inserted now, false if it already existed for the same merchant with the same scopes
     * @throws IllegalStateException if a token with this hash exists for another merchant or with other scopes
     */
    public boolean addToken(ApiToken token, String createdBy) {
        return inTransaction("api token", c -> {
            if (insertToken(c, token, createdBy, true)) {
                return true;
            }
            ApiToken stored = token(c, token.tokenSha256()).orElseThrow();
            if (!stored.merchantId().equals(token.merchantId()) || !stored.scopes().equals(token.scopes())) {
                throw new IllegalStateException("a different token with the same hash already exists");
            }
            return false;
        });
    }

    private static boolean insertToken(Connection c, ApiToken token, String createdBy, boolean ifAbsent)
            throws SQLException {
        IdRules.requireKey("createdBy", createdBy);
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO api_tokens (token_sha256, token_id, merchant_id, scopes, created_at, expires_at, revoked_at,
                                        created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """ + (ifAbsent ? " ON CONFLICT (token_sha256) DO NOTHING" : ""))) {
            ps.setString(1, token.tokenSha256().hex());
            ps.setString(2, token.tokenId());
            ps.setString(3, token.merchantId().map(MerchantId::value).orElse(null));
            Array scopes = c.createArrayOf("text", token.scopes().toArray(new String[0]));
            ps.setArray(4, scopes);
            ps.setObject(5, ts(token.createdAt()));
            ps.setObject(6, ts(token.expiresAt()));
            ps.setObject(7, token.revokedAt().map(PostgresDirectory::ts).orElse(null));
            ps.setString(8, createdBy);
            return ps.executeUpdate() == 1;
        }
    }

    /** What creating a token through the API did (D95). */
    public sealed interface TokenCreation {
    }

    /** Created now. */
    public record TokenCreated(String tokenId) implements TokenCreation {
    }

    /** An earlier request with this key created {@code tokenId}; nothing changed now. */
    public record TokenAlreadyCreated(String tokenId) implements TokenCreation {
    }

    /** The key was used for a different request. */
    public record TokenKeyReused() implements TokenCreation {
    }

    /** The merchant to bind the token to does not exist. */
    public record TokenUnknownMerchant() implements TokenCreation {
    }

    /**
     * Stores a new token and its audit row ({@code TOKEN_CREATE}; never the token) in one transaction (D95). The caller
     * checked the scopes and the lifetime; this checks the merchant and the request key (D94).
     */
    public TokenCreation createToken(ApiToken token, String actor, String note, String requestKey, Sha256 request) {
        return inTransaction("create api token", c -> {
            JdbcResolverRepos.Audit audit = new JdbcResolverRepos.Audit(new JdbcTx(c, BalanceStrategy.APPEND_SNAPSHOT));
            switch (ManualRequests.prior(audit, TOKEN_CREATE_ACTION, TOKENS_SUBJECT, requestKey, request)) {
                case ManualRequests.Different d -> {
                    return new TokenKeyReused();
                }
                case ManualRequests.Same same -> {
                    return new TokenAlreadyCreated(same.row().entry().details().get("token_id"));
                }
                case ManualRequests.None none -> {
                    // create below
                }
            }
            if (token.merchantId().isPresent()) {
                try (PreparedStatement ps = c.prepareStatement("SELECT 1 FROM merchants WHERE merchant_id = ?")) {
                    ps.setString(1, token.merchantId().get().value());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            return new TokenUnknownMerchant();
                        }
                    }
                }
            }
            insertToken(c, token, actor, false);
            TreeMap<String, String> details = new TreeMap<>();
            details.put("token_id", token.tokenId());
            details.put("merchant_id", token.merchantId().map(MerchantId::value).orElse(""));
            details.put("scopes", String.join(",", token.scopes()));
            details.put("expires_at", token.expiresAt().toString());
            details.put("note", note);
            ManualRequests.stamp(details, requestKey, request);
            audit.append(AuditRepo.Entry.of(token.createdAt(), actor, TOKEN_CREATE_ACTION, TOKENS_SUBJECT, details));
            return new TokenCreated(token.tokenId());
        });
    }

    /** What revoking a token through the API did (D95). */
    public enum Revocation {
        REVOKED,
        /** An earlier request with this key revoked it; nothing changed now. */
        REPLAYED,
        KEY_REUSED,
        /** No token has this id. */
        NOT_FOUND,
        /** The token has a scope the caller does not: a token may revoke only tokens no stronger than itself. */
        FORBIDDEN,
        ALREADY_REVOKED
    }

    /** A revocation's outcome; for REVOKED and REPLAYED, when the token stopped working. */
    public record RevocationResult(Revocation status, Optional<Instant> revokedAt) {
    }

    /**
     * Revokes a token from {@code now} on, with its audit row ({@code TOKEN_REVOKE}), in one transaction (D95). The
     * filter looks tokens up on every request, so the revocation takes effect at once.
     *
     * @param callerScopes the scopes of the token asking: it may revoke only a token whose scopes it holds all of
     */
    public RevocationResult revokeToken(String tokenId, List<String> callerScopes, Instant now, String actor,
                                        String note, String requestKey, Sha256 request) {
        return inTransaction("revoke api token", c -> {
            JdbcResolverRepos.Audit audit = new JdbcResolverRepos.Audit(new JdbcTx(c, BalanceStrategy.APPEND_SNAPSHOT));
            switch (ManualRequests.prior(audit, TOKEN_REVOKE_ACTION, tokenId, requestKey, request)) {
                case ManualRequests.Different d -> {
                    return new RevocationResult(Revocation.KEY_REUSED, Optional.empty());
                }
                case ManualRequests.Same same -> {
                    return new RevocationResult(Revocation.REPLAYED, Optional.of(same.row().entry().at()));
                }
                case ManualRequests.None none -> {
                    // revoke below
                }
            }
            List<String> scopes;
            boolean revoked;
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT scopes, revoked_at FROM api_tokens WHERE token_id = ? FOR UPDATE")) {
                ps.setString(1, tokenId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return new RevocationResult(Revocation.NOT_FOUND, Optional.empty());
                    }
                    scopes = List.of((String[]) rs.getArray(1).getArray());
                    revoked = rs.getObject(2) != null;
                }
            }
            if (!callerScopes.containsAll(scopes)) {
                return new RevocationResult(Revocation.FORBIDDEN, Optional.empty());
            }
            if (revoked) {
                return new RevocationResult(Revocation.ALREADY_REVOKED, Optional.empty());
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE api_tokens SET revoked_at = ? WHERE token_id = ? AND revoked_at IS NULL")) {
                ps.setObject(1, ts(now));
                ps.setString(2, tokenId);
                ps.executeUpdate();
            }
            TreeMap<String, String> details = new TreeMap<>();
            details.put("token_id", tokenId);
            details.put("note", note);
            ManualRequests.stamp(details, requestKey, request);
            AuditRepo.Row row = audit.append(AuditRepo.Entry.of(now, actor, TOKEN_REVOKE_ACTION, tokenId, details));
            return new RevocationResult(Revocation.REVOKED, Optional.of(row.entry().at()));
        });
    }

    /** Every token, newest first, for the token API's list (D95). */
    public List<TokenInfo> tokens() {
        return inTransaction("list api tokens", c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    SELECT token_id, merchant_id, scopes, created_at, expires_at, revoked_at, created_by FROM api_tokens
                     ORDER BY created_at DESC, token_id COLLATE "C"
                    """);
                 ResultSet rs = ps.executeQuery()) {
                List<TokenInfo> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(new TokenInfo(rs.getString(1), Optional.ofNullable(rs.getString(2)).map(MerchantId::of),
                            List.of((String[]) rs.getArray(3).getArray()),
                            rs.getObject(4, OffsetDateTime.class).toInstant(),
                            rs.getObject(5, OffsetDateTime.class).toInstant(), optionalInstant(rs, 6), rs.getString(7)));
                }
                return List.copyOf(out);
            }
        });
    }

    /** The token whose SHA-256 is {@code sha}, revoked or not. */
    public Optional<ApiToken> token(Sha256 sha) {
        return inTransaction("api token lookup", c -> token(c, sha));
    }

    private static Optional<ApiToken> token(Connection c, Sha256 sha) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT merchant_id, scopes, created_at, expires_at, revoked_at FROM api_tokens WHERE token_sha256 = ?
                """)) {
            ps.setString(1, sha.hex());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                String merchant = rs.getString(1);
                String[] scopes = (String[]) rs.getArray(2).getArray();
                return Optional.of(new ApiToken(sha, Optional.ofNullable(merchant).map(MerchantId::of),
                        List.of(scopes), rs.getObject(3, OffsetDateTime.class).toInstant(),
                        rs.getObject(4, OffsetDateTime.class).toInstant(), optionalInstant(rs, 5)));
            }
        }
    }
}
