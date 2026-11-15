package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
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
            "breaks:read", "breaks:resolve");

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

    /** One API token as stored: only its SHA-256. An empty merchant is an ops token. */
    public record ApiToken(Sha256 tokenSha256, Optional<MerchantId> merchantId, List<String> scopes, Instant createdAt,
                           Optional<Instant> revokedAt) {
        public ApiToken {
            Objects.requireNonNull(tokenSha256, "tokenSha256");
            Objects.requireNonNull(merchantId, "merchantId");
            scopes = List.copyOf(scopes);
            if (scopes.isEmpty() || !SCOPES.containsAll(scopes)) {
                throw new IllegalArgumentException("scopes must be a non-empty subset of " + SCOPES);
            }
            Objects.requireNonNull(createdAt, "createdAt");
            Objects.requireNonNull(revokedAt, "revokedAt");
        }

        public boolean activeAt(Instant now) {
            return revokedAt.isEmpty() || now.isBefore(revokedAt.get());
        }
    }

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
    public boolean addToken(ApiToken token) {
        return inTransaction("api token", c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO api_tokens (token_sha256, merchant_id, scopes, created_at, revoked_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (token_sha256) DO NOTHING
                    """)) {
                ps.setString(1, token.tokenSha256().hex());
                ps.setString(2, token.merchantId().map(MerchantId::value).orElse(null));
                Array scopes = c.createArrayOf("text", token.scopes().toArray(new String[0]));
                ps.setArray(3, scopes);
                ps.setObject(4, ts(token.createdAt()));
                ps.setObject(5, token.revokedAt().map(PostgresDirectory::ts).orElse(null));
                if (ps.executeUpdate() == 1) {
                    return true;
                }
            }
            ApiToken stored = token(c, token.tokenSha256()).orElseThrow();
            if (!stored.merchantId().equals(token.merchantId()) || !stored.scopes().equals(token.scopes())) {
                throw new IllegalStateException("a different token with the same hash already exists");
            }
            return false;
        });
    }

    /** The token whose SHA-256 is {@code sha}, revoked or not. */
    public Optional<ApiToken> token(Sha256 sha) {
        return inTransaction("api token lookup", c -> token(c, sha));
    }

    private static Optional<ApiToken> token(Connection c, Sha256 sha) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT merchant_id, scopes, created_at, revoked_at FROM api_tokens WHERE token_sha256 = ?
                """)) {
            ps.setString(1, sha.hex());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                String merchant = rs.getString(1);
                String[] scopes = (String[]) rs.getArray(2).getArray();
                return Optional.of(new ApiToken(sha, Optional.ofNullable(merchant).map(MerchantId::of),
                        List.of(scopes), rs.getObject(3, OffsetDateTime.class).toInstant(), optionalInstant(rs, 4)));
            }
        }
    }
}
