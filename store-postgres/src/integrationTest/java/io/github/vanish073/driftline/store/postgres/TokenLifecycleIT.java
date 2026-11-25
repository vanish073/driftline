package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.resolver.ManualRequests;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Migration V13 and the token API's storage on real PostgreSQL, as the application role (Shift 11, D95): tokens expire,
 * are created and revoked with their audit rows, a revocation is final, and the database refuses every other change
 * to a token, whoever asks.
 */
class TokenLifecycleIT {

    static final Instant NOW = Instant.parse("2026-11-25T06:00:00Z");
    static final MerchantId M = MerchantId.of("m_tok");

    static String url;
    static HikariDataSource pool;
    static PostgresDirectory directory;
    static PostgresStore store;

    @BeforeAll
    static void database() {
        url = PostgresFixture.newDatabase("token_lifecycle_it");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(PostgresFixture.APP_USER);
        config.setPassword(PostgresFixture.APP_PASSWORD);
        config.setMaximumPoolSize(2);
        config.setAutoCommit(false);
        pool = new HikariDataSource(config);
        directory = new PostgresDirectory(pool);
        store = PostgresFixture.store(url, BalanceStrategy.APPEND_SNAPSHOT, 2);
        directory.onboard(new MerchantTerms(M, 1, new FeeSchedule(200, 1_800), 1, LocalDate.parse("2026-11-24")), NOW);
    }

    @AfterAll
    static void close() {
        store.close();
        pool.close();
    }

    static PostgresDirectory.ApiToken token(String plain, Optional<MerchantId> merchant, List<String> scopes) {
        return new PostgresDirectory.ApiToken(Sha256.of(plain.getBytes(StandardCharsets.UTF_8)), merchant, scopes, NOW,
                NOW.plus(Duration.ofDays(30)), Optional.empty());
    }

    static void asApp(String sql) throws SQLException {
        try (Connection c = PostgresFixture.app(url); Statement st = c.createStatement()) {
            st.execute(sql);
            c.commit();
        }
    }

    static void asOwner(String sql) throws SQLException {
        try (Connection c = PostgresFixture.superuser(url); Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    @Test
    void aTokenIsCreatedWithItsAuditRowListedWithoutItsHashAndRevokedOnceForGood() {
        PostgresDirectory.ApiToken t = token("dl_mer_one", Optional.of(M), List.of("events:write", "breaks:read"));
        Sha256 asked = ManualRequests.hash("create", "one");
        assertThat(directory.createToken(t, "ops/aaaaaaaa", "onboarding", "mint-one", asked))
                .isEqualTo(new PostgresDirectory.TokenCreated(t.tokenId()));
        assertThat(directory.token(t.tokenSha256())).contains(t);
        assertThat(directory.token(t.tokenSha256()).orElseThrow().activeAt(NOW.plus(Duration.ofDays(30))))
                .as("expired at expires_at").isFalse();
        assertThat(directory.tokens()).filteredOn(i -> i.tokenId().equals(t.tokenId())).singleElement()
                .satisfies(i -> {
                    assertThat(i.createdBy()).isEqualTo("ops/aaaaaaaa");
                    assertThat(i.expiresAt()).isEqualTo(t.expiresAt());
                    assertThat(i.toString()).doesNotContain(t.tokenSha256().hex());
                });

        // a repeat makes no second token; the key with another request is refused
        assertThat(directory.createToken(token("dl_mer_two", Optional.of(M), List.of("breaks:read")), "ops/aaaaaaaa",
                "onboarding", "mint-one", asked)).isEqualTo(new PostgresDirectory.TokenAlreadyCreated(t.tokenId()));
        assertThat(directory.createToken(token("dl_mer_two", Optional.of(M), List.of("breaks:read")), "ops/aaaaaaaa",
                "onboarding", "mint-one", ManualRequests.hash("other")))
                .isEqualTo(new PostgresDirectory.TokenKeyReused());
        assertThat(directory.createToken(token("dl_mer_three", Optional.of(MerchantId.of("m_nobody")),
                List.of("breaks:read")), "ops/aaaaaaaa", "x", "mint-three", asked))
                .isEqualTo(new PostgresDirectory.TokenUnknownMerchant());

        // revocation: only by a caller holding every scope of the token, effective at once, final
        Sha256 revoke = ManualRequests.hash("revoke", "one");
        assertThat(directory.revokeToken(t.tokenId(), List.of("breaks:read"), NOW.plusSeconds(60), "ops/bbbbbbbb",
                "lost", "rev-1", revoke).status()).isEqualTo(PostgresDirectory.Revocation.FORBIDDEN);
        PostgresDirectory.RevocationResult done = directory.revokeToken(t.tokenId(),
                List.of("events:write", "breaks:read", "tokens:write"), NOW.plusSeconds(60), "ops/bbbbbbbb", "lost",
                "rev-2", revoke);
        assertThat(done).isEqualTo(new PostgresDirectory.RevocationResult(PostgresDirectory.Revocation.REVOKED,
                Optional.of(NOW.plusSeconds(60))));
        assertThat(directory.token(t.tokenSha256()).orElseThrow().activeAt(NOW.plusSeconds(61))).isFalse();
        assertThat(directory.revokeToken(t.tokenId(), List.of("events:write", "breaks:read"), NOW.plusSeconds(90),
                "ops/bbbbbbbb", "lost", "rev-2", revoke)).as("a repeat answers as the first")
                .isEqualTo(new PostgresDirectory.RevocationResult(PostgresDirectory.Revocation.REPLAYED,
                        Optional.of(NOW.plusSeconds(60))));
        assertThat(directory.revokeToken(t.tokenId(), List.of("events:write", "breaks:read"), NOW.plusSeconds(90),
                "ops/bbbbbbbb", "lost", "rev-3", revoke).status())
                .isEqualTo(PostgresDirectory.Revocation.ALREADY_REVOKED);
        assertThat(directory.revokeToken("tok_0000000000000000", List.of("breaks:read"), NOW, "ops/b", "x", "rev-4",
                revoke).status()).isEqualTo(PostgresDirectory.Revocation.NOT_FOUND);

        List<AuditRepo.Row> rows = store.transact("rows", tx -> tx.audit().rows());
        assertThat(rows).extracting(r -> r.entry().action()).containsExactly(PostgresDirectory.TOKEN_CREATE_ACTION,
                PostgresDirectory.TOKEN_REVOKE_ACTION);
        assertThat(rows.get(0).entry().details()).containsEntry("token_id", t.tokenId())
                .containsEntry("scopes", "events:write,breaks:read").containsEntry("merchant_id", "m_tok");
        assertThat(rows.toString()).doesNotContain("dl_mer_one").doesNotContain(t.tokenSha256().hex());
    }

    @Test
    void theDatabaseAllowsTheApplicationToSetRevokedAtOnceAndNothingElseAndNotEvenTheOwnerMore() throws SQLException {
        PostgresDirectory.ApiToken t = token("dl_ops_guarded", Optional.empty(), List.of("breaks:read"));
        directory.addToken(t, "it");
        String where = " WHERE token_id = '" + t.tokenId() + "'";
        assertThatThrownBy(() -> asApp("UPDATE api_tokens SET scopes = ARRAY['ledger:adjust']" + where))
                .hasMessageContaining("permission denied");
        assertThatThrownBy(() -> asApp("UPDATE api_tokens SET expires_at = now() + interval '10 years'" + where))
                .hasMessageContaining("permission denied");
        assertThatThrownBy(() -> asApp("DELETE FROM api_tokens" + where)).hasMessageContaining("permission denied");
        assertThatThrownBy(() -> asOwner("UPDATE api_tokens SET expires_at = now() + interval '10 years'" + where))
                .as("even the owner cannot extend a token").hasMessageContaining("only be revoked");
        assertThatThrownBy(() -> asOwner("DELETE FROM api_tokens" + where)).hasMessageContaining("never deleted");
        asApp("UPDATE api_tokens SET revoked_at = now()" + where);
        assertThatThrownBy(() -> asApp("UPDATE api_tokens SET revoked_at = NULL" + where))
                .as("a revocation cannot be undone").hasMessageContaining("only be revoked");
        assertThatThrownBy(() -> asOwner("UPDATE api_tokens SET revoked_at = NULL" + where))
                .hasMessageContaining("only be revoked");
        assertThatThrownBy(() -> asApp("INSERT INTO api_tokens (token_sha256, token_id, scopes, created_at, expires_at,"
                + " created_by) VALUES ('" + "b".repeat(64) + "', 'tok_" + "c".repeat(16) + "', ARRAY['breaks:read'],"
                + " now(), now() + interval '1 day', 'it')")).as("the id is the hash's prefix")
                .hasMessageContaining("check");
        assertThatThrownBy(() -> asApp("INSERT INTO api_tokens (token_sha256, token_id, scopes, created_at, expires_at,"
                + " created_by) VALUES ('" + "b".repeat(64) + "', 'tok_" + "b".repeat(16) + "', ARRAY['breaks:read'],"
                + " now(), now() - interval '1 day', 'it')")).as("it expires after it is created")
                .hasMessageContaining("check");
    }
}
