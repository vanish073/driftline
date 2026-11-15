package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Migration V5 on real PostgreSQL, as the application role (DESIGN §10, §12, §14, D47, D48): onboarding is idempotent
 * and never changes a merchant's terms, secrets and tokens are stored as given (ciphertext, hashes), the upload inbox
 * only moves forward, and the database itself refuses what the application must never do.
 */
class IngestTablesIT {

    static final Instant NOW = Instant.parse("2026-11-15T06:00:00Z");
    static final MerchantId M = MerchantId.of("m_v5");
    static final MerchantTerms TERMS = new MerchantTerms(M, 3, new FeeSchedule(200, 1_800), 1,
            LocalDate.parse("2026-11-14"));
    static final byte[] CIPHERTEXT = new byte[64];

    static String url;
    static HikariDataSource pool;
    static PostgresDirectory directory;
    static SettlementInbox inbox;

    @BeforeAll
    static void database() {
        url = PostgresFixture.newDatabase("ingest_tables_it");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(PostgresFixture.APP_USER);
        config.setPassword(PostgresFixture.APP_PASSWORD);
        config.setMaximumPoolSize(4);
        config.setAutoCommit(false);
        pool = new HikariDataSource(config);
        directory = new PostgresDirectory(pool);
        inbox = new SettlementInbox(pool);
        directory.onboard(TERMS, NOW);
    }

    @AfterAll
    static void close() {
        pool.close();
    }

    @Test
    void onboardingIsIdempotentAndTermsNeverChange() {
        assertThat(directory.onboard(TERMS, NOW.plusSeconds(5))).isFalse();
        assertThat(directory.merchants()).contains(TERMS);
        MerchantTerms moved = new MerchantTerms(M, 4, TERMS.fees(), 1, TERMS.firstSettlementDate());
        assertThatThrownBy(() -> directory.onboard(moved, NOW)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("never change");
    }

    @Test
    void webhookSecretsAreActiveInTheirWindowOnly() {
        directory.addWebhookSecret(new PostgresDirectory.WebhookSecret(M, "old", CIPHERTEXT, NOW.minusSeconds(100),
                Optional.of(NOW.minusSeconds(10))));
        directory.addWebhookSecret(new PostgresDirectory.WebhookSecret(M, "cur", CIPHERTEXT, NOW.minusSeconds(50),
                Optional.empty()));
        assertThat(directory.addWebhookSecret(new PostgresDirectory.WebhookSecret(M, "cur", new byte[70],
                NOW, Optional.empty()))).as("a second insert of the same key id changes nothing").isFalse();
        assertThat(directory.activeSecrets(NOW)).extracting(PostgresDirectory.WebhookSecret::kid).containsExactly("cur");
        assertThat(directory.activeSecrets(NOW.minusSeconds(20))).extracting(PostgresDirectory.WebhookSecret::kid)
                .containsExactly("cur", "old");
        assertThat(directory.activeSecrets(NOW).get(0).secretEnc()).isEqualTo(CIPHERTEXT);
    }

    @Test
    void tokensAreStoredAsHashesAndAHashCannotBeReboundToAnotherMerchant() {
        Sha256 hash = Sha256.of("tok-1".getBytes(StandardCharsets.UTF_8));
        PostgresDirectory.ApiToken token = new PostgresDirectory.ApiToken(hash, Optional.of(M),
                List.of("events:write", "breaks:read"), NOW, Optional.empty());
        assertThat(directory.addToken(token)).isTrue();
        assertThat(directory.addToken(token)).isFalse();
        assertThat(directory.token(hash)).contains(token);
        assertThatThrownBy(() -> directory.addToken(new PostgresDirectory.ApiToken(hash, Optional.empty(),
                List.of("breaks:read"), NOW, Optional.empty()))).isInstanceOf(IllegalStateException.class);
        assertThat(directory.token(Sha256.of(new byte[] {1}))).isEmpty();
        assertThatThrownBy(() -> new PostgresDirectory.ApiToken(hash, Optional.empty(), List.of("root"), NOW,
                Optional.empty())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anUploadIsStoredOnceAndMovesForwardOnce() {
        byte[] raw = "H,DLSF,1,stl,m_v5,2026-11-14,2026-11-14T04:30:00Z\nT,0,0.00\n".getBytes(StandardCharsets.UTF_8);
        SettlementInbox.Stored first = inbox.store(M, raw, NOW);
        assertThat(first.isNew()).isTrue();
        assertThat(first.sha256()).isEqualTo(Sha256.of(raw));
        assertThat(inbox.store(M, raw, NOW.plusSeconds(1)).isNew()).isFalse();
        assertThat(inbox.pending(10)).extracting(SettlementInbox.Upload::sha256).contains(first.sha256());
        assertThat(inbox.finish(M, first.sha256(), SettlementInbox.State.EMITTED, Optional.empty(), NOW)).isTrue();
        assertThat(inbox.finish(M, first.sha256(), SettlementInbox.State.REFUSED_CONFLICT, Optional.empty(), NOW))
                .as("a second worker finds it already finished").isFalse();
        assertThat(inbox.find(M, first.sha256()).orElseThrow().state()).isEqualTo(SettlementInbox.State.EMITTED);
        assertThat(inbox.pending(10)).extracting(SettlementInbox.Upload::sha256).doesNotContain(first.sha256());
        assertThat(inbox.find(M, first.sha256()).orElseThrow().raw()).isEqualTo(raw);
    }

    private static void asApp(String sql) throws SQLException {
        try (Connection c = PostgresFixture.app(url); Statement st = c.createStatement()) {
            st.execute(sql);
            c.commit();
        }
    }

    @Test
    void theDatabaseRefusesWhatTheApplicationMustNeverDo() throws SQLException {
        assertThatThrownBy(() -> asApp("UPDATE merchants SET partition = 9")).hasMessageContaining("permission denied");
        assertThatThrownBy(() -> asApp("DELETE FROM api_tokens")).hasMessageContaining("permission denied");
        assertThatThrownBy(() -> asApp("DELETE FROM settlement_uploads")).hasMessageContaining("permission denied");
        byte[] raw = "bytes".getBytes(StandardCharsets.UTF_8);
        SettlementInbox.Stored s = inbox.store(M, raw, NOW);
        assertThatThrownBy(() -> asApp("UPDATE settlement_uploads SET raw = 'x'::bytea WHERE sha256 = '"
                + s.sha256().hex() + "'")).hasMessageContaining("never change");
        inbox.finish(M, s.sha256(), SettlementInbox.State.REJECTED_MALFORMED, Optional.of("bad"), NOW);
        assertThatThrownBy(() -> asApp("UPDATE settlement_uploads SET state = 'RECEIVED', processed_at = NULL,"
                + " error = NULL WHERE sha256 = '" + s.sha256().hex() + "'")).hasMessageContaining("already");
        assertThatThrownBy(() -> asApp("INSERT INTO quarantine (id, partition, kafka_offset, merchant_id, reason, error,"
                + " raw) VALUES ('q1', 0, 1, 'm_v5', 'UNDECODABLE', 'bad', 'x'::bytea)"))
                .as("an undecodable row names no merchant").hasMessageContaining("quarantine_shape");
        assertThatThrownBy(() -> asApp("INSERT INTO quarantine (id, partition, kafka_offset, reason) VALUES"
                + " ('q2', 0, 2, 'IDEMPOTENCY_CONFLICT')")).hasMessageContaining("quarantine_shape");
        assertThatThrownBy(() -> asApp("INSERT INTO api_tokens (token_sha256, scopes, created_at) VALUES ('"
                + "a".repeat(64) + "', ARRAY['admin'], now())")).hasMessageContaining("check");
    }
}
