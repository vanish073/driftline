package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.vanish073.driftline.core.audit.AuditChain;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.resolver.ManualRequests;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Webhook secret rotation (§10.2, §14, D76), as the application role: the new secret is active at once, the old one
 * until the overlap ends, a third secret is refused while two are active, each rotation leaves an audit row without the
 * secret, and the application can close a secret's window but never rewrite the secret itself.
 */
class WebhookSecretRotationIT {

    static final Instant NOW = Instant.parse("2026-11-17T06:00:00Z");
    static final MerchantId M = MerchantId.of("m_rot");
    static final byte[] OLD = new byte[64];
    static final byte[] NEW = new byte[65];

    @Test
    void aRotationOverlapsTheOldSecretRefusesAThirdAndIsAudited() throws SQLException {
        String url = PostgresFixture.newDatabase("secret_rotation_it");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(PostgresFixture.APP_USER);
        config.setPassword(PostgresFixture.APP_PASSWORD);
        config.setMaximumPoolSize(2);
        config.setAutoCommit(false);
        try (HikariDataSource pool = new HikariDataSource(config);
             PostgresStore store = PostgresFixture.store(url, BalanceStrategy.APPEND_SNAPSHOT, 2)) {
            PostgresDirectory directory = new PostgresDirectory(pool);
            directory.onboard(new MerchantTerms(M, 1, new FeeSchedule(200, 1_800), 1, LocalDate.parse("2026-11-18")),
                    NOW.minusSeconds(3_600));
            directory.addWebhookSecret(new PostgresDirectory.WebhookSecret(M, "k1", OLD, NOW.minusSeconds(3_600),
                    Optional.empty()));

            Instant retire = NOW.plus(Duration.ofHours(24));
            Sha256 asked = ManualRequests.hash("rotate", "k2", "86400", "secret-hash-1");
            assertThat(directory.rotateWebhookSecret(M, "k2", NEW, NOW, retire, "ops/abc", "rot-1", asked))
                    .isEqualTo(new PostgresDirectory.RotationResult(PostgresDirectory.Rotation.ROTATED,
                            Optional.of(NOW), Optional.of(retire)));
            assertThat(directory.activeSecrets(NOW.plusSeconds(1))).extracting(PostgresDirectory.WebhookSecret::kid)
                    .as("both, during the overlap").containsExactly("k1", "k2");
            assertThat(directory.activeSecrets(retire)).extracting(PostgresDirectory.WebhookSecret::kid)
                    .as("the new one only, once the overlap ends").containsExactly("k2");

            // D94: the same request again changes nothing and answers with the first times; other content is refused
            assertThat(directory.rotateWebhookSecret(M, "k2", NEW, NOW.plusSeconds(5), retire.plusSeconds(5),
                    "ops/abc", "rot-1", asked)).isEqualTo(new PostgresDirectory.RotationResult(
                            PostgresDirectory.Rotation.REPLAYED, Optional.of(NOW), Optional.of(retire)));
            assertThat(directory.rotateWebhookSecret(M, "k2", NEW, NOW.plusSeconds(5), retire, "ops/abc", "rot-1",
                    ManualRequests.hash("rotate", "k2", "86400", "secret-hash-2")).status())
                    .isEqualTo(PostgresDirectory.Rotation.KEY_REUSED);

            assertThat(directory.rotateWebhookSecret(M, "k3", NEW, NOW.plusSeconds(60), retire, "ops/abc", "rot-2",
                    asked).status()).as("a rotation is in progress").isEqualTo(PostgresDirectory.Rotation.IN_PROGRESS);
            assertThat(directory.rotateWebhookSecret(M, "k1", NEW, retire.plusSeconds(1), retire.plusSeconds(60),
                    "ops/abc", "rot-3", asked).status()).as("a key id is never reused")
                    .isEqualTo(PostgresDirectory.Rotation.KID_TAKEN);
            assertThat(directory.rotateWebhookSecret(MerchantId.of("m_nobody"), "k9", NEW, NOW, retire, "ops/abc",
                    "rot-4", asked).status()).isEqualTo(PostgresDirectory.Rotation.UNKNOWN_MERCHANT);
            assertThatThrownBy(() -> directory.rotateWebhookSecret(M, "k4", NEW, NOW, NOW, "ops/abc", "rot-5", asked))
                    .isInstanceOf(IllegalArgumentException.class);

            List<AuditRepo.Row> rows = store.transact("rows", tx -> tx.audit().rows());
            assertThat(rows).singleElement().satisfies(r -> {
                assertThat(r.entry().action()).isEqualTo("WEBHOOK_SECRET_ROTATE");
                assertThat(r.entry().details()).containsEntry("new_kid", "k2").containsEntry("retired_kids", "k1")
                        .containsEntry("retire_at", retire.toString()).doesNotContainKeys("secret", "secret_enc")
                        .containsEntry(AuditRepo.REQUEST_KEY, "rot-1");
            });
            assertThat(AuditChain.verify(rows).intact()).isTrue();

            try (Connection c = PostgresFixture.app(url); Statement st = c.createStatement()) {
                assertThatThrownBy(() -> st.execute("UPDATE merchant_webhook_secrets SET secret_enc = secret_enc "
                        + "WHERE kid = 'k2'")).as("only active_to may change").isInstanceOf(SQLException.class);
                c.rollback();
            }
        }
    }
}
