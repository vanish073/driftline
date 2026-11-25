package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.resolver.ManualRequests;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
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
 * Migration V14 on real PostgreSQL, as the application role (Shift 11, D99): stored contacts are erased on request and
 * after the retention period, each leaving a tombstone that keeps them from being stored again; erasures are audited
 * without any contact; the vault's rows can be deleted but never changed, and tombstones never go away.
 */
class CustomerErasureIT {

    static final Instant NOW = Instant.parse("2026-11-25T06:00:00Z");
    static final MerchantId M = MerchantId.of("m_pii");
    static final CustomerVault.Sealed CONTACT = new CustomerVault.Sealed(Optional.of(new byte[40]),
            Optional.of(new byte[40]), Optional.empty(), Optional.of("a".repeat(64)));

    static String url;
    static HikariDataSource pool;
    static CustomerVault vault;
    static PostgresStore store;

    @BeforeAll
    static void database() {
        url = PostgresFixture.newDatabase("customer_erasure_it");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(PostgresFixture.APP_USER);
        config.setPassword(PostgresFixture.APP_PASSWORD);
        config.setMaximumPoolSize(2);
        config.setAutoCommit(false);
        pool = new HikariDataSource(config);
        vault = new CustomerVault(pool);
        store = PostgresFixture.store(url, BalanceStrategy.APPEND_SNAPSHOT, 2);
        new PostgresDirectory(pool).onboard(new MerchantTerms(M, 1, new FeeSchedule(200, 1_800), 1,
                LocalDate.parse("2026-11-24")), NOW);
    }

    @AfterAll
    static void close() {
        store.close();
        pool.close();
    }

    static void asApp(String sql) throws SQLException {
        try (Connection c = PostgresFixture.app(url); Statement st = c.createStatement()) {
            st.execute(sql);
            c.commit();
        }
    }

    @Test
    void anErasedContactIsGoneAuditedWithoutItAndNeverStoredAgain() {
        PaymentId p1 = PaymentId.of("pay_e1");
        PaymentId p2 = PaymentId.of("pay_e2");
        assertThat(vault.store(M, p1, CONTACT, NOW)).isTrue();
        assertThat(vault.store(M, p2, CONTACT, NOW)).isTrue();
        Sha256 asked = ManualRequests.hash("erase", "vpa");
        assertThat(vault.erase(M, List.of(p2, p1, PaymentId.of("pay_never")), NOW.plusSeconds(1), "ops/aaaaaaaa",
                "customer asked to be forgotten", "erase-1", asked))
                .isEqualTo(new CustomerVault.Erased(List.of(p1, p2)));
        assertThat(vault.find(M, p1)).isEmpty();
        assertThat(vault.find(M, p2)).isEmpty();
        assertThat(vault.store(M, p1, CONTACT, NOW.plusSeconds(2))).as("a later webhook stores nothing").isFalse();
        assertThat(vault.find(M, p1)).isEmpty();

        assertThat(vault.erase(M, List.of(p1, p2), NOW.plusSeconds(3), "ops/aaaaaaaa", "customer asked to be forgotten",
                "erase-1", asked)).isEqualTo(new CustomerVault.ErasureReplayed(2));
        assertThat(vault.erase(M, List.of(p1), NOW.plusSeconds(3), "ops/aaaaaaaa", "x", "erase-1",
                ManualRequests.hash("other"))).isEqualTo(new CustomerVault.ErasureKeyReused());

        List<AuditRepo.Row> rows = store.transact("rows", tx -> tx.audit().rows());
        assertThat(rows).filteredOn(r -> r.entry().action().equals(CustomerVault.ERASE_ACTION)).singleElement()
                .satisfies(r -> assertThat(r.entry().details()).containsEntry("count", "2")
                        .containsEntry("payment_ids", "pay_e1,pay_e2").doesNotContainKeys("vpa", "phone", "email"));
    }

    @Test
    void contactsOlderThanTheRetentionPeriodArePurgedInBatchesAndOthersKept() {
        for (int i = 0; i < 5; i++) {
            vault.store(M, PaymentId.of("pay_old" + i), CONTACT, NOW.minus(Duration.ofDays(200)).plusSeconds(i));
        }
        vault.store(M, PaymentId.of("pay_new"), CONTACT, NOW.minus(Duration.ofDays(10)));
        Instant cutoff = NOW.minus(Duration.ofDays(180));
        assertThat(vault.purgeOlderThan(cutoff, 3, NOW)).isEqualTo(3);
        assertThat(vault.purgeOlderThan(cutoff, 3, NOW)).isEqualTo(2);
        assertThat(vault.purgeOlderThan(cutoff, 3, NOW)).isZero();
        assertThat(vault.find(M, PaymentId.of("pay_old0"))).isEmpty();
        assertThat(vault.find(M, PaymentId.of("pay_new"))).isPresent();
        assertThat(vault.store(M, PaymentId.of("pay_old0"), CONTACT, NOW)).as("tombstoned").isFalse();
    }

    @Test
    void theApplicationMayDeleteAContactButNeverChangeOneOrForgetATombstone() throws SQLException {
        vault.store(M, PaymentId.of("pay_g1"), CONTACT, NOW);
        assertThatThrownBy(() -> asApp("UPDATE payment_customers SET vpa_index = '" + "b".repeat(64)
                + "' WHERE payment_id = 'pay_g1'")).hasMessageContaining("permission denied");
        try (Connection c = PostgresFixture.superuser(url); Statement st = c.createStatement()) {
            assertThatThrownBy(() -> st.execute("UPDATE payment_customers SET vpa_index = '" + "b".repeat(64)
                    + "' WHERE payment_id = 'pay_g1'")).as("not even the owner").hasMessageContaining("append-only");
            assertThatThrownBy(() -> st.execute("DELETE FROM payment_customers_erased"))
                    .hasMessageContaining("append-only");
        }
        assertThatThrownBy(() -> asApp("DELETE FROM payment_customers_erased")).hasMessageContaining("permission denied");
        assertThatThrownBy(() -> asApp("UPDATE payment_customers_erased SET reason = 'REQUEST'"))
                .hasMessageContaining("permission denied");
        asApp("DELETE FROM payment_customers WHERE payment_id = 'pay_g1'");
        assertThat(vault.find(M, PaymentId.of("pay_g1"))).isEmpty();
    }
}
