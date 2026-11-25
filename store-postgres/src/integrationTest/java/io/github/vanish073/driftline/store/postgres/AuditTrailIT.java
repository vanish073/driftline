package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.audit.AuditChain;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.Cause;
import io.github.vanish073.driftline.core.ledger.CauseType;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.resolver.ManualAdjustment;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * The audit trail (DESIGN §7.6, §14): the application role and even the owner cannot change or remove a row, and
 * {@code verify-audit} ({@link AuditChain}) catches the one edit that gets past both, a superuser's with the triggers
 * switched off, naming the row.
 */
class AuditTrailIT {

    private static final String URL = PostgresFixture.newDatabase("audit_trail");
    private static final Instant T0 = Instant.parse("2026-11-17T06:00:00Z");

    private static List<AuditRepo.Row> rows(PostgresStore store) {
        return store.transact("rows", tx -> tx.audit().rows());
    }

    @Test
    void theTrailCannotBeEditedAndAnEditAroundTheGuardsIsNamedByVerifyAudit() throws SQLException {
        try (PostgresStore store = PostgresFixture.store(URL, BalanceStrategy.APPEND_SNAPSHOT, 2)) {
            for (int i = 1; i <= 3; i++) {
                AuditRepo.Entry e = AuditRepo.Entry.of(T0.plusSeconds(i), "ops/abc", "BREAK_RESOLVE_MANUAL", "brk_" + i,
                        Map.of("note", "row " + i));
                store.transact("append", tx -> tx.audit().append(e));
            }
            assertThat(AuditChain.verify(rows(store))).isEqualTo(new AuditChain.Verdict(3, Optional.empty(),
                    "intact"));

            for (String sql : new String[] {"UPDATE audit_log SET actor = 'x' WHERE seq = 2",
                "DELETE FROM audit_log WHERE seq = 2", "TRUNCATE audit_log"}) {
                try (Connection c = PostgresFixture.app(URL); Statement st = c.createStatement()) {
                    assertThatThrownBy(() -> st.execute(sql)).as("app role: " + sql).isInstanceOf(SQLException.class);
                    c.rollback();
                }
                try (Connection c = PostgresFixture.superuser(URL); Statement st = c.createStatement()) {
                    assertThatThrownBy(() -> st.execute(sql)).as("owner, triggers on: " + sql)
                            .isInstanceOf(SQLException.class).hasMessageContaining("append-only");
                }
            }
            assertThat(AuditChain.verify(rows(store)).intact()).as("nothing changed").isTrue();

            // a superuser can switch the triggers off; the chain still names the row that was changed
            try (Connection c = PostgresFixture.superuser(URL); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                st.execute("SET LOCAL session_replication_role = replica");
                st.execute("UPDATE audit_log SET details = '{\"note\":\"row 2, rewritten\"}' WHERE seq = 2");
                c.commit();
            }
            AuditChain.Verdict tampered = AuditChain.verify(rows(store));
            assertThat(tampered.firstBadSeq()).contains(2L);
            assertThat(tampered.reason()).contains("changed after it was written");
        }
    }

    @Test
    void aManualAdjustmentPostsItsLedgerEntryUnderItsAuditRowInPostgres() {
        String url = PostgresFixture.newDatabase("audit_adjust");
        try (PostgresStore store = PostgresFixture.store(url, BalanceStrategy.APPEND_SNAPSHOT, 2)) {
            MerchantId m = MerchantId.of("m_adj");
            TreeMap<AccountCode, Money> amounts = new TreeMap<>();
            amounts.put(AccountCode.SETTLEMENT_DIFFERENCES, Money.parseSigned("12.34"));
            amounts.put(AccountCode.BANK, Money.parseSigned("-12.34"));
            ManualAdjustment.Result r = store.transact("adjust", tx -> ManualAdjustment.post(tx, m, "ops/abc",
                    "bank fee we had not booked", Optional.empty(), amounts, "adj-it-1", T0));
            assertThat(r).isInstanceOf(ManualAdjustment.Posted.class);
            long seq = ((ManualAdjustment.Posted) r).auditSeq();
            Optional<LedgerTxn> booked = store.transact("find", tx -> tx.ledger().find(m,
                    new Cause(CauseType.MANUAL_ADJUSTMENT, Long.toString(seq))));
            assertThat(booked).as("the ledger entry names its audit row").isPresent();
            assertThat(store.transact("bal", tx -> tx.ledger().balance(m, AccountCode.BANK)).paise()).isEqualTo(-1_234);
            assertThat(AuditChain.verify(rows(store)).intact()).isTrue();
        }
    }
}
