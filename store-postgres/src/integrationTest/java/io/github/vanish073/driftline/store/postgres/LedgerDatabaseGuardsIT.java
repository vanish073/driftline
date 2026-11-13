package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Layer 3 of "the ledger rejects any unbalanced transaction" (DESIGN §7.3): PostgreSQL itself, driven by raw SQL that
 * skips every line of Driftline code. The application role is used throughout, except where a test shows that even
 * the owner cannot rewrite history.
 */
class LedgerDatabaseGuardsIT {

    private static final String URL = PostgresFixture.newDatabase("ledger_guards");
    private static final AtomicInteger SEQ = new AtomicInteger();

    /** Opens the merchant's BANK and SALES accounts as the app role and returns their ids. */
    private static long[] accounts(Connection c, String merchant) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.executeUpdate("INSERT INTO ledger_accounts (merchant_id, code) VALUES ('" + merchant + "', 'BANK'), ('"
                    + merchant + "', 'SALES') ON CONFLICT DO NOTHING");
        }
        long bank = 0;
        long sales = 0;
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT account_id, code FROM ledger_accounts WHERE merchant_id = '" + merchant + "'")) {
            while (rs.next()) {
                if (rs.getString("code").equals("BANK")) {
                    bank = rs.getLong("account_id");
                } else if (rs.getString("code").equals("SALES")) {
                    sales = rs.getLong("account_id");
                }
            }
        }
        c.commit();
        return new long[] {bank, sales};
    }

    private static String newTxn(Connection c, String merchant) throws SQLException {
        String id = "txn-" + SEQ.incrementAndGet();
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO ledger_transactions"
                + " (txn_id, merchant_id, cause_type, cause_id, posted_at) VALUES (?, ?, 'MANUAL_ADJUSTMENT', ?, ?)")) {
            ps.setString(1, id);
            ps.setString(2, merchant);
            ps.setString(3, id);
            ps.setObject(4, OffsetDateTime.parse("2026-11-13T06:00:00Z"));
            ps.executeUpdate();
        }
        return id;
    }

    private static void entry(Connection c, String txn, long account, long paise) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO ledger_entries (txn_id, account_id, amount_paise) VALUES (?, ?, ?)")) {
            ps.setString(1, txn);
            ps.setLong(2, account);
            ps.setLong(3, paise);
            ps.executeUpdate();
        }
    }

    private static long count(String sql) throws SQLException {
        try (Connection c = PostgresFixture.superuser(URL); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void anUnbalancedTransactionFailsAtCommitNotBefore() throws SQLException {
        try (Connection c = PostgresFixture.app(URL)) {
            long[] a = accounts(c, "m-unbalanced");
            String txn = newTxn(c, "m-unbalanced");
            entry(c, txn, a[0], 100);
            entry(c, txn, a[1], -90);        // both inserts succeed: the check is deferred to COMMIT
            assertThatThrownBy(c::commit).isInstanceOf(SQLException.class).hasMessageContaining("does not balance");
            c.rollback();
            assertThat(count("SELECT count(*) FROM ledger_transactions WHERE txn_id = '" + txn + "'")).isZero();
        }
    }

    @Test
    void aSingleEntryOrNoEntryAtAllFailsAtCommit() throws SQLException {
        try (Connection c = PostgresFixture.app(URL)) {
            long[] a = accounts(c, "m-single");
            entry(c, newTxn(c, "m-single"), a[0], 100);
            assertThatThrownBy(c::commit).isInstanceOf(SQLException.class).hasMessageContaining("at least 2 are required");
            c.rollback();

            newTxn(c, "m-single");
            assertThatThrownBy(c::commit).isInstanceOf(SQLException.class).hasMessageContaining("has 0 entries");
            c.rollback();
        }
    }

    @Test
    void aZeroEntryIsRejectedAtOnce() throws SQLException {
        try (Connection c = PostgresFixture.app(URL)) {
            long[] a = accounts(c, "m-zero");
            String txn = newTxn(c, "m-zero");
            assertThatThrownBy(() -> entry(c, txn, a[0], 0)).isInstanceOf(SQLException.class)
                    .extracting(e -> ((SQLException) e).getSQLState()).isEqualTo("23514");
            c.rollback();
        }
    }

    @Test
    void anEntryCannotBeAddedToATransactionCreatedByAnotherDatabaseTransaction() throws SQLException {
        try (Connection c = PostgresFixture.app(URL)) {
            long[] a = accounts(c, "m-foreign");
            String txn = newTxn(c, "m-foreign");
            entry(c, txn, a[0], 100);
            entry(c, txn, a[1], -100);
            c.commit();

            // A later database transaction tries to append a balanced pair to the committed ledger transaction.
            assertThatThrownBy(() -> entry(c, txn, a[0], 50)).isInstanceOf(SQLException.class)
                    .hasMessageContaining("only by the database transaction that created");
            c.rollback();
            assertThat(count("SELECT count(*) FROM ledger_entries WHERE txn_id = '" + txn + "'")).isEqualTo(2);
        }
    }

    @Test
    void anEntryCannotPostToAnotherMerchantsAccount() throws SQLException {
        try (Connection c = PostgresFixture.app(URL)) {
            long[] mine = accounts(c, "m-mine");
            long[] theirs = accounts(c, "m-theirs");
            String txn = newTxn(c, "m-mine");
            entry(c, txn, mine[0], 100);
            assertThatThrownBy(() -> entry(c, txn, theirs[1], -100)).isInstanceOf(SQLException.class)
                    .hasMessageContaining("cannot post to an account of merchant m-theirs");
            c.rollback();
        }
    }

    @Test
    void theAppRoleCannotUpdateDeleteTruncateOrDisableTriggers() throws SQLException {
        String txn;
        try (Connection c = PostgresFixture.app(URL)) {
            long[] a = accounts(c, "m-history");
            txn = newTxn(c, "m-history");
            entry(c, txn, a[0], 100);
            entry(c, txn, a[1], -100);
            c.commit();
        }
        String[] statements = {
            "UPDATE ledger_entries SET amount_paise = 1000 WHERE txn_id = '" + txn + "'",
            "DELETE FROM ledger_entries WHERE txn_id = '" + txn + "'",
            "UPDATE ledger_transactions SET cause_id = 'x' WHERE txn_id = '" + txn + "'",
            "DELETE FROM ledger_transactions WHERE txn_id = '" + txn + "'",
            "TRUNCATE ledger_entries",
            "ALTER TABLE ledger_entries DISABLE TRIGGER ALL",
            "DELETE FROM audit_log",
        };
        for (String sql : statements) {
            try (Connection c = PostgresFixture.app(URL); Statement st = c.createStatement()) {
                assertThatThrownBy(() -> st.execute(sql)).as(sql).isInstanceOf(SQLException.class)
                        .extracting(e -> ((SQLException) e).getSQLState()).isIn("42501", "42809");
                c.rollback();
            }
        }
        assertThat(count("SELECT sum(amount_paise) FROM ledger_entries WHERE txn_id = '" + txn + "'")).isZero();
        assertThat(count("SELECT count(*) FROM ledger_entries WHERE txn_id = '" + txn + "'")).isEqualTo(2);
    }

    @Test
    void evenTheOwnerCannotRewriteHistory() throws SQLException {
        String txn;
        try (Connection c = PostgresFixture.app(URL)) {
            long[] a = accounts(c, "m-owner");
            txn = newTxn(c, "m-owner");
            entry(c, txn, a[0], 100);
            entry(c, txn, a[1], -100);
            c.commit();
        }
        String[] statements = {
            "UPDATE ledger_entries SET amount_paise = 1000 WHERE txn_id = '" + txn + "'",
            "DELETE FROM ledger_entries WHERE txn_id = '" + txn + "'",
            "DELETE FROM ledger_transactions WHERE txn_id = '" + txn + "'",
            "TRUNCATE ledger_entries CASCADE",
        };
        for (String sql : statements) {
            try (Connection c = PostgresFixture.superuser(URL); Statement st = c.createStatement()) {
                assertThatThrownBy(() -> st.execute(sql)).as(sql).isInstanceOf(SQLException.class)
                        .hasMessageContaining("is append-only");
            }
        }
        assertThat(count("SELECT count(*) FROM ledger_entries WHERE txn_id = '" + txn + "'")).isEqualTo(2);
    }
}
