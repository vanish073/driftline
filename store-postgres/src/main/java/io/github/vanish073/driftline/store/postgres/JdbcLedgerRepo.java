package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.Cause;
import io.github.vanish073.driftline.core.ledger.Entry;
import io.github.vanish073.driftline.core.ledger.LedgerChecks;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.LedgerRepo;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The ledger over JDBC (DESIGN §7.4, §7.5). A posting validates its entries again (layer 2), inserts the transaction
 * with {@code ON CONFLICT DO NOTHING} on {@code (merchant_id, cause_type, cause_id)}, and on a conflict compares the
 * stored entries: equal is a duplicate, different is a {@link PostResult.LedgerConflict}.
 *
 * <p>Concurrent posts of one cause are safe under READ COMMITTED: the second insert waits for the first to commit,
 * does nothing, and then reads the committed row. Locks are always taken in ascending account order, so two
 * postings that touch the same accounts cannot deadlock.
 */
final class JdbcLedgerRepo implements LedgerRepo {

    private final JdbcTx tx;
    private final BalanceStrategy strategy;

    JdbcLedgerRepo(JdbcTx tx, BalanceStrategy strategy) {
        this.tx = tx;
        this.strategy = strategy;
    }

    @Override
    public PostResult post(LedgerTxn txn) {
        Connection c = tx.connection();
        LedgerChecks.requireValid(txn.merchantId(), txn.entries());
        try {
            Map<AccountCode, Long> accounts = ensureAccounts(c, txn);
            if (!insertTransaction(c, txn)) {
                LedgerTxn existing = find(txn.merchantId(), txn.cause()).orElseThrow(
                        () -> new IllegalStateException("cause " + txn.cause() + " conflicted but cannot be read back"));
                return existing.sameEntriesAs(txn)
                        ? new PostResult.Duplicate(existing.id())
                        : new PostResult.LedgerConflict(existing, txn);
            }
            insertEntries(c, txn, accounts);
            if (strategy == BalanceStrategy.ROW_LOCK) {
                updateBalanceRows(c, txn, accounts);
            }
            return new PostResult.Posted(txn.id());
        } catch (SQLException e) {
            throw SqlErrors.translate("post " + txn.cause(), e);
        }
    }

    /** Creates the merchant's accounts on first use and returns their ids. */
    private Map<AccountCode, Long> ensureAccounts(Connection c, LedgerTxn txn) throws SQLException {
        TreeMap<AccountCode, Long> ids = new TreeMap<>();
        for (Entry entry : txn.entries()) {
            ids.put(entry.account(), 0L);
        }
        String merchant = txn.merchantId().value();
        try (PreparedStatement insert = c.prepareStatement(
                "INSERT INTO ledger_accounts (merchant_id, code) VALUES (?, ?) ON CONFLICT (merchant_id, code) DO NOTHING")) {
            for (AccountCode code : ids.keySet()) {
                insert.setString(1, merchant);
                insert.setString(2, code.name());
                insert.addBatch();
            }
            insert.executeBatch();
        }
        try (PreparedStatement select = c.prepareStatement(
                "SELECT account_id, code FROM ledger_accounts WHERE merchant_id = ? AND code = ANY (?)")) {
            select.setString(1, merchant);
            select.setArray(2, c.createArrayOf("text", ids.keySet().stream().map(Enum::name).toArray()));
            try (ResultSet rs = select.executeQuery()) {
                while (rs.next()) {
                    ids.put(AccountCode.valueOf(rs.getString("code")), rs.getLong("account_id"));
                }
            }
        }
        if (strategy == BalanceStrategy.ROW_LOCK) {
            try (PreparedStatement balanceRow = c.prepareStatement(
                    "INSERT INTO account_balances (account_id) VALUES (?) ON CONFLICT (account_id) DO NOTHING")) {
                for (long id : ids.values()) {
                    balanceRow.setLong(1, id);
                    balanceRow.addBatch();
                }
                balanceRow.executeBatch();
            }
        }
        return ids;
    }

    /** @return false if a transaction with this cause already exists */
    private static boolean insertTransaction(Connection c, LedgerTxn txn) throws SQLException {
        try (PreparedStatement insert = c.prepareStatement("""
                INSERT INTO ledger_transactions (txn_id, merchant_id, cause_type, cause_id, posted_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (merchant_id, cause_type, cause_id) DO NOTHING
                """)) {
            insert.setString(1, txn.id().value());
            insert.setString(2, txn.merchantId().value());
            insert.setString(3, txn.cause().type().name());
            insert.setString(4, txn.cause().id());
            insert.setObject(5, OffsetDateTime.ofInstant(txn.postedAt(), ZoneOffset.UTC));
            return insert.executeUpdate() == 1;
        }
    }

    private static void insertEntries(Connection c, LedgerTxn txn, Map<AccountCode, Long> accounts) throws SQLException {
        try (PreparedStatement insert = c.prepareStatement(
                "INSERT INTO ledger_entries (txn_id, account_id, amount_paise) VALUES (?, ?, ?)")) {
            for (Entry entry : txn.entries()) {
                insert.setString(1, txn.id().value());
                insert.setLong(2, accounts.get(entry.account()));
                insert.setLong(3, entry.amount().paise());
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    private static void updateBalanceRows(Connection c, LedgerTxn txn, Map<AccountCode, Long> accounts)
            throws SQLException {
        TreeMap<Long, Long> deltas = new TreeMap<>();   // ascending account id: a fixed lock order
        for (Entry entry : txn.entries()) {
            deltas.merge(accounts.get(entry.account()), entry.amount().paise(), Math::addExact);
        }
        try (PreparedStatement update = c.prepareStatement(
                "UPDATE account_balances SET balance_paise = balance_paise + ? WHERE account_id = ?")) {
            for (Map.Entry<Long, Long> delta : deltas.entrySet()) {
                update.setLong(1, delta.getValue());
                update.setLong(2, delta.getKey());
                if (update.executeUpdate() != 1) {
                    throw new IllegalStateException("no balance row for account " + delta.getKey());
                }
            }
        }
    }

    @Override
    public Optional<LedgerTxn> find(MerchantId merchantId, Cause cause) {
        Connection c = tx.connection();
        try (PreparedStatement select = c.prepareStatement("""
                SELECT t.posted_at, a.code, e.amount_paise
                  FROM ledger_transactions t
                  JOIN ledger_entries e ON e.txn_id = t.txn_id
                  JOIN ledger_accounts a ON a.account_id = e.account_id
                 WHERE t.merchant_id = ? AND t.cause_type = ? AND t.cause_id = ?
                 ORDER BY e.entry_id
                """)) {
            select.setString(1, merchantId.value());
            select.setString(2, cause.type().name());
            select.setString(3, cause.id());
            Instant postedAt = null;
            List<Entry> entries = new ArrayList<>();
            try (ResultSet rs = select.executeQuery()) {
                while (rs.next()) {
                    postedAt = rs.getObject("posted_at", OffsetDateTime.class).toInstant();
                    entries.add(new Entry(merchantId, AccountCode.valueOf(rs.getString("code")),
                            Money.ofPaise(rs.getLong("amount_paise"))));
                }
            }
            return postedAt == null ? Optional.empty() : Optional.of(LedgerTxn.of(merchantId, cause, postedAt, entries));
        } catch (SQLException e) {
            throw SqlErrors.translate("find " + cause, e);
        }
    }

    @Override
    public Money balance(MerchantId merchantId, AccountCode account) {
        Connection c = tx.connection();
        String sql = strategy == BalanceStrategy.ROW_LOCK
                ? """
                  SELECT coalesce((SELECT b.balance_paise
                                     FROM account_balances b
                                     JOIN ledger_accounts a ON a.account_id = b.account_id
                                    WHERE a.merchant_id = ? AND a.code = ?), 0)
                  """
                : """
                  WITH account AS (
                      SELECT account_id FROM ledger_accounts WHERE merchant_id = ? AND code = ?
                  ), snapshot AS (
                      SELECT s.horizon_xid, s.balance_paise
                        FROM account_snapshots s JOIN account USING (account_id)
                       ORDER BY s.horizon_xid DESC
                       LIMIT 1
                  )
                  SELECT (coalesce((SELECT balance_paise FROM snapshot), 0)
                          + coalesce((SELECT sum(e.amount_paise)
                                        FROM ledger_entries e JOIN account USING (account_id)
                                       WHERE NOT EXISTS (SELECT 1 FROM snapshot)
                                          OR e.xid >= (SELECT horizon_xid FROM snapshot)), 0))::bigint
                  """;
        try (PreparedStatement select = c.prepareStatement(sql)) {
            select.setString(1, merchantId.value());
            select.setString(2, account.name());
            try (ResultSet rs = select.executeQuery()) {
                rs.next();
                return Money.ofPaise(rs.getLong(1));
            }
        } catch (SQLException e) {
            throw SqlErrors.translate("balance " + merchantId + " " + account, e);
        }
    }
}
