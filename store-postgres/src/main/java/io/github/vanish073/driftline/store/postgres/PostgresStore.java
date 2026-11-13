package io.github.vanish073.driftline.store.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.TxBody;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * The production {@link Store} (DESIGN §3.2): plain JDBC over HikariCP, one database transaction per
 * {@code transact}, READ COMMITTED with explicit locks. No ORM and no Spring (D18).
 *
 * <p>The schema comes from the Flyway migrations in {@code db/migration}; Spring Boot runs them in production, and
 * the integration tests run them with the Flyway API.
 */
public final class PostgresStore implements Store, AutoCloseable {

    private final DataSource dataSource;
    private final BalanceStrategy strategy;
    private final HikariDataSource ownedPool;

    /** Uses a pool the caller owns and closes. */
    public PostgresStore(DataSource dataSource, BalanceStrategy strategy) {
        this(dataSource, strategy, null);
    }

    private PostgresStore(DataSource dataSource, BalanceStrategy strategy, HikariDataSource ownedPool) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.strategy = Objects.requireNonNull(strategy, "strategy");
        this.ownedPool = ownedPool;
    }

    /** Opens a HikariCP pool of {@code poolSize} connections; {@link #close()} closes it. */
    public static PostgresStore connect(String jdbcUrl, String user, String password, BalanceStrategy strategy,
                                        int poolSize) {
        HikariConfig config = new HikariConfig();
        config.setPoolName("driftline-store");
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(poolSize);
        config.setAutoCommit(false);
        config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        HikariDataSource pool = new HikariDataSource(config);
        return new PostgresStore(pool, strategy, pool);
    }

    public BalanceStrategy strategy() {
        return strategy;
    }

    @Override
    public <R> R transact(String txName, TxBody<R> body) {
        Objects.requireNonNull(txName, "txName");
        Objects.requireNonNull(body, "body");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            JdbcTx tx = new JdbcTx(connection, strategy);
            try {
                R result = body.run(tx);
                tx.close();
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException | Error failure) {
                tx.close();
                rollbackQuietly(connection, failure);
                throw failure;
            }
        } catch (SQLException e) {
            throw SqlErrors.translate("transaction " + txName, e);
        }
    }

    /**
     * Folds every finished entry into a new snapshot per account (APPEND_SNAPSHOT, DESIGN §7.5).
     *
     * <p>The horizon is {@code pg_snapshot_xmin(pg_current_snapshot())}: every database transaction below it has
     * finished, so the set of committed entries below it can never change. Folding by entry id instead would be
     * wrong, because ids are not handed out in commit order: an entry with a lower id can commit later and would be
     * skipped for good.
     *
     * @return the number of snapshot rows written
     */
    public int snapshotBalances() {
        String sql = """
                WITH horizon AS (
                    SELECT pg_snapshot_xmin(pg_current_snapshot()) AS xid
                ), latest AS (
                    SELECT DISTINCT ON (account_id) account_id, horizon_xid, balance_paise
                      FROM account_snapshots
                     ORDER BY account_id, horizon_xid DESC
                )
                INSERT INTO account_snapshots (account_id, horizon_xid, balance_paise)
                SELECT a.account_id,
                       h.xid,
                       coalesce(l.balance_paise, 0)
                         + coalesce((SELECT sum(e.amount_paise)
                                       FROM ledger_entries e
                                      WHERE e.account_id = a.account_id
                                        AND e.xid < h.xid
                                        AND (l.horizon_xid IS NULL OR e.xid >= l.horizon_xid)), 0)
                  FROM ledger_accounts a
                 CROSS JOIN horizon h
                  LEFT JOIN latest l ON l.account_id = a.account_id
                 WHERE l.horizon_xid IS NULL OR l.horizon_xid < h.xid
                """;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            try (Statement statement = connection.createStatement()) {
                int rows = statement.executeUpdate(sql);
                connection.commit();
                return rows;
            } catch (SQLException | RuntimeException failure) {
                rollbackQuietly(connection, failure);
                throw failure;
            }
        } catch (SQLException e) {
            throw SqlErrors.translate("snapshotBalances", e);
        }
    }

    private static void rollbackQuietly(Connection connection, Throwable cause) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            cause.addSuppressed(rollbackFailure);
        }
    }

    @Override
    public void close() {
        if (ownedPool != null) {
            ownedPool.close();
        }
    }
}
