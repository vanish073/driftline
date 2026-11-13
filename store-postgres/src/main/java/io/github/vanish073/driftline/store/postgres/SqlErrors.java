package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

/**
 * Maps JDBC failures onto the two failures a {@code Store} may report (DESIGN §3.2). Anything else, such as a ledger
 * trigger firing, means Driftline wrote something it should not have: that is a bug, and it surfaces as an
 * {@link IllegalStateException}.
 */
final class SqlErrors {

    private SqlErrors() {
    }

    static RuntimeException translate(String what, SQLException e) {
        String state = e.getSQLState() == null ? "" : e.getSQLState();
        String message = what + " failed (SQLSTATE " + state + "): " + e.getMessage();
        if (e instanceof SQLTransientConnectionException || state.startsWith("08")
                || state.equals("57P01") || state.equals("57P02") || state.equals("57P03") || state.equals("53300")) {
            return new StoreUnavailable(message, e);
        }
        if (state.equals("23505") || state.equals("40001") || state.equals("40P01")) {
            return new StoreConflict(message, e);
        }
        return new IllegalStateException(message, e);
    }
}
