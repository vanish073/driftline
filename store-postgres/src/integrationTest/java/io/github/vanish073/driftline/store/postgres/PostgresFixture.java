package io.github.vanish073.driftline.store.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import org.flywaydb.core.Flyway;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One PostgreSQL 17 container per test JVM, and a fresh migrated database per test class. The image is pinned by
 * digest (postgres:17, which was 17.12 on 2026-11-13). fsync is off because these tests check behaviour, not
 * durability; the benchmark in Shift 10 runs with the normal settings.
 */
final class PostgresFixture {

    static final String APP_USER = "driftline_app_it";
    static final String APP_PASSWORD = "app-it-password";

    private static final DockerImageName IMAGE = DockerImageName
            .parse("postgres@sha256:846a7ddaa67a4b354ae1d21087b683901aa6113a4ed626d3787cfbf6c85e3e07")
            .asCompatibleSubstituteFor("postgres");

    private static final PostgreSQLContainer CONTAINER = start();

    private PostgresFixture() {
    }

    private static PostgreSQLContainer start() {
        PostgreSQLContainer container = new PostgreSQLContainer(IMAGE)
                .withCommand("postgres", "-c", "fsync=off", "-c", "shared_buffers=32MB", "-c", "max_connections=80");
        container.start();
        return container;
    }

    /** Creates database {@code name}, runs every Flyway migration on it and returns its JDBC URL. */
    static synchronized String newDatabase(String name) {
        String database = name.toLowerCase(Locale.ROOT);
        try (Connection admin = superuser(CONTAINER.getJdbcUrl()); Statement st = admin.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS " + database);
            st.execute("CREATE DATABASE " + database);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        String url = urlOf(database);
        Flyway.configure().dataSource(url, CONTAINER.getUsername(), CONTAINER.getPassword()).load().migrate();
        try (Connection admin = superuser(url); Statement st = admin.createStatement()) {
            st.execute("""
                    DO $$ BEGIN
                        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '%s') THEN
                            CREATE ROLE %s LOGIN PASSWORD '%s' IN ROLE driftline_app;
                        END IF;
                    END $$
                    """.formatted(APP_USER, APP_USER, APP_PASSWORD));
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return url;
    }

    static String urlOf(String database) {
        String base = CONTAINER.getJdbcUrl();
        int slash = base.lastIndexOf('/');
        int query = base.indexOf('?', slash);
        return base.substring(0, slash + 1) + database + (query < 0 ? "" : base.substring(query));
    }

    /** The container's superuser, which also owns the migrations (the role of driftline_migrator in production). */
    static Connection superuser(String url) throws SQLException {
        return DriverManager.getConnection(url, CONTAINER.getUsername(), CONTAINER.getPassword());
    }

    /** A login in role driftline_app: only the privileges the application has. */
    static Connection app(String url) throws SQLException {
        Connection c = DriverManager.getConnection(url, APP_USER, APP_PASSWORD);
        c.setAutoCommit(false);
        return c;
    }

    /** A store that connects as the application role, never as the owner. */
    static PostgresStore store(String url, BalanceStrategy strategy, int poolSize) {
        return PostgresStore.connect(url, APP_USER, APP_PASSWORD, strategy, poolSize);
    }
}
