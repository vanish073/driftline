package io.github.vanish073.driftline.app;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.flywaydb.core.Flyway;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One PostgreSQL 17 and one Kafka 4.2.2 (KRaft, single node) per test JVM, both pinned by digest, Kafka capped at
 * {@code -Xmx384m} (DESIGN §15.1). Each test class gets a fresh migrated database and its own topics.
 */
public final class Containers {

    public static final String APP_USER = "driftline_app_it";
    public static final String APP_PASSWORD = "app-it-password";

    private static final DockerImageName POSTGRES_IMAGE = DockerImageName
            .parse("postgres@sha256:846a7ddaa67a4b354ae1d21087b683901aa6113a4ed626d3787cfbf6c85e3e07")
            .asCompatibleSubstituteFor("postgres");
    /** apache/kafka:4.2.2, the broker of the kafka-clients version Spring Boot 4.1.2 manages. */
    private static final DockerImageName KAFKA_IMAGE = DockerImageName
            .parse("apache/kafka@sha256:1213eb3943d551e5ed1fca7a4e109001cee35770b66a02a0c37a8964efe09b69")
            .asCompatibleSubstituteFor("apache/kafka");

    public static final PostgreSQLContainer POSTGRES = startPostgres();
    public static final KafkaContainer KAFKA = startKafka();

    private Containers() {
    }

    private static PostgreSQLContainer startPostgres() {
        PostgreSQLContainer c = new PostgreSQLContainer(POSTGRES_IMAGE)
                .withCommand("postgres", "-c", "fsync=off", "-c", "shared_buffers=32MB", "-c", "max_connections=80");
        c.start();
        return c;
    }

    private static KafkaContainer startKafka() {
        KafkaContainer c = new KafkaContainer(KAFKA_IMAGE)
                .withEnv("KAFKA_HEAP_OPTS", "-Xmx384m -Xms384m")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        c.start();
        return c;
    }

    /** Creates database {@code name}, runs every migration as the owner, adds the app login, returns the JDBC URL. */
    public static synchronized String newDatabase(String name) {
        String database = name.toLowerCase(Locale.ROOT);
        try (Connection admin = superuser(POSTGRES.getJdbcUrl()); Statement st = admin.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS " + database);
            st.execute("CREATE DATABASE " + database);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        String url = urlOf(database);
        Flyway.configure().dataSource(url, POSTGRES.getUsername(), POSTGRES.getPassword()).load().migrate();
        createAppLogin(url);
        return url;
    }

    /** An empty database (no migrations) for the migrate role's own test. */
    public static synchronized String emptyDatabase(String name) {
        String database = name.toLowerCase(Locale.ROOT);
        try (Connection admin = superuser(POSTGRES.getJdbcUrl()); Statement st = admin.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS " + database);
            st.execute("CREATE DATABASE " + database);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return urlOf(database);
    }

    static void createAppLogin(String url) {
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
    }

    public static String urlOf(String database) {
        String base = POSTGRES.getJdbcUrl();
        int slash = base.lastIndexOf('/');
        int query = base.indexOf('?', slash);
        return base.substring(0, slash + 1) + database + (query < 0 ? "" : base.substring(query));
    }

    public static Connection superuser(String url) throws SQLException {
        return DriverManager.getConnection(url, POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    /** A pool that connects as the application role, never as the owner. */
    public static HikariDataSource appPool(String url, int size) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(APP_USER);
        config.setPassword(APP_PASSWORD);
        config.setMaximumPoolSize(size);
        config.setAutoCommit(false);
        config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        return new HikariDataSource(config);
    }

    /** One number from the database, as the owner (tests read freely; the app role writes). */
    public static long count(String url, String sql) {
        try (Connection c = superuser(url); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) {
                throw new IllegalStateException("no row from: " + sql);
            }
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void createTopic(String topic, int partitions) {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(p)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)
                    .configs(Map.of("min.insync.replicas", "1")))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException("cannot create topic " + topic, e);
        }
    }

    /** Waits for {@code condition}, polling; fails with {@code what} after {@code timeout}. */
    public static void await(String what, Duration timeout, BooleanSupplier condition) {
        Instant deadline = Instant.now().plus(timeout);
        while (!condition.getAsBoolean()) {
            if (Instant.now().isAfter(deadline)) {
                throw new AssertionError("timed out after " + timeout + " waiting for " + what);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted waiting for " + what);
            }
        }
    }
}
