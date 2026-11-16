package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * The settlement upload inbox (DESIGN §6.2 step RECEIVED; migration V5; D48). The upload endpoint {@link #store}s the
 * raw bytes and answers; the separate settlement worker takes {@link #pending} uploads, parses them, records the
 * receipt through the Store, emits to Kafka, and then {@link #finish}es the upload.
 *
 * <p>It replaces a direct "parse and emit in the HTTP handler", which would be a dual write to the database and Kafka.
 * Every step is idempotent, so a worker that crashes anywhere simply does the upload again: the receipt comes back as a
 * duplicate and the matcher's dedupe absorbs the re-emitted records (§6.2). Two workers racing on one upload are safe
 * for the same reason; {@link #finish} only moves a row that is still RECEIVED. Like {@link PostgresDirectory} this
 * is production infrastructure, which the simulator replaces with a direct upload call, so it is not a Store method.
 */
public final class SettlementInbox {

    public enum State {
        RECEIVED,
        /** Parsed, receipt recorded, every record acknowledged by Kafka. */
        EMITTED,
        /** A different file for a date or file id that already has one: recorded and escalated, never emitted. */
        REFUSED_CONFLICT,
        /** The worker's parser refused the bytes. The upload endpoint parses first, so this needs a parser change. */
        REJECTED_MALFORMED
    }

    /** One upload. */
    public record Upload(MerchantId merchantId, Sha256 sha256, byte[] raw, Instant receivedAt, State state,
                         Optional<String> error) {
        public Upload {
            Objects.requireNonNull(merchantId, "merchantId");
            Objects.requireNonNull(sha256, "sha256");
            raw = raw.clone();
            Objects.requireNonNull(receivedAt, "receivedAt");
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(error, "error");
        }

        @Override
        public byte[] raw() {
            return raw.clone();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Upload u && u.merchantId.equals(merchantId) && u.sha256.equals(sha256)
                    && Arrays.equals(u.raw, raw) && u.receivedAt.equals(receivedAt) && u.state == state
                    && u.error.equals(error);
        }

        @Override
        public int hashCode() {
            return Objects.hash(merchantId, sha256, Arrays.hashCode(raw), receivedAt, state, error);
        }

        @Override
        public String toString() {
            return "Upload[" + merchantId + ", " + sha256.hex() + ", " + raw.length + " bytes, " + state + "]";
        }
    }

    /** The largest upload stored (the database checks it too). */
    public static final int MAX_BYTES = 16 * 1024 * 1024;

    private final DataSource dataSource;

    public SettlementInbox(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @FunctionalInterface
    private interface Work<R> {
        R run(Connection c) throws SQLException;
    }

    private <R> R inTransaction(String what, Work<R> work) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            try {
                R result = work.run(c);
                c.commit();
                return result;
            } catch (SQLException | RuntimeException failure) {
                try {
                    c.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } catch (SQLException e) {
            throw SqlErrors.translate(what, e);
        }
    }

    private static OffsetDateTime ts(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    /**
     * Stores an upload. Its SHA-256 is computed here, so a caller can never file bytes under another hash.
     *
     * @return the hash, and whether the upload is new (false: the merchant uploaded these exact bytes before)
     */
    public Stored store(MerchantId merchantId, byte[] raw, Instant receivedAt) {
        if (raw.length > MAX_BYTES) {
            throw new IllegalArgumentException("an upload is at most " + MAX_BYTES + " bytes");
        }
        Sha256 sha = Sha256.of(raw);
        boolean inserted = inTransaction("store upload " + merchantId, c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO settlement_uploads (merchant_id, sha256, raw, received_at, state)
                    VALUES (?, ?, ?, ?, 'RECEIVED')
                    ON CONFLICT (merchant_id, sha256) DO NOTHING
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, sha.hex());
                ps.setBytes(3, raw);
                ps.setObject(4, ts(receivedAt));
                return ps.executeUpdate() == 1;
            }
        });
        return new Stored(sha, inserted);
    }

    /** What {@link #store} did. */
    public record Stored(Sha256 sha256, boolean isNew) {
    }

    /** Up to {@code limit} uploads still RECEIVED, oldest first. */
    public List<Upload> pending(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
        return inTransaction("pending uploads", c -> {
            try (PreparedStatement ps = c.prepareStatement(SELECT + """
                     WHERE state = 'RECEIVED'
                     ORDER BY received_at, merchant_id COLLATE "C", sha256 COLLATE "C" LIMIT ?
                    """)) {
                ps.setInt(1, limit);
                return read(ps);
            }
        });
    }

    public Optional<Upload> find(MerchantId merchantId, Sha256 sha256) {
        return inTransaction("find upload", c -> {
            try (PreparedStatement ps = c.prepareStatement(SELECT + " WHERE merchant_id = ? AND sha256 = ?")) {
                ps.setString(1, merchantId.value());
                ps.setString(2, sha256.hex());
                List<Upload> found = read(ps);
                return found.isEmpty() ? Optional.empty() : Optional.of(found.get(0));
            }
        });
    }

    /**
     * Moves an upload out of RECEIVED.
     *
     * @return true if it moved now; false if another worker finished it first (or it does not exist)
     */
    public boolean finish(MerchantId merchantId, Sha256 sha256, State to, Optional<String> error, Instant at) {
        if (to == State.RECEIVED) {
            throw new IllegalArgumentException("an upload only moves forward from RECEIVED");
        }
        if ((to == State.REJECTED_MALFORMED) != error.isPresent()) {
            throw new IllegalArgumentException("an error is recorded exactly for REJECTED_MALFORMED");
        }
        return inTransaction("finish upload", c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    UPDATE settlement_uploads SET state = ?, error = ?, processed_at = ?
                     WHERE merchant_id = ? AND sha256 = ? AND state = 'RECEIVED'
                    """)) {
                ps.setString(1, to.name());
                ps.setString(2, error.orElse(null));
                ps.setObject(3, ts(at));
                ps.setString(4, merchantId.value());
                ps.setString(5, sha256.hex());
                return ps.executeUpdate() == 1;
            }
        });
    }

    private static final String SELECT = """
            SELECT merchant_id, sha256, raw, received_at, state, error FROM settlement_uploads
            """;

    private static List<Upload> read(PreparedStatement ps) throws SQLException {
        List<Upload> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new Upload(MerchantId.of(rs.getString(1)), new Sha256(rs.getString(2)), rs.getBytes(3),
                        rs.getObject(4, OffsetDateTime.class).toInstant(), State.valueOf(rs.getString(5)),
                        Optional.ofNullable(rs.getString(6))));
            }
        }
        return List.copyOf(out);
    }
}
