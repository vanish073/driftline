package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.resolver.ManualRequests;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.sql.DataSource;

/**
 * The customer vault (migration V10, D77, DESIGN §14): the encrypted contact details of a payment's customer. It holds
 * ciphertext and a blind index only; the caller (ingest, which has the key) encrypts and decrypts. A payment's row is
 * written once, by the first webhook that carries a contact, and never changed. It is deleted once older than the
 * retention period, or on request (Shift 11, D99), leaving a tombstone without personal data so that it is never
 * stored again. Like {@link SettlementInbox} this is
 * production infrastructure outside the core: personal data never reaches the matcher, Kafka or the simulator.
 */
public final class CustomerVault {

    private static final Pattern INDEX = Pattern.compile("[0-9a-f]{64}");

    /** One payment's stored contact: ciphertext per field. {@link #toString} prints which fields are present only. */
    public record Sealed(Optional<byte[]> vpa, Optional<byte[]> phone, Optional<byte[]> email,
                         Optional<String> vpaIndex) {
        public Sealed {
            vpa = vpa.map(byte[]::clone);
            phone = phone.map(byte[]::clone);
            email = email.map(byte[]::clone);
            Objects.requireNonNull(vpaIndex, "vpaIndex");
            if (vpa.isPresent() != vpaIndex.isPresent()) {
                throw new IllegalArgumentException("a VPA is stored with its blind index, and only then");
            }
            vpaIndex.ifPresent(i -> {
                if (!INDEX.matcher(i).matches()) {
                    throw new IllegalArgumentException("a blind index is 64 hex characters");
                }
            });
        }

        @Override
        public String toString() {
            return "Sealed[vpa=" + vpa.isPresent() + ", phone=" + phone.isPresent() + ", email=" + email.isPresent() + "]";
        }

        public boolean isEmpty() {
            return vpa.isEmpty() && phone.isEmpty() && email.isEmpty();
        }
    }

    private final DataSource dataSource;

    public CustomerVault(DataSource dataSource) {
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

    /** @return true if stored now; false if the payment already has a row (the first contact wins) or was erased */
    public boolean store(MerchantId merchantId, PaymentId paymentId, Sealed sealed, Instant at) {
        if (sealed.isEmpty()) {
            throw new IllegalArgumentException("nothing to store");
        }
        return inTransaction("store customer of " + merchantId + "/" + paymentId, c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO payment_customers (merchant_id, payment_id, vpa_enc, phone_enc, email_enc, vpa_index,
                                                   first_seen_at)
                    SELECT ?, ?, ?, ?, ?, ?, ?
                     WHERE NOT EXISTS (SELECT 1 FROM payment_customers_erased e
                                        WHERE e.merchant_id = ? AND e.payment_id = ?)
                    ON CONFLICT (merchant_id, payment_id) DO NOTHING
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, paymentId.value());
                ps.setBytes(3, sealed.vpa().orElse(null));
                ps.setBytes(4, sealed.phone().orElse(null));
                ps.setBytes(5, sealed.email().orElse(null));
                ps.setString(6, sealed.vpaIndex().orElse(null));
                ps.setObject(7, OffsetDateTime.ofInstant(at, ZoneOffset.UTC));
                ps.setString(8, merchantId.value());
                ps.setString(9, paymentId.value());
                return ps.executeUpdate() == 1;
            }
        });
    }

    public Optional<Sealed> find(MerchantId merchantId, PaymentId paymentId) {
        return inTransaction("find customer of " + merchantId + "/" + paymentId, c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    SELECT vpa_enc, phone_enc, email_enc, vpa_index FROM payment_customers
                     WHERE merchant_id = ? AND payment_id = ?
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, paymentId.value());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(new Sealed(Optional.ofNullable(rs.getBytes(1)), Optional.ofNullable(rs.getBytes(2)),
                            Optional.ofNullable(rs.getBytes(3)), Optional.ofNullable(rs.getString(4))));
                }
            }
        });
    }

    /** The merchant's payments whose customer VPA has this blind index, by payment id. */
    public List<PaymentId> paymentsByVpa(MerchantId merchantId, String vpaIndex) {
        if (!INDEX.matcher(vpaIndex).matches()) {
            throw new IllegalArgumentException("a blind index is 64 hex characters");
        }
        return inTransaction("payments by VPA for " + merchantId, c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    SELECT payment_id FROM payment_customers WHERE merchant_id = ? AND vpa_index = ?
                     ORDER BY payment_id COLLATE "C" LIMIT 1000
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, vpaIndex);
                List<PaymentId> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(PaymentId.of(rs.getString(1)));
                    }
                }
                return List.copyOf(out);
            }
        });
    }

    // ---- retention and erasure (Shift 11, D99) --------------------------------------------------------------------

    /** The audit action of an erasure on request. */
    public static final String ERASE_ACTION = "PII_ERASE";

    /** What an erasure on request did. */
    public sealed interface Erasure {
    }

    /** Erased now: the payments that had a stored contact (others had none, or were erased before). */
    public record Erased(List<PaymentId> paymentIds) implements Erasure {
        public Erased {
            paymentIds = List.copyOf(paymentIds);
        }
    }

    /** An earlier request with this key did it; nothing changed now. {@code count} is what it erased. */
    public record ErasureReplayed(int count) implements Erasure {
    }

    /** The key was used for a different request. */
    public record ErasureKeyReused() implements Erasure {
    }

    /**
     * Erases the stored contact of each of {@code payments}, leaves their tombstones and appends one audit row
     * ({@code PII_ERASE}; payment ids and a count, never a contact), in one transaction.
     */
    public Erasure erase(MerchantId merchantId, List<PaymentId> payments, Instant at, String actor, String note,
                         String requestKey, Sha256 request) {
        if (payments.size() > 1_000) {
            throw new IllegalArgumentException("at most 1000 payments per erasure");
        }
        return inTransaction("erase customers of " + merchantId, c -> {
            JdbcResolverRepos.Audit audit = new JdbcResolverRepos.Audit(new JdbcTx(c, BalanceStrategy.APPEND_SNAPSHOT));
            switch (ManualRequests.prior(audit, ERASE_ACTION, merchantId.value(), requestKey, request)) {
                case ManualRequests.Different d -> {
                    return new ErasureKeyReused();
                }
                case ManualRequests.Same same -> {
                    return new ErasureReplayed(Integer.parseInt(same.row().entry().details().get("count")));
                }
                case ManualRequests.None none -> {
                    // erase below
                }
            }
            OffsetDateTime ts = OffsetDateTime.ofInstant(at, ZoneOffset.UTC);
            List<PaymentId> erased = new ArrayList<>();
            for (PaymentId p : new java.util.TreeSet<>(payments)) {
                try (PreparedStatement ps = c.prepareStatement(
                        "DELETE FROM payment_customers WHERE merchant_id = ? AND payment_id = ?")) {
                    ps.setString(1, merchantId.value());
                    ps.setString(2, p.value());
                    if (ps.executeUpdate() == 1) {
                        erased.add(p);
                    }
                }
                tombstone(c, merchantId, p, ts, "REQUEST");
            }
            java.util.TreeMap<String, String> details = new java.util.TreeMap<>();
            details.put("count", Integer.toString(erased.size()));
            details.put("payment_ids", erased.stream().map(PaymentId::value).collect(Collectors.joining(",")));
            details.put("note", note);
            ManualRequests.stamp(details, requestKey, request);
            audit.append(AuditRepo.Entry.of(at, actor, ERASE_ACTION, merchantId.value(), details));
            return new Erased(erased);
        });
    }

    private static void tombstone(Connection c, MerchantId m, PaymentId p, OffsetDateTime at, String reason)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO payment_customers_erased (merchant_id, payment_id, erased_at, reason) VALUES (?, ?, ?, ?)
                ON CONFLICT (merchant_id, payment_id) DO NOTHING
                """)) {
            ps.setString(1, m.value());
            ps.setString(2, p.value());
            ps.setObject(3, at);
            ps.setString(4, reason);
            ps.executeUpdate();
        }
    }

    /**
     * Deletes up to {@code limit} contacts first seen before {@code cutoff}, oldest first, each with its tombstone
     * (reason RETENTION), in one transaction.
     *
     * @return how many were deleted; fewer than {@code limit} means none older remain
     */
    public int purgeOlderThan(Instant cutoff, int limit, Instant at) {
        return inTransaction("purge customer contacts", c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    WITH gone AS (
                        DELETE FROM payment_customers
                         WHERE (merchant_id, payment_id) IN (SELECT merchant_id, payment_id FROM payment_customers
                                                              WHERE first_seen_at < ? ORDER BY first_seen_at LIMIT ?)
                        RETURNING merchant_id, payment_id)
                    INSERT INTO payment_customers_erased (merchant_id, payment_id, erased_at, reason)
                    SELECT merchant_id, payment_id, ?, 'RETENTION' FROM gone
                    ON CONFLICT (merchant_id, payment_id) DO NOTHING
                    """)) {
                ps.setObject(1, OffsetDateTime.ofInstant(cutoff, ZoneOffset.UTC));
                ps.setInt(2, limit);
                ps.setObject(3, OffsetDateTime.ofInstant(at, ZoneOffset.UTC));
                return ps.executeUpdate();
            }
        });
    }
}
