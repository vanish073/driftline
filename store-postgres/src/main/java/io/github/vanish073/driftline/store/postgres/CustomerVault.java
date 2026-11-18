package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
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
import javax.sql.DataSource;

/**
 * The customer vault (migration V10, D77, DESIGN §14): the encrypted contact details of a payment's customer. It holds
 * ciphertext and a blind index only; the caller (ingest, which has the key) encrypts and decrypts. A payment's row is
 * written once, by the first webhook that carries a contact, and never changed. Like {@link SettlementInbox} this is
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

    /** @return true if stored now, false if the payment already has a row (the first contact wins) */
    public boolean store(MerchantId merchantId, PaymentId paymentId, Sealed sealed, Instant at) {
        if (sealed.isEmpty()) {
            throw new IllegalArgumentException("nothing to store");
        }
        return inTransaction("store customer of " + merchantId + "/" + paymentId, c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO payment_customers (merchant_id, payment_id, vpa_enc, phone_enc, email_enc, vpa_index,
                                                   first_seen_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (merchant_id, payment_id) DO NOTHING
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, paymentId.value());
                ps.setBytes(3, sealed.vpa().orElse(null));
                ps.setBytes(4, sealed.phone().orElse(null));
                ps.setBytes(5, sealed.email().orElse(null));
                ps.setString(6, sealed.vpaIndex().orElse(null));
                ps.setObject(7, OffsetDateTime.ofInstant(at, ZoneOffset.UTC));
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
}
