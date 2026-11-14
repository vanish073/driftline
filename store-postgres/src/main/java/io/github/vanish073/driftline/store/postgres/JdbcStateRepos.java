package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.order.OrderRule;
import io.github.vanish073.driftline.core.order.OrderState;
import io.github.vanish073.driftline.core.order.OrderStatus;
import io.github.vanish073.driftline.core.order.OrderTransition;
import io.github.vanish073.driftline.core.payment.PaymentRule;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import io.github.vanish073.driftline.core.port.DedupeRepo;
import io.github.vanish073.driftline.core.port.OrderRepo;
import io.github.vanish073.driftline.core.port.PartitionRepo;
import io.github.vanish073.driftline.core.port.PaymentRepo;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** The dedupe, payment, order and partition repositories over JDBC (DESIGN §12, migration V2). */
final class JdbcStateRepos {

    private JdbcStateRepos() {
    }

    private static OffsetDateTime ts(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Optional<Instant> instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? Optional.empty() : Optional.of(value.toInstant());
    }

    private static void setNullableTs(PreparedStatement ps, int index, Optional<Instant> value) throws SQLException {
        if (value.isPresent()) {
            ps.setObject(index, ts(value.get()));
        } else {
            ps.setObject(index, null);
        }
    }

    static final class Dedupe implements DedupeRepo {
        private final JdbcTx tx;

        Dedupe(JdbcTx tx) {
            this.tx = tx;
        }

        @Override
        public Claim claim(MerchantId merchantId, Source source, String eventKey, Sha256 payloadSha256, int partition,
                           long offset) {
            Connection c = tx.connection();
            try {
                try (PreparedStatement insert = c.prepareStatement("""
                        INSERT INTO processed_events (merchant_id, source, event_key, payload_sha256, partition, kafka_offset)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT (merchant_id, source, event_key) DO NOTHING
                        """)) {
                    insert.setString(1, merchantId.value());
                    insert.setString(2, source.name());
                    insert.setString(3, eventKey);
                    insert.setString(4, payloadSha256.hex());
                    insert.setInt(5, partition);
                    insert.setLong(6, offset);
                    if (insert.executeUpdate() == 1) {
                        return Claim.FIRST;
                    }
                }
                try (PreparedStatement select = c.prepareStatement(
                        "SELECT payload_sha256 FROM processed_events WHERE merchant_id = ? AND source = ? AND event_key = ?")) {
                    select.setString(1, merchantId.value());
                    select.setString(2, source.name());
                    select.setString(3, eventKey);
                    try (ResultSet rs = select.executeQuery()) {
                        if (!rs.next()) {
                            throw new IllegalStateException("dedupe row " + eventKey + " conflicted but cannot be read");
                        }
                        return rs.getString(1).equals(payloadSha256.hex()) ? Claim.DUPLICATE : Claim.KEY_REUSED;
                    }
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("dedupe claim " + eventKey, e);
            }
        }
    }

    static final class Payments implements PaymentRepo {
        private final JdbcTx tx;

        Payments(JdbcTx tx) {
            this.tx = tx;
        }

        @Override
        public Optional<PaymentState> find(MerchantId merchantId, PaymentId paymentId) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    SELECT status, order_id, amount_paise, failed_at, success_at, conflict
                      FROM payments WHERE merchant_id = ? AND payment_id = ?
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, paymentId.value());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(new PaymentState(paymentId, PaymentStatus.valueOf(rs.getString("status")),
                            Optional.of(OrderId.of(rs.getString("order_id"))),
                            Optional.of(Money.ofPaise(rs.getLong("amount_paise"))),
                            instant(rs, "failed_at"), instant(rs, "success_at"), rs.getBoolean("conflict")));
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("find payment " + paymentId, e);
            }
        }

        @Override
        public void save(MerchantId merchantId, PaymentState state) {
            if (state.status() == PaymentStatus.NONE) {
                throw new IllegalArgumentException("a payment with no events is not stored");
            }
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    INSERT INTO payments (merchant_id, payment_id, status, order_id, amount_paise, failed_at, success_at, conflict)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (merchant_id, payment_id) DO UPDATE SET
                        status = EXCLUDED.status, order_id = EXCLUDED.order_id, amount_paise = EXCLUDED.amount_paise,
                        failed_at = EXCLUDED.failed_at, success_at = EXCLUDED.success_at, conflict = EXCLUDED.conflict
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, state.paymentId().value());
                ps.setString(3, state.status().name());
                ps.setString(4, state.orderId().orElseThrow().value());
                ps.setLong(5, state.amount().orElseThrow().paise());
                setNullableTs(ps, 6, state.failedAt());
                setNullableTs(ps, 7, state.successAt());
                ps.setBoolean(8, state.conflict());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("save payment " + state.paymentId(), e);
            }
        }

        @Override
        public void appendTransition(MerchantId merchantId, PaymentTransition t) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    INSERT INTO payment_transitions
                        (merchant_id, payment_id, seq, from_status, to_status, rule, event_key, event_time)
                    SELECT ?, ?, coalesce(max(seq), 0) + 1, ?, ?, ?, ?, ?
                      FROM payment_transitions WHERE merchant_id = ? AND payment_id = ?
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, t.paymentId().value());
                ps.setString(3, t.from().name());
                ps.setString(4, t.to().name());
                ps.setString(5, t.rule().name());
                ps.setString(6, t.eventKey());
                ps.setObject(7, ts(t.eventTime()));
                ps.setString(8, merchantId.value());
                ps.setString(9, t.paymentId().value());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("append payment transition " + t.paymentId(), e);
            }
        }

        @Override
        public List<PaymentTransition> transitions(MerchantId merchantId, PaymentId paymentId) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    SELECT from_status, to_status, rule, event_key, event_time FROM payment_transitions
                     WHERE merchant_id = ? AND payment_id = ? ORDER BY seq
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, paymentId.value());
                List<PaymentTransition> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new PaymentTransition(paymentId, PaymentStatus.valueOf(rs.getString(1)),
                                PaymentStatus.valueOf(rs.getString(2)), PaymentRule.valueOf(rs.getString(3)),
                                rs.getString(4), rs.getObject(5, OffsetDateTime.class).toInstant()));
                    }
                }
                return List.copyOf(out);
            } catch (SQLException e) {
                throw SqlErrors.translate("payment transitions " + paymentId, e);
            }
        }
    }

    static final class Orders implements OrderRepo {
        private final JdbcTx tx;

        Orders(JdbcTx tx) {
            this.tx = tx;
        }

        @Override
        public Optional<OrderState> find(MerchantId merchantId, OrderId orderId) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    SELECT status, version, amount_paise, expires_at, paid_with
                      FROM orders WHERE merchant_id = ? AND order_id = ?
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, orderId.value());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    String paidWith = rs.getString("paid_with");
                    return Optional.of(new OrderState(orderId, OrderStatus.valueOf(rs.getString("status")),
                            rs.getLong("version"), Optional.of(Money.ofPaise(rs.getLong("amount_paise"))),
                            instant(rs, "expires_at"),
                            paidWith == null ? Optional.empty() : Optional.of(PaymentId.of(paidWith))));
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("find order " + orderId, e);
            }
        }

        @Override
        public void save(MerchantId merchantId, OrderState state) {
            if (state.status() == OrderStatus.NONE) {
                throw new IllegalArgumentException("an order with no events is not stored");
            }
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    INSERT INTO orders (merchant_id, order_id, status, version, amount_paise, expires_at, paid_with)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (merchant_id, order_id) DO UPDATE SET
                        status = EXCLUDED.status, version = EXCLUDED.version, amount_paise = EXCLUDED.amount_paise,
                        expires_at = EXCLUDED.expires_at, paid_with = EXCLUDED.paid_with
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, state.orderId().value());
                ps.setString(3, state.status().name());
                ps.setLong(4, state.version());
                ps.setLong(5, state.amount().orElseThrow().paise());
                ps.setObject(6, ts(state.expiresAt().orElseThrow()));
                ps.setString(7, state.paidWith().map(PaymentId::value).orElse(null));
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("save order " + state.orderId(), e);
            }
        }

        @Override
        public void appendTransition(MerchantId merchantId, OrderTransition t) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    INSERT INTO order_transitions
                        (merchant_id, order_id, seq, from_status, to_status, version, rule, event_key, event_time)
                    SELECT ?, ?, coalesce(max(seq), 0) + 1, ?, ?, ?, ?, ?, ?
                      FROM order_transitions WHERE merchant_id = ? AND order_id = ?
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, t.orderId().value());
                ps.setString(3, t.from().name());
                ps.setString(4, t.to().name());
                ps.setLong(5, t.version());
                ps.setString(6, t.rule().name());
                ps.setString(7, t.eventKey());
                ps.setObject(8, ts(t.eventTime()));
                ps.setString(9, merchantId.value());
                ps.setString(10, t.orderId().value());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("append order transition " + t.orderId(), e);
            }
        }

        @Override
        public List<OrderTransition> transitions(MerchantId merchantId, OrderId orderId) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    SELECT from_status, to_status, version, rule, event_key, event_time FROM order_transitions
                     WHERE merchant_id = ? AND order_id = ? ORDER BY seq
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, orderId.value());
                List<OrderTransition> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new OrderTransition(orderId, OrderStatus.valueOf(rs.getString(1)),
                                OrderStatus.valueOf(rs.getString(2)), rs.getLong(3), OrderRule.valueOf(rs.getString(4)),
                                rs.getString(5), rs.getObject(6, OffsetDateTime.class).toInstant()));
                    }
                }
                return List.copyOf(out);
            } catch (SQLException e) {
                throw SqlErrors.translate("order transitions " + orderId, e);
            }
        }
    }

    static final class Partitions implements PartitionRepo {
        private final JdbcTx tx;

        Partitions(JdbcTx tx) {
            this.tx = tx;
        }

        @Override
        public long nextOffset(int partition) {
            try (PreparedStatement ps = tx.connection().prepareStatement(
                    "SELECT next_offset FROM partition_leases WHERE partition = ?")) {
                ps.setInt(1, partition);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getLong(1) : 0L;
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("next offset of partition " + partition, e);
            }
        }

        @Override
        public void commitOffset(int partition, long nextOffset) {
            long current = nextOffset(partition);
            if (nextOffset < current) {
                throw new IllegalArgumentException("offset of partition " + partition + " cannot move back from "
                        + current + " to " + nextOffset);
            }
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    INSERT INTO partition_leases (partition, next_offset) VALUES (?, ?)
                    ON CONFLICT (partition) DO UPDATE SET next_offset = EXCLUDED.next_offset
                    """)) {
                ps.setInt(1, partition);
                ps.setLong(2, nextOffset);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("commit offset of partition " + partition, e);
            }
        }
    }
}
