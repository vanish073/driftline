package io.github.vanish073.driftline.testkit.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.DerivedId;
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
import io.github.vanish073.driftline.core.port.PartitionRepo;
import io.github.vanish073.driftline.core.port.QuarantineRepo;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.TxBody;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * The dedupe, payment, order, partition (with fencing) and quarantine part of the Store contract (DESIGN §3.2, §11.9). Runs against
 * {@code InMemoryStore} and {@code PostgresStore}. Every test works on its own merchant and partition number.
 */
public abstract class StateStoreContract {

    private static final Instant T0 = Instant.parse("2026-11-13T06:00:00.123456Z");
    private static final Sha256 HASH_A = Sha256.of("a".getBytes(StandardCharsets.UTF_8));
    private static final Sha256 HASH_B = Sha256.of("b".getBytes(StandardCharsets.UTF_8));

    private Store store;
    private MerchantId m;
    private int partition;

    protected abstract Store store();

    @BeforeEach
    void fresh(TestInfo test) {
        store = store();
        String name = getClass().getName() + "#" + test.getDisplayName();
        m = MerchantId.of("m-" + DerivedId.of("state-contract", name));
        partition = 1000 + Math.floorMod(name.hashCode(), 1_000_000);
    }

    private <R> R tx(TxBody<R> body) {
        return store.transact("contract", body);
    }

    private DedupeRepo.Claim claim(Source source, String key, Sha256 hash) {
        return tx(t -> t.dedupe().claim(m, source, key, hash, 0, 7));
    }

    private Optional<PaymentState> payment(PaymentId id) {
        return tx(t -> t.payments().find(m, id));
    }

    private Optional<OrderState> order(OrderId id) {
        return tx(t -> t.orders().find(m, id));
    }

    private List<PaymentTransition> paymentLog(PaymentId id) {
        return tx(t -> t.payments().transitions(m, id));
    }

    private long offset() {
        return tx(t -> t.partitions().nextOffset(partition));
    }

    @Test
    void dedupeClaimsAKeyOnceAndSpotsAReusedKey() {
        assertThat(claim(Source.GATEWAY, "evt_1", HASH_A)).isEqualTo(DedupeRepo.Claim.FIRST);
        assertThat(claim(Source.GATEWAY, "evt_1", HASH_A)).isEqualTo(DedupeRepo.Claim.DUPLICATE);
        assertThat(claim(Source.GATEWAY, "evt_1", HASH_B)).isEqualTo(DedupeRepo.Claim.KEY_REUSED);
        assertThat(claim(Source.MERCHANT, "evt_1", HASH_B)).as("the source is part of the key").isEqualTo(DedupeRepo.Claim.FIRST);
    }

    @Test
    void aRolledBackClaimIsForgotten() {
        assertThatThrownBy(() -> tx(t -> {
            t.dedupe().claim(m, Source.GATEWAY, "evt_rb", HASH_A, 0, 1);
            throw new IllegalStateException("crash");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(claim(Source.GATEWAY, "evt_rb", HASH_A)).isEqualTo(DedupeRepo.Claim.FIRST);
    }

    @Test
    void aPaymentRoundTripsWithEveryField() {
        PaymentId id = PaymentId.of("pay_1");
        assertThat(payment(id)).isEmpty();
        PaymentState late = new PaymentState(id, PaymentStatus.SUCCEEDED, Optional.of(OrderId.of("ord_1")),
                Optional.of(Money.ofPaise(49_900)), Optional.of(T0), Optional.of(T0.plusSeconds(5)), false);
        tx(t -> {
            t.payments().save(m, late);
            return null;
        });
        assertThat(payment(id)).contains(late);

        PaymentState conflicted = new PaymentState(id, PaymentStatus.SUCCEEDED, late.orderId(), late.amount(),
                late.failedAt(), late.successAt(), true);
        tx(t -> {
            t.payments().save(m, conflicted);
            return null;
        });
        assertThat(payment(id)).contains(conflicted);
        PaymentState pending = new PaymentState(PaymentId.of("pay_2"), PaymentStatus.PENDING, late.orderId(),
                late.amount(), Optional.empty(), Optional.empty(), false);
        tx(t -> {
            t.payments().save(m, pending);
            return null;
        });
        assertThat(payment(PaymentId.of("pay_2"))).contains(pending);
    }

    @Test
    void paymentTransitionsAreKeptInOrder() {
        PaymentId id = PaymentId.of("pay_1");
        PaymentTransition first = new PaymentTransition(id, PaymentStatus.NONE, PaymentStatus.FAILED,
                PaymentRule.FIRST_EVIDENCE, "evt_1", T0);
        PaymentTransition second = new PaymentTransition(id, PaymentStatus.FAILED, PaymentStatus.SUCCEEDED,
                PaymentRule.LATE_SUCCESS, "evt_2", T0.plusSeconds(3));
        tx(t -> {
            t.payments().appendTransition(m, first);
            return null;
        });
        tx(t -> {
            t.payments().appendTransition(m, second);
            return null;
        });
        assertThat(paymentLog(id)).containsExactly(first, second);
        assertThat(paymentLog(PaymentId.of("pay_other"))).isEmpty();
    }

    @Test
    void anOrderRoundTripsAndKeepsItsTransitions() {
        OrderId id = OrderId.of("ord_1");
        assertThat(order(id)).isEmpty();
        OrderState paid = new OrderState(id, OrderStatus.PAID, 3, Optional.of(Money.ofPaise(49_900)),
                Optional.of(T0.plusSeconds(900)), Optional.of(PaymentId.of("pay_1")));
        OrderTransition t1 = new OrderTransition(id, OrderStatus.NONE, OrderStatus.PAID, 3, OrderRule.FIRST_EVIDENCE,
                "idem_3", T0);
        tx(t -> {
            t.orders().save(m, paid);
            t.orders().appendTransition(m, t1);
            return null;
        });
        assertThat(order(id)).contains(paid);
        OrderState created = new OrderState(OrderId.of("ord_2"), OrderStatus.CREATED, 1, Optional.of(Money.ofPaise(1)),
                Optional.of(T0), Optional.empty());
        tx(t -> {
            t.orders().save(m, created);
            return null;
        });
        assertThat(order(OrderId.of("ord_2"))).contains(created);
        List<OrderTransition> log = tx(t -> t.orders().transitions(m, id));
        assertThat(log).containsExactly(t1);
    }

    private PartitionRepo.Lease acquire(String owner) {
        return tx(t -> t.partitions().acquire(partition, owner));
    }

    private List<QuarantineRepo.Undecodable> undecodable(int p) {
        return tx(t -> t.quarantine().undecodable(p));
    }

    private List<QuarantineRepo.Entry> quarantined(MerchantId merchant) {
        return tx(t -> t.quarantine().forMerchant(merchant));
    }

    private void commit(long epoch, long next) {
        tx(t -> {
            t.partitions().commitOffset(partition, epoch, next);
            return null;
        });
    }

    @Test
    void offsetsStartAtZeroAndOnlyMoveForward() {
        assertThat(offset()).isZero();
        long epoch = acquire("matcher-a.1").epoch();
        commit(epoch, 5);
        assertThat(offset()).isEqualTo(5);
        commit(epoch, 5);
        assertThatThrownBy(() -> commit(epoch, 4)).isInstanceOf(IllegalArgumentException.class);
        assertThat(offset()).isEqualTo(5);
    }

    @Test
    void anUnacquiredPartitionHasEpochZeroAndNoOwner() {
        PartitionRepo.Lease none = tx(t -> t.partitions().lease(partition));
        assertThat(none).isEqualTo(new PartitionRepo.Lease(partition, 0, Optional.empty(), 0));
        assertThatThrownBy(() -> commit(1, 1)).as("no one holds epoch 1 yet").isInstanceOf(StoreConflict.class);
        assertThat(offset()).isZero();
    }

    @Test
    void acquireBumpsTheEpochAndKeepsTheOffset() {
        PartitionRepo.Lease first = acquire("matcher-a.1");
        assertThat(first).isEqualTo(new PartitionRepo.Lease(partition, 1, Optional.of("matcher-a.1"), 0));
        commit(1, 7);
        PartitionRepo.Lease second = acquire("matcher-b.1");
        assertThat(second).as("the new owner seeks to the committed offset")
                .isEqualTo(new PartitionRepo.Lease(partition, 2, Optional.of("matcher-b.1"), 7));
        PartitionRepo.Lease stored = tx(t -> t.partitions().lease(partition));
        assertThat(stored).isEqualTo(second);
    }

    @Test
    void aZombieIsFencedAndItsWholeTransactionRollsBack() {
        long zombie = acquire("matcher-a.1").epoch();
        commit(zombie, 3);
        long owner = acquire("matcher-b.1").epoch();
        PaymentId id = PaymentId.of("pay_zombie");
        PaymentState pending = new PaymentState(id, PaymentStatus.PENDING, Optional.of(OrderId.of("o")),
                Optional.of(Money.ofPaise(1)), Optional.empty(), Optional.empty(), false);
        assertThatThrownBy(() -> tx(t -> {
            t.dedupe().claim(m, Source.GATEWAY, "evt_zombie", HASH_A, partition, 3);
            t.payments().save(m, pending);
            t.partitions().commitOffset(partition, zombie, 4);
            return null;
        })).isInstanceOf(StoreConflict.class);
        assertThat(offset()).isEqualTo(3);
        assertThat(payment(id)).as("nothing the zombie wrote survives").isEmpty();
        assertThat(claim(Source.GATEWAY, "evt_zombie", HASH_A)).isEqualTo(DedupeRepo.Claim.FIRST);
        commit(owner, 4);
        assertThat(offset()).isEqualTo(4);
        assertThatThrownBy(() -> commit(zombie, 9)).isInstanceOf(StoreConflict.class);
        assertThatThrownBy(() -> commit(owner + 1, 9)).as("a future epoch is not held either")
                .isInstanceOf(StoreConflict.class);
        assertThat(offset()).isEqualTo(4);
    }

    @Test
    void aRolledBackAcquireLeavesTheEpochAlone() {
        long epoch = acquire("matcher-a.1").epoch();
        assertThatThrownBy(() -> tx(t -> {
            t.partitions().acquire(partition, "matcher-b.1");
            throw new IllegalStateException("crash during assignment");
        })).isInstanceOf(IllegalStateException.class);
        long stored = tx(t -> t.partitions().lease(partition)).epoch();
        assertThat(stored).isEqualTo(epoch);
        commit(epoch, 2);
        assertThat(offset()).isEqualTo(2);
    }

    @Test
    void quarantineKeepsOneRowPerLogPositionInOrder() {
        QuarantineRepo.Entry late = new QuarantineRepo.Entry(partition, 9, m, Source.MERCHANT, "key_1", HASH_B,
                QuarantineRepo.Reason.IDEMPOTENCY_CONFLICT);
        QuarantineRepo.Entry early = new QuarantineRepo.Entry(partition, 4, m, Source.MERCHANT, "key_1", HASH_A,
                QuarantineRepo.Reason.IDEMPOTENCY_CONFLICT);
        tx(t -> {
            t.quarantine().add(late);
            t.quarantine().add(early);
            t.quarantine().add(late);
            assertThat(t.quarantine().forMerchant(m)).as("visible inside the transaction").containsExactly(early, late);
            return null;
        });
        tx(t -> {
            t.quarantine().add(early);
            return null;
        });
        assertThat(quarantined(m)).containsExactly(early, late);
        assertThat(quarantined(MerchantId.of("m-nobody"))).isEmpty();
    }

    @Test
    void anUndecodableRecordKeepsItsBytesAndTakesItsLogPositionOnce() {
        QuarantineRepo.Undecodable garbage = new QuarantineRepo.Undecodable(partition, 7, "not valid JSON",
                new byte[] {(byte) 0xff, 0x00, 0x7b});
        QuarantineRepo.Undecodable earlier = new QuarantineRepo.Undecodable(partition, 3, "schema_version 9",
                "{}".getBytes(StandardCharsets.UTF_8));
        tx(t -> {
            t.quarantine().addUndecodable(garbage);
            t.quarantine().addUndecodable(earlier);
            t.quarantine().addUndecodable(new QuarantineRepo.Undecodable(partition, 7, "a second try", new byte[] {1}));
            assertThat(t.quarantine().undecodable(partition)).as("visible inside the transaction")
                    .containsExactly(earlier, garbage);
            return null;
        });
        // A reused-key row for a position an undecodable record already holds changes nothing, and vice versa.
        tx(t -> {
            t.quarantine().add(new QuarantineRepo.Entry(partition, 7, m, Source.GATEWAY, "evt_x", HASH_A,
                    QuarantineRepo.Reason.IDEMPOTENCY_CONFLICT));
            t.quarantine().addUndecodable(garbage);
            return null;
        });
        assertThat(undecodable(partition)).containsExactly(earlier, garbage);
        assertThat(undecodable(partition + 1)).isEmpty();
        assertThat(quarantined(m)).as("undecodable rows belong to no merchant").isEmpty();
        assertThat(undecodable(partition).get(1).raw())
                .containsExactly((byte) 0xff, 0x00, 0x7b);
    }

    @Test
    void aRolledBackUndecodableRecordIsForgotten() {
        assertThatThrownBy(() -> tx(t -> {
            t.quarantine().addUndecodable(new QuarantineRepo.Undecodable(partition, 1, "bad", new byte[] {2}));
            throw new IllegalStateException("crash before commit");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(undecodable(partition)).isEmpty();
    }

    @Test
    void aRolledBackQuarantineRowIsForgotten() {
        QuarantineRepo.Entry entry = new QuarantineRepo.Entry(partition, 1, m, Source.GATEWAY, "evt_q", HASH_A,
                QuarantineRepo.Reason.IDEMPOTENCY_CONFLICT);
        assertThatThrownBy(() -> tx(t -> {
            t.quarantine().add(entry);
            throw new IllegalStateException("crash before commit");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(quarantined(m)).isEmpty();
    }

    @Test
    void everythingInOneTransactionCommitsOrNothingDoes() {
        PaymentId id = PaymentId.of("pay_atomic");
        PaymentState pending = new PaymentState(id, PaymentStatus.PENDING, Optional.of(OrderId.of("o")),
                Optional.of(Money.ofPaise(1)), Optional.empty(), Optional.empty(), false);
        assertThatThrownBy(() -> tx(t -> {
            t.dedupe().claim(m, Source.GATEWAY, "evt_atomic", HASH_A, partition, 0);
            t.payments().save(m, pending);
            t.payments().appendTransition(m, new PaymentTransition(id, PaymentStatus.NONE, PaymentStatus.PENDING,
                    PaymentRule.FIRST_EVIDENCE, "evt_atomic", T0));
            long epoch = t.partitions().acquire(partition, "matcher-a.1").epoch();
            t.partitions().commitOffset(partition, epoch, 1);
            throw new IllegalStateException("crash before commit");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(payment(id)).isEmpty();
        assertThat(paymentLog(id)).isEmpty();
        assertThat(offset()).isZero();
        assertThat(claim(Source.GATEWAY, "evt_atomic", HASH_A)).isEqualTo(DedupeRepo.Claim.FIRST);
    }
}
