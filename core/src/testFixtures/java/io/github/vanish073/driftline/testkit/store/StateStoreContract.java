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
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.TxBody;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * The dedupe, payment, order and partition part of the Store contract (DESIGN §3.2, §11.9). Runs against
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

    @Test
    void offsetsStartAtZeroAndOnlyMoveForward() {
        assertThat(offset()).isZero();
        tx(t -> {
            t.partitions().commitOffset(partition, 5);
            return null;
        });
        assertThat(offset()).isEqualTo(5);
        tx(t -> {
            t.partitions().commitOffset(partition, 5);
            return null;
        });
        assertThatThrownBy(() -> tx(t -> {
            t.partitions().commitOffset(partition, 4);
            return null;
        })).isInstanceOf(IllegalArgumentException.class);
        assertThat(offset()).isEqualTo(5);
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
            t.partitions().commitOffset(partition, 1);
            throw new IllegalStateException("crash before commit");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(payment(id)).isEmpty();
        assertThat(paymentLog(id)).isEmpty();
        assertThat(offset()).isZero();
        assertThat(claim(Source.GATEWAY, "evt_atomic", HASH_A)).isEqualTo(DedupeRepo.Claim.FIRST);
    }
}
