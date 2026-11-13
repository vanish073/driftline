package io.github.vanish073.driftline.sim.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.PostingRules;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** What only the in-memory store has: the STORE_DOWN switch and its one-at-a-time transactions. */
class InMemoryStoreTest {

    private static final MerchantId M = MerchantId.of("m_001");
    private final InMemoryStore store = new InMemoryStore();

    @Test
    void whileDownNoTransactionRunsAndNothingIsLost() {
        store.transact("before", tx -> tx.ledger().post(
                PostingRules.capture(M, PaymentId.of("pay_1"), Money.ofPaise(100), Instant.EPOCH)));
        store.setUnavailable(true);
        boolean[] ran = {false};
        assertThatThrownBy(() -> store.transact("during", tx -> ran[0] = true)).isInstanceOf(StoreUnavailable.class);
        assertThat(ran[0]).isFalse();

        store.setUnavailable(false);
        Money after = store.transact("after", tx -> tx.ledger().balance(M, AccountCode.GATEWAY_RECEIVABLE));
        assertThat(after).isEqualTo(Money.ofPaise(100));
    }

    @Test
    void transactionsDoNotNest() {
        assertThatThrownBy(() -> store.transact("outer", tx -> store.transact("inner", inner -> null)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("do not nest");
        String next = store.transact("next", tx -> "fine");
        assertThat(next).isEqualTo("fine");
    }
}
