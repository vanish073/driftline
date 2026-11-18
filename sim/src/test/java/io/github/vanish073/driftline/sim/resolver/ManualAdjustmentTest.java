package io.github.vanish073.driftline.sim.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.audit.AuditChain;
import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.Cause;
import io.github.vanish073.driftline.core.ledger.CauseType;
import io.github.vanish073.driftline.core.ledger.LedgerRejected;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.resolver.ManualAdjustment;
import io.github.vanish073.driftline.core.resolver.ManualResolution;
import io.github.vanish073.driftline.core.resolver.Resolver;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/** A person posts a balanced correcting entry (DESIGN §7, D8, D74): the audit row and the ledger entry, or nothing. */
class ManualAdjustmentTest {

    static final MerchantId M = MerchantId.of("m_0");
    static final MerchantId OTHER = MerchantId.of("m_1");
    static final Instant T0 = Instant.parse("2026-11-17T06:00:00Z");

    final InMemoryStore store = new InMemoryStore();

    static SortedMap<AccountCode, Money> amounts(Object... pairs) {
        TreeMap<AccountCode, Money> out = new TreeMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put((AccountCode) pairs[i], Money.parseSigned((String) pairs[i + 1]));
        }
        return out;
    }

    ManualAdjustment.Result post(MerchantId m, Optional<String> breakId, SortedMap<AccountCode, Money> amounts) {
        return store.transact("adjust", tx -> ManualAdjustment.post(tx, m, "ops/abc", "bank charged a fee we had not "
                + "booked", breakId, amounts, T0));
    }

    Money balance(AccountCode account) {
        return store.transact("bal", tx -> tx.ledger().balance(M, account));
    }

    @Test
    void aBalancedAdjustmentPostsItsLedgerEntryUnderItsAuditRow() {
        ManualAdjustment.Result r = post(M, Optional.empty(),
                amounts(AccountCode.SETTLEMENT_DIFFERENCES, "12.34", AccountCode.BANK, "-12.34"));
        assertThat(r).isInstanceOfSatisfying(ManualAdjustment.Posted.class, p -> {
            assertThat(p.auditSeq()).isEqualTo(1);
            assertThat(p.txn().cause()).isEqualTo(new Cause(CauseType.MANUAL_ADJUSTMENT, "1"));
        });
        assertThat(balance(AccountCode.BANK)).isEqualTo(Money.parseSigned("-12.34"));
        assertThat(balance(AccountCode.SETTLEMENT_DIFFERENCES)).isEqualTo(Money.parseSigned("12.34"));
        AuditRepo.Row row = store.committedAuditRows().getFirst();
        assertThat(row.entry().action()).isEqualTo(ManualAdjustment.ACTION);
        assertThat(row.entry().subject()).isEqualTo("m_0");
        assertThat(row.entry().details()).containsEntry("entries", "BANK=-12.34;SETTLEMENT_DIFFERENCES=12.34")
                .containsEntry("note", "bank charged a fee we had not booked").doesNotContainKey("break_id");
    }

    @Test
    void anUnbalancedOrOneSidedAdjustmentIsRefusedAndNothingIsWritten() {
        assertThatThrownBy(() -> post(M, Optional.empty(), amounts(AccountCode.BANK, "-12.34",
                AccountCode.SETTLEMENT_DIFFERENCES, "12.00"))).isInstanceOf(LedgerRejected.class);
        assertThatThrownBy(() -> post(M, Optional.empty(), amounts(AccountCode.BANK, "-12.34")))
                .isInstanceOf(LedgerRejected.class);
        assertThatThrownBy(() -> store.transact("adjust", tx -> ManualAdjustment.post(tx, M, "ops/abc", " ",
                Optional.empty(), amounts(AccountCode.BANK, "-1.00", AccountCode.SUSPENSE, "1.00"), T0)))
                .as("a note is required").isInstanceOf(IllegalArgumentException.class);
        assertThat(store.committedAuditRows()).isEmpty();
        assertThat(store.committedLedger()).isEmpty();
    }

    @Test
    void aBreakItCorrectsMustBeTheMerchantsAndTheChainStaysIntactAcrossManualActions() {
        Break b = Break.detected(M, BreakType.MISSING_SETTLEMENT, "pay_1", T0, Map.of());
        store.transact("raise", tx -> tx.breaks().raise(b, "detected"));
        store.transact("resolve", tx -> new Resolver().step(tx, T0.plusSeconds(1), 10));   // escalated at once
        store.transact("manual", tx -> ManualResolution.resolve(tx, b.breakId(), Optional.empty(), "ops/abc",
                "written off with the bank's agreement", T0.plusSeconds(2)));

        assertThat(post(OTHER, Optional.of(b.breakId()),
                amounts(AccountCode.SETTLEMENT_DIFFERENCES, "4.99", AccountCode.GATEWAY_RECEIVABLE, "-4.99")))
                .isEqualTo(new ManualAdjustment.BreakNotFound());
        assertThat(post(M, Optional.of(b.breakId()),
                amounts(AccountCode.SETTLEMENT_DIFFERENCES, "4.99", AccountCode.GATEWAY_RECEIVABLE, "-4.99")))
                .isInstanceOf(ManualAdjustment.Posted.class);
        assertThat(store.committedAuditRows()).hasSize(2);
        assertThat(store.committedAuditRows().get(1).entry().details()).containsEntry("break_id", b.breakId());
        assertThat(AuditChain.verify(store.committedAuditRows()).intact()).isTrue();
    }
}
