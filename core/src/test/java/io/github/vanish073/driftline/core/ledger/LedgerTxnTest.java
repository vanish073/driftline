package io.github.vanish073.driftline.core.ledger;

import static io.github.vanish073.driftline.core.ledger.AccountCode.BANK;
import static io.github.vanish073.driftline.core.ledger.AccountCode.GATEWAY_RECEIVABLE;
import static io.github.vanish073.driftline.core.ledger.AccountCode.SALES;
import static io.github.vanish073.driftline.core.ledger.AccountCode.SUSPENSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import net.jqwik.api.constraints.Size;
import org.junit.jupiter.api.Test;

/** Layer 1 of "the ledger rejects any unbalanced transaction" (DESIGN §7.3). */
class LedgerTxnTest {

    private static final MerchantId M = MerchantId.of("m_001");
    private static final MerchantId OTHER = MerchantId.of("m_002");
    private static final Cause CAUSE = new Cause(CauseType.MANUAL_ADJUSTMENT, "7");
    private static final Instant AT = Instant.parse("2026-11-13T06:00:00Z");

    private static Entry e(MerchantId m, AccountCode account, long paise) {
        return new Entry(m, account, Money.ofPaise(paise));
    }

    private static LedgerRejected.Reason rejection(List<Entry> entries) {
        try {
            LedgerTxn.of(M, CAUSE, AT, entries);
        } catch (LedgerRejected rejected) {
            return rejected.reason();
        }
        throw new AssertionError("accepted " + entries);
    }

    @Test
    void rejectsANonZeroSum() {
        assertThat(rejection(List.of(e(M, BANK, 100), e(M, SALES, -99)))).isEqualTo(LedgerRejected.Reason.UNBALANCED);
        assertThat(rejection(List.of(e(M, BANK, 100), e(M, SALES, 100)))).isEqualTo(LedgerRejected.Reason.UNBALANCED);
    }

    @Test
    void rejectsASingleEntryAndAnEmptyTransaction() {
        assertThat(rejection(List.of(e(M, BANK, 100)))).isEqualTo(LedgerRejected.Reason.TOO_FEW_ENTRIES);
        assertThat(rejection(List.of())).isEqualTo(LedgerRejected.Reason.TOO_FEW_ENTRIES);
    }

    @Test
    void rejectsAZeroEntry() {
        assertThat(rejection(List.of(e(M, BANK, 100), e(M, SALES, -100), e(M, SUSPENSE, 0))))
                .isEqualTo(LedgerRejected.Reason.ZERO_ENTRY);
    }

    @Test
    void rejectsMixedMerchants() {
        assertThat(rejection(List.of(e(M, BANK, 100), e(OTHER, SALES, -100)))).isEqualTo(LedgerRejected.Reason.MIXED_MERCHANTS);
    }

    @Test
    void rejectsOverflowWhateverTheEntryOrder() {
        long max = Long.MAX_VALUE;
        List<Entry> entries = List.of(e(M, BANK, max), e(M, SUSPENSE, 1), e(M, SALES, -max), e(M, GATEWAY_RECEIVABLE, -1));
        List<Entry> reordered = List.of(entries.get(0), entries.get(2), entries.get(1), entries.get(3));
        assertThat(rejection(entries)).isEqualTo(LedgerRejected.Reason.OVERFLOW);
        assertThat(rejection(reordered)).as("a running total would not overflow in this order").isEqualTo(LedgerRejected.Reason.OVERFLOW);
    }

    @Test
    void acceptsTheLargestBalancedAmounts() {
        LedgerTxn txn = LedgerTxn.of(M, CAUSE, AT, List.of(e(M, BANK, Long.MAX_VALUE), e(M, SALES, -Long.MAX_VALUE)));
        assertThat(txn.entries()).hasSize(2);
    }

    @Test
    void entriesAreCanonicalSoBuildOrderDoesNotMatter() {
        LedgerTxn a = LedgerTxn.of(M, CAUSE, AT, List.of(e(M, SALES, -100), e(M, BANK, 60), e(M, SUSPENSE, 40)));
        LedgerTxn b = LedgerTxn.of(M, CAUSE, AT, List.of(e(M, SUSPENSE, 40), e(M, SALES, -100), e(M, BANK, 60)));
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a.entries()).extracting(Entry::account).containsExactly(BANK, SALES, SUSPENSE);
    }

    @Test
    void sameEntriesIgnoresPostedAtButEqualityDoesNot() {
        LedgerTxn now = LedgerTxn.of(M, CAUSE, AT, List.of(e(M, BANK, 1), e(M, SALES, -1)));
        LedgerTxn later = LedgerTxn.of(M, CAUSE, AT.plusSeconds(60), List.of(e(M, BANK, 1), e(M, SALES, -1)));
        assertThat(now.sameEntriesAs(later)).isTrue();
        assertThat(now).isNotEqualTo(later);
        LedgerTxn different = LedgerTxn.of(M, CAUSE, AT, List.of(e(M, BANK, 2), e(M, SALES, -2)));
        assertThat(now.sameEntriesAs(different)).isFalse();
    }

    @Test
    void theIdIsDerivedFromMerchantAndCause() {
        LedgerTxn txn = LedgerTxn.of(M, CAUSE, AT, List.of(e(M, BANK, 1), e(M, SALES, -1)));
        assertThat(txn.id()).isEqualTo(DerivedId.of("ledger", "m_001", "MANUAL_ADJUSTMENT", "7"));
    }

    @Test
    void postedAtKeepsMicrosecondsLikePostgres() {
        LedgerTxn txn = LedgerTxn.of(M, CAUSE, Instant.parse("2026-11-13T06:00:00.123456789Z"),
                List.of(e(M, BANK, 1), e(M, SALES, -1)));
        assertThat(txn.postedAt()).isEqualTo(Instant.parse("2026-11-13T06:00:00.123456Z"));
    }

    @Test
    void uncheckedSkipsValidationButLedgerChecksStillRejects() {
        List<Entry> unbalanced = List.of(e(M, BANK, 100), e(M, SALES, -90));
        LedgerTxn txn = LedgerTxn.unchecked(M, CAUSE, AT, unbalanced);
        assertThatThrownBy(() -> LedgerChecks.requireValid(txn.merchantId(), txn.entries()))
                .isInstanceOf(LedgerRejected.class).hasMessageStartingWith("UNBALANCED");
    }

    /** Any balanced set of non-zero entries is accepted; changing any one amount by one paisa is rejected. */
    @Property(tries = 2_000)
    void balancedIsAcceptedAndAnyOnePaisaOffIsRejected(
            @ForAll @Size(min = 1, max = 6) List<@LongRange(min = 1, max = 1_000_000_000_000L) Long> debits,
            @ForAll @IntRange(min = 0, max = 50) int which) {
        List<Entry> entries = new ArrayList<>();
        long total = 0;
        AccountCode[] codes = AccountCode.values();
        for (int i = 0; i < debits.size(); i++) {
            entries.add(e(M, codes[i % codes.length], debits.get(i)));
            total += debits.get(i);
        }
        entries.add(e(M, SUSPENSE, -total));
        assertThat(LedgerTxn.of(M, CAUSE, AT, entries).entries()).hasSize(entries.size());

        int index = which % entries.size();
        List<Entry> tampered = new ArrayList<>(entries);
        Entry target = tampered.get(index);
        long changed = target.amount().paise() + 1;
        if (changed == 0) {
            changed = target.amount().paise() - 1;
        }
        tampered.set(index, e(M, target.account(), changed));
        assertThat(rejection(tampered)).isEqualTo(LedgerRejected.Reason.UNBALANCED);
    }
}
