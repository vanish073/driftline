package io.github.vanish073.driftline.sim.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.port.BreakRepo;
import io.github.vanish073.driftline.core.resolver.ManualResolution;
import io.github.vanish073.driftline.core.resolver.Resolver;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** A person closes an ESCALATED break (DESIGN §8.1, §12): one transition, the plan closed, one hash-chained audit row. */
class ManualResolutionTest {

    static final MerchantId M = MerchantId.of("m_0");
    static final MerchantId OTHER = MerchantId.of("m_1");
    static final Instant T0 = Instant.parse("2026-11-13T06:00:00Z");

    final InMemoryStore store = new InMemoryStore();

    ManualResolution.Result manual(String breakId, Optional<MerchantId> scope, String actor, String note, Instant at) {
        return store.transact("manual", tx -> ManualResolution.resolve(tx, breakId, scope, actor, note, at));
    }

    /** A MISSING_SETTLEMENT, which the resolver escalates at once (§8.2). */
    Break escalated() {
        Break b = Break.detected(M, BreakType.MISSING_SETTLEMENT, "pay_1", T0, Map.of("due", "2026-11-14"));
        store.transact("raise", tx -> tx.breaks().raise(b, "detected"));
        store.transact("resolve", tx -> new Resolver().step(tx, T0.plusSeconds(1), 10));
        assertThat(store.transact("read", tx -> tx.breaks().byId(b.breakId())).orElseThrow().state())
                .isEqualTo(BreakState.ESCALATED);
        return b;
    }

    @Test
    void anEscalatedBreakIsResolvedByHandWithItsAuditRow() {
        Break b = escalated();
        Instant at = T0.plusSeconds(3_600);
        ManualResolution.Result r = manual(b.breakId(),
                Optional.empty(), "ops/abc123", "bank confirmed the payout by phone", at);

        assertThat(r).isInstanceOfSatisfying(ManualResolution.Resolved.class, done -> {
            assertThat(done.breakId()).isEqualTo(b.breakId());
            assertThat(done.at()).isEqualTo(at);
            assertThat(done.auditSeq()).isEqualTo(1);
        });
        assertThat(store.transact("read", tx -> tx.breaks().byId(b.breakId())).orElseThrow().state())
                .isEqualTo(BreakState.RESOLVED_MANUAL);
        assertThat(store.transact("t", tx -> tx.breaks().transitions(M, b.breakId())).getLast().reason())
                .isEqualTo("resolved by hand: bank confirmed the payout by phone");
        BreakRepo.Plan plan = store.transact("plan", tx -> tx.breaks().planOf(M, b.breakId())).orElseThrow();
        assertThat(plan.resolvedAt()).contains(at);

        AuditRepo.Row row = store.committedAuditRows().getFirst();
        assertThat(row.entry().actor()).isEqualTo("ops/abc123");
        assertThat(row.entry().action()).isEqualTo(ManualResolution.ACTION);
        assertThat(row.entry().subject()).isEqualTo(b.breakId());
        assertThat(row.entry().details()).containsEntry("from", "ESCALATED").containsEntry("to", "RESOLVED_MANUAL")
                .containsEntry("type", "MISSING_SETTLEMENT").containsEntry("merchant_id", "m_0")
                .containsEntry("note", "bank confirmed the payout by phone");
        assertThat(row.prevHash()).isEqualTo(AuditRepo.GENESIS);
        assertThat(row.hash()).isEqualTo(AuditRepo.hash(AuditRepo.GENESIS, row.entry()));
    }

    @Test
    void aBreakThatIsNotEscalatedOrAlreadyClosedIsRefusedAndNothingIsWritten() {
        Break open = Break.detected(M, BreakType.DEBITED_NO_ORDER, "pay_9", T0, Map.of());
        store.transact("raise", tx -> tx.breaks().raise(open, "detected"));
        assertThat(manual(open.breakId(), Optional.empty(), "ops/x",
                "close it", T0)).isEqualTo(new ManualResolution.NotEscalated(BreakState.DETECTED));

        Break b = escalated();
        manual(b.breakId(), Optional.empty(), "ops/x", "first", T0);
        assertThat(manual(b.breakId(), Optional.empty(), "ops/y",
                "second", T0)).isEqualTo(new ManualResolution.NotEscalated(BreakState.RESOLVED_MANUAL));
        assertThat(store.committedAuditRows()).hasSize(1);
    }

    @Test
    void aMerchantTokenCannotResolveAnotherMerchantsBreakOrLearnThatItExists() {
        Break b = escalated();
        assertThat(manual(b.breakId(), Optional.of(OTHER),
                "merchant:m_1/x", "mine now", T0)).isEqualTo(new ManualResolution.NotFound());
        assertThat(manual("brk_nope", Optional.empty(), "ops/x",
                "nothing", T0)).isEqualTo(new ManualResolution.NotFound());
        assertThat(store.committedAuditRows()).isEmpty();
    }

    @Test
    void theNoteIsRequiredAndBoundedSoTheTransitionReasonAlwaysFits() {
        assertThatThrownBy(() -> ManualResolution.requireNote(" ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ManualResolution.requireNote("a\nb")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ManualResolution.requireNote("x".repeat(ManualResolution.MAX_NOTE + 1)))
                .isInstanceOf(IllegalArgumentException.class);
        Break b = escalated();
        String longest = "x".repeat(ManualResolution.MAX_NOTE);
        assertThat(manual(b.breakId(), Optional.empty(), "ops/x",
                longest, T0)).isInstanceOf(ManualResolution.Resolved.class);
    }
}
