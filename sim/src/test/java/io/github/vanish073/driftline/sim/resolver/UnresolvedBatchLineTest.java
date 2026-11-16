package io.github.vanish073.driftline.sim.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.OutboxRepo.Command;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandKind;
import io.github.vanish073.driftline.core.port.SettlementRepo;
import io.github.vanish073.driftline.core.resolver.Resolver;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Shift 7 carry-over (D56): an unreferenced line batch matching could not resolve is escalated at once, an ambiguous
 * one with its two witnesses in the escalation; a referenced orphan still waits for attribution until escalate_by.
 */
class UnresolvedBatchLineTest {

    static final MerchantId M = MerchantId.of("m_0");
    static final Instant T0 = Instant.parse("2026-11-13T06:00:00Z");
    static final LocalDate DAY = LocalDate.parse("2026-11-13");

    final InMemoryStore store = new InMemoryStore();

    BreakState review(Break b) {
        store.transact("resolve", tx -> new Resolver().step(tx, T0.plusSeconds(1), 10));
        return store.transact("read", tx -> tx.breaks().byId(b.breakId())).orElseThrow().state();
    }

    Break orphan(String line, Map<String, String> evidence) {
        Break b = Break.detected(M, BreakType.ORPHAN_SETTLEMENT, line, T0, evidence);
        store.transact("raise", tx -> tx.breaks().raise(b, "unreferenced line"));
        return b;
    }

    @Test
    void anAmbiguousLineIsEscalatedAtOnceWithItsTwoWitnesses() {
        SettlementRepo.Line line = new SettlementRepo.Line(M, "stl_1", 2, DAY, DAY, Money.ofPaise(97_444),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.of("UTR1"),
                SettlementRepo.LineStatus.AWAITING_BATCH)
                .ambiguous(List.of(PaymentId.of("pay_3"), PaymentId.of("pay_4")),
                        List.of(PaymentId.of("pay_5"), PaymentId.of("pay_6")));
        store.transact("line", tx -> {
            tx.settlement().saveLine(line);
            return null;
        });
        Break b = orphan("stl_1:2", Map.of("reason", "AMBIGUOUS", "amount", "974.44", "candidates", "4"));

        assertThat(review(b)).isEqualTo(BreakState.ESCALATED);
        String why = store.transact("t", tx -> tx.breaks().transitions(M, b.breakId())).getLast().reason();
        assertThat(why).isEqualTo("unreferenced line AMBIGUOUS; two subsets fit, for example pay_3+pay_4 and pay_5+pay_6");
        Command notice = store.committedCommands().stream().filter(c -> c.kind() == CommandKind.ESCALATION_NOTIFY)
                .findFirst().orElseThrow();
        assertThat(notice.args()).containsEntry("reason", why);
    }

    @Test
    void everyOtherUnresolvedOutcomeIsEscalatedAtOnceToo() {
        for (String reason : List.of("NO_SOLUTION", "BUDGET_EXCEEDED", "OUT_OF_RANGE", "ZERO_AMOUNT")) {
            Break b = orphan("stl_" + reason + ":1", Map.of("reason", reason, "amount", "1.00", "candidates", "0"));
            assertThat(review(b)).as(reason).isEqualTo(BreakState.ESCALATED);
        }
    }

    @Test
    void aReferencedOrphanStillWaitsForAttribution() {
        Break b = orphan("stl_9:1", Map.of("reason", "UNKNOWN_REFERENCE", "reference", "pay_77", "amount", "1.00"));
        assertThat(review(b)).isEqualTo(BreakState.AWAITING_EVIDENCE);
    }
}
