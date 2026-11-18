package io.github.vanish073.driftline.core.audit;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.port.AuditRepo;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** {@code verify-audit} (DESIGN §7.6): an intact chain passes; a changed, removed or re-pointed row is named. */
class AuditChainTest {

    static final Instant T0 = Instant.parse("2026-11-17T06:00:00Z");

    static AuditRepo.Row row(long seq, AuditRepo.Entry e, String prev) {
        return new AuditRepo.Row(seq, e, prev, AuditRepo.hash(prev, e));
    }

    static AuditRepo.Entry entry(String subject, String note) {
        return AuditRepo.Entry.of(T0, "ops/abc", "BREAK_RESOLVE_MANUAL", subject, Map.of("note", note));
    }

    final AuditRepo.Row r1 = row(1, entry("brk_1", "first"), AuditRepo.GENESIS);
    final AuditRepo.Row r2 = row(2, entry("brk_2", "second"), r1.hash());
    final AuditRepo.Row r3 = row(3, entry("brk_3", "third"), r2.hash());

    @Test
    void anIntactChainPassesAndAnEmptyOneToo() {
        assertThat(AuditChain.verify(List.of(r1, r2, r3))).isEqualTo(new AuditChain.Verdict(3, Optional.empty(), "intact"));
        assertThat(AuditChain.verify(List.of()).intact()).isTrue();
    }

    @Test
    void aRowChangedAfterItWasWrittenIsNamed() {
        AuditRepo.Row edited = new AuditRepo.Row(2, entry("brk_2", "second, edited"), r1.hash(), r2.hash());
        AuditChain.Verdict v = AuditChain.verify(List.of(r1, edited, r3));
        assertThat(v.firstBadSeq()).contains(2L);
        assertThat(v.reason()).contains("changed after it was written");
    }

    @Test
    void aRemovedRowIsNamedAndSoIsARowRePointedToHideIt() {
        assertThat(AuditChain.verify(List.of(r1, r3)).firstBadSeq()).as("a gap in the sequence").contains(3L);
        AuditRepo.Row renumbered = row(2, r3.entry(), r2.hash());   // seq 2 again, but it names the deleted row's hash
        assertThat(AuditChain.verify(List.of(r1, renumbered)).firstBadSeq()).as("re-pointed past the deleted row")
                .contains(2L);
    }

    @Test
    void reorderedRowsAreNamed() {
        assertThat(AuditChain.verify(List.of(r2, r1)).firstBadSeq()).contains(2L);
    }
}
