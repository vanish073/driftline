package io.github.vanish073.driftline.core.audit;

import io.github.vanish073.driftline.core.port.AuditRepo;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * {@code driftline verify-audit} (DESIGN §7.6), sans-I/O: walks the audit trail from its first row and recomputes every
 * hash. Sequence numbers must run 1, 2, 3, … with no gap; each row must name the previous row's hash
 * ({@link AuditRepo#GENESIS} for the first); and each stored hash must equal the hash of the previous hash and the
 * row's own content. Privileges and triggers stop the application from editing the trail; this check catches an edit
 * made around them (by a superuser who disabled the triggers, or on a restored copy), because changing any row breaks
 * its hash, and removing one breaks the chain.
 */
public final class AuditChain {

    /** What the walk found: how many rows it checked, and the first one that is wrong, if any. */
    public record Verdict(long rows, Optional<Long> firstBadSeq, String reason) {
        public Verdict {
            Objects.requireNonNull(firstBadSeq, "firstBadSeq");
            Objects.requireNonNull(reason, "reason");
        }

        public boolean intact() {
            return firstBadSeq.isEmpty();
        }
    }

    private AuditChain() {
    }

    /** @param rows every row, by sequence number, as {@link AuditRepo#rows()} returns them */
    public static Verdict verify(List<AuditRepo.Row> rows) {
        String prev = AuditRepo.GENESIS;
        long expected = 1;
        for (AuditRepo.Row r : rows) {
            if (r.seq() != expected) {
                return new Verdict(rows.size(), Optional.of(r.seq()), "row " + r.seq() + " follows row "
                        + (expected - 1) + ": a row is missing");
            }
            if (!r.prevHash().equals(prev)) {
                return new Verdict(rows.size(), Optional.of(r.seq()), "row " + r.seq()
                        + " does not name the previous row's hash");
            }
            if (!r.hash().equals(AuditRepo.hash(prev, r.entry()))) {
                return new Verdict(rows.size(), Optional.of(r.seq()), "row " + r.seq()
                        + "'s content does not match its hash: it was changed after it was written");
            }
            prev = r.hash();
            expected++;
        }
        return new Verdict(rows.size(), Optional.empty(), "intact");
    }
}
