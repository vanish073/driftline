package io.github.vanish073.driftline.core.breaks;

import io.github.vanish073.driftline.core.id.IdRules;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * One row of {@code break_transitions} (DESIGN §8.1): append-only, numbered from 1 per break. The first row of every
 * break has no {@code from}.
 */
public record BreakTransition(String breakId, int seq, Optional<BreakState> from, BreakState to, Instant at,
                              String reason) {

    public BreakTransition {
        IdRules.requireToken("breakId", breakId);
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(at, "at");
        Break.requireEvidence("reason", reason);
        if (seq < 1) {
            throw new IllegalArgumentException("transition numbers start at 1, got " + seq);
        }
        if ((seq == 1) != from.isEmpty()) {
            throw new IllegalArgumentException("exactly the first transition has no from-state");
        }
    }
}
