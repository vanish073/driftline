package io.github.vanish073.driftline.sim;

import io.github.vanish073.driftline.sim.invariant.Violation;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** The outcome of one seed: its trace digest, its counters and, if any, the first violated invariant. */
public record RunResult(long seed, Profile profile, long steps, long virtualMillis, String digest,
                        Optional<Violation> violation, Map<String, Long> stats, List<String> traceTail, List<String> trace) {

    public boolean passed() {
        return violation.isEmpty();
    }

    /** The line that reproduces this run (DESIGN §11.12). */
    public String repro() {
        return "./gradlew :sim:run --args=\"--seed " + seed + " --profile " + profile.id() + "\"";
    }
}
