package io.github.vanish073.driftline.sim;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.sim.invariant.Violation;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The outcome of one seed: its trace digest, its counters, its swarm vector and, if any, the first violated
 * invariant.
 */
public record RunResult(long seed, Profile profile, Canaries canaries, Optional<CrashPoint> forcedCrash, String swarm, long steps, long virtualMillis,
                        String digest, Optional<Violation> violation, Map<String, Long> stats, List<String> traceTail,
                        List<String> trace) {

    public boolean passed() {
        return violation.isEmpty();
    }

    /** The arguments that reproduce this run. */
    public String args() {
        StringBuilder args = new StringBuilder("--seed " + seed + " --profile " + profile.id());
        for (Canary c : canaries.enabled()) {
            args.append(" --canary ").append(c.code());
        }
        forcedCrash.ifPresent(cp -> args.append(" --crash ").append(cp.name()));
        return args.toString();
    }

    /** The line that reproduces this run (DESIGN §11.12). */
    public String repro() {
        return "./gradlew :sim:run --args=\"" + args() + "\"";
    }
}
