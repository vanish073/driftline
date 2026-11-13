package io.github.vanish073.driftline.core.hooks;

import java.util.List;
import java.util.Objects;

/**
 * The set of canaries switched on for one run (DESIGN §11.10). Immutable; {@link #NONE} in production.
 *
 * <p>ArchUnit allows only the {@code sim} module (and tests) to build any other value: no class outside
 * {@code ..sim..} may call the constructor or {@link #of}. From Shift 6 the app also refuses to start unless it
 * holds {@code NONE}.
 */
public record Canaries(List<Canary> enabled) {

    public static final Canaries NONE = new Canaries(List.of());

    /** Stores the canaries in declaration order; a canary listed twice is rejected. */
    public Canaries {
        Objects.requireNonNull(enabled, "enabled");
        List<Canary> ordered = enabled.stream().map(c -> Objects.requireNonNull(c, "canary")).sorted().toList();
        if (ordered.stream().distinct().count() != ordered.size()) {
            throw new IllegalArgumentException("a canary is listed twice");
        }
        enabled = ordered;
    }

    public static Canaries of(Canary... canaries) {
        return new Canaries(List.of(canaries));
    }

    public boolean isEnabled(Canary canary) {
        return enabled.contains(canary);
    }

    public boolean isNone() {
        return enabled.isEmpty();
    }
}
