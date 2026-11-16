package io.github.vanish073.driftline.app.port;

import io.github.vanish073.driftline.core.port.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * The production {@link Clock} (DESIGN §3.2): this node's wall clock, truncated to microseconds because PostgreSQL
 * stores microseconds and a stored instant must equal the one the core saw (D31).
 */
public final class SystemClock implements Clock {

    private final java.time.Clock wall;

    public SystemClock() {
        this(java.time.Clock.systemUTC());
    }

    public SystemClock(java.time.Clock wall) {
        this.wall = wall;
    }

    @Override
    public Instant now() {
        return wall.instant().truncatedTo(ChronoUnit.MICROS);
    }
}
