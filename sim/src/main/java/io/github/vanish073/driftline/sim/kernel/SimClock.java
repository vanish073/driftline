package io.github.vanish073.driftline.sim.kernel;

import io.github.vanish073.driftline.core.port.Clock;
import java.time.Instant;

/** The {@link Clock} port in the simulator: the scheduler's virtual time plus a per-node offset (skew, Shift 4). */
public final class SimClock implements Clock {

    private final Scheduler scheduler;
    private long offsetMillis;

    public SimClock(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void setOffsetMillis(long offsetMillis) {
        this.offsetMillis = offsetMillis;
    }

    @Override
    public Instant now() {
        return scheduler.instant().plusMillis(offsetMillis);
    }
}
