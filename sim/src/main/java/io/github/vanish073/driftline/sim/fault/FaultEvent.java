package io.github.vanish073.driftline.sim.fault;

/** One scheduled event fault of the RUN phase (DESIGN §11.5), drawn before the run starts. */
public sealed interface FaultEvent {

    /** Virtual time at which the fault strikes. */
    long atMillis();

    /** {@code node} crashes; with {@code midBatch} a matcher dies inside a batch transaction, before commit. */
    record Crash(long atMillis, String node, long downtimeMillis, boolean midBatch) implements FaultEvent {
    }

    /** Matcher {@code node} freezes for {@code durationMillis}. */
    record Pause(long atMillis, String node, long durationMillis) implements FaultEvent {
    }

    /** The store refuses every transaction for {@code durationMillis}. */
    record StoreDown(long atMillis, long durationMillis) implements FaultEvent {
    }

    /** {@code node}'s clock steps by {@code stepMillis} (negative: backward) until NTP corrects it. */
    record ClockJump(long atMillis, String node, long stepMillis, long durationMillis) implements FaultEvent {
    }
}
