package io.github.vanish073.driftline.core.hooks;

/**
 * Called by core code when it reaches a {@link CrashPoint} (DESIGN §9.4). Production wires {@link #NONE}, which does
 * nothing. The simulator and the crash integration tests wire an implementation that throws at a chosen point.
 * The calls are placed in Shift 8.
 */
@FunctionalInterface
public interface CrashPoints {

    CrashPoints NONE = point -> { };

    void reached(CrashPoint point);
}
