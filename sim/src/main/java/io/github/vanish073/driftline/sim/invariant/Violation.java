package io.github.vanish073.driftline.sim.invariant;

/** A broken invariant: its id from DESIGN §11.7, what was seen, and when in virtual time. */
public record Violation(String invariant, String detail, long atMillis) {
}
