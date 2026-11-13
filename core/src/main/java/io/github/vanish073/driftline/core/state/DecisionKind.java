package io.github.vanish073.driftline.core.state;

/** The four outcomes a state-machine rule can have. */
public enum DecisionKind {
    /** The event changed the state. */
    APPLIED,
    /** The event describes a stage the subject has already moved past; nothing changes. */
    IGNORED_STALE,
    /** The event repeats a fact already known (under a different event key); nothing changes. */
    IGNORED_DUPLICATE,
    /** The event contradicts what is known. The state is not advanced; the caller raises an anomaly. */
    CONFLICT
}
