package io.github.vanish073.driftline.sim.fault;

/**
 * The faults the injector can switch on (DESIGN §11.5). The rest of the catalogue (NET_PARTITION, CALL_AMBIGUOUS,
 * WEBHOOK_LOST and STATUS_STALE) arrives with the dispatcher it targets (Shift 8).
 *
 * <p>A <em>rate</em> fault is decided per message or per request, with a probability in permille. An <em>event</em>
 * fault is scheduled at random times of the RUN phase, a few times per traffic day. The settlement-file faults are
 * decided per file and model the bank's feed, not Driftline's infrastructure, so they keep happening after HEAL.
 */
public enum Fault {
    /** A message is lost (rate). */
    NET_DROP(true),
    /** A message is delivered twice (rate). */
    NET_DUP(true),
    /** A message is delayed by a heavy-tailed amount, which also reorders (rate). */
    NET_DELAY(true),
    /** A node crashes, losing its in-memory state and scheduled work, and restarts after a downtime (event). */
    NODE_CRASH(false),
    /** A matcher freezes; if the freeze outlasts the session timeout its partitions move and it wakes as a zombie (event). */
    NODE_PAUSE(false),
    /** Every Driftline node runs with its own fixed clock offset (applied once, at the start). */
    CLOCK_SKEW(false),
    /** A node's clock steps forward, or a little backward, until NTP corrects it (event). */
    CLOCK_JUMP(false),
    /** Every store transaction fails with StoreUnavailable for a window (event). */
    STORE_DOWN(false),
    /** The log append succeeds but the ack is lost, so ingest answers 503 and the sender retries (rate). */
    PRODUCE_AMBIGUOUS(true),
    /** A merchant's buggy client sends a new version under the previous version's idempotency key (rate). */
    MERCHANT_KEY_REUSE(true),
    /** A settlement file is held back for 6 to 36 hours (rate, per file). */
    FILE_LATE(true),
    /** A settlement file is uploaded a second time, byte for byte (rate, per file). */
    FILE_DUP(true),
    /** After a file was accepted, the bank uploads a different file for the same date (rate, per file). */
    FILE_CONFLICT(true);

    private final boolean rate;

    Fault(boolean rate) {
        this.rate = rate;
    }

    public boolean isRate() {
        return rate;
    }
}
