package io.github.vanish073.driftline.wire.settlement;

/**
 * A settlement file the strict parser rejected (DESIGN §6.1). The message names the record (1-based physical line)
 * and the rule that failed. Like the JSON codecs, it never echoes field values, because files arrive from outside and
 * messages end up in logs.
 */
public final class MalformedSettlementFile extends RuntimeException {

    private final int record;

    public MalformedSettlementFile(int record, String reason) {
        super(record > 0 ? "record " + record + ": " + reason : reason);
        this.record = record;
    }

    /** The 1-based physical line the problem is on, or 0 for a problem with the file as a whole. */
    public int record() {
        return record;
    }
}
