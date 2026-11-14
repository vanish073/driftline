package io.github.vanish073.driftline.core.port;

/**
 * Consumer progress per log partition, stored with the state it describes (D2). Mirrors {@code partition_leases}
 * (§12); the owner and fencing epoch arrive in Shift 4 (§5.1).
 */
public interface PartitionRepo {

    /** The offset of the next record to process; 0 for a partition never committed. */
    long nextOffset(int partition);

    /**
     * Records that every record below {@code nextOffset} is processed.
     *
     * @throws IllegalArgumentException if {@code nextOffset} is below the stored value; progress never goes back
     */
    void commitOffset(int partition, long nextOffset);
}
