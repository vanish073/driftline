package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.id.IdRules;
import java.util.Objects;
import java.util.Optional;

/**
 * Partition ownership and consumer progress, stored with the state they describe (D2, DESIGN §5.1). Mirrors
 * {@code partition_leases} (§12).
 *
 * <p>Fencing: every owner takes the partition with {@link #acquire}, which bumps its epoch. Progress is committed only
 * under the epoch the committer holds, so an instance that was paused while its partition moved on (a zombie) cannot
 * commit anything: its whole transaction rolls back (INV-FENCED).
 */
public interface PartitionRepo {

    /**
     * One partition's lease.
     *
     * @param epoch      bumped by every {@link #acquire}; 0 for a partition never acquired
     * @param owner      the instance that acquired it last, if any
     * @param nextOffset the offset of the next record to process
     */
    record Lease(int partition, long epoch, Optional<String> owner, long nextOffset) {

        public Lease {
            if (partition < 0 || epoch < 0 || nextOffset < 0) {
                throw new IllegalArgumentException("partition, epoch and offset must not be negative");
            }
            Objects.requireNonNull(owner, "owner");
            owner.ifPresent(o -> IdRules.requireToken("owner", o));
        }
    }

    /**
     * Takes the partition for {@code owner}: bumps the epoch, records the owner and returns the new lease, whose
     * {@code nextOffset} is where the new owner must seek (§5.1). Any earlier holder is fenced from now on.
     */
    Lease acquire(int partition, String owner);

    /** The lease as stored now; epoch 0, no owner and offset 0 for a partition never acquired or committed. */
    Lease lease(int partition);

    /** The offset of the next record to process; 0 for a partition never committed. */
    long nextOffset(int partition);

    /**
     * Records that every record below {@code nextOffset} is processed, if {@code epoch} is still the partition's
     * epoch.
     *
     * @throws StoreConflict            if the partition has been acquired under a newer epoch (or never under this
     *                                  one): the caller is fenced and must roll back and drop the partition
     * @throws IllegalArgumentException if {@code nextOffset} is below the stored value; progress never goes back
     */
    void commitOffset(int partition, long epoch, long nextOffset);
}
