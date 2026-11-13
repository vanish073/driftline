package io.github.vanish073.driftline.core.port;

/**
 * Partition leases: owner, fencing epoch and the next Kafka offset. Mirrors {@code partition_leases} in DESIGN §12.
 *
 * <p>Empty until the shift that needs it. Every method added here lands in {@code InMemoryStore} and
 * {@code PostgresStore} in the same change, with a contract test (DESIGN §3.2, CLAUDE.md "Store parity").
 */
public interface PartitionRepo {
}
