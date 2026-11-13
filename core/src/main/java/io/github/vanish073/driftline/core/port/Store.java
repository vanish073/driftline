package io.github.vanish073.driftline.core.port;

/**
 * The system of record (DESIGN §3.2). {@code transact} runs {@code body} atomically: either every write in it
 * commits, or none does. The body must have no effects outside the transaction.
 *
 * <p>Production wires {@code PostgresStore}; the simulator wires {@code InMemoryStore}, which runs transactions
 * one at a time. An abstract contract test runs against both.
 */
public interface Store {

    /**
     * @throws StoreUnavailable if the store cannot be reached; nothing was committed
     * @throws StoreConflict    if the transaction lost a race (a unique key, a version check or a fencing check)
     */
    <R> R transact(String txName, TxBody<R> body);
}
