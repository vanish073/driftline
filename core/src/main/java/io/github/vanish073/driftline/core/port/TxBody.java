package io.github.vanish073.driftline.core.port;

/** The work done inside one {@link Store} transaction. */
@FunctionalInterface
public interface TxBody<R> {

    R run(Tx tx);
}
