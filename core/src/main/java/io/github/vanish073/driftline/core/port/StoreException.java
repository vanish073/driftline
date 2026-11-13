package io.github.vanish073.driftline.core.port;

/** Base of the two failures a {@link Store} transaction may report. */
public abstract sealed class StoreException extends RuntimeException permits StoreUnavailable, StoreConflict {

    protected StoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
