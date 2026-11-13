package io.github.vanish073.driftline.core.port;

/**
 * The transaction lost a race (a unique key, an optimistic version or a fencing epoch) and was rolled back.
 * DESIGN §3.2 calls this {@code Conflict}; it is named {@code StoreConflict} so it never clashes with
 * {@code Decision.Conflict} (D27).
 */
public final class StoreConflict extends StoreException {

    public StoreConflict(String message) {
        super(message, null);
    }

    public StoreConflict(String message, Throwable cause) {
        super(message, cause);
    }
}
