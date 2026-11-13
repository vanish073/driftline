package io.github.vanish073.driftline.core.port;

/** The store could not be reached; nothing was committed and the caller may retry later. */
public final class StoreUnavailable extends StoreException {

    public StoreUnavailable(String message) {
        super(message, null);
    }

    public StoreUnavailable(String message, Throwable cause) {
        super(message, cause);
    }
}
