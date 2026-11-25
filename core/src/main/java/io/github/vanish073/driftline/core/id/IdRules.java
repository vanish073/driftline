package io.github.vanish073.driftline.core.id;

import java.util.Objects;

/**
 * Validation shared by every identifier and key. Messages name the field and the position of the problem, but
 * never echo the value, because identifiers arrive from outside and end up in logs.
 */
public final class IdRules {

    /** Longest identifier token: merchant, payment, order, file and call ids. */
    public static final int MAX_TOKEN_LENGTH = 64;

    /** Longest key: event keys and idempotency keys. */
    public static final int MAX_KEY_LENGTH = 255;

    private IdRules() {
    }

    /**
     * Returns {@code value} if it is 1 to 64 characters from {@code [A-Za-z0-9._-]}. The restricted alphabet
     * means a token can never contain the {@code 0x00} separator that {@link DerivedId} relies on.
     */
    public static String requireToken(String field, String value) {
        Objects.requireNonNull(value, field);
        if (value.isEmpty() || value.length() > MAX_TOKEN_LENGTH) {
            throw new IllegalArgumentException(field + " must be 1 to " + MAX_TOKEN_LENGTH
                    + " characters long, but has " + value.length());
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            boolean allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '.' || c == '_' || c == '-';
            if (!allowed) {
                throw new IllegalArgumentException(field + " has a character outside [A-Za-z0-9._-] at index " + i);
            }
        }
        return value;
    }

    /** Returns {@code value} if it is 1 to 255 printable ASCII characters (0x21 to 0x7E), so no spaces. */
    public static String requireKey(String field, String value) {
        Objects.requireNonNull(value, field);
        if (value.isEmpty() || value.length() > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException(field + " must be 1 to " + MAX_KEY_LENGTH
                    + " characters long, but has " + value.length());
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x21 || c > 0x7E) {
                throw new IllegalArgumentException(field + " has a non-printable or non-ASCII character at index " + i);
            }
        }
        return value;
    }

    /**
     * A key that arrives from outside (the {@code Idempotency-Key} header, a webhook's event id): 1 to 255 characters
     * from {@code [A-Za-z0-9._:-]}. Stricter than {@link #requireKey}, so markup, quotes, spaces and control characters
     * never become an event key that the ops API and the logs later show (Shift 11, D92). UUIDs and prefixed ids fit.
     */
    public static String requireExternalKey(String field, String value) {
        requireKey(field, value);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            boolean allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '.' || c == '_' || c == ':' || c == '-';
            if (!allowed) {
                throw new IllegalArgumentException(field + " has a character outside [A-Za-z0-9._:-] at index " + i);
            }
        }
        return value;
    }
}
