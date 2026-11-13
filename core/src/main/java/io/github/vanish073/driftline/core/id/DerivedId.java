package io.github.vanish073.driftline.core.id;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * An identifier derived from content, never generated (DESIGN §4.1, D4):
 *
 * <pre>
 * DerivedId = base32(SHA-256(namespace || 0x00 || field1 || 0x00 || field2 ...))[0..26]
 * </pre>
 *
 * <p>Strings are encoded as UTF-8. Base32 is RFC 4648 in lowercase, without padding; 26 characters carry 130 bits.
 * Fields must be non-empty and must not contain {@code 0x00}, so the encoding is injective: two different
 * (namespace, fields) inputs never hash the same bytes. Retries and re-detections therefore produce the same id,
 * which is what makes idempotency part of the data.
 */
public record DerivedId(String value) {

    public static final int LENGTH = 26;

    public DerivedId {
        Objects.requireNonNull(value, "value");
        if (value.length() != LENGTH) {
            throw new IllegalArgumentException("a DerivedId has " + LENGTH + " characters, not " + value.length());
        }
        for (int i = 0; i < LENGTH; i++) {
            if (Base32.ALPHABET.indexOf(value.charAt(i)) < 0) {
                throw new IllegalArgumentException("a DerivedId uses only [a-z2-7]; bad character at index " + i);
            }
        }
    }

    /**
     * Derives the id for {@code namespace} and {@code fields}, in order.
     *
     * @param namespace a lowercase ASCII word such as {@code "break"} or {@code "refund"}: {@code [a-z][a-z0-9_-]*}
     * @param fields    non-empty strings without {@code 0x00}
     */
    public static DerivedId of(String namespace, String... fields) {
        requireNamespace(namespace);
        MessageDigest sha256 = sha256();
        sha256.update(namespace.getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < fields.length; i++) {
            String field = Objects.requireNonNull(fields[i], "field " + i);
            if (field.isEmpty()) {
                throw new IllegalArgumentException("field " + i + " is empty");
            }
            if (field.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("field " + i + " contains the 0x00 separator");
            }
            sha256.update((byte) 0);
            sha256.update(field.getBytes(StandardCharsets.UTF_8));
        }
        return new DerivedId(Base32.encode(sha256.digest()).substring(0, LENGTH));
    }

    private static void requireNamespace(String namespace) {
        Objects.requireNonNull(namespace, "namespace");
        if (namespace.isEmpty() || namespace.charAt(0) < 'a' || namespace.charAt(0) > 'z') {
            throw new IllegalArgumentException("namespace must start with a lowercase ASCII letter");
        }
        for (int i = 1; i < namespace.length(); i++) {
            char c = namespace.charAt(i);
            boolean allowed = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
            if (!allowed) {
                throw new IllegalArgumentException("namespace has a character outside [a-z0-9_-] at index " + i);
            }
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("every Java platform must provide SHA-256", impossible);
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
