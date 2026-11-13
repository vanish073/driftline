package io.github.vanish073.driftline.core.event;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** A SHA-256 digest as 64 lowercase hex characters. */
public record Sha256(String hex) {

    public Sha256 {
        Objects.requireNonNull(hex, "hex");
        if (hex.length() != 64) {
            throw new IllegalArgumentException("a SHA-256 hex digest has 64 characters, not " + hex.length());
        }
        for (int i = 0; i < hex.length(); i++) {
            char c = hex.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                throw new IllegalArgumentException("a SHA-256 digest is lowercase hex; bad character at index " + i);
            }
        }
    }

    public static Sha256 of(byte[] bytes) {
        try {
            return new Sha256(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("every Java platform must provide SHA-256", impossible);
        }
    }

    @Override
    public String toString() {
        return hex;
    }
}
