package io.github.vanish073.driftline.app.merchant;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Encrypts stored webhook secrets with AES-256-GCM from the JDK (DESIGN §12 {@code secret_enc}, D48). The ciphertext
 * carries its key id, so keys can be rotated: new values use the active key, old values still decrypt with theirs.
 * The associated data binds a ciphertext to its merchant and secret id, so a value copied to another row fails to
 * decrypt. Keys come from configuration (an environment variable or a Kubernetes Secret), never from the database.
 *
 * <p>Layout: {@code 0x01 | kid length | kid | 12-byte nonce | ciphertext + 16-byte tag}.
 */
public final class SecretCipher {

    private static final byte VERSION = 1;
    private static final int NONCE_BYTES = 12;
    private static final int TAG_BITS = 128;

    private final TreeMap<String, SecretKeySpec> keys = new TreeMap<>();
    private final String activeKid;
    private final SecureRandom random = new SecureRandom();

    /** @param keys base64 AES-256 keys by key id; {@code activeKid} must be one of them */
    public SecretCipher(String activeKid, Map<String, String> keys) {
        Objects.requireNonNull(activeKid, "activeKid");
        for (Map.Entry<String, String> e : keys.entrySet()) {
            byte[] key;
            try {
                key = Base64.getDecoder().decode(e.getValue().trim());
            } catch (IllegalArgumentException notBase64) {
                throw new IllegalArgumentException("secret key " + e.getKey() + " is not base64");
            }
            if (key.length != 32) {
                throw new IllegalArgumentException("secret key " + e.getKey() + " must be 32 bytes (AES-256)");
            }
            if (e.getKey().isEmpty() || e.getKey().length() > 64) {
                throw new IllegalArgumentException("a key id is 1 to 64 characters");
            }
            this.keys.put(e.getKey(), new SecretKeySpec(key, "AES"));
        }
        if (!this.keys.containsKey(activeKid)) {
            throw new IllegalArgumentException("driftline.secrets.active-kid must name one of the configured keys");
        }
        this.activeKid = activeKid;
    }

    public byte[] encrypt(byte[] plaintext, String context) {
        byte[] nonce = new byte[NONCE_BYTES];
        random.nextBytes(nonce);
        byte[] kid = activeKid.getBytes(StandardCharsets.US_ASCII);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, keys.get(activeKid), new GCMParameterSpec(TAG_BITS, nonce));
            cipher.updateAAD(context.getBytes(StandardCharsets.UTF_8));
            byte[] sealed = cipher.doFinal(plaintext);
            return ByteBuffer.allocate(2 + kid.length + NONCE_BYTES + sealed.length)
                    .put(VERSION).put((byte) kid.length).put(kid).put(nonce).put(sealed).array();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM is required on every Java platform", e);
        }
    }

    /** @throws IllegalArgumentException if the value is malformed, its key is unknown, or it was tampered with */
    public byte[] decrypt(byte[] value, String context) {
        ByteBuffer in = ByteBuffer.wrap(value);
        if (value.length < 2 + 1 + NONCE_BYTES + TAG_BITS / 8 || in.get() != VERSION) {
            throw new IllegalArgumentException("not a v1 secret ciphertext");
        }
        int kidLength = in.get() & 0xff;
        if (kidLength == 0 || in.remaining() < kidLength + NONCE_BYTES + TAG_BITS / 8) {
            throw new IllegalArgumentException("not a v1 secret ciphertext");
        }
        byte[] kid = new byte[kidLength];
        in.get(kid);
        SecretKeySpec key = keys.get(new String(kid, StandardCharsets.US_ASCII));
        if (key == null) {
            throw new IllegalArgumentException("the secret was encrypted with a key that is not configured");
        }
        byte[] nonce = new byte[NONCE_BYTES];
        in.get(nonce);
        byte[] sealed = new byte[in.remaining()];
        in.get(sealed);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, nonce));
            cipher.updateAAD(context.getBytes(StandardCharsets.UTF_8));
            return cipher.doFinal(sealed);
        } catch (javax.crypto.AEADBadTagException e) {
            throw new IllegalArgumentException("the secret ciphertext failed authentication");
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM is required on every Java platform", e);
        }
    }
}
