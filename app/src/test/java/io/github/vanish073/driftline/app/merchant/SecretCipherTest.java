package io.github.vanish073.driftline.app.merchant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Stored webhook secrets: AES-256-GCM, bound to their row, rotatable (D48). */
class SecretCipherTest {

    private static final String K1 = Base64.getEncoder().encodeToString(new byte[32]);
    private static final String K2 = Base64.getEncoder().encodeToString("0123456789abcdef0123456789abcdef"
            .getBytes(StandardCharsets.US_ASCII));
    private static final byte[] SECRET = "whsec_0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.US_ASCII);

    @Test
    void aSecretRoundTripsAndTheStoredValueIsNotThePlaintext() {
        SecretCipher cipher = new SecretCipher("k1", Map.of("k1", K1));
        byte[] stored = cipher.encrypt(SECRET, "webhook-secret/m_1/a");
        assertThat(new String(stored, StandardCharsets.ISO_8859_1)).doesNotContain("whsec_");
        assertThat(stored.length).isBetween(60, 1024);
        assertThat(cipher.decrypt(stored, "webhook-secret/m_1/a")).isEqualTo(SECRET);
        assertThat(cipher.encrypt(SECRET, "webhook-secret/m_1/a")).as("a fresh nonce every time").isNotEqualTo(stored);
    }

    @Test
    void aValueCopiedToAnotherRowOrTamperedWithDoesNotDecrypt() {
        SecretCipher cipher = new SecretCipher("k1", Map.of("k1", K1));
        byte[] stored = cipher.encrypt(SECRET, "webhook-secret/m_1/a");
        assertThatThrownBy(() -> cipher.decrypt(stored, "webhook-secret/m_2/a"))
                .hasMessageContaining("failed authentication");
        stored[stored.length - 1] ^= 1;
        assertThatThrownBy(() -> cipher.decrypt(stored, "webhook-secret/m_1/a"))
                .hasMessageContaining("failed authentication");
        assertThatThrownBy(() -> cipher.decrypt(new byte[10], "x")).hasMessageContaining("not a v1");
    }

    @Test
    void afterRotationOldValuesStillDecryptAndNewOnesUseTheNewKey() {
        byte[] old = new SecretCipher("k1", Map.of("k1", K1)).encrypt(SECRET, "c");
        SecretCipher rotated = new SecretCipher("k2", Map.of("k1", K1, "k2", K2));
        assertThat(rotated.decrypt(old, "c")).isEqualTo(SECRET);
        byte[] fresh = rotated.encrypt(SECRET, "c");
        assertThatThrownBy(() -> new SecretCipher("k1", Map.of("k1", K1)).decrypt(fresh, "c"))
                .hasMessageContaining("not configured");
    }

    @Test
    void badKeysAreRefusedAtStartUp() {
        assertThatThrownBy(() -> new SecretCipher("k1", Map.of("k1", "c2hvcnQ="))).hasMessageContaining("32 bytes");
        assertThatThrownBy(() -> new SecretCipher("k9", Map.of("k1", K1))).hasMessageContaining("active-kid");
        assertThatThrownBy(() -> new SecretCipher("", Map.of())).hasMessageContaining("active-kid");
    }
}
