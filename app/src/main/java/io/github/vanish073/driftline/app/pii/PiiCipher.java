package io.github.vanish073.driftline.app.pii;

import io.github.vanish073.driftline.app.merchant.SecretCipher;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.store.postgres.CustomerVault;
import io.github.vanish073.driftline.wire.webhook.CustomerContact;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Encrypts customers' contact details for the vault (DESIGN §14, D77): AES-256-GCM through {@link SecretCipher}, with a
 * keyring of its own (a key id inside each ciphertext, so keys rotate: new values use the active key, old ones still
 * decrypt), separate from the webhook-secret keys so one leaked key exposes one kind of data. Each field is bound to its
 * payment and field name ({@code pii/vpa/m/p}), so a ciphertext copied to another row or column does not decrypt. The
 * VPA's blind index is HMAC-SHA256 under a separate key over the merchant and the normalised VPA: equal VPAs of one
 * merchant match, but an index reveals nothing and does not link a customer across merchants.
 */
public final class PiiCipher {

    private final SecretCipher cipher;
    private final SecretKeySpec indexKey;

    /** @param indexKey base64, at least 32 bytes */
    public PiiCipher(String activeKid, Map<String, String> keys, String indexKey) {
        this.cipher = new SecretCipher(activeKid, keys);
        byte[] k;
        try {
            k = Base64.getDecoder().decode(indexKey.trim());
        } catch (IllegalArgumentException notBase64) {
            throw new IllegalArgumentException("the blind-index key is not base64");
        }
        if (k.length < 32) {
            throw new IllegalArgumentException("the blind-index key must be at least 32 bytes");
        }
        this.indexKey = new SecretKeySpec(k, "HmacSHA256");
    }

    /** Contact details in clear, after decryption: only for masking or an authorised read, never for logs. */
    public record Opened(Optional<String> vpa, Optional<String> phone, Optional<String> email) {
        @Override
        public String toString() {
            return "Opened[vpa=" + vpa.isPresent() + ", phone=" + phone.isPresent() + ", email=" + email.isPresent()
                    + "]";
        }
    }

    public CustomerVault.Sealed seal(MerchantId m, PaymentId p, CustomerContact c) {
        return new CustomerVault.Sealed(c.vpa().map(v -> encrypt("vpa", m, p, v)),
                c.phone().map(v -> encrypt("phone", m, p, v)), c.email().map(v -> encrypt("email", m, p, v)),
                c.vpa().map(v -> vpaIndex(m, v)));
    }

    public Opened open(MerchantId m, PaymentId p, CustomerVault.Sealed s) {
        return new Opened(s.vpa().map(v -> decrypt("vpa", m, p, v)), s.phone().map(v -> decrypt("phone", m, p, v)),
                s.email().map(v -> decrypt("email", m, p, v)));
    }

    /** The blind index of a VPA as {@link CustomerContact} normalises it. */
    public String vpaIndex(MerchantId m, String normalisedVpa) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(indexKey);
            return HexFormat.of().formatHex(mac.doFinal(("vpa|" + m.value() + "|" + normalisedVpa)
                    .getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is unavailable", e);
        }
    }

    private byte[] encrypt(String field, MerchantId m, PaymentId p, String value) {
        return cipher.encrypt(value.getBytes(StandardCharsets.UTF_8), context(field, m, p));
    }

    private String decrypt(String field, MerchantId m, PaymentId p, byte[] value) {
        return new String(cipher.decrypt(value, context(field, m, p)), StandardCharsets.UTF_8);
    }

    private static String context(String field, MerchantId m, PaymentId p) {
        return "pii/" + field + "/" + m.value() + "/" + p.value();
    }
}
