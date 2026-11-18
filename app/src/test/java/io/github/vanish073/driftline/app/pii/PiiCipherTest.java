package io.github.vanish073.driftline.app.pii;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.store.postgres.CustomerVault;
import io.github.vanish073.driftline.wire.webhook.CustomerContact;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Customers' contacts encrypted for the vault (§14, D77): round trip, key rotation, row binding, blind index. */
class PiiCipherTest {

    static final MerchantId M = MerchantId.of("m_1");
    static final MerchantId OTHER = MerchantId.of("m_2");
    static final PaymentId P = PaymentId.of("pay_1");
    static final String K1 = Base64.getEncoder().encodeToString(bytes(1));
    static final String K2 = Base64.getEncoder().encodeToString(bytes(2));
    static final String INDEX = Base64.getEncoder().encodeToString(bytes(9));
    static final CustomerContact CONTACT = CustomerContact.of(Optional.of("buyer@okaxis"), Optional.of("9876543210"),
            Optional.of("buyer@example.com"));

    static byte[] bytes(int fill) {
        byte[] b = new byte[32];
        Arrays.fill(b, (byte) fill);
        return b;
    }

    @Test
    void aSealedContactHoldsNoPlaintextAndOpensToTheSameValues() {
        PiiCipher cipher = new PiiCipher("k1", Map.of("k1", K1), INDEX);
        CustomerVault.Sealed s = cipher.seal(M, P, CONTACT);
        for (byte[] value : new byte[][] {s.vpa().orElseThrow(), s.phone().orElseThrow(), s.email().orElseThrow()}) {
            String text = new String(value, StandardCharsets.ISO_8859_1);
            assertThat(text).doesNotContain("buyer").doesNotContain("9876543210");
        }
        assertThat(cipher.open(M, P, s)).isEqualTo(new PiiCipher.Opened(Optional.of("buyer@okaxis"),
                Optional.of("9876543210"), Optional.of("buyer@example.com")));
    }

    @Test
    void aValueSealedUnderTheOldKeyStillOpensAfterARotationAndNewValuesUseTheNewKey() {
        CustomerVault.Sealed old = new PiiCipher("k1", Map.of("k1", K1), INDEX).seal(M, P, CONTACT);
        PiiCipher rotated = new PiiCipher("k2", Map.of("k1", K1, "k2", K2), INDEX);
        assertThat(rotated.open(M, P, old).vpa()).contains("buyer@okaxis");
        CustomerVault.Sealed fresh = rotated.seal(M, P, CONTACT);
        assertThat(new String(fresh.vpa().orElseThrow(), StandardCharsets.US_ASCII)).as("the key id travels inside")
                .contains("k2");
        assertThatThrownBy(() -> new PiiCipher("k2", Map.of("k2", K2), INDEX).open(M, P, old))
                .as("a retired key that was removed no longer opens its values").isInstanceOf(RuntimeException.class);
    }

    @Test
    void aCiphertextMovedToAnotherPaymentOrFieldDoesNotOpen() {
        PiiCipher cipher = new PiiCipher("k1", Map.of("k1", K1), INDEX);
        CustomerVault.Sealed s = cipher.seal(M, P, CONTACT);
        assertThatThrownBy(() -> cipher.open(M, PaymentId.of("pay_2"), s)).isInstanceOf(RuntimeException.class);
        CustomerVault.Sealed swapped = new CustomerVault.Sealed(s.email(), Optional.empty(), Optional.empty(),
                s.vpaIndex());
        assertThatThrownBy(() -> cipher.open(M, P, swapped)).as("an email ciphertext in the VPA column")
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void theBlindIndexMatchesEqualVpasWithinAMerchantOnly() {
        PiiCipher cipher = new PiiCipher("k1", Map.of("k1", K1), INDEX);
        String index = cipher.vpaIndex(M, "buyer@okaxis");
        assertThat(index).matches("[0-9a-f]{64}").doesNotContain("buyer");
        assertThat(cipher.vpaIndex(M, "buyer@okaxis")).isEqualTo(index);
        assertThat(cipher.vpaIndex(OTHER, "buyer@okaxis")).as("not linkable across merchants").isNotEqualTo(index);
        assertThat(new PiiCipher("k1", Map.of("k1", K1), Base64.getEncoder().encodeToString(bytes(8)))
                .vpaIndex(M, "buyer@okaxis")).as("keyed: useless without the key").isNotEqualTo(index);
        assertThatThrownBy(() -> new PiiCipher("k1", Map.of("k1", K1), Base64.getEncoder().encodeToString(new byte[16])))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
