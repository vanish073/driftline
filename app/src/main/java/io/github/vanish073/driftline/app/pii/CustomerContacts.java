package io.github.vanish073.driftline.app.pii;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.store.postgres.CustomerVault;
import io.github.vanish073.driftline.wire.webhook.CustomerContact;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Customers' contact details (DESIGN §14, D77): ingest stores them encrypted, the ops API reads them back masked or
 * looks payments up by VPA through the blind index. Without personal-data keys ({@code driftline.pii.*}) storage is off:
 * contacts are discarded, never stored or logged, and lookups answer that the feature is not configured.
 */
public final class CustomerContacts {

    private final CustomerVault vault;
    private final Optional<PiiCipher> cipher;

    public CustomerContacts(CustomerVault vault, Optional<PiiCipher> cipher) {
        this.vault = Objects.requireNonNull(vault, "vault");
        this.cipher = Objects.requireNonNull(cipher, "cipher");
    }

    public boolean enabled() {
        return cipher.isPresent();
    }

    /**
     * Stores a payment's contact, encrypted, if storage is on; the first contact for a payment wins.
     *
     * @throws io.github.vanish073.driftline.core.port.StoreUnavailable if the vault cannot be reached
     */
    public void store(MerchantId m, PaymentId p, CustomerContact contact, Instant at) {
        if (cipher.isEmpty() || contact.isEmpty()) {
            return;
        }
        vault.store(m, p, cipher.get().seal(m, p, contact), at);
    }

    /** The payment's contact, decrypted: for masking, never for logs. Empty if none or storage is off. */
    public Optional<PiiCipher.Opened> find(MerchantId m, PaymentId p) {
        return cipher.flatMap(c -> vault.find(m, p).map(s -> c.open(m, p, s)));
    }

    /** The vault itself, for erasure (D99): erasing needs no key, only the payment ids. */
    public CustomerVault vault() {
        return vault;
    }

    /** The VPA's blind index for this merchant, if storage is on and the VPA is well-formed (D99). */
    public Optional<String> vpaIndex(MerchantId m, String vpa) {
        PiiCipher c = cipher.orElseThrow(() -> new IllegalStateException("personal data storage is not configured"));
        return CustomerContact.of(Optional.of(vpa), Optional.empty(), Optional.empty()).vpa()
                .map(v -> c.vpaIndex(m, v));
    }

    /** The merchant's payments whose customer used this VPA (normalised as a webhook's would be). */
    public List<PaymentId> paymentsByVpa(MerchantId m, String vpa) {
        PiiCipher c = cipher.orElseThrow(() -> new IllegalStateException("personal data storage is not configured"));
        Optional<String> normalised = CustomerContact.of(Optional.of(vpa), Optional.empty(), Optional.empty()).vpa();
        return normalised.map(v -> vault.paymentsByVpa(m, c.vpaIndex(m, v))).orElse(List.of());
    }
}
