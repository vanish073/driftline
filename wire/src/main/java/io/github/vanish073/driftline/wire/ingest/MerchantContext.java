package io.github.vanish073.driftline.wire.ingest;

import io.github.vanish073.driftline.core.id.MerchantId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** What ingest knows about a merchant: its partition (D5) and its active webhook secrets (one, or two while rotating). */
public record MerchantContext(MerchantId merchantId, int partition, List<byte[]> webhookSecrets) {

    public MerchantContext {
        Objects.requireNonNull(merchantId, "merchantId");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must not be negative");
        }
        List<byte[]> copies = new ArrayList<>();
        for (byte[] secret : webhookSecrets) {
            if (secret.length < 32) {
                throw new IllegalArgumentException("a webhook secret must be at least 32 bytes");
            }
            copies.add(secret.clone());
        }
        if (copies.isEmpty() || copies.size() > 2) {
            throw new IllegalArgumentException("a merchant has one active webhook secret, or two while rotating");
        }
        webhookSecrets = List.copyOf(copies);
    }

    @Override
    public List<byte[]> webhookSecrets() {
        return webhookSecrets.stream().map(byte[]::clone).toList();
    }
}
