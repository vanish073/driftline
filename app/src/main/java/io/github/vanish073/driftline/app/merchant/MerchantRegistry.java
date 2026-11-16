package io.github.vanish073.driftline.app.merchant;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.matcher.MerchantDirectory;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The merchants this process serves, loaded once at start-up from PostgreSQL (DESIGN §12): the matcher's immutable
 * {@link MerchantDirectory} and ingest's {@link MerchantContext}s with decrypted webhook secrets. Every merchant must
 * have one active webhook secret, or two while rotating; anything else stops the start-up, because a merchant whose
 * webhooks cannot be verified would silently lose them.
 *
 * <p>Known limit (D49): a merchant onboarded later, or a rotated secret, is picked up on the next restart, and the
 * matcher must restart before ingest accepts the new merchant's events.
 */
public final class MerchantRegistry {

    private final MerchantDirectory directory;
    private final List<MerchantTerms> merchants;
    private final TreeMap<MerchantId, MerchantContext> contexts;

    private MerchantRegistry(List<MerchantTerms> merchants, TreeMap<MerchantId, MerchantContext> contexts) {
        this.merchants = List.copyOf(merchants);
        this.directory = new MerchantDirectory(merchants);
        this.contexts = contexts;
    }

    public static MerchantRegistry load(PostgresDirectory store, SecretCipher cipher, Instant now) {
        List<MerchantTerms> merchants = store.merchants();
        TreeMap<MerchantId, List<byte[]>> secrets = new TreeMap<>();
        for (PostgresDirectory.WebhookSecret s : store.activeSecrets(now)) {
            secrets.computeIfAbsent(s.merchantId(), m -> new ArrayList<>())
                    .add(cipher.decrypt(s.secretEnc(), context(s.merchantId(), s.kid())));
        }
        TreeMap<MerchantId, MerchantContext> contexts = new TreeMap<>();
        for (MerchantTerms t : merchants) {
            List<byte[]> active = secrets.getOrDefault(t.merchantId(), List.of());
            if (active.isEmpty() || active.size() > 2) {
                throw new IllegalStateException("merchant " + t.merchantId() + " has " + active.size()
                        + " active webhook secrets; it needs one, or two while rotating");
            }
            contexts.put(t.merchantId(), new MerchantContext(t.merchantId(), t.partition(), active));
        }
        return new MerchantRegistry(merchants, contexts);
    }

    /** The associated data of a stored secret: the ciphertext only decrypts for its own row. */
    public static String context(MerchantId merchantId, String kid) {
        return "webhook-secret/" + merchantId.value() + "/" + kid;
    }

    public MerchantDirectory directory() {
        return directory;
    }

    public List<MerchantTerms> merchants() {
        return merchants;
    }

    public Optional<MerchantContext> context(MerchantId merchantId) {
        return Optional.ofNullable(contexts.get(merchantId));
    }

    /** The highest partition any merchant uses, or -1 with no merchants. */
    public int maxPartition() {
        return merchants.stream().mapToInt(MerchantTerms::partition).max().orElse(-1);
    }
}
