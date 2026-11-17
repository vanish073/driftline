package io.github.vanish073.driftline.app.merchant;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Dev and test only (D49): onboards the merchants, webhook secrets and API tokens listed in a bootstrap file, which the
 * sandbox generates with fresh random secrets for each Compose run ({@code sandbox bootstrap}), so no secret is ever
 * committed. It runs only when {@code driftline.bootstrap-file} is set, and it is idempotent, so every role can run it.
 *
 * <pre>
 * {"format":"driftline-bootstrap-v1",
 *  "ops_token":"…", "service_token":"…",
 *  "merchants":{"m_e2e_01":{"partition":1,"fee_bps":200,"gst_bps":1800,"cycle_days":1,
 *                           "first_settlement_date":"2026-11-12","webhook_secret":"…","token":"…"}}}
 * </pre>
 *
 * Merchants are an object keyed by merchant id (no arrays), so the sandbox reads the same file with the strict parser
 * of {@code wire}. A merchant token gets {@code events:write}, {@code settlement:write} and {@code breaks:read} for its merchant; the
 * ops token gets {@code breaks:read} and {@code breaks:resolve} across merchants; the service token, which the dispatcher
 * sends to {@code /internal/v1/observations}, gets {@code observations:write} only.
 */
public final class DevBootstrap {

    public static final String FORMAT = "driftline-bootstrap-v1";
    public static final String SECRET_KID = "bootstrap";
    private static final Logger LOG = LoggerFactory.getLogger(DevBootstrap.class);

    private DevBootstrap() {
    }

    public static void apply(Path file, PostgresDirectory directory, SecretCipher cipher, Instant now) {
        JsonNode root;
        try {
            root = JsonMapper.builder().build().readTree(Files.readAllBytes(file));
        } catch (IOException e) {
            throw new IllegalStateException("cannot read the bootstrap file", e);
        }
        if (!FORMAT.equals(root.path("format").asString(""))) {
            throw new IllegalStateException("the bootstrap file is not " + FORMAT);
        }
        int onboarded = 0;
        for (Map.Entry<String, JsonNode> entry : root.path("merchants").properties()) {
            MerchantId id = MerchantId.of(entry.getKey());
            JsonNode m = entry.getValue();
            MerchantTerms terms = new MerchantTerms(id, m.path("partition").asInt(-1),
                    new FeeSchedule(m.path("fee_bps").asLong(-1), m.path("gst_bps").asLong(-1)),
                    m.path("cycle_days").asInt(-1), LocalDate.parse(text(m, "first_settlement_date")));
            if (directory.onboard(terms, now)) {
                onboarded++;
            }
            byte[] secret = text(m, "webhook_secret").getBytes(StandardCharsets.UTF_8);
            directory.addWebhookSecret(new PostgresDirectory.WebhookSecret(id, SECRET_KID,
                    cipher.encrypt(secret, MerchantRegistry.context(id, SECRET_KID)), now, Optional.empty()));
            directory.addToken(new PostgresDirectory.ApiToken(tokenHash(text(m, "token")), Optional.of(id),
                    List.of("events:write", "settlement:write", "breaks:read"), now, Optional.empty()));
        }
        directory.addToken(new PostgresDirectory.ApiToken(tokenHash(text(root, "ops_token")), Optional.empty(),
                List.of("breaks:read", "breaks:resolve"), now, Optional.empty()));
        directory.addToken(new PostgresDirectory.ApiToken(tokenHash(text(root, "service_token")), Optional.empty(),
                List.of("observations:write"), now, Optional.empty()));
        LOG.info("bootstrap file applied: {} merchants onboarded now, {} listed", onboarded, root.path("merchants").size());
    }

    /** Tokens are stored only as the SHA-256 of their UTF-8 bytes (§14). */
    public static Sha256 tokenHash(String token) {
        return Sha256.of(token.getBytes(StandardCharsets.UTF_8));
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isString() || value.asString().isEmpty()) {
            throw new IllegalStateException("the bootstrap file needs a non-empty string " + field);
        }
        return value.asString();
    }
}
