package io.github.vanish073.driftline.sandbox;

import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.wire.webhook.CustomerContact;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Optional;

/**
 * Fake customer contacts for the sandbox's payment webhooks (D77), so the Compose stack carries personal data the way a
 * real gateway's webhooks do and {@code scripts/pii-scan.sh} can prove none of it leaks into logs, Kafka or any table
 * but the vault. The contact is a function of the payment id ({@code buyerNNNNNN@okaxis}, {@code
 * buyerNNNNNN@example.com}, {@code 6000NNNNNN}), so every retry of a webhook carries the same one. These patterns are
 * what the scan looks for. The simulator never adds contacts: personal data stays out of the core and its tests.
 */
final class FakeContacts {

    private FakeContacts() {
    }

    static CustomerContact of(PaymentId payment) {
        String n = String.format(Locale.ROOT, "%06d", number(payment));
        return CustomerContact.of(Optional.of("buyer" + n + "@okaxis"), Optional.of("6000" + n),
                Optional.of("buyer" + n + "@example.com"));
    }

    /** The webhook with the payment's fake contact added if it is a payment event; any other body unchanged. */
    static byte[] addTo(byte[] body) {
        GatewayWebhook w = GatewayWebhookCodec.decode(body);
        if (!(w.payload() instanceof PaymentPayload p)) {
            return body;
        }
        return GatewayWebhookCodec.encode(w, Optional.of(of(p.paymentId())));
    }

    private static int number(PaymentId payment) {
        try {
            byte[] h = MessageDigest.getInstance("SHA-256").digest(payment.value().getBytes(StandardCharsets.UTF_8));
            int v = ((h[0] & 0xff) << 16) | ((h[1] & 0xff) << 8) | (h[2] & 0xff);
            return v % 1_000_000;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
