package io.github.vanish073.driftline.app.security;

import io.github.vanish073.driftline.core.event.Sha256;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Optional;

/**
 * Who may scrape the metrics endpoint (Shift 11, D98): a request whose {@code Authorization} header is exactly
 * {@code Bearer <driftline.metrics.token>}, compared in constant time over SHA-256 digests so neither the token's
 * content nor its length leaks through timing. With no token configured, nobody may.
 */
public final class MetricsAccess {

    private final Optional<byte[]> expected;

    public MetricsAccess(Optional<String> token) {
        this.expected = token.filter(t -> !t.isBlank())
                .map(t -> digest("Bearer " + t));
    }

    private static byte[] digest(String value) {
        return Sha256.of(value.getBytes(StandardCharsets.UTF_8)).hex().getBytes(StandardCharsets.US_ASCII);
    }

    public boolean allows(HttpServletRequest request) {
        String header = request.getHeader("Authorization");
        return expected.isPresent() && header != null && MessageDigest.isEqual(expected.get(), digest(header));
    }
}
