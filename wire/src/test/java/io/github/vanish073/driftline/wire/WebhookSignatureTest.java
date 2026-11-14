package io.github.vanish073.driftline.wire;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature.Failure;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** DESIGN §14 row 1: tampered body, wrong secret, stale or future t, rotated secret. */
class WebhookSignatureTest {

    private static final byte[] SECRET = "s".repeat(32).getBytes(StandardCharsets.UTF_8);
    private static final byte[] NEW_SECRET = "n".repeat(32).getBytes(StandardCharsets.UTF_8);
    private static final byte[] BODY = "{\"id\":\"evt_1\"}".getBytes(StandardCharsets.UTF_8);
    private static final Instant NOW = Instant.parse("2026-11-13T06:00:00Z");

    private static Optional<Failure> verify(String header, byte[] body, List<byte[]> secrets, Instant now) {
        return WebhookSignature.verify(header, body, secrets, now).failure();
    }

    @Test
    void matchesAnIndependentlyComputedHmac() {
        // HMAC-SHA256(key = "s" x 32, "1794549600." + body), computed with Python's hmac module on 2026-11-13.
        assertThat(WebhookSignature.sign(SECRET, NOW, BODY))
                .isEqualTo("t=1794549600,v1=4700138803dd5c6c920a6642d5750115525517ceade0e0b574f53285d0b5f3c3");
    }

    @Test
    void acceptsAFreshCorrectSignature() {
        assertThat(verify(WebhookSignature.sign(SECRET, NOW, BODY), BODY, List.of(SECRET), NOW.plusSeconds(10))).isEmpty();
    }

    @Test
    void rejectsATamperedBodyAndAWrongSecret() {
        String header = WebhookSignature.sign(SECRET, NOW, BODY);
        byte[] tampered = "{\"id\":\"evt_2\"}".getBytes(StandardCharsets.UTF_8);
        assertThat(verify(header, tampered, List.of(SECRET), NOW)).contains(Failure.NO_MATCH);
        assertThat(verify(header, BODY, List.of(NEW_SECRET), NOW)).contains(Failure.NO_MATCH);
    }

    @Test
    void enforcesTheReplayWindowBothWays() {
        String header = WebhookSignature.sign(SECRET, NOW, BODY);
        assertThat(verify(header, BODY, List.of(SECRET), NOW.plusSeconds(300))).isEmpty();
        assertThat(verify(header, BODY, List.of(SECRET), NOW.plusSeconds(301))).contains(Failure.OUTSIDE_REPLAY_WINDOW);
        assertThat(verify(header, BODY, List.of(SECRET), NOW.minusSeconds(301))).contains(Failure.OUTSIDE_REPLAY_WINDOW);
    }

    @Test
    void acceptsEitherSecretWhileRotatingAndAnyOfSeveralSignatures() {
        String signedWithNew = WebhookSignature.sign(NEW_SECRET, NOW, BODY);
        assertThat(verify(signedWithNew, BODY, List.of(SECRET, NEW_SECRET), NOW)).isEmpty();
        String both = signedWithNew + "," + WebhookSignature.sign(SECRET, NOW, BODY).substring("t=1794549600,".length())
                + ",v0=legacy";
        assertThat(verify(both, BODY, List.of(SECRET), NOW)).isEmpty();
    }

    @Test
    void rejectsMissingMalformedAndOversizedInput() {
        assertThat(verify(null, BODY, List.of(SECRET), NOW)).contains(Failure.MISSING);
        assertThat(verify("v1=abc", BODY, List.of(SECRET), NOW)).contains(Failure.MALFORMED);
        assertThat(verify("t=1794549600", BODY, List.of(SECRET), NOW)).contains(Failure.MALFORMED);
        assertThat(verify("t=1794549600,t=1794549600,v1=" + "0".repeat(64), BODY, List.of(SECRET), NOW)).contains(Failure.MALFORMED);
        assertThat(verify("t=-5,v1=" + "0".repeat(64), BODY, List.of(SECRET), NOW)).contains(Failure.MALFORMED);
        assertThat(verify("t=1794549600,v1=" + "A".repeat(64), BODY, List.of(SECRET), NOW)).contains(Failure.MALFORMED);
        byte[] huge = new byte[WebhookSignature.MAX_BODY_BYTES + 1];
        assertThat(verify(WebhookSignature.sign(SECRET, NOW, huge), huge, List.of(SECRET), NOW)).contains(Failure.TOO_LARGE);
    }
}
