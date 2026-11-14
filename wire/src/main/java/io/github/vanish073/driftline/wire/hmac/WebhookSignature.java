package io.github.vanish073.driftline.wire.hmac;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Webhook signatures (DESIGN §10.2). Header {@code Driftline-Signature: t=<unix seconds>,v1=<hex>[,v1=<hex>]}. The
 * signed payload is {@code t + "." + rawBody}, under HMAC-SHA256 with the merchant's secret. Each delivery attempt is
 * signed with a fresh {@code t}; replays inside the window pass here and are absorbed by dedupe on the event id.
 *
 * <p>Verification happens on the raw bytes, before any JSON parsing (D21), compares in constant time
 * ({@link MessageDigest#isEqual}), enforces the replay window and the body limit, and accepts any of the merchant's
 * active secrets, so a secret can be rotated without downtime.
 */
public final class WebhookSignature {

    public static final String HEADER = "Driftline-Signature";
    public static final Duration REPLAY_WINDOW = Duration.ofSeconds(300);
    public static final int MAX_BODY_BYTES = 64 * 1024;

    /** Why a signature was rejected. All of them answer 401, except TOO_LARGE, which answers 413. */
    public enum Failure {
        TOO_LARGE,
        MISSING,
        MALFORMED,
        OUTSIDE_REPLAY_WINDOW,
        NO_MATCH
    }

    /** The outcome of {@link #verify}: empty when the signature is valid. */
    public record Verification(Optional<Failure> failure) {
        public boolean valid() {
            return failure.isEmpty();
        }

        static Verification ok() {
            return new Verification(Optional.empty());
        }

        static Verification fail(Failure f) {
            return new Verification(Optional.of(f));
        }
    }

    private WebhookSignature() {
    }

    /** The header value for one delivery attempt at {@code timestamp}. */
    public static String sign(byte[] secret, Instant timestamp, byte[] body) {
        return "t=" + timestamp.getEpochSecond() + ",v1=" + HexFormat.of().formatHex(hmac(secret, timestamp.getEpochSecond(), body));
    }

    public static Verification verify(String header, byte[] body, List<byte[]> secrets, Instant now) {
        Objects.requireNonNull(body, "body");
        Objects.requireNonNull(secrets, "secrets");
        Objects.requireNonNull(now, "now");
        if (body.length > MAX_BODY_BYTES) {
            return Verification.fail(Failure.TOO_LARGE);
        }
        if (header == null || header.isEmpty()) {
            return Verification.fail(Failure.MISSING);
        }
        long timestamp = -1;
        List<byte[]> candidates = new ArrayList<>();
        for (String part : header.split(",", -1)) {
            int eq = part.indexOf('=');
            if (eq <= 0) {
                return Verification.fail(Failure.MALFORMED);
            }
            String key = part.substring(0, eq);
            String value = part.substring(eq + 1);
            if (key.equals("t")) {
                if (timestamp >= 0 || !isDigits(value) || value.length() > 12) {
                    return Verification.fail(Failure.MALFORMED);
                }
                timestamp = Long.parseLong(value);
            } else if (key.equals("v1")) {
                if (value.length() != 64 || !isLowerHex(value)) {
                    return Verification.fail(Failure.MALFORMED);
                }
                candidates.add(HexFormat.of().parseHex(value));
            }
            // Other schemes are ignored, so new ones can be added without breaking old verifiers.
        }
        if (timestamp < 0 || candidates.isEmpty()) {
            return Verification.fail(Failure.MALFORMED);
        }
        long skew = Math.abs(Math.subtractExact(now.getEpochSecond(), timestamp));
        if (skew > REPLAY_WINDOW.toSeconds()) {
            return Verification.fail(Failure.OUTSIDE_REPLAY_WINDOW);
        }
        boolean match = false;
        for (byte[] secret : secrets) {
            byte[] expected = hmac(secret, timestamp, body);
            for (byte[] candidate : candidates) {
                match |= MessageDigest.isEqual(expected, candidate);   // no early exit: constant time per pair
            }
        }
        return match ? Verification.ok() : Verification.fail(Failure.NO_MATCH);
    }

    private static byte[] hmac(byte[] secret, long timestamp, byte[] body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            mac.update(Long.toString(timestamp).getBytes(StandardCharsets.US_ASCII));
            mac.update((byte) '.');
            mac.update(body);
            return mac.doFinal();
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HmacSHA256 is required on every Java platform", e);
        }
    }

    private static boolean isDigits(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) < '0' || s.charAt(i) > '9') {
                return false;
            }
        }
        return true;
    }

    private static boolean isLowerHex(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                return false;
            }
        }
        return true;
    }
}
