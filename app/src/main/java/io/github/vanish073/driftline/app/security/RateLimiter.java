package io.github.vanish073.driftline.app.security;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Request limits in front of the API (Shift 11, D96). Two mechanisms, both in memory and per process:
 * <ul>
 *   <li><b>Lockout after failed authentication</b>, per client address: {@code maxFailures} answers of 401 (an unknown,
 *       expired or revoked token, a bad webhook signature) within {@code failureWindow} lock that address out for
 *       {@code lockout}. A locked-out request is refused before its token is even looked up, so guessing tokens or
 *       signatures gains nothing and costs the database nothing. Tokens are 256-bit random values, so guessing could
 *       not succeed anyway; this stops the attempt from being free.</li>
 *   <li><b>A token bucket per credential</b>: per bearer token, per webhook merchant, else per address. A leaked
 *       token, or a flood of webhooks, cannot take more than {@code requestsPerSecond} (bursts of {@code burst}). The
 *       limit is per credential rather than per address because one gateway sends every merchant's webhooks from a
 *       few addresses, and the load test sends 100 events a second from one.</li>
 * </ul>
 * Each table keeps at most {@code maxTracked} keys, dropping the least recently used, so memory stays bounded whatever
 * the traffic. Someone with more addresses than {@code maxTracked} can push old entries out, but could equally just use
 * those addresses. Limits are per process: with N instances behind a load balancer a client gets up to N times the
 * rate, and a lockout holds on the instances that saw the failures.
 */
public final class RateLimiter {

    /** The limits; {@code enabled = false} turns both mechanisms off. */
    public record Limits(boolean enabled, int maxFailures, Duration failureWindow, Duration lockout,
                         int requestsPerSecond, int burst, int maxTracked) {
        public Limits {
            if (maxFailures < 1 || requestsPerSecond < 1 || burst < 1 || maxTracked < 1) {
                throw new IllegalArgumentException("rate limits must be positive");
            }
            if (failureWindow.isNegative() || failureWindow.isZero() || lockout.isNegative() || lockout.isZero()) {
                throw new IllegalArgumentException("the failure window and the lockout must be positive");
            }
        }
    }

    /** A refusal: how long to wait, rounded up to whole seconds for {@code Retry-After}. */
    public record Refusal(String reason, long retryAfterSeconds) {
    }

    private static final class Failures {
        long windowStart;
        int count;
        long lockedUntil;
    }

    private static final class Bucket {
        double tokens;
        long refilledAt;
    }

    private final Limits limits;
    private final LongSupplier nanos;
    private final Map<String, Failures> failures;
    private final Map<String, Bucket> buckets;

    public RateLimiter(Limits limits, LongSupplier nanos) {
        this.limits = limits;
        this.nanos = nanos;
        this.failures = lru(limits.maxTracked());
        this.buckets = lru(limits.maxTracked());
    }

    private static <V> Map<String, V> lru(int max) {
        return new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, V> eldest) {
                return size() > max;
            }
        };
    }

    public boolean enabled() {
        return limits.enabled();
    }

    /** Before the request: refused if {@code address} is locked out, or {@code credential} has no tokens left. */
    public synchronized java.util.Optional<Refusal> admit(String address, String credential) {
        if (!limits.enabled()) {
            return java.util.Optional.empty();
        }
        long now = nanos.getAsLong();
        Failures f = failures.get(address);
        if (f != null && now - f.lockedUntil < 0) {
            return java.util.Optional.of(new Refusal("locked out after repeated failed authentication",
                    ceilSeconds(f.lockedUntil - now)));
        }
        Bucket b = buckets.get(credential);
        if (b == null) {
            b = new Bucket();
            b.tokens = limits.burst();
            b.refilledAt = now;
            buckets.put(credential, b);
        } else {
            double refill = (now - b.refilledAt) / 1e9 * limits.requestsPerSecond();
            b.tokens = Math.min(limits.burst(), b.tokens + refill);
            b.refilledAt = now;
        }
        if (b.tokens < 1) {
            return java.util.Optional.of(new Refusal("too many requests for this credential",
                    ceilSeconds((long) ((1 - b.tokens) / limits.requestsPerSecond() * 1e9))));
        }
        b.tokens -= 1;
        return java.util.Optional.empty();
    }

    /** After a request answered 401: one more failure for {@code address}, which may lock it out. */
    public synchronized void failedAuthentication(String address) {
        if (!limits.enabled()) {
            return;
        }
        long now = nanos.getAsLong();
        Failures f = failures.computeIfAbsent(address, a -> new Failures());
        if (f.count == 0 || now - f.windowStart >= limits.failureWindow().toNanos()) {
            f.windowStart = now;
            f.count = 0;
        }
        f.count++;
        if (f.count >= limits.maxFailures()) {
            f.lockedUntil = now + limits.lockout().toNanos();
            f.count = 0;
        }
    }

    /** How many addresses and credentials are tracked now (bounded by {@code maxTracked}). */
    public synchronized int trackedKeys() {
        return failures.size() + buckets.size();
    }

    private static long ceilSeconds(long nanos) {
        return Math.max(1, (nanos + 999_999_999L) / 1_000_000_000L);
    }
}
