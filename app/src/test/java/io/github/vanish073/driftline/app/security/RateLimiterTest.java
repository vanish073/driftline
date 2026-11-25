package io.github.vanish073.driftline.app.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** D96: the lockout after failed authentication, the per-credential bucket, and the bound on what is remembered. */
class RateLimiterTest {

    final AtomicLong nanos = new AtomicLong(1_000_000_000_000L);

    RateLimiter limiter(int maxTracked) {
        return new RateLimiter(new RateLimiter.Limits(true, 5, Duration.ofSeconds(60), Duration.ofMinutes(5), 10, 20,
                maxTracked), nanos::get);
    }

    void advance(Duration d) {
        nanos.addAndGet(d.toNanos());
    }

    @Test
    void anAddressIsLockedOutAfterRepeatedFailuresForEveryCredentialUntilTheLockoutEnds() {
        RateLimiter l = limiter(100);
        for (int i = 0; i < 4; i++) {
            l.failedAuthentication("10.0.0.7");
        }
        assertThat(l.admit("10.0.0.7", "bearer:guess")).isEmpty();
        l.failedAuthentication("10.0.0.7");
        assertThat(l.admit("10.0.0.7", "bearer:the-real-token")).hasValueSatisfying(r -> {
            assertThat(r.reason()).contains("locked out");
            assertThat(r.retryAfterSeconds()).isEqualTo(300);
        });
        assertThat(l.admit("10.0.0.8", "bearer:guess")).as("another address is not affected").isEmpty();
        advance(Duration.ofMinutes(5).minusSeconds(1));
        assertThat(l.admit("10.0.0.7", "bearer:x")).hasValueSatisfying(r -> assertThat(r.retryAfterSeconds())
                .isEqualTo(1));
        advance(Duration.ofSeconds(1));
        assertThat(l.admit("10.0.0.7", "bearer:x")).as("the lockout ends").isEmpty();
    }

    @Test
    void failuresSpreadOverMoreThanTheWindowDoNotLockOut() {
        RateLimiter l = limiter(100);
        for (int i = 0; i < 20; i++) {
            l.failedAuthentication("10.0.0.7");
            advance(Duration.ofSeconds(15));   // four failures per minute
            if (i % 4 == 3) {
                advance(Duration.ofSeconds(1));
            }
        }
        assertThat(l.admit("10.0.0.7", "bearer:x")).isEmpty();
    }

    @Test
    void eachCredentialHasABucketThatRefillsAtTheRate() {
        RateLimiter l = limiter(100);
        for (int i = 0; i < 20; i++) {
            assertThat(l.admit("10.0.0.1", "bearer:a")).as("burst %d", i).isEmpty();
        }
        assertThat(l.admit("10.0.0.1", "bearer:a")).hasValueSatisfying(r -> {
            assertThat(r.reason()).contains("too many requests");
            assertThat(r.retryAfterSeconds()).isEqualTo(1);
        });
        assertThat(l.admit("10.0.0.1", "bearer:b")).as("another token from the same address").isEmpty();
        advance(Duration.ofMillis(100));   // 10 per second: one more
        assertThat(l.admit("10.0.0.1", "bearer:a")).isEmpty();
        assertThat(l.admit("10.0.0.1", "bearer:a")).isPresent();
        advance(Duration.ofSeconds(60));
        for (int i = 0; i < 20; i++) {
            assertThat(l.admit("10.0.0.1", "bearer:a")).as("refilled to the burst, no more").isEmpty();
        }
        assertThat(l.admit("10.0.0.1", "bearer:a")).isPresent();
    }

    @Test
    void whatIsRememberedStaysBoundedWhateverTheTraffic() {
        RateLimiter l = limiter(50);
        for (int i = 0; i < 10_000; i++) {
            l.failedAuthentication("10.1." + (i / 256) + "." + (i % 256));
            l.admit("10.1." + (i / 256) + "." + (i % 256), "bearer:" + i);
        }
        assertThat(l.trackedKeys()).isLessThanOrEqualTo(100);
    }

    @Test
    void disabledLimitsNeverRefuse() {
        RateLimiter l = new RateLimiter(new RateLimiter.Limits(false, 1, Duration.ofSeconds(60), Duration.ofMinutes(5),
                1, 1, 10), nanos::get);
        for (int i = 0; i < 100; i++) {
            l.failedAuthentication("10.0.0.7");
            assertThat(l.admit("10.0.0.7", "bearer:a")).isEmpty();
        }
    }
}
