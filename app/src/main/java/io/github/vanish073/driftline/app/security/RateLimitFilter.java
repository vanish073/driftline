package io.github.vanish073.driftline.app.security;

import io.github.vanish073.driftline.core.event.Sha256;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Applies the {@link RateLimiter} (Shift 11, D96) ahead of token authentication: a refused request answers 429 with
 * {@code Retry-After} and never reaches the token lookup; an answer of 401 counts as a failed authentication for the
 * client's address. The client's address is the TCP peer's ({@code getRemoteAddr}); {@code X-Forwarded-For} is never
 * trusted, because anyone can send it. Behind a proxy every client then shares the proxy's address, so a deployment
 * behind one relies on the proxy's own limits for addresses and on this filter's per-credential limits.
 */
public final class RateLimitFilter extends OncePerRequestFilter {

    private static final Pattern WEBHOOK = Pattern.compile("^/v1/webhooks/gateway/([A-Za-z0-9._-]{1,64})$");

    private final RateLimiter limiter;
    private final Runnable onRefusal;

    public RateLimitFilter(RateLimiter limiter, Runnable onRefusal) {
        this.limiter = limiter;
        this.onRefusal = onRefusal;
    }

    /** What a request is limited by: its bearer token (hashed), else the webhook's merchant, else its address. */
    static String credential(HttpServletRequest request) {
        String auth = request.getHeader("Authorization");
        if (auth != null && !auth.isEmpty()) {
            return "bearer:" + Sha256.of(auth.getBytes(StandardCharsets.UTF_8)).hex().substring(0, 32);
        }
        Matcher m = WEBHOOK.matcher(request.getRequestURI());
        if (m.matches()) {
            return "webhook:" + m.group(1);
        }
        return "address:" + request.getRemoteAddr();
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String address = request.getRemoteAddr();
        Optional<RateLimiter.Refusal> refused = limiter.admit(address, credential(request));
        if (refused.isPresent()) {
            onRefusal.run();
            response.setStatus(429);
            response.setHeader("Retry-After", Long.toString(refused.get().retryAfterSeconds()));
            response.setContentType("application/json");
            response.getOutputStream().write(("{\"error\":\"" + refused.get().reason() + "; retry after "
                    + refused.get().retryAfterSeconds() + " s\"}").getBytes(StandardCharsets.UTF_8));
            return;
        }
        chain.doFilter(request, response);
        if (response.getStatus() == 401) {
            limiter.failedAuthentication(address);
        }
    }
}
