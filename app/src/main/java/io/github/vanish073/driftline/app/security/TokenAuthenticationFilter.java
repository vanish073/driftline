package io.github.vanish073.driftline.app.security;

import io.github.vanish073.driftline.app.merchant.DevBootstrap;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Maps {@code Authorization: Bearer <token>} to a {@link TokenAuthentication} (§14): the token is hashed with SHA-256
 * and looked up in {@code api_tokens}; a revoked or unknown token authenticates nothing, and the request then fails as
 * unauthenticated (401) wherever authentication is required. Tokens are looked up on every request, so a revocation
 * takes effect at once.
 */
public final class TokenAuthenticationFilter extends OncePerRequestFilter {

    private static final String PREFIX = "Bearer ";
    private static final int MAX_TOKEN_LENGTH = 512;

    private final PostgresDirectory directory;
    private final Clock clock;

    public TokenAuthenticationFilter(PostgresDirectory directory, Clock clock) {
        this.directory = directory;
        this.clock = clock;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String header = request.getHeader("Authorization");
        if (header != null && header.startsWith(PREFIX) && header.length() > PREFIX.length()
                && header.length() <= PREFIX.length() + MAX_TOKEN_LENGTH) {
            Sha256 hash = DevBootstrap.tokenHash(header.substring(PREFIX.length()).trim());
            Optional<PostgresDirectory.ApiToken> token = directory.token(hash);
            if (token.isPresent() && token.get().activeAt(clock.now())) {
                TokenAuthentication auth = new TokenAuthentication(token.get().merchantId(), token.get().scopes(),
                        hash.hex().substring(0, 8));
                auth.setDetails(request.getRemoteAddr());
                SecurityContextHolder.getContext().setAuthentication(auth);
            }
        }
        chain.doFilter(request, response);
    }
}
