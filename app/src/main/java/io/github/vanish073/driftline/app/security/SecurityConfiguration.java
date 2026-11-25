package io.github.vanish073.driftline.app.security;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.config.DriftlineProperties;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.security.autoconfigure.actuate.web.servlet.EndpointRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter;

/**
 * Spring Security for the API (§10.1, §14): stateless, no sessions, no CSRF (no cookies or browser forms; every write is
 * authenticated by a bearer token or a webhook signature), no login forms. Gateway webhooks pass the filter chain
 * because their authentication is the HMAC signature, checked in the controller on the raw bytes (D21). Every other
 * route needs a token; which token may do what is decided by {@code @PreAuthorize} on each method. Health is open, for
 * the orchestrator's probes, and shows only UP or DOWN; metrics need Prometheus's own token (D98). Both are served only
 * on the management port, which is never exposed outside the cluster (D21).
 */
@Configuration
@Profile(Roles.SERVICE)
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfiguration {

    @Bean
    MerchantAccess merchantAccess() {
        return new MerchantAccess();
    }

    @Bean
    SecurityFilterChain api(HttpSecurity http, PostgresDirectory directory, Clock clock,
                            ObjectProvider<RateLimiter> limiter, ObjectProvider<MeterRegistry> meters,
                            ObjectProvider<DriftlineProperties> properties) throws Exception {
        MetricsAccess metrics = new MetricsAccess(Optional.ofNullable(properties.getIfAvailable())
                .map(p -> p.metrics().token()));
        http.csrf(c -> c.disable())
                .httpBasic(b -> b.disable())
                .formLogin(f -> f.disable())
                .logout(l -> l.disable())
                .requestCache(r -> r.disable())
                // Spring Security's defaults (nosniff, frame DENY, no-store) plus a policy that lets a browser load and
                // run nothing from an answer, and send no referrer (Shift 11, D92). No CORS configuration exists, so a
                // browser on another site gets no grant to call the API.
                .headers(h -> h
                        .contentSecurityPolicy(c -> c.policyDirectives("default-src 'none'; frame-ancestors 'none'"))
                        .referrerPolicy(r -> r.policy(ReferrerPolicyHeaderWriter.ReferrerPolicy.NO_REFERRER)))
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .addFilterBefore(new TokenAuthenticationFilter(directory, clock), AnonymousAuthenticationFilter.class)
                .authorizeHttpRequests(a -> a
                        .requestMatchers(EndpointRequest.to("health")).permitAll()
                        // D98: Prometheus scrapes with a bearer token of its own; no token configured, no scrape
                        .requestMatchers(EndpointRequest.to("prometheus")).access((authentication, context) ->
                                new AuthorizationDecision(metrics.allows(context.getRequest())))
                        .requestMatchers(HttpMethod.POST, "/v1/webhooks/gateway/*").permitAll()
                        .requestMatchers("/error").permitAll()
                        .anyRequest().authenticated())
                .exceptionHandling(e -> e
                        .authenticationEntryPoint((request, response, ex) -> json(response, 401, "unauthenticated"))
                        .accessDeniedHandler((request, response, ex) -> json(response, 403, "forbidden")));
        RateLimiter rateLimiter = limiter.getIfAvailable();
        if (rateLimiter != null) {   // D96: ahead of the token lookup; absent only in MVC slice tests
            MeterRegistry registry = meters.getIfAvailable();
            Runnable counted = registry == null ? () -> { }
                    : registry.counter("driftline_requests_limited_total")::increment;
            http.addFilterBefore(new RateLimitFilter(rateLimiter, counted), TokenAuthenticationFilter.class);
        }
        return http.build();
    }

    private static void json(jakarta.servlet.http.HttpServletResponse response, int status, String error)
            throws java.io.IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        response.getOutputStream().write(("{\"error\":\"" + error + "\"}").getBytes(StandardCharsets.UTF_8));
    }
}
