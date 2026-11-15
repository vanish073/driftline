package io.github.vanish073.driftline.app.security;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import java.nio.charset.StandardCharsets;
import org.springframework.boot.security.autoconfigure.actuate.web.servlet.EndpointRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;

/**
 * Spring Security for the API (§10.1, §14): stateless, no sessions, no CSRF (no cookies or browser forms; every write is
 * authenticated by a bearer token or a webhook signature), no login forms. Gateway webhooks pass the filter chain
 * because their authentication is the HMAC signature, checked in the controller on the raw bytes (D21). Every other
 * route needs a token; which token may do what is decided by {@code @PreAuthorize} on each method. Health and metrics
 * are open but served only on the management port, which is never exposed outside the cluster (D21).
 */
@Configuration
@Profile("!" + Roles.MIGRATE)
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfiguration {

    @Bean
    MerchantAccess merchantAccess() {
        return new MerchantAccess();
    }

    @Bean
    SecurityFilterChain api(HttpSecurity http, PostgresDirectory directory, Clock clock) throws Exception {
        http.csrf(c -> c.disable())
                .httpBasic(b -> b.disable())
                .formLogin(f -> f.disable())
                .logout(l -> l.disable())
                .requestCache(r -> r.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .addFilterBefore(new TokenAuthenticationFilter(directory, clock), AnonymousAuthenticationFilter.class)
                .authorizeHttpRequests(a -> a
                        .requestMatchers(EndpointRequest.to("health", "prometheus")).permitAll()
                        .requestMatchers(HttpMethod.POST, "/v1/webhooks/gateway/*").permitAll()
                        .requestMatchers("/error").permitAll()
                        .anyRequest().authenticated())
                .exceptionHandling(e -> e
                        .authenticationEntryPoint((request, response, ex) -> json(response, 401, "unauthenticated"))
                        .accessDeniedHandler((request, response, ex) -> json(response, 403, "forbidden")));
        return http.build();
    }

    private static void json(jakarta.servlet.http.HttpServletResponse response, int status, String error)
            throws java.io.IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        response.getOutputStream().write(("{\"error\":\"" + error + "\"}").getBytes(StandardCharsets.UTF_8));
    }
}
