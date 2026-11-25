package io.github.vanish073.driftline.app.security;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.ingest.IngestController;
import io.github.vanish073.driftline.app.kafka.EventPublisher;
import io.github.vanish073.driftline.app.merchant.DevBootstrap;
import io.github.vanish073.driftline.app.merchant.MerchantRegistry;
import io.github.vanish073.driftline.app.merchant.SecretCipher;
import io.github.vanish073.driftline.app.merchant.SecretRefresher;
import io.github.vanish073.driftline.app.observability.IngestMetrics;
import io.github.vanish073.driftline.app.observability.Tracing;
import io.github.vanish073.driftline.app.ops.OpsController;
import io.github.vanish073.driftline.app.pii.CustomerContacts;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import io.github.vanish073.driftline.store.postgres.SettlementInbox;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/**
 * "Guess the login code" (Shift 11, D96): repeated wrong tokens or webhook signatures from one address lock it out
 * with 429, before any token is looked up; other addresses carry on. Each context gets a fresh limiter.
 */
@WebMvcTest(controllers = {IngestController.class, OpsController.class})
@Import({SecurityConfiguration.class, RateLimitApiTest.Limits.class})
@ActiveProfiles(Roles.INGEST)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RateLimitApiTest {

    static final Instant NOW = Instant.parse("2026-11-25T10:00:00Z");
    static final String TOKEN = "test-token-ops_reader";

    @TestConfiguration
    static class Limits {
        @Bean
        RateLimiter rateLimiter() {
            return new RateLimiter(new RateLimiter.Limits(true, 5, Duration.ofSeconds(60), Duration.ofMinutes(5), 1_000,
                    1_000, 1_000), System::nanoTime);
        }
    }

    @Autowired
    MockMvc mvc;

    @MockitoBean
    MerchantRegistry merchants;
    @MockitoBean
    EventPublisher publisher;
    @MockitoBean
    SettlementInbox inbox;
    @MockitoBean
    Clock clock;
    @MockitoBean
    Store store;
    @MockitoBean
    PostgresDirectory directory;
    @MockitoBean
    SecretCipher cipher;
    @MockitoBean
    SecretRefresher refresher;
    @MockitoBean
    CustomerContacts contacts;
    @MockitoBean
    IngestMetrics ingestMetrics;
    @MockitoBean
    Tracing tracing;

    @BeforeEach
    void world() {
        when(clock.now()).thenReturn(NOW);
        Sha256 hash = DevBootstrap.tokenHash(TOKEN);
        when(directory.token(hash)).thenReturn(Optional.of(new PostgresDirectory.ApiToken(hash, Optional.empty(),
                List.of("breaks:read"), NOW.minusSeconds(60), NOW.plusSeconds(3_600), Optional.empty())));
        when(merchants.context(MerchantId.of("m_a"))).thenReturn(Optional.of(new MerchantContext(MerchantId.of("m_a"),
                1, List.of("test-only-webhook-secret-a-not-a-real-key".getBytes(StandardCharsets.US_ASCII)))));
        when(merchants.merchants()).thenReturn(List.of());
        when(store.transact(anyString(), any())).thenReturn(List.of());
    }

    static RequestPostProcessor from(String address) {
        return r -> {
            r.setRemoteAddr(address);
            return r;
        };
    }

    @Test
    void fiveWrongTokensLockTheAddressOutEvenForTheRightTokenAndNothingIsLookedUpThen() throws Exception {
        for (int i = 0; i < 5; i++) {
            mvc.perform(get("/v1/breaks").with(from("203.0.113.9")).header("Authorization", "Bearer guess-" + i))
                    .andExpect(status().isUnauthorized());
        }
        org.mockito.Mockito.clearInvocations(directory);
        mvc.perform(get("/v1/breaks").with(from("203.0.113.9")).header("Authorization", "Bearer " + TOKEN))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().string("Retry-After", "300"))
                .andExpect(header().string("X-Content-Type-Options", "nosniff"));
        verify(directory, never()).token(any());
        mvc.perform(get("/v1/breaks").with(from("198.51.100.4")).header("Authorization", "Bearer " + TOKEN))
                .andExpect(status().isOk());
    }

    @Test
    void forgedWebhookSignaturesAndProbingWithoutATokenCountToo() throws Exception {
        for (int i = 0; i < 3; i++) {
            mvc.perform(post("/v1/webhooks/gateway/m_a").with(from("203.0.113.10")).content("{}")
                            .header("Driftline-Signature", "t=1,v1=00"))
                    .andExpect(status().isUnauthorized());
        }
        for (String path : List.of("/admin", "/actuator/env")) {
            mvc.perform(get(path).with(from("203.0.113.10"))).andExpect(status().isUnauthorized());
        }
        mvc.perform(get("/v1/breaks").with(from("203.0.113.10")).header("Authorization", "Bearer " + TOKEN))
                .andExpect(status().isTooManyRequests());
        verify(publisher, never()).publish(any());
        verify(merchants, org.mockito.Mockito.atLeastOnce()).context(any());
        verify(directory, org.mockito.Mockito.atMost(0)).rotateWebhookSecret(any(), anyString(), any(), any(), any(),
                anyString(), anyString(), any());
    }

    @Test
    void aCorrectTokenNeverCountsAsAFailure() throws Exception {
        for (int i = 0; i < 50; i++) {
            mvc.perform(get("/v1/breaks").with(from("203.0.113.11")).header("Authorization", "Bearer " + TOKEN))
                    .andExpect(status().isOk());
        }
    }
}
