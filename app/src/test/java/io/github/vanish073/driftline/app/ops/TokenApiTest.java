package io.github.vanish073.driftline.app.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.merchant.DevBootstrap;
import io.github.vanish073.driftline.app.security.SecurityConfiguration;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The token API (Shift 11, D95): tokens are created with a lifetime and shown once, can be revoked at once, and no
 * token can create or revoke one stronger than itself. That is the "make myself admin at signup" attack: Driftline has
 * no signup, and the one way to get a token is from a token holding at least the same powers.
 */
@WebMvcTest(controllers = TokenController.class)
@Import(SecurityConfiguration.class)
@ActiveProfiles(Roles.INGEST)
class TokenApiTest {

    static final Instant NOW = Instant.parse("2026-11-25T10:00:00Z");
    static final MerchantId A = MerchantId.of("m_a");
    static final String ADMIN = "test-token-ops_admin";      // every scope
    static final String HELPDESK = "test-token-ops_helpdesk"; // tokens:write and breaks:read only
    static final String MERCHANT_WITH_TOKENS = "test-token-a_tokens";
    static final String READER = "test-token-ops_reader";
    static final String EXPIRED = "test-token-ops_expired";

    @Autowired
    MockMvc mvc;

    @MockitoBean
    PostgresDirectory directory;
    @MockitoBean
    Clock clock;

    @BeforeEach
    void world() {
        when(clock.now()).thenReturn(NOW);
        token(ADMIN, Optional.empty(), PostgresDirectory.SCOPES, NOW.plusSeconds(3_600));
        token(HELPDESK, Optional.empty(), List.of("tokens:write", "breaks:read"), NOW.plusSeconds(3_600));
        token(MERCHANT_WITH_TOKENS, Optional.of(A), List.of("tokens:write", "breaks:read"), NOW.plusSeconds(3_600));
        token(READER, Optional.empty(), List.of("breaks:read"), NOW.plusSeconds(3_600));
        token(EXPIRED, Optional.empty(), PostgresDirectory.SCOPES, NOW);
        when(directory.createToken(any(), anyString(), anyString(), anyString(), any()))
                .thenAnswer(i -> new PostgresDirectory.TokenCreated(i.getArgument(0, PostgresDirectory.ApiToken.class)
                        .tokenId()));
    }

    void token(String token, Optional<MerchantId> merchant, List<String> scopes, Instant expiresAt) {
        Sha256 hash = DevBootstrap.tokenHash(token);
        when(directory.token(hash)).thenReturn(Optional.of(new PostgresDirectory.ApiToken(hash, merchant, scopes,
                NOW.minusSeconds(60), expiresAt, Optional.empty())));
    }

    static MockHttpServletRequestBuilder create(String caller, String body) {
        return create(caller, body, "mint-1");
    }

    static MockHttpServletRequestBuilder create(String caller, String body, String key) {
        return post("/v1/tokens").header("Authorization", "Bearer " + caller).header("Idempotency-Key", key)
                .content(body);
    }

    static String request(String merchant, String scopes, long ttl) {
        return "{" + (merchant == null ? "" : "\"merchant_id\":\"" + merchant + "\",") + "\"scopes\":\"" + scopes
                + "\",\"ttl_seconds\":" + ttl + ",\"note\":\"onboarding m_a\"}";
    }

    static final long DAY = 86_400;

    @Test
    void aTokenIsCreatedWithItsLifetimeShownOnceAndOnlyItsHashIsStored() throws Exception {
        String body = mvc.perform(create(ADMIN, request("m_a", "events:write,breaks:read", 30 * DAY)))
                .andExpect(status().isCreated())
                .andExpect(header().string("Cache-Control", org.hamcrest.Matchers.containsString("no-store")))
                .andExpect(jsonPath("$.merchant_id").value("m_a"))
                .andExpect(jsonPath("$.scopes").value("breaks:read,events:write"))
                .andExpect(jsonPath("$.expires_at").value(NOW.plus(Duration.ofDays(30)).toString()))
                .andReturn().getResponse().getContentAsString();
        JsonNode json = JsonMapper.builder().build().readTree(body);
        String token = json.get("token").asString();
        assertThat(token).startsWith("dl_mer_").hasSize("dl_mer_".length() + 64);
        ArgumentCaptor<PostgresDirectory.ApiToken> stored = ArgumentCaptor.forClass(PostgresDirectory.ApiToken.class);
        String actor = "ops/" + DevBootstrap.tokenHash(ADMIN).hex().substring(0, 8);
        verify(directory).createToken(stored.capture(), eq(actor), eq("onboarding m_a"), eq("mint-1"), any());
        assertThat(stored.getValue().tokenSha256()).isEqualTo(DevBootstrap.tokenHash(token));
        assertThat(stored.getValue().toString()).doesNotContain(token);
        assertThat(json.get("token_id").asString()).isEqualTo(stored.getValue().tokenId())
                .matches("tok_[0-9a-f]{16}");

        String second = mvc.perform(create(ADMIN, request(null, "breaks:read", DAY), "mint-2"))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        assertThat(JsonMapper.builder().build().readTree(second).get("token").asString()).startsWith("dl_ops_")
                .isNotEqualTo(token);
    }

    @Test
    void noTokenCanGrantAScopeItDoesNotHoldSoNobodyCanMakeThemselvesAdmin() throws Exception {
        for (String scopes : List.of("ledger:adjust", "breaks:read,secrets:write", "tokens:write,ledger:adjust",
                "breaks:resolve")) {
            mvc.perform(create(HELPDESK, request(null, scopes, DAY))).andExpect(status().isForbidden())
                    .andExpect(jsonPath("$.error").value("a token can grant only scopes it holds itself"));
        }
        mvc.perform(create(HELPDESK, request(null, "tokens:write,breaks:read", DAY))).andExpect(status().isCreated());
        mvc.perform(create(MERCHANT_WITH_TOKENS, request("m_a", "breaks:read", DAY)))
                .andExpect(status().isForbidden());
        mvc.perform(create(READER, request(null, "breaks:read", DAY))).andExpect(status().isForbidden());
        mvc.perform(post("/v1/tokens").header("Idempotency-Key", "x").content(request(null, "breaks:read", DAY)))
                .andExpect(status().isUnauthorized());
        mvc.perform(create(EXPIRED, request(null, "breaks:read", DAY))).andExpect(status().isUnauthorized());
        verify(directory, org.mockito.Mockito.times(1))
                .createToken(any(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void roleFieldsUnknownScopesWildcardsAndMerchantTokensWithStaffScopesAreRefused() throws Exception {
        for (String body : List.of(
                "{\"scopes\":\"breaks:read\",\"ttl_seconds\":86400,\"note\":\"x\",\"role\":\"admin\"}",
                "{\"scopes\":\"breaks:read\",\"ttl_seconds\":86400,\"note\":\"x\",\"is_admin\":\"true\"}",
                request(null, "admin", DAY), request(null, "*", DAY), request(null, "", DAY),
                request(null, "breaks:read,breaks:read", DAY), request(null, "breaks:read,", DAY),
                request(null, "BREAKS:READ", DAY), request("m_a", "secrets:write", DAY),
                request("m_a", "tokens:write", DAY), request("m_a", "ledger:adjust", DAY),
                request("<script>", "breaks:read", DAY), "{\"scopes\":\"breaks:read\",\"ttl_seconds\":86400}")) {
            mvc.perform(create(ADMIN, body)).andExpect(status().isBadRequest());
        }
        verify(directory, never()).createToken(any(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void everyTokenExpiresWithinItsKindsLimit() throws Exception {
        for (long ttl : List.of(0L, -1L, 299L, 90 * DAY + 1, Long.MAX_VALUE)) {
            mvc.perform(create(ADMIN, request(null, "breaks:read", ttl))).andExpect(status().isBadRequest());
        }
        mvc.perform(create(ADMIN, request("m_a", "breaks:read", 365 * DAY + 1))).andExpect(status().isBadRequest());
        mvc.perform(create(ADMIN, request(null, "breaks:read", 90 * DAY))).andExpect(status().isCreated());
        mvc.perform(create(ADMIN, request("m_a", "breaks:read", 365 * DAY), "mint-2"))
                .andExpect(status().isCreated());
    }

    @Test
    void anExpiredTokenAuthenticatesNothing() throws Exception {
        mvc.perform(get("/v1/tokens").header("Authorization", "Bearer " + EXPIRED))
                .andExpect(status().isUnauthorized());
        when(clock.now()).thenReturn(NOW.minusSeconds(1));
        when(directory.tokens()).thenReturn(List.of());
        mvc.perform(get("/v1/tokens").header("Authorization", "Bearer " + EXPIRED)).andExpect(status().isOk());
    }

    @Test
    void aRepeatedCreateMakesNoSecondTokenAndNamesTheFirst() throws Exception {
        org.mockito.Mockito.doReturn(new PostgresDirectory.TokenAlreadyCreated("tok_0123456789abcdef"))
                .when(directory).createToken(any(), anyString(), anyString(), anyString(), any());
        mvc.perform(create(ADMIN, request(null, "breaks:read", DAY))).andExpect(status().isConflict())
                .andExpect(jsonPath("$.token_id").value("tok_0123456789abcdef"))
                .andExpect(jsonPath("$.token").doesNotExist());
        org.mockito.Mockito.doReturn(new PostgresDirectory.TokenKeyReused())
                .when(directory).createToken(any(), anyString(), anyString(), anyString(), any());
        mvc.perform(create(ADMIN, request(null, "breaks:read", DAY))).andExpect(status().isConflict());
        org.mockito.Mockito.doReturn(new PostgresDirectory.TokenUnknownMerchant())
                .when(directory).createToken(any(), anyString(), anyString(), anyString(), any());
        mvc.perform(create(ADMIN, request("m_nobody", "breaks:read", DAY))).andExpect(status().isNotFound());
        mvc.perform(post("/v1/tokens").header("Authorization", "Bearer " + ADMIN)
                        .content(request(null, "breaks:read", DAY)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("the Idempotency-Key header is required"));
    }

    @Test
    void theListShowsIdsScopesAndDatesButNeverATokenOrItsHash() throws Exception {
        Sha256 hash = DevBootstrap.tokenHash("dl_ops_secret");
        when(directory.tokens()).thenReturn(List.of(new PostgresDirectory.TokenInfo(
                PostgresDirectory.ApiToken.tokenId(hash), Optional.empty(), List.of("breaks:read"),
                NOW.minusSeconds(60), NOW.plusSeconds(60), Optional.of(NOW.minusSeconds(1)), "ops/abcd1234")));
        String body = mvc.perform(get("/v1/tokens").header("Authorization", "Bearer " + ADMIN))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tokens[0].token_id").value(PostgresDirectory.ApiToken.tokenId(hash)))
                .andExpect(jsonPath("$.tokens[0].active").value(false))
                .andExpect(jsonPath("$.tokens[0].created_by").value("ops/abcd1234"))
                .andReturn().getResponse().getContentAsString();
        assertThat(body).doesNotContain(hash.hex()).doesNotContain("dl_ops_secret").doesNotContain("\"token\"");
        mvc.perform(get("/v1/tokens").header("Authorization", "Bearer " + MERCHANT_WITH_TOKENS))
                .andExpect(status().isForbidden());
        mvc.perform(get("/v1/tokens").header("Authorization", "Bearer " + READER)).andExpect(status().isForbidden());
    }

    static MockHttpServletRequestBuilder revoke(String caller, String tokenId) {
        return post("/v1/tokens/" + tokenId + "/revoke").header("Authorization", "Bearer " + caller)
                .header("Idempotency-Key", "revoke-1").content("{\"note\":\"laptop stolen\"}");
    }

    @Test
    void revocationTakesEffectThroughTheDirectoryAndOnlyForTokensNoStrongerThanTheCaller() throws Exception {
        String id = "tok_0123456789abcdef";
        when(directory.revokeToken(eq(id), anyList(), eq(NOW), anyString(), eq("laptop stolen"), eq("revoke-1"), any()))
                .thenReturn(new PostgresDirectory.RevocationResult(PostgresDirectory.Revocation.REVOKED,
                        Optional.of(NOW)));
        mvc.perform(revoke(HELPDESK, id)).andExpect(status().isOk())
                .andExpect(jsonPath("$.revoked_at").value(NOW.toString()));
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> scopes = ArgumentCaptor.forClass(List.class);
        verify(directory).revokeToken(eq(id), scopes.capture(), eq(NOW), anyString(), anyString(), anyString(), any());
        assertThat(scopes.getValue()).containsExactlyInAnyOrder("tokens:write", "breaks:read");

        for (var outcome : List.of(PostgresDirectory.Revocation.FORBIDDEN, PostgresDirectory.Revocation.NOT_FOUND,
                PostgresDirectory.Revocation.ALREADY_REVOKED, PostgresDirectory.Revocation.KEY_REUSED)) {
            when(directory.revokeToken(eq(id), anyList(), any(), anyString(), anyString(), anyString(), any()))
                    .thenReturn(new PostgresDirectory.RevocationResult(outcome, Optional.empty()));
            int expected = switch (outcome) {
                case FORBIDDEN -> 403;
                case NOT_FOUND -> 404;
                default -> 409;
            };
            mvc.perform(revoke(HELPDESK, id)).andExpect(status().is(expected));
        }
        when(directory.revokeToken(eq(id), anyList(), any(), anyString(), anyString(), anyString(), any()))
                .thenReturn(new PostgresDirectory.RevocationResult(PostgresDirectory.Revocation.REPLAYED,
                        Optional.of(NOW.minusSeconds(5))));
        mvc.perform(revoke(HELPDESK, id)).andExpect(status().isOk())
                .andExpect(header().string("Idempotent-Replayed", "true"));
        for (String bad : List.of("tok_XYZ", "brk_1", "tok_0123456789abcdef0")) {
            mvc.perform(revoke(HELPDESK, bad)).andExpect(status().isNotFound());
        }
        assertThat(mvc.perform(revoke(HELPDESK, "..")).andReturn().getResponse().getStatus())
                .as("a path with .. is refused by the firewall, or finds nothing").isIn(400, 404);
        mvc.perform(revoke(MERCHANT_WITH_TOKENS, id)).andExpect(status().isForbidden());
    }
}
