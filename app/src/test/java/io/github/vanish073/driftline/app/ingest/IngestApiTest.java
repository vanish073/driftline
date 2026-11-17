package io.github.vanish073.driftline.app.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.kafka.EventPublisher;
import io.github.vanish073.driftline.app.merchant.DevBootstrap;
import io.github.vanish073.driftline.app.merchant.MerchantRegistry;
import io.github.vanish073.driftline.app.merchant.SecretCipher;
import io.github.vanish073.driftline.app.merchant.SecretRefresher;
import io.github.vanish073.driftline.app.ops.OpsController;
import io.github.vanish073.driftline.app.security.SecurityConfiguration;
import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import io.github.vanish073.driftline.store.postgres.SettlementInbox;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import io.github.vanish073.driftline.wire.order.OrderEventCodec;
import io.github.vanish073.driftline.wire.order.OrderEventMessage;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
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
import org.springframework.test.json.JsonCompareMode;
import org.springframework.test.web.servlet.MockMvc;

/**
 * The ingest API with Spring Security, Kafka mocked (DESIGN §10, §14): signatures checked on the raw bytes before any
 * parsing, the replay window, the required idempotency key, token scopes per merchant (the IDOR test), size limits,
 * 202 only after the log acknowledged, 503 when it did not.
 */
@WebMvcTest(controllers = {IngestController.class, OpsController.class})
@Import(SecurityConfiguration.class)
@ActiveProfiles(Roles.INGEST)
class IngestApiTest {

    static final Instant NOW = Instant.parse("2026-11-15T10:00:00Z");
    static final MerchantId A = MerchantId.of("m_a");
    static final MerchantId B = MerchantId.of("m_b");
    static final byte[] SECRET_A = "test-only-webhook-secret-a-not-a-real-key".getBytes(StandardCharsets.US_ASCII);
    static final byte[] SECRET_B = "test-only-webhook-secret-b-not-a-real-key".getBytes(StandardCharsets.US_ASCII);
    static final String TOKEN_A = "test-token-a_writer";
    static final String TOKEN_A_READER = "test-token-a_reader";
    static final String TOKEN_B = "test-token-b_writer";

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

    @BeforeEach
    void world() {
        when(clock.now()).thenReturn(NOW);
        when(merchants.context(A)).thenReturn(Optional.of(new MerchantContext(A, 1, List.of(SECRET_A))));
        when(merchants.context(B)).thenReturn(Optional.of(new MerchantContext(B, 2, List.of(SECRET_B))));
        when(merchants.context(MerchantId.of("m_nobody"))).thenReturn(Optional.empty());
        token(TOKEN_A, Optional.of(A), List.of("events:write", "settlement:write", "breaks:read"));
        token(TOKEN_A_READER, Optional.of(A), List.of("breaks:read"));
        token(TOKEN_B, Optional.of(B), List.of("events:write", "settlement:write", "breaks:read"));
        when(inbox.store(any(), any(), any())).thenAnswer(i -> new SettlementInbox.Stored(
                Sha256.of(i.getArgument(1, byte[].class)), true));
    }

    void token(String token, Optional<MerchantId> merchant, List<String> scopes) {
        Sha256 hash = DevBootstrap.tokenHash(token);
        when(directory.token(hash)).thenReturn(Optional.of(new PostgresDirectory.ApiToken(hash, merchant, scopes,
                NOW.minusSeconds(60), Optional.empty())));
    }

    static byte[] webhook(String eventId) {
        return GatewayWebhookCodec.encode(new GatewayWebhook(eventId, EventType.PAYMENT_SUCCEEDED,
                NOW.minusSeconds(30), new PaymentPayload(PaymentId.of("pay_1"), OrderId.of("ord_1"),
                Money.ofPaise(49_900))));
    }

    static byte[] orderEvent() {
        return OrderEventCodec.encode(new OrderEventMessage(EventType.ORDER_CREATED, NOW.minusSeconds(60),
                new OrderPayload(OrderId.of("ord_1"), 1, Money.ofPaise(49_900), NOW.plusSeconds(900), Optional.empty())));
    }

    @SuppressWarnings("unchecked")
    private List<EventEnvelope> published() {
        ArgumentCaptor<List<EventEnvelope>> captor = ArgumentCaptor.forClass(List.class);
        verify(publisher).publish(captor.capture());
        return captor.getValue();
    }

    // ---- webhooks -------------------------------------------------------------------------------------------------

    @Test
    void aSignedWebhookIsAppendedToTheMerchantsPartitionAndAnswered202WithItsKey() throws Exception {
        byte[] body = webhook("evt_1");
        mvc.perform(post("/v1/webhooks/gateway/m_a").content(body)
                        .header(WebhookSignature.HEADER, WebhookSignature.sign(SECRET_A, NOW, body)))
                .andExpect(status().isAccepted())
                .andExpect(content().json("{\"accepted\":true,\"key\":\"evt_1\"}", JsonCompareMode.STRICT));
        EventEnvelope e = published().get(0);
        assertThat(e.merchantId()).isEqualTo(A);
        assertThat(e.partition()).isEqualTo(1);
        assertThat(e.source()).isEqualTo(Source.GATEWAY);
        assertThat(e.eventKey()).isEqualTo("evt_1");
        assertThat(e.receivedAt()).isEqualTo(NOW);
    }

    @Test
    void aReplayedWebhookGetsTheSameAnswerAndDedupeDownstreamMakesItHarmless() throws Exception {
        byte[] body = webhook("evt_1");
        String first = mvc.perform(post("/v1/webhooks/gateway/m_a").content(body)
                        .header(WebhookSignature.HEADER, WebhookSignature.sign(SECRET_A, NOW, body)))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
        String replay = mvc.perform(post("/v1/webhooks/gateway/m_a").content(body)
                        .header(WebhookSignature.HEADER, WebhookSignature.sign(SECRET_A, NOW.minusSeconds(100), body)))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
        assertThat(replay).isEqualTo(first);
    }

    @Test
    void aTamperedBodyAWrongSecretAndAMissingSignatureAre401AndNothingIsPublished() throws Exception {
        byte[] body = webhook("evt_1");
        String signature = WebhookSignature.sign(SECRET_A, NOW, body);
        byte[] tampered = Arrays.copyOf(body, body.length);
        tampered[tampered.length - 3] = (byte) '9';
        mvc.perform(post("/v1/webhooks/gateway/m_a").content(tampered).header(WebhookSignature.HEADER, signature))
                .andExpect(status().isUnauthorized()).andExpect(jsonPath("$.error").value("signature NO_MATCH"));
        mvc.perform(post("/v1/webhooks/gateway/m_a").content(body)
                        .header(WebhookSignature.HEADER, WebhookSignature.sign(SECRET_B, NOW, body)))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/v1/webhooks/gateway/m_a").content(body))
                .andExpect(status().isUnauthorized()).andExpect(jsonPath("$.error").value("signature MISSING"));
        verify(publisher, never()).publish(any());
    }

    @Test
    void aStaleOrFutureTimestampIsRejected() throws Exception {
        byte[] body = webhook("evt_1");
        for (Instant t : List.of(NOW.minusSeconds(301), NOW.plusSeconds(301))) {
            mvc.perform(post("/v1/webhooks/gateway/m_a").content(body)
                            .header(WebhookSignature.HEADER, WebhookSignature.sign(SECRET_A, t, body)))
                    .andExpect(status().isUnauthorized())
                    .andExpect(jsonPath("$.error").value("signature OUTSIDE_REPLAY_WINDOW"));
        }
        verify(publisher, never()).publish(any());
    }

    @Test
    void anUnknownMerchantLooksExactlyLikeABadSignature() throws Exception {
        byte[] body = webhook("evt_1");
        mvc.perform(post("/v1/webhooks/gateway/m_nobody").content(body)
                        .header(WebhookSignature.HEADER, WebhookSignature.sign(SECRET_A, NOW, body)))
                .andExpect(status().isUnauthorized()).andExpect(jsonPath("$.error").value("signature NO_MATCH"));
    }

    @Test
    void aBodyOverTheLimitIs413BeforeAnyParsing() throws Exception {
        byte[] huge = new byte[WebhookSignature.MAX_BODY_BYTES + 1];
        mvc.perform(post("/v1/webhooks/gateway/m_a").content(huge)
                        .header(WebhookSignature.HEADER, WebhookSignature.sign(SECRET_A, NOW, huge)))
                .andExpect(status().is(413));
    }

    @Test
    void whenTheLogDoesNotAcknowledgeTheAnswerIs503SoTheSenderRetries() throws Exception {
        doThrow(new EventPublisher.EventLogUnavailable("down", null)).when(publisher).publish(any());
        byte[] body = webhook("evt_1");
        mvc.perform(post("/v1/webhooks/gateway/m_a").content(body)
                        .header(WebhookSignature.HEADER, WebhookSignature.sign(SECRET_A, NOW, body)))
                .andExpect(status().isServiceUnavailable());
    }

    // ---- order events: token scopes per merchant ------------------------------------------------------------------

    @Test
    void anOrderEventWithTheMerchantsTokenAndAKeyIsAccepted() throws Exception {
        mvc.perform(post("/v1/merchants/m_a/order-events").content(orderEvent())
                        .header("Authorization", "Bearer " + TOKEN_A).header("Idempotency-Key", "ord_1-v1"))
                .andExpect(status().isAccepted())
                .andExpect(content().json("{\"accepted\":true,\"key\":\"ord_1-v1\"}", JsonCompareMode.STRICT));
        EventEnvelope e = published().get(0);
        assertThat(e.source()).isEqualTo(Source.MERCHANT);
        assertThat(e.eventKey()).isEqualTo("ord_1-v1");
    }

    @Test
    void theIdempotencyKeyIsRequired() throws Exception {
        mvc.perform(post("/v1/merchants/m_a/order-events").content(orderEvent())
                        .header("Authorization", "Bearer " + TOKEN_A))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("the Idempotency-Key header is required"));
    }

    @Test
    void noTokenIs401AndAnUnknownTokenIs401() throws Exception {
        mvc.perform(post("/v1/merchants/m_a/order-events").content(orderEvent()).header("Idempotency-Key", "k"))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/v1/merchants/m_a/order-events").content(orderEvent()).header("Idempotency-Key", "k")
                        .header("Authorization", "Bearer not-a-token"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void aMerchantATokenOnMerchantBIs403TheIdorTest() throws Exception {
        mvc.perform(post("/v1/merchants/m_b/order-events").content(orderEvent()).header("Idempotency-Key", "k")
                        .header("Authorization", "Bearer " + TOKEN_A))
                .andExpect(status().isForbidden());
        mvc.perform(post("/v1/merchants/m_b/settlement-files").content("x".getBytes(StandardCharsets.UTF_8))
                        .header("Authorization", "Bearer " + TOKEN_A))
                .andExpect(status().isForbidden());
        verify(publisher, never()).publish(any());
        verify(inbox, never()).store(any(), any(), any());
    }

    @Test
    void aTokenWithoutTheScopeIs403() throws Exception {
        mvc.perform(post("/v1/merchants/m_a/order-events").content(orderEvent()).header("Idempotency-Key", "k")
                        .header("Authorization", "Bearer " + TOKEN_A_READER))
                .andExpect(status().isForbidden());
    }

    @Test
    void aRevokedTokenAuthenticatesNothing() throws Exception {
        Sha256 hash = DevBootstrap.tokenHash("test-token-revoked");
        when(directory.token(hash)).thenReturn(Optional.of(new PostgresDirectory.ApiToken(hash, Optional.of(A),
                List.of("events:write"), NOW.minusSeconds(600), Optional.of(NOW.minusSeconds(1)))));
        mvc.perform(post("/v1/merchants/m_a/order-events").content(orderEvent()).header("Idempotency-Key", "k")
                        .header("Authorization", "Bearer test-token-revoked"))
                .andExpect(status().isUnauthorized());
    }

    // ---- settlement uploads ---------------------------------------------------------------------------------------

    static final String FILE_A = """
            H,DLSF,1,stl_m_a_20261114,m_a,2026-11-14,2026-11-14T04:30:00Z
            D,1,2026-11-14,4784.36,98.00,17.64,pay_12_a,UTR1,settlement
            T,1,4784.36
            """;

    @Test
    void aValidSettlementFileIsStoredForTheWorkerAndAnsweredWithItsHash() throws Exception {
        byte[] file = FILE_A.getBytes(StandardCharsets.UTF_8);
        mvc.perform(post("/v1/merchants/m_a/settlement-files").content(file)
                        .header("Authorization", "Bearer " + TOKEN_A))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.key").value(Sha256.of(file).hex()));
        verify(inbox, times(1)).store(any(), any(), any());
        verify(publisher, never()).publish(any());
    }

    @Test
    void aMalformedFileIs400AndNothingIsStored() throws Exception {
        byte[] bad = FILE_A.replace("T,1,4784.36", "T,2,4784.36").getBytes(StandardCharsets.UTF_8);
        mvc.perform(post("/v1/merchants/m_a/settlement-files").content(bad)
                        .header("Authorization", "Bearer " + TOKEN_A))
                .andExpect(status().isBadRequest());
        byte[] othersFile = FILE_A.replace(",m_a,", ",m_b,").getBytes(StandardCharsets.UTF_8);
        mvc.perform(post("/v1/merchants/m_a/settlement-files").content(othersFile)
                        .header("Authorization", "Bearer " + TOKEN_A))
                .andExpect(status().isBadRequest());
        verify(inbox, never()).store(any(), any(), any());
    }

    // ---- observations (D10, D65) ----------------------------------------------------------------------------------

    static final String SERVICE_TOKEN = "test-token-service";
    static final String TOKEN_A_OBSERVER = "test-token-a_observer";

    static byte[] observation(String merchant) {
        return ("{\"merchant_id\":\"" + merchant + "\",\"type\":\"payment.succeeded\",\"event_key\":\"obs_abc\","
                + "\"occurred_at\":\"2026-11-15T09:00:00Z\","
                + "\"payment\":{\"id\":\"pay_1\",\"order_id\":\"ord_1\",\"amount_paise\":49900}}")
                .getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void anObservationWithTheServiceTokenIsAppendedAsAGatewayEventUnderItsDerivedKey() throws Exception {
        token(SERVICE_TOKEN, Optional.empty(), List.of("observations:write"));
        mvc.perform(post("/internal/v1/observations").content(observation("m_b"))
                        .header("Authorization", "Bearer " + SERVICE_TOKEN))
                .andExpect(status().isAccepted())
                .andExpect(content().json("{\"accepted\":true,\"key\":\"obs_abc\"}", JsonCompareMode.STRICT));
        EventEnvelope e = published().get(0);
        assertThat(e.merchantId()).isEqualTo(B);
        assertThat(e.partition()).isEqualTo(2);
        assertThat(e.source()).isEqualTo(Source.GATEWAY);
        assertThat(e.eventType()).isEqualTo(EventType.PAYMENT_SUCCEEDED);
        assertThat(e.eventTime()).isEqualTo(Instant.parse("2026-11-15T09:00:00Z"));
    }

    @Test
    void anObservationNeedsTheObservationsScopeAndAMerchantTokenOnlyItsOwnMerchant() throws Exception {
        token(TOKEN_A_OBSERVER, Optional.of(A), List.of("observations:write"));
        mvc.perform(post("/internal/v1/observations").content(observation("m_a"))
                        .header("Authorization", "Bearer " + TOKEN_A))
                .andExpect(status().isForbidden());
        mvc.perform(post("/internal/v1/observations").content(observation("m_b"))
                        .header("Authorization", "Bearer " + TOKEN_A_OBSERVER))
                .andExpect(status().isForbidden());
        mvc.perform(post("/internal/v1/observations").content(observation("m_a")))
                .andExpect(status().isUnauthorized());
        verify(publisher, never()).publish(any());
    }

    @Test
    void aMalformedObservationIs400AndAnUnknownMerchant404() throws Exception {
        token(SERVICE_TOKEN, Optional.empty(), List.of("observations:write"));
        mvc.perform(post("/internal/v1/observations").content("{\"merchant_id\":\"m_a\"}")
                        .header("Authorization", "Bearer " + SERVICE_TOKEN))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/internal/v1/observations").content(observation("m_nobody"))
                        .header("Authorization", "Bearer " + SERVICE_TOKEN))
                .andExpect(status().isNotFound());
        verify(publisher, never()).publish(any());
    }
}
