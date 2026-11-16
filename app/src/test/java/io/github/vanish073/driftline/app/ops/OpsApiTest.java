package io.github.vanish073.driftline.app.ops;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.ingest.IngestController;
import io.github.vanish073.driftline.app.kafka.EventPublisher;
import io.github.vanish073.driftline.app.merchant.DevBootstrap;
import io.github.vanish073.driftline.app.merchant.MerchantRegistry;
import io.github.vanish073.driftline.app.security.SecurityConfiguration;
import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.port.BreakRepo;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.port.TxBody;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import io.github.vanish073.driftline.store.postgres.SettlementInbox;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

/** The ops read API: a merchant token sees only its merchant, an ops token sees all (§10.1, §14). */
@WebMvcTest(controllers = {IngestController.class, OpsController.class})
@Import(SecurityConfiguration.class)
@ActiveProfiles(Roles.INGEST)
class OpsApiTest {

    static final Instant NOW = Instant.parse("2026-11-15T10:00:00Z");
    static final MerchantId A = MerchantId.of("m_a");
    static final MerchantId B = MerchantId.of("m_b");
    static final Break BREAK_A = Break.detected(A, BreakType.DUPLICATE_CAPTURE, "pay_2", NOW, Map.of("kept", "pay_1"));
    static final Break BREAK_B = Break.detected(B, BreakType.DEBITED_NO_ORDER, "pay_9", NOW, Map.of());

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

    @BeforeEach
    void world() {
        when(clock.now()).thenReturn(NOW);
        FeeSchedule fees = new FeeSchedule(200, 1_800);
        LocalDate first = LocalDate.parse("2026-11-14");
        when(merchants.merchants()).thenReturn(List.of(new MerchantTerms(A, 1, fees, 1, first),
                new MerchantTerms(B, 2, fees, 1, first)));
        token("test-token-a", Optional.of(A));
        token("test-token-ops", Optional.empty());
        Tx tx = Mockito.mock(Tx.class);
        BreakRepo breaks = Mockito.mock(BreakRepo.class);
        when(tx.breaks()).thenReturn(breaks);
        when(breaks.forMerchant(A)).thenReturn(List.of(BREAK_A));
        when(breaks.forMerchant(B)).thenReturn(List.of(BREAK_B));
        when(breaks.byId(anyString())).thenReturn(Optional.empty());
        when(breaks.byId(BREAK_A.breakId())).thenReturn(Optional.of(BREAK_A));
        when(breaks.byId(BREAK_B.breakId())).thenReturn(Optional.of(BREAK_B));
        when(breaks.transitions(any(), anyString())).thenReturn(List.of());
        when(store.transact(anyString(), any())).thenAnswer(i -> i.getArgument(1, TxBody.class).run(tx));
    }

    void token(String token, Optional<MerchantId> merchant) {
        Sha256 hash = DevBootstrap.tokenHash(token);
        when(directory.token(hash)).thenReturn(Optional.of(new PostgresDirectory.ApiToken(hash, merchant,
                List.of("breaks:read"), NOW.minusSeconds(60), Optional.empty())));
    }

    @Test
    void aMerchantTokenListsOnlyItsOwnBreaks() throws Exception {
        mvc.perform(get("/v1/breaks").header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.breaks[0].type").value("DUPLICATE_CAPTURE"))
                .andExpect(jsonPath("$.breaks[0].evidence.kept").value("pay_1"));
        mvc.perform(get("/v1/breaks?merchantId=m_b").header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isForbidden());
    }

    @Test
    void anOpsTokenListsEveryMerchantAndFilters() throws Exception {
        mvc.perform(get("/v1/breaks").header("Authorization", "Bearer test-token-ops"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.count").value(2));
        mvc.perform(get("/v1/breaks?type=debited_no_order").header("Authorization", "Bearer test-token-ops"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.breaks[0].merchant_id").value("m_b"));
        mvc.perform(get("/v1/breaks?state=NOPE").header("Authorization", "Bearer test-token-ops"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void anotherMerchantsBreakIs404NotForbiddenSoItsExistenceDoesNotLeak() throws Exception {
        mvc.perform(get("/v1/breaks/" + BREAK_A.breakId()).header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.subject_key").value("pay_2"));
        mvc.perform(get("/v1/breaks/" + BREAK_B.breakId()).header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isNotFound());
        mvc.perform(get("/v1/breaks/" + BREAK_B.breakId()).header("Authorization", "Bearer test-token-ops"))
                .andExpect(status().isOk());
    }

    @Test
    void anOpsTokenMustNameTheMerchantOfAPaymentTimeline() throws Exception {
        mvc.perform(get("/v1/payments/pay_1/timeline").header("Authorization", "Bearer test-token-ops"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/v1/payments/pay_1/timeline?merchantId=m_b").header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isForbidden());
    }

    @Test
    void readingNeedsAToken() throws Exception {
        mvc.perform(get("/v1/breaks")).andExpect(status().isUnauthorized());
    }
}
