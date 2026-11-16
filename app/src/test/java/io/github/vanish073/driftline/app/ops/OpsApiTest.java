package io.github.vanish073.driftline.app.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
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
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.port.BreakRepo;
import io.github.vanish073.driftline.core.port.PaymentRepo;
import io.github.vanish073.driftline.core.port.SettlementRepo;
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
    /** Shift 7: an unreferenced line that two subsets fit, escalated as an orphan with both witnesses on the line. */
    static final Break BREAK_AMBIGUOUS = Break.detected(A, BreakType.ORPHAN_SETTLEMENT, "stl_1:2", NOW,
            Map.of("reason", "AMBIGUOUS", "amount", "974.44", "candidates", "3"));
    static final SettlementRepo.Line AMBIGUOUS_LINE = new SettlementRepo.Line(A, "stl_1", 2,
            LocalDate.parse("2026-11-14"), LocalDate.parse("2026-11-14"), Money.ofPaise(97_444), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.of("UTR20261114MA"), SettlementRepo.LineStatus.AWAITING_BATCH)
            .ambiguous(List.of(PaymentId.of("pay_3"), PaymentId.of("pay_4")),
                    List.of(PaymentId.of("pay_5"), PaymentId.of("pay_6")));

    Tx tx;
    PaymentRepo payments;
    SettlementRepo settlement;

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
        tx = Mockito.mock(Tx.class);
        BreakRepo breaks = Mockito.mock(BreakRepo.class);
        payments = Mockito.mock(PaymentRepo.class);
        settlement = Mockito.mock(SettlementRepo.class);
        when(tx.breaks()).thenReturn(breaks);
        when(tx.payments()).thenReturn(payments);
        when(tx.settlement()).thenReturn(settlement);
        when(settlement.line(A, "stl_1", 2)).thenReturn(Optional.of(AMBIGUOUS_LINE));
        when(payments.find(A, PaymentId.of("pay_2"))).thenReturn(Optional.of(new PaymentState(PaymentId.of("pay_2"),
                PaymentStatus.SUCCEEDED, Optional.of(OrderId.of("ord_7")), Optional.of(Money.ofPaise(49_900)),
                Optional.empty(), Optional.of(NOW), false)));
        when(breaks.forMerchant(A)).thenReturn(List.of(BREAK_A));
        when(breaks.forMerchant(B)).thenReturn(List.of(BREAK_B));
        when(breaks.byId(anyString())).thenReturn(Optional.empty());
        when(breaks.byId(BREAK_A.breakId())).thenReturn(Optional.of(BREAK_A));
        when(breaks.byId(BREAK_B.breakId())).thenReturn(Optional.of(BREAK_B));
        when(breaks.byId(BREAK_AMBIGUOUS.breakId())).thenReturn(Optional.of(BREAK_AMBIGUOUS));
        when(breaks.transitions(any(), anyString())).thenReturn(List.of());
        when(store.transact(anyString(), any())).thenAnswer(i -> i.getArgument(1, TxBody.class).run(tx));
    }

    void token(String token, Optional<MerchantId> merchant) {
        Sha256 hash = DevBootstrap.tokenHash(token);
        when(directory.token(hash)).thenReturn(Optional.of(new PostgresDirectory.ApiToken(hash, merchant,
                List.of("breaks:read"), NOW.minusSeconds(60), Optional.empty())));
    }

    @Test
    void anAmbiguousLineShowsItsTwoWitnessesInTheBreakDetail() throws Exception {
        mvc.perform(get("/v1/breaks/" + BREAK_AMBIGUOUS.breakId()).header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.evidence.reason").value("AMBIGUOUS"))
                .andExpect(jsonPath("$.line.status").value("AMBIGUOUS"))
                .andExpect(jsonPath("$.line.amount").value("974.44"))
                .andExpect(jsonPath("$.line.batch_id").value("UTR20261114MA"))
                .andExpect(jsonPath("$.line.witnesses.length()").value(2))
                .andExpect(jsonPath("$.line.witnesses[0][0]").value("pay_3"))
                .andExpect(jsonPath("$.line.witnesses[1][1]").value("pay_6"));
    }

    @Test
    void theCsvExportListsTheMerchantsBreaksInSheetColumns() throws Exception {
        when(tx.breaks().forMerchant(A)).thenReturn(List.of(BREAK_A, BREAK_AMBIGUOUS));
        String csv = mvc.perform(get("/v1/exports/breaks.csv").header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Type", "text/csv; charset=utf-8"))
                .andReturn().getResponse().getContentAsString();
        List<String> rows = List.of(csv.split("\r\n"));
        assertThat(rows.get(0)).isEqualTo(String.join(",", OpsController.CSV_COLUMNS));
        assertThat(rows).hasSize(3);
        assertThat(rows.get(1)).as("the duplicate capture: payment, order, expected amounts")
                .startsWith(BREAK_A.breakId() + ",m_a,DUPLICATE_CAPTURE,BREAK,DETECTED,")
                .contains(",pay_2,pay_2,ord_7,499.00,9.98,1.80,487.22,");
        assertThat(rows.get(2)).as("the ambiguous line: file, line, UTR, amount, reason and both witnesses")
                .endsWith(",stl_1,2,2026-11-14,UTR20261114MA,974.44,AMBIGUOUS,AMBIGUOUS,pay_3 pay_4,pay_5 pay_6");
        assertThat(csv).doesNotContain("m_b");
    }

    @Test
    void aMerchantTokenCannotExportAnotherMerchant() throws Exception {
        mvc.perform(get("/v1/exports/breaks.csv?merchantId=m_b").header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isForbidden());
    }

    @Test
    void csvFieldsAreQuotedOnlyWhenTheyMustBe() {
        assertThat(OpsController.csvField("plain")).isEqualTo("plain");
        assertThat(OpsController.csvField("a,b")).isEqualTo("\"a,b\"");
        assertThat(OpsController.csvField("say \"hi\"")).isEqualTo("\"say \"\"hi\"\"\"");
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
