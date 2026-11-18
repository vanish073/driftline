package io.github.vanish073.driftline.app.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
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
import io.github.vanish073.driftline.app.pii.CustomerContacts;
import io.github.vanish073.driftline.app.pii.PiiCipher;
import io.github.vanish073.driftline.app.security.SecurityConfiguration;
import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.port.BreakRepo;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.LedgerRepo;
import io.github.vanish073.driftline.core.port.OrderRepo;
import io.github.vanish073.driftline.core.port.PaymentRepo;
import io.github.vanish073.driftline.core.port.SettlementRepo;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.port.TxBody;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import io.github.vanish073.driftline.store.postgres.SettlementInbox;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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
        token(token, merchant, List.of("breaks:read"));
    }

    void token(String token, Optional<MerchantId> merchant, List<String> scopes) {
        Sha256 hash = DevBootstrap.tokenHash(token);
        when(directory.token(hash)).thenReturn(Optional.of(new PostgresDirectory.ApiToken(hash, merchant,
                scopes, NOW.minusSeconds(60), Optional.empty())));
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
        assertThat(OpsController.csvField("")).isEmpty();
    }

    @Test
    void aCellThatASpreadsheetWouldRunAsAFormulaIsExportedAsText() {
        // §14, OWASP CSV injection: = + - @ tab and carriage return start a formula in Excel and Sheets
        assertThat(OpsController.csvField("=HYPERLINK(\"http://x\",\"y\")"))
                .isEqualTo("\"'=HYPERLINK(\"\"http://x\"\",\"\"y\"\")\"");
        assertThat(OpsController.csvField("+1")).isEqualTo("\"'+1\"");
        assertThat(OpsController.csvField("-2+3")).isEqualTo("\"'-2+3\"");
        assertThat(OpsController.csvField("@SUM(A1)")).isEqualTo("\"'@SUM(A1)\"");
        assertThat(OpsController.csvField("\tcmd")).isEqualTo("\"'\tcmd\"");
        assertThat(OpsController.csvField("pay_1-a")).as("only the first character matters").isEqualTo("pay_1-a");
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

    // ---- manual resolution (§8.1, §12) ----------------------------------------------------------------------------

    static final Break DETECTED_B = Break.detected(B, BreakType.SHORT_SETTLEMENT, "stl_9:1", NOW.minusSeconds(7_200),
            Map.of());
    static final Break ESCALATED_B = new Break(DETECTED_B.breakId(), B, BreakType.SHORT_SETTLEMENT, "stl_9:1",
            BreakState.ESCALATED, DETECTED_B.detectedAt(), new TreeMap<>(), 2);
    static final String ESCALATED_PATH = "/v1/breaks/" + ESCALATED_B.breakId() + "/resolve";

    void resolvable() {
        token("test-token-ops-resolver", Optional.empty(), List.of("breaks:read", "breaks:resolve"));
        token("test-token-a-resolver", Optional.of(A), List.of("breaks:read", "breaks:resolve"));
        when(tx.breaks().byId(ESCALATED_B.breakId())).thenReturn(Optional.of(ESCALATED_B));
        AuditRepo audit = Mockito.mock(AuditRepo.class);
        when(tx.audit()).thenReturn(audit);
        when(audit.append(any())).thenAnswer(i -> {
            AuditRepo.Entry e = i.getArgument(0);
            return new AuditRepo.Row(7, e, AuditRepo.GENESIS, AuditRepo.hash(AuditRepo.GENESIS, e));
        });
    }

    @Test
    void anOpsTokenResolvesAnEscalatedBreakByHandWithAnAuditRowNamingTheToken() throws Exception {
        resolvable();
        mvc.perform(post(ESCALATED_PATH).content("{\"note\":\"bank confirmed by phone\"}")
                        .header("Authorization", "Bearer test-token-ops-resolver"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.break_id").value(ESCALATED_B.breakId()))
                .andExpect(jsonPath("$.state").value("RESOLVED_MANUAL"))
                .andExpect(jsonPath("$.resolved_at").value(NOW.toString()))
                .andExpect(jsonPath("$.audit_seq").value(7));
        verify(tx.breaks()).transition(B, ESCALATED_B.breakId(), BreakState.RESOLVED_MANUAL, NOW,
                "resolved by hand: bank confirmed by phone");
        ArgumentCaptor<AuditRepo.Entry> entry = ArgumentCaptor.forClass(AuditRepo.Entry.class);
        verify(tx.audit()).append(entry.capture());
        assertThat(entry.getValue().actor()).startsWith("ops/");
        assertThat(entry.getValue().subject()).isEqualTo(ESCALATED_B.breakId());
    }

    @Test
    void resolvingNeedsTheResolveScopeAndAMerchantTokenSeesOnlyItsOwnBreaks() throws Exception {
        resolvable();
        mvc.perform(post(ESCALATED_PATH).content("{\"note\":\"x\"}")
                        .header("Authorization", "Bearer test-token-ops"))
                .andExpect(status().isForbidden());
        mvc.perform(post(ESCALATED_PATH).content("{\"note\":\"x\"}")
                        .header("Authorization", "Bearer test-token-a-resolver"))
                .andExpect(status().isNotFound());
        verify(tx.breaks(), never()).transition(any(), anyString(), any(), any(), anyString());
    }

    @Test
    void aBreakThatIsNotEscalatedIs409AndAMissingNote400() throws Exception {
        resolvable();
        mvc.perform(post("/v1/breaks/" + BREAK_A.breakId() + "/resolve").content("{\"note\":\"x\"}")
                        .header("Authorization", "Bearer test-token-ops-resolver"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value(
                        "the break is DETECTED; only an ESCALATED break can be resolved by hand"));
        mvc.perform(post(ESCALATED_PATH).content("{}")
                        .header("Authorization", "Bearer test-token-ops-resolver"))
                .andExpect(status().isBadRequest());
        mvc.perform(post(ESCALATED_PATH).content("{\"note\":\"   \"}")
                        .header("Authorization", "Bearer test-token-ops-resolver"))
                .andExpect(status().isBadRequest());
        verify(tx.breaks(), never()).transition(eq(B), anyString(), any(), any(), anyString());
    }

    // ---- manual ledger adjustments (§7, D8, D74) ------------------------------------------------------------------

    static final String ADJUST_A = "/v1/merchants/m_a/ledger/adjustments";
    static final String BALANCED = "{\"note\":\"bank fee we had not booked\","
            + "\"entries\":{\"SETTLEMENT_DIFFERENCES\":\"12.34\",\"BANK\":\"-12.34\"}}";

    LedgerRepo adjustable() {
        token("test-token-ops-adjuster", Optional.empty(), List.of("breaks:read", "ledger:adjust"));
        token("test-token-a-adjuster", Optional.of(A), List.of("breaks:read", "ledger:adjust"));
        when(merchants.context(A)).thenReturn(Optional.of(new MerchantContext(A, 1, List.of(new byte[32]))));
        AuditRepo audit = Mockito.mock(AuditRepo.class);
        when(tx.audit()).thenReturn(audit);
        when(audit.append(any())).thenAnswer(i -> {
            AuditRepo.Entry e = i.getArgument(0);
            return new AuditRepo.Row(4, e, AuditRepo.GENESIS, AuditRepo.hash(AuditRepo.GENESIS, e));
        });
        LedgerRepo ledger = Mockito.mock(LedgerRepo.class);
        when(tx.ledger()).thenReturn(ledger);
        when(ledger.post(any())).thenAnswer(i -> new PostResult.Posted(i.getArgument(0, LedgerTxn.class).id()));
        return ledger;
    }

    @Test
    void anOpsTokenPostsABalancedAdjustmentUnderItsAuditRow() throws Exception {
        LedgerRepo ledger = adjustable();
        mvc.perform(post(ADJUST_A).content(BALANCED).header("Authorization", "Bearer test-token-ops-adjuster"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.audit_seq").value(4))
                .andExpect(jsonPath("$.cause").value("MANUAL_ADJUSTMENT:4"))
                .andExpect(jsonPath("$.entries.BANK").value("-12.34"))
                .andExpect(jsonPath("$.entries.SETTLEMENT_DIFFERENCES").value("12.34"));
        ArgumentCaptor<LedgerTxn> txn = ArgumentCaptor.forClass(LedgerTxn.class);
        verify(ledger).post(txn.capture());
        assertThat(txn.getValue().merchantId()).isEqualTo(A);
        verify(tx.audit()).append(any());
    }

    @Test
    void adjustingNeedsTheLedgerScopeAndAnOpsToken() throws Exception {
        LedgerRepo ledger = adjustable();
        mvc.perform(post(ADJUST_A).content(BALANCED).header("Authorization", "Bearer test-token-a-adjuster"))
                .andExpect(status().isForbidden());
        mvc.perform(post(ADJUST_A).content(BALANCED).header("Authorization", "Bearer test-token-ops"))
                .andExpect(status().isForbidden());
        verify(ledger, never()).post(any());
    }

    @Test
    void anUnbalancedOrMalformedAdjustmentIs400AndAnUnknownMerchant404() throws Exception {
        LedgerRepo ledger = adjustable();
        String ops = "Bearer test-token-ops-adjuster";
        mvc.perform(post(ADJUST_A).header("Authorization", ops).content("{\"note\":\"x\","
                        + "\"entries\":{\"SETTLEMENT_DIFFERENCES\":\"12.34\",\"BANK\":\"-12.00\"}}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post(ADJUST_A).header("Authorization", ops).content("{\"note\":\"x\","
                        + "\"entries\":{\"PETTY_CASH\":\"1.00\",\"BANK\":\"-1.00\"}}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post(ADJUST_A).header("Authorization", ops).content("{\"note\":\"x\","
                        + "\"entries\":{\"SUSPENSE\":100,\"BANK\":\"-1.00\"}}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/v1/merchants/m_nobody/ledger/adjustments").header("Authorization", ops).content(BALANCED))
                .andExpect(status().isNotFound());
        verify(ledger, never()).post(any());
    }

    // ---- webhook secret rotation (§10.2, §14, D76) ----------------------------------------------------------------

    static final String ROTATE_A = "/v1/merchants/m_a/webhook-secrets/rotate";
    static final String NEW_SECRET = "test-only-rotated-webhook-secret-not-a-real-key";

    void rotatable(PostgresDirectory.Rotation outcome) {
        token("test-token-ops-rotator", Optional.empty(), List.of("breaks:read", "secrets:write"));
        token("test-token-a-rotator", Optional.of(A), List.of("breaks:read", "secrets:write"));
        when(cipher.encrypt(any(), anyString())).thenReturn(new byte[64]);
        when(directory.rotateWebhookSecret(any(), anyString(), any(), any(), any(), anyString())).thenReturn(outcome);
    }

    @Test
    void anOpsTokenRotatesASecretWithADayOfOverlapAndThisInstanceReloadsAtOnce() throws Exception {
        rotatable(PostgresDirectory.Rotation.ROTATED);
        List<byte[]> seen = new ArrayList<>();   // a copy taken during the call: the endpoint wipes the array afterwards
        List<byte[]> passed = new ArrayList<>();
        when(cipher.encrypt(any(), anyString())).thenAnswer(i -> {
            byte[] plain = i.getArgument(0);
            passed.add(plain);
            seen.add(plain.clone());
            return new byte[64];
        });
        String body = mvc.perform(post(ROTATE_A).header("Authorization", "Bearer test-token-ops-rotator")
                        .content("{\"kid\":\"k2\",\"secret\":\"" + NEW_SECRET + "\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.kid").value("k2"))
                .andExpect(jsonPath("$.active_from").value(NOW.toString()))
                .andExpect(jsonPath("$.previous_secrets_retire_at").value(NOW.plusSeconds(86_400).toString()))
                .andReturn().getResponse().getContentAsString();
        assertThat(body).as("the secret is never echoed").doesNotContain(NEW_SECRET);
        verify(cipher).encrypt(any(), eq("webhook-secret/m_a/k2"));
        assertThat(seen).singleElement().isEqualTo(NEW_SECRET.getBytes(StandardCharsets.UTF_8));
        assertThat(passed.getFirst()).as("the plaintext is wiped once encrypted").containsOnly((byte) 0);
        verify(directory).rotateWebhookSecret(eq(A), eq("k2"), any(), eq(NOW), eq(NOW.plusSeconds(86_400)),
                anyString());
        verify(refresher).refresh();
    }

    @Test
    void rotatingNeedsTheSecretsScopeAndAnOpsToken() throws Exception {
        rotatable(PostgresDirectory.Rotation.ROTATED);
        String body = "{\"kid\":\"k2\",\"secret\":\"" + NEW_SECRET + "\"}";
        mvc.perform(post(ROTATE_A).header("Authorization", "Bearer test-token-a-rotator").content(body))
                .andExpect(status().isForbidden());
        mvc.perform(post(ROTATE_A).header("Authorization", "Bearer test-token-ops").content(body))
                .andExpect(status().isForbidden());
        verify(directory, never()).rotateWebhookSecret(any(), anyString(), any(), any(), any(), anyString());
    }

    @Test
    void aWeakOrMalformedRotationIs400AndTheDirectorysRefusalsMapTo404And409() throws Exception {
        rotatable(PostgresDirectory.Rotation.ROTATED);
        String ops = "Bearer test-token-ops-rotator";
        mvc.perform(post(ROTATE_A).header("Authorization", ops).content("{\"kid\":\"k2\",\"secret\":\"short\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post(ROTATE_A).header("Authorization", ops)
                        .content("{\"kid\":\"k 2\",\"secret\":\"" + NEW_SECRET + "\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post(ROTATE_A).header("Authorization", ops)
                        .content("{\"kid\":\"k2\",\"secret\":\"" + NEW_SECRET + "\",\"overlap_seconds\":10}"))
                .andExpect(status().isBadRequest());
        verify(directory, never()).rotateWebhookSecret(any(), anyString(), any(), any(), any(), anyString());
        String body = "{\"kid\":\"k2\",\"secret\":\"" + NEW_SECRET + "\"}";
        for (var outcome : new PostgresDirectory.Rotation[] {PostgresDirectory.Rotation.IN_PROGRESS,
            PostgresDirectory.Rotation.KID_TAKEN}) {
            when(directory.rotateWebhookSecret(any(), anyString(), any(), any(), any(), anyString())).thenReturn(outcome);
            mvc.perform(post(ROTATE_A).header("Authorization", ops).content(body)).andExpect(status().isConflict());
        }
        when(directory.rotateWebhookSecret(any(), anyString(), any(), any(), any(), anyString()))
                .thenReturn(PostgresDirectory.Rotation.UNKNOWN_MERCHANT);
        mvc.perform(post(ROTATE_A).header("Authorization", ops).content(body)).andExpect(status().isNotFound());
        verify(refresher, never()).refresh();
    }

    // ---- customer contacts (D77) ----------------------------------------------------------------------------------

    @Test
    void aVpaSearchUsesTheBlindIndexWithinTheTokensMerchantAndAnswers503WhenStorageIsOff() throws Exception {
        when(contacts.enabled()).thenReturn(true);
        when(contacts.paymentsByVpa(A, "buyer@okaxis")).thenReturn(List.of(PaymentId.of("pay_2")));
        mvc.perform(get("/v1/payments/search?vpa=buyer@okaxis").header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.payments[0].payment_id").value("pay_2"))
                .andExpect(jsonPath("$.payments[0].merchant_id").value("m_a"));
        verify(contacts, never()).paymentsByVpa(eq(B), anyString());
        mvc.perform(get("/v1/payments/search?vpa=buyer@okaxis&merchantId=m_b").header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isForbidden());

        when(contacts.enabled()).thenReturn(false);
        mvc.perform(get("/v1/payments/search?vpa=buyer@okaxis").header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void aTimelineShowsTheCustomerMaskedNeverInClear() throws Exception {
        when(tx.orders()).thenReturn(Mockito.mock(OrderRepo.class));
        when(contacts.find(A, PaymentId.of("pay_2"))).thenReturn(Optional.of(new PiiCipher.Opened(
                Optional.of("buyer.one@okaxis"), Optional.of("9876543210"), Optional.empty())));
        String body = mvc.perform(get("/v1/payments/pay_2/timeline").header("Authorization", "Bearer test-token-a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.customer.vpa").value("bu***@okaxis"))
                .andExpect(jsonPath("$.customer.phone").value("98******10"))
                .andReturn().getResponse().getContentAsString();
        assertThat(body).doesNotContain("buyer.one@okaxis").doesNotContain("9876543210");
    }
}
