package io.github.vanish073.driftline.app.ops;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.merchant.MerchantRegistry;
import io.github.vanish073.driftline.app.security.MerchantAccess;
import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakTransition;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.order.OrderState;
import io.github.vanish073.driftline.core.order.OrderTransition;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import io.github.vanish073.driftline.core.port.SettlementRepo;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The ops read API (DESIGN §10.1): breaks, one break with its transitions (and, for a break about a settlement line,
 * that line with its witnesses), a payment's timeline, and the exception list as CSV (§6.4: ambiguous lines are
 * reported as ambiguous, with both witnesses). Read-only, each request in one Store transaction. A merchant token sees
 * only its merchant; another merchant's break answers 404, so a token cannot even learn that it exists. An ops token
 * sees every merchant. Resolving breaks arrives with the resolver (Shift 8).
 */
@RestController
@Profile(Roles.INGEST)
public class OpsController {

    private final Store store;
    private final MerchantRegistry merchants;

    public OpsController(Store store, MerchantRegistry merchants) {
        this.store = store;
        this.merchants = merchants;
    }

    /** {@code GET /v1/breaks?state=&type=&merchantId=}. */
    @GetMapping("/v1/breaks")
    @PreAuthorize("hasAuthority('SCOPE_breaks:read')")
    public ResponseEntity<?> breaks(Authentication auth, @RequestParam Optional<String> state,
                                    @RequestParam Optional<String> type, @RequestParam Optional<String> merchantId) {
        Optional<BreakState> stateFilter;
        Optional<BreakType> typeFilter;
        try {
            stateFilter = state.map(s -> BreakState.valueOf(s.toUpperCase(Locale.ROOT)));
            typeFilter = type.map(t -> BreakType.valueOf(t.toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException e) {
            return error(400, "unknown state or type");
        }
        Optional<MerchantId> bound = MerchantAccess.boundMerchant(auth);
        List<MerchantId> scope = new ArrayList<>();
        if (merchantId.isPresent()) {
            MerchantId requested;
            try {
                requested = MerchantId.of(merchantId.get());
            } catch (IllegalArgumentException e) {
                return error(400, "merchantId is not a merchant id");
            }
            if (bound.isPresent() && !bound.get().equals(requested)) {
                return error(403, "forbidden");
            }
            scope.add(requested);
        } else if (bound.isPresent()) {
            scope.add(bound.get());
        } else {
            merchants.merchants().stream().map(MerchantTerms::merchantId).forEach(scope::add);
        }
        try {
            List<Map<String, Object>> out = store.transact("ops-breaks", tx -> {
                List<Map<String, Object>> rows = new ArrayList<>();
                for (MerchantId m : scope) {
                    for (Break b : tx.breaks().forMerchant(m)) {
                        if (stateFilter.map(s -> s == b.state()).orElse(true)
                                && typeFilter.map(t -> t == b.type()).orElse(true)) {
                            rows.add(view(b));
                        }
                    }
                }
                return rows;
            });
            return ResponseEntity.ok(Map.of("breaks", out, "count", out.size()));
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        }
    }

    /** {@code GET /v1/breaks/{id}}, with its transitions. */
    @GetMapping("/v1/breaks/{breakId}")
    @PreAuthorize("hasAuthority('SCOPE_breaks:read')")
    public ResponseEntity<?> oneBreak(Authentication auth, @PathVariable String breakId) {
        Optional<MerchantId> bound = MerchantAccess.boundMerchant(auth);
        try {
            Optional<Map<String, Object>> found = store.transact("ops-break", tx -> tx.breaks().byId(breakId)
                    .filter(b -> bound.map(m -> m.equals(b.merchantId())).orElse(true))
                    .map(b -> {
                        Map<String, Object> v = view(b);
                        List<Map<String, Object>> transitions = new ArrayList<>();
                        for (BreakTransition t : tx.breaks().transitions(b.merchantId(), b.breakId())) {
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("seq", t.seq());
                            row.put("from", t.from().map(Enum::name).orElse(null));
                            row.put("to", t.to().name());
                            row.put("at", t.at().toString());
                            row.put("reason", t.reason());
                            transitions.add(row);
                        }
                        v.put("transitions", transitions);
                        lineOf(tx, b).ifPresent(l -> v.put("line", lineView(l)));
                        return v;
                    }));
            return found.<ResponseEntity<?>>map(ResponseEntity::ok).orElseGet(() -> error(404, "no such break"));
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        }
    }

    /**
     * {@code GET /v1/payments/{id}/timeline}: the payment, its decisions, its order, its settlement match and its breaks.
     * Payment ids are unique per merchant, so an ops token names the merchant with {@code merchantId}.
     */
    @GetMapping("/v1/payments/{paymentId}/timeline")
    @PreAuthorize("hasAuthority('SCOPE_breaks:read')")
    public ResponseEntity<?> timeline(Authentication auth, @PathVariable String paymentId,
                                      @RequestParam Optional<String> merchantId) {
        Optional<MerchantId> bound = MerchantAccess.boundMerchant(auth);
        MerchantId merchant;
        PaymentId payment;
        try {
            payment = PaymentId.of(paymentId);
            if (bound.isPresent()) {
                if (merchantId.isPresent() && !merchantId.get().equals(bound.get().value())) {
                    return error(403, "forbidden");
                }
                merchant = bound.get();
            } else if (merchantId.isPresent()) {
                merchant = MerchantId.of(merchantId.get());
            } else {
                return error(400, "an ops token names the merchant with ?merchantId=");
            }
        } catch (IllegalArgumentException e) {
            return error(400, "not a payment or merchant id");
        }
        try {
            Optional<Map<String, Object>> found = store.transact("ops-timeline", tx -> {
                Optional<PaymentState> p = tx.payments().find(merchant, payment);
                if (p.isEmpty()) {
                    return Optional.empty();
                }
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("merchant_id", merchant.value());
                v.put("payment", paymentView(p.get()));
                List<Map<String, Object>> decisions = new ArrayList<>();
                for (PaymentTransition t : tx.payments().transitions(merchant, payment)) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("event_time", t.eventTime().toString());
                    row.put("event_key", t.eventKey());
                    row.put("from", t.from().name());
                    row.put("to", t.to().name());
                    row.put("rule", t.rule().name());
                    decisions.add(row);
                }
                v.put("payment_transitions", decisions);
                p.get().orderId().flatMap(o -> tx.orders().find(merchant, o)).ifPresent(o -> {
                    v.put("order", orderView(o));
                    List<Map<String, Object>> orderRows = new ArrayList<>();
                    for (OrderTransition t : tx.orders().transitions(merchant, o.orderId())) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("event_time", t.eventTime().toString());
                        row.put("event_key", t.eventKey());
                        row.put("version", t.version());
                        row.put("from", t.from().name());
                        row.put("to", t.to().name());
                        row.put("rule", t.rule().name());
                        orderRows.add(row);
                    }
                    v.put("order_transitions", orderRows);
                });
                Optional<SettlementRepo.Match> match = tx.settlement().matchOf(merchant, payment);
                v.put("settlement_match", match.map(m -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("file_id", m.fileId());
                    row.put("line_no", m.lineNo());
                    row.put("how", m.how().name());
                    return row;
                }).orElse(null));
                List<Map<String, Object>> breaks = new ArrayList<>();
                for (BreakType type : BreakType.values()) {
                    tx.breaks().find(merchant, type, payment.value()).ifPresent(b -> breaks.add(view(b)));
                }
                v.put("breaks", breaks);
                return Optional.of(v);
            });
            return found.<ResponseEntity<?>>map(ResponseEntity::ok).orElseGet(() -> error(404, "no such payment"));
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        }
    }

    /**
     * {@code GET /v1/exports/breaks.csv?merchantId=}: every break as one CSV row, in columns that fit a reconciliation
     * sheet (the desk research, D54): the payment, its order and expected gross, fee, GST and net; the settlement
     * line, its date, its UTR and amount; the reason; and an ambiguous line's two witnesses, payment ids separated by
     * spaces. RFC 4180 quoting; UTF-8. A refusal (400, 403, 503) is the usual JSON error, so the CSV media type is set
     * on success only.
     */
    @GetMapping("/v1/exports/breaks.csv")
    @PreAuthorize("hasAuthority('SCOPE_breaks:read')")
    public ResponseEntity<?> exportCsv(Authentication auth, @RequestParam Optional<String> merchantId) {
        Optional<MerchantId> bound = MerchantAccess.boundMerchant(auth);
        List<MerchantId> scope = new ArrayList<>();
        try {
            Optional<MerchantId> requested = merchantId.map(MerchantId::of);
            if (bound.isPresent() && requested.isPresent() && !requested.get().equals(bound.get())) {
                return error(403, "forbidden");
            }
            if (requested.isPresent()) {
                scope.add(requested.get());
            } else if (bound.isPresent()) {
                scope.add(bound.get());
            } else {
                merchants.merchants().stream().map(MerchantTerms::merchantId).forEach(scope::add);
            }
        } catch (IllegalArgumentException e) {
            return error(400, "not a merchant id");
        }
        try {
            String csv = store.transact("ops-export", tx -> {
                StringBuilder out = new StringBuilder(String.join(",", CSV_COLUMNS)).append("\r\n");
                for (MerchantId m : scope) {
                    for (Break b : tx.breaks().forMerchant(m)) {
                        out.append(String.join(",", csvRow(tx, b).stream().map(OpsController::csvField).toList()))
                                .append("\r\n");
                    }
                }
                return out.toString();
            });
            return ResponseEntity.ok()
                    .header("Content-Type", "text/csv; charset=utf-8")
                    .header("Content-Disposition", "attachment; filename=\"driftline-breaks.csv\"")
                    .body(csv);
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        }
    }

    static final List<String> CSV_COLUMNS = List.of("break_id", "merchant_id", "type", "kind", "state", "detected_at",
            "subject", "payment_id", "order_id", "gross", "expected_fee", "expected_gst", "expected_net", "file_id",
            "line_no", "settlement_date", "batch_id", "line_amount", "line_status", "reason", "witness_1",
            "witness_2");

    /** Breaks whose subject is a payment id. */
    static final List<BreakType> PAYMENT_TYPES = List.of(BreakType.DEBITED_NO_ORDER, BreakType.LATE_SUCCESS,
            BreakType.DUPLICATE_CAPTURE, BreakType.MISSING_SETTLEMENT);
    /** Breaks whose subject is a settlement line, {@code fileId:lineNo}. */
    static final List<BreakType> LINE_TYPES = List.of(BreakType.SHORT_SETTLEMENT, BreakType.ORPHAN_SETTLEMENT,
            BreakType.OVER_SETTLEMENT, BreakType.SETTLEMENT_CONFLICT, BreakType.INFERRED_MATCH_CHALLENGED);

    private List<String> csvRow(Tx tx, Break b) {
        List<String> row = new ArrayList<>(List.of(b.breakId(), b.merchantId().value(), b.type().name(),
                b.type().kind().name(), b.state().name(), b.detectedAt().toString(), b.subjectKey()));
        Optional<PaymentState> p = PAYMENT_TYPES.contains(b.type())
                ? tx.payments().find(b.merchantId(), PaymentId.of(b.subjectKey())) : Optional.empty();
        row.add(p.map(x -> x.paymentId().value()).orElse(""));
        row.add(p.flatMap(PaymentState::orderId).map(o -> o.value()).orElse(""));
        Optional<FeeSchedule.Breakdown> expected = p.flatMap(PaymentState::amount).filter(Money::isPositive)
                .flatMap(gross -> merchants.merchants().stream().filter(t -> t.merchantId().equals(b.merchantId()))
                        .findFirst().map(t -> t.fees().expected(gross)));
        row.add(expected.map(e -> e.gross().toString()).orElse(""));
        row.add(expected.map(e -> e.fee().toString()).orElse(""));
        row.add(expected.map(e -> e.gst().toString()).orElse(""));
        row.add(expected.map(e -> e.net().toString()).orElse(""));
        Optional<SettlementRepo.Line> line = lineOf(tx, b);
        row.add(line.map(SettlementRepo.Line::fileId).orElse(""));
        row.add(line.map(l -> Integer.toString(l.lineNo())).orElse(""));
        row.add(line.map(l -> l.settlementDate().toString()).orElse(""));
        row.add(line.flatMap(SettlementRepo.Line::batchId).orElse(""));
        row.add(line.map(l -> l.amount().toString()).orElse(""));
        row.add(line.map(l -> l.status().name()).orElse(""));
        row.add(b.evidence().getOrDefault("reason", ""));
        List<List<PaymentId>> witnesses = line.map(SettlementRepo.Line::witnesses).orElse(List.of());
        for (int i = 0; i < 2; i++) {
            row.add(i < witnesses.size()
                    ? String.join(" ", witnesses.get(i).stream().map(PaymentId::value).toList()) : "");
        }
        return row;
    }

    /** The settlement line a break is about, if its subject is one ({@code fileId:lineNo}). */
    private static Optional<SettlementRepo.Line> lineOf(Tx tx, Break b) {
        if (!LINE_TYPES.contains(b.type())) {
            return Optional.empty();
        }
        int colon = b.subjectKey().lastIndexOf(':');
        if (colon < 1) {
            return Optional.empty();
        }
        try {
            int lineNo = Integer.parseInt(b.subjectKey().substring(colon + 1));
            return tx.settlement().line(b.merchantId(), b.subjectKey().substring(0, colon), lineNo);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private static Map<String, Object> lineView(SettlementRepo.Line l) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("file_id", l.fileId());
        v.put("line_no", l.lineNo());
        v.put("settlement_date", l.settlementDate().toString());
        v.put("amount", l.amount().toString());
        v.put("reference", l.reference().orElse(null));
        v.put("batch_id", l.batchId().orElse(null));
        v.put("status", l.status().name());
        v.put("candidate_hash", l.candidateHash().map(h -> h.hex()).orElse(null));
        v.put("witnesses", l.witnesses().stream().map(w -> w.stream().map(PaymentId::value).toList()).toList());
        return v;
    }

    /** RFC 4180: quote a field that holds a comma, a quote or a line break, doubling any quote. */
    static String csvField(String value) {
        if (value.indexOf(',') < 0 && value.indexOf('"') < 0 && value.indexOf('\n') < 0 && value.indexOf('\r') < 0) {
            return value;
        }
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static Map<String, Object> view(Break b) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("break_id", b.breakId());
        v.put("merchant_id", b.merchantId().value());
        v.put("kind", b.type().kind().name());
        v.put("type", b.type().name());
        v.put("subject_key", b.subjectKey());
        v.put("state", b.state().name());
        v.put("detected_at", b.detectedAt().toString());
        v.put("version", b.version());
        v.put("evidence", new LinkedHashMap<>(b.evidence()));
        return v;
    }

    private static Map<String, Object> paymentView(PaymentState p) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("payment_id", p.paymentId().value());
        v.put("status", p.status().name());
        v.put("order_id", p.orderId().map(o -> o.value()).orElse(null));
        v.put("amount_paise", p.amount().map(Money::paise).orElse(null));
        v.put("success_at", p.successAt().map(Instant::toString).orElse(null));
        v.put("failed_at", p.failedAt().map(Instant::toString).orElse(null));
        v.put("conflict", p.conflict());
        return v;
    }

    private static Map<String, Object> orderView(OrderState o) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("order_id", o.orderId().value());
        v.put("status", o.status().name());
        v.put("version", o.version());
        v.put("amount_paise", o.amount().map(Money::paise).orElse(null));
        v.put("expires_at", o.expiresAt().map(Instant::toString).orElse(null));
        v.put("paid_with", o.paidWith().map(PaymentId::value).orElse(null));
        return v;
    }

    private static ResponseEntity<Map<String, Object>> error(int status, String reason) {
        return ResponseEntity.status(status).body(Map.of("error", reason));
    }
}
