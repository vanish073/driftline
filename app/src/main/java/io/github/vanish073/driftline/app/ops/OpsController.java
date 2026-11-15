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
 * The ops read API (DESIGN §10.1): breaks, one break with its transitions, and a payment's timeline. Read-only, each
 * request in one Store transaction. A merchant token sees only its merchant; another merchant's break answers 404, so
 * a token cannot even learn that it exists. An ops token sees every merchant. Resolving breaks and the CSV export
 * arrive with the resolver (Shift 8) and the security work (Shift 9).
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
