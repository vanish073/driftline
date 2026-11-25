package io.github.vanish073.driftline.app.ops;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.ingest.BoundedBody;
import io.github.vanish073.driftline.app.merchant.MerchantRegistry;
import io.github.vanish073.driftline.app.merchant.SecretCipher;
import io.github.vanish073.driftline.app.merchant.SecretRefresher;
import io.github.vanish073.driftline.app.pii.CustomerContacts;
import io.github.vanish073.driftline.app.pii.PiiCipher;
import io.github.vanish073.driftline.app.pii.PiiMasking;
import io.github.vanish073.driftline.app.security.MerchantAccess;
import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakTransition;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.order.OrderState;
import io.github.vanish073.driftline.core.order.OrderTransition;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.SettlementRepo;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.resolver.ManualAdjustment;
import io.github.vanish073.driftline.core.resolver.ManualRequests;
import io.github.vanish073.driftline.core.resolver.ManualResolution;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.store.postgres.CustomerVault;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.StrictJson;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The ops read API (DESIGN §10.1): breaks, one break with its transitions (and, for a break about a settlement line,
 * that line with its witnesses), a payment's timeline, and the exception list as CSV (§6.4: ambiguous lines are
 * reported as ambiguous, with both witnesses). Read-only, each request in one Store transaction. A merchant token sees
 * only its merchant; another merchant's break answers 404, so a token cannot even learn that it exists. An ops token
 * sees every merchant. {@code POST /v1/breaks/{id}/resolve} closes an ESCALATED break by hand, with its audit row
 * ({@link ManualResolution}); it needs {@code breaks:resolve}. {@code POST /v1/merchants/{m}/ledger/adjustments}
 * posts a balanced correcting entry under its audit row ({@link ManualAdjustment}); it needs {@code ledger:adjust}.
 */
@RestController
@Profile(Roles.INGEST)
public class OpsController {

    static final int MAX_RESOLVE_BODY = 4_096;

    private final Store store;
    private final MerchantRegistry merchants;
    private final Clock clock;
    private final PostgresDirectory directory;
    private final SecretCipher cipher;
    private final SecretRefresher refresher;
    private final CustomerContacts contacts;

    public OpsController(Store store, MerchantRegistry merchants, Clock clock, PostgresDirectory directory,
                         SecretCipher cipher, SecretRefresher refresher, CustomerContacts contacts) {
        this.store = store;
        this.merchants = merchants;
        this.clock = clock;
        this.directory = directory;
        this.cipher = cipher;
        this.refresher = refresher;
        this.contacts = contacts;
    }

    /** Overlap bounds for a webhook secret rotation (D76): long enough for the gateway to switch, at most a week. */
    static final Duration MIN_OVERLAP = Duration.ofMinutes(1);
    static final Duration MAX_OVERLAP = Duration.ofDays(7);
    static final Duration DEFAULT_OVERLAP = Duration.ofHours(24);

    /**
     * {@code POST /v1/merchants/{m}/webhook-secrets/rotate} (§10.2, §14, D76) with
     * {@code {"kid":"k2","secret":"…","overlap_seconds":86400}}: the secret the gateway issued becomes active now, the
     * current one stays valid for the overlap (default 24 h, 1 min to 7 days), and an audit row records it, never the
     * secret. Scope {@code secrets:write}, ops tokens only. The secret is encrypted before it is stored (D48); this
     * instance reloads at once and every other within its refresh interval. 201; 400 for a malformed request or a secret
     * under 32 bytes; 404 for an unknown merchant; 409 while a rotation is in progress or if the key id was used before.
     */
    @PostMapping("/v1/merchants/{merchantId}/webhook-secrets/rotate")
    @PreAuthorize("hasAuthority('SCOPE_secrets:write')")
    public ResponseEntity<?> rotateSecret(Authentication auth, @PathVariable String merchantId,
                                          HttpServletRequest request,
                                          @RequestHeader(name = IDEMPOTENCY_KEY, required = false) Optional<String> key)
            throws IOException {
        if (MerchantAccess.boundMerchant(auth).isPresent()) {
            return error(403, "only an ops token may rotate a webhook secret");
        }
        Optional<ResponseEntity<?>> badKey = requireKey(key);
        if (badKey.isPresent()) {
            return badKey.get();
        }
        MerchantId m;
        try {
            m = MerchantId.of(merchantId);
        } catch (IllegalArgumentException e) {
            return error(404, "unknown merchant");
        }
        Optional<byte[]> body = BoundedBody.read(request, MAX_RESOLVE_BODY);
        if (body.isEmpty()) {
            return error(413, "body larger than " + MAX_RESOLVE_BODY + " bytes");
        }
        String kid;
        byte[] secret;
        Duration overlap;
        try {
            JsonObject root = StrictJson.parse(body.get());
            kid = IdRules.requireToken("kid", root.string("kid"));
            secret = root.string("secret").getBytes(StandardCharsets.UTF_8);
            overlap = root.has("overlap_seconds") ? Duration.ofSeconds(root.integer("overlap_seconds"))
                    : DEFAULT_OVERLAP;
        } catch (IllegalArgumentException e) {   // MalformedJson or a bad key id
            return error(400, e.getMessage() == null ? "malformed rotation" : e.getMessage());
        }
        if (secret.length < 32 || secret.length > 512) {
            return error(400, "a webhook secret is 32 to 512 bytes");
        }
        if (overlap.compareTo(MIN_OVERLAP) < 0 || overlap.compareTo(MAX_OVERLAP) > 0) {
            return error(400, "overlap_seconds must be between 60 and 604800");
        }
        Instant now = clock.now();
        PostgresDirectory.RotationResult result;
        try {
            // the secret's own hash is part of the request's, so a repeat with another secret is refused, not replayed
            Sha256 asked = ManualRequests.hash(PostgresDirectory.ROTATE_ACTION, m.value(), kid,
                    Long.toString(overlap.toSeconds()), Sha256.of(secret).hex());
            result = directory.rotateWebhookSecret(m, kid, cipher.encrypt(secret, MerchantRegistry.context(m, kid)),
                    now, now.plus(overlap), String.valueOf(auth.getPrincipal()), key.get(), asked);
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        } catch (StoreConflict e) {
            return error(409, KEY_IN_FLIGHT);
        } finally {
            Arrays.fill(secret, (byte) 0);
        }
        return switch (result.status()) {
            case UNKNOWN_MERCHANT -> error(404, "unknown merchant");
            case IN_PROGRESS -> error(409, "a rotation is in progress: two secrets are active until its overlap ends");
            case KID_TAKEN -> error(409, "this key id was used before; choose a new one");
            case KEY_REUSED -> error(409, KEY_REUSED);
            case ROTATED, REPLAYED -> {
                if (result.status() == PostgresDirectory.Rotation.ROTATED) {
                    refresher.refresh();
                }
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("kid", kid);
                v.put("active_from", result.activeFrom().orElseThrow().toString());
                v.put("previous_secrets_retire_at", result.retireAt().orElseThrow().toString());
                yield replayed(ResponseEntity.status(201), result.status() == PostgresDirectory.Rotation.REPLAYED)
                        .body(v);
            }
        };
    }

    /**
     * {@code POST /v1/breaks/{id}/resolve} with {@code {"note":"…"}} (§8.1, §10.1, §12): ESCALATED → RESOLVED_MANUAL and
     * an audit row naming the token, in one transaction. 404 for a break the token may not see, 409 for one that is not
     * ESCALATED (including one another person resolved a moment ago).
     */
    @PostMapping("/v1/breaks/{breakId}/resolve")
    @PreAuthorize("hasAuthority('SCOPE_breaks:resolve')")
    public ResponseEntity<?> resolve(Authentication auth, @PathVariable String breakId, HttpServletRequest request,
                                     @RequestHeader(name = IDEMPOTENCY_KEY, required = false) Optional<String> key)
            throws IOException {
        Optional<ResponseEntity<?>> badKey = requireKey(key);
        if (badKey.isPresent()) {
            return badKey.get();
        }
        Optional<byte[]> body = BoundedBody.read(request, MAX_RESOLVE_BODY);
        if (body.isEmpty()) {
            return error(413, "body larger than " + MAX_RESOLVE_BODY + " bytes");
        }
        String note;
        try {
            note = ManualResolution.requireNote(StrictJson.parse(body.get()).string("note"));
        } catch (IllegalArgumentException e) {   // MalformedJson included
            return error(400, e.getMessage());
        }
        Optional<MerchantId> bound = MerchantAccess.boundMerchant(auth);
        String actor = String.valueOf(auth.getPrincipal());
        ManualResolution.Result result;
        try {
            Instant now = clock.now();
            result = store.transact("ops-resolve", tx -> ManualResolution.resolve(tx, breakId, bound, actor, note,
                    key.get(), now));
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        } catch (StoreConflict | IllegalArgumentException e) {   // a concurrent resolution, or a concurrent repeat
            return error(409, "the break changed while it was being resolved; read it again");
        }
        return switch (result) {
            case ManualResolution.NotFound n -> error(404, "no such break");
            case ManualResolution.NotEscalated n -> error(409, "the break is " + n.state()
                    + "; only an ESCALATED break can be resolved by hand");
            case ManualResolution.KeyReused k -> error(409, KEY_REUSED);
            case ManualResolution.Resolved r -> {
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("break_id", r.breakId());
                v.put("state", BreakState.RESOLVED_MANUAL.name());
                v.put("resolved_at", r.at().toString());
                v.put("compensation", r.compensation().toString());
                v.put("audit_seq", r.auditSeq());
                yield replayed(ResponseEntity.ok(), r.replayed()).body(v);
            }
        };
    }

    /**
     * {@code POST /v1/merchants/{m}/ledger/adjustments} (§7, D8, D74) with
     * {@code {"note":"…","break_id":"…","entries":{"BANK":"-12.34","SETTLEMENT_DIFFERENCES":"12.34"}}}: a balanced
     * correcting entry, posted under its own audit row ({@link ManualAdjustment}). Scope {@code ledger:adjust}, ops
     * tokens only: a merchant does not adjust its own books. Amounts are signed, debit positive; {@code break_id} is
     * optional. 201 with the audit sequence number and the ledger cause; 400 for an unbalanced or malformed request;
     * 404 for an unknown merchant or break.
     */
    @PostMapping("/v1/merchants/{merchantId}/ledger/adjustments")
    @PreAuthorize("hasAuthority('SCOPE_ledger:adjust')")
    public ResponseEntity<?> adjust(Authentication auth, @PathVariable String merchantId, HttpServletRequest request,
                                    @RequestHeader(name = IDEMPOTENCY_KEY, required = false) Optional<String> key)
            throws IOException {
        if (MerchantAccess.boundMerchant(auth).isPresent()) {
            return error(403, "only an ops token may adjust a merchant's books");
        }
        Optional<ResponseEntity<?>> badKey = requireKey(key);
        if (badKey.isPresent()) {
            return badKey.get();
        }
        MerchantId m;
        try {
            m = MerchantId.of(merchantId);
        } catch (IllegalArgumentException e) {
            return error(404, "unknown merchant");
        }
        if (merchants.context(m).isEmpty()) {
            return error(404, "unknown merchant");
        }
        Optional<byte[]> body = BoundedBody.read(request, MAX_RESOLVE_BODY);
        if (body.isEmpty()) {
            return error(413, "body larger than " + MAX_RESOLVE_BODY + " bytes");
        }
        String note;
        Optional<String> breakId;
        TreeMap<AccountCode, Money> amounts = new TreeMap<>();
        try {
            JsonObject root = StrictJson.parse(body.get());
            note = ManualResolution.requireNote(root.string("note"));
            breakId = Optional.ofNullable(root.optionalString("break_id"));
            for (Map.Entry<String, Object> e : root.object("entries").fields().entrySet()) {
                if (!(e.getValue() instanceof String amount)) {
                    return error(400, "each entry is an account code and a signed amount as a string");
                }
                amounts.put(AccountCode.valueOf(e.getKey()), Money.parseSigned(amount));
            }
        } catch (IllegalArgumentException e) {   // MalformedJson, an unknown account or a bad amount
            return error(400, e.getMessage() == null ? "malformed adjustment" : e.getMessage());
        }
        String actor = String.valueOf(auth.getPrincipal());
        ManualAdjustment.Result result;
        try {
            Instant now = clock.now();
            result = store.transact("ops-adjust", tx -> ManualAdjustment.post(tx, m, actor, note, breakId, amounts,
                    key.get(), now));
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        } catch (StoreConflict e) {   // a concurrent repeat of this request committed first
            return error(409, KEY_IN_FLIGHT);
        } catch (IllegalArgumentException e) {   // LedgerRejected, or an amount above AmountLimits (D93)
            return error(400, e.getMessage());
        }
        return switch (result) {
            case ManualAdjustment.BreakNotFound n -> error(404, "no such break for this merchant");
            case ManualAdjustment.KeyReused k -> error(409, KEY_REUSED);
            case ManualAdjustment.Posted posted -> {
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("audit_seq", posted.auditSeq());
                v.put("cause", posted.txn().cause().type() + ":" + posted.txn().cause().id());
                Map<String, String> entries = new LinkedHashMap<>();
                posted.txn().entries().forEach(en -> entries.put(en.account().name(), en.amount().toString()));
                v.put("entries", entries);
                yield replayed(ResponseEntity.status(201), posted.replayed()).body(v);
            }
        };
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

    /** A customer's contact as the ops API shows it: masked, never in clear (D77). */
    static Map<String, Object> maskedContact(PiiCipher.Opened c) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("vpa", c.vpa().map(PiiMasking::maskAddress).orElse(null));
        v.put("phone", c.phone().map(PiiMasking::maskPhone).orElse(null));
        v.put("email", c.email().map(PiiMasking::maskAddress).orElse(null));
        return v;
    }

    /**
     * {@code POST /v1/merchants/{m}/customers/erase} with {@code {"payment_id":"pay_1","note":"…"}} or
     * {@code {"vpa":"name@bank","note":"…"}} (Shift 11, D99): erases the stored contact of one payment, or of every
     * payment of the merchant whose customer used that VPA (found through the blind index, at most 1000), leaving
     * tombstones so the contacts are never stored again. Scope {@code pii:erase}, ops tokens only, with an
     * {@code Idempotency-Key}; the audit row lists payment ids and a count, never a contact. 200 with what was erased;
     * 404 for an unknown merchant; 503 for a VPA while personal data storage is off.
     */
    @PostMapping("/v1/merchants/{merchantId}/customers/erase")
    @PreAuthorize("hasAuthority('SCOPE_pii:erase')")
    public ResponseEntity<?> eraseCustomer(Authentication auth, @PathVariable String merchantId,
                                           HttpServletRequest request,
                                           @RequestHeader(name = IDEMPOTENCY_KEY, required = false)
                                           Optional<String> key) throws IOException {
        if (MerchantAccess.boundMerchant(auth).isPresent()) {
            return error(403, "only an ops token may erase customers' contacts");
        }
        Optional<ResponseEntity<?>> badKey = requireKey(key);
        if (badKey.isPresent()) {
            return badKey.get();
        }
        MerchantId m;
        try {
            m = MerchantId.of(merchantId);
        } catch (IllegalArgumentException e) {
            return error(404, "unknown merchant");
        }
        if (merchants.context(m).isEmpty()) {
            return error(404, "unknown merchant");
        }
        Optional<byte[]> body = BoundedBody.read(request, MAX_RESOLVE_BODY);
        if (body.isEmpty()) {
            return error(413, "body larger than " + MAX_RESOLVE_BODY + " bytes");
        }
        String note;
        Optional<String> payment;
        Optional<String> vpa;
        try {
            JsonObject root = StrictJson.parse(body.get());
            note = ManualResolution.requireNote(root.string("note"));
            payment = Optional.ofNullable(root.optionalString("payment_id"));
            vpa = Optional.ofNullable(root.optionalString("vpa"));
            payment.ifPresent(PaymentId::of);
        } catch (IllegalArgumentException e) {
            return error(400, e.getMessage() == null ? "malformed erasure" : e.getMessage());
        }
        if (payment.isPresent() == vpa.isPresent()) {
            return error(400, "name exactly one of payment_id and vpa");
        }
        List<PaymentId> targets;
        Sha256 asked;
        try {
            if (payment.isPresent()) {
                targets = List.of(PaymentId.of(payment.get()));
                asked = ManualRequests.hash(CustomerVault.ERASE_ACTION, m.value(), "payment", payment.get(), note);
            } else {
                if (!contacts.enabled()) {
                    return error(503, "personal data storage is not configured");
                }
                Optional<String> index = contacts.vpaIndex(m, vpa.get());
                if (index.isEmpty()) {
                    return error(400, "vpa is not a UPI address");
                }
                targets = contacts.vault().paymentsByVpa(m, index.get());
                asked = ManualRequests.hash(CustomerVault.ERASE_ACTION, m.value(), "vpa", index.get(), note);
            }
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        }
        CustomerVault.Erasure result;
        try {
            result = contacts.vault().erase(m, targets, clock.now(), String.valueOf(auth.getPrincipal()), note,
                    key.get(), asked);
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        } catch (StoreConflict e) {
            return error(409, KEY_IN_FLIGHT);
        }
        return switch (result) {
            case CustomerVault.ErasureKeyReused r -> error(409, KEY_REUSED);
            case CustomerVault.ErasureReplayed r -> replayed(ResponseEntity.ok(), true).body(Map.of("erased", r.count()));
            case CustomerVault.Erased e -> {
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("erased", e.paymentIds().size());
                v.put("payment_ids", e.paymentIds().stream().map(PaymentId::value).toList());
                yield ResponseEntity.ok(v);
            }
        };
    }

    /**
     * {@code GET /v1/payments/search?vpa=&merchantId=} (D77): the payments whose customer paid with this UPI VPA, found
     * through the blind index, so the VPA itself is never compared or stored in clear. A merchant token searches its own
     * merchant; an ops token searches the named merchant, or every merchant. 503 if personal data storage is off.
     */
    @GetMapping("/v1/payments/search")
    @PreAuthorize("hasAuthority('SCOPE_breaks:read')")
    public ResponseEntity<?> searchByVpa(Authentication auth, @RequestParam String vpa,
                                         @RequestParam Optional<String> merchantId) {
        if (!contacts.enabled()) {
            return error(503, "personal data storage is not configured on this instance");
        }
        Optional<MerchantId> bound = MerchantAccess.boundMerchant(auth);
        List<MerchantId> scope = new ArrayList<>();
        try {
            Optional<MerchantId> requested = merchantId.map(MerchantId::of);
            if (bound.isPresent() && requested.isPresent() && !requested.get().equals(bound.get())) {
                return error(403, "forbidden");
            }
            if (bound.isPresent()) {
                scope.add(bound.get());
            } else if (requested.isPresent()) {
                scope.add(requested.get());
            } else {
                merchants.merchants().stream().map(MerchantTerms::merchantId).forEach(scope::add);
            }
        } catch (IllegalArgumentException e) {
            return error(400, "merchantId is not a merchant id");
        }
        try {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (MerchantId m : scope) {
                for (PaymentId p : contacts.paymentsByVpa(m, vpa)) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("merchant_id", m.value());
                    row.put("payment_id", p.value());
                    rows.add(row);
                }
            }
            return ResponseEntity.ok(Map.of("payments", rows, "count", rows.size()));
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        }
    }

    /**
     * {@code GET /v1/payments/{id}/timeline}: the payment, its decisions, its order, its settlement match and its breaks,
     * and its customer's contact, masked (D77). Payment ids are unique per merchant, so an ops token names the merchant
     * with {@code merchantId}.
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
            found.ifPresent(v -> v.put("customer", contacts.find(merchant, payment).map(OpsController::maskedContact)
                    .orElse(null)));
            return found.<ResponseEntity<?>>map(ResponseEntity::ok).orElseGet(() -> error(404, "no such payment"));
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        }
    }

    /**
     * {@code GET /v1/exports/breaks.csv?merchantId=}: every break as one CSV row, in columns that fit a reconciliation
     * sheet (the desk research, D54): the payment, its order and expected gross, fee, GST and net; the settlement
     * line, its date, its UTR and amount; the reason; and an ambiguous line's two witnesses, payment ids separated by
     * spaces. RFC 4180 quoting, with formula-like cells neutralised (§14); UTF-8. A refusal (400, 403, 503) is the usual JSON error, so the CSV media type is set
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
    /**
     * One CSV cell (RFC 4180), safe to open in a spreadsheet (§14, OWASP "CSV injection"): a cell that starts with
     * {@code = + - @}, a tab or a carriage return would be read as a formula, so it gets a leading apostrophe, which
     * spreadsheets show as text, and is quoted. A negative amount therefore exports as text; the export has none today.
     */
    static String csvField(String value) {
        boolean formula = !value.isEmpty() && "=+-@\t\r".indexOf(value.charAt(0)) >= 0;
        String cell = formula ? "'" + value : value;
        if (!formula && cell.indexOf(',') < 0 && cell.indexOf('"') < 0 && cell.indexOf('\n') < 0 && cell.indexOf('\r') < 0) {
            return cell;
        }
        return '"' + cell.replace("\"", "\"\"") + '"';
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

    /** The header every manual POST requires (D94); the same name as ingest's (§10.3). */
    static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    static final String KEY_REUSED = "this Idempotency-Key was used before for a different request";
    static final String KEY_IN_FLIGHT = "a request with this Idempotency-Key committed a moment ago; retry it";

    /** 400 unless a well-formed {@code Idempotency-Key} was sent ({@code IdRules.requireExternalKey}, D92, D94). */
    static Optional<ResponseEntity<?>> requireKey(Optional<String> key) {
        if (key.isEmpty()) {
            return Optional.of(error(400, "the " + IDEMPOTENCY_KEY + " header is required"));
        }
        try {
            IdRules.requireExternalKey(IDEMPOTENCY_KEY, key.get());
            return Optional.empty();
        } catch (IllegalArgumentException e) {
            return Optional.of(error(400, e.getMessage()));
        }
    }

    /** A repeat's answer is the first one's, marked so the caller can tell (D94). */
    static ResponseEntity.BodyBuilder replayed(ResponseEntity.BodyBuilder b, boolean replayed) {
        return replayed ? b.header("Idempotent-Replayed", "true") : b;
    }

    static ResponseEntity<Map<String, Object>> error(int status, String reason) {
        return ResponseEntity.status(status).body(Map.of("error", reason));
    }
}
