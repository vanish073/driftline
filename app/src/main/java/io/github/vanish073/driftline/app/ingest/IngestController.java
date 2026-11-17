package io.github.vanish073.driftline.app.ingest;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.kafka.EventPublisher;
import io.github.vanish073.driftline.app.merchant.MerchantRegistry;
import io.github.vanish073.driftline.app.pii.CustomerContacts;
import io.github.vanish073.driftline.app.security.MerchantAccess;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.store.postgres.SettlementInbox;
import io.github.vanish073.driftline.wire.call.CallWire;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.ingest.IngestNormaliser;
import io.github.vanish073.driftline.wire.ingest.IngestResult;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import io.github.vanish073.driftline.wire.json.MalformedJson;
import io.github.vanish073.driftline.wire.settlement.MalformedSettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFileParser;
import io.github.vanish073.driftline.wire.webhook.CustomerContact;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * The ingest API (DESIGN §10.1–§10.3). Stateless (D3): it never dedupes; it verifies, normalises with the same pure
 * {@link IngestNormaliser} the simulator runs, appends to Kafka, and answers {@code 202 {accepted, key}} only after
 * Kafka acknowledged the record. The answer is the same for every retry of the same request; the matcher's dedupe makes
 * retries harmless. The event log being unavailable is a {@code 503}, which senders retry with the same key.
 */
@RestController
@Profile(Roles.INGEST)
public class IngestController {

    /** Settlement files are larger than events; this bounds one upload (the database checks it too). */
    static final int MAX_SETTLEMENT_BYTES = SettlementInbox.MAX_BYTES;
    private static final Logger LOG = LoggerFactory.getLogger(IngestController.class);

    private final MerchantRegistry merchants;
    private final EventPublisher publisher;
    private final SettlementInbox inbox;
    private final Clock clock;
    private final CustomerContacts contacts;

    public IngestController(MerchantRegistry merchants, EventPublisher publisher, SettlementInbox inbox, Clock clock,
                            CustomerContacts contacts) {
        this.merchants = merchants;
        this.publisher = publisher;
        this.inbox = inbox;
        this.clock = clock;
        this.contacts = contacts;
    }

    /** Authenticated by its HMAC signature over the raw bytes, verified before any parsing (§10.2, D21). */
    @PostMapping("/v1/webhooks/gateway/{merchantId}")
    public ResponseEntity<Map<String, Object>> webhook(@PathVariable String merchantId, HttpServletRequest request,
            @RequestHeader(name = WebhookSignature.HEADER, required = false) Optional<String> signature,
            @RequestHeader(name = "traceparent", required = false) Optional<String> traceparent) throws IOException {
        Optional<MerchantContext> merchant = merchant(merchantId);
        if (merchant.isEmpty()) {
            // The same answer as a bad signature: an unauthenticated caller learns nothing about which merchants exist.
            return error(401, "signature NO_MATCH");
        }
        Optional<byte[]> body = BoundedBody.read(request, WebhookSignature.MAX_BODY_BYTES);
        if (body.isEmpty()) {
            return error(413, "body larger than " + WebhookSignature.MAX_BODY_BYTES + " bytes");
        }
        IngestResult result = IngestNormaliser.webhook(merchant.get(), body.get(), signature, clock.now(), traceparent);
        if (result instanceof IngestResult.Accepted a && a.customer().isPresent()
                && a.envelope().payload() instanceof PaymentPayload payment) {
            // D77: the contact goes to the encrypted vault before the event is published, never into the event
            CustomerContact contact = a.customer().get();
            if (contact.dropped() > 0) {
                LOG.info("webhook {}: {} customer fields did not validate and were not stored", a.envelope().eventKey(),
                        contact.dropped());
            }
            try {
                contacts.store(merchant.get().merchantId(), payment.paymentId(), contact, clock.now());
            } catch (StoreUnavailable e) {
                return error(503, "the store is unavailable; retry with the same event");
            }
        }
        return accept(result);
    }

    /** {@code Idempotency-Key} is required (§10.3); the token needs {@code events:write} for this merchant. */
    @PostMapping("/v1/merchants/{merchantId}/order-events")
    @PreAuthorize("hasAuthority('SCOPE_events:write') and @merchantAccess.allows(authentication, #merchantId)")
    public ResponseEntity<Map<String, Object>> orderEvent(@PathVariable String merchantId, HttpServletRequest request,
            @RequestHeader(name = "Idempotency-Key", required = false) Optional<String> idempotencyKey,
            @RequestHeader(name = "traceparent", required = false) Optional<String> traceparent) throws IOException {
        Optional<MerchantContext> merchant = merchant(merchantId);
        if (merchant.isEmpty()) {
            return error(404, "unknown merchant");
        }
        Optional<byte[]> body = BoundedBody.read(request, WebhookSignature.MAX_BODY_BYTES);
        if (body.isEmpty()) {
            return error(413, "body larger than " + WebhookSignature.MAX_BODY_BYTES + " bytes");
        }
        return accept(IngestNormaliser.orderEvent(merchant.get(), body.get(), idempotencyKey, clock.now(), traceparent));
    }

    /**
     * Stores the raw file for the separate settlement worker (§6.2, D48) and answers {@code 202 {accepted, key}} with
     * the file's SHA-256 as the key. The strict parser runs here first, so a malformed file is a {@code 400} the
     * uploader sees at once; the worker parses the stored bytes again and does everything else.
     */
    @PostMapping("/v1/merchants/{merchantId}/settlement-files")
    @PreAuthorize("hasAuthority('SCOPE_settlement:write') and @merchantAccess.allows(authentication, #merchantId)")
    public ResponseEntity<Map<String, Object>> settlementFile(@PathVariable String merchantId,
            HttpServletRequest request) throws IOException {
        Optional<MerchantContext> merchant = merchant(merchantId);
        if (merchant.isEmpty()) {
            return error(404, "unknown merchant");
        }
        Optional<byte[]> body = BoundedBody.read(request, MAX_SETTLEMENT_BYTES);
        if (body.isEmpty()) {
            return error(413, "body larger than " + MAX_SETTLEMENT_BYTES + " bytes");
        }
        MerchantId id = merchant.get().merchantId();
        try {
            SettlementFileParser.parse(id, body.get());
        } catch (MalformedSettlementFile e) {
            return error(400, e.getMessage() == null ? "malformed settlement file" : e.getMessage());
        }
        try {
            SettlementInbox.Stored stored = inbox.store(id, body.get(), clock.now());
            return accepted(stored.sha256().hex());
        } catch (StoreUnavailable e) {
            return error(503, "the store is unavailable; retry");
        }
    }

    /**
     * {@code POST /internal/v1/observations} (§9.1, D10, D65): a fact the dispatcher learned by asking enters through
     * Kafka like every other event. The service token needs {@code observations:write}; the merchant is named in the
     * body, and a merchant-bound token may post only for its own merchant.
     */
    @PostMapping(CallWire.OBSERVATIONS_PATH)
    @PreAuthorize("hasAuthority('SCOPE_observations:write')")
    public ResponseEntity<Map<String, Object>> observation(HttpServletRequest request, Authentication authentication)
            throws IOException {
        Optional<byte[]> body = BoundedBody.read(request, WebhookSignature.MAX_BODY_BYTES);
        if (body.isEmpty()) {
            return error(413, "body larger than " + WebhookSignature.MAX_BODY_BYTES + " bytes");
        }
        CallWire.Incoming incoming;
        try {
            incoming = CallWire.parseObservation(body.get());
        } catch (MalformedJson e) {
            return error(400, e.getMessage());
        }
        if (!new MerchantAccess().allows(authentication, incoming.merchantId().value())) {
            return error(403, "this token may not post for that merchant");
        }
        Optional<MerchantContext> merchant = merchants.context(incoming.merchantId());
        if (merchant.isEmpty()) {
            return error(404, "unknown merchant");
        }
        return accept(IngestNormaliser.observation(merchant.get(), (CallRequest.Observation) incoming.request(),
                clock.now()));
    }

    private Optional<MerchantContext> merchant(String merchantId) {
        try {
            return merchants.context(MerchantId.of(merchantId));
        } catch (IllegalArgumentException notAnId) {
            return Optional.empty();
        }
    }

    private ResponseEntity<Map<String, Object>> accept(IngestResult result) {
        return switch (result) {
            case IngestResult.Rejected r -> error(r.status(), r.reason());
            case IngestResult.Accepted a -> {
                try {
                    publisher.publish(List.of(a.envelope()));
                    yield accepted(a.envelope().eventKey());
                } catch (EventPublisher.EventLogUnavailable e) {
                    LOG.warn("event log unavailable: {}", e.getMessage());
                    yield error(503, "the event log is unavailable; retry with the same key");
                }
            }
        };
    }

    private static ResponseEntity<Map<String, Object>> accepted(String key) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("accepted", true);
        body.put("key", key);
        return ResponseEntity.status(202).body(body);
    }

    static ResponseEntity<Map<String, Object>> error(int status, String reason) {
        return ResponseEntity.status(status).body(Map.of("error", reason));
    }
}
