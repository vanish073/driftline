package io.github.vanish073.driftline.core.event;

import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * The canonical event that every source is normalised into before it reaches Kafka (DESIGN §4.2).
 *
 * <p>The constructor rejects any envelope that breaks the schema: a source the event type does not allow, a payload
 * of the wrong record type, an order snapshot whose {@code paidWith} does not match the event type, or a malformed
 * {@code traceparent}.
 *
 * @param eventKey      the idempotency key: gateway event id, {@code Idempotency-Key} header, {@code fileSha:lineNo},
 *                      or a derived key; dedupe is on {@code (merchantId, source, eventKey)}
 * @param eventTime     the time the source asserts; the only time state machines and watermarks use
 * @param receivedAt    ingest processing time; used only for event-to-match latency
 * @param payloadSha256 hash of the canonical payload, which detects a key reused with a different body
 * @param traceparent   W3C trace context, version 00, if the request carried a valid one
 */
public record EventEnvelope(
        int schemaVersion,
        MerchantId merchantId,
        int partition,
        Source source,
        String eventKey,
        EventType eventType,
        Instant eventTime,
        Instant receivedAt,
        Sha256 payloadSha256,
        EventPayload payload,
        Optional<String> traceparent) {

    public static final int SCHEMA_VERSION = 1;

    public EventEnvelope {
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported schemaVersion " + schemaVersion);
        }
        Objects.requireNonNull(merchantId, "merchantId");
        Objects.requireNonNull(source, "source");
        IdRules.requireKey("eventKey", eventKey);
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(eventTime, "eventTime");
        Objects.requireNonNull(receivedAt, "receivedAt");
        Objects.requireNonNull(payloadSha256, "payloadSha256");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(traceparent, "traceparent");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must not be negative, got " + partition);
        }
        if (!eventType.allows(source)) {
            throw new IllegalArgumentException(eventType.wireName() + " cannot come from source " + source);
        }
        if (!eventType.payloadType().isInstance(payload)) {
            throw new IllegalArgumentException(eventType.wireName() + " carries a "
                    + eventType.payloadType().getSimpleName() + ", not a " + payload.getClass().getSimpleName());
        }
        if (payload instanceof OrderPayload order && order.paidWith().isPresent() != eventType.carriesPaidPayment()) {
            throw new IllegalArgumentException(eventType.carriesPaidPayment()
                    ? eventType.wireName() + " must name the payment used (paidWith)"
                    : eventType.wireName() + " must not name a payment (paidWith)");
        }
        traceparent.ifPresent(EventEnvelope::requireTraceparent);
    }

    private static void requireTraceparent(String value) {
        if (!isValidTraceparent(value)) {
            throw new IllegalArgumentException("traceparent is not a valid W3C version-00 trace context");
        }
    }

    /**
     * True for {@code 00-<32 hex trace id>-<16 hex parent id>-<2 hex flags>}, lowercase, ids not all zero. Ingest drops
     * an invalid header instead of rejecting the request.
     */
    public static boolean isValidTraceparent(String value) {
        boolean shape = value.length() == 55
                && value.startsWith("00-")
                && value.charAt(35) == '-'
                && value.charAt(52) == '-'
                && isLowerHex(value, 3, 35)
                && isLowerHex(value, 36, 52)
                && isLowerHex(value, 53, 55);
        return shape && !isAllZeros(value, 3, 35) && !isAllZeros(value, 36, 52);
    }

    private static boolean isLowerHex(String value, int from, int to) {
        for (int i = from; i < to; i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAllZeros(String value, int from, int to) {
        for (int i = from; i < to; i++) {
            if (value.charAt(i) != '0') {
                return false;
            }
        }
        return true;
    }
}
