package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.EventPayload;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/**
 * The typed body of an {@link OutboundCall}: one per command kind of DESIGN §9.1 (sealed in Shift 8, D27). The
 * idempotency key travels on the call itself; a refund also carries it as {@code metadata.driftline_refund_key}.
 */
public sealed interface CallRequest {

    /** {@code STATUS_QUERY_PAYMENT}: what the gateway says about a payment now (a read). */
    record StatusQuery(PaymentId paymentId) implements CallRequest {
        public StatusQuery {
            Objects.requireNonNull(paymentId, "paymentId");
        }
    }

    /**
     * {@code STATUS_QUERY_ORDER}: every payment the gateway holds for an order, with its status (a read): the
     * pending-order probe, which recovers payments whose webhooks were lost (§9.2, D61).
     */
    record StatusQueryOrder(OrderId orderId) implements CallRequest {
        public StatusQueryOrder {
            Objects.requireNonNull(orderId, "orderId");
        }
    }

    /**
     * {@code PAYMENT_SWEEP}: every payment the gateway captured for the merchant on one IST day, as it knows them now
     * (a read): before a file's unreferenced lines are solved, so a capture none of whose webhooks arrived is still a
     * candidate (D72).
     */
    record PaymentSweep(LocalDate capturedOn) implements CallRequest {
        public PaymentSweep {
            Objects.requireNonNull(capturedOn, "capturedOn");
        }
    }

    /** {@code REFUND}: a full refund of a captured payment (v1 has full refunds only). */
    record Refund(PaymentId paymentId, OrderId orderId, Money amount) implements CallRequest {
        public Refund {
            Objects.requireNonNull(paymentId, "paymentId");
            Objects.requireNonNull(orderId, "orderId");
            if (!amount.isPositive()) {
                throw new IllegalArgumentException("a refund amount must be positive");
            }
        }
    }

    /** {@code REFUND_LOOKUP}: the refunds the gateway holds for a payment (a read; §9.2 step 6). */
    record RefundLookup(PaymentId paymentId) implements CallRequest {
        public RefundLookup {
            Objects.requireNonNull(paymentId, "paymentId");
        }
    }

    /** {@code MERCHANT_CONFIRM}: asks the merchant to accept a late success for a pending order. */
    record MerchantConfirm(OrderId orderId, PaymentId paymentId, Money amount) implements CallRequest {
        public MerchantConfirm {
            Objects.requireNonNull(orderId, "orderId");
            Objects.requireNonNull(paymentId, "paymentId");
            Objects.requireNonNull(amount, "amount");
        }
    }

    /**
     * {@code PUBLISH_OBSERVATION}: a fact learned by asking, published to Driftline's own observation endpoint so it
     * enters through Kafka like every other event (D10). The event key is derived, so repeats are deduplicated.
     */
    record Observation(EventType eventType, String eventKey, Instant eventTime, EventPayload payload)
            implements CallRequest {
        public Observation {
            Objects.requireNonNull(eventType, "eventType");
            IdRules.requireKey("eventKey", eventKey);
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(payload, "payload");
            if (!eventType.payloadType().isInstance(payload)) {
                throw new IllegalArgumentException(eventType + " needs a " + eventType.payloadType().getSimpleName());
            }
        }
    }

    /** {@code ESCALATION_NOTIFY}: tells the ops sink that a break needs a person. */
    record EscalationNotify(String breakId, BreakType type, String subject, String reason) implements CallRequest {
        public EscalationNotify {
            IdRules.requireToken("breakId", breakId);
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(subject, "subject");
            Objects.requireNonNull(reason, "reason");
        }
    }
}
