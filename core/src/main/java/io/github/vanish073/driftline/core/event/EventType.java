package io.github.vanish073.driftline.core.event;

import java.util.List;
import java.util.Objects;

/** Every event type in DESIGN §4.2, with its wire name, its payload record and the sources allowed to send it. */
public enum EventType {
    PAYMENT_PENDING("payment.pending", PaymentPayload.class, Source.GATEWAY, Source.OBSERVATION),
    PAYMENT_SUCCEEDED("payment.succeeded", PaymentPayload.class, Source.GATEWAY, Source.OBSERVATION),
    PAYMENT_FAILED("payment.failed", PaymentPayload.class, Source.GATEWAY, Source.OBSERVATION),
    REFUND_PROCESSED("refund.processed", RefundPayload.class, Source.GATEWAY, Source.OBSERVATION),
    REFUND_FAILED("refund.failed", RefundPayload.class, Source.GATEWAY, Source.OBSERVATION),
    ORDER_CREATED("order.created", OrderPayload.class, Source.MERCHANT),
    ORDER_PENDING_PAYMENT("order.pending_payment", OrderPayload.class, Source.MERCHANT),
    ORDER_PAID("order.paid", OrderPayload.class, Source.MERCHANT),
    ORDER_FULFILLED("order.fulfilled", OrderPayload.class, Source.MERCHANT),
    ORDER_EXPIRED("order.expired", OrderPayload.class, Source.MERCHANT),
    ORDER_CANCELLED("order.cancelled", OrderPayload.class, Source.MERCHANT),
    SETTLEMENT_LINE("settlement.line", SettlementLinePayload.class, Source.BANK),
    SETTLEMENT_FILE_END("settlement.file_end", SettlementFileEndPayload.class, Source.BANK);

    private final String wireName;
    private final Class<? extends EventPayload> payloadType;
    private final List<Source> sources;

    EventType(String wireName, Class<? extends EventPayload> payloadType, Source... sources) {
        this.wireName = wireName;
        this.payloadType = payloadType;
        this.sources = List.of(sources);
    }

    public String wireName() {
        return wireName;
    }

    public Class<? extends EventPayload> payloadType() {
        return payloadType;
    }

    public List<Source> sources() {
        return sources;
    }

    public boolean allows(Source source) {
        return sources.contains(source);
    }

    /** True for {@code order.paid} and {@code order.fulfilled}, whose payload names the payment used. */
    public boolean carriesPaidPayment() {
        return this == ORDER_PAID || this == ORDER_FULFILLED;
    }

    public static EventType fromWireName(String wireName) {
        Objects.requireNonNull(wireName, "wireName");
        for (EventType type : values()) {
            if (type.wireName.equals(wireName)) {
                return type;
            }
        }
        throw new IllegalArgumentException("unknown event type");
    }
}
