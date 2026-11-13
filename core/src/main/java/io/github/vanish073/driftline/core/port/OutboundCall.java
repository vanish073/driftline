package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import java.util.Objects;
import java.util.Optional;

/**
 * One call handed to {@link Network} (DESIGN §9).
 *
 * @param callId         correlates the result with this attempt; derived from the outbox command id and attempt
 * @param idempotencyKey sent as {@code Idempotency-Key} when the call has effects, for example a refund
 * @param request        the typed request; the codec in the production adapter turns it into HTTP
 */
public record OutboundCall(
        String callId, MerchantId merchantId, CallTarget target, Optional<String> idempotencyKey, CallRequest request) {

    public OutboundCall {
        IdRules.requireToken("callId", callId);
        Objects.requireNonNull(merchantId, "merchantId");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        Objects.requireNonNull(request, "request");
        idempotencyKey.ifPresent(key -> IdRules.requireKey("idempotencyKey", key));
    }
}
