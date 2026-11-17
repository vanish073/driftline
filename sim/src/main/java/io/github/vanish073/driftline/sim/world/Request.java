package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.CallRequest;
import java.util.Optional;

/** An HTTP request from the world to Driftline's ingest, as raw bytes plus the headers that matter. */
public sealed interface Request {

    MerchantId merchantId();

    byte[] body();

    /** {@code POST /v1/webhooks/gateway/{merchantId}} with a {@code Driftline-Signature} header. */
    record Webhook(MerchantId merchantId, byte[] body, Optional<String> signature) implements Request {
    }

    /** {@code POST /v1/merchants/{merchantId}/order-events} with an {@code Idempotency-Key} header. */
    record OrderEvent(MerchantId merchantId, byte[] body, Optional<String> idempotencyKey) implements Request {
    }

    /** {@code POST /internal/v1/observations} from Driftline's own dispatcher (D10): a typed fact, not raw bytes. */
    record Observation(MerchantId merchantId, CallRequest.Observation observation) implements Request {
        @Override
        public byte[] body() {
            return new byte[0];
        }
    }
}
