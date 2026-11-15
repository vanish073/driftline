package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.core.id.MerchantId;
import java.util.function.IntConsumer;

/** Where the bank uploads settlement files: Driftline's settlement worker. The reply carries the HTTP status. */
@FunctionalInterface
public interface SettlementUploads {

    void upload(MerchantId merchantId, byte[] file, IntConsumer reply);
}
