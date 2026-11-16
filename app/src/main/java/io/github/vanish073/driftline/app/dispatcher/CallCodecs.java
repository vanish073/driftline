package io.github.vanish073.driftline.app.dispatcher;

import io.github.vanish073.driftline.app.port.HttpNetwork;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.wire.call.CallWire;
import java.net.URI;
import java.net.http.HttpRequest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The production codecs of {@link HttpNetwork}: every call kind encoded by the shared {@link CallWire} (D65). A path is
 * appended to its target's base URI, so a base may carry a prefix ({@code http://sandbox:8085/gateway}). Only calls to
 * Driftline's own observation endpoint carry the service token; no other target ever sees it.
 */
public final class CallCodecs {

    private static final List<Class<? extends CallRequest>> KINDS = List.of(CallRequest.StatusQuery.class,
            CallRequest.StatusQueryOrder.class, CallRequest.Refund.class, CallRequest.RefundLookup.class,
            CallRequest.MerchantConfirm.class, CallRequest.Observation.class, CallRequest.EscalationNotify.class);

    private CallCodecs() {
    }

    public static Map<Class<? extends CallRequest>, HttpNetwork.CallCodec> all(String serviceToken) {
        Objects.requireNonNull(serviceToken, "serviceToken");
        HttpNetwork.CallCodec codec = (call, base) -> {
            CallWire.HttpCall http = CallWire.request(call);
            String root = base.toString().endsWith("/") ? base.toString().substring(0, base.toString().length() - 1)
                    : base.toString();
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(root + http.path()));
            if (http.method().equals("GET")) {
                b.GET();
            } else {
                b.method(http.method(), HttpRequest.BodyPublishers.ofByteArray(http.body()))
                        .header("Content-Type", "application/json");
            }
            if (call.target() == CallTarget.DRIFTLINE) {
                b.header("Authorization", "Bearer " + serviceToken);
            }
            return b;
        };
        Map<Class<? extends CallRequest>, HttpNetwork.CallCodec> out = new LinkedHashMap<>();
        KINDS.forEach(kind -> out.put(kind, codec));
        return out;
    }
}
