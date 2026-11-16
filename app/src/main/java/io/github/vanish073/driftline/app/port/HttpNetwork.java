package io.github.vanish073.driftline.app.port;

import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.core.port.Network;
import io.github.vanish073.driftline.core.port.OutboundCall;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The production {@link Network} (DESIGN §3.2): {@code java.net.http}, asynchronous. {@link #send} never blocks; the
 * outcome of every call (a status and body, or the transport error) arrives later in the inbox, which is how the
 * sending node learns it, exactly like a {@code CallResult} in the simulator. The caller owns timeouts beyond the
 * per-request one.
 *
 * <p>Requests are encoded by a {@link CallCodec} per {@link CallRequest} type. The call kinds and their codecs arrive
 * with the dispatcher in Shift 8 (§9.1, D27); until then no codec is registered, and a call with no codec comes back
 * at once as an error outcome rather than being dropped.
 */
public final class HttpNetwork implements Network {

    /** Turns one call into an HTTP request against the target's base URI. */
    @FunctionalInterface
    public interface CallCodec {
        HttpRequest.Builder encode(OutboundCall call, URI base);
    }

    /** What happened to one call: a response, or why there was none. */
    public record CallOutcome(OutboundCall call, Optional<Integer> status, byte[] body, Optional<String> error) {
        public CallOutcome {
            Objects.requireNonNull(call, "call");
            Objects.requireNonNull(status, "status");
            body = body.clone();
            Objects.requireNonNull(error, "error");
        }

        @Override
        public byte[] body() {
            return body.clone();
        }
    }

    private final HttpClient client;
    private final EnumMap<CallTarget, URI> targets;
    private final Map<Class<? extends CallRequest>, CallCodec> codecs;
    private final Consumer<CallOutcome> inbox;
    private final Duration requestTimeout;

    public HttpNetwork(HttpClient client, Map<CallTarget, URI> targets, Map<Class<? extends CallRequest>, CallCodec> codecs,
                       Consumer<CallOutcome> inbox, Duration requestTimeout) {
        this.client = Objects.requireNonNull(client, "client");
        this.targets = targets.isEmpty() ? new EnumMap<>(CallTarget.class) : new EnumMap<>(targets);
        this.codecs = new LinkedHashMap<>(codecs);
        this.inbox = Objects.requireNonNull(inbox, "inbox");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
    }

    @Override
    public void send(OutboundCall call) {
        URI base = targets.get(call.target());
        CallCodec codec = codecs.get(call.request().getClass());
        if (base == null || codec == null) {
            inbox.accept(new CallOutcome(call, Optional.empty(), new byte[0], Optional.of(base == null
                    ? "no base URI for target " + call.target()
                    : "no codec for " + call.request().getClass().getSimpleName())));
            return;
        }
        HttpRequest.Builder request = codec.encode(call, base).timeout(requestTimeout);
        call.idempotencyKey().ifPresent(key -> request.header("Idempotency-Key", key));
        client.sendAsync(request.build(), HttpResponse.BodyHandlers.ofByteArray()).whenComplete((response, failure) -> {
            if (failure != null) {
                inbox.accept(new CallOutcome(call, Optional.empty(), new byte[0],
                        Optional.of(failure.getClass().getSimpleName())));
            } else {
                inbox.accept(new CallOutcome(call, Optional.of(response.statusCode()), response.body(), Optional.empty()));
            }
        });
    }
}
