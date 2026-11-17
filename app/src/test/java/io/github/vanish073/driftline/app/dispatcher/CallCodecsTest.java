package io.github.vanish073.driftline.app.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.core.port.OutboundCall;
import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The production codecs (D65): every call kind, the base prefix kept, the service token only to Driftline. */
class CallCodecsTest {

    private static final MerchantId M = MerchantId.of("m_1");

    @Test
    void everyKindOfCallHasACodec() {
        List<Class<?>> kinds = new ArrayList<>(CallCodecs.all("test-only-service-token").keySet());
        assertThat(kinds).containsExactlyInAnyOrderElementsOf(List.of(CallRequest.class.getPermittedSubclasses()));
    }

    @Test
    void aPathIsAppendedToItsBaseAndOnlyDriftlineSeesTheServiceToken() {
        OutboundCall refund = new OutboundCall("cmd_1.1", M, CallTarget.GATEWAY, Optional.of("rfd_k"),
                new CallRequest.Refund(PaymentId.of("pay_1"), OrderId.of("ord_1"), Money.ofPaise(100)));
        HttpRequest r = CallCodecs.all("test-only-service-token").get(CallRequest.Refund.class)
                .encode(refund, URI.create("http://sandbox:8085/gateway/")).build();
        assertThat(r.uri()).isEqualTo(URI.create("http://sandbox:8085/gateway/v1/merchants/m_1/payments/pay_1/refunds"));
        assertThat(r.method()).isEqualTo("POST");
        assertThat(r.headers().firstValue("Content-Type")).contains("application/json");
        assertThat(r.headers().firstValue("Authorization")).isEmpty();

        OutboundCall observation = new OutboundCall("cmd_2.1", M, CallTarget.DRIFTLINE, Optional.empty(),
                new CallRequest.Observation(EventType.REFUND_PROCESSED, "obs_1", Instant.parse("2026-11-13T06:00:00Z"),
                        new RefundPayload(PaymentId.of("pay_1"), Money.ofPaise(100))));
        HttpRequest o = CallCodecs.all("test-only-service-token").get(CallRequest.Observation.class)
                .encode(observation, URI.create("http://driftline:8080")).build();
        assertThat(o.uri()).isEqualTo(URI.create("http://driftline:8080/internal/v1/observations"));
        assertThat(o.headers().firstValue("Authorization")).contains("Bearer test-only-service-token");

        OutboundCall status = new OutboundCall("cmd_3.1", M, CallTarget.GATEWAY, Optional.empty(),
                new CallRequest.StatusQuery(PaymentId.of("pay_1")));
        HttpRequest s = CallCodecs.all("test-only-service-token").get(CallRequest.StatusQuery.class)
                .encode(status, URI.create("http://gw.example")).build();
        assertThat(s.method()).isEqualTo("GET");
        assertThat(s.uri().getPath()).isEqualTo("/v1/merchants/m_1/payments/pay_1");
    }
}
