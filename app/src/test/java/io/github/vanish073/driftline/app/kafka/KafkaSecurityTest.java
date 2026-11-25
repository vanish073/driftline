package io.github.vanish073.driftline.app.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.app.config.DriftlineProperties;
import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** D100: Kafka clients use TLS with the broker's certificate checked, and log in; nothing else is accepted. */
class KafkaSecurityTest {

    static DriftlineProperties.Kafka kafka(String protocol, String user, String password, String ca) {
        return new DriftlineProperties.Kafka("kafka:9092", "t", 12, "g", 100, Duration.ofMillis(200),
                Duration.ofSeconds(10), Duration.ofSeconds(300), Duration.ofSeconds(10), Duration.ofSeconds(60), protocol,
                user, password, ca);
    }

    @Test
    void saslSslTrustsTheCaChecksTheHostAndLogsIn() {
        DriftlineProperties.Kafka k = kafka("SASL_SSL", "driftline", "test-only-pass\"word\\", "/run/tls/ca.crt");
        for (Properties p : java.util.List.of(KafkaClients.producer(k, "p"), KafkaClients.consumer(k, "c"),
                KafkaClients.admin(k, "a"))) {
            assertThat(p).containsEntry("security.protocol", "SASL_SSL").containsEntry("sasl.mechanism", "PLAIN")
                    .containsEntry("ssl.truststore.type", "PEM").containsEntry("ssl.truststore.location", "/run/tls/ca.crt")
                    .containsEntry("ssl.endpoint.identification.algorithm", "https")
                    .containsEntry("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required"
                            + " username=\"driftline\" password=\"test-only-pass\\\"word\\\\\";");
        }
        assertThat(k.toString()).doesNotContain("test-only-pass").contains("password=***");
    }

    @Test
    void plaintextAddsNothingAndAnythingElseOrAnIncompleteLoginIsRefused() {
        assertThat(KafkaClients.producer(kafka("PLAINTEXT", null, null, null), "p")).doesNotContainKey("security.protocol");
        for (DriftlineProperties.Kafka bad : java.util.List.of(kafka("SSL", "u", "p", "/ca"),
                kafka("SASL_PLAINTEXT", "u", "p", "/ca"), kafka("SASL_SSL", null, "p", "/ca"),
                kafka("SASL_SSL", "u", "", "/ca"), kafka("SASL_SSL", "u", "p", null),
                kafka("SASL_SSL", "u\" admin=\"x", "p", "/ca"))) {
            assertThatThrownBy(() -> KafkaClients.producer(bad, "p")).isInstanceOf(IllegalStateException.class);
        }
    }
}
