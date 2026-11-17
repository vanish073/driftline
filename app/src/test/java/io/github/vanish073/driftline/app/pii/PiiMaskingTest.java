package io.github.vanish073.driftline.app.pii;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Personal data is masked in text and in every log line the real logback configuration writes (§14, D77). */
class PiiMaskingTest {

    @Test
    void contactsAreMaskedAndOrdinaryLogTextIsLeftAlone() {
        assertThat(PiiMasking.mask("vpa buyer.one@okaxis paid")).isEqualTo("vpa bu***@okaxis paid");
        assertThat(PiiMasking.mask("mail Buyer.One@Example.com")).isEqualTo("mail Bu***@Example.com");
        assertThat(PiiMasking.mask("call 9876543210 or +91 9876543210")).isEqualTo("call 98******10 or +91 98******10");
        assertThat(PiiMasking.mask("a@okaxis")).isEqualTo("a***@okaxis");
        assertThat(PiiMasking.mask("pay_9876543210_a and offset 12 epoch 1763359200 and 98765432101"))
                .as("ids, epoch seconds and longer numbers are not mobiles")
                .isEqualTo("pay_9876543210_a and offset 12 epoch 1763359200 and 98765432101");
        assertThat(PiiMasking.mask("org.postgresql.jdbc.PgConnection@64b0d1fa"))
                .as("an object hash starting with a digit").isEqualTo("org.postgresql.jdbc.PgConnection@64b0d1fa");
        assertThat(PiiMasking.maskAddress("buyer@okaxis")).isEqualTo("bu***@okaxis");
        assertThat(PiiMasking.maskPhone("9876543210")).isEqualTo("98******10");
    }

    @Test
    void theRealLogbackConfigurationMasksMessagesAndStackTraces() throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        LoggerContext context = new LoggerContext();
        context.setMDCAdapter(new LogbackMDCAdapter());
        try {
            System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            configurator.doConfigure(getClass().getResource("/logback-spring.xml"));
            Logger log = context.getLogger("pii-test");
            log.info("refund for buyer.one@okaxis, phone 9876543210, mail buyer.one@example.com");
            log.warn("failed", new IllegalStateException("customer buyer.two@okaxis on +919812345678"));
        } finally {
            context.stop();
            System.setOut(original);
        }
        String out = captured.toString(StandardCharsets.UTF_8);
        assertThat(out).contains("bu***@okaxis").contains("98******10").contains("bu***@example.com")
                .contains("IllegalStateException").contains("98******78");
        assertThat(out).doesNotContain("buyer.one@okaxis").doesNotContain("buyer.two@okaxis")
                .doesNotContain("9876543210").doesNotContain("9812345678").doesNotContain("buyer.one@example.com");
    }
}
