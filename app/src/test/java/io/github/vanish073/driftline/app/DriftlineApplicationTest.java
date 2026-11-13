package io.github.vanish073.driftline.app;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

/** The application starts and stops inside the laptop memory budget: -Xmx256m with SerialGC (DESIGN §15.1). */
class DriftlineApplicationTest {

    private static final long HEAP_BUDGET_BYTES = 256L * 1024 * 1024;

    @Test
    void startsAndStopsWithinTheHeapBudget() {
        assertThat(Runtime.getRuntime().maxMemory())
                .as("the test JVM must run with the production heap cap")
                .isLessThanOrEqualTo(HEAP_BUDGET_BYTES);

        ConfigurableApplicationContext context = SpringApplication.run(DriftlineApplication.class);
        try {
            assertThat(context.isRunning()).isTrue();
            assertThat(context.getBean(DriftlineApplication.class)).isNotNull();
        } finally {
            context.close();
        }
        assertThat(context.isActive()).isFalse();
    }
}
