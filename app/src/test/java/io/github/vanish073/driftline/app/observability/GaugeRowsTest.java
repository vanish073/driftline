package io.github.vanish073.driftline.app.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GaugeRowsTest {

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    final GaugeRows rows = new GaugeRows(MultiGauge.builder("t.rows").register(registry));

    Gauge gauge(String p) {
        return registry.find("t.rows").tag("p", p).gauge();
    }

    static Map<Tags, Number> of(Object... pairs) {
        Map<Tags, Number> out = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put(Tags.of("p", (String) pairs[i]), (Number) pairs[i + 1]);
        }
        return out;
    }

    @Test
    void aRowThatStaysKeepsItsMeterAndTakesTheNewValue() {
        rows.set(of("0", 5L, "1", 1.5));
        Gauge first = gauge("0");
        assertThat(first.value()).isEqualTo(5);

        rows.set(of("0", 7L, "1", 2.5));
        assertThat(gauge("0")).as("not removed and registered again, so no scrape can miss it").isSameAs(first);
        assertThat(first.value()).isEqualTo(7);
        assertThat(gauge("1").value()).isEqualTo(2.5);
    }

    @Test
    void aRowThatGoesIsRemovedAndCanComeBack() {
        rows.set(of("0", 5L, "1", 1L));
        rows.set(of("1", 2L));
        assertThat(gauge("0")).isNull();
        assertThat(gauge("1").value()).isEqualTo(2);

        rows.set(of("0", 9L, "1", 3L));
        assertThat(gauge("0").value()).isEqualTo(9);
        rows.set(Map.of());
        assertThat(registry.find("t.rows").gauges()).isEmpty();
    }
}
