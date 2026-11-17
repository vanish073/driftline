package io.github.vanish073.driftline.sim.breaks;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.breaks.DeadlineRule;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.dataformat.yaml.YAMLMapper;

/** {@code config/deadlines.yaml} (cited in the README) and core's {@link DeadlineRule} table must agree (§8.3). */
class DeadlinesYamlTest {

    @Test
    void theYamlFileAndTheCodeTableAreTheSame() throws IOException {
        JsonNode root;
        try (InputStream in = DeadlinesYamlTest.class.getClassLoader().getResourceAsStream("deadlines.yaml")) {
            assertThat(in).as("config/deadlines.yaml is on the test classpath").isNotNull();
            root = new YAMLMapper().readTree(in);
        }
        List<String> ids = new ArrayList<>();
        for (JsonNode r : root.get("rules")) {
            DeadlineRule rule = DeadlineRule.valueOf(r.get("id").asString());
            ids.add(rule.name());
            assertThat(r.get("from").asString()).as("%s from", rule).isEqualTo(rule.anchor().name());
            assertThat(r.path("days").asInt(0)).as("%s days", rule).isEqualTo(rule.days());
            assertThat(r.path("hours").asInt(0)).as("%s hours", rule).isEqualTo(rule.hours());
            assertThat(r.get("margin_hours").asLong()).as("%s margin", rule).isEqualTo(rule.margin().toHours());
            assertThat(r.get("compensation_rupees_per_day").asLong()).as("%s compensation", rule)
                    .isEqualTo(rule.compensationRupeesPerDay());
            TreeSet<String> types = new TreeSet<>();
            r.get("applies_to").forEach(t -> types.add(t.asString()));
            assertThat(types).as("%s applies to", rule)
                    .isEqualTo(new TreeSet<>(rule.appliesTo().stream().map(BreakType::name).toList()));
            assertThat(r.get("source").asString()).as("%s cites its source", rule).isNotBlank();
        }
        assertThat(ids).containsExactly(java.util.Arrays.stream(DeadlineRule.values()).map(Enum::name)
                .toArray(String[]::new));
    }
}
