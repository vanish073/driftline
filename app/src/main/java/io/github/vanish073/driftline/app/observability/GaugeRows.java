package io.github.vanish073.driftline.app.observability;

import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The rows of one {@link MultiGauge}, set as a whole on each refresh (D83). A row that stays keeps its meter and only
 * its value changes; a row that goes is removed. {@code MultiGauge.register(rows, true)} instead removes and registers
 * every row again on each refresh, so a scrape in between could miss a series whose value had changed.
 */
final class GaugeRows {

    private final MultiGauge gauge;
    private final ConcurrentHashMap<Tags, AtomicReference<Double>> values = new ConcurrentHashMap<>();

    GaugeRows(MultiGauge gauge) {
        this.gauge = Objects.requireNonNull(gauge, "gauge");
    }

    /** Makes {@code rows} the gauge's rows: these tags with these values, and no others. */
    synchronized void set(Map<Tags, ? extends Number> rows) {
        List<MultiGauge.Row<?>> out = new ArrayList<>(rows.size());
        rows.forEach((tags, value) -> {
            AtomicReference<Double> holder = values.computeIfAbsent(tags, t -> new AtomicReference<>(0.0));
            holder.set(value.doubleValue());
            out.add(MultiGauge.Row.of(tags, holder, AtomicReference::get));
        });
        values.keySet().retainAll(rows.keySet());
        gauge.register(out, false);
    }
}
