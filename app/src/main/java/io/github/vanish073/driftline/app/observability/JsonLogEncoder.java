package io.github.vanish073.driftline.app.observability;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.encoder.EncoderBase;
import io.github.vanish073.driftline.app.pii.PiiMasking;
import io.github.vanish073.driftline.wire.json.StrictJson;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;

/**
 * One JSON object per log line (DESIGN §13, D78): {@code timestamp} (UTC, milliseconds), {@code level}, {@code logger},
 * {@code thread}, {@code message}, {@code trace_id} and {@code span_id} when a span is current (the OpenTelemetry agent
 * puts them in the MDC), and {@code error} with the stack trace. The message and the stack trace go through
 * {@link PiiMasking} first (D77), so the JSON never holds a raw customer contact the masker recognises.
 */
public final class JsonLogEncoder extends EncoderBase<ILoggingEvent> {

    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX",
            Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final byte[] NEWLINE = {'\n'};

    @Override
    public byte[] headerBytes() {
        return null;
    }

    @Override
    public byte[] footerBytes() {
        return null;
    }

    @Override
    public byte[] encode(ILoggingEvent event) {
        Map<String, String> mdc = event.getMDCPropertyMap();
        IThrowableProxy error = event.getThrowableProxy();
        byte[] json = StrictJson.write(g -> {
            g.writeStringProperty("timestamp", TIMESTAMP.format(Instant.ofEpochMilli(event.getTimeStamp())));
            g.writeStringProperty("level", event.getLevel().toString());
            g.writeStringProperty("logger", event.getLoggerName());
            g.writeStringProperty("thread", event.getThreadName());
            g.writeStringProperty("message", PiiMasking.mask(event.getFormattedMessage()));
            String traceId = mdc.get("trace_id");
            if (traceId != null) {
                g.writeStringProperty("trace_id", traceId);
                String spanId = mdc.get("span_id");
                if (spanId != null) {
                    g.writeStringProperty("span_id", spanId);
                }
            }
            if (error != null) {
                g.writeStringProperty("error", PiiMasking.mask(ThrowableProxyUtil.asString(error)));
            }
        });
        byte[] line = new byte[json.length + 1];
        System.arraycopy(json, 0, line, 0, json.length);
        System.arraycopy(NEWLINE, 0, line, json.length, 1);
        return line;
    }
}
