package io.github.vanish073.driftline.app.pii;

import ch.qos.logback.classic.pattern.MessageConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

/** {@code %maskedMsg}: the log message with personal data masked ({@link PiiMasking}, D77). */
public final class MaskingMessageConverter extends MessageConverter {

    @Override
    public String convert(ILoggingEvent event) {
        return PiiMasking.mask(super.convert(event));
    }
}
