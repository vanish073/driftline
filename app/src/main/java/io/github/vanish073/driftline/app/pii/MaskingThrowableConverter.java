package io.github.vanish073.driftline.app.pii;

import ch.qos.logback.classic.pattern.ExtendedThrowableProxyConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

/** {@code %maskedEx}: the stack trace with personal data masked, since exception messages can carry it (D77). */
public final class MaskingThrowableConverter extends ExtendedThrowableProxyConverter {

    @Override
    public String convert(ILoggingEvent event) {
        return PiiMasking.mask(super.convert(event));
    }
}
