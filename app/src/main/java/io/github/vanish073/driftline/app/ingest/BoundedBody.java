package io.github.vanish073.driftline.app.ingest;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Reads a request body as raw bytes, but never more than a limit (§14 "request size limits"). A declared
 * {@code Content-Length} over the limit is refused before any byte is read; a chunked body is read up to the limit plus
 * one byte and refused if it is longer. The raw bytes are what the webhook signature covers (D21).
 */
public final class BoundedBody {

    private BoundedBody() {
    }

    /** @return the body, or empty if it is longer than {@code limit} */
    public static Optional<byte[]> read(HttpServletRequest request, int limit) throws IOException {
        long declared = request.getContentLengthLong();
        if (declared > limit) {
            return Optional.empty();
        }
        try (InputStream in = request.getInputStream()) {
            byte[] body = in.readNBytes(limit + 1);
            return body.length > limit ? Optional.empty() : Optional.of(body);
        }
    }
}
