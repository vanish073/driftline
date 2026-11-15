package io.github.vanish073.driftline.sandbox;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.StrictJson;
import java.security.SecureRandom;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * The dev bootstrap file (D49), shared by the sandbox and the app: a pool of merchants with fresh random webhook
 * secrets and tokens, plus an ops token. Generated for each Compose run and never committed. Merchants are an object
 * keyed by id, so the strict parser of {@code wire} reads it (it refuses arrays).
 */
public final class BootstrapFile {

    public static final String FORMAT = "driftline-bootstrap-v1";

    /** One pooled merchant. {@code secret} is the webhook secret's bytes as ASCII hex (64 characters). */
    public record Merchant(MerchantId id, int partition, LocalDate firstSettlementDate, String secret, String token) {
    }

    public record Pool(String opsToken, List<Merchant> merchants) {
        public Pool {
            merchants = List.copyOf(merchants);
        }
    }

    private BootstrapFile() {
    }

    public static byte[] generate(int count, int partitions, LocalDate firstSettlementDate, SecureRandom random) {
        if (count < 1 || count > 99 || partitions < 1) {
            throw new IllegalArgumentException("1 to 99 merchants on at least one partition");
        }
        return StrictJson.write(g -> {
            g.writeStringProperty("format", FORMAT);
            g.writeStringProperty("ops_token", "dl_ops_" + hex(random, 24));
            g.writeName("merchants");
            g.writeStartObject();
            for (int i = 1; i <= count; i++) {
                g.writeName("m_e2e_%02d".formatted(i));
                g.writeStartObject();
                g.writeNumberProperty("partition", i % partitions);
                g.writeNumberProperty("fee_bps", 200);
                g.writeNumberProperty("gst_bps", 1_800);
                g.writeNumberProperty("cycle_days", 1);
                g.writeStringProperty("first_settlement_date", firstSettlementDate.toString());
                g.writeStringProperty("webhook_secret", hex(random, 32));
                g.writeStringProperty("token", "dl_" + hex(random, 24));
                g.writeEndObject();
            }
            g.writeEndObject();
        });
    }

    public static Pool read(byte[] file) {
        JsonObject root = StrictJson.parse(file);
        if (!FORMAT.equals(root.string("format"))) {
            throw new IllegalArgumentException("not a " + FORMAT + " file");
        }
        List<Merchant> merchants = new ArrayList<>();
        for (Map.Entry<String, Object> e : root.object("merchants").fields().entrySet()) {
            JsonObject m = (JsonObject) e.getValue();
            merchants.add(new Merchant(MerchantId.of(e.getKey()), Math.toIntExact(m.integer("partition")),
                    LocalDate.parse(m.string("first_settlement_date")), m.string("webhook_secret"), m.string("token")));
        }
        return new Pool(root.string("ops_token"), merchants);
    }

    private static String hex(SecureRandom random, int bytes) {
        byte[] b = new byte[bytes];
        random.nextBytes(b);
        return HexFormat.of().formatHex(b);
    }
}
