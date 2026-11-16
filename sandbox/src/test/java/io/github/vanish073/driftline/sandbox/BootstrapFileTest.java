package io.github.vanish073.driftline.sandbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.security.SecureRandom;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

/** The merchant pool file: fresh secrets every time, one merchant per partition in turn (D49). */
class BootstrapFileTest {

    @Test
    void aGeneratedPoolReadsBackWithDistinctSecretsAndTokens() {
        LocalDate first = LocalDate.parse("2026-11-12");
        BootstrapFile.Pool pool = BootstrapFile.read(BootstrapFile.generate(12, 12, first, new SecureRandom()));
        assertThat(pool.merchants()).hasSize(12);
        assertThat(pool.merchants()).extracting(BootstrapFile.Merchant::partition).doesNotHaveDuplicates();
        assertThat(pool.merchants()).extracting(BootstrapFile.Merchant::secret).doesNotHaveDuplicates()
                .allMatch(s -> s.length() == 64);
        assertThat(pool.merchants()).extracting(BootstrapFile.Merchant::token).doesNotHaveDuplicates();
        assertThat(pool.merchants()).allMatch(m -> m.firstSettlementDate().equals(first));
        assertThat(pool.opsToken()).startsWith("dl_ops_");
        BootstrapFile.Pool again = BootstrapFile.read(BootstrapFile.generate(12, 12, first, new SecureRandom()));
        assertThat(again.merchants().get(0).secret()).isNotEqualTo(pool.merchants().get(0).secret());
    }
}
