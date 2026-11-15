package io.github.vanish073.driftline.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** DRIFTLINE_ROLE → profiles (DESIGN §3.3, D49). */
class RolesTest {

    @Test
    void allRunsEverythingThatExistsInOneJvm() {
        assertThat(Roles.profiles("all")).containsExactly("ingest", "matcher", "settlement-worker");
        assertThat(Roles.profiles(" Matcher ")).containsExactly("matcher");
        assertThat(Roles.profiles("settlement-worker")).containsExactly("settlement-worker");
        assertThat(Roles.profiles("migrate")).containsExactly("migrate");
    }

    @Test
    void rolesThatDoNotExistYetAreRefusedRatherThanSilentlyRunningNothing() {
        assertThatThrownBy(() -> Roles.profiles("resolver")).hasMessageContaining("Shift 8");
        assertThatThrownBy(() -> Roles.profiles("dispatcher")).hasMessageContaining("Shift 8");
        assertThatThrownBy(() -> Roles.profiles("everything")).hasMessageContaining("must be one of");
    }
}
