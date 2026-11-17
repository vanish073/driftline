package io.github.vanish073.driftline.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** DRIFTLINE_ROLE → profiles (DESIGN §3.3, D49). */
class RolesTest {

    @Test
    void allRunsEveryRoleInOneJvm() {
        assertThat(Roles.profiles("all"))
                .containsExactly("ingest", "matcher", "settlement-worker", "resolver", "dispatcher");
        assertThat(Roles.profiles(" Matcher ")).containsExactly("matcher");
        assertThat(Roles.profiles("settlement-worker")).containsExactly("settlement-worker");
        assertThat(Roles.profiles("resolver")).containsExactly("resolver");
        assertThat(Roles.profiles("dispatcher")).containsExactly("dispatcher");
        assertThat(Roles.profiles("migrate")).containsExactly("migrate");
    }

    @Test
    void aListRunsJustThoseRolesAndTheOneShotsRunAlone() {
        assertThat(Roles.profiles("ingest, settlement-worker,resolver"))
                .containsExactly("ingest", "settlement-worker", "resolver");
        assertThat(Roles.profiles("matcher,matcher")).containsExactly("matcher");
        assertThat(Roles.profiles("verify")).containsExactly("verify");
        assertThat(Roles.profiles("verify-audit")).containsExactly("verify-audit");
        assertThatThrownBy(() -> Roles.profiles("verify,matcher")).hasMessageContaining("must be");
        assertThatThrownBy(() -> Roles.profiles("migrate,ingest")).hasMessageContaining("must be");
    }

    @Test
    void anUnknownRoleIsRefusedRatherThanSilentlyRunningNothing() {
        assertThatThrownBy(() -> Roles.profiles("everything")).hasMessageContaining("must be");
        assertThatThrownBy(() -> Roles.profiles("ingest,")).hasMessageContaining("must be");
    }
}
