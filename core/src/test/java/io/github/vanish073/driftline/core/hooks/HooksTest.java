package io.github.vanish073.driftline.core.hooks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class HooksTest {

    @Test
    void noneHasNoCanaries() {
        assertThat(Canaries.NONE.isNone()).isTrue();
        for (Canary canary : Canary.values()) {
            assertThat(Canaries.NONE.isEnabled(canary)).isFalse();
        }
    }

    @Test
    void canariesAreKeptInDeclarationOrderAndNeverTwice() {
        Canaries some = Canaries.of(Canary.NO_FENCING, Canary.DEDUPE_OFF);
        assertThat(some.enabled()).containsExactly(Canary.DEDUPE_OFF, Canary.NO_FENCING);
        assertThat(some.isEnabled(Canary.NO_FENCING)).isTrue();
        assertThat(some.isEnabled(Canary.NO_REFUND_KEY)).isFalse();
        assertThat(some).isEqualTo(Canaries.of(Canary.DEDUPE_OFF, Canary.NO_FENCING));
        assertThatThrownBy(() -> Canaries.of(Canary.DEDUPE_OFF, Canary.DEDUPE_OFF)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> some.enabled().add(Canary.NO_REFUND_KEY)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void theElevenCanariesOfTheDesignAreNumberedInOrder() {
        assertThat(Canary.values()).hasSize(11);
        for (int i = 0; i < Canary.values().length; i++) {
            assertThat(Canary.values()[i].code()).isEqualTo("C" + (i < 9 ? "0" : "") + (i + 1));
        }
    }

    @Test
    void theProductionCrashHookDoesNothing() {
        for (CrashPoint point : CrashPoint.values()) {
            assertThatCode(() -> CrashPoints.NONE.reached(point)).doesNotThrowAnyException();
        }
        assertThat(CrashPoint.values()).hasSize(8);
    }
}
