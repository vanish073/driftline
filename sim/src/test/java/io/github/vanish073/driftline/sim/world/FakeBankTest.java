package io.github.vanish073.driftline.sim.world;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import net.jqwik.api.constraints.Size;
import org.junit.jupiter.api.Test;

/** The simulator's own check (Shift 3 Prove): FakeBank conserves money under random transfers. */
class FakeBankTest {


    @Property(tries = 2_000)
    void randomTransfersNeverCreateOrDestroyMoney(
            @ForAll @Size(max = 60) List<@IntRange(min = 0, max = 4) Integer> froms,
            @ForAll @Size(max = 60) List<@IntRange(min = 0, max = 4) Integer> tos,
            @ForAll @Size(max = 60) List<@LongRange(min = 1, max = 3_000) Long> amounts) {
        FakeBank bank = new FakeBank();
        for (int a = 0; a < 5; a++) {
            bank.open("a" + a, 1_000L * a);
        }
        int moves = Math.min(froms.size(), Math.min(tos.size(), amounts.size()));
        for (int i = 0; i < moves; i++) {
            String from = "a" + froms.get(i);
            long before = bank.balance(from);
            try {
                bank.transfer(from, "a" + tos.get(i), amounts.get(i));
            } catch (FakeBank.InsufficientFunds e) {
                assertThat(bank.balance(from)).isEqualTo(before);
            }
            assertThat(bank.total()).isEqualTo(bank.opened()).isEqualTo(10_000L);
            bank.accounts().values().forEach(b -> assertThat(b).isNotNegative());
        }
    }

    @Test
    void rejectsUnknownAccountsAndNonPositiveAmounts() {
        FakeBank bank = new FakeBank();
        bank.open("x", 10);
        assertThatThrownBy(() -> bank.transfer("x", "nobody", 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> bank.transfer("x", "x", 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> bank.open("x", 1)).isInstanceOf(IllegalArgumentException.class);
        assertThat(bank.balance("x")).isEqualTo(10);
    }
}
