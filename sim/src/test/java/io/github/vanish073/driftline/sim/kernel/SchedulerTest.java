package io.github.vanish073.driftline.sim.kernel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SchedulerTest {

    @Test
    void runsByTimeThenInsertionOrder() {
        Scheduler s = new Scheduler(Instant.EPOCH);
        List<String> ran = new ArrayList<>();
        s.at(20, "n", () -> ran.add("b20"));
        s.at(10, "n", () -> ran.add("a10"));
        s.at(20, "n", () -> ran.add("c20"));
        s.at(10, "n", () -> {
            ran.add("d10");
            s.after(10, "n", () -> ran.add("e20-inserted-last"));
        });
        while (s.step(Long.MAX_VALUE)) {
            // run everything
        }
        assertThat(ran).containsExactly("a10", "d10", "b20", "c20", "e20-inserted-last");
        assertThat(s.now()).isEqualTo(20);
        assertThat(s.instant()).isEqualTo(Instant.EPOCH.plusMillis(20));
    }

    @Test
    void stopsAtTheLimitAndRefusesThePast() {
        Scheduler s = new Scheduler(Instant.EPOCH);
        s.at(100, "n", () -> { });
        assertThat(s.step(99)).isFalse();
        assertThat(s.step(100)).isTrue();
        assertThatThrownBy(() -> s.at(50, "n", () -> { })).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void advanceToRunsWhatIsDueThenMovesTheClockEvenWithNothingQueued() {
        Scheduler s = new Scheduler(Instant.EPOCH);
        List<String> ran = new ArrayList<>();
        s.at(10, "a", () -> ran.add("a@" + s.now()));
        s.at(50, "b", () -> ran.add("b@" + s.now()));
        s.advanceTo(30);
        assertThat(ran).containsExactly("a@10");
        assertThat(s.now()).isEqualTo(30);
        s.advanceTo(20);
        assertThat(s.now()).as("never backwards").isEqualTo(30);
        s.advanceTo(1_000);
        assertThat(ran).containsExactly("a@10", "b@50");
        assertThat(s.now()).isEqualTo(1_000);
    }
}
