package io.github.vanish073.driftline.sim.world;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.sim.kernel.Scheduler;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** At-least-once delivery retries with one key on the backoff schedule and gives up after the horizon (DESIGN §2.1). */
class DeliveryTest {

    private final Scheduler scheduler = new Scheduler(Instant.parse("2026-11-12T18:30:00Z"));
    private final List<Long> attemptsAt = new ArrayList<>();
    private final List<Long> gaveUpAt = new ArrayList<>();

    private Delivery delivery(int ackOnAttempt) {
        return new Delivery(scheduler, "merchant", (attemptNo, answered, reply) -> {
            attemptsAt.add(scheduler.now());
            if (attemptNo == ackOnAttempt) {
                answered.accept(200);
                reply.accept(200);
            } else if (attemptNo % 2 == 0) {
                answered.accept(503);
                reply.accept(503);
            }
        }, at -> {
            gaveUpAt.add(at);
            return null;
        });
    }

    private void drain() {
        while (scheduler.step(Long.MAX_VALUE)) {
            // run to quiescence
        }
    }

    @Test
    void retriesOnTheBackoffScheduleThenEvery6HoursAndGivesUpAfterTheHorizon() {
        Delivery d = delivery(Integer.MAX_VALUE);
        d.start();
        drain();
        assertThat(attemptsAt.subList(0, 6)).containsExactly(0L, 30_000L, 150_000L, 750_000L, 4_350_000L, 25_950_000L);
        for (int i = 6; i < attemptsAt.size(); i++) {
            assertThat(attemptsAt.get(i) - attemptsAt.get(i - 1)).isEqualTo(21_600_000L);
        }
        assertThat(attemptsAt.getLast()).isLessThanOrEqualTo(Delivery.HORIZON_MILLIS);
        assertThat(attemptsAt.getLast() + 21_600_000L).isGreaterThan(Delivery.HORIZON_MILLIS);
        assertThat(d.gaveUp()).isTrue();
        assertThat(d.acked()).isFalse();
        assertThat(d.accepted()).as("never answered with a 2xx: a failure to drain").isFalse();
        assertThat(gaveUpAt).containsExactly(attemptsAt.getLast() + 21_600_000L);
        assertThat(d.attempts()).isEqualTo(attemptsAt.size());
    }

    @Test
    void aTwoHundredReplyStopsTheRetries() {
        Delivery d = delivery(3);
        d.start();
        drain();
        assertThat(d.acked()).isTrue();
        assertThat(d.accepted()).isTrue();
        assertThat(d.gaveUp()).isFalse();
        assertThat(attemptsAt).containsExactly(0L, 30_000L, 150_000L);
        assertThat(gaveUpAt).isEmpty();
    }

    @Test
    void aMessageDriftlineAcceptedIsDrainedEvenIfEveryAnswerWasLostAndTheSenderGaveUp() {
        // D87 (seed 209037988302897762, nightly): ingest answered 202 eleven times, every answer was lost on the way
        // back, and the merchant gave up after 3 days, all inside the 5 faulty traffic days
        Delivery d = new Delivery(scheduler, "merchant", (attemptNo, answered, reply) -> {
            attemptsAt.add(scheduler.now());
            answered.accept(202);
        }, at -> {
            gaveUpAt.add(at);
            return null;
        });
        d.start();
        drain();
        assertThat(d.gaveUp()).isTrue();
        assertThat(d.acked()).isFalse();
        assertThat(d.accepted()).isTrue();
        assertThat(d.attempts()).isEqualTo(attemptsAt.size()).isGreaterThan(6);
        assertThat(d.undrained(Long.MAX_VALUE)).isFalse();
    }

    @Test
    void aMessageNeverAcceptedIsUndrainedOnlyIfFirstSentBeforeTheGraceBeforeTheEnd() {
        // D87 (seed 209053576945360396, nightly): a settlement file held back by FILE_LATE was released 88 ms before
        // the end of the run, too late for any delivery
        Delivery d = new Delivery(scheduler, "bank", (attemptNo, answered, reply) -> attemptsAt.add(scheduler.now()),
                at -> null);
        scheduler.after(5_000L, "bank", d::start);
        scheduler.step(Long.MAX_VALUE);
        assertThat(attemptsAt).containsExactly(5_000L);
        assertThat(d.undrained(5_000L)).as("sent at the cut-off: not judged").isFalse();
        assertThat(d.undrained(5_001L)).as("sent before it, never accepted").isTrue();
        assertThat(Delivery.DRAIN_GRACE_MILLIS).isEqualTo(600_000L);
    }
}
