package io.github.vanish073.driftline.sim.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.dispatcher.Dispatcher;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.core.port.OutboundCall;
import io.github.vanish073.driftline.core.port.OutboxRepo.Command;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandKind;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandStatus;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/** The dispatcher's loop (DESIGN §9.2) against the in-memory store, with the test as the network. */
class DispatcherTest {

    static final MerchantId M = MerchantId.of("m_0");
    static final Instant T0 = Instant.parse("2026-11-13T06:00:00Z");

    final InMemoryStore store = new InMemoryStore();
    Dispatcher dispatcher = new Dispatcher();
    final Break b = Break.detected(M, BreakType.DEBITED_NO_ORDER, "pay_1", T0, Map.of());

    DispatcherTest() {
        store.transact("setup", tx -> tx.breaks().raise(b, "test"));
    }

    void refundCommand(Instant at) {
        store.transact("enqueue", tx -> tx.outbox().enqueue(Command.pending("cmd_r", M, Optional.of(b.breakId()),
                CommandKind.REFUND, new TreeMap<>(Map.of("payment_id", "pay_1", "order_id", "ord_1", "amount", "499.00",
                        "seq", "1")), Optional.of("rfd_key"), at)));
    }

    List<OutboundCall> claim(Instant now, String owner) {
        return store.transact("claim", tx -> dispatcher.claim(tx, now, owner));
    }

    List<OutboundCall> result(OutboundCall call, Map<String, String> facts, Instant now, String owner) {
        return store.transact("result", tx -> dispatcher.onResult(tx, call, new TreeMap<>(facts), now, owner));
    }

    Command command(String id) {
        return store.transact("read", tx -> tx.outbox().find(id)).orElseThrow();
    }

    @Test
    void aRefundIsSentWithItsKeyAndATimeoutRetriesItLaterWithTheSameKey() {
        refundCommand(T0);
        List<OutboundCall> first = claim(T0, "d1");
        assertThat(first).hasSize(1);
        OutboundCall call = first.get(0);
        assertThat(call.callId()).isEqualTo("cmd_r.1");
        assertThat(call.target()).isEqualTo(CallTarget.GATEWAY);
        assertThat(call.idempotencyKey()).contains("rfd_key");
        assertThat(call.request()).isEqualTo(new CallRequest.Refund(PaymentId.of("pay_1"), OrderId.of("ord_1"),
                Money.ofPaise(49_900)));

        Command leased = command("cmd_r");
        Duration timeout = dispatcher.config().callTimeout();
        assertThat(leased.leaseUntil()).contains(T0.plus(timeout));
        assertThat(leased.nextAttemptAt()).isAfter(T0.plus(timeout));
        assertThat(claim(T0.plus(timeout).minusSeconds(1), "d2")).as("still leased").isEmpty();

        List<OutboundCall> retry = claim(leased.nextAttemptAt(), "d2");
        assertThat(retry).extracting(OutboundCall::callId).containsExactly("cmd_r.2");
        assertThat(retry.get(0).idempotencyKey()).as("the same key on every attempt").contains("rfd_key");
    }

    @Test
    void theBackoffIsDeterministicAndGrowsWithTheAttempt() {
        refundCommand(T0);
        Command c = command("cmd_r");
        Command a1 = new Command(c.commandId(), c.merchantId(), c.breakId(), c.kind(), c.args(), c.idempotencyKey(),
                CommandStatus.IN_FLIGHT, 1, Optional.of("d"), Optional.of(T0), T0, Optional.of(T0), Optional.empty());
        Command a4 = new Command(c.commandId(), c.merchantId(), c.breakId(), c.kind(), c.args(), c.idempotencyKey(),
                CommandStatus.IN_FLIGHT, 4, Optional.of("d"), Optional.of(T0), T0, Optional.of(T0), Optional.empty());
        assertThat(dispatcher.backoff(a1)).isEqualTo(dispatcher.backoff(a1));
        assertThat(dispatcher.backoff(a4)).isGreaterThan(dispatcher.backoff(a1));
        Command a60 = new Command(c.commandId(), c.merchantId(), c.breakId(), c.kind(), c.args(), c.idempotencyKey(),
                CommandStatus.IN_FLIGHT, 60, Optional.of("d"), Optional.of(T0), T0, Optional.of(T0), Optional.empty());
        assertThat(dispatcher.backoff(a60)).as("capped")
                .isLessThanOrEqualTo(dispatcher.config().backoffCap().plus(dispatcher.config().backoffBase()));
    }

    @Test
    void aProcessedRefundIsRecordedOncePublishedAsAnObservationAndWakesTheBreak() {
        refundCommand(T0);
        OutboundCall call = claim(T0, "d1").get(0);
        Map<String, String> processed = Map.of("status", "PROCESSED", "refund_id", "rfd_9",
                "processed_at", "2026-11-13T06:00:05Z");
        result(call, processed, T0.plusSeconds(5), "d1");
        result(call, Map.of("status", "FAILED"), T0.plusSeconds(6), "d1");   // a duplicate reply: the first wins
        assertThat(command("cmd_r").result().orElseThrow()).containsEntry("status", "PROCESSED");

        List<Command> observations = store.committedCommands().stream()
                .filter(c -> c.kind() == CommandKind.PUBLISH_OBSERVATION).toList();
        assertThat(observations).hasSize(1);
        OutboundCall publish = Dispatcher.call(observations.get(0));
        assertThat(publish.target()).isEqualTo(CallTarget.DRIFTLINE);
        CallRequest.Observation o = (CallRequest.Observation) publish.request();
        assertThat(o.eventType()).isEqualTo(EventType.REFUND_PROCESSED);
        assertThat(o.eventTime()).isEqualTo(Instant.parse("2026-11-13T06:00:05Z"));
        assertThat(((RefundPayload) o.payload()).amount().paise()).isEqualTo(49_900);
    }

    @Test
    void anOldRetryLooksUpFirstAndSendsTheRefundOnlyIfNoneExists() {
        refundCommand(T0);
        claim(T0, "d1");
        Instant late = T0.plus(Duration.ofHours(24));   // past key TTL - margin since the first send
        List<OutboundCall> second = claim(late, "d2");
        assertThat(second).hasSize(1);
        assertThat(second.get(0).request()).isInstanceOf(CallRequest.RefundLookup.class);
        assertThat(second.get(0).callId()).isEqualTo("cmd_r.2.lookup");

        List<OutboundCall> then = result(second.get(0), Map.of("found", "no"), late.plusSeconds(1), "d2");
        assertThat(then).extracting(c -> c.request().getClass().getSimpleName()).containsExactly("Refund");
        assertThat(then.get(0).idempotencyKey()).contains("rfd_key");
        assertThat(result(second.get(0), Map.of("found", "no"), late.plusSeconds(1), "d1"))
                .as("only the lease holder sends").isEmpty();
    }

    @Test
    void aLookupThatFindsTheRefundCompletesItWithoutSendingAgain() {
        refundCommand(T0);
        claim(T0, "d1");
        Instant late = T0.plus(Duration.ofHours(24));
        OutboundCall lookup = claim(late, "d2").get(0);
        List<OutboundCall> then = result(lookup, Map.of("found", "yes", "refund_id", "rfd_7",
                "processed_at", "2026-11-13T06:00:09Z"), late.plusSeconds(1), "d2");
        assertThat(then).isEmpty();
        assertThat(command("cmd_r").result().orElseThrow()).containsEntry("status", "PROCESSED")
                .containsEntry("via", "lookup");
    }

    @Test
    void canaryC04SkipsTheLookupAndC03MarksTheRefundDoneBeforeSending() {
        dispatcher = new Dispatcher(Dispatcher.Config.DEFAULT, Canaries.of(Canary.SKIP_TTL_LOOKUP), CrashPoints.NONE);
        refundCommand(T0);
        claim(T0, "d1");
        assertThat(claim(T0.plus(Duration.ofHours(24)), "d2").get(0).request()).isInstanceOf(CallRequest.Refund.class);

        DispatcherTest other = new DispatcherTest();
        other.dispatcher = new Dispatcher(Dispatcher.Config.DEFAULT, Canaries.of(Canary.MARK_BEFORE_SEND), CrashPoints.NONE);
        other.refundCommand(T0);
        assertThat(other.claim(T0, "d1")).hasSize(1);
        assertThat(other.command("cmd_r").status()).isEqualTo(CommandStatus.DONE);
    }

    @Test
    void aCrashAtCp5LosesTheResultAndTheRetryRecordsItThen() {
        refundCommand(T0);
        OutboundCall call = claim(T0, "d1").get(0);
        dispatcher = new Dispatcher(Dispatcher.Config.DEFAULT, Canaries.NONE, point -> {
            if (point == CrashPoint.CP5) {
                throw new IllegalStateException("crash at CP5");
            }
        });
        assertThatThrownBy(() -> result(call, Map.of("status", "PROCESSED"), T0.plusSeconds(1), "d1"))
                .hasMessage("crash at CP5");
        assertThat(command("cmd_r").status()).as("the result was lost").isEqualTo(CommandStatus.IN_FLIGHT);
        dispatcher = new Dispatcher();
        OutboundCall retry = claim(command("cmd_r").nextAttemptAt(), "d2").get(0);
        result(retry, Map.of("status", "PROCESSED"), command("cmd_r").nextAttemptAt().plusSeconds(1), "d2");
        assertThat(command("cmd_r").status()).isEqualTo(CommandStatus.DONE);
    }

    @Test
    void aStatusThatRevealsAnUnseenSuccessIsPublishedButAKnownOneIsNot() {
        store.transact("probe", tx -> tx.outbox().enqueue(Command.pending("cmd_p", M, Optional.empty(),
                CommandKind.STATUS_QUERY_PAYMENT, new TreeMap<>(Map.of("payment_id", "pay_7", "seq", "1")),
                Optional.empty(), T0)));
        OutboundCall call = claim(T0, "d1").get(0);
        result(call, Map.of("status", "SUCCEEDED", "order_id", "ord_7", "amount", "250.00",
                "at", "2026-11-13T05:59:00Z"), T0.plusSeconds(1), "d1");
        List<Command> observations = store.committedCommands().stream()
                .filter(c -> c.kind() == CommandKind.PUBLISH_OBSERVATION).toList();
        assertThat(observations).hasSize(1);
        CallRequest.Observation o = (CallRequest.Observation) Dispatcher.call(observations.get(0)).request();
        assertThat(o.eventType()).isEqualTo(EventType.PAYMENT_SUCCEEDED);
        assertThat(o.eventTime()).as("the gateway's time, not the dispatcher's").isEqualTo(Instant.parse("2026-11-13T05:59:00Z"));
        assertThat(observations.get(0).breakId()).as("a probe has no break").isEmpty();
    }
}
