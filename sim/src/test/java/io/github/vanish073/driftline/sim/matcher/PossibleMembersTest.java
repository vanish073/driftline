package io.github.vanish073.driftline.sim.matcher;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventPayload;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.SettlementFileEndPayload;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.matcher.BatchOutcome;
import io.github.vanish073.driftline.core.matcher.LogRecord;
import io.github.vanish073.driftline.core.matcher.Matcher;
import io.github.vanish073.driftline.core.matcher.MerchantDirectory;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.matcher.PartitionBatch;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.OutboxRepo.Command;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandKind;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandStatus;
import io.github.vanish073.driftline.core.port.SettlementRepo.Line;
import io.github.vanish073.driftline.core.port.SettlementRepo.LineStatus;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * Payments whose outcome Driftline has not learned are possible members of unreferenced lines (BUG-006, D69), with
 * two limits (BUG-007): one first seen after a line's window cannot be in it, and once a possible member's outcome is
 * learned, an ambiguity it witnessed is solved again. And a capture Driftline never saw at all is found by the payment
 * sweep the lines wait for (BUG-010, D72).
 */
class PossibleMembersTest {

    static final MerchantId M = MerchantId.of("m_0");
    static final FeeSchedule FEES = new FeeSchedule(200, 1_800);
    static final LocalDate D = LocalDate.parse("2026-11-14");
    static final Money PRICE = Money.ofPaise(49_900);
    static final Money NET = FEES.expected(PRICE).net();

    final InMemoryStore store = new InMemoryStore();
    final List<LogRecord> log = new ArrayList<>();
    final Matcher matcher = new Matcher(new MerchantDirectory(List.of(new MerchantTerms(M, 0, FEES, 1, D))));
    final long epoch = store.transact("assign", tx -> tx.partitions().acquire(0, "matcher-a.1")).epoch();
    int pendingFrom;

    static Instant ist(LocalDate day, int hour) {
        return IstCalendar.startOfIstDay(day).plusSeconds(hour * 3_600L);
    }

    void append(Source source, EventType type, Instant at, EventPayload payload) {
        String key = "k" + log.size();
        log.add(new LogRecord(0, log.size(), new EventEnvelope(1, M, 0, source, key, type, at, at,
                Sha256.of(key.getBytes(StandardCharsets.UTF_8)), payload, Optional.empty())));
    }

    void batch(Instant now) {
        List<LogRecord> records = List.copyOf(log.subList(pendingFrom, log.size()));
        pendingFrom = log.size();
        BatchOutcome o = store.transact("batch", tx -> matcher.process(new PartitionBatch(0, epoch, now, records), tx));
        matcher.committed(o);
    }

    void tick(Instant now) {
        BatchOutcome o = store.transact("tick", tx -> matcher.tick(0, epoch, now, tx));
        matcher.committed(o);
    }

    /** The world's captures: payment id → capture time. */
    final TreeMap<String, Instant> captured = new TreeMap<>();

    /** Plays the gateway's captures API (D72) for every open sweep, then lets the deferred solve run. */
    void sweepAndTick(Instant now) {
        store.transact("sweep", tx -> {
            for (Command c : store.committedCommands()) {
                if (c.kind() != CommandKind.PAYMENT_SWEEP
                        || tx.outbox().find(c.commandId()).filter(x -> x.status() != CommandStatus.DONE).isEmpty()) {
                    continue;
                }
                LocalDate day = LocalDate.parse(c.args().get("captured_on"));
                TreeMap<String, String> answer = new TreeMap<>();
                answer.put("as_of", now.toString());
                int i = 0;
                for (Map.Entry<String, Instant> e : captured.entrySet()) {
                    if (IstCalendar.istDate(e.getValue()).equals(day)) {
                        i++;
                        answer.put("payment_" + i, e.getKey());
                        answer.put("order_" + i, "ord_" + e.getKey());
                        answer.put("amount_" + i, PRICE.toString());
                        answer.put("at_" + i, e.getValue().toString());
                    }
                }
                answer.put("count", Integer.toString(i));
                tx.outbox().complete(c.commandId(), answer);
            }
            return null;
        });
        tick(now.plusSeconds(180));
    }

    void order(String payment, Instant at) {
        append(Source.MERCHANT, EventType.ORDER_CREATED, at.minusSeconds(60),
                new OrderPayload(OrderId.of("ord_" + payment), 1, PRICE, at.plusSeconds(900), Optional.empty()));
    }

    void payment(String id, EventType type, Instant at) {
        append(Source.GATEWAY, type, at, new PaymentPayload(PaymentId.of(id), OrderId.of("ord_" + id), PRICE));
    }

    void succeeded(String id, Instant at) {
        captured.put(id, at);
        order(id, at);
        payment(id, EventType.PAYMENT_SUCCEEDED, at);
        append(Source.MERCHANT, EventType.ORDER_PAID, at.plusSeconds(5), new OrderPayload(OrderId.of("ord_" + id), 2,
                PRICE, at.plusSeconds(900), Optional.of(PaymentId.of(id))));
    }

    /** The file for D: one unreferenced line worth one payment's net. */
    void fileWithOneBatchLine() {
        String fileId = "stl_m_0_20261114";
        Instant at = ist(D, 10);
        append(Source.BANK, EventType.SETTLEMENT_LINE, at, new SettlementLinePayload(fileId, 1, D, D, NET,
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.of("UTR20261114"), "batch payout"));
        append(Source.BANK, EventType.SETTLEMENT_FILE_END, at, new SettlementFileEndPayload(fileId, D, 1, NET,
                Sha256.of(fileId.getBytes(StandardCharsets.UTF_8))));
    }

    Line line() {
        return store.committedLines().stream().filter(l -> l.key().equals("stl_m_0_20261114:1")).findFirst()
                .orElseThrow();
    }

    @Test
    void aPaymentFirstSeenAfterTheWindowClosedIsNotAPossibleMember() {
        succeeded("pay_1", ist(D.minusDays(1), 12));             // batch date D: in the window
        order("pay_3", ist(D, 9));
        payment("pay_3", EventType.PAYMENT_PENDING, ist(D, 9)); // first seen on D: captured on D at the earliest
        fileWithOneBatchLine();
        batch(ist(D, 10).plusSeconds(1_800));
        tick(ist(D, 20));
        sweepAndTick(ist(D, 20));
        assertThat(line().status()).as("pay_1 is the only payment that can be in the line").isEqualTo(LineStatus.INFERRED);
    }

    @Test
    void anAmbiguityWitnessedByAPendingPaymentIsSolvedAgainWhenThatPaymentFails() {
        succeeded("pay_1", ist(D.minusDays(1), 12));
        order("pay_2", ist(D.minusDays(1), 13));
        payment("pay_2", EventType.PAYMENT_PENDING, ist(D.minusDays(1), 13));   // may have succeeded on D - 1
        fileWithOneBatchLine();
        batch(ist(D, 10).plusSeconds(1_800));
        tick(ist(D, 20));
        sweepAndTick(ist(D, 20));
        assertThat(line().status()).as("pay_1 or the pending pay_2").isEqualTo(LineStatus.AMBIGUOUS);
        assertThat(line().witnesses()).containsExactlyInAnyOrder(List.of(PaymentId.of("pay_1")),
                List.of(PaymentId.of("pay_2")));

        payment("pay_2", EventType.PAYMENT_FAILED, ist(D, 21));
        batch(ist(D, 21).plusSeconds(60));
        assertThat(line().status()).as("pay_2 failed, so only pay_1 fits").isEqualTo(LineStatus.INFERRED);
        assertThat(line().witnesses()).isEmpty();
    }

    @Test
    void theLinesWaitForTheSweepAndACaptureOnlyTheSweepRevealsMakesTheLineAmbiguous() {
        // BUG-010: pay_9 left no trace at all (every webhook lost); its net equals pay_1's, and the world paid it
        succeeded("pay_1", ist(D.minusDays(1), 12));
        captured.put("pay_9", ist(D.minusDays(1), 12).plusSeconds(60));
        fileWithOneBatchLine();
        batch(ist(D, 10).plusSeconds(1_800));
        tick(ist(D, 20));
        assertThat(line().status()).as("not solved before the window's days are swept").isEqualTo(LineStatus.AWAITING_BATCH);
        assertThat(store.committedCommands()).filteredOn(c -> c.kind() == CommandKind.PAYMENT_SWEEP)
                .extracting(c -> c.args().get("captured_on")).containsExactlyInAnyOrder("2026-11-12", "2026-11-13");

        sweepAndTick(ist(D, 20));
        assertThat(line().status()).as("pay_9's observation has not arrived yet").isEqualTo(LineStatus.AWAITING_BATCH);

        payment("pay_9", EventType.PAYMENT_SUCCEEDED, captured.get("pay_9"));   // the dispatcher's observation
        batch(ist(D, 20).plusSeconds(240));
        tick(ist(D, 20).plusSeconds(420));
        assertThat(line().status()).as("pay_1 or pay_9: reported, not guessed").isEqualTo(LineStatus.AMBIGUOUS);
        assertThat(line().witnesses()).containsExactlyInAnyOrder(List.of(PaymentId.of("pay_1")),
                List.of(PaymentId.of("pay_9")));
    }
}
