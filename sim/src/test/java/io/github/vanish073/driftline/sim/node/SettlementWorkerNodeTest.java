package io.github.vanish073.driftline.sim.node;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
import io.github.vanish073.driftline.sim.fault.Swarm;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimClock;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.sim.random.SplitMix64;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import io.github.vanish073.driftline.wire.settlement.SettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFileWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/** The settlement worker's gate (DESIGN §6.2): what reaches the log, and what never does. */
class SettlementWorkerNodeTest {

    private static final MerchantId M = MerchantId.of("m_0");
    private static final Instant T0 = Instant.parse("2026-11-14T04:30:00Z");
    private static final LocalDate D = LocalDate.parse("2026-11-14");

    private final Scheduler scheduler = new Scheduler(T0);
    private final InMemoryStore store = new InMemoryStore();
    private final SimLog log = new SimLog(1);
    private final SettlementWorkerNode worker;
    private final List<Integer> replies = new ArrayList<>();

    SettlementWorkerNodeTest() {
        TreeMap<MerchantId, MerchantContext> merchants = new TreeMap<>();
        merchants.put(M, new MerchantContext(M, 0, List.of("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.US_ASCII))));
        Trace trace = new Trace(false);
        worker = new SettlementWorkerNode(scheduler, new SimClock(scheduler), merchants, store, log,
                new Coordinator(scheduler, trace, 1), new FaultInjector(Swarm.NONE, new SplitMix64(1)), trace);
    }

    private static byte[] file(Instant generatedAt) {
        SettlementLinePayload line = new SettlementLinePayload("stl_1", 1, D, D, Money.ofPaise(48_722), Optional.empty(),
                Optional.empty(), Optional.of("pay_1"), Optional.empty(), "settlement pay_1");
        return SettlementFileWriter.write(SettlementFile.of("stl_1", M, D, generatedAt, List.of(line)));
    }

    private void upload(byte[] body) {
        worker.upload(M, body, replies::add);
    }

    @Test
    void anAcceptedFileIsEmittedAndItsDuplicateIsEmittedAgainForDedupeToAbsorb() {
        upload(file(T0));
        assertThat(log.endOffset(0)).as("one line and the trailer").isEqualTo(2);
        assertThat(log.read(0, 0, 2)).extracting(r -> r.envelope().eventType())
                .containsExactly(EventType.SETTLEMENT_LINE, EventType.SETTLEMENT_FILE_END);
        upload(file(T0));
        assertThat(log.endOffset(0)).isEqualTo(4);
        assertThat(log.read(0, 2, 2)).extracting(r -> r.envelope().eventKey())
                .as("the same event keys, so the matcher's dedupe absorbs them")
                .containsExactlyElementsOf(log.read(0, 0, 2).stream().map(r -> r.envelope().eventKey()).toList());
        assertThat(replies).containsExactly(202, 202);
    }

    @Test
    void aConflictingFileAndEveryLaterCopyOfItNeverReachTheLog() {
        upload(file(T0));
        upload(file(T0.plusSeconds(3_600)));   // a different file for the same date
        upload(file(T0.plusSeconds(3_600)));   // the same refused bytes again (BUG-002, seed 143)
        assertThat(log.endOffset(0)).as("only the accepted file was emitted").isEqualTo(2);
        assertThat(store.committedBreaks()).extracting(Break::type).containsExactly(BreakType.FILE_CONFLICT);
        assertThat(store.committedBreaks().get(0).evidence()).containsKeys("file_id", "settlement_date", "sha256");
        assertThat(worker.conflicts()).containsExactly("m_0/2026-11-14");
        assertThat(replies).containsExactly(202, 202, 202);
    }

    @Test
    void aMalformedFileIsRejectedWithFourHundred() {
        upload("H,DLSF,1,stl_1,m_0,2026-11-14,2026-11-14T04:30:00Z\nT,1,0.00\n".getBytes(StandardCharsets.UTF_8));
        assertThat(replies).containsExactly(400);
        assertThat(log.endOffset(0)).isZero();
        assertThat(worker.rejected()).isEqualTo(1);
    }
}
