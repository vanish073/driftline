package io.github.vanish073.driftline.sim;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sim.fault.FaultEvent;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
import io.github.vanish073.driftline.sim.fault.Swarm;
import io.github.vanish073.driftline.sim.invariant.Invariants;
import io.github.vanish073.driftline.sim.invariant.Violation;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimClock;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.kernel.SimNetwork;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.sim.node.Coordinator;
import io.github.vanish073.driftline.sim.node.IngestNode;
import io.github.vanish073.driftline.sim.node.MatcherInstance;
import io.github.vanish073.driftline.sim.random.SplitMix64;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import io.github.vanish073.driftline.sim.world.FakeBank;
import io.github.vanish073.driftline.sim.world.FakeGateway;
import io.github.vanish073.driftline.sim.world.FakeMerchant;
import io.github.vanish073.driftline.sim.world.PlannedOrder;
import io.github.vanish073.driftline.sim.world.Scenario;
import io.github.vanish073.driftline.sim.world.Workload;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One deterministic run (DESIGN §11). Everything is driven by the seed through separate streams (§11.2): the
 * workload decides what happens, the network the base delays, the swarm which faults are on, and the faults stream
 * when they strike. The real ingest normaliser, matcher, state machines and ledger rules run against fakes of the
 * gateway, merchants, bank, network, clocks, log and store, with two matcher instances under a partition coordinator.
 *
 * <p>Phases (§11.6): RUN (traffic plus faults) for the traffic days; HEAL (no new faults, crashed and paused nodes
 * come back, the store comes up, the network becomes reliable, clocks resynchronise); DRAIN to the end of the drain
 * days; then the final checks. Invariants are checked after every step; the first violation stops the run. An
 * exception escaping a step is reported as a violation too, never as a crash of the campaign.
 */
public final class Simulation {

    /** 00:00 IST on 2026-11-13: the start of traffic day 1. */
    public static final Instant EPOCH = Instant.parse("2026-11-12T18:30:00Z");
    public static final long DAY_MILLIS = 86_400_000L;
    static final long CUSTOMER_FUNDS_PAISE = 100_000_000L;
    static final long MAX_STEPS = 3_000_000L;
    static final List<String> MATCHERS = List.of("matcher-a", "matcher-b");
    /** An exception escaped a step: a bug in Driftline or in the simulator, reported like any other violation. */
    public static final String STEP_THREW = "SIM-STEP-THREW";
    public static final String LIVENESS = "SIM-LIVENESS";

    private Simulation() {
    }

    public static RunResult run(long seed, Profile profile, boolean keepTrace) {
        return run(seed, profile, Canaries.NONE, keepTrace);
    }

    public static RunResult run(long seed, Profile profile, Canaries canaries, boolean keepTrace) {
        SplitMix64 workloadRng = SplitMix64.stream(seed, "workload");
        SplitMix64 networkRng = SplitMix64.stream(seed, "network");
        Swarm swarm = profile.faults ? Swarm.draw(SplitMix64.stream(seed, "swarm")) : Swarm.NONE;
        FaultInjector faults = new FaultInjector(swarm, SplitMix64.stream(seed, "faults"));
        int merchantCount = workloadRng.between(profile.minMerchants, profile.maxMerchants);
        int partitions = workloadRng.between(profile.minPartitions, profile.maxPartitions);

        Scheduler scheduler = new Scheduler(EPOCH);
        Trace trace = new Trace(keepTrace);
        trace.line(0, "kernel", "seed=" + seed + " profile=" + profile.id() + " merchants=" + merchantCount
                + " partitions=" + partitions + " canaries=" + canaries.enabled() + " swarm=" + swarm);

        InMemoryStore store = new InMemoryStore(canaries);
        SimLog log = new SimLog(partitions);
        SimNetwork network = new SimNetwork(scheduler, networkRng, faults);
        FakeBank bank = new FakeBank();
        TreeMap<MerchantId, FakeMerchant> merchants = new TreeMap<>();
        Invariants invariants = new Invariants(store, bank, () -> reusedKeys(merchants));

        List<MerchantId> merchantIds = new ArrayList<>();
        TreeMap<MerchantId, byte[]> secrets = new TreeMap<>();
        TreeMap<MerchantId, MerchantContext> contexts = new TreeMap<>();
        for (int i = 0; i < merchantCount; i++) {
            MerchantId m = MerchantId.of("m_" + i);
            merchantIds.add(m);
            byte[] secret = Sha256.of(("webhook-secret/" + seed + "/" + m).getBytes(StandardCharsets.UTF_8)).hex()
                    .getBytes(StandardCharsets.US_ASCII);
            secrets.put(m, secret);
            contexts.put(m, new MerchantContext(m, i % partitions, List.of(secret)));
            bank.open(FakeGateway.merchantAccount(m), 0);
        }
        for (int c = 0; c < profile.customers; c++) {
            bank.open("customer:" + c, CUSTOMER_FUNDS_PAISE);
        }

        Coordinator coordinator = new Coordinator(scheduler, trace, partitions);
        TreeMap<String, MatcherInstance> matchers = new TreeMap<>();
        for (String name : MATCHERS) {
            matchers.put(name, new MatcherInstance(name, store, log, scheduler, trace, coordinator, faults, canaries,
                    invariants::anomaly));
        }
        IngestNode ingest = new IngestNode(scheduler, new SimClock(scheduler), contexts, log, coordinator, faults, trace);
        for (MerchantId m : merchantIds) {
            merchants.put(m, new FakeMerchant(m, scheduler, network, ingest, trace, faults));
        }
        FakeGateway gateway = new FakeGateway(scheduler, network, bank, ingest, trace, secrets, merchants);

        long runMillis = profile.trafficDays * DAY_MILLIS;
        long endMillis = (profile.trafficDays + profile.drainDays) * DAY_MILLIS;
        TreeMap<String, SimClock> clocks = new TreeMap<>();
        clocks.put(IngestNode.NAME, ingest.clock());
        matchers.forEach((name, m) -> clocks.put(name, m.clock()));
        scheduleFaults(faults, scheduler, trace, store, ingest, matchers, clocks, runMillis, profile.trafficDays);
        scheduler.at(runMillis, "kernel", () -> heal(faults, scheduler, trace, store, ingest, matchers, clocks));
        coordinator.start();

        int orderCount = workloadRng.between(profile.minOrders, profile.maxOrders);
        List<PlannedOrder> orders = Workload.generate(workloadRng, merchantIds, profile.customers, orderCount, runMillis);
        TreeMap<String, Long> stats = new TreeMap<>();
        for (PlannedOrder order : orders) {
            stats.merge("scenario." + order.scenario().name(), 1L, Long::sum);
            scheduler.at(order.createdAtMillis(), "workload", () -> play(order, scheduler, workloadRng, gateway,
                    merchants.get(order.merchantId())));
        }
        for (int day = 1; day <= profile.trafficDays + profile.drainDays; day++) {
            LocalDate settlementDate = IstCalendar.istDate(EPOCH).plusDays(day);
            long tenAmIst = IstCalendar.startOfIstDay(settlementDate).plusSeconds(10 * 3_600).toEpochMilli()
                    - EPOCH.toEpochMilli();
            scheduler.at(tenAmIst, "gateway", () -> gateway.settle(settlementDate));
        }

        Optional<Violation> violation = Optional.empty();
        try {
            while (violation.isEmpty() && scheduler.step(endMillis)) {
                violation = invariants.afterStep(scheduler.now());
                if (scheduler.steps() > MAX_STEPS) {
                    violation = Optional.of(new Violation(LIVENESS, "more than " + MAX_STEPS + " steps", scheduler.now()));
                }
            }
            if (violation.isEmpty()) {
                violation = invariants.atEnd(scheduler.now(), log, gateway.undelivered()
                        + merchants.values().stream().mapToLong(FakeMerchant::undelivered).sum());
            }
            if (violation.isEmpty() && !coordinator.allAssigned()) {
                violation = Optional.of(new Violation(Invariants.DRAINED, "a partition has no owner after HEAL",
                        scheduler.now()));
            }
        } catch (RuntimeException e) {
            violation = Optional.of(new Violation(STEP_THREW, scheduler.currentNode() + ": " + e, scheduler.now()));
        }
        violation.ifPresent(v -> trace.line(v.atMillis(), "kernel", "VIOLATION " + v.invariant() + ": " + v.detail()));

        stats.put("orders", (long) orders.size());
        stats.put("steps", scheduler.steps());
        stats.put("ingest.accepted", ingest.accepted());
        stats.put("ingest.rejected", ingest.rejected());
        stats.put("store.commits", store.commits());
        stats.put("ledger.transactions", (long) store.committedLedger().size());
        stats.put("transitions.payment", (long) store.committedPaymentTransitions().size());
        stats.put("transitions.order", (long) store.committedOrderTransitions().size());
        stats.put("quarantined", (long) store.committedQuarantine().size());
        for (var t : store.committedPaymentTransitions()) {
            stats.merge("rule.payment." + t.row().rule().name(), 1L, Long::sum);
        }
        stats.put("settlement.lines", (long) gateway.settlementLines().size());
        stats.put("coordinator.reassignments", coordinator.reassignments());
        long records = 0;
        for (int p = 0; p < log.partitionCount(); p++) {
            records += log.endOffset(p);
        }
        stats.put("log.records", records);
        faults.counters().forEach((k, v) -> stats.put("fault." + k, v));
        return new RunResult(seed, profile, canaries, swarm.toString(), scheduler.steps(), scheduler.now(),
                trace.digestHex(), violation, stats, trace.tail(), trace.all());
    }

    private static TreeSet<String> reusedKeys(TreeMap<MerchantId, FakeMerchant> merchants) {
        TreeSet<String> keys = new TreeSet<>();
        merchants.forEach((m, merchant) -> merchant.reusedKeys().forEach(k -> keys.add(m + "/" + k)));
        return keys;
    }

    /** Schedules the planned event faults and the per-node clock skews of the RUN phase. */
    private static void scheduleFaults(FaultInjector faults, Scheduler scheduler, Trace trace, InMemoryStore store,
                                       IngestNode ingest, TreeMap<String, MatcherInstance> matchers,
                                       TreeMap<String, SimClock> clocks, long runMillis, int trafficDays) {
        TreeMap<String, Long> skews = faults.skews(new ArrayList<>(clocks.keySet()));
        skews.forEach((node, offset) -> clocks.get(node).setOffsetMillis(offset));
        if (faults.swarm().isNone()) {
            return;
        }
        List<String> crashable = new ArrayList<>(MATCHERS);
        crashable.add(IngestNode.NAME);
        List<FaultEvent> plan = faults.plan(runMillis, trafficDays, crashable, MATCHERS, new ArrayList<>(clocks.keySet()));
        int[] storeDown = {0};
        for (FaultEvent event : plan) {
            scheduler.at(event.atMillis(), "faults", () -> {
                if (faults.healed()) {
                    return;
                }
                switch (event) {
                    case FaultEvent.Crash c when c.node().equals(IngestNode.NAME) -> ingest.crash(c.downtimeMillis());
                    case FaultEvent.Crash c when c.midBatch() -> matchers.get(c.node()).armCrash(c.downtimeMillis());
                    case FaultEvent.Crash c -> matchers.get(c.node()).crash(c.downtimeMillis());
                    case FaultEvent.Pause p -> matchers.get(p.node()).armPause(p.durationMillis());
                    case FaultEvent.StoreDown d -> {
                        storeDown[0]++;
                        store.setUnavailable(true);
                        faults.count("store.down.windows");
                        trace.line(scheduler.now(), "store", "down for " + d.durationMillis() + " ms");
                        scheduler.after(d.durationMillis(), "store", () -> {
                            if (storeDown[0] > 0 && --storeDown[0] == 0) {
                                store.setUnavailable(false);
                                trace.line(scheduler.now(), "store", "up");
                            }
                        });
                    }
                    case FaultEvent.ClockJump j -> {
                        SimClock clock = clocks.get(j.node());
                        long before = clock.offsetMillis();
                        clock.setOffsetMillis(before + j.stepMillis());
                        faults.count("clock.jumps");
                        trace.line(scheduler.now(), j.node(), "clock steps " + j.stepMillis() + " ms");
                        scheduler.after(j.durationMillis(), j.node(), () -> {
                            if (!faults.healed()) {
                                clock.setOffsetMillis(clock.offsetMillis() - j.stepMillis());
                                trace.line(scheduler.now(), j.node(), "clock corrected by " + -j.stepMillis() + " ms");
                            }
                        });
                    }
                }
            });
        }
        scheduler.at(runMillis, "faults", () -> storeDown[0] = 0);
    }

    /** HEAL (§11.6): stop faults, bring every node and the store back, resynchronise clocks. */
    private static void heal(FaultInjector faults, Scheduler scheduler, Trace trace, InMemoryStore store,
                             IngestNode ingest, TreeMap<String, MatcherInstance> matchers, TreeMap<String, SimClock> clocks) {
        faults.heal();
        store.setUnavailable(false);
        clocks.values().forEach(c -> c.setOffsetMillis(0));
        ingest.heal();
        matchers.values().forEach(MatcherInstance::heal);
        trace.line(scheduler.now(), "kernel", "HEAL");
    }

    /** Plays one order's scenario from its start time; every delay comes from the workload stream. */
    private static void play(PlannedOrder o, Scheduler scheduler, SplitMix64 rng, FakeGateway gateway, FakeMerchant merchant) {
        Scenario scenario = o.scenario();
        merchant.createOrder(o.orderId(), o.amount(), scenario != Scenario.ABANDONED);
        if (scenario == Scenario.ABANDONED) {
            return;
        }
        PaymentId first = PaymentId.of("pay_" + o.orderId().value().substring(4) + "_a");
        long opensAt = rng.between(1_000L, 5_000L);
        long decidedAfter = rng.between(1_000L, 30_000L);
        scheduler.after(opensAt, "gateway", () -> {
            FakeGateway.Payment p = gateway.start(o.merchantId(), first, o.orderId(), o.customer(), o.amount());
            switch (scenario) {
                case NORMAL -> scheduler.after(decidedAfter, "gateway", () -> gateway.succeed(p, true));
                case PAYMENT_FAILED -> scheduler.after(decidedAfter, "gateway", () -> gateway.fail(p));
                case DEBITED_NO_ORDER -> scheduler.after(decidedAfter, "gateway", () -> gateway.succeed(p, false));
                case LATE_SUCCESS -> {
                    long lateBy = rng.between(1_000L, 30 * 60_000L);   // a reversal can follow in seconds
                    scheduler.after(decidedAfter, "gateway", () -> gateway.fail(p));
                    scheduler.after(decidedAfter + lateBy, "gateway", () -> gateway.succeed(p, true));
                }
                case DUPLICATE_CAPTURE -> {
                    PaymentId second = PaymentId.of("pay_" + o.orderId().value().substring(4) + "_b");
                    long secondAfter = rng.between(2_000L, 20_000L);
                    scheduler.after(decidedAfter, "gateway", () -> gateway.succeed(p, true));
                    scheduler.after(secondAfter, "gateway", () -> {
                        FakeGateway.Payment q = gateway.start(o.merchantId(), second, o.orderId(), o.customer(), o.amount());
                        scheduler.after(decidedAfter, "gateway", () -> gateway.succeed(q, true));
                    });
                }
                case ABANDONED -> { }
            }
        });
    }
}
