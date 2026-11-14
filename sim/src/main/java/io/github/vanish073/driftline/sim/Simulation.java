package io.github.vanish073.driftline.sim;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sim.invariant.Invariants;
import io.github.vanish073.driftline.sim.invariant.Violation;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimClock;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.kernel.SimNetwork;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.sim.node.IngestNode;
import io.github.vanish073.driftline.sim.node.MatcherNode;
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

/**
 * One deterministic run (DESIGN §11). Everything is driven by the seed through separate streams: the workload decides
 * what happens and the network decides delays. The real ingest normaliser, matcher, state machines and ledger rules run
 * against fakes of the gateway, merchants, bank, network, clock, log and store. Invariants are checked after every step;
 * the first violation stops the run.
 */
public final class Simulation {

    /** 00:00 IST on 2026-11-13: the start of traffic day 1. */
    public static final Instant EPOCH = Instant.parse("2026-11-12T18:30:00Z");
    static final long DAY_MILLIS = 86_400_000L;
    static final long CUSTOMER_FUNDS_PAISE = 10_000_000L;
    static final long MAX_STEPS = 2_000_000L;

    private Simulation() {
    }

    public static RunResult run(long seed, Profile profile, boolean keepTrace) {
        SplitMix64 workloadRng = SplitMix64.stream(seed, "workload");
        SplitMix64 networkRng = SplitMix64.stream(seed, "network");
        Scheduler scheduler = new Scheduler(EPOCH);
        Trace trace = new Trace(keepTrace);
        trace.line(0, "kernel", "seed=" + seed + " profile=" + profile.id() + " swarm=none");

        InMemoryStore store = new InMemoryStore();
        SimLog log = new SimLog(profile.partitions);
        SimNetwork network = new SimNetwork(scheduler, networkRng);
        FakeBank bank = new FakeBank();
        Invariants invariants = new Invariants(store, bank);

        List<MerchantId> merchantIds = new ArrayList<>();
        TreeMap<MerchantId, byte[]> secrets = new TreeMap<>();
        TreeMap<MerchantId, MerchantContext> contexts = new TreeMap<>();
        for (int i = 0; i < profile.merchants; i++) {
            MerchantId m = MerchantId.of("m_" + i);
            merchantIds.add(m);
            byte[] secret = Sha256.of(("webhook-secret/" + seed + "/" + m).getBytes(StandardCharsets.UTF_8)).hex()
                    .getBytes(StandardCharsets.US_ASCII);
            secrets.put(m, secret);
            contexts.put(m, new MerchantContext(m, i % profile.partitions, List.of(secret)));
            bank.open(FakeGateway.merchantAccount(m), 0);
        }
        for (int c = 0; c < profile.customers; c++) {
            bank.open("customer:" + c, CUSTOMER_FUNDS_PAISE);
        }

        TreeMap<Integer, MatcherNode> matchers = new TreeMap<>();
        for (int p = 0; p < profile.partitions; p++) {
            matchers.put(p, new MatcherNode(p, store, log, scheduler, trace, invariants::anomaly));
        }
        IngestNode ingest = new IngestNode(scheduler, new SimClock(scheduler), contexts, log, matchers, trace);
        TreeMap<MerchantId, FakeMerchant> merchants = new TreeMap<>();
        for (MerchantId m : merchantIds) {
            merchants.put(m, new FakeMerchant(m, scheduler, network, ingest, trace));
        }
        FakeGateway gateway = new FakeGateway(scheduler, network, bank, ingest, trace, secrets, merchants);

        int orderCount = workloadRng.between(profile.minOrders, profile.maxOrders);
        List<PlannedOrder> orders = Workload.generate(workloadRng, merchantIds, profile.customers, orderCount,
                profile.trafficDays * DAY_MILLIS);
        TreeMap<String, Long> stats = new TreeMap<>();
        for (PlannedOrder order : orders) {
            stats.merge("scenario." + order.scenario().name(), 1L, Long::sum);
            scheduler.at(order.createdAtMillis(), "workload", () -> play(order, scheduler, workloadRng, gateway,
                    merchants.get(order.merchantId())));
        }
        long endMillis = (profile.trafficDays + profile.drainDays) * DAY_MILLIS;
        for (int day = 1; day <= profile.trafficDays + profile.drainDays; day++) {
            LocalDate settlementDate = IstCalendar.istDate(EPOCH).plusDays(day);
            long tenAmIst = IstCalendar.startOfIstDay(settlementDate).plusSeconds(10 * 3_600).toEpochMilli()
                    - EPOCH.toEpochMilli();
            scheduler.at(tenAmIst, "gateway", () -> gateway.settle(settlementDate));
        }

        Optional<Violation> violation = Optional.empty();
        while (violation.isEmpty() && scheduler.step(endMillis)) {
            violation = invariants.afterStep(scheduler.now());
            if (scheduler.steps() > MAX_STEPS) {
                violation = Optional.of(new Violation("LIVENESS", "more than " + MAX_STEPS + " steps", scheduler.now()));
            }
        }
        if (violation.isEmpty()) {
            violation = invariants.atEnd(scheduler.now(), log, gateway.undelivered()
                    + merchants.values().stream().mapToLong(FakeMerchant::undelivered).sum());
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
        stats.put("settlement.lines", (long) gateway.settlementLines().size());
        long records = 0;
        for (int p = 0; p < log.partitionCount(); p++) {
            records += log.endOffset(p);
        }
        stats.put("log.records", records);
        return new RunResult(seed, profile, scheduler.steps(), scheduler.now(), trace.digestHex(), violation, stats,
                trace.tail(), trace.all());
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
                    long lateBy = rng.between(30_000L, 30 * 60_000L);
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
