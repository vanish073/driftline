package io.github.vanish073.driftline.sim;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.dispatcher.Dispatcher;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.matcher.MatcherConfig;
import io.github.vanish073.driftline.core.matcher.MerchantDirectory;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.resolver.Resolver;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
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
import io.github.vanish073.driftline.sim.node.DispatcherNode;
import io.github.vanish073.driftline.sim.node.ForcedCrash;
import io.github.vanish073.driftline.sim.node.IngestNode;
import io.github.vanish073.driftline.sim.node.MatcherInstance;
import io.github.vanish073.driftline.sim.node.ResolverNode;
import io.github.vanish073.driftline.sim.node.SettlementWorkerNode;
import io.github.vanish073.driftline.sim.random.SplitMix64;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import io.github.vanish073.driftline.sim.world.FakeBank;
import io.github.vanish073.driftline.sim.world.FakeGateway;
import io.github.vanish073.driftline.sim.world.FakeMerchant;
import io.github.vanish073.driftline.sim.world.PlannedOrder;
import io.github.vanish073.driftline.sim.world.Request;
import io.github.vanish073.driftline.sim.world.Scenario;
import io.github.vanish073.driftline.sim.world.SettlementFeed;
import io.github.vanish073.driftline.sim.world.Workload;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * One deterministic run (DESIGN §11). Everything is driven by the seed through separate streams (§11.2): the
 * workload decides what happens, the network the base delays, the swarm which faults are on, and the faults stream
 * when they strike. The real ingest normaliser, matcher, state machines and ledger rules run against fakes of the
 * gateway, merchants, bank, network, clocks, log and store, with two matcher instances under a partition coordinator.
 * Settlement files travel from the bank's feed to a separate settlement worker, which runs the real CSV parser.
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
    /** The merchants' contracted pricing in the simulator: the same model the fake gateway settles with. */
    static final FeeSchedule FEES = new FeeSchedule(200, 1_800);
    static final int CYCLE_DAYS = 1;
    /** An exception escaped a step: a bug in Driftline or in the simulator, reported like any other violation. */
    public static final String STEP_THREW = "SIM-STEP-THREW";
    public static final String LIVENESS = "SIM-LIVENESS";

    private Simulation() {
    }

    public static RunResult run(long seed, Profile profile, boolean keepTrace) {
        return run(seed, profile, Canaries.NONE, keepTrace);
    }

    public static RunResult run(long seed, Profile profile, Canaries canaries, boolean keepTrace) {
        return run(seed, profile, canaries, keepTrace, Optional.empty());
    }

    /** The resolver and dispatcher nodes of a run (Shift 8). */
    record ResolutionNodes(ResolverNode resolver, DispatcherNode dispatcher) {
    }

    /** The dispatcher's constants in the simulator: the gateway's key TTL is compressed to 5 minutes (D62). */
    static final Dispatcher.Config DISPATCHER = new Dispatcher.Config(Duration.ofSeconds(30), Duration.ofSeconds(5),
            Duration.ofMinutes(10), 20);
    /**
     * D62's assumption, made explicit: a request reaches the gateway within this long of being sent, well inside the
     * gateway's key TTL ({@code FakeGateway.KEY_TTL_MILLIS}, 5 min), so an attempt delayed in the network still meets
     * its key. The network caps gateway-bound delays at it. (Production: a 30 s call timeout against a 24 h TTL.)
     */
    static final long GATEWAY_DELIVERY_BOUND_MILLIS = 60_000L;
    /** How many times a forced crash point (dedicated seeds) strikes in one run. */
    static final int FORCED_CRASHES = 3;

    /**
     * @param forcedCrash a crash point to force on purpose (Shift 8 Prove): the node that reaches it crashes the first
     *                    {@link #FORCED_CRASHES} times
     */
    public static RunResult run(long seed, Profile profile, Canaries canaries, boolean keepTrace,
                                Optional<CrashPoint> forcedCrash) {
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
        LocalDate firstSettlementDate = IstCalendar.istDate(EPOCH).plusDays(1);
        List<MerchantTerms> terms = new ArrayList<>();
        for (MerchantId m : merchantIds) {
            terms.add(new MerchantTerms(m, contexts.get(m).partition(), FEES, CYCLE_DAYS, firstSettlementDate));
        }
        MerchantDirectory directory = new MerchantDirectory(terms);
        MatcherConfig config = MatcherConfig.DEFAULT;

        Coordinator coordinator = new Coordinator(scheduler, trace, partitions);
        TreeMap<String, MatcherInstance> matchers = new TreeMap<>();
        for (String name : MATCHERS) {
            matchers.put(name, new MatcherInstance(name, store, log, scheduler, trace, coordinator, faults, directory,
                    config, canaries, invariants::anomaly));
        }
        IngestNode ingest = new IngestNode(scheduler, new SimClock(scheduler), contexts, log, coordinator, faults, trace);
        for (MerchantId m : merchantIds) {
            merchants.put(m, new FakeMerchant(m, scheduler, network, ingest, trace, faults));
        }
        SettlementWorkerNode worker = new SettlementWorkerNode(scheduler, new SimClock(scheduler), contexts, store, log,
                coordinator, faults, trace);
        SettlementFeed feed = new SettlementFeed(scheduler, network, faults, trace, worker);
        FakeGateway gateway = new FakeGateway(scheduler, network, bank, ingest, trace, secrets, merchants,
                SplitMix64.stream(seed, "bank"), feed, FakeGateway.BATCH_DAY_PERCENT, faults);
        if (forcedCrash.filter(cp -> cp.ordinal() > CrashPoint.CP5.ordinal()).isPresent()) {
            throw new IllegalArgumentException(forcedCrash.get() + " is armed by NODE_CRASH faults; only CP1-CP5 are forced");
        }
        ForcedCrash forced = forcedCrash.isPresent() ? new ForcedCrash(forcedCrash, FORCED_CRASHES) : ForcedCrash.none();
        ResolverNode resolverNode = new ResolverNode(scheduler, new SimClock(scheduler), store, trace, faults,
                Resolver.Config.DEFAULT, canaries, forced);
        long[] opsNotices = {0};
        DispatcherNode.Router router = (call, reply) -> {
            Consumer<Map<String, String>> back = facts -> network.send(DispatcherNode.NAME, () -> reply.accept(facts));
            switch (call.target()) {
                case GATEWAY -> network.send("gateway", GATEWAY_DELIVERY_BOUND_MILLIS,
                        () -> gateway.handle(call, back::accept));
                case MERCHANT -> {
                    FakeMerchant merchant = merchants.get(call.merchantId());
                    network.send("merchant:" + call.merchantId(), () -> back.accept(
                            merchant.confirm((CallRequest.MerchantConfirm) call.request(), call.idempotencyKey())));
                }
                case DRIFTLINE -> network.send(IngestNode.NAME, () -> ingest.receive(new Request.Observation(
                        call.merchantId(), (CallRequest.Observation) call.request()), status -> {
                            if (status == 202) {
                                back.accept(status("ACCEPTED"));
                            }
                        }));
                case OPS -> network.send("ops", () -> {
                    opsNotices[0]++;
                    back.accept(status("DELIVERED"));
                });
            }
        };
        DispatcherNode dispatcherNode = new DispatcherNode(scheduler, new SimClock(scheduler), store, trace, faults,
                DISPATCHER, canaries, router, forced);
        resolverNode.dispatcherWake(dispatcherNode::wake);
        dispatcherNode.resolverWake(resolverNode::wake);
        matchers.values().forEach(m -> m.afterCommit(() -> {
            resolverNode.wake();
            dispatcherNode.wake();
        }));
        worker.afterConflict(resolverNode::wake);   // BUG-011: a FILE_CONFLICT the worker raised must be reviewed
        ResolutionNodes resolution = new ResolutionNodes(resolverNode, dispatcherNode);
        LocalDate lastFileDate = IstCalendar.istDate(EPOCH).plusDays(profile.trafficDays + profile.drainDays);
        invariants.world(gateway, feed, merchants, EPOCH, CYCLE_DAYS, config.graceDays(), lastFileDate);

        long runMillis = profile.trafficDays * DAY_MILLIS;
        long endMillis = (profile.trafficDays + profile.drainDays) * DAY_MILLIS;
        TreeMap<String, SimClock> clocks = new TreeMap<>();
        clocks.put(IngestNode.NAME, ingest.clock());
        clocks.put(SettlementWorkerNode.NAME, worker.clock());
        matchers.forEach((name, m) -> clocks.put(name, m.clock()));
        clocks.put(ResolverNode.NAME, resolverNode.clock());
        clocks.put(DispatcherNode.NAME, dispatcherNode.clock());
        scheduleFaults(faults, scheduler, trace, store, ingest, worker, matchers, resolution, clocks, runMillis,
                profile.trafficDays);
        scheduler.at(runMillis, "kernel", () -> heal(faults, scheduler, trace, store, ingest, worker, matchers,
                resolution, clocks));
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
                violation = invariants.atEnd(scheduler.now(), log, gateway.undelivered() + feed.undelivered()
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
        stats.put("settlement.files", feed.files());
        stats.put("settlement.worker.accepted", worker.accepted());
        stats.put("settlement.worker.duplicates", worker.duplicates());
        stats.put("settlement.worker.conflicts", (long) worker.conflicts().size());
        stats.put("settlement.matched", (long) store.committedMatches().size());
        for (var line : store.committedLines()) {
            stats.merge("settlement.line." + line.status().name(), 1L, Long::sum);
        }
        stats.put("settlement.batch_lines.world", (long) gateway.batchLines().size());
        for (Break b : store.committedBreaks()) {
            stats.merge("breaks." + b.type().name(), 1L, Long::sum);
            if (b.evidence().containsKey("order_paid_later") || b.evidence().containsKey("attributed_to")
                    || b.evidence().containsKey("kept_by_order")) {
                stats.merge("breaks.late_evidence", 1L, Long::sum);
            }
        }
        stats.put("oracle.expected", (long) invariants.expected().size());
        stats.put("oracle.covered.escalated_line", invariants.coveredByEscalatedLine());
        stats.put("resolver.decisions", resolverNode.decisions());
        stats.put("dispatcher.sent", dispatcherNode.sent());
        stats.put("dispatcher.results", dispatcherNode.results());
        stats.put("ops.notices", opsNotices[0]);
        stats.put("gateway.webhooks.lost", gateway.lostWebhooks());
        stats.put("forced.crashes", (long) forced.taken());
        for (var c : store.committedCommands()) {
            stats.merge("outbox." + c.kind().name(), 1L, Long::sum);
        }
        long refunds = 0;
        for (FakeGateway.Payment p : gateway.payments().values()) {
            refunds += p.refunds().size();
        }
        stats.put("gateway.refunds", refunds);
        for (Break b : store.committedBreaks()) {
            stats.merge("resolution." + b.type().name() + "." + b.state().name(), 1L, Long::sum);
        }
        stats.put("oracle.detected", (long) invariants.detected().size());
        stats.put("oracle.covered.idempotency_conflict", invariants.coveredByIdempotencyConflict());
        stats.put("oracle.covered.unresolved_batch", invariants.coveredByUnresolvedBatch());
        long cacheHits = 0;
        long cacheMisses = 0;
        for (MatcherInstance m : matchers.values()) {
            cacheHits += m.cacheHits();
            cacheMisses += m.cacheMisses();
        }
        stats.put("keyed_state.hits", cacheHits);
        stats.put("keyed_state.misses", cacheMisses);
        stats.put("coordinator.reassignments", coordinator.reassignments());
        long records = 0;
        for (int p = 0; p < log.partitionCount(); p++) {
            records += log.endOffset(p);
        }
        stats.put("log.records", records);
        faults.counters().forEach((k, v) -> stats.put("fault." + k, v));
        return new RunResult(seed, profile, canaries, forcedCrash, swarm.toString(), scheduler.steps(), scheduler.now(),
                trace.digestHex(), violation, stats, trace.tail(), trace.all());
    }

    /** A one-fact answer, {@code status → value} (no Map.of in the simulator: its order is salted per JVM). */
    private static TreeMap<String, String> status(String value) {
        TreeMap<String, String> out = new TreeMap<>();
        out.put("status", value);
        return out;
    }

    private static TreeSet<String> reusedKeys(TreeMap<MerchantId, FakeMerchant> merchants) {
        TreeSet<String> keys = new TreeSet<>();
        merchants.forEach((m, merchant) -> merchant.reusedKeys().forEach(k -> keys.add(m + "/" + k)));
        return keys;
    }

    /** Schedules the planned event faults and the per-node clock skews of the RUN phase. */
    private static void scheduleFaults(FaultInjector faults, Scheduler scheduler, Trace trace, InMemoryStore store,
                                       IngestNode ingest, SettlementWorkerNode worker,
                                       TreeMap<String, MatcherInstance> matchers, ResolutionNodes resolution,
                                       TreeMap<String, SimClock> clocks, long runMillis, int trafficDays) {
        TreeMap<String, Long> skews = faults.skews(new ArrayList<>(clocks.keySet()));
        skews.forEach((node, offset) -> clocks.get(node).setOffsetMillis(offset));
        if (faults.swarm().isNone()) {
            return;
        }
        List<String> crashable = new ArrayList<>(MATCHERS);
        crashable.add(IngestNode.NAME);
        crashable.add(SettlementWorkerNode.NAME);
        crashable.add(ResolverNode.NAME);
        crashable.add(DispatcherNode.NAME);
        List<FaultEvent> plan = faults.plan(runMillis, trafficDays, crashable, MATCHERS, new ArrayList<>(clocks.keySet()));
        int[] storeDown = {0};
        for (FaultEvent event : plan) {
            scheduler.at(event.atMillis(), "faults", () -> {
                if (faults.healed()) {
                    return;
                }
                switch (event) {
                    case FaultEvent.Crash c when c.node().equals(IngestNode.NAME) -> ingest.crash(c.downtimeMillis());
                    case FaultEvent.Crash c when c.node().equals(SettlementWorkerNode.NAME) -> worker.crash(c.downtimeMillis());
                    case FaultEvent.Crash c when c.node().equals(ResolverNode.NAME) -> {
                        if (c.midBatch()) {
                            resolution.resolver().armCrash(c.downtimeMillis());
                        } else {
                            resolution.resolver().crash(c.downtimeMillis());
                        }
                    }
                    case FaultEvent.Crash c when c.node().equals(DispatcherNode.NAME) -> {
                        if (c.midBatch()) {
                            resolution.dispatcher().armCrash(c.downtimeMillis());
                        } else {
                            resolution.dispatcher().crash(c.downtimeMillis());
                        }
                    }
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
                             IngestNode ingest, SettlementWorkerNode worker, TreeMap<String, MatcherInstance> matchers,
                             ResolutionNodes resolution, TreeMap<String, SimClock> clocks) {
        faults.heal();
        store.setUnavailable(false);
        clocks.values().forEach(c -> c.setOffsetMillis(0));
        ingest.heal();
        worker.heal();
        matchers.values().forEach(MatcherInstance::heal);
        resolution.resolver().heal();
        resolution.dispatcher().heal();
        trace.line(scheduler.now(), "kernel", "HEAL");
    }

    /**
     * Plays one order's scenario from its start time; every delay comes from the workload stream. The sandbox calls it
     * too, so the Compose world plays the very same scenarios as the simulator (DESIGN §21.10, D51).
     */
    public static void play(PlannedOrder o, Scheduler scheduler, SplitMix64 rng, FakeGateway gateway, FakeMerchant merchant) {
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
                case SHORT_SETTLEMENT, MISSING_SETTLEMENT, ORPHAN_SETTLEMENT -> {
                    gateway.plan(p, switch (scenario) {
                        case SHORT_SETTLEMENT -> FakeGateway.SettlementPlan.SHORT;
                        case MISSING_SETTLEMENT -> FakeGateway.SettlementPlan.WITHHELD;
                        default -> FakeGateway.SettlementPlan.WITH_ORPHAN;
                    });
                    scheduler.after(decidedAfter, "gateway", () -> gateway.succeed(p, true));
                }
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
