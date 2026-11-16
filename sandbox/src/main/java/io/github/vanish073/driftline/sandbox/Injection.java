package io.github.vanish073.driftline.sandbox;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sim.Simulation;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
import io.github.vanish073.driftline.sim.fault.Swarm;
import io.github.vanish073.driftline.sim.invariant.Oracle;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimNetwork;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.sim.random.SplitMix64;
import io.github.vanish073.driftline.sim.world.FakeBank;
import io.github.vanish073.driftline.sim.world.FakeGateway;
import io.github.vanish073.driftline.sim.world.FakeMerchant;
import io.github.vanish073.driftline.sim.world.PlannedOrder;
import io.github.vanish073.driftline.sim.world.Scenario;
import io.github.vanish073.driftline.sim.world.SettlementFeed;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One scenario injection (D51): the simulator's own world models (fake gateway, merchant and bank, and the scenario
 * code of {@link Simulation#play}) play {@code count} orders of one scenario for one pooled merchant, with every fault
 * off, against a real Driftline over HTTP. The world runs on virtual time from the day before the merchant's first
 * settlement date, so everything it does lies in the past: one traffic day, then the settlement files for the next
 * days, all delivered at once. When it is done, the simulator's end-state {@link Oracle} names the one-to-one breaks
 * the world implies, which the e2e suite then expects Driftline to show.
 */
final class Injection {

    static final int CYCLE_DAYS = 1;
    static final int GRACE_DAYS = 1;
    /** Files for the first settlement date and the two after it: enough for MISSING_SETTLEMENT's due date plus grace. */
    static final int FILE_DAYS = 3;
    static final int CUSTOMERS = 10;
    private static final long CUSTOMER_FUNDS_PAISE = 1_000_000_000L;

    /** What was injected and what Driftline must now show. */
    record Report(MerchantId merchantId, Scenario scenario, int count, LocalDate lastFileDate, TreeSet<String> expected,
                  long webhooks, long orderEvents, long files, long notAccepted) {
    }

    private Injection() {
    }

    /**
     * @param today the real IST date: files are produced only for dates before it, so the world never runs ahead of the
     *              real clock
     */
    static Report run(BootstrapFile.Merchant merchant, Scenario scenario, int count, HttpDriftline driftline,
                      LocalDate today) {
        Objects.requireNonNull(scenario, "scenario");
        if (count < 1 || count > 50) {
            throw new IllegalArgumentException("count must be 1 to 50");
        }
        LocalDate first = merchant.firstSettlementDate();
        LocalDate lastFileDate = first.plusDays(FILE_DAYS - 1L);
        if (!lastFileDate.isBefore(today)) {
            throw new IllegalArgumentException("the pool's first settlement date must be at least " + FILE_DAYS
                    + " days before today, so every file lies in the past");
        }
        long seed = seed(merchant.id(), scenario);
        Instant epoch = IstCalendar.startOfIstDay(first.minusDays(1));
        Scheduler scheduler = new Scheduler(epoch);
        Trace trace = new Trace(false);
        FaultInjector faults = new FaultInjector(Swarm.NONE, SplitMix64.stream(seed, "faults"));
        SimNetwork network = new SimNetwork(scheduler, SplitMix64.stream(seed, "network"), faults);
        SplitMix64 workload = SplitMix64.stream(seed, "workload");
        MerchantId m = merchant.id();

        FakeBank bank = new FakeBank();
        bank.open(FakeGateway.merchantAccount(m), 0);
        for (int c = 0; c < CUSTOMERS; c++) {
            bank.open("customer:" + c, CUSTOMER_FUNDS_PAISE);
        }
        TreeMap<MerchantId, FakeMerchant> merchants = new TreeMap<>();
        merchants.put(m, new FakeMerchant(m, scheduler, network, driftline, trace, faults));
        TreeMap<MerchantId, byte[]> secrets = new TreeMap<>();
        secrets.put(m, merchant.secret().getBytes(StandardCharsets.US_ASCII));
        SettlementFeed feed = new SettlementFeed(scheduler, network, faults, trace, driftline);
        FakeGateway gateway = new FakeGateway(scheduler, network, bank, driftline, trace, secrets, merchants,
                SplitMix64.stream(seed, "bank"), feed, 0);

        long tenAm = 10 * 3_600_000L;
        for (int i = 0; i < count; i++) {
            PlannedOrder order = new PlannedOrder(m, OrderId.of("ord_" + i), "customer:" + (i % CUSTOMERS),
                    Money.ofPaise(workload.between(10_000L, 500_000L)), tenAm + i * 120_000L, scenario);
            scheduler.at(order.createdAtMillis(), "workload",
                    () -> Simulation.play(order, scheduler, workload, gateway, merchants.get(m)));
        }
        for (LocalDate date = first; !date.isAfter(lastFileDate); date = date.plusDays(1)) {
            LocalDate settlementDate = date;
            long at = IstCalendar.startOfIstDay(date).toEpochMilli() - epoch.toEpochMilli() + tenAm;
            scheduler.at(at, "gateway", () -> gateway.settle(settlementDate));
        }
        long end = IstCalendar.startOfIstDay(lastFileDate.plusDays(1)).toEpochMilli() - epoch.toEpochMilli();
        while (scheduler.step(end)) {
            // every step is a real HTTP request or a world-internal action
        }
        TreeSet<String> expected = Oracle.expected(gateway, merchants, epoch, CYCLE_DAYS, GRACE_DAYS, lastFileDate);
        return new Report(m, scenario, count, lastFileDate, expected, driftline.webhooks.get(),
                driftline.orderEvents.get(), driftline.files.get(), driftline.notAccepted.get());
    }

    /** The same merchant and scenario always play the same world. */
    private static long seed(MerchantId merchant, Scenario scenario) {
        return Long.parseLong(Sha256.of((merchant.value() + "/" + scenario.name()).getBytes(StandardCharsets.UTF_8))
                .hex().substring(0, 15), 16);
    }

    static List<Scenario> oneToOne() {
        return List.of(Scenario.DEBITED_NO_ORDER, Scenario.LATE_SUCCESS, Scenario.DUPLICATE_CAPTURE,
                Scenario.SHORT_SETTLEMENT, Scenario.MISSING_SETTLEMENT, Scenario.ORPHAN_SETTLEMENT);
    }
}
