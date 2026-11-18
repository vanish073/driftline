package io.github.vanish073.driftline.bench;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.ledger.PostingRules;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.store.postgres.BalanceStrategy;
import io.github.vanish073.driftline.store.postgres.PostgresStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.flywaydb.core.Flyway;

/**
 * The hot-account benchmark (DESIGN §7.5, §17). One merchant's two hot accounts (gateway receivable and payments
 * clearing) take every capture, posted by N concurrent writers, each capture a distinct payment, as when the single
 * writer per merchant is relaxed. For each strategy and writer count it measures, over a fixed window after a warm-up:
 * committed transactions per second, the p50 and p99 commit latency of a ledger post, and the p50 and p99 latency of a
 * balance read taken every 50 ms by one reader alongside. Under APPEND_SNAPSHOT the snapshotter folds finished entries
 * every second, as the design intends. Each run uses a fresh merchant, and ends by checking that the balance equals the
 * sum of what was posted.
 *
 * <pre>
 *   --jdbc URL --admin-user U --admin-password P   superuser, to migrate the scratch database (Flyway)
 *   --user U --password P                           the DML login the benchmark posts as (production's role)
 *   --concurrency 1,4,16,64  --warmup 5  --seconds 20  --out results.csv
 * </pre>
 */
public final class HotAccountBenchmark {

    private static final Instant AT = Instant.parse("2026-11-17T06:00:00Z");
    private static final long AMOUNT_PAISE = 49_900;

    /** One measured run. */
    record Result(BalanceStrategy strategy, int writers, int seconds, long commits, long errors, long[] commitNanos,
                  long[] readNanos) {

        double tps() {
            return commits / (double) seconds;
        }

        String csv() {
            return String.format(Locale.ROOT, "%s,%d,%d,%.1f,%.2f,%.2f,%d,%.2f,%.2f,%d", strategy, writers, commits,
                    tps(), millis(commitNanos, 0.50), millis(commitNanos, 0.99), readNanos.length,
                    millis(readNanos, 0.50), millis(readNanos, 0.99), errors);
        }
    }

    static final String HEADER = "strategy,writers,commits,tps,commit_p50_ms,commit_p99_ms,reads,read_p50_ms,"
            + "read_p99_ms,errors";

    /** The q-quantile (nearest rank) of nanosecond samples, in milliseconds; NaN for none. */
    static double millis(long[] sortedNanos, double q) {
        if (sortedNanos.length == 0) {
            return Double.NaN;
        }
        int rank = (int) Math.ceil(q * sortedNanos.length) - 1;
        return sortedNanos[Math.max(0, Math.min(sortedNanos.length - 1, rank))] / 1e6;
    }

    public static void main(String[] args) throws Exception {
        String jdbc = null;
        String adminUser = "postgres";
        String adminPassword = null;
        String user = "driftline";
        String password = null;
        int[] concurrency = {1, 4, 16, 64};
        int warmup = 5;
        int seconds = 20;
        Path out = Path.of("hot-accounts.csv");
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--jdbc" -> jdbc = args[++i];
                case "--admin-user" -> adminUser = args[++i];
                case "--admin-password" -> adminPassword = args[++i];
                case "--user" -> user = args[++i];
                case "--password" -> password = args[++i];
                case "--concurrency" -> concurrency = Arrays.stream(args[++i].split(",")).mapToInt(Integer::parseInt)
                        .toArray();
                case "--warmup" -> warmup = Integer.parseInt(args[++i]);
                case "--seconds" -> seconds = Integer.parseInt(args[++i]);
                case "--out" -> out = Path.of(args[++i]);
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        if (jdbc == null || adminPassword == null || password == null) {
            throw new IllegalArgumentException("--jdbc, --admin-password and --password are required");
        }
        Flyway.configure().dataSource(jdbc, adminUser, adminPassword).locations("classpath:db/migration").load()
                .migrate();
        List<String> lines = new ArrayList<>(List.of(HEADER));
        for (BalanceStrategy strategy : List.of(BalanceStrategy.ROW_LOCK, BalanceStrategy.APPEND_SNAPSHOT)) {
            for (int writers : concurrency) {
                Result r = run(jdbc, user, password, strategy, writers, warmup, seconds);
                System.out.println("hot-accounts: " + r.csv());
                lines.add(r.csv());
            }
        }
        Files.write(out, lines, StandardCharsets.UTF_8);
        System.out.println("hot-accounts: wrote " + out);
    }

    static Result run(String jdbc, String user, String password, BalanceStrategy strategy, int writers, int warmup,
                      int seconds) throws InterruptedException {
        MerchantId m = MerchantId.of("m_hot_" + strategy.name().toLowerCase(Locale.ROOT) + "_" + writers);
        try (PostgresStore store = PostgresStore.connect(jdbc, user, password, strategy, writers + 2)) {
            long start = System.nanoTime();
            long measureFrom = start + warmup * 1_000_000_000L;
            long end = measureFrom + seconds * 1_000_000_000L;
            AtomicLong posted = new AtomicLong();
            AtomicLong errors = new AtomicLong();
            List<long[]> perWriter = new ArrayList<>();
            List<Thread> threads = new ArrayList<>();
            CountDownLatch go = new CountDownLatch(1);
            long[] committed = new long[writers];
            for (int w = 0; w < writers; w++) {
                perWriter.add(new long[0]);
            }
            for (int w = 0; w < writers; w++) {
                int writer = w;
                threads.add(Thread.ofPlatform().name("writer-" + w).start(() -> {
                    awaitQuietly(go);
                    long[] samples = new long[1_024];
                    int recorded = 0;
                    for (long i = 0; System.nanoTime() < end; i++) {
                        String payment = "pay_" + writer + "_" + i;
                        long t0 = System.nanoTime();
                        try {
                            PostResult result = store.transact("bench", tx -> tx.ledger().post(PostingRules.capture(m,
                                    PaymentId.of(payment), Money.ofPaise(AMOUNT_PAISE), AT)));
                            long t1 = System.nanoTime();
                            if (!(result instanceof PostResult.Posted)) {
                                errors.incrementAndGet();
                                continue;
                            }
                            posted.incrementAndGet();
                            if (t0 >= measureFrom && t1 <= end) {
                                if (recorded == samples.length) {
                                    samples = Arrays.copyOf(samples, samples.length * 2);
                                }
                                samples[recorded++] = t1 - t0;
                            }
                        } catch (RuntimeException e) {
                            errors.incrementAndGet();
                        }
                    }
                    committed[writer] = recorded;
                    synchronized (perWriter) {
                        perWriter.set(writer, Arrays.copyOf(samples, recorded));
                    }
                }));
            }
            long[] reads = new long[200_000];
            int[] readCount = {0};
            Thread reader = Thread.ofPlatform().name("reader").start(() -> {
                awaitQuietly(go);
                while (System.nanoTime() < end) {
                    long t0 = System.nanoTime();
                    try {
                        store.transact("bench-read", tx -> tx.ledger().balance(m, AccountCode.GATEWAY_RECEIVABLE));
                        long t1 = System.nanoTime();
                        if (t0 >= measureFrom && readCount[0] < reads.length) {
                            reads[readCount[0]++] = t1 - t0;
                        }
                    } catch (RuntimeException e) {
                        errors.incrementAndGet();
                    }
                    sleepQuietly(50);
                }
            });
            Thread snapshotter = Thread.ofPlatform().name("snapshotter").start(() -> {
                awaitQuietly(go);
                while (strategy == BalanceStrategy.APPEND_SNAPSHOT && System.nanoTime() < end) {
                    try {
                        store.snapshotBalances();
                    } catch (RuntimeException e) {
                        errors.incrementAndGet();
                    }
                    sleepQuietly(1_000);
                }
            });
            go.countDown();
            for (Thread t : threads) {
                t.join();
            }
            reader.join();
            snapshotter.join();
            long expected = Math.multiplyExact(posted.get(), AMOUNT_PAISE);
            Money balance = store.transact("bench-check", tx -> tx.ledger().balance(m, AccountCode.GATEWAY_RECEIVABLE));
            if (balance.paise() != expected) {
                throw new IllegalStateException(strategy + " x" + writers + ": balance " + balance.paise()
                        + " paise, but " + expected + " were posted");
            }
            long commits = Arrays.stream(committed).sum();
            long[] commitNanos;
            synchronized (perWriter) {
                commitNanos = perWriter.stream().flatMapToLong(Arrays::stream).sorted().toArray();
            }
            long[] readNanos = Arrays.copyOf(reads, readCount[0]);
            Arrays.sort(readNanos);
            return new Result(strategy, writers, seconds, commits, errors.get(), commitNanos, readNanos);
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private HotAccountBenchmark() {
    }
}
