package io.github.vanish073.driftline.sim.runner;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.sim.Profile;
import io.github.vanish073.driftline.sim.RunResult;
import io.github.vanish073.driftline.sim.Simulation;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * {@code ./gradlew :sim:campaign --args="--from 1 --count 10000 --threads 8 --profile pr"} (DESIGN §11.12).
 *
 * <p>Replays the regression corpus first ({@code --regression}, default {@code regression-seeds.txt}), then runs
 * {@code count} seeds from {@code from} on {@code threads} threads. {@code --shard i/n} runs the i-th of n equal
 * slices (CI's matrix). {@code --double-run k} runs the first k seeds a second time and compares digests in this JVM;
 * {@code --digests file} writes "seed digest" lines so two JVMs can be compared with {@code diff}; {@code --summary file}
 * writes the run's counts as JSON (profile, seed range, seeds run and failed, where it ran), which the cumulative seed
 * count adds up (D85). Every failing seed
 * prints its violation and REPRO line and gets its full trace in {@code build/sim-failures/seed-<n>.trace}. The exit
 * status is 1 if anything failed. This driver sits outside the determinism rules (D22).
 */
public final class CampaignMain {

    static final Path FAILURES = Path.of("build", "sim-failures");

    private CampaignMain() {
    }

    record Job(long seed, Profile profile, Canaries canaries, String origin) {
    }

    public static void main(String[] args) throws Exception {
        long from = 1;
        long count = 1_000;
        int threads = Runtime.getRuntime().availableProcessors();
        Profile profile = Profile.PR;
        Path regression = Path.of("regression-seeds.txt");
        int doubleRun = 0;
        Path digests = null;
        Path summary = null;
        int shard = 0;
        int shards = 1;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--from" -> from = Long.parseLong(args[++i]);
                case "--count" -> count = Long.parseLong(args[++i]);
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--profile" -> profile = Profile.fromId(args[++i]);
                case "--regression" -> regression = args[++i].equals("none") ? null : Path.of(args[i]);
                case "--double-run" -> doubleRun = Integer.parseInt(args[++i]);
                case "--digests" -> digests = Path.of(args[++i]);
                case "--summary" -> summary = Path.of(args[++i]);
                case "--shard" -> {
                    String[] parts = args[++i].split("/");
                    shard = Integer.parseInt(parts[0]);
                    shards = Integer.parseInt(parts[1]);
                }
                default -> {
                    System.err.println("unknown argument " + args[i]);
                    System.exit(2);
                }
            }
        }
        if (shard < 0 || shards < 1 || shard >= shards) {
            throw new IllegalArgumentException("--shard i/n needs 0 <= i < n");
        }
        long perShard = count / shards;
        long first = from + shard * perShard;
        long n = shard == shards - 1 ? count - shard * perShard : perShard;

        List<Job> jobs = new ArrayList<>();
        if (regression != null) {
            jobs.addAll(readCorpus(regression));
        }
        int corpus = jobs.size();
        for (long s = first; s < first + n; s++) {
            jobs.add(new Job(s, profile, Canaries.NONE, "campaign"));
        }
        for (long s = first; s < first + Math.min(doubleRun, n); s++) {
            jobs.add(new Job(s, profile, Canaries.NONE, "double-run"));
        }

        System.out.printf(Locale.ROOT, "campaign: profile=%s seeds=%d..%d (%d) shard=%d/%d threads=%d corpus=%d double-run=%d%n",
                profile.id(), first, first + n - 1, n, shard, shards, threads, corpus, Math.min(doubleRun, n));
        long started = System.nanoTime();
        List<RunResult> results = runAll(jobs, threads);
        double seconds = (System.nanoTime() - started) / 1e9;

        List<String> failures = new ArrayList<>();
        TreeMap<Long, String> digestBySeed = new TreeMap<>();
        TreeMap<String, Long> totals = new TreeMap<>();
        long campaignSeeds = 0;
        long failedSeeds = 0;
        long failedCorpus = 0;
        for (int i = 0; i < jobs.size(); i++) {
            Job job = jobs.get(i);
            RunResult r = results.get(i);
            if (job.origin().equals("double-run")) {
                String firstDigest = digestBySeed.get(job.seed());
                if (!r.digest().equals(firstDigest)) {
                    failures.add("INV-DETERMINISTIC seed " + job.seed() + ": " + firstDigest + " then " + r.digest());
                }
                continue;
            }
            if (job.origin().equals("campaign")) {
                campaignSeeds++;
                digestBySeed.put(job.seed(), r.digest());
                r.stats().forEach((k, v) -> totals.merge(k, v, Long::sum));
            }
            if (!r.passed()) {
                failures.add(report(job, r));
                if (job.origin().equals("campaign")) {
                    failedSeeds++;
                } else {
                    failedCorpus++;
                }
            }
        }
        if (digests != null) {
            List<String> lines = new ArrayList<>();
            digestBySeed.forEach((seed, d) -> lines.add(seed + " " + d));
            if (digests.getParent() != null) {
                Files.createDirectories(digests.getParent());
            }
            Files.write(digests, lines, StandardCharsets.UTF_8);
        }
        if (summary != null) {
            writeSummary(summary, profile, first, n, campaignSeeds, failedSeeds, corpus, failedCorpus,
                    Math.min(doubleRun, n), failures.size(), seconds, threads, shard, shards);
        }
        System.out.println("totals over " + campaignSeeds + " campaign seeds:");
        totals.forEach((k, v) -> System.out.println("  " + k + " = " + v));
        System.out.printf(Locale.ROOT, "wall=%.1fs  per-seed=%.2fms (on %d threads)  runs=%d%n",
                seconds, seconds * 1000 / Math.max(1, jobs.size()) * threads, threads, jobs.size());
        if (failures.isEmpty()) {
            System.out.println("PASS: " + corpus + " corpus seeds, " + campaignSeeds + " campaign seeds, "
                    + Math.min(doubleRun, n) + " double-runs");
            return;
        }
        System.out.println("FAILED: " + failures.size());
        failures.forEach(System.out::println);
        System.exit(1);
    }

    /** The run's counts as one JSON object; {@code where} is the GitHub run, or "local". */
    static void writeSummary(Path file, Profile profile, long first, long n, long seeds, long failedSeeds, int corpus,
                             long failedCorpus, long doubleRuns, int failures, double seconds, int threads, int shard,
                             int shards) throws IOException {
        String runId = System.getenv().getOrDefault("GITHUB_RUN_ID", "");
        String where = runId.isEmpty() ? "local" : "github-run-" + runId;
        String commit = System.getenv().getOrDefault("GITHUB_SHA", "");
        String json = String.format(Locale.ROOT, """
                {"profile":"%s","first_seed":%d,"last_seed":%d,"seeds":%d,"failed_seeds":%d,"corpus_seeds":%d,\
                "failed_corpus_seeds":%d,"double_runs":%d,"failures":%d,"wall_seconds":%.1f,"threads":%d,\
                "shard":"%d/%d","where":"%s","commit":"%s","finished_at":"%s"}
                """, profile.id(), first, first + n - 1, seeds, failedSeeds, corpus, failedCorpus, doubleRuns,
                failures, seconds, threads, shard, shards, where, commit, java.time.Instant.now().toString());
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }
        Files.writeString(file, json, StandardCharsets.UTF_8);
    }

    static List<Job> readCorpus(Path file) throws IOException {
        List<Job> jobs = new ArrayList<>();
        if (!Files.exists(file)) {
            // A corpus that silently goes missing would let every run pass without replaying a single bug.
            throw new IOException("regression corpus " + file.toAbsolutePath() + " not found; pass --regression none to skip it");
        }
        for (String raw : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            String line = raw.contains("#") ? raw.substring(0, raw.indexOf('#')) : raw;
            line = line.strip();
            if (line.isEmpty()) {
                continue;
            }
            String[] parts = line.split("\\s+");
            List<Canary> canaries = new ArrayList<>();
            for (int i = 2; i < parts.length; i++) {
                canaries.add(Canary.fromCode(parts[i]));
            }
            jobs.add(new Job(Long.parseLong(parts[0]), Profile.fromId(parts[1]), new Canaries(canaries), "corpus"));
        }
        return jobs;
    }

    static List<RunResult> runAll(List<Job> jobs, int threads) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<RunResult>> futures = new ArrayList<>();
            for (Job job : jobs) {
                futures.add(pool.submit(() -> Simulation.run(job.seed(), job.profile(), job.canaries(), false)));
            }
            List<RunResult> results = new ArrayList<>();
            for (Future<RunResult> f : futures) {
                results.add(f.get());
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    /** Prints a failure the way DESIGN §11.12 asks and writes its full trace. */
    static String report(Job job, RunResult r) throws IOException {
        var v = r.violation().orElseThrow();
        RunResult full = Simulation.run(job.seed(), job.profile(), job.canaries(), true);
        Files.createDirectories(FAILURES);
        Path file = FAILURES.resolve("seed-" + job.seed() + ".trace");
        Files.write(file, full.trace(), StandardCharsets.UTF_8);
        System.out.println("---- " + job.origin() + " seed " + job.seed() + ": " + v.invariant() + " at " + v.atMillis()
                + " ms: " + v.detail());
        System.out.println("swarm: " + r.swarm());
        int from = Math.max(0, r.traceTail().size() - 40);
        r.traceTail().subList(from, r.traceTail().size()).forEach(l -> System.out.println("  " + l));
        System.out.println("full trace: " + file.toAbsolutePath());
        System.out.println("REPRO: " + r.repro() + "  (at commit " + System.getenv().getOrDefault("GITHUB_SHA", "HEAD") + ")");
        return job.origin() + " seed " + job.seed() + " " + v.invariant() + "  REPRO: " + r.repro();
    }
}
