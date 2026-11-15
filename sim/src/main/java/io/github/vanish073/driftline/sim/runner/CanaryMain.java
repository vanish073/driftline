package io.github.vanish073.driftline.sim.runner;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.sim.CanaryCatalog;
import io.github.vanish073.driftline.sim.Profile;
import io.github.vanish073.driftline.sim.RunResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * {@code ./gradlew :sim:canaries} (DESIGN §11.10, §11.12): for each active canary, runs {@code pr} seeds from
 * {@code --from} (default 1) with that one bug planted until a seed fails, and prints <em>seeds-to-detection</em>
 * and the invariant that caught it. A canary is a miss if nothing fails within {@code --budget} seeds (default
 * {@value CanaryCatalog#SEED_BUDGET}) or if the first failure is not one of its expected invariants. Exit status 1 on
 * any miss. {@code --out file} also writes the table as Markdown.
 */
public final class CanaryMain {

    private CanaryMain() {
    }

    public static void main(String[] args) throws Exception {
        long from = 1;
        int budget = CanaryCatalog.SEED_BUDGET;
        int threads = Runtime.getRuntime().availableProcessors();
        Path out = null;
        List<Canary> canaries = new ArrayList<>(CanaryCatalog.active());
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--from" -> from = Long.parseLong(args[++i]);
                case "--budget" -> budget = Integer.parseInt(args[++i]);
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--out" -> out = Path.of(args[++i]);
                case "--canary" -> {
                    canaries.clear();
                    for (String code : args[++i].split(",")) {
                        canaries.add(Canary.fromCode(code));
                    }
                }
                default -> {
                    System.err.println("unknown argument " + args[i]);
                    System.exit(2);
                }
            }
        }
        List<String> table = new ArrayList<>();
        table.add("| Canary | Planted bug | Seeds to detection | First failing seed | Caught by | Expected |");
        table.add("|---|---|---|---|---|---|");
        boolean allCaught = true;
        long started = System.nanoTime();
        for (Canary canary : canaries) {
            List<String> expected = CanaryCatalog.expected(canary);
            RunResult caught = null;
            int tried = 0;
            for (long chunk = from; chunk < from + budget && caught == null; chunk += threads) {
                List<CampaignMain.Job> jobs = new ArrayList<>();
                for (long s = chunk; s < Math.min(chunk + threads, from + budget); s++) {
                    jobs.add(new CampaignMain.Job(s, Profile.PR, Canaries.of(canary), "canary"));
                }
                for (RunResult r : CampaignMain.runAll(jobs, threads)) {
                    if (caught == null) {
                        tried++;
                        if (!r.passed()) {
                            caught = r;
                        }
                    }
                }
            }
            String row;
            if (caught == null) {
                allCaught = false;
                row = String.format(Locale.ROOT, "| %s `%s` | %s | **missed** (> %d) | — | — | %s |",
                        canary.code(), canary.name(), canary.plantedBug(), budget, String.join(", ", expected));
            } else {
                String by = caught.violation().orElseThrow().invariant();
                boolean ok = expected.contains(by);
                allCaught &= ok;
                row = String.format(Locale.ROOT, "| %s `%s` | %s | %d | %d | %s%s | %s |", canary.code(), canary.name(),
                        canary.plantedBug(), tried, caught.seed(), by, ok ? "" : " **(unexpected)**",
                        String.join(", ", expected));
                System.out.println(canary.code() + ": " + caught.violation().orElseThrow().detail());
                System.out.println("  REPRO: " + caught.repro());
            }
            table.add(row);
        }
        System.out.printf(Locale.ROOT, "canaries: profile=pr from=%d budget=%d threads=%d wall=%.1fs%n",
                from, budget, threads, (System.nanoTime() - started) / 1e9);
        table.forEach(System.out::println);
        if (out != null) {
            if (out.getParent() != null) {
                Files.createDirectories(out.getParent());
            }
            Files.write(out, table, StandardCharsets.UTF_8);
        }
        if (!allCaught) {
            System.out.println("FAILED: a canary was missed or caught by an unexpected invariant");
            System.exit(1);
        }
        System.out.println("PASS: every canary caught within " + budget + " seeds");
    }
}
