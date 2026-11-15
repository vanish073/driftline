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
import java.util.Map;

/**
 * {@code ./gradlew :sim:run --args="--seed 8812 --profile pr [--canary C09] [--trace]"} (DESIGN §11.12). The driver sits outside
 * the determinism rules (D22): it prints, writes files and exits with a status. A failing seed prints the violated
 * invariant, the last trace lines and the REPRO line, and writes the full trace to
 * {@code build/sim-failures/seed-<seed>.trace}.
 */
public final class SimMain {

    static final String USAGE = "usage: --seed <n> [--profile calm|pr] [--canary C01 ...] [--trace]";

    private SimMain() {
    }

    public static void main(String[] args) throws IOException {
        Long seed = null;
        Profile profile = Profile.CALM;
        boolean printTrace = false;
        List<Canary> canaries = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--seed" -> seed = Long.parseLong(args[++i]);
                case "--profile" -> profile = Profile.fromId(args[++i]);
                case "--trace" -> printTrace = true;
                case "--canary" -> canaries.add(Canary.fromCode(args[++i]));
                default -> {
                    System.err.println("unknown argument " + args[i]);
                    System.err.println(USAGE);
                    System.exit(2);
                }
            }
        }
        if (seed == null) {
            System.err.println(USAGE);
            System.exit(2);
        }
        Canaries planted = new Canaries(canaries);
        RunResult result = Simulation.run(seed, profile, planted, printTrace);
        if (printTrace) {
            result.trace().forEach(System.out::println);
        }
        System.out.println("seed=" + result.seed() + " profile=" + profile.id() + " steps=" + result.steps()
                + " virtual-ms=" + result.virtualMillis() + " canaries=" + planted.enabled());
        System.out.println("swarm: " + result.swarm());
        for (Map.Entry<String, Long> stat : result.stats().entrySet()) {
            System.out.println("  " + stat.getKey() + " = " + stat.getValue());
        }
        System.out.println("digest=" + result.digest());
        if (result.passed()) {
            System.out.println("PASS");
            return;
        }
        var v = result.violation().orElseThrow();
        System.out.println("VIOLATION " + v.invariant() + " at " + v.atMillis() + " ms: " + v.detail());
        System.out.println("--- last " + result.traceTail().size() + " trace lines ---");
        result.traceTail().forEach(System.out::println);
        RunResult full = Simulation.run(seed, profile, planted, true);
        Path file = Path.of("build", "sim-failures", "seed-" + seed + ".trace");
        Files.createDirectories(file.getParent());
        Files.write(file, full.trace(), StandardCharsets.UTF_8);
        System.out.println("full trace: " + file.toAbsolutePath());
        System.out.println("REPRO: " + result.repro());
        System.exit(1);
    }
}
