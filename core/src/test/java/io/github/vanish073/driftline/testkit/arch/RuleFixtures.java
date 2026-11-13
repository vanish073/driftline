package io.github.vanish073.driftline.testkit.arch;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.archfixture.SpringType;

/** Classes that break exactly one rule each, plus one that breaks none. Test code: never imported as production. */
final class RuleFixtures {

    private RuleFixtures() {
    }

    static final class Clean {
        static final List<String> NAMES = List.of("a", "b");

        long add(long a, long b) {
            return Math.addExact(a, b);
        }

        Instant later(Instant at) {
            return at.plus(Duration.ofSeconds(5));
        }

        String loud(String s) {
            return s.toUpperCase(Locale.ROOT) + String.format(Locale.ROOT, "%d", 1);
        }

        Map<String, Integer> lengths(List<String> words) {
            return words.stream().collect(Collectors.toMap(Function.identity(), String::length, (x, y) -> x, TreeMap::new));
        }

        Map<String, String> ordered() {
            return new LinkedHashMap<>();
        }
    }

    static final class ReadsWallClock {
        Instant now() {
            return Instant.now();
        }
    }

    static final class ReferencesWallClock {
        java.util.function.Supplier<Instant> clock() {
            return Instant::now;
        }
    }

    static final class ReadsChronoClock {
        java.time.chrono.JapaneseDate today() {
            return java.time.chrono.JapaneseDate.now();
        }
    }

    static final class ReferencesSetOf {
        Function<String, Set<String>> singleton() {
            return Set::of;
        }
    }

    static final class ReadsSystemMillis {
        long now() {
            return System.currentTimeMillis();
        }
    }

    static final class UsesRandom {
        int roll() {
            return new Random(42).nextInt();
        }
    }

    static final class UsesRandomUuid {
        String id() {
            return java.util.UUID.randomUUID().toString();
        }
    }

    static final class StartsThread {
        void run() {
            new Thread(() -> { }).start();
        }
    }

    static final class UsesParallelStream {
        long count(List<String> words) {
            return words.parallelStream().count();
        }
    }

    static final class ReadsEnvironment {
        String home() {
            return System.getenv("HOME");
        }
    }

    static final class ReadsSystemProperty {
        String dir() {
            return System.getProperty("user.dir");
        }
    }

    static final class PrintsToConsole {
        void hello() {
            System.out.println("hello");
        }
    }

    static final class UsesHashMap {
        Map<String, String> map() {
            return new HashMap<>();
        }
    }

    static final class UsesSetOf {
        Set<String> set() {
            return Set.of("a", "b");
        }
    }

    static final class UsesCollectorsToSet {
        Set<String> set(List<String> words) {
            return words.stream().collect(Collectors.toSet());
        }
    }

    static final class FormatsWithDefaultLocale {
        String format() {
            return String.format("%d", 1);
        }
    }

    static final class UpperCasesWithDefaultLocale {
        String loud(String s) {
            return s.toUpperCase();
        }
    }

    static final class HoldsMutableStatic {
        static int counter;
    }

    static final class HoldsStaticArray {
        static final int[] TABLE = {1, 2};
    }

    static final class UsesDouble {
        double half(long paise) {
            return paise / 2.0;
        }
    }

    static final class ReferencesDoubleMethod {
        java.util.function.DoubleUnaryOperator root() {
            return Math::sqrt;
        }
    }

    static final class DependsOnSpring {
        SpringType bean;
    }

    static final class BuildsCanaries {
        Canaries canaries() {
            return Canaries.of(Canary.DEDUPE_OFF);
        }
    }
}
