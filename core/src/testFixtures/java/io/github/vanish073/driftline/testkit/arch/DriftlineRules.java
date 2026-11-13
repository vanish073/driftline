package io.github.vanish073.driftline.testkit.arch;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.fields;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.AccessTarget;
import com.tngtech.archunit.core.domain.AccessTarget.CodeUnitAccessTarget;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaCodeUnitAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaFieldAccess;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.CompositeArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import java.time.Clock;
import java.time.InstantSource;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * The build-enforced rules of DESIGN §11.11 (determinism), §4.1 (no floating point in money code), D18 (Spring stays
 * in {@code app}), §11.10 (only the simulator builds canaries) and D22 (nothing depends on the simulator's driver).
 * Details of the rule list are in D26.
 *
 * <p>Each module's {@code ArchitectureTest} imports its own main classes with {@link #importMainClasses} and checks
 * the rules that apply to it. Every rule allows an empty import, because some modules have no code yet; each test
 * separately asserts that the import found the module's package, so a typo cannot make a rule pass vacuously.
 *
 * <p>Calls and method references are both checked ({@code Instant::now} counts as reading the clock).
 *
 * <p>Limit, stated plainly: ArchUnit reads class files, so it sees signatures, field types, calls and dependencies,
 * not arithmetic on local variables. A local {@code double} computed from literals would pass the floating-point rule.
 */
public final class DriftlineRules {

    public static final String ROOT = "io.github.vanish073.driftline";
    public static final String SIM = ROOT + ".sim";
    public static final String SIM_RUNNER = SIM + ".runner";

    private DriftlineRules() {
    }

    /**
     * The production classes of {@code packageName}: no test classes and no test fixtures. Fails if nothing is found,
     * so a mistyped package can never make a rule pass on an empty set. Every module has at least its
     * {@code package-info} class (compiled with {@code -Xpkginfo:always}).
     */
    public static JavaClasses importMainClasses(String packageName) {
        JavaClasses classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .withImportOption(location -> !location.contains("/testFixtures/") && !location.contains("-test-fixtures"))
                .importPackages(packageName);
        if (classes.isEmpty()) {
            throw new IllegalStateException("no production classes found in " + packageName
                    + "; check the package name and that the module has a package-info.java");
        }
        return classes;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Call and dependency matchers
    // ---------------------------------------------------------------------------------------------------------------

    /** A call to, or a method reference of, exactly this signature. */
    private static DescribedPredicate<JavaAccess<?>> call(Class<?> owner, String name, Class<?>... parameters) {
        return DescribedPredicate.describe(
                owner.getSimpleName() + "." + name + "("
                        + String.join(", ", Arrays.stream(parameters).map(Class::getSimpleName).toList()) + ")",
                c -> isOwnedBy(c.getTarget(), owner) && c.getTarget().getName().equals(name)
                        && sameParameters(c.getTarget(), parameters));
    }

    /** A call to, or a method reference of, any overload of {@code owner.name}. */
    private static DescribedPredicate<JavaAccess<?>> anyOverload(Class<?> owner, String name) {
        return DescribedPredicate.describe(owner.getSimpleName() + "." + name + "(..)",
                c -> isOwnedBy(c.getTarget(), owner) && c.getTarget().getName().equals(name));
    }

    /** A call to any method {@code name} declared by a type in {@code packageName} or below it. */
    private static DescribedPredicate<JavaAccess<?>> anyInPackage(String packageName, String name) {
        return DescribedPredicate.describe(packageName + ".*." + name + "(..)",
                c -> inPackageTree(c.getTarget().getOwner(), packageName) && c.getTarget().getName().equals(name));
    }

    /** A call to any method {@code name}, whatever its owner. */
    private static DescribedPredicate<JavaAccess<?>> anyNamed(String name) {
        return DescribedPredicate.describe("*." + name + "(..)", c -> c.getTarget().getName().equals(name));
    }

    @SafeVarargs
    private static DescribedPredicate<JavaAccess<?>> anyCallOf(String description, DescribedPredicate<JavaAccess<?>>... calls) {
        List<DescribedPredicate<JavaAccess<?>>> collected = new ArrayList<>(calls.length);
        for (DescribedPredicate<JavaAccess<?>> call : calls) {
            collected.add(call);
        }
        List<DescribedPredicate<JavaAccess<?>>> all = List.copyOf(collected);
        return DescribedPredicate.describe(description + " " + all.stream().map(DescribedPredicate::getDescription).toList(),
                c -> all.stream().anyMatch(p -> p.test(c)));
    }

    private static boolean isOwnedBy(AccessTarget target, Class<?> owner) {
        return target.getOwner().isEquivalentTo(owner);
    }

    private static boolean sameParameters(AccessTarget target, Class<?>[] parameters) {
        if (!(target instanceof CodeUnitAccessTarget codeUnit)) {
            return false;
        }
        List<JavaClass> actual = codeUnit.getRawParameterTypes();
        if (actual.size() != parameters.length) {
            return false;
        }
        for (int i = 0; i < parameters.length; i++) {
            if (!actual.get(i).isEquivalentTo(parameters[i])) {
                return false;
            }
        }
        return true;
    }

    /** A class named exactly, or nested inside it. */
    private static boolean belongsTo(JavaClass type, String className) {
        return type.getName().equals(className) || type.getName().startsWith(className + "$");
    }

    private static boolean inPackageTree(JavaClass type, String packageName) {
        return type.getPackageName().equals(packageName) || type.getPackageName().startsWith(packageName + ".");
    }

    private static DescribedPredicate<JavaClass> named(String description, String... classNames) {
        List<String> names = List.of(classNames);
        return DescribedPredicate.describe(description, type -> names.stream().anyMatch(n -> belongsTo(type, n)));
    }

    private static DescribedPredicate<JavaClass> inPackages(String description, String... packageNames) {
        List<String> names = List.of(packageNames);
        return DescribedPredicate.describe(description, type -> names.stream().anyMatch(p -> inPackageTree(type, p)));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Determinism (DESIGN §11.11)
    // ---------------------------------------------------------------------------------------------------------------

    private static final DescribedPredicate<JavaAccess<?>> WALL_CLOCK_CALLS = anyCallOf("a wall-clock read",
            call(System.class, "currentTimeMillis"),
            call(System.class, "nanoTime"),
            anyInPackage("java.time", "now"),
            anyOverload(Clock.class, "systemUTC"),
            anyOverload(Clock.class, "systemDefaultZone"),
            anyOverload(Clock.class, "system"),
            anyOverload(Clock.class, "tickMillis"),
            anyOverload(Clock.class, "tickSeconds"),
            anyOverload(Clock.class, "tickMinutes"),
            anyOverload(InstantSource.class, "system"),
            anyOverload(ZoneId.class, "systemDefault"),
            anyOverload(TimeZone.class, "getDefault"),
            anyOverload(TimeZone.class, "setDefault"));

    private static final DescribedPredicate<JavaClass> LEGACY_TIME = named("legacy date and time classes",
            "java.util.Date", "java.util.Calendar", "java.util.GregorianCalendar", "java.util.TimeZone",
            "java.util.SimpleTimeZone");

    public static final ArchRule NO_WALL_CLOCK = noClasses()
            .should().accessTargetWhere(WALL_CLOCK_CALLS)
            .orShould().dependOnClassesThat(LEGACY_TIME)
            .because("processing time comes only from the Clock port, and event time only from events (DESIGN §11.11)")
            .allowEmptyShould(true);

    private static final DescribedPredicate<JavaAccess<?>> RANDOM_CALLS = anyCallOf("a source of randomness",
            call(Math.class, "random"),
            call(StrictMath.class, "random"),
            call(UUID.class, "randomUUID"),
            anyOverload(Collections.class, "shuffle"),
            anyOverload(System.class, "identityHashCode"));

    private static final DescribedPredicate<JavaClass> RANDOM_TYPES = DescribedPredicate.describe(
            "JDK random generators",
            type -> belongsTo(type, "java.util.Random") || belongsTo(type, "java.security.SecureRandom")
                    || belongsTo(type, "java.util.SplittableRandom")
                    || belongsTo(type, "java.util.concurrent.ThreadLocalRandom")
                    || inPackageTree(type, "java.util.random")
                    || type.isAssignableTo("java.util.random.RandomGenerator"));

    public static final ArchRule NO_UNSEEDED_RANDOMNESS = noClasses()
            .should().accessTargetWhere(RANDOM_CALLS)
            .orShould().dependOnClassesThat(RANDOM_TYPES)
            .because("the only randomness is the injected, seeded SplitMix64 (DESIGN §11.2, D13)")
            .allowEmptyShould(true);

    private static final DescribedPredicate<JavaAccess<?>> THREAD_CALLS = anyCallOf("parallel execution",
            anyNamed("parallelStream"),
            anyInPackage("java.util.stream", "parallel"),
            anyOverload(Arrays.class, "parallelSort"),
            anyOverload(Arrays.class, "parallelSetAll"),
            anyOverload(Arrays.class, "parallelPrefix"));

    private static final DescribedPredicate<JavaClass> THREAD_TYPES = DescribedPredicate.describe(
            "threads, timers and java.util.concurrent",
            type -> belongsTo(type, "java.lang.Thread") || belongsTo(type, "java.lang.ThreadGroup")
                    || type.isAssignableTo("java.lang.ThreadLocal")
                    || belongsTo(type, "java.util.Timer") || belongsTo(type, "java.util.TimerTask")
                    || inPackageTree(type, "java.util.concurrent"));

    public static final ArchRule NO_THREADS = noClasses()
            .should().accessTargetWhere(THREAD_CALLS)
            .orShould().dependOnClassesThat(THREAD_TYPES)
            .because("the core is single-threaded and sans-I/O; drivers own the loops (DESIGN §3.1, §11.11)")
            .allowEmptyShould(true);

    private static final DescribedPredicate<JavaAccess<?>> ENVIRONMENT_CALLS = anyCallOf("an environment read",
            anyOverload(System.class, "getenv"),
            anyOverload(System.class, "getProperty"),
            anyOverload(System.class, "getProperties"),
            anyOverload(System.class, "setProperty"),
            anyOverload(System.class, "setProperties"),
            anyOverload(System.class, "clearProperty"),
            anyOverload(System.class, "console"),
            anyOverload(System.class, "getLogger"),
            anyOverload(System.class, "exit"),
            anyOverload(System.class, "load"),
            anyOverload(System.class, "loadLibrary"),
            anyOverload(Runtime.class, "getRuntime"),
            anyOverload(Integer.class, "getInteger"),
            anyOverload(Long.class, "getLong"),
            anyOverload(Boolean.class, "getBoolean"));

    private static final DescribedPredicate<JavaClass> IO_TYPES = DescribedPredicate.describe(
            "file, network, process, logging and service-loading classes",
            type -> named("", "java.io.File", "java.io.FileInputStream", "java.io.FileOutputStream",
                    "java.io.FileReader", "java.io.FileWriter", "java.io.RandomAccessFile", "java.io.Console",
                    "java.lang.Process", "java.lang.ProcessBuilder", "java.lang.ProcessHandle",
                    "java.util.ServiceLoader").test(type)
                    || inPackages("", "java.nio.file", "java.nio.channels", "javax.net", "java.rmi", "java.sql",
                    "javax.sql", "java.util.logging", "java.util.prefs").test(type)
                    || (inPackageTree(type, "java.net") && !type.getName().equals("java.net.URI")
                        && !type.getName().equals("java.net.URLEncoder")
                        && !type.getName().equals("java.net.URLDecoder")));

    public static final ArchRule NO_ENVIRONMENT_OR_IO = noClasses()
            .should().accessTargetWhere(ENVIRONMENT_CALLS)
            .orShould().accessField(System.class, "out")
            .orShould().accessField(System.class, "err")
            .orShould().accessField(System.class, "in")
            .orShould().dependOnClassesThat(IO_TYPES)
            .because("the core reads nothing from its environment and performs no I/O (DESIGN §3.1, §11.11)")
            .allowEmptyShould(true);

    private static final DescribedPredicate<JavaAccess<?>> HASH_ORDER_CALLS = anyCallOf("a collection with salted order",
            anyOverload(Set.class, "of"),
            anyOverload(Set.class, "copyOf"),
            anyOverload(Map.class, "of"),
            anyOverload(Map.class, "ofEntries"),
            anyOverload(Map.class, "copyOf"),
            call(Collectors.class, "toSet"),
            anyOverload(Collectors.class, "toUnmodifiableSet"),
            call(Collectors.class, "toMap", Function.class, Function.class),
            call(Collectors.class, "toMap", Function.class, Function.class, BinaryOperator.class),
            anyOverload(Collectors.class, "toUnmodifiableMap"),
            call(Collectors.class, "groupingBy", Function.class),
            call(Collectors.class, "groupingBy", Function.class, Collector.class),
            anyOverload(Collectors.class, "groupingByConcurrent"),
            anyOverload(Collectors.class, "toConcurrentMap"));

    private static final DescribedPredicate<JavaClass> HASH_ORDER_TYPES = named("hash-ordered collections",
            "java.util.HashMap", "java.util.HashSet", "java.util.Hashtable", "java.util.IdentityHashMap",
            "java.util.WeakHashMap", "java.util.Properties");

    public static final ArchRule NO_HASH_ORDERED_COLLECTIONS = noClasses()
            .should().accessTargetWhere(HASH_ORDER_CALLS)
            .orShould().dependOnClassesThat(HASH_ORDER_TYPES)
            .because("iteration order must not depend on identity hashes or the per-JVM salt of Set.of and Map.of;"
                    + " use TreeMap, LinkedHashMap, EnumMap or a supplied map (DESIGN §11.11)")
            .allowEmptyShould(true);

    private static final DescribedPredicate<JavaAccess<?>> DEFAULT_LOCALE_CALLS = anyCallOf("a default-locale call",
            call(String.class, "format", String.class, Object[].class),
            call(String.class, "formatted", Object[].class),
            call(String.class, "toUpperCase"),
            call(String.class, "toLowerCase"),
            anyOverload(Locale.class, "getDefault"),
            anyOverload(Locale.class, "setDefault"),
            call(DateTimeFormatter.class, "ofPattern", String.class),
            anyOverload(DateTimeFormatter.class, "ofLocalizedDate"),
            anyOverload(DateTimeFormatter.class, "ofLocalizedTime"),
            anyOverload(DateTimeFormatter.class, "ofLocalizedDateTime"));

    private static final DescribedPredicate<JavaClass> LOCALE_TYPES = DescribedPredicate.describe(
            "java.text, Formatter, Scanner and ResourceBundle",
            type -> inPackageTree(type, "java.text")
                    || named("", "java.util.Formatter", "java.util.Scanner", "java.util.ResourceBundle").test(type));

    public static final ArchRule NO_DEFAULT_LOCALE = noClasses()
            .should().accessTargetWhere(DEFAULT_LOCALE_CALLS)
            .orShould().dependOnClassesThat(LOCALE_TYPES)
            .because("output must not change with the machine's locale; pass Locale.ROOT (DESIGN §11.11)")
            .allowEmptyShould(true);

    private static final ArchCondition<JavaField> NOT_BE_AN_ARRAY = new ArchCondition<>("not be an array") {
        @Override
        public void check(JavaField field, ConditionEvents events) {
            if (field.getRawType().isArray()) {
                events.add(SimpleConditionEvent.violated(field, field.getFullName() + " is a static array, which is mutable"));
            }
        }
    };

    public static final ArchRule NO_MUTABLE_STATIC_STATE = fields()
            .that().areStatic().and().doNotHaveModifier(JavaModifier.SYNTHETIC)
            .should().beFinal()
            .andShould(NOT_BE_AN_ARRAY)
            .because("static state survives between simulator runs in one JVM and breaks reproducibility"
                    + " (DESIGN §11.11)")
            .allowEmptyShould(true);

    /** Every determinism rule of DESIGN §11.11. Applied to {@code core}, {@code wire} and {@code sim}. */
    public static final ArchRule DETERMINISM = CompositeArchRule.of(NO_WALL_CLOCK)
            .and(NO_UNSEEDED_RANDOMNESS)
            .and(NO_THREADS)
            .and(NO_ENVIRONMENT_OR_IO)
            .and(NO_HASH_ORDERED_COLLECTIONS)
            .and(NO_DEFAULT_LOCALE)
            .and(NO_MUTABLE_STATIC_STATE)
            .allowEmptyShould(true);

    // ---------------------------------------------------------------------------------------------------------------
    // Money: no floating point (DESIGN §4.1, D26)
    // ---------------------------------------------------------------------------------------------------------------

    private static boolean isFloatingPoint(JavaClass type) {
        JavaClass base = type.isArray() ? type.getBaseComponentType() : type;
        return base.isEquivalentTo(double.class) || base.isEquivalentTo(float.class)
                || base.isEquivalentTo(Double.class) || base.isEquivalentTo(Float.class);
    }

    private static final ArchCondition<JavaClass> NOT_USE_FLOATING_POINT =
            new ArchCondition<>("not use floating-point types") {
                @Override
                public void check(JavaClass type, ConditionEvents events) {
                    for (JavaField field : type.getFields()) {
                        if (isFloatingPoint(field.getRawType())) {
                            events.add(SimpleConditionEvent.violated(field, field.getFullName() + " is floating point"));
                        }
                    }
                    for (JavaCodeUnit unit : type.getCodeUnits()) {
                        boolean inSignature = isFloatingPoint(unit.getRawReturnType())
                                || unit.getRawParameterTypes().stream().anyMatch(DriftlineRules::isFloatingPoint);
                        if (inSignature) {
                            events.add(SimpleConditionEvent.violated(unit, unit.getFullName() + " has a floating-point signature"));
                        }
                    }
                    // Calls and method references alike: Math::pow would otherwise slip through.
                    for (JavaCodeUnitAccess<?> called : type.getCodeUnitAccessesFromSelf()) {
                        CodeUnitAccessTarget target = called.getTarget();
                        boolean inSignature = isFloatingPoint(target.getRawReturnType())
                                || target.getRawParameterTypes().stream().anyMatch(DriftlineRules::isFloatingPoint);
                        if (inSignature) {
                            events.add(SimpleConditionEvent.violated(called, called.getDescription()));
                        }
                    }
                    for (JavaFieldAccess access : type.getFieldAccessesFromSelf()) {
                        if (isFloatingPoint(access.getTarget().getRawType())) {
                            events.add(SimpleConditionEvent.violated(access, access.getDescription()));
                        }
                    }
                }
            };

    /** Applied to {@code core} and {@code wire}: all of them is money code (D26). */
    public static final ArchRule NO_FLOATING_POINT = classes()
            .should(NOT_USE_FLOATING_POINT)
            .because("money is long paise with exact arithmetic; float and double are banned (DESIGN §4.1)")
            .allowEmptyShould(true);

    // ---------------------------------------------------------------------------------------------------------------
    // Module boundaries
    // ---------------------------------------------------------------------------------------------------------------

    /** Applied to every module except {@code app}. A Gradle check (verifyNoSpring) guards the classpath as well. */
    public static final ArchRule NO_SPRING = noClasses()
            .should().dependOnClassesThat().resideInAPackage("org.springframework..")
            .because("Spring Boot lives in the app module only (D17, D18)")
            .allowEmptyShould(true);

    /** Applied to every module. Only {@code sim} (and test code, which is never imported) may switch canaries on. */
    public static final ArchRule CANARIES_BUILT_ONLY_IN_SIM = noClasses()
            .that().resideOutsideOfPackage(SIM + "..")
            .and().doNotBelongToAnyOf(Canaries.class)
            .should().callConstructor(Canaries.class, List.class)
            .orShould().callMethod(Canaries.class, "of", Canary[].class)
            .because("production always runs with Canaries.NONE (DESIGN §11.10)")
            .allowEmptyShould(true);

    /** Applied to every module. Skipping ledger layer 1 is for the simulator's canaries and tests only (D28). */
    public static final ArchRule UNCHECKED_LEDGER_TXNS_ONLY_IN_SIM = noClasses()
            .that().resideOutsideOfPackage(SIM + "..")
            .and().doNotBelongToAnyOf(LedgerTxn.class)
            .should().accessTargetWhere(anyOverload(LedgerTxn.class, "unchecked"))
            .because("production code always builds ledger transactions through the validating LedgerTxn.of (D28)")
            .allowEmptyShould(true);

    /** Everything only the simulator (and tests) may construct: canaries and unvalidated ledger transactions. */
    public static final ArchRule SIM_ONLY_CONSTRUCTION = CompositeArchRule.of(CANARIES_BUILT_ONLY_IN_SIM)
            .and(UNCHECKED_LEDGER_TXNS_ONLY_IN_SIM)
            .allowEmptyShould(true);

    /** Applied to {@code sim}. The driver may use the kernel, never the other way round (D22). */
    public static final ArchRule NOTHING_DEPENDS_ON_SIM_RUNNER = noClasses()
            .that().resideInAPackage(SIM + "..")
            .and().resideOutsideOfPackage(SIM_RUNNER + "..")
            .should().dependOnClassesThat().resideInAPackage(SIM_RUNNER + "..")
            .because("sim.runner (CLI, campaign threads, files) sits outside the determinism rules (D22)")
            .allowEmptyShould(true);
}
