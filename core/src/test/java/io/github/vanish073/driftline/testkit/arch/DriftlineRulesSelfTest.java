package io.github.vanish073.driftline.testkit.arch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import io.github.vanish073.driftline.sim.archfixture.CanaryOnlyPart;
import io.github.vanish073.driftline.sim.archfixture.KernelPart;
import io.github.vanish073.driftline.sim.runner.archfixture.DriverPart;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Proves each rule bites: a fixture that breaks it fails, and a clean fixture passes every rule. */
class DriftlineRulesSelfTest {

    private static final List<ArchRule> ALL_RULES = List.of(
            DriftlineRules.NO_WALL_CLOCK, DriftlineRules.NO_UNSEEDED_RANDOMNESS, DriftlineRules.NO_THREADS,
            DriftlineRules.NO_ENVIRONMENT_OR_IO, DriftlineRules.NO_HASH_ORDERED_COLLECTIONS,
            DriftlineRules.NO_DEFAULT_LOCALE, DriftlineRules.NO_MUTABLE_STATIC_STATE, DriftlineRules.DETERMINISM,
            DriftlineRules.NO_FLOATING_POINT, DriftlineRules.NO_SPRING, DriftlineRules.CANARIES_BUILT_ONLY_IN_SIM,
            DriftlineRules.UNCHECKED_LEDGER_TXNS_ONLY_IN_SIM, DriftlineRules.SIM_ONLY_CONSTRUCTION,
            DriftlineRules.NOTHING_DEPENDS_ON_SIM_RUNNER);

    private static JavaClasses only(Class<?>... classes) {
        return new ClassFileImporter().importClasses(classes);
    }

    /** The Shift 1 requirement, spelled out: a class that calls Instant.now() must fail the determinism rule. */
    @Test
    void aClassThatCallsInstantNowFailsTheDeterminismRule() {
        assertThatThrownBy(() -> DriftlineRules.DETERMINISM.check(only(RuleFixtures.ReadsWallClock.class)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("java.time.Instant.now()");
    }

    static Stream<Arguments> violations() {
        return Stream.of(
                arguments("NO_WALL_CLOCK", DriftlineRules.NO_WALL_CLOCK, RuleFixtures.ReadsWallClock.class),
                arguments("NO_WALL_CLOCK", DriftlineRules.NO_WALL_CLOCK, RuleFixtures.ReadsSystemMillis.class),
                arguments("NO_WALL_CLOCK", DriftlineRules.NO_WALL_CLOCK, RuleFixtures.ReferencesWallClock.class),
                arguments("NO_WALL_CLOCK", DriftlineRules.NO_WALL_CLOCK, RuleFixtures.ReadsChronoClock.class),
                arguments("NO_HASH_ORDERED_COLLECTIONS", DriftlineRules.NO_HASH_ORDERED_COLLECTIONS, RuleFixtures.ReferencesSetOf.class),
                arguments("NO_UNSEEDED_RANDOMNESS", DriftlineRules.NO_UNSEEDED_RANDOMNESS, RuleFixtures.UsesRandom.class),
                arguments("NO_UNSEEDED_RANDOMNESS", DriftlineRules.NO_UNSEEDED_RANDOMNESS, RuleFixtures.UsesRandomUuid.class),
                arguments("NO_THREADS", DriftlineRules.NO_THREADS, RuleFixtures.StartsThread.class),
                arguments("NO_THREADS", DriftlineRules.NO_THREADS, RuleFixtures.UsesParallelStream.class),
                arguments("NO_ENVIRONMENT_OR_IO", DriftlineRules.NO_ENVIRONMENT_OR_IO, RuleFixtures.ReadsEnvironment.class),
                arguments("NO_ENVIRONMENT_OR_IO", DriftlineRules.NO_ENVIRONMENT_OR_IO, RuleFixtures.ReadsSystemProperty.class),
                arguments("NO_ENVIRONMENT_OR_IO", DriftlineRules.NO_ENVIRONMENT_OR_IO, RuleFixtures.PrintsToConsole.class),
                arguments("NO_HASH_ORDERED_COLLECTIONS", DriftlineRules.NO_HASH_ORDERED_COLLECTIONS, RuleFixtures.UsesHashMap.class),
                arguments("NO_HASH_ORDERED_COLLECTIONS", DriftlineRules.NO_HASH_ORDERED_COLLECTIONS, RuleFixtures.UsesSetOf.class),
                arguments("NO_HASH_ORDERED_COLLECTIONS", DriftlineRules.NO_HASH_ORDERED_COLLECTIONS, RuleFixtures.UsesCollectorsToSet.class),
                arguments("NO_DEFAULT_LOCALE", DriftlineRules.NO_DEFAULT_LOCALE, RuleFixtures.FormatsWithDefaultLocale.class),
                arguments("NO_DEFAULT_LOCALE", DriftlineRules.NO_DEFAULT_LOCALE, RuleFixtures.UpperCasesWithDefaultLocale.class),
                arguments("NO_MUTABLE_STATIC_STATE", DriftlineRules.NO_MUTABLE_STATIC_STATE, RuleFixtures.HoldsMutableStatic.class),
                arguments("NO_MUTABLE_STATIC_STATE", DriftlineRules.NO_MUTABLE_STATIC_STATE, RuleFixtures.HoldsStaticArray.class),
                arguments("NO_FLOATING_POINT", DriftlineRules.NO_FLOATING_POINT, RuleFixtures.UsesDouble.class),
                arguments("NO_FLOATING_POINT", DriftlineRules.NO_FLOATING_POINT, RuleFixtures.ReferencesDoubleMethod.class),
                arguments("NO_SPRING", DriftlineRules.NO_SPRING, RuleFixtures.DependsOnSpring.class),
                arguments("CANARIES_BUILT_ONLY_IN_SIM", DriftlineRules.CANARIES_BUILT_ONLY_IN_SIM, RuleFixtures.BuildsCanaries.class),
                arguments("UNCHECKED_LEDGER_TXNS_ONLY_IN_SIM", DriftlineRules.UNCHECKED_LEDGER_TXNS_ONLY_IN_SIM, RuleFixtures.BuildsUncheckedLedgerTxn.class),
                arguments("SIM_ONLY_CONSTRUCTION", DriftlineRules.SIM_ONLY_CONSTRUCTION, RuleFixtures.BuildsUncheckedLedgerTxn.class),
                arguments("SIM_ONLY_CONSTRUCTION", DriftlineRules.SIM_ONLY_CONSTRUCTION, RuleFixtures.BuildsCanaries.class),
                arguments("NOTHING_DEPENDS_ON_SIM_RUNNER", DriftlineRules.NOTHING_DEPENDS_ON_SIM_RUNNER, KernelPart.class));
    }

    @ParameterizedTest(name = "{0} catches {2}")
    @MethodSource("violations")
    void eachRuleCatchesItsViolation(String name, ArchRule rule, Class<?> fixture) {
        assertThat(rule.evaluate(only(fixture)).hasViolation()).as(name + " should reject " + fixture.getName()).isTrue();
    }

    @Test
    void theDeterminismRuleIncludesEveryDeterminismRule() {
        List<Class<?>> fixtures = List.of(RuleFixtures.ReadsWallClock.class, RuleFixtures.UsesRandom.class,
                RuleFixtures.StartsThread.class, RuleFixtures.ReadsEnvironment.class, RuleFixtures.UsesHashMap.class,
                RuleFixtures.FormatsWithDefaultLocale.class, RuleFixtures.HoldsMutableStatic.class);
        for (Class<?> fixture : fixtures) {
            assertThat(DriftlineRules.DETERMINISM.evaluate(only(fixture)).hasViolation()).as(fixture.getName()).isTrue();
        }
    }

    @Test
    void aCleanClassPassesEveryRule() {
        JavaClasses clean = only(RuleFixtures.Clean.class);
        for (ArchRule rule : ALL_RULES) {
            assertThat(rule.evaluate(clean).hasViolation()).as(rule.getDescription()).isFalse();
        }
    }

    @Test
    void theSimulatorMayBuildCanariesAndItsDriverMayUseIt() {
        assertThat(DriftlineRules.CANARIES_BUILT_ONLY_IN_SIM.evaluate(only(KernelPart.class)).hasViolation()).isFalse();
        assertThat(DriftlineRules.NOTHING_DEPENDS_ON_SIM_RUNNER.evaluate(only(CanaryOnlyPart.class, DriverPart.class))
                .hasViolation()).isFalse();
    }
}
