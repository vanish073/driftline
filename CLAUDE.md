# Driftline: working rules (read at the start of every shift)

- **Scope (source of truth):** `docs/spec/REQUIREMENTS.md`. The brief is verbatim, plus the reviewer concerns.
- **Design:** `docs/DESIGN.md`. Change it only by appending to §19 Decisions.
- **Plan and status:** `docs/SHIFTS.md`. Work on the first TODO shift only; anything else goes on its Carry-over line.
- **Bug log:** `docs/BUGS.md`. Regression seeds: `sim/regression-seeds.txt`.

## Non-negotiable rules

**Determinism** (applies to `core`, `wire`, `sim`; ArchUnit enforces it; never add exceptions). The code must not use:
- a wall clock;
- randomness other than the injected SplitMix64;
- threads or I/O;
- env or system properties;
- `HashMap`, `HashSet`, `Set.of`, `Map.of`, `Collectors.toSet`, `toMap` or `groupingBy`;
- default-locale formatting;
- static mutable state.

**Money and IDs**
- Money is `long` paise, always with `Math.*Exact`. Never `float` or `double`.
- IDs are derived from content (`DerivedId`), never random.

**Store parity:** every Store method exists in `InMemoryStore` **and** `PostgresStore` with a contract test, in the same change.

**Claims**
- Never write "exactly-once". Write "effectively-once effects" and link DESIGN §2. CI `claims-lint` enforces this.
- Every number in the README comes from a committed script plus raw output in `docs/results/`.

**Bugs:** every real simulator-found bug gets a BUGS.md row plus a regression seed. Canaries are not bugs.

**Commits:** only when the user says so.

**Spring:** Spring Boot lives in `app` only. `core`, `wire`, `sim` and `store-postgres` must never depend on Spring (DESIGN D17, D18). Wire the ports explicitly in `@Configuration` classes; no component scanning of the core.

## Environment

- WSL2 with 4 GB RAM and 8 GB swap (`.wslconfig`, verified 2026-11-13), of which about 2 GB is taken by VS Code and Claude sessions; 12 cores; Java 21.
- Docker Desktop is integrated with WSL (verified 2026-11-13).
- VS Code's Java extension imports this project with its own Gradle (9.7.1 on its bundled JRE 25). When it builds while `./gradlew` runs, the two clobber `build-logic/build` ("Storage corrupted", class file version 69). The Java 21 pins (see Commands) stop the version clash. If a build still fails that way: stop VS Code's daemon with `~/.gradle/wrapper/dists/gradle-9.7.1-bin/*/gradle-9.7.1/bin/gradle --stop`, run `rm -rf build-logic/build`, and run the build again.
- Keep heaps capped: Gradle `-Xmx1g`, app `-Xmx256m` with SerialGC, Kafka `-Xmx384m`, sandbox `-Xmx128m`. See DESIGN §15.1.
- The 10k-seed runs, long campaigns and the kind cluster run in CI (public repo).
- GitHub: `gh` is logged in as `vanish073` (scopes `repo` and `workflow`). Use **HTTPS** remotes, because git authenticates through `gh auth git-credential`. SSH needs a key passphrase that non-interactive shells can't supply.

## Commands

- Everything (compile with `-Werror`, unit tests, jqwik properties, ArchUnit, the Spring check): `./gradlew check`
- One module: `./gradlew :core:test`
- One test class: `./gradlew :core:test --tests '*PaymentTransitionTableTest'`
- App unit tests (MVC slices, security, no Docker): `./gradlew :app:test`
- PostgreSQL integration tests (Testcontainers, needs Docker Desktop running): `./gradlew :store-postgres:integrationTest`.
- App integration tests (PostgreSQL and Kafka in Testcontainers, all under the production heap cap of 256 MB with SerialGC; includes the start/stop smoke test `*ApplicationSmokeIT`): `./gradlew :app:integrationTest`. `check` includes both suites; CI runs them as the separate `integration` job.
- Everything except the Testcontainers suites (what CI's `build` job runs): `./gradlew check -x :store-postgres:integrationTest -x :app:integrationTest`
- End to end in Docker Compose (M3; close VS Code first; stops Gradle itself): `scripts/e2e.sh [results-file]`. Generated secrets and the Compose log go to `deploy/compose/.dev/` (git-ignored).
- Inject a scenario into a running stack: `sandbox/build/install/sandbox/bin/sandbox inject --type DUPLICATE_CAPTURE --count 5`
- Claims lint (CI job `claims-lint`): `scripts/claims-lint.sh`
- Free memory when the machine is swapping: `./gradlew --stop`
- One simulator seed: `./gradlew :sim:run --args="--seed 8812 --profile calm"`; add `--trace` for the timeline. A failing seed prints the REPRO line and writes `sim/build/sim-failures/seed-<n>.trace`.
- 1,000 `calm` seeds, each run twice (determinism): `./gradlew :sim:test --tests '*CalmCampaignTest'`
- Seeds-to-detection per canary (writes the table of `docs/results/canaries.md`): `./gradlew -q :sim:canaries --args="--threads 8 --out ../docs/results/canaries.md"`
- The settlement case corpus (35 cases and 6 golden files, run through the real matcher): `./gradlew :sim:test --tests '*SettlementCaseCorpusTest'`. Regenerate it with `python3 scripts/gen-settlement-cases.py core/src/test/resources/settlement-cases`; the script re-checks the expectations with an independent model and reproduces the committed files byte for byte.
- VS Code's Gradle import is switched off in `.vscode/settings.json` (gitignored), because it raced `./gradlew`.

Gradle 9.9.1 through the wrapper only. `gradle/gradle-daemon-jvm.properties` pins the daemon to Java 21, and `build-logic` compiles to Java 21 bytecode.
