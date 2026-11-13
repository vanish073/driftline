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
- App start/stop smoke test under the production heap cap (256 MB, SerialGC): `./gradlew :app:test`
- PostgreSQL integration tests (Testcontainers, needs Docker Desktop running): `./gradlew :store-postgres:integrationTest`. `check` includes them; CI runs them as the separate `integration` job.
- Everything except the Testcontainers suite (what CI's `build` job runs): `./gradlew check -x :store-postgres:integrationTest`
- Free memory when the machine is swapping: `./gradlew --stop`
- Simulator commands (`:sim:run`, `:sim:campaign`, `:sim:canaries`) arrive in Shifts 3 and 4 (DESIGN §11.12).

Gradle 9.9.1 through the wrapper only. `gradle/gradle-daemon-jvm.properties` pins the daemon to Java 21, and `build-logic` compiles to Java 21 bytecode.
