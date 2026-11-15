# Driftline: the 10-shift build plan

Read with: [`docs/spec/REQUIREMENTS.md`](spec/REQUIREMENTS.md) (scope) and [`docs/DESIGN.md`](DESIGN.md) (how).
A **shift** is one focused working session. The shifts run in order. Each one ends green or records exactly what carries over.

## Status

| Shift | Theme | Milestone | Status |
|---|---|---|---|
| 0 | Design and plan | — | **DONE** (2026-11-13) |
| 1 | Foundations, ports, state machines | M1 (part 1) | **DONE** (2026-11-13) |
| 2 | Double-entry ledger, PostgreSQL, Store contract | **M1 done** | **DONE** (2026-11-13); **M1 DONE** |
| 3 | Simulator kernel and world models | M2 (part 1) | **DONE** (2026-11-13) |
| 4 | Faults, invariants, canaries, 10k seeds in CI | **M2 done** | **DONE** (2026-11-15); **M2 DONE** |
| 5 | Matcher: join, watermarks, timers, one-to-one breaks (in the simulator) | M3 (part 1) | **DONE** (2026-11-15) |
| 6 | Ingest API, Kafka drivers, settlement worker, Compose e2e | **M3 done** | **Green locally** (2026-11-15); M3 waits on the CI run |
| 7 | Batch settlement matching and test corpus | M4 (part 1) | TODO |
| 8 | Resolver, outbox, dispatcher, crash tests | **M4 done** | TODO |
| 9 | Observability, security, Kubernetes | M5 (part 1) | TODO |
| 10 | Load test, hot-account benchmark, results, README | **M5 done** | TODO |

## Human tasks (only you can do these)

| By | Task | Why |
|---|---|---|
| ~~Before Shift 2~~ | ~~Enable Docker Desktop WSL integration~~ **DONE.** Verified 2026-11-13: Engine 29.8.0, `hello-world` ran. | — |
| ~~Before Shift 2~~ | ~~Create `%UserProfile%\.wslconfig`~~ **DONE.** Verified 2026-11-13: the file has `memory=4GB`, `swap=8GB` and `autoMemoryReclaim=gradual`; `free` shows 3.9 GB RAM and 8 GB swap. | — |
| Before Shifts 6, 8, 9, 10 | Close VS Code windows and any extra Claude sessions while the Compose stack runs | They hold about 2 GB of the 3.7 GB |
| ~~Before Shift 4~~ | ~~Install `gh` and log in~~ **DONE.** Verified 2026-11-13: account `vanish073`, scopes `repo` and `workflow`. Git uses the gh token over HTTPS (`gh auth setup-git`), because the SSH key has a passphrase and there's no agent in non-interactive shells. | — |
| ~~Before Shift 4~~ | ~~Approve creating the public repo~~ **DONE.** Created 2026-11-13: https://github.com/vanish073/driftline (public, empty until the first push). | — |
| **Before Shift 7** | **Interview one finance-ops person** using [`docs/research/finance-ops-interview.md`](research/finance-ops-interview.md); save the notes there | The brief requires it before M4 |
| End of each shift | Approve the commit | Commits happen only when you say so |

## Rules for every shift

1. **Start.** Open this file, find the first TODO shift, and read its section and the DESIGN sections it lists. Re-read the matching lines of REQUIREMENTS.md.
2. **Scope.** Build only that shift's list. Anything else goes on that shift's *Carry-over* line.
3. **Tests come first for every invariant and transition table.** No feature is done without its proof.
4. **Determinism.** Core, wire and sim follow DESIGN §11.11. ArchUnit enforces it, and no exceptions are ever added.
5. **Store parity.** Every new Store method lands in `InMemoryStore` **and** `PostgresStore` (once it exists) with a contract test, in the same shift.
6. **Claims.** Never write "exactly-once". Write "effectively-once effects" and link DESIGN §2. Every README number comes from `docs/results/`.
7. **Bugs.** Every simulator failure gets triaged. A real bug gets a `docs/BUGS.md` row and its seed goes into `sim/regression-seeds.txt`.
8. **End.** `./gradlew check` is green; the simulation campaign is green (from Shift 4); the Status table, the Carry-over line and BUGS.md are updated; any design change is appended to DESIGN §19; you approve the commit.

---

## Shift 1: Foundations, ports, state machines (M1, part 1)

**Read:** DESIGN §3, §4, §11.11, §18.

**Build**
- `git init`, `.gitignore`, `.editorconfig`. Install the Gradle wrapper: download the Gradle distribution into the scratchpad, run `gradle wrapper`, commit the wrapper. Use the Kotlin DSL and `gradle/libs.versions.toml`, with versions checked on Maven Central on the day. Java 21 toolchain. `org.gradle.jvmargs=-Xmx1g`.
- Empty modules: `core`, `wire`, `sim`, `store-postgres`, `app`, `sandbox`, `bench`. `core` has **no** third-party `implementation` dependencies. Only `app` applies the Spring Boot plugin and BOM (D17, D18). A minimal `@SpringBootApplication` must start and stop in a test with `-Xmx256m`.
- `core`:
  - value types: `Money` (paise, exact arithmetic, strict parser), `MerchantId`, `PaymentId`, `OrderId`;
  - `DerivedId` (SHA-256/base32), `IstCalendar` (T date, end of the IST day, deadline instants);
  - `EventEnvelope` and the event payload records;
  - ports: `Clock`, `Network`, `Store`, `Tx` with empty repository interfaces;
  - `Canaries` (`NONE`) and the `CrashPoints` no-op.
- `PaymentStateMachine` and `OrderStateMachine`, driven by **data tables** that mirror DESIGN §4.3 and §4.4. Each decision returns `Applied | IgnoredStale | IgnoredDuplicate | Conflict` together with the rule id.
- ArchUnit determinism rule set (§11.11), active from day one, including Spring confinement (no `org.springframework` outside `app`).
- `.github/workflows/ci.yml` skeleton (build plus tests), with actions pinned by SHA.
- Fill in the CLAUDE.md "Commands" section.

**Prove**
- An exhaustive table test: every state × event × time order (before, equal, after) gives the result in DESIGN §4.3.
- **jqwik order-insensitivity property** (§4.5): random contradiction-free event multisets; any permutation with random duplicates gives the same final state. 10,000 tries.
- `Money` tests: parse edge cases (`"0.5"`, `"1.234"`, `"-1"`, `"1e3"`, overflow), exact arithmetic overflow.
- IST tests: 23:59:59.999 vs 00:00:00.000 IST; T+5 end-of-day instants.
- ArchUnit self-test: a fixture class that calls `Instant.now()` must fail the rule.

**Exit:** `./gradlew check` is green locally; CLAUDE.md commands are filled in.
**Carry-over:** — (committed as `fab4aa1`; the first CI run on GitHub was green). The design refinements are D22–D27.

---

## Shift 2: Ledger, PostgreSQL, Store contract (**M1 done**)

**Read:** DESIGN §7, §11.9, §12. **Needs:** Docker in WSL.

**Build**
- `core` ledger:
  - `AccountCode`, `LedgerTxn` (domain validation, layer 1);
  - `PostingRules` as pure functions for every cause in §7.2;
  - `LedgerRepo` port with an idempotent `post` (equality check on conflict).
- `sim`: `InMemoryStore` with a buffered write set and atomic commit. Abort discards the write set. It enforces uniqueness. It survives node "crashes". It has a `StoreUnavailable` switch.
- `store-postgres`:
  - Flyway `V1__ledger.sql`: accounts, transactions, entries, `xid8` columns, a **deferred constraint trigger** for the balance, the trigger blocking a foreign transaction, append-only triggers, the `driftline_migrator` and `driftline_app` roles and grants, and the audit_log table;
  - `PostgresStore` (Hikari, JDBC, no ORM);
  - both balance strategies: `ROW_LOCK` and `APPEND_SNAPSHOT` with the `xid8` horizon snapshotter.
- `core` testFixtures: an abstract `StoreContractTest` (ledger part), run by `InMemoryStoreContractTest` and `PostgresStoreContractIT`.
- An `integrationTest` test suite (Testcontainers `postgres:17`).

**Prove** ("ledger rejects any unbalanced transaction", at all three layers)
- Domain: rejects a non-zero sum, a single entry, a zero entry, mixed merchants, overflow.
- Both stores: duplicate cause gives a no-op; same cause with different entries gives a `LedgerConflict`.
- PostgreSQL via **raw SQL** as the app role: an unbalanced insert fails at COMMIT; an entry added to another transaction's txn fails; UPDATE and DELETE fail.
- Concurrency integration test: 16 threads post the same cause, and exactly 1 wins.
- Snapshot correctness integration test: a slow transaction holds a lower id; the snapshot must not skip it (the sequence-gap trap).
- Balance equivalence: random postings give the same balance under both strategies.

**Exit (M1):** all unit and contract tests are green on both stores; the three-layer rejection is proven. Status → M1 DONE.
**Carry-over:** — (committed as `6e5d480`; CI green on both jobs). The design details are D28 and D29. Naming: the Postgres side of the contract runs as `RowLockStoreContractIT` and `AppendSnapshotStoreContractIT` (one per balance strategy) instead of a single `PostgresStoreContractIT`. Evidence: `./gradlew check` ran 403 tests, 0 failed, including 33 Testcontainers tests on PostgreSQL 17.12.

---

## Shift 3: Simulator kernel and world models (M2, part 1)

**Read:** DESIGN §11.1–11.4, §11.12, §10.2, §2.3.

**Build**
- `sim` kernel: `SplitMix64` with stream derivation; `Scheduler` keyed by `(time, seq)`; nodes with incarnation numbers; `SimClock` (per-node offset); `SimNetwork` (delivery only for now); `SimLog` (partitions, offsets).
- `wire`: the webhook JSON codec, `HmacSigner` and `HmacVerifier` (replay window), and the order-event codec.
- World models (first cut): `FakeGateway` (payments, webhooks on a retry schedule, status API); `FakeMerchant` (orders, expiry, versions, event retries with the same key); `FakeBank` (accounts with conservation, per-payment settlement lines); the workload generator with scenario labels.
- `IngestNode`, which uses the real `wire` verify and normalise code, and a first `MatcherNode` running the real core matcher (dedupe, state machines, CAPTURE and ORDER_MATCH postings, offsets in the Store).
- Trace lines plus a SHA-256 digest; CLI `:sim:run --seed --profile --trace`.
- Invariant framework (incremental per-step checks plus end-state checks), starting with INV-LEDGER-BALANCED, -LEDGER-ONCE, -EVENT-ONCE, -TRANSITIONS-LEGAL, -ONE-OUTCOME and -WORLD-CONSERVED.

**Prove**
- Determinism: 1,000 seeds run twice give identical digests.
- 1,000 seeds pass under the `calm` profile.
- A unit test of the simulator itself: FakeBank conservation holds under random transfers.

**Exit:** `:sim:run` reproduces a run exactly from its seed; 1,000 `calm` seeds are green.
**Carry-over:** Everything on the Build and Prove lists is done; the design details are D30–D32. For Shift 4: the matcher counts a reused key (`KEY_REUSED`) but does not yet write a `quarantine` row; that belongs with the `MERCHANT_KEY_REUSE` fault. Evidence: `CalmCampaignTest` ran 1,000 seeds twice each, with 0 violations, identical digests, and 1,000 distinct digests; seed 1 gives digest `c20b4caf…` in separate JVMs; a clean, uncached check ran 421 unit and 40 PostgreSQL integration tests, 0 failed.

---

## Shift 4: Faults, invariants, canaries, 10k seeds in CI (**M2 done**)

**Read:** DESIGN §11.5–11.15, §5.1. **Needs:** the GitHub repo.

**Build**
- The fault injector with **swarm** vectors: `NET_DROP`, `NET_DUP`, `NET_DELAY`, `NODE_CRASH` and restart, `NODE_PAUSE` (zombie) with a partition coordinator, `CLOCK_SKEW` and `CLOCK_JUMP`, `STORE_DOWN`, `PRODUCE_AMBIGUOUS`, `MERCHANT_KEY_REUSE`.
- Partition epoch fencing in the core and both stores (INV-FENCED).
- The RUN → HEAL → DRAIN phases and the `pr` profile.
- Canary framework plus C01, C05, C06 and C09. A `:sim:canaries` task prints seeds-to-detection.
- `:sim:campaign` (parallel threads, sharding, base seed); failing-seed artifacts; `sim/regression-seeds.txt` replayed every run; a determinism double-run task.
- CI `simulation` job: a 4 × 2,500 matrix = **10,000 seeds**, plus the regression corpus, plus 100 determinism double-runs, plus canaries-quick.
- `docs/BUGS.md` goes live: every failure found while bringing faults online is triaged into it.

**Prove**
- 10,000 `pr` seeds are green locally (target < 5 min on 8 threads) **and** in CI on a real pull request.
- Each active canary is caught within 500 seeds.
- Each bug found has a BUGS.md row and a regression seed.

**Exit (M2):** a pull request on GitHub shows the 10k-seed job green. Status → M2 DONE.
**Carry-over:** — (committed as `b6a5dc6`, PR #1). The design details are D33–D37. M2 exit: on PR #1, GitHub Actions run 43709077412 went green on every job. The `simulation` matrix ran 10,000 distinct `pr` seeds (4 × 2,500), shard 0 replayed the regression corpus and found identical digests for 100 seeds across two JVMs, and `canaries-quick` passed (`docs/results/sim-campaign-pr-ci-run-43709077412.txt`). Local evidence: 10,000 `pr` seeds and 100 determinism double-runs passed in 114 s on 8 threads (`docs/results/sim-campaign-pr-10k.txt`); C01, C05, C06 and C09 were caught in 1, 1, 20 and 5 seeds, each by its intended invariant (`docs/results/canaries.md`); the full `./gradlew check` ran 447 unit and 49 PostgreSQL integration tests, 0 failed. No simulator-found bug, so BUGS.md and `sim/regression-seeds.txt` stay empty. Per-seed cost (D36): 20–32 ms in CI and about 90 ms locally, against the 20 ms target of §11.15.

---

## Shift 5: Matcher with watermarks and one-to-one breaks (M3, part 1, in the simulator)

**Read:** DESIGN §5, §6.1–6.3, §6.5, §6.6, §8.2 (detection side only).

**Build**
- Keyed state per partition (write-through cache, load on assignment, eviction of closed payments).
- The three-source join (§5.3).
- **Watermarks:** stream watermarks (bounded lateness plus idle advance on tick), the **settlement completeness watermark** (trailer validated, settlement calendar), and the `FEED_STALL` alert.
- **Timers** (`TimerRepo` in both stores), with deterministic firing order.
- Detection of DEBITED_NO_ORDER, LATE_SUCCESS, DUPLICATE_CAPTURE, MISSING_SETTLEMENT, ORPHAN_SETTLEMENT (unknown reference) and SHORT_SETTLEMENT (referenced); the action-owner rule; anomalies.
- `wire`: the settlement CSV parser (strict). **Research first:** read public gateway settlement-report docs and write `docs/formats.md` with citations.
- FakeBank: late, duplicate and conflicting files; missing, extra and short lines (referenced only); itemised fees.
- Oracle and invariants: INV-MISSING-IS-REAL and INV-DETECTED (one-to-one types). Canary C07.

**Prove**
- Unit tests per detection rule, including the late-evidence and self-heal paths.
- Watermark tests: a late file gives no false "missing"; an idle source still advances; a stalled feed raises an alert, not breaks.
- Parser tests: golden files plus malformed ones (count mismatch, total mismatch, 3 decimal places, unknown record type).
- 10,000 seeds are green; detection recall is 100% for one-to-one breaks at drain; C07 is caught.

**Exit:** all one-to-one breaks are detected **in the simulator**.
**Carry-over:** Build list done; the design details are D38–D45. Two simulator-found bugs: BUG-001 (seed 361, a quarantined merchant event left a duplicate-captured order un-escalated) and BUG-002 (seed 143, the settlement worker re-emitted a file it had refused), both fixed, both in `sim/regression-seeds.txt`. Settlement files are generated by `FakeGateway` and sent by `SettlementFeed`; `FakeBank` stays the world's account ledger. Evidence: 10,000 `pr` seeds, the regression corpus and 100 determinism double-runs passed in 240 s on 8 threads (`docs/results/sim-campaign-pr-10k-shift5.txt`). Recall at drain is 100%: the oracle expected 325,411 one-to-one breaks, 325,394 were raised exactly, and the other 17 belong to orders escalated as IDEMPOTENCY_CONFLICT (D41). C07 was caught in 13 seeds by INV-MISSING-IS-REAL, and C01, C05, C06 and C09 are still caught (`docs/results/canaries.md`). `./gradlew check`: 562 unit and 72 PostgreSQL integration tests, 0 failed. A `pr` seed costs 2.2× the CPU of Shift 4 (D44). **For later shifts:** Shift 6 stores raw upload bytes with the upload API; Shift 7 solves the unreferenced lines now held as AWAITING_BATCH and adds the "not a candidate of an unattributed line" clause to MISSING_SETTLEMENT; Shift 8 must take no automatic action on an order with an open IDEMPOTENCY_CONFLICT (D41), and its resolver turns the late-evidence keys (`order_paid_later`, `attributed_to`, `kept_by_order`) into SELF_HEALED.

---

## Shift 6: Ingest API, Kafka, settlement worker, Compose e2e (**M3 done**)

**Read:** DESIGN §3.3, §10, §5.1, §15.1–15.2, §14 (first two rows).

**Build**
- `app` (**Spring Boot**): explicit `@Configuration` wiring of `SystemClock`, `HttpNetwork` and `PostgresStore` into the core (no annotations in the core); `Canaries.NONE` enforced at startup; `DRIFTLINE_ROLE` mapped to Spring profiles; virtual threads; Actuator on management port 9091 (D21).
- **Ingest API** (Spring MVC): webhook HMAC checked on the raw `byte[]` body before JSON binding (D21), the replay window, order events with a required `Idempotency-Key`, **token scopes per merchant** (Spring Security filter plus `@PreAuthorize` plus the merchant-binding check), size limits, and the Kafka producer (§10.4).
- **Kafka partition driver:** plain kafka-clients inside a `SmartLifecycle` bean (D19); offsets in PostgreSQL, epoch bump and seek on assignment, one transaction per batch, fencing, graceful shutdown.
- Settlement upload plus the **separate settlement worker**.
- Ops read API: breaks, payment timeline.
- `store-postgres`: migrations for every table so far, with full parity in the contract suite.
- `sandbox`: the world models served over HTTP, plus a scenario injector CLI (`inject --type DUPLICATE_CAPTURE --count 5`).
- `deploy/compose`: the `core` profile (memory-capped, §15.1), health checks, and the migration service.
- `scripts/e2e.sh` plus a JUnit e2e suite; a CI `e2e-compose` job; a `claims-lint` job.

**Prove**
- Integration tests: two Kafka consumers, rebalance, and the zombie fenced; crash mid-batch, then a replay with no double effects (CP6, CP7).
- E2E in Compose: inject each one-to-one break type; each is **detected** and visible via the ops API within a time limit. Replayed webhooks are harmless. A bad signature gives 401. A stale timestamp is rejected. A cross-merchant token gives 403.

**Exit (M3):** "One-to-one breaks are detected end to end in Docker Compose" is green locally and in CI. Status → M3 DONE.
**Carry-over:** Build and Prove lists done locally; the design details are D46–D52. M3 still needs the `e2e-compose` CI job green on a pull request. Evidence: the Compose e2e passed 5 of 5 in two consecutive runs (`docs/results/e2e-compose-shift6.txt`): all 19 one-to-one breaks the Oracle expected for the six injected types were raised and visible through the ops API 0–3 s after each injection (D51: these times measure the idle rule on replayed history, not production lag), and 7 more DEBITED_NO_ORDER breaks were raised before the merchant's `order.paid` and already carried `order_paid_later` (D52). A replayed webhook changed nothing, a bad signature gave 401, a stale timestamp was rejected, and a cross-merchant token gave 403. `./gradlew check`: 604 unit tests and 89 Testcontainers tests (80 PostgreSQL, 9 app: two consumers with a rebalance, the zombie fenced at CP7, a crash mid-batch replayed at CP6, the settlement worker crash at CP8, an undecodable record, the migrate role, and the start-up smoke test), 0 failed. 10,000 `pr` seeds, the regression corpus and 100 determinism double-runs passed in 153 s on 8 threads, with break totals identical to Shift 5 (`docs/results/sim-campaign-pr-10k-shift6.txt`). The Testcontainers suites found two bugs the mocked tests could not: Spring's binder has no converter to `Optional<Path>`, so the app could not start with a bootstrap file (now a nullable `Path` read through `bootstrap()`), and a test helper read a result set without `next()`. Neither came from the simulator, so BUGS.md and `sim/regression-seeds.txt` are unchanged. **For later shifts:** Shift 8's resolver must treat a DEBITED_NO_ORDER with `order_paid_later` (and the other late-evidence keys) as SELF_HEALED before any refund. This matters in production as well as in the e2e suite, because a consumer catching up on a backlog in both sources raises such breaks (D52). D49's limit stands: a merchant onboarded or a secret rotated after start-up needs a restart.

> **Before Shift 7:** the finance-ops interview has to be done, and its notes saved.

---

## Shift 7: Batch settlement matching and its test corpus (M4, part 1)

**Read:** DESIGN §6.4, §6.5, §11.7 (the inferred and ambiguity invariants); the interview notes.

**Build**
- Candidate narrowing (conservative, watermark-gated).
- The solver: gcd reduction, the positive fast path and complement, meet-in-the-middle (sorted arrays), capped-count DP with witness reconstruction, and the operation budget.
- Multi-line forced-assignment propagation.
- `MATCHED_INFERRED`, `AMBIGUOUS` with two witnesses, challenge detection.
- FakeBank batch lines with and without references; per-payment fee rounding.
- Invariants INV-INFERRED-HONEST and INV-AMBIGUITY-WITNESSED. Canary C08.
- Apply what the interview taught (for example, the columns of the CSV export).

**Settlement case corpus:** `core/src/test/resources/settlement-cases/*.yaml`. Each case lists its candidates, line(s), fee schedule and **expected result**. All 35 cases are required.

| # | Case | # | Case |
|---|---|---|---|
| 1 | ref_single_exact | 19 | noref_gcd_reduction (all multiples of 100) |
| 2 | ref_batch_exact | 20 | noref_target_not_divisible_by_gcd → NONE |
| 3 | ref_short_itemised_fee_explains | 21 | noref_zero_amount_line → rejected and flagged |
| 4 | ref_short_itemised_gst_rounding_explains | 22 | noref_single_candidate_equal |
| 5 | ref_short_unitemised → escalate | 23 | noref_capture_at_ist_cutoff_boundary |
| 6 | ref_short_itemised_not_summing → escalate | 24 | noref_lookback_includes_previous_day |
| 7 | ref_over_settlement → anomaly | 25 | noref_excludes_already_matched |
| 8 | ref_unknown_reference → orphan | 26 | noref_excludes_failed_payments |
| 9 | ref_already_settled → SETTLEMENT_CONFLICT | 27 | multi_line_forced_propagation_resolves_both |
| 10 | noref_all_candidates_exact (fast path) | 28 | multi_line_propagation_leaves_ambiguous |
| 11 | noref_unique_subset_small | 29 | multi_line_jointly_infeasible → second line orphan |
| 12 | noref_unique_n40_mitm_boundary | 30 | noref_refund_netted → NO_SOLUTION, escalated |
| 13 | noref_n200_dp_after_gcd | 31 | mixed_file_ref_and_noref |
| 14 | noref_budget_exceeded → orphan(BUDGET) | 32 | duplicate_bank_lines_same_amount_same_day |
| 15 | noref_two_subsets_distinct_amounts → AMBIGUOUS | 33 | near_overflow_amounts (exact arithmetic) |
| 16 | noref_equal_amounts_₹499×k → AMBIGUOUS | 34 | watermark_gate_not_passed → deferred |
| 17 | noref_no_solution → orphan | 35 | late_payment_after_inferred_match → CHALLENGED |
| 18 | noref_no_candidates → orphan | | |

Plus about 6 **golden end-to-end files** (`*.csv`, parsed, matched, ledger postings asserted).

**Property tests (jqwik)**
- P1, solver equals a brute-force oracle (n ≤ 18, small value ranges to force collisions, negatives allowed for meet-in-the-middle).
- P2, permutation invariance.
- P3, scaling invariance.
- P4, a planted solution never gives NONE.
- P5, witnesses always verify.
- P6, a bigger budget never changes a non-BUDGET answer.

**Prove:** all 35 cases, the golden files and P1–P6 are green; 10,000 seeds are green; C08 is caught; the ambiguous lines show two witnesses in the API.
**Exit:** batch lines are matched exactly or reported as ambiguous or unresolved, never guessed.

---

## Shift 8: Resolver, outbox, dispatcher, crash tests (**M4 done**)

**Read:** DESIGN §8, §9, §2.

**Build**
- The break state machine (§8.1, as a data table); playbooks for all six types plus anomalies; `config/deadlines.yaml` (§8.3); escalate_by, compensation calculation, `SELF_HEALED`, `LINKED`; manual resolve API with an audit row.
- Outbox and dispatcher (sans-I/O): `SKIP LOCKED` claims, leases, deterministic backoff, payment-scoped refund keys, **TTL-aware `REFUND_LOOKUP`**, results as facts, `PUBLISH_OBSERVATION`, the pending-order probe path.
- FakeGateway: `CALL_AMBIGUOUS`, key TTL, `STATUS_STALE`, `WEBHOOK_LOST`. FakeMerchant: the confirm API.
- Invariants INV-REFUND-ONCE, -NO-REFUND-OF-FULFILLED, -DEADLINE, -BOOKS-MATCH-WORLD, -CUSTOMER-WHOLE. Canaries C02, C03, C04, C10, C11.
- Crash points CP1–CP8 wired to the `CrashPoints` hook.
- `scripts/chaos.sh`: a Compose run that kills the matcher and dispatcher containers at random during traffic, then `driftline verify` (SQL checks plus a sandbox refund count).

**Prove**
- Simulator: each crash point is forced on purpose in dedicated seeds; 10,000 seeds are green; **all 11 canaries** are caught.
- Testcontainers `OutboxCrashIT`, one test per crash point (CP1–CP5) with real PostgreSQL and the sandbox gateway: **exactly one refund** at the gateway and **exactly one** REFUND ledger transaction.
- E2E in Compose: all six types are **handled** (refund, fulfil, post fees, suspense, escalate); the ambiguous line is reported as ambiguous; the chaos run verifies clean.
- The per-type auto-resolution share is computed from the simulation campaign (it goes to `docs/results/`).

**Exit (M4):** "All six break types handled; ambiguous matches reported as such." Status → M4 DONE.

---

## Shift 9: Observability, security, Kubernetes (M5, part 1)

**Read:** DESIGN §13, §14, §15.3, §16.

**Build**
- The OpenTelemetry agent in the image, manual spans with links, `traceparent` in Kafka headers; the metrics in §13; JSON logs with trace ids.
- `deploy/grafana` (a provisioned dashboard), `deploy/prometheus` (scrape config and alert rules), and the Compose `obs` profile.
- Security: AES-GCM field encryption with a blind index and `kid` rotation; the masking converter plus a test that scans logs for PII; the audit hash chain plus `verify-audit`; webhook secret rotation; the CSV export with injection-safe escaping.
- CI `security` job: dependency-review, OSV-Scanner, gitleaks, Trivy (nightly); `dependabot.yml`.
- `docs/THREAT_MODEL.md`.
- `deploy/k8s` with kustomize (base plus the `kind` overlay; §15.3); `scripts/kind-smoke.sh`; `nightly.yml`.

**Prove**
- The dashboard shows live data during a Compose load run (screenshot saved in `docs/results/`).
- One trace spans ingest → Kafka → matcher → resolver → dispatcher.
- Every row of DESIGN §14 has a passing test or CI job.
- The kind smoke test passes in CI (nightly or a manual trigger).

**Exit:** the observability and security checklist is complete; the manifests deploy.

---

## Shift 10: Load test, hot accounts, results, README (**M5 done**)

**Read:** DESIGN §17, §7.5, §2; the whole REQUIREMENTS.md.

**Build**
- `bench`: an open-model load generator (signed webhooks plus order events, step-rate) and scripts that save Prometheus queries to `docs/results/load-*.json`.
- The hot-account benchmark: `ROW_LOCK` vs `APPEND_SNAPSHOT` × concurrency {1, 4, 16, 64}, giving `docs/results/hot-accounts.csv` and a chart.
- The final simulation campaign (≥ 1,000,000 seeds across nightly runs and local runs), the canary table, the cumulative seed count, and the per-type auto-resolution shares.
- `scripts/render-results` fills in the README tables.
- **README:** the problem; the architecture diagram; **Guarantees and limits** (copied from DESIGN §2); the numbers; simulation and canaries; bug log highlights; security; who would pay (with interview insights); prior art; how to run.
- Stretch, only if time remains: seed shrinking.
- A final pass over the traceability matrix below: every row needs evidence. Tag `v1.0`.

**Prove:** every README number can be traced to a file in `docs/results/` and the script that produced it.
**Exit (M5):** "README carries throughput, latency and simulation results." Status → M5 DONE.

---

## Traceability: requirement → shift → evidence

| # | Requirement (REQUIREMENTS.md) | Shift | Evidence |
|---|---|---|---|
| R1 | Watch three records (orders, gateway, bank file) | 3, 5, 6 | world models; join; e2e |
| R2 | Streaming: a break is found minutes after it happens | 5, 6, 10 | `driftline_detection_lag_seconds`; README |
| R3 | Six break types with their automatic actions | 5, 7, 8 | detection tests; playbooks; e2e |
| R4 | REST ingest; HMAC; idempotency key on every write; separate settlement worker | 6 | API integration tests; e2e |
| R5 | Kafka, partitioned by merchant | 6 | partition driver integration tests |
| R6 | Matcher in Java without Flink; keyed state; 3-way join; event-time watermarks | 5, 6 | watermark tests; simulation |
| R7 | Double-entry ledger in PostgreSQL; append-only; every transaction sums to 0 | 2 | three-layer rejection tests |
| R8 | Resolver state machine; outbox; never refund twice or lose a refund | 8 | crash-point simulation plus `OutboxCrashIT` |
| R9 | Fake bank, gateway, network and clock, one seed | 3, 4 | kernel; determinism double-run |
| R10 | OTel, Prometheus, Grafana, Compose, K8s, GitHub Actions | 1, 6, 9 | workflows; compose; dashboard; kind smoke |
| R11 | Explicit legal transitions; dedupe table | 1, 3 | table tests; order-insensitivity property; INV-EVENT-ONCE |
| R12 | Subset-sum; narrow by merchant, date and batch; exact paise; ambiguous; never guess | 7 | 35-case corpus; P1–P6; INV-AMBIGUITY-WITNESSED |
| R13 | "Missing" only after the due date in event time | 5 | settlement watermark; INV-MISSING-IS-REAL; C07 |
| R14 | Outbox + idempotency keys + ledger uniqueness; three layers | 2, 8 | DESIGN §9.3 matrix; C02, C03, C04, C05, C11 |
| R15 | Hot accounts: row locking vs append-only with snapshots, both measured | 2, 10 | `hot-accounts.csv` |
| R16 | Clock, Network, Store interfaces; real in production, fakes in the simulator | 1, 2, 3, 6 | ports; ArchUnit; composition roots |
| R17 | Seeded single-threaded scheduler: deliver, drop, duplicate, delay, crash, jump clock | 3, 4 | fault catalogue |
| R18 | Invariants after every step (money conserved, one final state, no double refund, deadlines) | 3, 4, 8 | §11.7 invariants |
| R19 | A violation prints the seed; the seed reproduces | 3, 4 | REPRO line; regression corpus |
| R20 | State the in-memory store limit; PostgreSQL via Testcontainers | 2, 10 | contract suite; README limits |
| R21 | Bug log | 4–10 | `docs/BUGS.md` |
| R22 | Security list, written in the README | 6, 9, 10 | §14 table tests; README |
| R23 | M1–M5 "done when" criteria | 2, 4, 6, 8, 10 | Status table |
| R24 | Numbers to report (events/s, p99, auto share, seeds, bugs) | 8, 10 | `docs/results/`; README |
| R25 | Interview a finance-ops person before M4; who would pay | before 7, 10 | interview notes; README |
| R26 | Prior art stated; data from the simulator; formats modelled on public docs | 5, 10 | `docs/formats.md`; README |
| R27 | RBI T+1 / T+5 and ₹100 per day | 8 | `deadlines.yaml`; compensation tests |

## Reviewer concerns: how each is closed

| Concern | Closed by | Proof the reviewer can check |
|---|---|---|
| "Kafka + PostgreSQL + Kubernetes + simulation is a large scope" | Simulator first; one image and one topic; K8s is manifests plus a kind smoke test, not a production cluster; time-boxed shifts; explicit cut and never-cut lists (DESIGN §20) | This plan; the Carry-over lines; the cut list honoured |
| "Don't claim exactly-once processing merely because you use Kafka" | DESIGN §2: at-least-once delivery, effectively-once *effects*, the mechanism per effect, non-claims, the failure model; offsets in PostgreSQL rather than Kafka EOS; **CI `claims-lint`** | The README Guarantees section; the green `claims-lint` job |
| "Demonstrate idempotent effects, crash recovery and ledger invariants" | 17 simulator invariants including world-oracle checks; crash points CP1–CP8 in the simulator **and** with real PostgreSQL (`OutboxCrashIT`); the Compose chaos run; DB-level ledger enforcement tested with raw SQL; 11 canaries proving the checks bite | CI jobs; `docs/results/canaries.md`; `docs/BUGS.md` |
| "Settlement matching needs real test cases, not just an algorithm" | A 35-case YAML corpus with expected outcomes; golden CSV files run end to end; 6 properties against a brute-force oracle; simulator ground-truth checks on inferred and ambiguous results | `settlement-cases/`; the jqwik reports; INV-INFERRED-HONEST |
