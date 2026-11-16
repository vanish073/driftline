# Canary sensitivity (DESIGN §11.10)

Seeds to first detection per planted bug, `pr` profile, seeds from 1, budget 500. A canary counts only when an expected invariant trips (D37).

- Command: `./gradlew -q :sim:canaries --args="--threads 8 --out ../docs/results/canaries.md"` (then this header was added)
- Base commit: d482bda plus the uncommitted Shift 7 tree; date: 2026-11-16 (UTC)
- Active so far: C01, C05, C06, C07, C08, C09. C02, C03, C04, C10 and C11 arrive with the resolver and dispatcher (Shift 8).
- C08 (Shift 7) counts as caught by INV-INFERRED-HONEST or INV-MISSING-IS-REAL: a wrong inference leaves the payment really in the line unmatched, so its MISSING_SETTLEMENT would be false (D57). Here it was caught by INV-INFERRED-HONEST: in seed 3 the inferred subset and the world's differ by one payment each (pay_35_a for pay_104_a), so the two have the same net.
- C06 moved from 22 to 117 seeds (still within the 500-seed budget) because Shift 7 changed what every `pr` seed does: catalogue prices and batch days give every seed a different world (D57). The Shift 4 and 5 tables are in git history.

| Canary | Planted bug | Seeds to detection | First failing seed | Caught by | Expected |
|---|---|---|---|---|---|
| C01 `DEDUPE_OFF` | skip the dedupe insert | 1 | 1 | INV-EVENT-ONCE | INV-EVENT-ONCE, INV-LEDGER-ONCE |
| C05 `LEDGER_UNIQUE_OFF` | post without the cause uniqueness check | 1 | 1 | INV-LEDGER-ONCE | INV-LEDGER-ONCE |
| C06 `SUCCESS_TO_FAILED` | allow SUCCEEDED to FAILED | 117 | 117 | INV-TRANSITIONS-LEGAL | INV-TRANSITIONS-LEGAL, INV-ONE-OUTCOME |
| C07 `PROCESSING_TIME_WATERMARK` | treat missing by wall clock | 13 | 13 | INV-MISSING-IS-REAL | INV-MISSING-IS-REAL |
| C08 `FIRST_SOLUTION_WINS` | subset-sum returns the first fit, with no ambiguity check | 3 | 3 | INV-INFERRED-HONEST | INV-INFERRED-HONEST, INV-MISSING-IS-REAL |
| C09 `NO_FENCING` | commit without the epoch check | 3 | 3 | INV-FENCED | INV-FENCED, INV-LEDGER-ONCE |
