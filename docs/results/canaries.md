# Canary sensitivity (DESIGN §11.10)

Seeds to first detection per planted bug, `pr` profile, seeds from 1, budget 500. A canary counts only when an expected invariant trips (D37).

- Command: `./gradlew -q :sim:canaries --args="--threads 8 --out ../docs/results/canaries.md"` (then this header was added)
- Base commit: 64a5874 plus the uncommitted Shift 5 tree; date: 2026-11-15
- Active so far: C01, C05, C06, C07, C09. C08 arrives with the subset-sum solver (Shift 7); C02, C03, C04, C10 and C11 with the resolver and dispatcher (Shift 8).
- The Shift 4 table (C01, C05, C06, C09 at 1, 1, 20 and 5 seeds) changed for C06 (20 to 22) and C09 (5 to 3) because Shift 5 changed what every `pr` seed does: settlement files, the worker node and three file faults.

| Canary | Planted bug | Seeds to detection | First failing seed | Caught by | Expected |
|---|---|---|---|---|---|
| C01 `DEDUPE_OFF` | skip the dedupe insert | 1 | 1 | INV-EVENT-ONCE | INV-EVENT-ONCE, INV-LEDGER-ONCE |
| C05 `LEDGER_UNIQUE_OFF` | post without the cause uniqueness check | 1 | 1 | INV-LEDGER-ONCE | INV-LEDGER-ONCE |
| C06 `SUCCESS_TO_FAILED` | allow SUCCEEDED to FAILED | 22 | 22 | INV-TRANSITIONS-LEGAL | INV-TRANSITIONS-LEGAL, INV-ONE-OUTCOME |
| C07 `PROCESSING_TIME_WATERMARK` | treat missing by wall clock | 13 | 13 | INV-MISSING-IS-REAL | INV-MISSING-IS-REAL |
| C09 `NO_FENCING` | commit without the epoch check | 3 | 3 | INV-FENCED | INV-FENCED, INV-LEDGER-ONCE |
