# Canary sensitivity (DESIGN §11.10)

Seeds to first detection per planted bug, `pr` profile, seeds from 1, budget 500. A canary counts only when an expected invariant trips (D37).

- Command: `./gradlew -q :sim:canaries --args="--threads 8 --out ../docs/results/canaries.md"` (then this header was added)
- Base commit: 5cb9e54 plus the uncommitted Shift 4 tree; date: 2026-11-15; wall time 13.6 s on 8 threads
- Active so far: C01, C05, C06, C09. The other seven arrive with the code they plant a bug in (Shifts 5, 7, 8).

| Canary | Planted bug | Seeds to detection | First failing seed | Caught by | Expected |
|---|---|---|---|---|---|
| C01 `DEDUPE_OFF` | skip the dedupe insert | 1 | 1 | INV-EVENT-ONCE | INV-EVENT-ONCE, INV-LEDGER-ONCE |
| C05 `LEDGER_UNIQUE_OFF` | post without the cause uniqueness check | 1 | 1 | INV-LEDGER-ONCE | INV-LEDGER-ONCE |
| C06 `SUCCESS_TO_FAILED` | allow SUCCEEDED to FAILED | 20 | 20 | INV-TRANSITIONS-LEGAL | INV-TRANSITIONS-LEGAL, INV-ONE-OUTCOME |
| C09 `NO_FENCING` | commit without the epoch check | 5 | 5 | INV-FENCED | INV-FENCED, INV-LEDGER-ONCE |
