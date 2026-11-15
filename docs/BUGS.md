# Simulator bug log

Every real bug found by the deterministic simulator (or by property and integration tests run because of a simulator finding) goes here.
Canary detections are **not** bugs. They go in `docs/results/canaries.md`.
Each row's seed is also added to `sim/regression-seeds.txt`, which every CI run replays.

| # | Date | Shift | Seed · profile · commit | Invariant violated | Symptom | Root cause | Fix (commit) | Would a unit test have caught it? |
|---|---|---|---|---|---|---|---|---|
| — | — | — | — | — | *(no entries yet: no simulator-found bug through Shift 4's 10,000-seed `pr` campaign)* | — | — | — |

## Entry template (detailed write-up below the table)

```
### BUG-NNN: <one-line title>
- Found: <date>, Shift <n>, seed <seed>, profile <profile>, commit <sha>, swarm <vector>
- Repro: ./gradlew :sim:run --args="--seed <seed> --profile <profile>"
- Invariant: INV-...
- What happened (trace excerpt, 10–30 lines):
- Root cause:
- Fix:
- Why ordinary tests missed it:
- Regression: seed added to sim/regression-seeds.txt; unit or property test added: <name>
```

## Hunting targets (subtle areas to aim the simulator at; these are *not* bugs)

- Lease expiry while a dispatcher is paused, followed by a double send. Is it absorbed by the idempotency key?
- A refund retried after the gateway key TTL. Does `REFUND_LOOKUP` always run first?
- A zombie matcher waking after reassignment. Is the commit fenced?
- A settlement file one day late vs a payment really missing (`swm` vs the stall alert).
- A capture at 23:59:59.999 IST vs 00:00 IST (T date and batch date).
- A `refund.processed` webhook arriving *before* our own refund observation (layer 3).
- A late success arriving after its order expired, racing a merchant confirm.
- Two unreferenced lines in one file with overlapping candidate sets.
- A snapshotter folding entries around an in-flight transaction (the `xid8` horizon).
- Webhook retries re-signed with a fresh timestamp under node clock skew (the replay window).
