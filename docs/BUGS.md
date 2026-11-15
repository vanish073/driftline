# Simulator bug log

Every real bug found by the deterministic simulator (or by property and integration tests run because of a simulator finding) goes here.
Canary detections are **not** bugs. They go in `docs/results/canaries.md`.
Each row's seed is also added to `sim/regression-seeds.txt`, which every CI run replays.

| # | Date | Shift | Seed · profile · commit | Invariant violated | Symptom | Root cause | Fix (commit) | Would a unit test have caught it? |
|---|---|---|---|---|---|---|---|---|
| BUG-001 | 2026-11-15 | 5 | 361 · `pr` · 64a5874 + Shift 5 work | INV-DETECTED | The world implied DUPLICATE_CAPTURE for `pay_46_a`; Driftline flagged `pay_46_b`, the payment the merchant had actually paid and fulfilled the order with, and escalated nothing | A merchant client reusing idempotency keys made Driftline quarantine `order.paid` and `order.fulfilled`. The matcher never learned which payment paid the order, kept the earliest capture, and the quarantine row was not tied to the order, so nothing marked the order's breaks as untrustworthy | Quarantining a record now also raises an IDEMPOTENCY_CONFLICT anomaly naming its order and payment (DESIGN §10.3, §8.2), and INV-DETECTED accepts that escalation for the order's DEBITED_NO_ORDER and DUPLICATE_CAPTURE (D41) · Shift 5 commit | No: it needs a reused key to land on exactly the event that says which payment paid a duplicate-captured order |
| BUG-002 | 2026-11-15 | 5 | 143 · `pr` · 64a5874 + Shift 5 work | none at first (a stats mismatch: 1,810 FILE_CONFLICT breaks for 1,797 refused uploads in 1,000 seeds); now INV-NO-ANOMALY | The settlement worker emitted a file it had refused as a conflict; the matcher's own guard caught its lines and raised FILE_CONFLICT | A network duplicate of a refused upload is a DUPLICATE receipt, and the worker re-emitted every duplicate without checking that the stored receipt was ACCEPTED | A duplicate of a REJECTED_CONFLICT receipt is treated as a conflict and never emitted; a FILE_CONFLICT raised by the matcher (not the worker) is now an INV-NO-ANOMALY violation, verified to fail on seed 143 without the fix · Shift 5 commit | Yes, once written: `SettlementWorkerNodeTest` now covers it. Nobody had thought to upload a refused file twice |

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

### BUG-001: a quarantined merchant event left a duplicate-captured order's breaks un-escalated
- Found: 2026-11-15, Shift 5, seed 361, profile `pr`, commit 64a5874 plus the Shift 5 work, swarm includes `MERCHANT_KEY_REUSE`
- Repro (before the fix): `./gradlew :sim:run --args="--seed 361 --profile pr"`
- Invariant: INV-DETECTED
- What happened (trace excerpt):
  ```
  000057220431 gateway payment.succeeded pay_46_a 2782.97
  000057222967 gateway payment.succeeded pay_46_b 2782.97
  000057223099 merchant:m_1 order.paid ord_46 v3 REUSING key xthuztjvtwsdr62rth3ksgscce
  000057283099 merchant:m_1 order.fulfilled ord_46 v4 REUSING key xthuztjvtwsdr62rth3ksgscce
  000057633250 matcher-b p1 tick timers=1 raised=DUPLICATE_CAPTURE(pay_46_b)
  000058110714 matcher-b p1 tick timers=2 raised=DEBITED_NO_ORDER(pay_46_a) DEBITED_NO_ORDER(pay_46_b)
  000770400000 kernel VIOLATION INV-DETECTED: the world implies DUPLICATE_CAPTURE m_1/pay_46_a, but Driftline never raised it
  ```
- Root cause: both merchant events that named `pay_46_b` arrived under an already-used key with a new body, so they were quarantined (correctly). Driftline's view of the order stayed "pending", DUP_CHECK kept the earliest capture, and the break pointed at the payment the merchant had fulfilled. Shift 8's playbook would have refunded it. The quarantine row existed, but nothing connected it to the order.
- Fix: the matcher raises an IDEMPOTENCY_CONFLICT anomaly for each quarantined record, with the order and payment it was about, so the order is escalated (DESIGN §8.2: anomalies are never auto-resolved). INV-DETECTED accepts that escalation in place of the exact DEBITED_NO_ORDER or DUPLICATE_CAPTURE for that order. Shift 8 must not act automatically on an order with an open IDEMPOTENCY_CONFLICT (carried over).
- Why ordinary tests missed it: the key reuse has to hit exactly the event that decides which of two captures is kept.
- Regression: seed 361 added to `sim/regression-seeds.txt`; `MatcherTest.aKeyReusedWithADifferentBodyIsQuarantinedAndNotApplied` asserts the anomaly and its order id.

### BUG-002: the settlement worker re-emitted a file it had refused
- Found: 2026-11-15, Shift 5, seed 143, profile `pr`, commit 64a5874 plus the Shift 5 work, swarm `NET_DUP=34‰ FILE_DUP=285‰ FILE_CONFLICT=234‰`
- Repro (before the fix): `./gradlew :sim:run --args="--seed 143 --profile pr --trace"`
- Invariant: none at first. A 1,000-seed campaign showed 1,810 FILE_CONFLICT breaks for 1,797 refused uploads. After the fix, INV-NO-ANOMALY fails on a matcher-raised FILE_CONFLICT; verified to fail on seed 143 with the fix reverted.
- What happened (trace excerpt):
  ```
  000129600656 settlement-worker 202 CONFLICT stl_m_1_20261114 34ffc1a93f8b
  000129600727 settlement-worker 202 DUPLICATE stl_m_1_20261114 lines=13
  000129600777 matcher-a p0 epoch 3 batch @579 applied=14 ... raised=FILE_CONFLICT(stl_m_1_20261114)
  ```
- Root cause: the worker re-emits every DUPLICATE receipt (correct for an accepted file whose earlier emission may not have finished), without checking whether the stored receipt was REJECTED_CONFLICT. A network duplicate of the refused upload therefore reached the log. The matcher's guard (a second file with the same file id) stopped it from settling anything twice.
- Fix: a DUPLICATE of a REJECTED_CONFLICT receipt is answered like a conflict and never emitted.
- Why ordinary tests missed it: it needs a conflicting upload and a duplicate delivery of that same upload.
- Regression: seed 143 added to `sim/regression-seeds.txt`; `SettlementWorkerNodeTest.aConflictingFileAndEveryLaterCopyOfItNeverReachTheLog`.

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
