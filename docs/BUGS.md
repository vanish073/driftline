# Simulator bug log

Every real bug found by the deterministic simulator (or by property and integration tests run because of a simulator finding) goes here.
Canary detections are **not** bugs. They go in `docs/results/canaries.md`.
Each row's seed is also added to `sim/regression-seeds.txt`, which every CI run replays.

| # | Date | Shift | Seed · profile · commit | Invariant violated | Symptom | Root cause | Fix (commit) | Would a unit test have caught it? |
|---|---|---|---|---|---|---|---|---|
| BUG-001 | 2026-11-15 | 5 | 361 · `pr` · 64a5874 + Shift 5 work | INV-DETECTED | The world implied DUPLICATE_CAPTURE for `pay_46_a`; Driftline flagged `pay_46_b`, the payment the merchant had actually paid and fulfilled the order with, and escalated nothing | A merchant client reusing idempotency keys made Driftline quarantine `order.paid` and `order.fulfilled`. The matcher never learned which payment paid the order, kept the earliest capture, and the quarantine row was not tied to the order, so nothing marked the order's breaks as untrustworthy | Quarantining a record now also raises an IDEMPOTENCY_CONFLICT anomaly naming its order and payment (DESIGN §10.3, §8.2), and INV-DETECTED accepts that escalation for the order's DEBITED_NO_ORDER and DUPLICATE_CAPTURE (D41) · Shift 5 commit | No: it needs a reused key to land on exactly the event that says which payment paid a duplicate-captured order |
| BUG-002 | 2026-11-15 | 5 | 143 · `pr` · 64a5874 + Shift 5 work | none at first (a stats mismatch: 1,810 FILE_CONFLICT breaks for 1,797 refused uploads in 1,000 seeds); now INV-NO-ANOMALY | The settlement worker emitted a file it had refused as a conflict; the matcher's own guard caught its lines and raised FILE_CONFLICT | A network duplicate of a refused upload is a DUPLICATE receipt, and the worker re-emitted every duplicate without checking that the stored receipt was ACCEPTED | A duplicate of a REJECTED_CONFLICT receipt is treated as a conflict and never emitted; a FILE_CONFLICT raised by the matcher (not the worker) is now an INV-NO-ANOMALY violation, verified to fail on seed 143 without the fix · Shift 5 commit | Yes, once written: `SettlementWorkerNodeTest` now covers it. Nobody had thought to upload a refused file twice |
| BUG-003 | 2026-11-15 | 6 | 437715671286076 · `pr` · c8aeab0 (also fails there; found by PR #3's CI, run 43771567128) | INV-DRAINED | Partition 0 stopped at offset 230 of 306 and never moved again, though the coordinator still assigned it to a live matcher | Both matchers paused together and both expired. matcher-a came back first, rejoined and acquired p0 (epoch 2). matcher-b then woke and ran an assignment of p0 delivered to its expired member id, bumping the epoch to 3 before its heartbeat told it to rejoin. matcher-a, the rightful owner, was fenced on its next commit, dropped p0, and the simulator's matcher node had no rule to take it back. The production driver already has that rule (D50) | The simulator's matcher node now acquires a fenced partition again when the coordinator still assigns it to the same member after three session timeouts, as `PartitionDriver` does (D53). Verified: the seed fails without the fix (on `main` and on the branch) and passes with it, resuming at offset 230 under epoch 4 · Shift 6 commit | No: it needs two pauses that overlap, end in a particular order, and straddle an assignment. The regression seed covers the simulator, and a new `PartitionDriverIT` test covers the production rule, which had no test |
| BUG-004 | 2026-11-16 | 8 | 5 · `pr` · 03b0654 + Shift 8 phase 4 work (seed 21 fails the same way) | none: the resolver threw (SIM-STEP-THREW) | `ACTION_PENDING -> SELF_HEALED is not in the lifecycle` for a LATE_SUCCESS break, and the resolver stopped | The resolver asked the merchant to confirm a late success (ACTION_PENDING). The merchant's own `order.paid` with that payment reached the matcher before the merchant's answer. On the next review the playbook saw the order paid with this payment and, with no accepted confirmation on record, closed the break SELF_HEALED. D60 leaves that move out on purpose: once Driftline has acted, the fix is not the world's alone | A break that has acted and finds its order paid with this payment closes RESOLVED_AUTO ("paid while a confirmation was pending"). The fake merchant also answers a confirmation of the payment its order is already paid with as ACCEPTED (an idempotent no-op), as a real merchant API would. Verified: seeds 5 and 21 fail with the fix reverted and pass with it · Shift 8 phase 4 commit | Yes, and it should have: the playbook's tests covered "accepted, then paid" and "rejected", not "paid before any answer". `ResolverTest.aLateSuccessPaidWhileTheMerchantConfirmIsPendingResolvesAutomatically` now does |
| BUG-005 | 2026-11-16 | 8 | 7 · `pr` · 03b0654 + Shift 8 phase 4 work (seeds 17 and 35 fail the same way) | INV-CUSTOMER-WHOLE | A duplicate capture was never refunded: `m_0/pay_46_b was debited, but its order was not fulfilled with it, it was not refunded, and no break about it was escalated` | Every webhook of the second payment was lost (WEBHOOK_LOST) and the order was paid with the first, so the pending-order probe rightly skipped the order (D61). The payment's only trace was a referenced settlement line, raised as ORPHAN_SETTLEMENT (UNKNOWN_REFERENCE). The orphan playbook only waited for attribution and escalated the line after 4 h, though the line named the payment and the gateway could have been asked | The orphan playbook asks the gateway about the line's reference (STATUS_QUERY_PAYMENT, re-asked while the answer is not final, D64). The dispatcher publishes the outcome as an observation, the matcher attributes the line and raises the DUPLICATE_CAPTURE, and the resolver refunds it. Verified: seeds 7, 17 and 35 fail with the query disabled and pass with it · Shift 8 phase 4 commit | No: it needs a payment whose webhooks are all lost, on an order already paid with another payment. `ResolverTest.anOrphanNamingAPaymentAsksTheGatewayAboutItAndAsksAgainWhileTheAnswerIsNotFinal` now covers the playbook step |
| BUG-006 | 2026-11-16 | 8 | 849 · `pr` · found while building phase 4; re-found on the phase 6 tree by switching the fix off | INV-INFERRED-HONEST | A batch line was inferred to pay a set that differed from the world's by one payment with the same net | WEBHOOK_LOST (Shift 8) means Driftline can know a payment only as PENDING while the world captured it. Batch matching proved uniqueness over the payments Driftline saw succeed, so a line holding a lost capture could be inferred to the wrong set. The pending-order probe skipped any order already paid, so a second capture on it whose webhooks were all lost stayed unknown | Payments Driftline still sees as PENDING are possible members of every unreferenced line: an inference must stay unique with them added, or the line is AMBIGUOUS with both solutions as witnesses (D69). The probe also runs for a paid order that still has a PENDING payment, so its outcome is published · Shift 8 phase 4 commit `9167337`. **This row and its seed were not written in phase 4**; with both halves switched off on the phase 6 tree, 4 of 2,000 `pr` seeds fail (546, 849, 1105, 1696); either half alone passes all 2,000, because the probe's observation also tells Driftline about the lost capture before the line is solved | Not without knowing what to ask: it needs a capture whose success webhook alone was lost |
| BUG-007 | 2026-11-16 | 8 | 2490 and 2293 · `pr` · 1cafc6b (also 9167337); found by the first Shift 8 10,000-seed campaign | INV-AMBIGUITY-WITNESSED | 2490: a witness names `pay_37_a`, "outside the line's window". 2293: a witness names `pay_99_a`, "not a distinct captured (or possibly captured) payment" | BUG-006's possible members had two gaps. A pending payment was placed inside every line's window although Driftline had its first event, which bounds its capture from below: `pay_37_a` went pending on 14 November, so it could not be in a line for captures before 14 November. And a line found AMBIGUOUS was never solved again: when `pay_99_a`'s failure arrived, the line kept a witness that no longer could be | A pending payment whose first event's batch date is after a file's date is not a possible member of its lines. When a payment leaves PENDING (or turns conflicted), every AMBIGUOUS line naming it in a witness goes back to AWAITING_BATCH and is solved again in the same transaction. INV-AMBIGUITY-WITNESSED now judges "possibly captured" by what Driftline knows (last status PENDING, first seen in time), not by whether the world captured it: 4 of the 10 failing seeds were that check being too strict, and pass with the invariant change alone; 6 fail without the matcher change · Shift 8 phase 6 commit | Yes, once written: `PossibleMembersTest` has one case per gap, and both fail with the matcher change reverted |
| BUG-008 | 2026-11-16 | 8 | 8551 · `pr` · 1cafc6b (also 9167337); found by the same campaign | INV-REFUND-ONCE | `m_1/pay_80_a was refunded 2 times`, both times with the same idempotency key | The first refund's reply was lost (CALL_AMBIGUOUS) while the dispatcher's clock ran 11 minutes fast (CLOCK_JUMP), so its first send was stamped 11 minutes late. The retry came 11.8 minutes after the real send, past the gateway's 5-minute key TTL, but by the stamp the refund looked 42 seconds old, so the TTL-aware rule (§9.2 step 6) skipped the lookup and resent; the gateway had forgotten the key and refunded again | Every REFUND retry sends a REFUND_LOOKUP first, whatever its age (D70). That needs no clock: if the gateway has forgotten the key, the refund is older than the TTL and far past the lookup's lag, so the lookup shows it; if the key is known, the resend is deduplicated. `gatewayKeyTtl` and `lookupMargin` leave the dispatcher's configuration · Shift 8 phase 6 commit | Not as written: the dispatcher's tests used one clock. `DispatcherTest.aRetryLooksUpFirstEvenWhenTheFirstSendWasStampedByAFastClock` now covers it |
| BUG-009 | 2026-11-16 | 8 | 1298 · `pr` · phase 6 tree, before this fix (seed 1643 fails the same way) | INV-NO-REFUND-OF-FULFILLED | `m_1/pay_60_b was refunded, and its order was fulfilled with it`; the order's other payment was refunded too | The DUPLICATE_CAPTURE playbook refunded the payment the detector did not keep. Until the merchant's `order.paid` arrives, the detector keeps the earliest payment (§5.6), so that choice is provisional. Here both payments succeeded, the later one was refunded at once, and then the merchant paid the order with it; the earlier one then became the duplicate and was refunded as well | A duplicate is refunded only once the order is paid with another payment, or has expired or been cancelled. While the order awaits payment the break waits for the merchant (order events wake it) until escalate_by; an order the merchant never reported is escalated (D15). D71 · Shift 8 phase 6 commit | Yes, and it should have: the playbook's test paid the order before deciding. `ResolverTest.aDuplicateIsNotRefundedUntilTheMerchantPaysTheOrderWithTheOtherPayment` fails without the fix |
| BUG-010 | 2026-11-17 | 8 | 439901216016483 · `pr` · becfc9d; found by PR #5's second CI run (run 43990121601, shard 2) | INV-INFERRED-HONEST | `stl_m_1_20261114:4 was inferred to pay [pay_12_a, pay_32_a, pay_9_a], but the world paid [pay_12_a, pay_32_a, pay_9_b]` | Order `ord_9` was paid twice at 2270.60. Every webhook of `pay_9_b` was lost and the merchant paid the order with `pay_9_a`, so Driftline had no trace of `pay_9_b`: the pending-order probe skips a paid order with nothing pending (D61), and the line carried no reference to ask about (D64). The world settled `pay_9_b` in an unreferenced line and withheld `pay_9_a`, whose equal net then made the line's only visible solution. The customer's second charge went undetected | Before a file's unreferenced lines are solved, the gateway is asked for every capture it made on each day the lines' window covers (`PAYMENT_SWEEP`, `GET /v1/merchants/{m}/days/{date}/captures`), each capture Driftline has not seen succeed is published as an observation, and the lines wait until the answers are final and applied (D72). Migration V8 allows the new command kind · Shift 8 commit after becfc9d | Not without the case: it needs a duplicate that left no event at all, a paid order, a batch day, equal amounts and a withheld twin. `PossibleMembersTest.theLinesWaitForTheSweepAndACaptureOnlyTheSweepRevealsMakesTheLineAmbiguous` now covers it |

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

### BUG-003: a zombie's epoch bump left the rightful owner fenced, and the partition stalled
- Found: 2026-11-15, Shift 6, seed 437715671286076, profile `pr`, by the `simulation` job on PR #3 (GitHub Actions run 43771567128, shard 2), swarm `NET_DELAY=176‰ NODE_CRASH=1/day NODE_PAUSE=3/day CLOCK_JUMP=4/day STORE_DOWN=2/day PRODUCE_AMBIGUOUS=47‰ FILE_CONFLICT=55‰`. It fails on `main` (c8aeab0) too, so it predates Shift 6: the fresh CI seeds reached it first.
- Repro (before the fix): `./gradlew :sim:run --args="--seed 437715671286076 --profile pr --trace"`
- Invariant: INV-DRAINED ("partition 0 stopped at offset 230 of 306").
- What happened (trace excerpt):
  ```
  000122400161 matcher-a pauses for 19076 ms holding [p0@202..229 epoch 1]
  000122400238 matcher-b pauses for 28660 ms holding [p1@187..201 epoch 1]
  000122410161 coordinator expire matcher-a.1 partitions=[0]
  000122410161 coordinator assign p0 -> matcher-b.1
  000122410238 coordinator expire matcher-b.1 partitions=[0, 1]
  000122419237 matcher-a resumes as matcher-a.1
  000122419237 coordinator assign p0 -> matcher-a.2
  000122419237 matcher-a acquires p0 epoch 2 at offset 230
  000122428898 matcher-b resumes as matcher-b.1
  000122428898 matcher-b acquires p0 epoch 3 at offset 230
  000122428898 matcher-b learns it lost p0
  000124550150 matcher-a fenced on p0 with epoch 2: partition 0 is fenced: epoch 2 was superseded by 3
  ```
- Root cause: the assignment of p0 to matcher-b.1 was made while b was paused, so b ran it on waking, before the heartbeat that told it its member id had expired. Its epoch bump was safe, because fencing held and nothing was written twice, but the fenced rightful owner dropped p0 for good. The simulator's matcher node only acquired on an assignment, and the coordinator, seeing p0 still assigned to matcher-a.2, never assigned it again. The production `PartitionDriver` has had the recovery rule since Shift 6 (D50: re-acquire a fenced partition still assigned after three session timeouts); the simulator's model of it did not.
- Fix: the same rule in the simulator's `MatcherInstance` (D53). With it, matcher-a acquires p0 again 30 s after being fenced, at epoch 4, and resumes at offset 230.
- Why ordinary tests missed it: it needs two overlapping pauses that end in a particular order and straddle an assignment. `PartitionDriverIT` proved a zombie is fenced, but nothing tested the production driver's re-acquire branch either.
- Regression: seed 437715671286076 added to `sim/regression-seeds.txt`; `PartitionDriverIT.aFencedPartitionStillAssignedHereIsAcquiredAgainAndNothingIsLost` now covers the production rule (an epoch bumped behind a running driver: fenced, acquired again, every event applied once).

### BUG-004: a late success paid while its merchant confirmation was pending crashed the resolver
- Found: 2026-11-16, Shift 8 (phase 4, the first campaign with the resolver and dispatcher in the simulator), seed 5, profile `pr`, commit 03b0654 plus uncommitted phase 4 work, swarm `NET_DUP=135‰ NET_DELAY=271‰ NODE_PAUSE=2/day STORE_DOWN=1/day FILE_DUP=239‰ FILE_CONFLICT=63‰ CALL_AMBIGUOUS=112‰ WEBHOOK_LOST=41‰ STATUS_STALE=258‰`. Seed 21 fails the same way.
- Repro (before the fix): `./gradlew :sim:run --args="--seed 5 --profile pr --trace"`
- Invariant: none was violated first; the resolver threw (SIM-STEP-THREW), on the lifecycle guard of D60.
- What happened (trace excerpt):
  ```
  000146708887 gateway payment.succeeded pay_121_a 3655.69
  000146709099 matcher-b p0 epoch 2 batch @850 applied=1 ... raised=LATE_SUCCESS(pay_121_a)
  000146709149 resolver reviews brk_l2zbcejd3nfno23hninsg6lml3->AWAITING_EVIDENCE
  000146710155 dispatcher records cmd_spslm62sipleru4cz4ccwnc4wv.1 {..., status=SUCCEEDED}
  000146710205 resolver reviews brk_l2zbcejd3nfno23hninsg6lml3->ACTION_PENDING
  000146710214 merchant:m_1 order.paid ord_121 v3
  000146710255 dispatcher sends cmd_k73xwclleorg6tt6cordm6sdri.1 MerchantConfirm to MERCHANT
  000146710272 matcher-b p0 epoch 2 batch @851 applied=1 ...
  000146710304 merchant:m_1 confirm ord_121 with pay_121_a: REJECTED
  000146710322 kernel VIOLATION SIM-STEP-THREW: resolver: java.lang.IllegalStateException:
               brk_l2zbcejd3nfno23hninsg6lml3: ACTION_PENDING -> SELF_HEALED is not in the lifecycle
  ```
- Root cause: the LATE_SUCCESS playbook's "the order is paid with this payment" branch had two outcomes, RESOLVED_AUTO after an accepted confirmation and SELF_HEALED otherwise. It did not consider a break that had already asked the merchant (ACTION_PENDING) and found the order paid before the answer arrived. The fake merchant made it worse: it answered REJECTED to a confirmation of the very payment its order was already paid with.
- Fix: in that branch, a break whose state cannot move to SELF_HEALED (it has acted) closes RESOLVED_AUTO. The fake merchant answers ACCEPTED when the order is already paid with the confirmed payment.
- Why ordinary tests missed it: the playbook's tests answered the confirmation before the order changed. The race needs the merchant's own `order.paid` to overtake its answer.
- Regression: seed 5 added to `sim/regression-seeds.txt`; `ResolverTest.aLateSuccessPaidWhileTheMerchantConfirmIsPendingResolvesAutomatically`.

### BUG-005: a duplicate capture whose webhooks were all lost was left unrefunded
- Found: 2026-11-16, Shift 8 (phase 4), seed 7, profile `pr`, commit 03b0654 plus uncommitted phase 4 work, swarm `NODE_PAUSE=1/day CLOCK_JUMP=3/day STORE_DOWN=1/day PRODUCE_AMBIGUOUS=127‰ MERCHANT_KEY_REUSE=30‰ FILE_DUP=167‰ WEBHOOK_LOST=31‰`. Seeds 17 and 35 fail the same way.
- Repro (before the fix): `./gradlew :sim:run --args="--seed 7 --profile pr --trace"`
- Invariant: INV-CUSTOMER-WHOLE.
- What happened (trace excerpt):
  ```
  000168195834 gateway payment.pending pay_46_a 927.45 (webhook given up on)
  000168202710 gateway payment.succeeded pay_46_a 927.45
  000168202910 merchant:m_0 order.paid ord_46 v3
  000168211514 gateway payment.pending pay_46_b 927.45 (webhook given up on)
  000168218390 gateway payment.succeeded pay_46_b 927.45 (webhook given up on)
  000168218585 merchant:m_0 ignores payment pay_46_b for ord_46 in order.paid
  000208800126 matcher-b p0 epoch 2 batch @711 ... raised=... ORPHAN_SETTLEMENT(stl_m_0_20261115:8) ...
  000770400050 kernel VIOLATION INV-CUSTOMER-WHOLE: m_0/pay_46_b was debited, but its order was not
               fulfilled with it, it was not refunded, and no break about it was escalated
  ```
- Root cause: Driftline could learn of pay_46_b only from the bank's line naming it. The ORPHAN_SETTLEMENT playbook held the money in suspense and waited for attribution, which could only come from a webhook that would never arrive, then escalated the line, not the customer's double charge.
- Fix: D64. The orphan playbook asks the gateway about the reference; a new outcome enters as an observation (D10) and heals the orphan, and the DUPLICATE_CAPTURE it implies is raised and refunded.
- Why ordinary tests missed it: the probe (D61) was tested for unpaid orders; a lost duplicate on a paid order reaches Driftline only through settlement.
- Regression: seed 7 added to `sim/regression-seeds.txt`; a `ResolverTest` case for the reference query.

### BUG-006: lost captures were invisible to batch matching and to the pending-order probe
- Found: while building Shift 8 phase 4 (2026-11-16), by the first campaigns with WEBHOOK_LOST. The fix shipped in `9167337`, but this entry and its seed were not written then; they were added in phase 6, when the gap was noticed. With both halves of the fix switched off on the phase 6 tree, 4 of 2,000 `pr` seeds fail on INV-INFERRED-HONEST (546, 849, 1105, 1696); with either half alone all 2,000 pass. (A first attempt, before BUG-009 was fixed, also counted seeds 1298 and 1643; those are BUG-009 and fail with this fix in place.)
- Repro (with both halves switched off): `./gradlew :sim:run --args="--seed 849 --profile pr"`.
- Root cause: under WEBHOOK_LOST the world can capture a payment that Driftline only ever sees as PENDING. Batch matching proved uniqueness over the payments Driftline saw succeed, so a line that really held a lost capture was inferred to a different set with the same sum. The pending-order probe (D61) skipped an order already paid with a known success, so a second capture on it, all of whose webhooks were lost, stayed unknown to Driftline.
- Fix: D69. Payments still PENDING are possible members of unreferenced lines; an inference must stay the unique solution with them added, or the line is AMBIGUOUS with both solutions as witnesses. The probe also runs for a paid order that still has a PENDING payment, and the outcome it finds is published as an observation.
- Regression: seed 849 in `sim/regression-seeds.txt`.

### BUG-007: possible members were placed in windows they could not be in, and their ambiguities never cleared
- Found: 2026-11-16, Shift 8 phase 6, by the first Shift 8 campaign of 10,000 `pr` seeds (seeds 1–10,000 at `1cafc6b`); 10 seeds failed on INV-AMBIGUITY-WITNESSED; the two replayed at `9167337` (phase 4), 2490 and 2293, fail there too. Seed 2490 swarm: `NET_DUP=18‰ NODE_CRASH=2/day NODE_PAUSE=2/day CLOCK_SKEW=1 CLOCK_JUMP=3/day STORE_DOWN=3/day MERCHANT_KEY_REUSE=193‰ FILE_DUP=243‰ WEBHOOK_LOST=46‰`.
- Repro (before the fix): `./gradlew :sim:run --args="--seed 2490 --profile pr --trace"`.
- What happened (seed 2490, trace excerpt):
  ```
  000121539541 gateway payment.pending pay_37_a 499.00
  000121568254 gateway payment.succeeded pay_37_a 499.00 (webhook given up on)
  000121568435 merchant:m_1 order.paid ord_37 v3
  000122400079 matcher-a p1 epoch 2 batch @127 ... raised=ORPHAN_SETTLEMENT(stl_m_1_20261114:2)
  000122400079 kernel VIOLATION INV-AMBIGUITY-WITNESSED: m_1/stl_m_1_20261114:2 witness [pay_80_a, pay_32_a,
               pay_60_a, pay_16_a, pay_71_a, pay_37_a] names pay_37_a, outside the line's window
  ```
- Root cause: two gaps in BUG-006's fix. (a) Every PENDING payment was placed on the file's own date, inside every line's window, though its first event (here `payment.pending` on 14 November IST) bounds its capture from below; a line of the 14 November file holds captures from before 14 November. (b) A line found AMBIGUOUS was never solved again: in seed 2293, `pay_99_a` was pending when the line was solved and failed later, and the line kept a witness that could no longer be one.
- Fix: (a) a pending payment whose first event's batch date is after the file's date is not a possible member of its lines; (b) when a payment leaves PENDING or turns conflicted, every AMBIGUOUS line naming it in a witness, in complete files from its first event's batch date to today, goes back to AWAITING_BATCH and is solved again in the same transaction. The invariant was also too strict: it took "possibly captured" to mean the world never captured the payment, while Driftline cannot tell a lost capture from a payment still pending. It now asks what Driftline knows: last recorded status PENDING, first seen in time. 4 of the 10 seeds (4252, 6572, 8087, 9056) pass with the invariant change alone; 6 (2293, 2490, 2812, 2941, 8465, 8571) still fail without the matcher change.
- Why ordinary tests missed it: batch matching's tests had no pending payments in the pool. `PossibleMembersTest` now has one case per gap; both fail with the matcher change reverted.
- Regression: seeds 2490 and 2293 in `sim/regression-seeds.txt`.

### BUG-008: a fast dispatcher clock skipped the refund lookup, and the payment was refunded twice
- Found: 2026-11-16, Shift 8 phase 6, by the same 10,000-seed campaign: seed 8551, swarm `NET_DROP=104‰ NET_DELAY=34‰ NODE_CRASH=2/day NODE_PAUSE=3/day CLOCK_SKEW=1 CLOCK_JUMP=4/day FILE_DUP=125‰ CALL_AMBIGUOUS=128‰ WEBHOOK_LOST=33‰`. It also fails at `9167337`.
- Repro (before the fix): `./gradlew :sim:run --args="--seed 8551 --profile pr --trace"`.
- What happened (trace excerpt):
  ```
  000151793624 dispatcher sends cmd_phddk6gfpeiucol7q7uhuy3iyk.1 Refund to GATEWAY
  000151793658 gateway refunds pay_80_a 1499.00 as rfd_qt7ozgvspigiawbhqu3xnk6tvx key rfd_caxm6b3mhk2ejqby7b2k77tuvq
  000151834662 dispatcher clock corrected by -667704 ms
  000152503200 dispatcher sends cmd_phddk6gfpeiucol7q7uhuy3iyk.2 Refund to GATEWAY
  000152503365 gateway refunds pay_80_a 1499.00 as rfd_576fqi5xpi7wtj3kg5cw5mwgc4 key rfd_caxm6b3mhk2ejqby7b2k77tuvq
  000152503365 kernel VIOLATION INV-REFUND-ONCE: m_1/pay_80_a was refunded 2 times
  ```
- Root cause: the reply to the first refund was lost (CALL_AMBIGUOUS). The dispatcher's clock was 11 minutes fast when it claimed the command, so `first_sent_at` was 11 minutes late. The retry came 11.8 minutes after the real send, past the sandbox gateway's 5-minute key TTL, but by `first_sent_at` the refund was 42 seconds old, under `gatewayKeyTtl − lookupMargin`, so the dispatcher resent without a REFUND_LOOKUP. The gateway had forgotten the key and refunded again. Judging a key's age depends on the clock having been right at the first send.
- Fix: D70. Every REFUND retry sends a REFUND_LOOKUP first, whatever its age. If the gateway has forgotten the key, the refund happened more than the TTL ago, far past the lookup's lag, so the lookup shows it; if the key is still known, the resend is deduplicated. The age rule's two settings left the dispatcher. D62's delivery assumption stays: an attempt held in the network past the key TTL could still double-refund, so the simulator still caps gateway-bound delays (now the named constant `GATEWAY_DELIVERY_BOUND_MILLIS`).
- Why ordinary tests missed it: the dispatcher's tests ran on one consistent clock. `DispatcherTest.aRetryLooksUpFirstEvenWhenTheFirstSendWasStampedByAFastClock` now covers a first send stamped by a fast clock.
- Regression: seed 8551 in `sim/regression-seeds.txt`.

### BUG-009: a duplicate capture was refunded before the merchant said which payment it kept
- Found: 2026-11-16, Shift 8 phase 6, by the 10,000-seed campaign run after the BUG-007 and BUG-008 fixes: seeds 1298 and 1643. The playbook has had this behaviour since phase 2; the fixes changed these seeds' timing enough to expose it.
- Repro (before the fix): `./gradlew :sim:run --args="--seed 1298 --profile pr --trace"`.
- What happened (trace excerpt):
  ```
  000163704539 gateway payment.succeeded pay_60_a 999.00
  000163720971 gateway payment.succeeded pay_60_b 999.00
  000164133110 matcher-a p1 tick timers=1 raised=DUPLICATE_CAPTURE(pay_60_b)
  000164212289 gateway refunds pay_60_b 999.00 as rfd_bzwnifxwjal7pcubb7imv2oltd key rfd_6lpe7bn3snpydp45tbphvqitwm
  000164321093 merchant:m_1 order.paid ord_60 v3
  000164321225 matcher-a p1 ... raised=DUPLICATE_CAPTURE(pay_60_a)
  000164359720 gateway refunds pay_60_a 999.00 as rfd_mlqcoucywv4y3jqb7i6fg3vfpq key rfd_o7xcus7gjeq57g4q74cc3eebyk
  000164381093 merchant:m_1 order.fulfilled ord_60 v4
  000770443631 kernel VIOLATION INV-NO-REFUND-OF-FULFILLED: m_1/pay_60_b was refunded, and its order was fulfilled with it
  ```
- Root cause: §5.6 keeps "the payment referenced by `order.paid` if there is one, otherwise the earliest"; §8.2 then refunds the non-kept one. Before `order.paid`, "kept" is a provisional default, and the playbook acted on it: a guess (D14). The merchant then paid the order with the refunded payment, and the matcher, rightly re-checking on `order.paid` (D40), made the other payment the duplicate, which was refunded too. The customer got both payments back for a fulfilled order.
- Fix: D71. A duplicate is refunded only when the order is paid with another payment, or expired or cancelled. While it awaits payment the break waits (AWAITING_EVIDENCE) and any order event wakes it, until escalate_by; an order the merchant never reported is escalated (D15).
- Why ordinary tests missed it: the playbook's test paid the order before the duplicate was decided. `ResolverTest.aDuplicateIsNotRefundedUntilTheMerchantPaysTheOrderWithTheOtherPayment` now covers the race and fails without the fix.
- Regression: seed 1298 in `sim/regression-seeds.txt`.

### BUG-010: a duplicate that left no trace was settled in a batch line, and the line was inferred to its twin
- Found: 2026-11-17, by the second CI run of PR #5 (GitHub Actions run 43990121601, simulation shard 2), on fresh seed 439901216016483 at `becfc9d` (merge commit `ed46406`). The first CI run and every local campaign had passed; this seed was new. Swarm: `NET_DUP=70‰ FILE_LATE=328‰ FILE_CONFLICT=148‰ WEBHOOK_LOST=46‰ STATUS_STALE=388‰`.
- Repro (before the fix): `./gradlew :sim:run --args="--seed 439901216016483 --profile pr --trace"`.
- What happened (trace excerpt):
  ```
  000016364073 gateway payment.pending pay_9_a 2270.60
  000016368534 gateway payment.pending pay_9_b 2270.60 (webhook given up on)
  000016380034 gateway payment.succeeded pay_9_a 2270.60
  000016380145 merchant:m_1 order.paid ord_9 v3
  000016384495 gateway payment.succeeded pay_9_b 2270.60 (webhook given up on)
  000016384604 merchant:m_1 ignores payment pay_9_b for ord_9 in order.paid
  000770400050 kernel VIOLATION INV-INFERRED-HONEST: m_1/stl_m_1_20261114:4 was inferred to pay [pay_12_a,
               pay_32_a, pay_9_a], but the world paid [pay_12_a, pay_32_a, pay_9_b] in it, and nothing challenged it
  ```
- Root cause: Driftline's batch candidates were the payments it had seen. A capture none of whose events arrived, on an order paid with another payment, left no trace: the pending-order probe skips a paid order with nothing pending (D61), D64 needs a reference to ask about, and D69 needs at least a pending event. The world withheld `pay_9_a` and paid `pay_9_b`, which has the same net, so the only solution Driftline could see was wrong, and `pay_9_b`'s second charge was never detected.
- Fix: D72. Before a file's unreferenced lines are solved, each capture day of their window is swept: the gateway lists that day's captures as of a stated time, the dispatcher publishes every one Driftline has not seen succeed, and the lines wait until the answer is final (as of the day's end or later; a stale one is asked again, up to 5 times) and applied (every listed capture known as SUCCEEDED, or 30 minutes since the answer). An experiment first let the probe ask once about paid orders, which also fixed this seed; it was set aside because it costs one gateway read per order and misses a duplicate captured more than 30 minutes after the order began.
- Why ordinary tests missed it: no test had a payment that left no event at all. `PossibleMembersTest.theLinesWaitForTheSweepAndACaptureOnlyTheSweepRevealsMakesTheLineAmbiguous` now has one, and `DispatcherTest.aPaymentSweepPublishesEveryCaptureNotYetSeenAsSucceeded` covers the publishing.
- Regression: seed 439901216016483 in `sim/regression-seeds.txt`.

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
