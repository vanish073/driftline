# Driftline: system design

Version 1.0, written in Shift 0 (2026-11-13).
Scope source of truth: [`docs/spec/REQUIREMENTS.md`](spec/REQUIREMENTS.md). Build plan: [`docs/SHIFTS.md`](SHIFTS.md). Bug log: [`docs/BUGS.md`](BUGS.md).

**Change rule:** behaviour described here changes only through a new entry in §19 (Decisions). Never edit it silently.

---

## 0. Summary

Driftline is a streaming reconciler for UPI merchant payments in India. It serves many merchants. It takes in three records of every payment: merchant order events, payment-gateway webhooks and bank settlement files. It joins them per merchant in a stateful stream processor written in plain Java. It detects six kinds of disagreement ("breaks") within minutes. It then resolves each break before its regulatory or internal deadline, by refunding, fulfilling, posting fees, holding money in suspense, or escalating.

The correctness argument is **not** "we used Kafka". It rests on four things:

1. **Deterministic simulation.** The same matcher, resolver, dispatcher and ledger code runs against a fake gateway, bank, merchant, network, clock and store, all driven by one seed. Invariants are checked after every step.
2. **A world oracle.** The simulator knows the ground truth, so it checks Driftline's books against what actually happened, not only against themselves.
3. **Canary bugs.** Known bugs are planted on purpose to measure whether the simulator catches them, and how many seeds it takes.
4. **Real-infrastructure tests.** Testcontainers and a Docker Compose chaos run cover what the simulator cannot see.

---

## 1. Scope

**In scope (v1)**
- UPI merchant (P2M) payments in INR. All amounts are integer **paise** (1 rupee = 100 paise).
- Full refunds only. At most one refund per payment.
- The six break types from the brief, plus *anomalies* (§8.2). Anomalies are always escalated.
- One gateway webhook format and one settlement file format. Both are Driftline's own, modelled on public gateway documentation and cited in `docs/formats.md` (Shift 5).
- PostgreSQL as the system of record. Kafka as the event log. One region.

**Out of scope (stated in the README)**
- Partial refunds, chargebacks and disputes.
- Card rails, multi-currency.
- Real bank connectivity (SFTP or host-to-host links).
- A web UI. The ops surface is the ops API, Grafana and a CSV export.
- Kafka transactions, multi-region deployment, re-partitioning a live cluster.

---

## 2. Guarantees: what Driftline claims and what it does not

This section comes first on purpose. The README copies it word for word.

### 2.1 Claims

**Delivery is at-least-once everywhere.** Gateways redeliver webhooks. Merchants retry. Consumers re-read after a crash. The dispatcher retries calls. No component assumes a message arrives exactly once.

**Effects are idempotent, so the observable outcome is *effectively-once*.** Under the failure model in §2.3, any mix of duplicated, reordered, delayed or lost-then-retried messages, and any process crash between or inside transactions, still gives:

- (a) each source event changes Driftline's state at most once;
- (b) each payment is refunded at most once at the gateway;
- (c) each economic event is recorded in the ledger at most once;
- (d) no refund that was decided is ever lost.

| Effect | Mechanism | Atomic with |
|---|---|---|
| State change caused by an event | Dedupe row `(merchant, source, event_key)` inserted in the same DB transaction as the state change | PostgreSQL transaction |
| Consumer progress | Kafka offsets stored **in PostgreSQL** in that same transaction, fenced by a partition epoch (§5.1) | PostgreSQL transaction |
| Refund at the gateway | Outbox row written in the same transaction as the decision. `Idempotency-Key` derived from the payment. Refund lookup before any retry that is older than the gateway's key TTL (§9) | PostgreSQL transaction + the gateway's key store |
| Ledger record | `UNIQUE(merchant_id, cause_type, cause_id)` plus `ON CONFLICT DO NOTHING` plus an equality check on conflict | PostgreSQL constraint |

### 2.2 Non-claims

- **No "exactly-once processing" claim.** Kafka's exactly-once semantics (idempotent producer plus transactions) cover read-process-write *inside Kafka*. Driftline writes to PostgreSQL and calls an HTTP gateway, so Kafka EOS does not apply and is not used. The idempotent producer is switched on because it removes producer-retry duplicates within one producer session. Correctness does not depend on it.
- If a gateway ignores idempotency keys *and* its refund lookup API is wrong, a double refund is possible. The fake gateway honours keys for a limited time (the TTL), and its lookup API is eventually consistent. The README states this assumption.
- The simulator's store is in-memory and runs transactions one at a time, so isolation anomalies are invisible to it. They are tested against real PostgreSQL (§11.9).
- PostgreSQL durability is assumed: no lost commits and no lying fsync.
- Watermarks on the gateway and merchant streams are heuristics (bounded out-of-orderness). Events that arrive behind the watermark are still processed and counted, and they can change a decision only by the rules in §5.6.

### 2.3 Failure model (what the simulator injects)

- Process crash at any point between transactions and at named crash points (§9.4).
- Process pause, then resume as a zombie after its partition has been reassigned.
- Message drop, duplicate, delay and reorder.
- Ambiguous calls: the effect is applied but the response is lost.
- Database unavailability windows.
- Per-node clock skew, plus forward and small backward clock jumps.
- Late, duplicated, conflicting and incomplete settlement files.
- Stale reads from the gateway's status API.
- Expiry of gateway idempotency keys.

---

## 3. Architecture

```
  UPPER BAND  (production adapters; the simulator replaces all of this)
 +--------------------------------------------------------------------------------+
 |  gateway webhooks --+                                                          |
 |  merchant orders ---+--> Ingest API (HMAC, scopes, idempotency key) --+        |
 |  bank files ---------> Settlement upload --> Settlement worker -------+--> Kafka|
 |  SystemClock     HttpNetwork (java.net.http)     PostgresStore (JDBC)          |
 |  Kafka partition driver      poll drivers (resolver, dispatcher)               |
 +-------------------------------+----------------------------^-------------------+
          inputs: batches, ticks,|                            | three ports:
          call results           v                            | Clock, Network, Store
 +--------------------------------------------------------------------------------+
 |  CORE  (identical in production and simulation; JDK only, no I/O, no threads)  |
 |                                                                                |
 |   Matcher ------- breaks -------> Resolver ------> Outbox ------> Dispatcher   |
 |   (dedupe, state machines,       (break state      (durable       (calls out   |
 |    join, watermarks, timers,      machine,          intents)       through     |
 |    settlement solver)             deadlines)                       Network)    |
 |      |                                                                         |
 |      +--> Ledger (single writer per merchant partition)                        |
 +--------------------------------------------------------------------------------+
```

### 3.1 Sans-I/O core

The core never performs I/O, never blocks, never starts a thread, and reads time only through `Clock`. Each core component is a function from *(input, transaction)* to *(state changes, outbound intents)*. Drivers own the loops.

| Component | Driven by | Input | Writes |
|---|---|---|---|
| Matcher | partition driver | `PartitionBatch` (records with offsets) or `Tick` | dedupe, orders, payments, watermarks, timers, breaks, settlement, ledger, offsets, outbox (probes) |
| Resolver | poll driver | `Tick` | breaks, outbox |
| Dispatcher | poll driver and network | `Tick`, `CallResult` | outbox, break evidence |
| Settlement parser | settlement worker | file bytes | nothing (pure function) → events |
| Ingest normaliser | HTTP handler | raw request | nothing (pure function) → envelope or rejection |

### 3.2 The three ports

```java
/** Processing time of this node. May jump; may differ between nodes. Never used as event time. */
public interface Clock { Instant now(); }

/** Non-blocking. The result arrives later as a CallResult input to the sending node, or never.
 *  The caller decides timeouts using its Clock. */
public interface Network { void send(OutboundCall call); }

/** Runs body atomically. May throw StoreUnavailable or Conflict. The body must have no
 *  effects outside the transaction. */
public interface Store { <R> R transact(String txName, TxBody<R> body); }

public interface Tx {
  DedupeRepo dedupe();      OrderRepo orders();       PaymentRepo payments();
  WatermarkRepo watermarks(); TimerRepo timers();     SettlementRepo settlement();
  BreakRepo breaks();       OutboxRepo outbox();      LedgerRepo ledger();
  PartitionRepo partitions(); AuditRepo audit();
}
```

| Port | Production | Simulation |
|---|---|---|
| Clock | `SystemClock` | `SimClock`: per-node skew and jumps over the scheduler's virtual time |
| Network | `HttpNetwork`: `java.net.http` async; results go into the node's inbox | `SimNetwork`: the scheduler decides delivery, drop, duplication and delay |
| Store | `PostgresStore`: JDBC, HikariCP, READ COMMITTED with explicit locks and optimistic versions | `InMemoryStore`: transactions run one at a time; survives node crashes |

**Kafka is not a port. The core never sees it.** The production partition driver polls Kafka and passes batches in. The simulation driver passes batches from `SimLog`. Offsets are ordinary core data, stored through `Store`.

**Rule:** every `Store` method exists in both implementations, and an abstract contract test runs against both (§11.9).

### 3.3 Roles and topics

- **One image, many roles.** `DRIFTLINE_ROLE` is one of `ingest`, `matcher`, `resolver`, `dispatcher`, `settlement-worker`, or `all`. `all` is the default for the laptop, which has about 4 GB of RAM (§15.1).
- **One topic:** `driftline.events.v1`, 12 partitions. Each merchant's partition is assigned at onboarding and stored in `merchants.partition`. Ingest sets it explicitly on every record, so a merchant's three sources share one partition and one single-threaded matcher sees that merchant's whole story in log order.
- Changing the partition count needs an offline re-partition procedure (out of scope; documented).

---

## 4. Core model

### 4.1 Money, time and identity

**Money**
- `long` paise everywhere. Arithmetic uses `Math.addExact`, `subtractExact` and `multiplyExact`.
- Parsing `"1234.50"` gives `123450`. More than 2 decimal places, exponents and signs where none are allowed are all rejected.
- `float` and `double` are banned in money code (ArchUnit).

**Time**
- *Event time* is the `Instant` (UTC) asserted by the source.
- *Processing time* comes from `Clock`.
- Business dates are calendar dates in `Asia/Kolkata`, always computed through `ZoneId`, never a hard-coded offset.
- **T** is the IST calendar date of the payment's first debit evidence.

**Identity: IDs are derived, never generated.**

```
DerivedId = base32(SHA-256(namespace || 0x00 || field1 || 0x00 || field2 ...))[0..26]
```

| ID | Derived from |
|---|---|
| breakId | `("break", merchant, type, subjectKey)` |
| outbox commandId | `("cmd", breakId, kind, n)` |
| refund idempotency key | `("refund", merchant, paymentId)`, scoped to the payment, not the break (D11) |
| ledger cause | `(CAPTURE, paymentId)`, `(REFUND, paymentId)`, `(SETTLEMENT, fileId:lineNo)` … |

Random IDs are banned from the core. Because retries and re-detections produce the same identifiers, idempotency is built into the data rather than added afterwards.

### 4.2 Canonical event envelope

```
EventEnvelope {
  schemaVersion  : 1
  merchantId, partition
  source         : GATEWAY | MERCHANT | BANK | OBSERVATION
  eventKey       : the idempotency key (gateway event id | Idempotency-Key header | fileSha:lineNo | derived)
  eventType      : payment.pending | payment.succeeded | payment.failed | refund.processed | refund.failed
                 | order.created | order.pending_payment | order.paid | order.fulfilled | order.expired | order.cancelled
                 | settlement.line | settlement.file_end
  eventTime      : Instant (asserted by the source)
  receivedAt     : Instant (ingest processing time, used for event-to-match latency)
  payloadSha256  : hash of the canonical payload (used to detect a key reused with a different body)
  payload        : typed record
  traceparent    : W3C trace context
}
```

`OBSERVATION` events are facts Driftline learned by asking (status re-queries, refund results). They come back in through Kafka like everything else (D10).

### 4.3 Payment state machine (explicit legal transitions)

**States:** `NONE` → `PENDING` | `SUCCEEDED` | `FAILED`. Refunds are tracked separately in `refundState`: `NONE`, `REQUESTED`, `REFUNDED`, `REFUND_FAILED`.

**Ordering** uses `(eventTime, eventKey)`. Duplicates by `eventKey` never reach this table, because the dedupe row stops them first.

Rows are the current state; columns are the incoming event.

| current \ incoming | `pending` | `succeeded` (time s) | `failed` (time f) |
|---|---|---|---|
| `NONE` | → PENDING | → SUCCEEDED | → FAILED |
| `PENDING` | ignore | → SUCCEEDED | → FAILED |
| `SUCCEEDED` (at s₀) | ignore (stale) | ignore if the amount is equal, otherwise CONFLICT | f < s₀: ignore (stale, arrived out of order). f > s₀: CONFLICT |
| `FAILED` (at f₀) | ignore (stale) | s > f₀: → SUCCEEDED and set `lateSuccess` (**LATE_SUCCESS**). s < f₀: CONFLICT | ignore |

- **CONFLICT** leaves the state unchanged, sets `conflict = true`, and raises a `STATUS_CONFLICT` anomaly. The anomaly is re-queried for evidence and then **escalated, never auto-resolved** (D14).
- Any amount mismatch between events for the same payment is also a CONFLICT.
- Every applied or ignored decision is written to `payment_transitions` with the rule that fired.

**Payment outcome** is set at most once and never changes (INV-ONE-OUTCOME):
- `FULFILLED`: an order was paid with this payment.
- `REFUNDED`.
- `NO_DEBIT`: the payment failed, and its settlement due date passed in settlement event time with no line for it.

### 4.4 Order state machine

**Versions.** Merchant events carry a monotonic `version` per order.
- `version` > current: apply.
- `version` = current with a different payload: `ORDER_ANOMALY`.
- `version` < current: stale, ignore.

**Legal graph:**
- `CREATED` → `PENDING_PAYMENT` → `PAID` → `FULFILLED`
- `CREATED` or `PENDING_PAYMENT` → `EXPIRED` or `CANCELLED`
- `FULFILLED`, `EXPIRED` and `CANCELLED` are terminal.

Skipped versions are allowed when the target is reachable in the graph. An illegal target (for example `EXPIRED` → `PAID`) leaves the state unchanged and raises `ORDER_ANOMALY`. `order.paid` carries the `paymentId` the merchant used.

### 4.5 Property that proves out-of-order handling

Take any multiset of events with no contradictions. Deliver it in **any permutation, with any duplicates**. The final payment and order states must be identical to delivering it in event-time order.

This is a jqwik property in Shift 1 and an invariant in the simulator.

---

## 5. Matcher

### 5.1 Partition ownership and fencing

- **On assignment:** `UPDATE partition_leases SET epoch = epoch + 1, owner = :me WHERE partition = :p RETURNING epoch, next_offset`. Then load keyed state for the partition and `seek(next_offset)`.
- **On every batch commit:** `UPDATE partition_leases SET next_offset = :n WHERE partition = :p AND epoch = :myEpoch`. If 0 rows change, the instance has been **fenced**: roll back and drop the partition.
- A zombie, such as a paused instance that wakes after reassignment, therefore cannot commit. The simulator produces zombies on purpose (`NODE_PAUSE`).

### 5.2 Keyed state

- Per partition, the matcher keeps a write-through cache (`LinkedHashMap`) of **open** orders and payments. The source of truth is the Store.
- A payment closes once its outcome is final and its settlement is resolved. It is then evicted, which keeps state bounded.
- On assignment, state is loaded with `WHERE partition = :p AND open`.

### 5.3 Join keys

- **order ↔ payment:** the payment carries `orderId`; `order.paid` carries `paymentId`.
- **payment ↔ settlement line:** by reference (paymentId, RRN or UTR), or by batch inference (§6.4).

### 5.4 Watermarks

| Kind | Scope | Definition | Nature |
|---|---|---|---|
| Stream watermark `wm_G`, `wm_M` | (partition, source ∈ {GATEWAY, MERCHANT}) | `max(eventTime seen) − bound_source`. If idle on a tick (no event for `idleTimeout` of processing time): `max(wm, now − bound_source − skewAllowance)`. Never decreases. | Heuristic (bounded out-of-orderness) |
| Settlement watermark `swm` | merchant | The greatest settlement date D such that every *expected* file date ≤ D (per the merchant's settlement calendar) is COMPLETE (trailer validated) | **Exact**: files are complete units |
| Combined `wm_GM` | partition | `min(wm_G, wm_M)` | Used when both streams matter |

**Late vs missing.**
- A late file does not advance `swm`, so it never produces false "missing" breaks.
- A file that *never* arrives is a different problem. A processing-time `FEED_STALL` timer raises an operational alert ("settlement feed stalled for merchant M, date D"). Payments are not flagged.
- An event that arrives behind its stream watermark is processed normally and counted in `driftline_events_late_total`.

### 5.5 Timers

A timer is `(partition, kind, subjectKey, fireAt, clock ∈ {STREAM_GM, STREAM_G, SETTLEMENT, PROCESSING})`.
- It is unique per `(kind, subjectKey)`, so re-registering a timer is an upsert.
- Due timers fire in the same transaction that advanced the watermark, ordered by `(fireAt, kind, subjectKey)`, which is deterministic.

| Timer | fireAt | Clock | On fire |
|---|---|---|---|
| `ORDER_CHECK(payment)` | successTime + `orderGrace` (10 min) | STREAM_GM | evaluate DEBITED_NO_ORDER |
| `DUP_CHECK(order)` | max(successTimes) + `bound_G` | STREAM_G | evaluate DUPLICATE_CAPTURE |
| `PENDING_PROBE(order)` | order.createdTime + `probeAfter` (15 min) | STREAM_GM | if still pending with no successful payment known: enqueue `STATUS_QUERY_ORDER` (catches lost webhooks) |
| `SETTLEMENT_DUE(payment)` | dueDate + `graceDays` (1) | SETTLEMENT | MISSING_SETTLEMENT (if succeeded) or outcome `NO_DEBIT` (if failed) |
| `BATCH_SOLVE(file)` | end of the file's batch window + `bound_G` | STREAM_G | run batch matching for unreferenced lines (§6.4) |
| `FEED_STALL(merchant, date)` | expected arrival + `stallTimeout` | PROCESSING | operational alert |

### 5.6 Detection rules

| Break | Precondition at evaluation time | Subject key | Evidence attached |
|---|---|---|---|
| **DEBITED_NO_ORDER** | Success evidence exists for payment p: either gateway SUCCEEDED, or a settlement line references p (in that case a status probe runs first). p's order is `CREATED`, `PENDING_PAYMENT`, `EXPIRED`, `CANCELLED`, or unknown. The order was not paid with p, and not paid with another payment (that case is a duplicate). | paymentId | payment timeline, order timeline, watermarks |
| **LATE_SUCCESS** | Transition `FAILED → SUCCEEDED` (s > f₀) | paymentId | both events, order state |
| **DUPLICATE_CAPTURE** | ≥ 2 SUCCEEDED payments for one order, evaluated after `DUP_CHECK`. *Keep* the payment referenced by `order.paid` if there is one, otherwise the earliest by `(successTime, paymentId)`. Raise one break per other payment. | paymentId (the one to refund) | all payments for the order |
| **SHORT_SETTLEMENT** | A referenced line where received < expected **net** (§6.5) | fileId:lineNo | expected breakdown vs received |
| **MISSING_SETTLEMENT** | p SUCCEEDED, `swm ≥ dueDate(p) + graceDays`, p not matched, **and p is not a candidate of any unattributed line in its window**. If it is, p is linked to that line's escalation instead, so it is never double-reported. | paymentId | expected date, files checked, payment timeline |
| **ORPHAN_SETTLEMENT** | A line with an unknown reference, or an unreferenced line left unattributed after `BATCH_SOLVE`. The reason is one of `NO_CANDIDATES`, `NO_SOLUTION`, `AMBIGUOUS`, `BUDGET_EXCEEDED`. | fileId:lineNo | candidate-set hash, two witnesses if ambiguous |

**One action owner per payment (D11).** A payment can qualify for several refund-capable breaks, for example both late success and duplicate.
- The first break to claim it sets `payments.action_owner_break_id`.
- Later breaks for the same payment become `LINKED` and follow the owner's outcome.
- Breaks detected in the same transaction take precedence in this order: `DUPLICATE_CAPTURE` > `LATE_SUCCESS` > `DEBITED_NO_ORDER`.
- This rule, together with the payment-scoped refund key, is what makes "no payment has two final states" hold.

**Late evidence after detection.** The matcher appends the evidence to the break (bumping its version). Then:
- If the break has no action yet, the resolver may mark it `SELF_HEALED`.
- If an action has already been dispatched, the action stands. A contradiction such as "order paid with a payment we refunded" becomes an `ORDER_ANOMALY`, which is escalated.

### 5.7 Batching

- One `PartitionBatch` holds up to `max.poll.records` records and is processed in **one** Store transaction: dedupe inserts, state updates, ledger postings, timers, watermark, offsets.
- Records that cannot be decoded are written to `quarantine` *in that same transaction*. No dead-letter topic is needed, which avoids a dual write.

---

## 6. Settlement

### 6.1 File format

- Driftline Settlement File v1 is CSV, modelled on public gateway settlement-report documentation (exact citations in `docs/formats.md`, Shift 5).
- **Record types:** `H` (header: fileId, merchantId, settlementDate, generatedAt, formatVersion); `D` (detail: lineNo, valueDate, amount in rupees with 2 decimals, optional fee, optional tax, optional reference, optional batchId, narration); `T` (trailer: lineCount, totalAmount).
- The parser is strict: exact decimals, the declared count and total must match, unknown record types are rejected. It is a pure function in the `wire` module.

### 6.2 File lifecycle

1. **RECEIVED.** Raw bytes are stored and SHA-256 is unique.
2. **PARSED.** The worker emits `settlement.line` × N, then `settlement.file_end`, to Kafka with `eventKey = sha:lineNo`.
3. **COMPLETE.** The matcher has seen lines 1..N and the trailer validates. `swm` may then advance.

Exceptions:
- A duplicate upload (same SHA) is a no-op.
- The same `(merchant, settlementDate)` with a *different* SHA is `REJECTED_CONFLICT` and raises a `FILE_CONFLICT` anomaly.
- A worker crash mid-emit means the worker re-emits everything, and dedupe absorbs the duplicates.

### 6.3 Reference matching

A line with a reference that resolves to known payments is `MATCHED_REFERENCED` if `received == Σ expected net`. Otherwise §6.5 applies.

A payment can be settled at most once: `UNIQUE(merchant_id, payment_id)` on `settlement_matches`. A reference to an already-settled payment raises a `SETTLEMENT_CONFLICT` anomaly.

### 6.4 Batch matching (subset-sum, exact, never guesses)

**Narrowing must be conservative**: the candidate set must be a superset of the true batch.
1. Same merchant (partition-local).
2. SUCCEEDED, not yet settled, not claimed by a referenced line.
3. `batchDate(p) ∈ [D − lookbackDays, D]`, where `batchDate` comes from capture time, the merchant's cutoff and cycle, in IST. The lookback window also catches payments settled a day late.
4. `batchId`, if the line has one.
5. **Watermark gate.** Solve only once `wm_G ≥ window end + bound_G`, meaning we believe every payment in the window has been seen.

**Expected net per candidate** (per-payment rounding is the configured model):
```
fee_i = roundHalfUp(gross_i × feeBps / 10_000) + fixedFee
gst_i = roundHalfUp(fee_i × gstBps / 10_000)          // GST 18% = 1800 bps on the fee
net_i = gross_i − fee_i − gst_i
```
Driftline never tries several fee models and keeps whichever fits. That would be guessing (D16).

**Solver.** Deterministic, with a budget counted in operations, never wall-clock time (D7).
```
solve(values[], target, budgetOps) -> NONE | UNIQUE(subset) | AMBIGUOUS(w1, w2) | BUDGET_EXCEEDED
  0. Canonical candidate order: (captureTime, paymentId).
  1. g = gcd(values). If target % g != 0 -> NONE. Otherwise divide everything by g
     (amounts are often multiples of 100 paise).
  2. All values positive:
       sum == target  -> UNIQUE(all)          (any proper subset is strictly smaller)
       sum <  target  -> NONE
       otherwise solve the complement for (sum - target) when that side is smaller.
  3. Pick the cheaper engine:
       meet-in-the-middle (n <= 40; sorted long[] arrays, no hash maps), or
       capped-count DP (positive values, n * S <= budget, counts capped at 2, witnesses reconstructed).
     If neither fits the budget -> BUDGET_EXCEEDED.
  4. Both engines count solutions up to 2 and return witnesses.
     AMBIGUOUS always carries two distinct, verified subsets.
```

**Several unreferenced lines in one file: forced-assignment propagation.** If line L has a *unique* solution S over the full candidate set C, then every joint assignment must give L exactly S. So it is sound to commit S, remove it from C, and re-solve the remaining lines, repeating to a fixpoint. Lines that are still ambiguous at the fixpoint are reported `AMBIGUOUS`. This is conservative: two lines that are individually ambiguous but jointly unique are still reported as ambiguous. That is never wrong, only cautious.

**Results:**
- `MATCHED_INFERRED`, recorded with its witness and the candidate-set hash.
- `AMBIGUOUS` with two witnesses, or `NO_SOLUTION`, or `BUDGET_EXCEEDED`. These become ORPHAN_SETTLEMENT with that reason: the money is held in suspense and the line is flagged. Ambiguous lines are reported *as ambiguous* in the API, metrics and CSV export.

**Residual risk, stated openly.** A unique fit is a proof *relative to the candidate set*. If the bank line contains a payment Driftline never saw, a coincidental unique fit is possible. Mitigations:
1. The watermark gate.
2. The wide lookback window.
3. **Challenge detection.** A SUCCEEDED payment that arrives later with a `batchDate` inside an already-solved window raises `INFERRED_MATCH_CHALLENGED` on that line, which is escalated.
4. The simulator measures inferred-match accuracy against ground truth (INV-INFERRED-HONEST).

**Refund netting.** Gateways net refunds out of settlements. In v1 this is supported on **referenced** lines with itemised data. On unreferenced lines, netted refunds give `NO_SOLUTION`, which is escalated. Signed meet-in-the-middle for netted batches is a cut-line stretch (§20).

### 6.5 Short settlement (D16)

`expected` means **expected net** per the merchant's fee schedule. "Below expected gross" would flag every normal settlement.

| Case | Result |
|---|---|
| received = expected net | MATCHED. Fees and GST are posted as part of the settlement transaction. |
| received < expected net, the line itemises fee and tax, and `gross − received == itemisedFee + itemisedTax` exactly | **SHORT_SETTLEMENT, auto-resolved.** Post the itemised fee and tax entries. The difference from the schedule is recorded as `pricing_drift` (a metric and an evidence field). |
| received < expected net, with no itemisation, or itemisation that does not add up | **SHORT_SETTLEMENT, escalated** with expected vs received. A partial posting keeps the remaining receivable open. |
| received > expected net | `OVER_SETTLEMENT` anomaly, escalated |

---

## 7. Ledger

### 7.1 Chart of accounts (per merchant)

Debits are positive, credits negative, and every transaction sums to zero.

| Code | Type | Holds |
|---|---|---|
| `GATEWAY_RECEIVABLE` | asset | captured gross not yet settled, net of refunds |
| `BANK` | asset | settled cash |
| `PAYMENTS_CLEARING` | liability | customer money not yet tied to a paid order |
| `SALES` | income | payments matched to paid orders |
| `GATEWAY_FEES` | expense | fees deducted at settlement |
| `GST_INPUT_CREDIT` | asset | GST paid on fees (claimable) |
| `SUSPENSE` | liability | bank money not yet attributed |
| `SETTLEMENT_DIFFERENCES` | expense | only through audited manual resolution |

### 7.2 Posting rules

The ledger has **one writer per merchant partition: the matcher** (D8). Exception: `MANUAL_ADJUSTMENT` through the audited ops API.

| Cause `(type, id)` | Entries |
|---|---|
| `CAPTURE, paymentId` | Dr GATEWAY_RECEIVABLE +g · Cr PAYMENTS_CLEARING −g |
| `ORDER_MATCH, paymentId` | Dr PAYMENTS_CLEARING +g · Cr SALES −g |
| `REFUND, paymentId` | Dr PAYMENTS_CLEARING +g (or SALES, if the payment was matched) · Cr GATEWAY_RECEIVABLE −g |
| `SETTLEMENT, fileId:lineNo` | Dr BANK +received · Dr GATEWAY_FEES +Σfee · Dr GST_INPUT_CREDIT +Σgst · Cr GATEWAY_RECEIVABLE −Σgross |
| `SETTLEMENT_PARTIAL, fileId:lineNo` | Dr BANK +received · Cr GATEWAY_RECEIVABLE −received (the rest stays open) |
| `SUSPENSE_IN, fileId:lineNo` | Dr BANK +a · Cr SUSPENSE −a |
| `SUSPENSE_RELEASE, fileId:lineNo` | Dr SUSPENSE +a · Dr fees and GST · Cr GATEWAY_RECEIVABLE −gross |
| `MANUAL_ADJUSTMENT, auditSeq` | as entered; must balance; audited |

### 7.3 "Rejects any unbalanced transaction": three layers

1. **Domain.** The `LedgerTxn` constructor rejects a transaction whose sum ≠ 0, that has fewer than 2 entries, that has a zero entry, that mixes merchants, or that overflows.
2. **Store.** Both stores validate again and enforce cause uniqueness.
3. **PostgreSQL**, so even raw SQL cannot break it:
   - a `CONSTRAINT TRIGGER … DEFERRABLE INITIALLY DEFERRED` checks the per-transaction sum at commit;
   - a trigger rejects entries added to a transaction created by a different DB transaction (compares `xid8`);
   - `BEFORE UPDATE OR DELETE` triggers raise an error;
   - the application role has only `INSERT` and `SELECT`.

### 7.4 Idempotent posting

`INSERT … ON CONFLICT (merchant_id, cause_type, cause_id) DO NOTHING`. On conflict, the existing transaction is loaded and its canonical entries compared:
- equal: no-op, counted as a duplicate;
- different: `LEDGER_CONFLICT` anomaly. That is a bug, and the simulator fails the seed.

Two confirmation paths for one refund (Driftline's own observation and the gateway's `refund.processed` webhook) therefore produce exactly one ledger record. This is layer 3 doing its job.

### 7.5 Hot accounts: two strategies, both measured

| Strategy | Write path | Balance read |
|---|---|---|
| `ROW_LOCK` (baseline) | `UPDATE account_balances SET balance = balance + :d WHERE account_id = :a` inside every ledger transaction. All writers on that row run one after another, each waiting for the previous commit's fsync. | one row |
| `APPEND_SNAPSHOT` (default) | insert entries only, with no shared row | `snapshot.balance + Σ entries with xid ≥ snapshot.horizon` |

**The trap and its fix.** Sequence IDs are not in commit order: entry 1001 can commit after entry 1002. So "snapshot up to entry id N" can silently skip an entry. Instead, each entry stores `xid8 = pg_current_xact_id()`. The snapshotter folds only entries whose `xid < pg_snapshot_xmin(pg_current_snapshot())`. Every transaction below that horizon has finished, so the folded set can never change afterwards.

**Honest framing.** Driftline's topology gives each merchant a single ledger writer, which sidesteps contention. The benchmark (Shift 10) measures what happens when that assumption is relaxed (several writers per merchant, for example a giant merchant split by order): throughput and p99 at concurrency 1, 4, 16 and 64.

### 7.6 Audit trail

- `audit_log` is append-only and hash-chained: `hash = SHA-256(prev_hash || canonical(row))`.
- It records ops actions (manual resolutions, adjustments, token and secret changes).
- `driftline verify-audit` walks the chain. The DB privileges and triggers match the ledger's.
- Sealing the ledger itself into hash blocks, using the same `xid8` horizon, is a stretch (§20).

---

## 8. Resolver

### 8.1 Break lifecycle

```
DETECTED ──> AWAITING_EVIDENCE ──> ACTION_PENDING ──> RESOLVED_AUTO
   │               │                     │
   │               └──> SELF_HEALED      └──> ESCALATED ──> RESOLVED_MANUAL
   └──> LINKED (follows the payment's action-owner break)
any non-final state ──(now ≥ escalate_by)──> ESCALATED
```

- Each decision happens in **one** transaction: read the break and the current payment and order state, choose, then write the break state and outbox command(s).
- `breaks.action` is set at most once.
- Every transition is appended to `break_transitions`.

### 8.2 Playbooks

| Type | Steps | Deadline rule |
|---|---|---|
| DEBITED_NO_ORDER | 1. `STATUS_QUERY_PAYMENT`. 2. If confirmed SUCCEEDED: order EXPIRED, CANCELLED or failed → `REFUND`. Order still pending and not expired → wait (event-time timer at `expiresAt`); `order.paid` with p → SELF_HEALED. Order unknown → **escalate** (D15). 3. Gateway still not SUCCEEDED after K re-queries → `STATUS_CONFLICT` → escalate. 4. Refund confirmed → RESOLVED_AUTO. | RBI P2M, T+5 |
| LATE_SUCCESS | Confirm by status query. Order pending and not expired → `MERCHANT_CONFIRM` (fulfil): ACCEPTED → wait for `order.paid` → RESOLVED_AUTO; REJECTED (expired) → `REFUND`. Order expired or cancelled, or paid by another payment → `REFUND`. | RBI P2M, T+5 |
| DUPLICATE_CAPTURE | Status-query both payments, then `REFUND` the non-kept one. | internal, T+1 |
| SHORT_SETTLEMENT | Auto-resolved by the matcher when the itemised entries explain the gap (§6.5); otherwise escalate with evidence. | internal, detection + 4 h |
| MISSING_SETTLEMENT | Escalate with evidence: payment timeline, due date, files checked, watermark. | internal, detection + 4 h |
| ORPHAN_SETTLEMENT | `SUSPENSE_IN` (matcher) and flag. Escalate with reason and witnesses. SELF_HEALED if it is later attributed (`SUSPENSE_RELEASE`). | internal, detection + 4 h |
| Anomalies (`STATUS_CONFLICT`, `ORDER_ANOMALY`, `OVER_SETTLEMENT`, `FILE_CONFLICT`, `SETTLEMENT_CONFLICT`, `INFERRED_MATCH_CHALLENGED`, `IDEMPOTENCY_CONFLICT`, `LEDGER_CONFLICT`) | Gather evidence, then escalate. Never auto-resolved. | internal, detection + 4 h |

### 8.3 Deadlines and compensation

Deadline rules live in `config/deadlines.yaml` and are cited in the README.

| Rule id | Applies to | Deadline | Source |
|---|---|---|---|
| `RBI_UPI_P2M_T5` | DEBITED_NO_ORDER; LATE_SUCCESS (refund path) | end of IST day T+5 | RBI 2019 TAT harmonisation circular: "debited, confirmation not received at merchant" |
| `RBI_UPI_P2P_T1` | none of the six (P2P transfers are outside a merchant's reconciliation) | end of IST day T+1 | same circular; kept in the table for completeness and stated in the README |
| `INTERNAL_DUP_T1` | DUPLICATE_CAPTURE | end of IST day T+1 | internal policy |
| `INTERNAL_ESCALATE_4H` | SHORT, MISSING, ORPHAN, anomalies | detectedAt + 4 h | internal policy |

- `deadline_at` is 00:00 IST on day T+N+1 (exclusive).
- `escalate_by = deadline_at − margin`. The default margin is 24 h for T+N rules. **Assumption checked by the simulator:** the margin is longer than the longest modelled outage plus gateway refund latency.
- **"Resolved"** for a refund means the gateway has confirmed the refund was processed. The RBI's "R" is the day the funds reach the customer's bank, which Driftline cannot observe. The margin covers that gap, and the README says so.
- **Compensation exposure.** If the resolution date (IST) is after the deadline date, exposure is `₹100 × (R − deadlineDate)` days. It is exported as a gauge and a per-break field. Escalation does not stop the clock.

---

## 9. Outbox and dispatcher

### 9.1 Command kinds

| Kind | Target | Idempotency |
|---|---|---|
| `STATUS_QUERY_PAYMENT`, `STATUS_QUERY_ORDER` | gateway | reads (safe to repeat) |
| `REFUND` | gateway | `Idempotency-Key = H("refund", merchant, paymentId)` and `metadata.driftline_refund_key` |
| `REFUND_LOOKUP` | gateway | read: refunds for a payment |
| `MERCHANT_CONFIRM` | merchant callback | `Idempotency-Key = H("confirm", merchant, orderId, paymentId)` |
| `PUBLISH_OBSERVATION` | Driftline's own `/internal/v1/observations` → Kafka | derived `eventKey`; dedupe absorbs repeats |
| `ESCALATION_NOTIFY` | ops sink | derived key |

### 9.2 Dispatch loop (sans-I/O)

1. **Claim.** Run `claim(tx)`: `SELECT … WHERE status IN ('PENDING','IN_FLIGHT') AND next_attempt_at <= now AND (lease_until IS NULL OR lease_until < now) ORDER BY next_attempt_at, command_id FOR UPDATE SKIP LOCKED LIMIT n`. Set `lease_owner`, set `lease_until = now + lease`, increment `attempt`, commit.
2. **Send** each call through `Network`.
3. **Record.** `onResult(result, tx)` stores the result. **Results are facts, applied idempotently whatever attempt produced them.** The lease controls only who may *send*.
4. **Timeout.** A tick after `sentAt + callTimeout` with no result means the outcome is unknown. The command is rescheduled with the same key.
5. **Backoff.** `min(cap, base × 2^attempt) + (H(commandId, attempt) mod base)`. The jitter is deterministic, with no RNG.
6. **Old retries.** Before resending a `REFUND` whose first attempt is older than `gatewayKeyTtl − margin`, the dispatcher first runs `REFUND_LOOKUP`, and resends only if no refund exists.
7. **Status results that reveal new facts**, such as a success Driftline never received as a webhook, enqueue `PUBLISH_OBSERVATION`, so the fact enters through Kafka (D10).

### 9.3 Three layers, because each one fails differently

| Layer | Protects against | Fails when | Covered by |
|---|---|---|---|
| 1. Transactional outbox | A decision whose effect is lost (crash after deciding), or an effect with no decision (crash before commit) | The dispatcher crashes after the call but before recording the result; two dispatchers send after a lease expires | Layer 2 |
| 2. Idempotency key on outgoing calls | Duplicate refunds from retries, zombie dispatchers or ambiguous timeouts | The gateway forgets the key (TTL); a key is derived from the wrong fields; a gateway bug | `REFUND_LOOKUP` before late retries; derived keys; the world invariant INV-REFUND-ONCE |
| 3. Ledger uniqueness | Recording one economic event twice in our books (two confirmation paths, replays) | Two different cause ids for the same event (a bug) | Causes derived from the payment id; INV-BOOKS-MATCH-WORLD catches drift |

Every row has a **canary** (§11.10) that switches the layer off and shows the simulator catching the result.

### 9.4 Crash points

A `CrashPoints` hook is a no-op in production. The simulator and integration tests use it to throw at a named point.

| ID | Where | Required outcome after restart |
|---|---|---|
| CP1 | resolver: decided, before commit | nothing happened; decided again from scratch |
| CP2 | resolver: committed, before any claim | the dispatcher finds the command; refund sent once |
| CP3 | dispatcher: claim committed, before send | lease expires; resent with the same key; one refund |
| CP4 | dispatcher: sent, the gateway applied it, response lost | timeout; retried with the same key; the gateway dedupes; one refund |
| CP5 | dispatcher: result received, before the record commits | the result is lost; retry or lookup; one refund, recorded once |
| CP6 | matcher: mid-batch, before commit | batch replayed from the stored offset; dedupe; no double effects |
| CP7 | matcher: zombie after reassignment | commit fenced (0 rows); no writes |
| CP8 | settlement worker: mid-emit | re-emitted; deduped; file COMPLETE exactly once |

---

## 10. Ingest and API

### 10.1 Endpoints

| Method | Path | Auth |
|---|---|---|
| POST | `/v1/webhooks/gateway/{merchantId}` | HMAC signature |
| POST | `/v1/merchants/{merchantId}/order-events` | token `events:write`; `Idempotency-Key` header **required** |
| POST | `/v1/merchants/{merchantId}/settlement-files` | token `settlement:write` |
| POST | `/internal/v1/observations` | service token `observations:write` |
| GET | `/v1/breaks?state=&type=` | `breaks:read` |
| GET | `/v1/breaks/{id}` | `breaks:read` |
| POST | `/v1/breaks/{id}/resolve` | `breaks:resolve` (audited) |
| GET | `/v1/payments/{id}/timeline` | `breaks:read` |
| GET | `/v1/exports/breaks.csv` | `breaks:read` (CSV-injection safe, §14) |
| GET | `/actuator/health/liveness`, `/actuator/health/readiness`, `/actuator/prometheus` | none, but served only on management port 9091, never exposed outside the cluster (D21) |

### 10.2 Webhook verification

- Header: `Driftline-Signature: t=<unix>,v1=<hex>[,v1=<hex>]`. The signed payload is `t + "." + rawBody`, signed with HMAC-SHA256 using the merchant's secret(s). Two secrets may be active at once, to allow rotation.
- **Checks:** verify **before** parsing; constant-time compare (`MessageDigest.isEqual`); replay window `|now − t| ≤ 300 s`; body ≤ 64 KiB.
- Each delivery attempt is signed with a fresh timestamp. Replays inside the window pass ingest and are made harmless by dedupe on the event id.

### 10.3 Idempotency keys on every write

| Write | Key |
|---|---|
| Webhook | the gateway event id |
| Order event | the `Idempotency-Key` header (a missing header gives 400) |
| Settlement line | `sha256:lineNo` |
| Observation | derived |

- **Ingest is stateless (D3).** Dedupe happens in the consumer, *atomically with the state change*. That is the only place it can be atomic; anywhere else it is just a cache.
- Ingest returns `202 {accepted, key}` after a Kafka ack. The response is identical for retries.
- The same key with a different payload hash is quarantined as `IDEMPOTENCY_CONFLICT` and is visible in the ops API.

### 10.4 Kafka client settings

**Producer:**
- `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`;
- explicit partition, key = merchantId, lz4 compression.

**Consumer:**
- `enable.auto.commit=false`, the cooperative-sticky assignor;
- offsets are kept in PostgreSQL;
- `seek` on assignment;
- graceful shutdown on SIGTERM: finish the batch, commit, close.

---

## 11. Deterministic simulation

### 11.1 Kernel

- **Discrete-event, single-threaded.**
- The event queue is ordered by `(virtualTime, seq)`, where `seq` is an insertion counter. This gives a total order, so ties are deterministic.
- **A step** pops one item and sets virtual time. It then hands one input to one node. The node runs **at most one** Store transaction and emits sends and timer requests. Finally, the incremental invariants are checked.
- Time skipping means days of simulated time run in milliseconds. That is essential for T+5 deadlines.

### 11.2 Randomness

- The simulator uses its own **SplitMix64** PRNG, so a seed reproduces across JDK versions (D13).
- Separate streams are derived as `SHA-256(seed || label)` for `workload`, `faults`, `network` and `swarm`. This is why a fault can be switched off without disturbing the workload, which also enables shrinking (§11.14).

### 11.3 What is real and what is fake

| Real (production code, unchanged) | Replaced |
|---|---|
| Matcher (dedupe, state machines, join, watermarks, timers, settlement solver), Resolver, Dispatcher, Ledger rules, `wire` codecs (HMAC sign and verify, JSON codecs, settlement CSV parser), ingest normaliser | Kafka → `SimLog` · HTTP → `SimNetwork` · PostgreSQL → `InMemoryStore` · system clock → `SimClock` · gateway, bank and merchant → world models |

### 11.4 World models

- **FakeGateway.** Holds payments and status per payment. Sends webhooks on a retry schedule, each attempt re-signed. The status API can be stale for a configurable lag. The refund API honours idempotency keys for a TTL and supports `metadata`. Supports refund lookup.
- **FakeMerchant.** Runs order lifecycles with expiry and versions. Sends events with retries and the same key. A *buggy client* mode reuses a key with a different body. Provides the confirm API (rejects expired orders).
- **FakeBank.** Holds the accounts of customers, the gateway float and merchants. **Money is conserved** (INV-WORLD-CONSERVED). Generates settlement files: per-payment and batch lines, with and without references, itemised fees, per-payment rounding, files that are late, duplicated or conflicting, lines that are missing, extra or short, and refund netting.
- **Workload.** Customers, orders and payments, plus labelled break scenarios for all six types. The labels are used for the per-type numbers. Correctness checks use the end-state oracle (§11.8), not the labels.

### 11.5 Fault catalogue and swarm testing

Each seed switches each fault type on or off (swarm testing: varying which faults are active finds more bugs than turning everything on at once) and draws an intensity. The swarm vector is printed in the trace header.

| Fault | Effect |
|---|---|
| `NET_DROP` / `NET_DUP` / `NET_DELAY` | message lost, duplicated, or delayed (heavy-tailed delay), which also reorders |
| `NET_PARTITION` | a node is cut off from the gateway for an interval |
| `CALL_AMBIGUOUS` | the fake applies the effect, then the response is lost |
| `NODE_CRASH` | a random node crashes at a random step and restarts after a downtime; in-memory state is lost and its pending events are dropped (incarnation numbers) |
| `NODE_PAUSE` | the node freezes, its partition is reassigned, then it resumes as a zombie |
| `CLOCK_SKEW` / `CLOCK_JUMP` | per-node offset; forward jumps; small backward steps |
| `STORE_DOWN` | `transact` throws `StoreUnavailable` for a window |
| `PRODUCE_AMBIGUOUS` | the log append succeeds but the ack is lost, so ingest returns 5xx and the sender retries |
| `WEBHOOK_LOST` | the gateway gives up; only the pending-order probe can recover the fact |
| `STATUS_STALE` | the status API lags reality |
| `FILE_LATE` / `FILE_DUP` / `FILE_CONFLICT` | settlement feed problems |
| `MERCHANT_KEY_REUSE` | the buggy client |

### 11.6 Phases

1. **RUN:** workload plus faults.
2. **HEAL:** no new faults; crashed nodes restart; the network becomes reliable.
3. **DRAIN:** run until only far-future timers remain and every deadline has passed.
4. **FINAL CHECKS.**

The liveness assumptions (maximum downtime, maximum partition) are constants in the profile and are checked against the escalation margin.

### 11.7 Invariants

| ID | Checked | Property |
|---|---|---|
| INV-LEDGER-BALANCED | every step | every committed ledger transaction sums to 0, has ≥ 2 entries, and no zero entries |
| INV-LEDGER-ONCE | every step | ≤ 1 ledger transaction per `(merchant, cause_type, cause_id)` |
| INV-EVENT-ONCE | every step | each `eventKey` is applied to a state machine at most once |
| INV-TRANSITIONS-LEGAL | every step | every recorded transition is in its table (§4.3, §4.4, §8.1) |
| INV-ONE-OUTCOME | every step | **no payment has two final states**: outcome set once, never changed |
| INV-REFUND-ONCE | every step (world) | **no refund is issued twice**: the FakeGateway executed ≤ 1 refund per payment, for the captured amount |
| INV-NO-REFUND-OF-FULFILLED | every step (world) | never both refunded and fulfilled for the same payment |
| INV-FENCED | every step | no commit with a stale partition epoch |
| INV-WORLD-CONSERVED | every step | **money is conserved** in the world (Σ of all fake balances is constant). This also checks the simulator itself. |
| INV-MISSING-IS-REAL | every step | a MISSING_SETTLEMENT break exists only if the world did not settle that payment by its due date plus grace |
| INV-DEADLINE | as virtual time passes | **every break is RESOLVED or ESCALATED by** `max(escalate_by, detected_at + reactionBound)` |
| INV-BOOKS-MATCH-WORLD | end state | per merchant: ledger `BANK` = the world bank balance; `GATEWAY_RECEIVABLE` = the world's captured − refunded − settled gross (allowing for open escalations); `SUSPENSE` = Σ unattributed lines |
| INV-CUSTOMER-WHOLE | end state | every debited world payment is attached to exactly one fulfilled order, or refunded exactly once, or covered by an escalated break |
| INV-DETECTED | end state | every world discrepancy found by the oracle has a break or escalation (recall). Breaks that were raised and later self-healed are reported as a metric, not as violations. |
| INV-INFERRED-HONEST | end state | every MATCHED_INFERRED line equals the ground truth or carries `INFERRED_MATCH_CHALLENGED` |
| INV-AMBIGUITY-WITNESSED | every step | every AMBIGUOUS line has two distinct witnesses, each a subset of the candidates and each summing to the target |
| INV-DETERMINISTIC | CI | the same seed gives the same trace digest (each CI run double-runs a sample of seeds) |

### 11.8 Oracles

- The **end-state oracle** derives the expected discrepancies from the *final world state*, for example "captured at the gateway, but the merchant order is not paid", rather than from injection labels. Faults can create real breaks nobody planned, so labels alone would miss them.
- The **solver oracle**: for n ≤ 18, brute force in tests (§ Shift 7).

### 11.9 What the simulator cannot see, and what covers it

| Blind spot | Covered by |
|---|---|
| The in-memory store runs one transaction at a time, so it sees no isolation anomalies; it has no SQL triggers or constraints | The **Store contract suite** (same abstract tests on `InMemoryStore` and `PostgresStore`); Testcontainers integration tests for concurrency, triggers, `SKIP LOCKED`, fencing races and lease expiry |
| No real Kafka (rebalance protocol, poll timeouts) | Kafka integration tests with Testcontainers (two consumers, rebalance, fencing) |
| No real HTTP or TLS timeouts | App-level integration tests; Compose end-to-end tests |
| Performance | Load test and benchmark (Shift 10), never sim time |
| JVM faults (OOM, GC) | Out of scope; stated |

### 11.10 Canary bugs: measuring the simulator itself

`Canaries` is an immutable record. It is `NONE` in production, and the app refuses to start otherwise. ArchUnit enforces that only `sim` and tests may build a non-`NONE` value. CI asserts that every canary is caught within its seed budget, and the README reports *seeds-to-first-detection* per canary.

| Canary | Planted bug | Expected to trip |
|---|---|---|
| C01 `DEDUPE_OFF` | skip the dedupe insert | INV-EVENT-ONCE, INV-LEDGER-ONCE |
| C02 `NO_REFUND_KEY` | send refunds without an idempotency key | INV-REFUND-ONCE |
| C03 `MARK_BEFORE_SEND` | mark the outbox command done before sending | INV-CUSTOMER-WHOLE (a lost refund) |
| C04 `SKIP_TTL_LOOKUP` | resend old refunds without a lookup | INV-REFUND-ONCE (only under long outages) |
| C05 `LEDGER_UNIQUE_OFF` | post without the cause uniqueness check | INV-LEDGER-ONCE, INV-BOOKS-MATCH-WORLD |
| C06 `SUCCESS_TO_FAILED` | allow SUCCEEDED → FAILED | INV-TRANSITIONS-LEGAL, INV-CUSTOMER-WHOLE |
| C07 `PROCESSING_TIME_WATERMARK` | treat "missing" by wall clock | INV-MISSING-IS-REAL |
| C08 `FIRST_SOLUTION_WINS` | subset-sum returns the first fit, with no ambiguity check | INV-INFERRED-HONEST |
| C09 `NO_FENCING` | commit without the epoch check | INV-FENCED, INV-LEDGER-ONCE |
| C10 `IGNORE_ESCALATE_BY` | never escalate on time | INV-DEADLINE |
| C11 `BREAK_SCOPED_REFUND_KEY` | refund key per break instead of per payment | INV-REFUND-ONCE |

### 11.11 Determinism enforcement

ArchUnit runs on `core`, `wire` and `sim`. It fails the build on:

- **Time:** `System.currentTimeMillis` or `nanoTime`, `Instant.now`, `LocalDate.now` and friends, `ZoneId.systemDefault`, `java.time.Clock.system*`.
- **Randomness:** `Math.random`, `java.util.Random`, `ThreadLocalRandom`, `SecureRandom`, `UUID.randomUUID`.
- **Threads:** `Thread`, `ExecutorService`, `CompletableFuture.*Async`, parallel streams.
- **Environment and I/O:** `System.getenv`, `System.getProperty`, file and socket I/O.
- **Iteration order:**
  - `HashMap` and `HashSet` (keys with identity hashes, such as enums and records containing enums, iterate in a different order on every run);
  - `Set.of`, `Map.of` and `Map.ofEntries` (their iteration order uses a salt chosen per JVM run);
  - `Collectors.toSet`, `toMap` and `groupingBy` without a supplied map.
- **Locale and static state:** `String.format`, `toUpperCase` or `toLowerCase` without an explicit `Locale`; non-final static fields.

- **Framework confinement:** no `org.springframework` in `core`, `wire`, `sim` or `store-postgres` (D18).

On top of these rules, there is the empirical check INV-DETERMINISTIC.

### 11.12 Running and reproducing

```
./gradlew :sim:run --args="--seed 8812 --profile pr"                # one seed
./gradlew :sim:run --args="--seed 8812 --profile pr --trace"        # human-readable timeline
./gradlew :sim:campaign --args="--from 1 --count 10000 --threads 8" # many seeds
./gradlew :sim:canaries                                             # sensitivity table
```

A failing seed prints:
- the violated invariant with details;
- the last 200 trace lines;
- the swarm vector;
- `REPRO: ./gradlew :sim:run --args="--seed 8812 --profile pr"` (at commit `<sha>`).

The full trace goes to `build/sim-failures/seed-8812.trace`.

### 11.13 Bug log and regression corpus

Every simulator-found bug gets an entry in `docs/BUGS.md` (template there), and its seed is appended to `sim/regression-seeds.txt`, which every CI run replays. Canary detections are *not* bugs. They go to `docs/results/canaries.md`.

### 11.14 Seed shrinking (stretch)

Fault decisions are recorded as an indexed list. A failing run is replayed with subsets of the faults masked off (delta debugging, the same idea as git bisect applied to faults) to find a minimal failing fault set. The workload stream is untouched, because the RNG streams are separate.

### 11.15 Profiles

| Profile | Merchants | Partitions | Payments | Traffic days | Use |
|---|---|---|---|---|---|
| `calm` | 2 | 2 | ~80 | 1 | baseline, no faults |
| `pr` | 2–3 | 1–3 | 60–150 | 2 (+ drain to T+7) | 10,000 seeds per PR; target ≤ 20 ms per seed |
| `nightly` | 5 | 6 | ~2,000 | 5 | long campaign |

---

## 12. Data model (PostgreSQL; `InMemoryStore` mirrors it)

```
merchants(merchant_id PK, partition, fee_bps, fixed_fee_paise, gst_bps, cycle_days, cutoff_local_time, calendar_id)
merchant_webhook_secrets(merchant_id, kid, secret_enc, active_from, active_to, PK(merchant_id,kid))
api_tokens(token_sha256 PK, merchant_id NULL(=ops), scopes text[], created_at, revoked_at)
partition_leases(partition PK, epoch, owner, next_offset)
processed_events(merchant_id, source, event_key, payload_sha256, partition, kafka_offset, PK(merchant_id,source,event_key))
quarantine(id PK, partition, kafka_offset, reason, raw)
orders(merchant_id, order_id, state, version, amount_paise, expires_at, paid_payment_id, PK(merchant_id,order_id))
payments(merchant_id, payment_id, order_id, state, amount_paise, t_date_ist, success_at, failed_at, late_success,
         conflict, refund_state, outcome, action_owner_break_id, settled, open, partition,
         customer_vpa_enc, customer_vpa_bidx, PK(merchant_id,payment_id))
payment_transitions / order_transitions(..., seq, from, to, event_key, event_time, rule)          -- append-only
watermarks(partition, source, value, last_event_processing_at, PK(partition,source))
settlement_watermarks(merchant_id PK, complete_through)
timers(kind, subject_key, partition, clock, fire_at, PK(kind,subject_key)); INDEX(partition,clock,fire_at)
settlement_files(file_id PK, merchant_id, settlement_date, sha256 UNIQUE, line_count, total_paise, status, raw)
settlement_lines(file_id, line_no, merchant_id, value_date, amount_paise, fee_paise, tax_paise, reference, batch_id,
                 status, reason, PK(file_id,line_no))
settlement_matches(merchant_id, payment_id, file_id, line_no, how, candidate_set_sha, PK(merchant_id,payment_id))
ambiguity_witnesses(file_id, line_no, witness_no, payment_ids text[], PK(file_id,line_no,witness_no))
breaks(break_id PK, merchant_id, kind, type, subject_key, state, action, rule_id, detected_at, deadline_at,
       escalate_by, resolved_at, compensation_paise, evidence jsonb, version, UNIQUE(type,subject_key))
break_transitions(break_id, seq, from, to, at, reason, PK(break_id,seq))                         -- append-only
outbox(command_id PK, break_id, merchant_id, kind, payload jsonb, idempotency_key, status, attempt,
       lease_owner, lease_until, next_attempt_at, first_sent_at, last_result jsonb)
ledger_accounts(account_id PK, merchant_id, code, UNIQUE(merchant_id,code))
ledger_transactions(txn_id PK, merchant_id, cause_type, cause_id, posted_at, created_xid xid8,
                    UNIQUE(merchant_id,cause_type,cause_id))                                     -- append-only
ledger_entries(entry_id bigserial PK, txn_id, account_id, amount_paise CHECK(<>0), xid xid8)     -- append-only
account_balances(account_id PK, balance_paise)                         -- ROW_LOCK strategy
account_snapshots(account_id, horizon_xid xid8, balance_paise, PK(account_id,horizon_xid))      -- APPEND_SNAPSHOT
audit_log(seq bigserial PK, at, actor, action, subject, details jsonb, prev_hash, hash)          -- append-only
```

- **Retention.** `processed_events` rows are kept for at least 14 days (longer than the longest webhook retry plus Kafka retention). A replay older than that still hits state-machine idempotence, which is layer 0.
- **Database roles.** `driftline_migrator` owns the schema. `driftline_app` has DML only, with no UPDATE or DELETE on append-only tables.

---

## 13. Observability

- **Traces** (OpenTelemetry Java agent plus manual spans).
  - `ingest.webhook` → (Kafka headers carry `traceparent`) → `matcher.batch` (span links to each record) → `resolver.decide` → `dispatcher.call` (HTTP client auto-instrumented).
  - Logs are JSON with `trace_id`.
- **Metrics** (Prometheus):

| Area | Metrics |
|---|---|
| Ingest | `driftline_ingest_requests_total{route,status}`, `driftline_webhook_rejected_total{reason}` |
| Events | `driftline_events_consumed_total{source}`, `driftline_events_duplicate_total{source}`, `driftline_events_late_total{source}` |
| Latency | `driftline_event_to_match_seconds` (histogram, from ingest `receivedAt` to the matcher commit that changes reconciliation status), `driftline_detection_lag_seconds{type}` |
| Progress | `driftline_watermark_lag_seconds{partition,source}`, `driftline_consumer_lag_records{partition}`, `driftline_partition_fenced_total` |
| Breaks | `driftline_breaks_detected_total{type}`, `driftline_breaks_closed_total{type,how=auto\|self_healed\|manual}`, `driftline_breaks_escalated_total{type,reason}`, `driftline_breaks_open{type}` |
| Deadlines | `driftline_deadline_headroom_seconds{type}`, `driftline_compensation_exposure_rupees` |
| Outbox | `driftline_outbox_pending{kind}`, `driftline_outbox_oldest_pending_seconds`, `driftline_outbox_attempts_total{kind,result}` |
| Settlement | `driftline_settlement_lines_total{result}`, `driftline_subset_sum_ops` (histogram), `driftline_pricing_drift_total` |

- **Grafana board** (provisioned JSON): throughput, event-to-match p50 and p99, watermark and consumer lag, breaks by type and state, deadline headroom, compensation exposure, outbox health, settlement results.
- **Alert rules:** oldest pending outbox command > 10 min; an open break with < 6 h headroom; settlement feed stalled; consumer lag growing for 10 min.

---

## 14. Security (each item has a test or a CI job)

| Item | Implementation | Proof |
|---|---|---|
| Signed webhooks with a replay window | §10.2 | unit tests: tampered body, wrong secret, stale or future `t`, rotated secret; e2e test returns 401 |
| Token scopes per merchant | opaque tokens stored as SHA-256; a Spring Security filter maps each token to scopes as `SCOPE_*` authorities; `@PreAuthorize` checks the scope; a merchant-binding check compares the path's `{merchantId}` with the token's merchant; ops tokens are separate | integration test: a merchant-A token on merchant B gives 403 (an IDOR test); a missing scope gives 403 |
| Encrypted personal fields | customer VPA, phone and email encrypted with AES-256-GCM (JDK), key id (`kid`) for rotation, key from env or K8s Secret; HMAC blind index for lookups | round-trip and rotation tests; the DB column holds only ciphertext |
| Masked logs | Logback masking converter (`ab***@okaxis`, `98******21`) | test that captures all log output from an e2e run and fails if any raw PII pattern appears |
| Append-only audit trail | §7.6 hash chain, privileges, triggers | `verify-audit` test; UPDATE or DELETE rejected by the DB |
| Dependency scanning in CI | GitHub dependency-review (PRs), OSV-Scanner (lockfile), Trivy (image), Dependabot | CI jobs fail on high or critical findings |
| Extras | CSV export escapes cells that start with `= + - @` (finance users open CSVs in Excel); DB least privilege; non-root, read-only-root containers; NetworkPolicy; request size limits; secret scanning (gitleaks) | unit tests and CI jobs |

`docs/THREAT_MODEL.md` (Shift 9) holds a short STRIDE-style table.

---

## 15. Deployment

### 15.1 Local machine constraints (measured 2026-11-13)

**Facts:**
- The Windows host has 7.7 GB RAM. WSL2 gets 3.7 GB (the default, half of the host), and Docker Desktop runs inside that same VM (`docker info` reports 3.67 GiB). There are 12 cores.
- Docker Desktop 4.92 / Engine 29.8 **is now integrated with WSL** (verified: `hello-world` ran).
- About 2 GB of the 3.7 GB is already used by tools: VS Code server ≈ 1.2 GB, Claude Code sessions ≈ 0.8 GB. Swap was 75% full before any Driftline process ran.

**Memory budget per shift:**

| Workload | Approximate RAM |
|---|---|
| Gradle daemon plus test JVM | 1.0–1.3 GB |
| + PostgreSQL (Testcontainers) | +0.15 GB |
| + Kafka (Testcontainers or Compose, `-Xmx384m`) | +0.55 GB |
| + Spring Boot app, all roles in one JVM (`-Xmx256m -XX:+UseSerialGC -Xss512k`) | +0.40 GB |
| + sandbox (JDK HttpServer, `-Xmx128m`) | +0.15 GB |
| + `obs` profile (Prometheus, Grafana, OTel collector, Jaeger) | +0.50 GB |

**Consequences:**
- Shifts 1–5 and 7 fit comfortably.
- Shifts 6 and 8 (the Compose `core` profile, about 2.3 GB with Gradle) fit **only with VS Code and extra Claude sessions closed** and Gradle stopped while the stack runs (`./gradlew --stop`; e2e tests run from a fat jar).
- Shift 9 runs the `obs` profile only long enough to take the dashboard screenshot.
- The kind cluster, the 200k-seed campaigns and the 10k-seed PR runs happen **in CI**.
- Shift 10 laptop numbers are reported with this exact spec and are expected to be modest. Results taken while swapping are discarded: the load script records `vmstat` and rejects runs with sustained swap-in.
- Recommended `%UserProfile%\.wslconfig`: `[wsl2]` with `memory=4GB` and `swap=8GB`, plus `[experimental]` with `autoMemoryReclaim=gradual`. Swap prevents the out-of-memory killer from causing flaky tests. Memory is not raised further because Windows needs the rest.

### 15.2 Docker Compose (`deploy/compose/`)

- `postgres:17` and `apache/kafka:4.x` (KRaft, single node). Both are pinned by digest in Shift 6.
- `driftline` (roles), `sandbox` (an HTTP-served fake gateway, merchant and bank, reusing the simulator's world models), and the `obs` profile.
- Health checks; Flyway migration runs as a one-shot service.

### 15.3 Kubernetes (`deploy/k8s/`, kustomize)

- **Base:** one Deployment per role (ingest with an HPA; matcher with replicas ≤ partitions), Services, ConfigMap, Secret *templates* (no values committed), PodDisruptionBudgets, NetworkPolicies, a migration Job, probes, resource requests and limits, `securityContext` (non-root, read-only root filesystem, no privilege escalation).
- **Overlay `kind`:** dev-only StatefulSets for Postgres and Kafka, plus the sandbox.
- `scripts/kind-smoke.sh` deploys, runs one scenario per break type, and verifies.

---

## 16. CI (GitHub Actions)

| Workflow | Jobs |
|---|---|
| `ci.yml` (pull requests and pushes to main) | `build` (unit, ArchUnit, jqwik, settlement corpus) · `integration` (Testcontainers) · `simulation` (matrix of 4 shards × 2,500 seeds = **10,000 seeds**, with the base seed derived from the run id; plus the regression corpus and the determinism double-run) · `canaries-quick` · `security` (dependency-review, OSV-Scanner, gitleaks) · `e2e-compose` (from Shift 6) · `claims-lint` |
| `nightly.yml` | 200k-seed `nightly` campaign · full canary matrix · kind smoke · Trivy image scan |

- **`claims-lint`** scans the claim-bearing files: `README.md`, `docs/results/**`, and the `description` fields of the K8s and Compose manifests. It fails if any of them contains "exactly-once", "exactly once", "zero data loss" or "guaranteed delivery" outside blocks marked `<!-- claims:allow -->`. The only allowed block is the README guarantees section that explains why we do *not* claim it. Design and spec documents are not scanned, because they have to discuss the term in order to disclaim it.
- All third-party actions are pinned by commit SHA.
- **Make the repo public.** GitHub's standard runners for public repos are larger (4 vCPU / 16 GB) than for private repos, and Actions minutes are free for public repos. 10,000 seeds plus Testcontainers plus Compose e2e on every PR would burn through a private repo's free monthly allowance. A public repo also suits a portfolio project. No secrets are ever committed (gitleaks enforces this).

---

## 17. Measurement methodology (every README number comes from a committed script)

| Number | Method |
|---|---|
| Events per second, sustained | An open-model load generator (constant arrival rate, which avoids coordinated omission), signed webhooks plus order events through HTTP ingest. The rate is stepped up. "Sustained" means consumer lag did not grow over the last 5 minutes of a 10-minute step, with zero errors. Hardware and configuration are reported. |
| p99 event-to-match | `driftline_event_to_match_seconds` over the sustained window at 50% and 80% of the maximum rate |
| Auto-resolved share per break type | (a) A simulation campaign aggregated over 10,000 seeds (denominators shown). (b) A Compose scenario run with N injected breaks per type. Shares: `auto` / `self_healed` / `escalated`. |
| Seeds with zero violations; real bug count | The cumulative seed count from CI artifacts; the bug count from the `docs/BUGS.md` rows (canaries excluded) |
| Canary sensitivity | Seeds to first detection, per canary |
| Hot accounts | Both strategies × concurrency {1, 4, 16, 64}: transactions per second, p50 and p99 commit latency, balance-read latency |

Raw outputs live in `docs/results/` (JSON or CSV). The README tables are generated from them by `scripts/render-results`.

---

## 18. Stack and repository layout

**Stack:**
- Java 21 (installed), Gradle (Kotlin DSL, version catalog, wrapper).
- **App:** **Spring Boot** (Web MVC on virtual threads, Security, Actuator, Validation), plain kafka-clients (D19), PostgreSQL JDBC, HikariCP (Boot-managed), Flyway (migrations live in `store-postgres`; Boot runs them in `app`, tests call the Flyway API directly), SLF4J and Logback, OpenTelemetry (Java agent plus API), Micrometer with the Prometheus registry through Actuator.
- **Jackson:** `wire` and `app` only, on the same Jackson major version that Spring Boot manages (checked on the day).
- **Spring Boot version:** the latest stable release, checked on Maven Central in Shift 1. Its BOM is applied in `app` only (D18).
- **Tests:** JUnit 5, AssertJ, jqwik, Testcontainers, ArchUnit.
- Exact versions are pinned in `gradle/libs.versions.toml` in Shift 1, after checking Maven Central: no ranges, no "latest".

```
driftline/
  core/            JDK only: domain, ports, state machines, ledger rules, matcher, resolver, dispatcher, solver
                   testFixtures: Store contract suite, builders
  wire/            Jackson: envelope and webhook codecs, HMAC, settlement CSV parser (deterministic; used by sim)
  sim/             kernel, SplitMix64, SimClock/SimNetwork/SimLog, InMemoryStore, world models, faults,
                   invariants, oracles, canaries, CLI;  regression-seeds.txt
  store-postgres/  PostgresStore, Flyway migrations, balance strategies, integration tests
  app/             Spring Boot: explicit @Configuration wiring of the ports, roles, ingest and ops API (MVC),
                   Spring Security token scopes, Kafka partition driver, HttpNetwork, settlement worker, telemetry
  sandbox/         world models served over the JDK HttpServer (no Spring) for Compose, e2e, chaos and load runs
  bench/           load generator, hot-account benchmark
  deploy/          compose/, k8s/, grafana/, prometheus/
  docs/            spec/, DESIGN.md, SHIFTS.md, BUGS.md, formats.md, THREAT_MODEL.md, research/, results/
  scripts/         e2e.sh, chaos.sh, kind-smoke.sh, claims-lint.sh, render-results
  .github/workflows/
```

---

## 19. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | Sans-I/O core, three ports, drivers own the loops | Lets the simulator run the *same* code, step by step |
| D2 | Kafka offsets stored in PostgreSQL in the state transaction; no Kafka transactions | Kafka EOS does not cover a PostgreSQL sink; this is atomic and honest |
| D3 | Dedupe in the consumer, atomically with the state change; ingest stateless | Dedupe anywhere else is a cache, not a guarantee |
| D4 | IDs derived from content; no random IDs in the core | Retries and re-detections collide on purpose; deterministic |
| D5 | One topic, partition chosen explicitly from `merchants.partition` | One merchant's three sources stay in one ordered stream; testable |
| D6 | Settlement watermark from file completeness; stream watermarks are bounded-out-of-orderness with idleness | Exact where the data allows it; separates "late file" from "missing payment" |
| D7 | Subset-sum is exact and budgeted by operation count; ambiguity needs two witnesses; forced propagation across lines; inferred matches are marked and challengeable | Never guess; wall-clock timeouts would break determinism |
| D8 | The matcher is the only automatic ledger writer per merchant | No hot-row contention in the real flow; manual adjustments go through the audited API |
| D9 | `APPEND_SNAPSHOT` with an `xid8` horizon is the default; `ROW_LOCK` is the measured baseline | Avoids the sequence-gap trap; the brief asks for both to be measured |
| D10 | Facts learned by asking re-enter through Kafka (`PUBLISH_OBSERVATION`) | "Everything lands on Kafka"; one ordered path into the matcher |
| D11 | Refund key scoped to the payment; one action owner per payment | Two breaks can never yield two refunds or two outcomes |
| D12 | ~~Javalin with a manual composition root~~ **Superseded by D17** (2026-11-13, at the user's request) | — |
| D13 | Own SplitMix64 PRNG | Seed reproducibility across JDK upgrades |
| D14 | CONFLICT transitions are escalated, never auto-resolved | Never guess |
| D15 | An unknown merchant order is escalated, not refunded | Refunding an order that exists but was never reported would be harmful |
| D16 | Short means below expected *net*; fee and tax entries explain the gap only when itemised and exact | "Below gross" would flag every normal settlement |
| D17 | **Spring Boot for the `app` module**: Spring MVC on virtual threads, Spring Security, Actuator, Micrometer. Plain `@Configuration` classes wire the core's ports **explicitly**: the core has no Spring annotations and no component scanning. | The user chose it; banks and card networks recognise it. Explicit wiring keeps "production wires real ports, the simulator wires fakes" visible in two short files. |
| D18 | **Spring stays inside `app` only.** `core`, `wire`, `sim` and `store-postgres` have no `org.springframework` dependency (ArchUnit plus a Gradle check). `store-postgres` uses plain JDBC, so its contract tests run without a Spring context. | The simulator must run the same code with no framework; a Spring context is neither deterministic nor fast enough for 10,000 seeds. |
| D19 | **Plain `kafka-clients`, not Spring Kafka**, run inside a `SmartLifecycle` bean | The partition driver needs exact control: offsets in PostgreSQL, an epoch bump and `seek` on assignment, fencing on commit. A listener container would hide those steps. |
| D20 | The `sandbox` (fake gateway, merchant and bank over HTTP) uses the JDK's `HttpServer`, not Spring | It is a test fixture, not the product, and it saves about 150–200 MB of RAM on a 3.7 GB machine |
| D21 | Webhook HMAC is checked in the controller on the raw `@RequestBody byte[]` **before** any JSON binding. Actuator runs on a separate management port (9091) that is never exposed outside the cluster. | A signature over re-serialised JSON is a classic bug. Metrics and health must not be public. |
| D22 | (Shift 1) **`sim.runner` is the simulator's driver** (CLI, campaign threads, file output) and sits outside the determinism rules. Nothing else in `sim` may depend on it (ArchUnit `NOTHING_DEPENDS_ON_SIM_RUNNER`). | Campaigns need threads and files; the kernel, world models and invariants must not. The one-way dependency keeps the deterministic part checkable. |
| D23 | (Shift 1) **Two JUnit lines.** The non-Spring modules test with JUnit 5.14.4 (Platform 1.14.4), because jqwik 1.10.2 is built against Platform 1.14. `app` uses the JUnit that Spring Boot 4.1.2 manages (6.0.3). The two never share a test classpath. | jqwik does not yet run on JUnit Platform 6; Boot 4 requires it. Keeping them apart avoids a broken engine discovery. |
| D24 | (Shift 1) **The payment machine is a function of its evidence, not of arrival order.** This refines the §4.3 cells as follows. (a) `successAt` is the *earliest* success seen and `failedAt` the *latest* failure seen; `lateSuccess` holds when both exist. (b) A failure older than the recorded success is **not ignored**: it is recorded and sets `lateSuccess` (status stays SUCCEEDED). The matcher raises LATE_SUCCESS whenever `lateSuccess` becomes true, which refines §5.6 ("transition FAILED → SUCCEEDED"). (c) A success and a failure at the **same instant** are a CONFLICT. (d) A later failure moves `failedAt` later; an earlier success moves `successAt` earlier, so T and its deadline never depend on arrival order. (e) An order-id mismatch is a CONFLICT, like an amount mismatch. (f) After a conflict the payment is **frozen**: every later event is a CONFLICT (`ALREADY_IN_CONFLICT`), so no automatic action ever runs on contradictory records. | Read literally, §4.3 fails the §4.5 property on the full state. {failed@1, succeeded@3} set `lateSuccess` only when the failure arrived first. {failed@1, failed@5, succeeded@3} was a conflict in some orders and silently accepted in another. A T date could move by a day with arrival order. Breaking a same-instant tie by `eventKey` would be a guess (D14). The jqwik properties prove both order-insensitivity and that a conflict is raised in every order exactly when the records contradict. |
| D25 | (Shift 1) **Order machine details for §4.4.** Reachability counts zero steps: a higher version in the same state is an update, not an anomaly. A newer snapshot that names a different payment than the one the order was paid with is ORDER_ANOMALY (`PAID_WITH_CHANGED`). **Known limit:** an older version is ignored as stale without being checked against the order's history, so a merchant history that contradicts itself (v2 EXPIRED, v3 PAID) raises ORDER_ANOMALY only if v2 arrives before v3. | The merchant's newest version is its system of record, and the final order state is still order-insensitive (proved by the §4.5 property). Checking stale versions would need the transition history in the state. Revisit if the Shift 5 oracle shows it matters. |
| D26 | (Shift 1) **Determinism rule details for §11.11.** Beyond the listed items, ArchUnit also bans: `Set.copyOf`, `Map.copyOf` and `Collectors.toUnmodifiableSet/Map` (same per-JVM salt); all of `java.util.concurrent`, `ThreadLocal` and `Timer`; the legacy `Date`, `Calendar` and `TimeZone`; `System.identityHashCode` and `Collections.shuffle`; property reads such as `Integer.getInteger`; `System.out`, `err` and `in`; `java.text`, `Formatter` and `Scanner`; and network classes except `URI`, `URLEncoder` and `URLDecoder`. A static final array counts as mutable static state. "Money code" for the float/double ban (§4.1) means all of `core` and `wire`. Calls and method references are both checked (`Instant::now` counts). **Limit:** ArchUnit sees signatures, fields, calls, references and dependencies, not arithmetic on local variables. | Each extra ban closes a hole with the same failure mode as a listed one. Core and wire have no legitimate use for floating point. The limit is written down rather than hidden. |
| D27 | (Shift 1) **Port and event details.** The store's conflict exception is `StoreConflict` (§3.2 says `Conflict`), so it never clashes with `Decision.Conflict`. `OutboundCall` is (callId, merchantId, target, idempotencyKey, request); `CallRequest` stays an open marker interface until Shift 8 seals it with the §9.1 command kinds. Every order event carries a full snapshot (amount, expiry, and `paidWith` exactly for paid and fulfilled), so an order first seen through a later event is still complete. | The first is naming only. The second avoids guessing command shapes before the dispatcher exists. The third is what lets the order machine apply events in any order. |
| D28 | (Shift 2) **Ledger layer details for §7.3.** Layer 1 sums debits and credits separately, each with exact arithmetic, so overflow never depends on entry order. Entries are kept in canonical order (account, then amount), and the equality check of §7.4 compares merchant, cause and entries, not `postedAt`. `postedAt` is kept at microsecond precision, as PostgreSQL stores it. `LedgerTxn.unchecked` skips layer 1 so tests can prove that each store (layer 2) and PostgreSQL (layer 3) reject a bad transaction on their own. ArchUnit (`UNCHECKED_LEDGER_TXNS_ONLY_IN_SIM`) lets only the simulator and test code call it, as with canaries. | Without a way past layer 1, layer 2 would be unreachable code that no test could exercise. The confinement keeps production on the validating path. |
| D29 | (Shift 2) **PostgreSQL details for §7 and §12.** Both roles are NOLOGIN; deployments grant them to login users. The migration makes `driftline_migrator` the owner and gives `driftline_app` SELECT and INSERT only, plus UPDATE on `account_balances`, which is a derived cache used by ROW_LOCK. A merchant's accounts are created on first posting (`ON CONFLICT DO NOTHING`), so concurrent *first* postings of one merchant wait for each other, which is acceptable with one ledger writer per merchant (D8); ROW_LOCK takes row locks in ascending account order so that postings cannot deadlock. The snapshotter is a `PostgresStore` method, not a `Store` or `Tx` method, because only APPEND_SNAPSHOT has snapshots, so parity does not apply. Triggers force `created_xid` and `xid`, so raw SQL cannot forge them; they also reject an entry that posts to another merchant's account, and TRUNCATE. Flyway is a test dependency of `store-postgres` only; Boot runs the same migrations in `app` (§18). Testcontainers uses `postgres:17` pinned by digest, with fsync off because the tests check behaviour, not durability. | Least privilege and in-database enforcement, without inventing a deployment layout before Shift 6. |
| D30 | (Shift 3) **Matcher first cut and its repositories.** One `PartitionBatch` runs in one Store transaction: records below the stored offset are skipped (a replay), then the dedupe row is claimed on `(merchant, source, eventKey)`. A repeat with the same payload hash is a duplicate; a repeat with a different hash is counted as `KEY_REUSED` and not applied. Every state-machine decision is appended to its transition log. CAPTURE is posted when a payment's decision is APPLIED and it is SUCCEEDED. ORDER_MATCH is posted when the order is paid with that payment, the payment succeeded without a conflict, and the amounts agree; whichever side arrives second triggers it, and ledger uniqueness absorbs repeats. Then the offset is committed. Refund and settlement events are claimed but not yet applied (Shifts 5 and 8). `DedupeRepo`, `PaymentRepo`, `OrderRepo` and `PartitionRepo` exist in both stores (migration V2) with a contract suite. | The smallest matcher that exercises dedupe, both state machines, postings and offsets, the parts the Shift 3 invariants check. |
| D31 | (Shift 3) **Wire formats v1 and strict JSON.** Driftline Gateway Webhook v1 and the order-event body are defined in the `wire` codecs; `docs/formats.md` cites them against public gateway documentation in Shift 5. Parsing uses Jackson's streaming core only, and rejects duplicate keys, fractional or exponent numbers, arrays, nesting deeper than 3, trailing content, currencies other than INR, and instants that are not UTC or are finer than a microsecond. Unknown fields are ignored. `payloadSha256` hashes the canonical re-encoding, so whitespace and field order cannot split one event in two. The webhook signature is verified on the raw bytes before parsing (D21). | Duplicate keys are a known way to make two parsers read different bodies under one signature. Microsecond precision keeps a stored state equal to the event it came from in PostgreSQL. |
| D32 | (Shift 3) **Simulation first cut.** The `calm` profile has 2 merchants, 2 partitions, 20 customers, 70 to 90 orders over one traffic day, and 7 drain days. Scenarios are weighted 70/8/6/6/5/5 (normal, payment failed, abandoned, late success, duplicate capture, debited with no order). World models deliver at least once (retries after 30 s, 2 min, 10 min, 1 h and 6 h, with the same key) and are acknowledged by a 2xx reply. The gateway settles each day at 10:00 IST, net of a 2% fee and 18% GST on it; settlement files reach Driftline in Shift 5. A matcher step runs exactly one Store transaction. Two checks join the §11.7 list: `INV-NO-ANOMALY` (no ledger conflict and no reused key in a fault-free run) and `INV-DRAINED` (at the end every log record is processed and every world message acknowledged). Shift 3's INV-ONE-OUTCOME checks status, because the outcome field arrives with resolution. | The fault-free baseline every later fault is measured against. The two extra checks catch a stuck matcher or a lost message, which the step invariants alone would not. |
| D33 | (Shift 4) **Fault injector, first set.** Shift 4 switches on the ten faults it can target: `NET_DROP`, `NET_DUP`, `NET_DELAY`, `PRODUCE_AMBIGUOUS` and `MERCHANT_KEY_REUSE` as *rate* faults (rolled per message or request, 1–30% depending on the fault), and `NODE_CRASH`, `NODE_PAUSE`, `CLOCK_JUMP` and `STORE_DOWN` as *event* faults (planned up front for the RUN phase, 1–4 per traffic day), plus `CLOCK_SKEW` (a fixed offset per node, up to ±2 min). The rest of §11.5 arrives with the component it targets: `NET_PARTITION`, `CALL_AMBIGUOUS`, `WEBHOOK_LOST` and `STATUS_STALE` with the dispatcher (Shift 8), and the file faults with settlement files (Shift 5). Each fault is on with probability 1/2 (swarm). Every roll consumes the same draws whether its fault is on or off, so turning one fault off never shifts the others. Half the matcher crashes strike **mid-batch**, after the batch's writes and before its commit. The bounds are the §11.6 liveness assumptions: downtime ≤ 10 min, store outage ≤ 5 min, pause ≤ 30 s, forward jump ≤ 15 min, backward step ≤ 2 s; world senders keep retrying every 6 h until a **3-day horizon**, far longer than any outage, so a healed run must drain (`INV-DRAINED`). | Swarm vectors with stable draws keep each seed's faults independent and its repro exact. A mid-batch crash is the case where at-least-once delivery meets a half-done transaction. Without the long horizon, a lost ack during an outage would end as a dropped message, and the run would fail for a reason that has nothing to do with Driftline. |
| D34 | (Shift 4) **Partition fencing, as implemented.** `PartitionRepo.acquire(partition, owner)` bumps the epoch and returns the offset to seek to. `commitOffset(partition, epoch, next)` throws `StoreConflict` when the epoch is no longer current, and the matcher rolls back the whole batch and drops the partition (§5.1). PostgreSQL also has a forward-only trigger, so even raw SQL cannot move a lease's epoch or offset back (V3). The simulator runs **two matcher instances** under a `Coordinator` that plays Kafka's group coordinator: a member that misses heartbeats for 10 s loses its partitions to the live member with the fewest, and assignment is sticky. A `NODE_PAUSE` longer than the session timeout therefore makes a real zombie. The coordinator never fences anyone itself; only the new owner's epoch bump does. `INV-FENCED` checks every committed offset: the epoch it was committed under must be the lease's epoch at that moment. `PartitionFencingIT` checks the race on real PostgreSQL: a zombie's commit that waits on a new owner's uncommitted acquire comes out fenced. | The store, not the coordinator, is the source of truth for ownership, as §5.1 requires. With one instance a pause could never lead to a takeover, so the fencing path would never run. |
| D35 | (Shift 4) **Quarantine of reused keys.** When an event key reappears with a different payload hash, the matcher writes a `quarantine` row (`IDEMPOTENCY_CONFLICT`) in the batch's own transaction. The row's id is derived from its `(partition, offset)`, so a replayed batch rewrites the same row (V3, `QuarantineRepo` in both stores). `INV-NO-ANOMALY` now counts a quarantine row as a bug only when the world never reused that key; a key that `MERCHANT_KEY_REUSE` really reused is expected to land there. | This closes the Shift 3 carry-over. The refused record stays visible for operators rather than living on as a counter, and the invariant still catches Driftline splitting one event in two. |
| D36 | (Shift 4) **Phases and the `pr` profile.** RUN covers the traffic days with faults on. HEAL stops new faults, restarts crashed and paused nodes, brings the store up, makes the network reliable and resynchronises clocks; a partition still without an owner after HEAL is a violation. DRAIN runs to the end of the drain days. The `pr` profile draws its shape per seed: 2–3 merchants, 1–3 partitions, 30 customers, 60–150 orders, 2 traffic days and 7 drain days. A run that exceeds 3,000,000 steps fails with `SIM-LIVENESS`. Campaigns use ParallelGC: with SerialGC, each collection paused all 8 threads at once. **Per-seed cost against the ≤ 20 ms target of §11.15:** 20–32 ms of thread time on GitHub's runners (`docs/results/sim-campaign-pr-ci-run-43709077412.txt`), and about 90 ms on the 3.9 GB WSL machine running 8 threads under memory pressure (`docs/results/sim-campaign-pr-10k.txt`). The target is nearly met in CI and missed locally. Shift 4's own target, 10,000 seeds in under 5 minutes on 8 threads, is met on both. | Drawing the shape per seed lets small and large configurations both appear in every campaign. The step cap turns a livelock into a reported failure instead of a hung CI job. Both per-seed measurements are recorded, not just the better one; profiling found no single hotspot to remove. |
| D37 | (Shift 4) **Canary expectations, for now.** C01, C05, C06 and C09 are active, with a budget of 500 `pr` seeds each. A canary counts as caught only when one of its expected invariants trips; tripping any other check counts as a miss. Until the Shift 8 invariants exist, the expected sets are: C01 {EVENT-ONCE, LEDGER-ONCE}, C05 {LEDGER-ONCE}, C06 {TRANSITIONS-LEGAL, ONE-OUTCOME} and C09 {FENCED, LEDGER-ONCE}. C05 gains BOOKS-MATCH-WORLD and C06 gains CUSTOMER-WHOLE in Shift 8. `:sim:canaries` writes seeds-to-detection to `docs/results/canaries.md`. | §11.10 lists invariants that do not exist yet. Counting only the intended invariant keeps a canary from passing because of some unrelated failure. |
| D38 | (Shift 5) **Settlement File v1 and who owns which settlement rows.** The format is specified, with public sources, in `docs/formats.md`: CSV with H, D and T records, amounts in rupees with exactly two decimals, net per line with optional itemised fee and tax, and a trailer whose count and total must match exactly. The parser is all-or-nothing and never echoes a value. Every date on the merchant's calendar (`DAILY` in v1) has one file, even an empty one, because "nothing settled" must be told apart from "the file is late". §12's single `settlement_files.status` is split by writer. The worker records receipts (`settlement_files`: ACCEPTED, or REJECTED_CONFLICT for a different file for a date or file id that already has one). The matcher records what it read from the log (`settlement_lines`, `settlement_file_ends` with COMPLETE, `settlement_matches`), so its state can be rebuilt from Kafka alone. The matcher also refuses a second file id or date on its own (FILE_CONFLICT) as a second guard. Storing the raw upload bytes arrives with the upload API (Shift 6). | One writer per table keeps the matcher's state a pure function of the log. The second guard caught BUG-002 before it could settle anything twice. |
| D39 | (Shift 5) **Watermarks, timers and ticks, as implemented.** `bound_G` and `bound_M` are 2 min, `idleTimeout` 1 min, `skewAllowance` 2 min, `orderGrace` 10 min, `graceDays` 1. A file is expected at 10:00 IST on its date; FEED_STALL fires 12 h later and is stored as an ALERT row of `breaks`. Batches carry the driver's processing time, and the matcher returns `nextWakeAt`, the processing time at which a tick could fire a timer not yet due; the driver ticks then. A tick advances idle watermarks and fires due timers, and commits the unchanged offset under its epoch, so a zombie's tick is fenced like its batches. The settlement clock is the start of the IST day of `swm`; `swm` starts at the day before the merchant's first settlement date. Timers are unique per `(merchant, kind, subject)`, and breaks per `(merchant, type, subject)`, where §12 has no merchant in either key. PENDING_PROBE and BATCH_SOLVE are not registered yet (Shifts 8 and 7). | Ticks driven by the next due timer, rather than a fixed period, keep the simulator's step count low. Adding the merchant to the keys means two merchants' payment or file ids can never collide. |
| D40 | (Shift 5) **Detection rules, as implemented (§5.6).** DEBITED_NO_ORDER fires at ORDER_CHECK when the order is unknown, CREATED, PENDING_PAYMENT, EXPIRED or CANCELLED and is not paid with any payment. LATE_SUCCESS fires when `lateSuccess` becomes true (D24). DUPLICATE_CAPTURE fires at DUP_CHECK, and again when the order is paid after DUP_CHECK fired; the kept payment's break gets `kept_by_order`. MISSING_SETTLEMENT fires at SETTLEMENT_DUE when the payment has no match. A referenced line whose payment is unknown or not yet captured is an ORPHAN held in SUSPENSE, and is attributed (SUSPENSE_RELEASE, `attributed_to`) once the payment succeeds. A short line that the itemised fee and tax explain exactly is RESOLVED_AUTO by the matcher, with `pricing_drift` as evidence; any other short line gets a partial posting. Over-settlement and a second line for a settled payment are anomalies held in suspense. Contradictory gateway records raise STATUS_CONFLICT, and contradictory order histories ORDER_ANOMALY. Action owners are claimed at the end of each transaction in the order DUPLICATE > LATE_SUCCESS > DEBITED_NO_ORDER, and later breaks become LINKED. Evidence is a small sorted map, stored as `jsonb`. The matcher gets merchant terms (partition, fee schedule, cycle, calendar) from an immutable `MerchantDirectory`, not from a Store repository. | Re-checking duplicates when the order is paid is what lets the simulator's recall check pass when the merchant's event lags the gateway. Late attribution is what keeps a line that beats its webhook from turning into a false MISSING. |
| D41 | (Shift 5, BUG-001) **A quarantined record escalates its order.** Besides the quarantine row, the matcher raises an IDEMPOTENCY_CONFLICT anomaly for each refused record, naming its order and payment. INV-DETECTED accepts that escalation in place of the exact DEBITED_NO_ORDER or DUPLICATE_CAPTURE for that order, because the refused event may be the one that says which payment paid it. **For Shift 8:** no automatic action may run on an order with an open IDEMPOTENCY_CONFLICT. | Without it, Driftline flagged the payment the merchant had actually fulfilled, and Shift 8 would have refunded it. Never guess (D14). |
| D42 | (Shift 5) **The keyed-state cache (§5.2).** It is a `TreeMap`, not the `LinkedHashMap` §5.2 names, because the determinism rules ban the `HashMap` family. A key written in a batch is dropped from the cache at once, and the cache is refilled only from the batch's delta, which the driver applies after the commit; values are never cached on read. A rolled-back, crashed or fenced batch can therefore leave entries missing, never stale. Open payments are the ones with no settlement match; open orders are CREATED, PENDING_PAYMENT or PAID. The cache is warmed after every acquire and dropped on any ownership change or crash. | Correct under every fault the simulator injects, and simple enough to reason about. The Store stays the source of truth. |
| D43 | (Shift 5) **The simulator's settlement world and checks.** A separate settlement worker node (crashable) runs the real parser. The bank's feed writes real v1 files and applies three faults: FILE_LATE (6–36 h), FILE_DUP, and FILE_CONFLICT, which uploads a regenerated file only after the original was acknowledged, so the original always wins. These model the bank, not Driftline's infrastructure, so they continue after HEAL; 36 h is far inside the 3-day delivery horizon and the 7 drain days. Three scenarios join the workload (weights 61/8/6/6/5/5/3/3/3): a short line (half of them itemised so that the fee explains the gap), a payment the bank never settles, and an extra line with an unknown reference; all move real money. The end-state oracle derives the expected one-to-one breaks from the final world (§11.8), and INV-DETECTED requires each of them. INV-MISSING-IS-REAL runs at every step. INV-NO-ANOMALY now also fails on any anomaly the world never causes, on a FILE_CONFLICT with no conflicting upload, and on a FILE_CONFLICT raised by the matcher's guard. C07 is active: it registers SETTLEMENT_DUE on the processing clock at the end of the due date, and is expected to trip INV-MISSING-IS-REAL. | The oracle checks recall without trusting labels; the step check catches a false MISSING the moment it is raised. |
| D44 | (Shift 5) **Simulator cost.** On the same machine, under the same conditions, a `pr` seed costs 2.2× the CPU of Shift 4 (148 s against 66 s of CPU for seeds 1–200 on one thread). Steps grew 11%, store commits 28% (ticks), and log records 21% (settlement files); the rest is the detection queries. The in-memory store indexes timers by partition and by merchant (as PostgreSQL's `timers_due` indexes do) and payments by order, and reads ranges by merging sorted maps instead of copying. | Measured, not guessed: JFR showed whole-table copies in timer queries as the first hotspot. The rest is recorded rather than tuned away. |
| D45 | (Shift 5) **Ordering parity in PostgreSQL.** Every `ORDER BY` on text uses `COLLATE "C"`, so PostgreSQL returns rows in exactly the order `String.compareTo` gives the in-memory store, whatever the database's default collation. Watermarks and the settlement watermark have forward-only triggers, as leases do. | Determinism across the two stores depends on order; a locale collation would sort `pay_10` and `pay_2`, or `_` and letters, differently. |
| D46 | (Shift 6) **The Kafka record.** The value of a record on `driftline.events.v1` is the envelope of §4.2 as strict JSON (`wire` `EnvelopeCodec`): a fixed field order, `schema_version` 1, optional fields left out rather than written as null, and decoding through the envelope's own constructor, so a record that breaks the schema is refused. The key is the merchant id, the partition is set explicitly (D5), and a valid `traceparent` travels as a header. A jqwik property proves that every envelope round-trips and re-encodes to the same bytes. `wire` now exposes Jackson's streaming core as API, because `StrictJson.write` hands its callers a `JsonGenerator`. | No schema registry to run on a 4 GB laptop, and the same strictness rules as the webhook codecs (D31). A canonical encoding makes a re-emitted record byte-identical to the first one. |
| D47 | (Shift 6) **Undecodable records and the ops lookup (§5.7).** `QuarantineRepo` gains `addUndecodable` and `undecodable(partition)`. An undecodable row keeps only its log position, a reason that names fields and never values (at most 300 characters), and the raw bytes (at most 1 MiB). It shares the `quarantine` table and its derived id with reused-key rows, so one log position holds one row, and a `quarantine_shape` CHECK keeps the two kinds apart (V5). The partition driver splits a polled chunk around undecodable records and runs every part in **one** Store transaction: `Matcher.quarantineUndecodable` writes the row and commits the offset past it, a replay of that position changes nothing, and a stale epoch is fenced like a batch. `BreakRepo` gains `byId` for `GET /v1/breaks/{id}`. Both stores implement both methods, with contract tests. | A record that cannot be read must neither stall its partition nor vanish, and it must leave the evidence behind. One transaction keeps the quarantine row and the offset atomic, as §5.7 requires, without a dead-letter topic. |
| D48 | (Shift 6) **What ingest stores, and where (V5).** `merchants` holds the columns the v1 model uses: §12's `fixed_fee_paise` and `cutoff_local_time` have no meaning in `FeeSchedule` yet, so they are not stored. `merchant_webhook_secrets.secret_enc` is AES-256-GCM from the JDK, carrying its key id; the associated data binds a ciphertext to its merchant and key id; the keys come from configuration, never from the database. `api_tokens` stores only the SHA-256 of a token, and a CHECK limits scopes to the five of §10.1. `settlement_uploads` is the upload inbox: one row per merchant and file hash, raw bytes kept, moving forward once out of RECEIVED (a trigger enforces it). These are `PostgresStore`-side classes (`PostgresDirectory`, `SettlementInbox`), not `Store` methods, as with the snapshotter (D29): the simulator has no tokens and uploads files to its worker directly. **The upload flow:** the endpoint runs the strict parser first, so a malformed file is a 400 at once, then stores the raw bytes and answers `202 {accepted, key = sha256}`. The separate worker parses the stored bytes again, records the receipt with `SettlementIntake` (the code the simulator's worker now runs too), emits to Kafka, waits for every acknowledgement, and marks the upload EMITTED. CP8 sits between the emit and the mark. The receipt time is the upload time, so a re-emission is byte-identical. | Storing the raw bytes is the RECEIVED step of §6.2. The inbox removes the dual write that "parse and emit in the HTTP handler" would be. Every worker step is idempotent, so a crash anywhere just repeats the upload, and the matcher's dedupe absorbs the repeats. |
| D49 | (Shift 6) **Roles, start-up and the dev bootstrap.** `DRIFTLINE_ROLE` maps to Spring profiles: `all` runs `ingest`, `matcher` and `settlement-worker`. `migrate` is the one-shot Flyway service of §15.2: the same image as the schema owner, with no web server. `resolver` and `dispatcher` are refused until Shift 8, so no deployment can believe it runs them. The merchants, with decrypted webhook secrets, are loaded once at start-up into an immutable `MerchantRegistry`. Each merchant must have one active secret, or two while rotating, or start-up fails. **Known limit:** a merchant onboarded later, or a rotated secret, is picked up on restart, and the matcher must restart before ingest accepts that merchant's events. A dev and test bootstrap file, generated by `sandbox bootstrap` with fresh random secrets for each run and never committed, is applied idempotently when `driftline.bootstrap-file` is set. A webhook for an unknown merchant answers 401 exactly like a bad signature. A start-up guard refuses any `Canaries` bean other than `NONE`. `HttpNetwork` is wired with no targets or codecs until the dispatcher exists (Shift 8). | Explicit roles keep the one-image design honest. Immutable start-up state keeps the matcher's directory as simple as in the simulator, and the limit is written down rather than hidden. Answering 401 for an unknown merchant means an unauthenticated caller cannot learn which merchants exist. |
| D50 | (Shift 6) **The partition driver, as implemented (D19, §5.1).** One platform thread owns the `KafkaConsumer`. A newly assigned partition is paused. In the poll loop it is then acquired (the epoch bumped), its cache is dropped and warmed, and the consumer seeks to the stored offset and resumes it. Nothing blocks inside a rebalance callback. Each partition's records from one poll run in one Store transaction. A fenced commit rolls the transaction back and pauses the partition; if Kafka still assigns it to this instance after three session timeouts, the instance acquires it again. Ticks run at the matcher's `nextWakeAt` and at least every 60 s. With the store down, the chunk is re-read from its first offset. Any other exception stops the driver (fail-stop); the liveness check reports it, so the process restarts and replays from the stored offsets. `auto.offset.reset` is `earliest`: the driver always seeks before it resumes a partition, an early reset can only replay (which the matcher skips by offset), and a log truncated past the stored offset is caught because the matcher refuses a batch that skips offsets. The consumer uses the classic group protocol with the cooperative-sticky assignor. CP7 is reached before a chunk's transaction and CP6 inside it. Request bodies are read through a bounded reader that checks `Content-Length` first: still the raw bytes, before any JSON (D21), but a huge body is refused before it is read. | `auto.offset.reset=none` would throw for a paused, not-yet-sought partition. Fail-stop, plus a restart that replays, is the one recovery path the simulator already proves; anything cleverer would be a path no test covers. |
| D51 | (Shift 6) **The sandbox and the Compose e2e suite.** The sandbox runs the simulator's own world models (`FakeGateway`, `FakeMerchant`, `SettlementFeed`, `FakeBank`, and the scenario code of `Simulation.play`, now public) with every fault off, against a real Driftline over HTTP. Each injection takes the next unused merchant of the bootstrap pool, whose first settlement date is three days before today. It plays one traffic day on virtual time the day before that date, then the files for that date and the two after it. The whole world lies in the past and is delivered at once, and webhooks are re-signed with the real clock for each attempt (§10.2). The simulator's end-state `Oracle` then names the one-to-one breaks the world implies. The e2e suite injects each of the six types and requires Driftline's one-to-one breaks for each merchant to **equal** that set (no break missed, none extra) through the ops API within a time limit. It runs from the JUnit console launcher with Gradle stopped (§15.1). **What the e2e times are not:** injected events lie in the past, so stream watermarks advance by the idle rule (`idleTimeout` 1 min), and the times measure that, not production detection lag (R2 is measured in Shift 10). | One set of world models (§21.10), and an expectation the e2e suite does not have to hand-write. Equality checks precision as well as recall. Writing down what the times mean keeps them from being misread as a latency claim. |
| D52 | (Shift 6, refines D51) **Precision in the e2e suite: an extra break must already carry the evidence that heals it.** The first Compose run raised `DEBITED_NO_ORDER` for a LATE_SUCCESS payment whose order the merchant did pay. The cause is the replay, not the matcher. Before any injection both stream watermarks are idle, so the first tick moves them to processing time (§5.4), and they never move back. The injected world lies days behind them, so every event-time timer fires at the end of the poll chunk that registered it. When the success webhook ends one chunk and the merchant's `order.paid` starts the next, ORDER_CHECK finds no paid order, and the break then gets `order_paid_later`, as designed (D40; §8: "`order.paid` with p → SELF_HEALED"). Which payments this hits depends on where Kafka splits chunks, so exact equality would make the suite flaky. The suite therefore requires: every break the `Oracle` implies is raised (recall), and any other one-to-one break carries the one healing key for its type: `order_paid_later` for DEBITED_NO_ORDER, `kept_by_order` for DUPLICATE_CAPTURE, `attributed_to` for ORPHAN_SETTLEMENT. These are the keys the simulator counts as `breaks.late_evidence`. Before it asserts, it re-reads the ops API for up to 15 s, so evidence written by the next chunk can land. Any other extra still fails: a LATE_SUCCESS, SHORT_SETTLEMENT or MISSING_SETTLEMENT the world does not imply, or a DEBITED_NO_ORDER whose order is never paid with that payment. The suite prints how many breaks were raised early and healed. The same mechanism applies in production when a consumer catches up on a backlog in both sources, as after a matcher outage. The simulator's faults also produce such healed breaks: 21 in the Shift 6 10k-seed campaign (`docs/results/sim-campaign-pr-10k-shift6.txt`). | Healing evidence is written by the matcher itself and only when the late fact is true, so accepting it keeps the precision check meaningful while removing a dependence on chunk boundaries. Not counting these breaks as false positives depends on Shift 8's resolver acting on them as SELF_HEALED before any refund (§8, D41). |

---

## 20. Risks and cut lines

| Risk | Mitigation |
|---|---|
| Scope (Kafka + PostgreSQL + K8s + simulation) | Simulator-first; infrastructure kept thin (one image, one topic); every shift time-boxed with a carry-over list |
| 3.7 GB RAM on WSL (about 1.7 GB free with VS Code and Claude open), plus Spring Boot's footprint | All roles in one JVM locally with heap caps and SerialGC; sandbox without Spring (D20); Compose profiles; a swap file; kind and long campaigns in CI; a public repo so CI runners are larger (§16) |
| Simulator code growing larger than product code | World models reused by the sandbox; incremental invariants; profile budgets |
| Flaky Testcontainers or Kafka on WSL | Pinned images; retries only in fixtures, never in assertions; CI is the reference |

**Cut first if behind, in this order:**
1. seed shrinking;
2. ledger hash-block sealing;
3. signed meet-in-the-middle for refund-netted batches;
4. kind smoke in CI (keep the manifests and the script);
5. Grafana polish beyond the core panels;
6. manual OTel spans (keep the agent).

**Never cut:**
- the §2 guarantees wording and claims-lint;
- the invariants and world oracle;
- the outbox crash tests (simulator **and** Testcontainers);
- the settlement case corpus and property tests;
- 10,000 seeds per PR;
- at least 8 canaries;
- the bug log;
- the stated limits.

---

## 21. What sets Driftline apart

1. **Canary bugs.** The simulator's power is measured, not assumed: seeds to detection, per planted bug.
2. **A world oracle.** The books are checked against ground truth, not only for internal consistency.
3. **Swarm testing** of fault profiles.
4. **An exact settlement watermark** from file trailers, plus feed-stall detection: "late file" is never confused with "missing payment".
5. **Derived identifiers**, so idempotency is built into the data rather than added afterwards.
6. **Forced-assignment propagation** across lines in one file, witness-verified ambiguity, and a deterministic operation budget.
7. **Zombie-instance fencing** with epochs, exercised by pause faults.
8. **The `xid8` horizon** for correct hot-account snapshots.
9. **Compensation exposure in ₹** as a first-class metric.
10. **One set of world models** powers the simulator, the Compose sandbox, the chaos run and the load test.
11. **Determinism hazards enforced by the build** (`Set.of` salt, enum hash codes, `HashMap` order, default locale) plus a double-run digest.
12. **A CSV export the finance team can open safely.**
