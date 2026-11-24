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
| D53 | (Shift 6, BUG-003) **The simulator's matcher takes a fenced partition back, as the production driver does.** A fenced `MatcherInstance` drops the partition, as before. If after three session timeouts (30 s) the coordinator still assigns that partition to the same member id, the instance acquires it again: the epoch is bumped and it resumes at the stored offset. This is the rule of `PartitionDriver` (D50, `reacquireAfter`), which the simulator's model of it lacked. A zombie that wakes and runs an assignment delivered to its expired member id may still bump the epoch first; the simulator keeps that schedule on purpose, because the production driver could do the same if a pause fell between an assignment and its acquire. Fencing keeps that safe; this rule keeps it live. `PartitionDriverIT` now also tests the production rule. | Without it, a fenced rightful owner left its partition stalled forever (INV-DRAINED, seed 437715671286076). The store stays the only judge of ownership (D34); the coordinator's assignment only says who should try again. |
| D54 | (before Shift 7) **Public desk research replaced the brief's finance-ops interview.** The brief asks: "Find one person who does this job and ask them what their spreadsheet looks like before you build M4." The project owner could not reach anyone who does the job, so on 2026-11-15 they chose to start M4 from public research instead: Shopify Community threads by merchants and bookkeepers, Reddit posts the owner collected (not linked), Indian gateway documentation and vendor guides, and RBI's circular RBI/2019-20/67 for the refund deadlines. All of it is in `docs/research/finance-ops-desk-research.md`, with its sources and its limits. It is never called an interview. Questions it could not answer (hours spent, the ranking of break types, deadline tracking, the evidence a person would trust, one real sheet's columns) stay open, and the interview guide remains in place for a real conversation later. Every Shift 7 change that comes from this research cites it. | A documented, sourced substitute is better than guessing silently or inventing an interview. Writing the deviation down keeps the brief's intent visible and the gap honest. |
| D55 | (Shift 7) **The batch solver, as implemented (§6.4).** `SubsetSum` answers NONE, UNIQUE, AMBIGUOUS (two distinct, verified witnesses) or BUDGET_EXCEEDED, over non-empty subsets only. Steps: divide by the gcd (NONE if the target is off the lattice); when every value is positive, the whole set is unique if it sums to the target, nothing fits if the target is larger, and the complement is solved when it is the smaller target; then the cheaper engine runs. Meet-in-the-middle takes n ≤ 40, any signs, with sorted `long[]` partial sums and `int` masks built by merging into two fixed buffers (no hash maps); its cost is estimated as 3 × (2^⌊n/2⌋ + 2^⌈n/2⌉). The capped-count dynamic programme takes positive values with (n + 1)(S + 1) cells, counts capped at 2; walking back once preferring to skip and once preferring to take gives two different subsets exactly when there are two or more. **The engine is chosen by estimated cost alone, and the budget only decides whether it may run**, so a bigger budget never changes an answer that is not BUDGET_EXCEEDED (property P6). The budget is 20,000,000 operations, which also bounds the DP table at 20 MB. Exactness: every partial sum lies within ±Σ\|values\|, so the solver requires Σ\|values\| to fit in a `long` and refuses otherwise; the matcher reports such a line as OUT_OF_RANGE instead of risking a wrapped sum. Canary C08 reports the first fit found as unique. P1–P6 hold against a brute-force oracle (n ≤ 18, negatives included), and two deliberately planted solver bugs each failed four properties. | Exact, deterministic and bounded, with answers that do not depend on how much budget was spare. The overflow rule is the weakest one that makes every operation provably exact. |
| D56 | (Shift 7) **Batch matching in the matcher (§6.4).** A file's unreferenced lines are held AWAITING_BATCH in suspense; when the file is COMPLETE a BATCH_SOLVE timer is set on the gateway watermark at the end of IST day `D − cycleDays` plus `bound_G` (the gate of step 5). The pool is every payment that SUCCEEDED, has no conflict, is not settled and is named by no line; a line keeps those whose **batch date** (IST capture date + cycle; the cutoff is 00:00 IST, since D48 stores no other) lies in `[D − lookback, D]`, in the order (capture time, payment id). The lookback equals `graceDays` (1), so every file a payment could be settled in is complete when its SETTLEMENT_DUE fires. Step 4 (`batchId`) narrows nothing in v1: webhooks carry no batch id. Propagation commits a unique line, removes its payments and starts the pass again; a line's outcome is cached against its candidate list, so it is re-solved only when that changes. **Outcomes:** INFERRED posts SUSPENSE_RELEASE against the payments' contracted fees and GST, records `MatchHow.INFERRED` per payment and the SHA-256 of the candidate set on the line; AMBIGUOUS, NO_SOLUTION, BUDGET_EXCEEDED and OUT_OF_RANGE stay in suspense as ORPHAN_SETTLEMENT with that reason; ZERO_AMOUNT is rejected and flagged. **An ambiguous line's two witnesses are stored on the line, not in the break's evidence,** because evidence values are capped at 500 characters (V6: `candidate_hash` and `witnesses` columns, each with a CHECK that ties it to its status). **MISSING_SETTLEMENT** is not raised for a payment that an unresolved unreferenced line within its lookback could hold, since its money may be in that escalated line; solving re-checks the payments it held back. This is the clause Shift 5 deferred. **Challenge detection:** a success whose batch date falls in the window of an INFERRED line raises INFERRED_MATCH_CHALLENGED on it. Case 2 (`ref_batch_exact`) is read as several referenced lines in one payout batch, since a reference names one payment (docs/formats.md). **Inferred matches post without approval.** The desk research (D54) puts "primary id missing, other evidence agrees" at *suggest for approval*. We weighed that: an inferred match is a proof of uniqueness over a gated candidate set, not a similarity score; its posting is internal (suspense to receivable) and moves no money outside Driftline; a late payment in its window challenges it; and the simulator measures it against ground truth. **For Shift 8:** no external action (a refund, a status query that leads to one) may rest on an inferred match alone. | The design's algorithm with its open details closed. Keeping witnesses on the line keeps them whole for the API and the export. Holding back MISSING_SETTLEMENT where it would be a guess matches never guessing on the matching side. |
| D57 | (Shift 7) **The simulator's batch world and checks.** On about 30% of a merchant's settlement dates its NORMAL payments are paid in one or two unreferenced lines (a coin splits them), each the exact sum of their nets; SHORT, WITHHELD and orphan-plan payments keep referenced lines. The gateway records which payments each batch line pays: the ground truth. One order in five has a catalogue price (499.00, 999.00 or 1499.00), because equal amounts are what make real lump sums ambiguous. **INV-INFERRED-HONEST** (end state) compares each INFERRED line's matched payments with the truth, unless the line is challenged. **INV-AMBIGUITY-WITNESSED** checks each AMBIGUOUS line when its orphan is raised (the same transaction) and again at the end: two distinct witnesses, each of distinct captured payments in the line's window whose nets sum to it. INFERRED_MATCH_CHALLENGED leaves the never-expected list (a success delayed past the gate causes it) and must name a captured payment. INV-DRAINED also fails on a complete file with a line still AWAITING_BATCH. **INV-DETECTED accepts an unresolved batch line in place of a MISSING_SETTLEMENT it could hold**, by the matcher's own rule, as it accepts D41's escalations. **C08 is active**, expected to trip INV-INFERRED-HONEST or INV-MISSING-IS-REAL, since a wrong inference also leaves the payment really in the line unmatched (refines D37). The sandbox sets the batch share to 0 (refines D51): its Compose suite proves one-to-one detection with exact break sets, and batch matching is proven by the corpus and the simulator. **Results** (10,000 `pr` seeds, `docs/results/sim-campaign-pr-10k-shift7.txt`): 22,793 world batch lines, 10,580 inferred (each exactly the payments the world put in it; no challenge was raised), 11,371 ambiguous, 842 over budget; of 324,626 expected one-to-one breaks, 316,856 were raised exactly, 14 were covered by D41 escalations, and 7,756 MISSING_SETTLEMENTs were covered by an escalated unresolved batch line. C08 was caught in 3 seeds; C06 moved from 22 to 117 seeds because every seed's world changed (`docs/results/canaries.md`). | Ground truth from the world, never from labels, as in §11.8. The coverage split is reported rather than folded into recall: those payments are flagged through an escalated line, not as missing. |
| D58 | (Shift 7) **The settlement case corpus, the golden files and the ops view.** The 35 cases and 6 golden files live at `core/src/test/resources/settlement-cases`; `SettlementCaseCorpusTest` in `sim` runs each through the real matcher on the in-memory store and asserts every line's status and payments, the exact set of breaks, and, where given, ledger balances (debit-positive). The golden files are real Settlement File v1 CSVs (one CRLF) run through the strict parser, marked `-text` in `.gitattributes`. `scripts/gen-settlement-cases.py` writes them with exact amounts and re-derives every expectation it can with an independent model of §6.4; it reproduces the committed files byte for byte. **A finding while writing case 12:** 40 realistic amounts (1,000–50,000 rupees) gave 38,560 fitting subsets for one target, so case 12 uses super-increasing amounts to test the 40-value boundary with a certain answer; realistic batches of 40 are almost never uniquely solvable unless the line is the whole set, and saying so (AMBIGUOUS) is the point. **The ops API** shows, on a break about a settlement line, that line with its status, UTR, candidate hash and both witnesses; `GET /v1/exports/breaks.csv` exports every break in columns from the desk research (D54): payment, order, expected gross, fee, GST and net, file, line, settlement date, UTR, amount, line status, reason and the two witnesses. | Expected results written down, checked by a second implementation, and run through the production matcher. The export columns come from what finance teams said they reconcile against, not from the data model. |
| D59 | (Shift 7) **Simulator cost.** On the same machine, seeds 1–200 on one thread cost 54.8 s of CPU against 46.3 s for Shift 6 (1.18×); the batch solver is most of the difference. A JFR profile found meet-in-the-middle's per-step array allocation to be the first hotspot, now replaced by two fixed buffers; propagation re-solves a line only when its candidates change (D56). The 10,000-seed campaign took 202 s on 8 threads. | Measured, as D44 was. |
| D60 | (Shift 8) **The break lifecycle, plans and the outbox, as implemented (§8.1, §8.3, §9.1).** `BreakLifecycle` is the §8.1 diagram as a data table, plus DETECTED → RESOLVED_AUTO (an itemised short line), DETECTED → SELF_HEALED (late evidence before the first review), AWAITING_EVIDENCE → RESOLVED_AUTO (an accepted late success then paid) and ACTION_PENDING → AWAITING_EVIDENCE (a lookup must be answered first); there is deliberately no ACTION_PENDING → SELF_HEALED. LINKED and ESCALATED count as handled for INV-DEADLINE. Each break carries a **plan** (V7): deadline rule, deadline, escalate_by, review_at, action (only `REFUND`, set at most once), resolved_at and compensation, instants kept to the microsecond (as D28). Outbox commands carry flat string maps for args and results, like evidence; a claim computes each command's retry time from its attempt; call ids are `commandId.attempt[.lookup]`; the first result wins. **Playbook specifics:** a refund needs the gateway to confirm the success first (at most `maxStatusQueries` = 3 queries, else STATUS_CONFLICT and escalation); an order merely past `expiresAt` is never refunded, only one the merchant expired, cancelled or paid with another payment, and a merchant who never says escalates at escalate_by; MISSING_SETTLEMENT, unexplained short lines and anomalies escalate at once; each escalation enqueues an ESCALATION_NOTIFY. | The lifecycle as data is what lets the simulator check every committed transition (INV-TRANSITIONS-LEGAL). Leaving out ACTION_PENDING → SELF_HEALED means a break that may have refunded is never quietly healed (BUG-004 is the case that tested it). |
| D61 | (Shift 8) **The pending-order probe (§9.2).** Keyed by order, because a lost success can hide a payment entirely but never the merchant's order: a PENDING_PROBE timer on STREAM_GM at the order's first event + 30 min sends up to 3 break-less STATUS_QUERY_ORDER commands (the gateway lists the order's payments), and every outcome Driftline has not seen is published as an observation. It is skipped once the order is paid with a known success and no other payment of the order is still PENDING (D69). `outbox.break_id` is nullable for these commands only (V7 changed before it left the machine). | WEBHOOK_LOST makes some payments knowable only by asking; asking per order needs no break to exist first. |
| D62 | (Shift 8) **The new world faults and their assumptions.** CALL_AMBIGUOUS (the gateway applies a refund and its reply is lost; 20–200‰), WEBHOOK_LOST (the gateway gives up on a webhook; 5–50‰) and STATUS_STALE (status answers lag by 1–10 min; 50–400‰); counters `fault.gateway.{ambiguous,keys.replayed,webhooks.lost,status.stale}`. NET_PARTITION is not modelled: node crashes, drops and delays cover the dispatcher's failure modes. The simulated gateway forgets a refund key after 5 min (production: 24 h), so modelled outages of up to 10 min can outlast a key. **Assumption made explicit:** a request reaches the gateway within a bound well inside the key TTL (`Simulation.GATEWAY_DELIVERY_BOUND_MILLIS`, 1 min; production: a 30 s call timeout against 24 h), and the network caps gateway-bound delays there: before the cap, seed 15 delayed a first attempt past its key's expiry and refunded twice, a modelling inconsistency rather than a Driftline bug, so no BUGS row. The world's refunds come from the float when the payment is unsettled (it is then never settled) and from the reserve when it was settled (the merchant owes it back: the negative receivable the REFUND posting records). The key-age settings first written here were removed by D70. | A fault is modelled when it exercises a distinct recovery path; the delivery bound is the one assumption an idempotency key cannot remove, so it is stated and enforced rather than left implicit. |
| D63 | (Shift 8) **The simulator's Shift 8 checks.** INV-REFUND-ONCE, INV-NO-REFUND-OF-FULFILLED, INV-BOOKS-MATCH-WORLD, INV-CUSTOMER-WHOLE (a payment with a decided refund must be refunded by the world: no decided refund is lost; escalation covers only undecided payments) and INV-DEADLINE (handled by max(escalate_by, detected_at) + 1 h, checked every 60 s of virtual time and at the end; the hour covers the longest modelled resolver or store outage, 10 min, plus a review). Refined by the first campaigns: LATE_SUCCESS needs a *reported* failure; STATUS_CONFLICT is world-caused only after ≥ `maxStatusQueries` stale answers about the payment; a lost-success payment is covered by an escalated break on the line carrying its money when that line is an unreferenced batch line or its reference stayed stale for every query (`oracle.covered.escalated_line`); a payment refunded before settlement is not MISSING and not a batch candidate. **Canaries:** C10 forgets the deadline instead of spinning on it (which tripped the step limit first). C11 alone (a break-scoped key) went unseen in 200 seeds, because D11's single action owner already keeps a payment to one refund, so C11 plants both halves of D11 off. **Forced crash points:** `--crash CP1..CP5` strikes the first 3 times the point is reached, for 5 s, and dispatcher points strike only a REFUND call; CP6–CP8 stay armed by NODE_CRASH faults. Results: all 11 canaries caught (`docs/results/canaries.md`). | Each claim of §9.4 has an invariant that fails when it is broken, and each defence layer a canary that shows the simulator noticing when it is removed. |
| D64 | (Shift 8, BUG-005) **The orphan reference query.** An ORPHAN_SETTLEMENT whose line names a payment (UNKNOWN_REFERENCE, REFERENCE_NOT_CAPTURED) gets a STATUS_QUERY_PAYMENT about that reference, asked again after `requeryAfter` while the answer is not final, up to `maxStatusQueries`. A new outcome is published as an observation; the matcher attributes the line (SUSPENSE_RELEASE), the orphan heals, and any break the payment implies, such as a DUPLICATE_CAPTURE, is raised and resolved. | Found by INV-CUSTOMER-WHOLE (seed 7): every webhook of a duplicate was lost on an order paid with the other payment, so the probe rightly skipped it, and its only trace was a referenced settlement line. |
| D65 | (Shift 8) **The dispatcher over HTTP, the production drivers and the sandbox's world APIs.** One codec, `wire.call.CallWire`, serves both ends: the app encodes requests and decodes replies into the same fact maps the simulator's world models answer with; the sandbox parses requests and encodes those facts. Routes: gateway `GET /v1/merchants/{m}/payments/{p}`, `GET …/orders/{o}/payments`, `POST` and `GET …/payments/{p}/refunds`; merchant `POST /v1/merchants/{m}/orders/{o}/confirm`; Driftline `POST /internal/v1/observations` (service token, `observations:write` only, sent to no other target); ops `POST /v1/escalations`. Lists are objects keyed by id (StrictJson has no arrays); a lookup reports the earliest refund. **Only an answer is a result:** any other status or an unreadable body records nothing, so the lease runs out and the call is retried with the same key, and a refund is never inferred FAILED from an HTTP status. `ResolverDriver` and `DispatcherDriver` are platform threads like the settlement worker, reporting failure through liveness; each sleeps until its next due time but at most 1 s, since the other roles may run in other processes; replies are queued for the dispatcher thread, so every Store write happens there; CP2, CP3 and CP4 sit in the drivers, CP1 and CP5 in the core. The resolver and dispatcher roles now run (ending D49's refusal), also inside `all`; the dispatcher refuses to start without its four URLs and its token. The sandbox keeps each injected world alive, and `Scheduler.advanceTo` brings its clock to real time before each call and every 200 ms, which also delivers what a call set off; `GET /v1/stats` counts refunds, payments refunded more than once and escalation notices. Compose worlds lie at least four days in the past (D51), so there DUPLICATE_CAPTURE is past its escalate_by and is escalated, and LATE_SUCCESS is refunded because its order expired long ago; the accept-and-fulfil path needs a live order and is proven by the simulator and `ResolverTest`. **Evidence:** `OutboxCrashIT` (CP1–CP5, exactly one refund and one REFUND ledger transaction each), `docs/results/e2e-compose-shift8.txt`. | One codec means the sandbox cannot accept a request shape the app does not send, or the reverse. Treating anything but an answer as no result keeps every ambiguous case on the safe path the lookup was built for. |
| D66 | (Shift 8) **Manual resolution.** `POST /v1/breaks/{id}/resolve` with a note (scope `breaks:resolve`; the dev ops token has it) runs `ManualResolution` in the core: only ESCALATED → RESOLVED_MANUAL; the note is required, 1–482 characters, so `resolved by hand: ` + note fits a 500-character reason; the plan records resolved_at and the compensation exposure at that moment; one hash-chained audit row names the token's principal, the action `BREAK_RESOLVE_MANUAL` and the merchant, type, subject, from and to states, compensation and note. A break the token may not see is 404; one not ESCALATED, or closed a moment earlier by someone else (the row lock in `transition` refuses the second), is 409. Moving money by hand (MANUAL_ADJUSTMENT) is not part of it. | A person closes only what the resolver handed over, and every such close is attributable and tamper-evident. |
| D67 | (Shift 8) **The chaos run and `driftline verify`.** `DRIFTLINE_ROLE` accepts a comma-separated list of service roles; `verify` is a one-shot role like `migrate`. Verify re-runs read-only checks until clean or `wait-for` runs out (5 min in Compose): the ledger balances; one REFUND command per payment; every transition is in the lifecycle table; no DETECTED, unplanned break older than 10 min; no unhandled break an hour past escalate_by; no outbox command unclaimed 2 min past due; and, with the sandbox, no payment refunded twice by the gateway and REFUND ledger transactions equal to the refunds it executed. `scripts/chaos.sh` (CI job `chaos-compose`) runs the matcher and the dispatcher in containers of their own, SIGKILLs them in turn every 5–15 s during 3 minutes of traffic (gaps from a printed seed), then runs verify; a run with no refund, or with a role never killed, fails, because it would prove nothing. The shared Compose set-up is `scripts/lib/compose-stack.sh`. **Evidence:** `docs/results/chaos-compose-shift8.txt`. | The simulator proves the protocol; the chaos run proves the same claims hold with real processes, a real database and real SIGKILLs. Failing an empty run came from a first run whose traffic ended before any useful kill. |
| D68 | (Shift 8; the Shift 7 carry-over, missed in phase 2 and done in phase 5) **Unresolved batch lines are escalated at once.** An ORPHAN_SETTLEMENT whose reason is a batch-matching outcome other than INFERRED (AMBIGUOUS, NO_SOLUTION, BUDGET_EXCEEDED, OUT_OF_RANGE, ZERO_AMOUNT) is escalated on its first review; an ambiguous one names its two witnesses in the reason, and the line keeps them whole for the ops API. A referenced orphan still waits for attribution until escalate_by, with D64's query. The sandbox's `AMBIGUOUS_BATCH` injection plants such lines in Compose (at least 3 orders at 499.00, every settlement date a batch day, each day paid in two lines, so a line of k of n equal nets fits C(n, k) ≥ 2 subsets), and the e2e suite requires them AMBIGUOUS with two witnesses and escalated. | Batch matching never guesses, so a line it could not resolve can only be resolved by a person; waiting four hours first would only delay that. |
| D69 | (Shift 8, BUG-006 and BUG-007) **Pending payments as possible members of batch lines.** Payments Driftline still sees as PENDING (unsettled, unconflicted, named by no line, not refunded) may be captures whose success webhook was lost, so they are possible members of a file's unreferenced lines: an INFERRED outcome must stay the unique solution with them added, or the line is AMBIGUOUS with the inference and the wider solution as witnesses. Limits (BUG-007): a pending payment whose first event's batch date is after the file's date is not a possible member, since no capture precedes the first event; and when a payment leaves PENDING or turns conflicted, every AMBIGUOUS line naming it in a witness, in complete files from its first event's batch date to today, returns to AWAITING_BATCH and is solved again in the same transaction (an orphan already escalated by D68 keeps that state for a person). INV-AMBIGUITY-WITNESSED judges "possibly captured" by Driftline's knowledge (last status PENDING, first seen in time). Phase 4 shipped the first half in code citing "D65" and "BUG-006" without writing either down; both were recorded in phase 6. | Nothing can tell a lost capture from a payment still pending, so an inference that ignored such payments would be a guess; bounding them by their first event and re-solving when they resolve keeps the ambiguity reports true. |
| D70 | (Shift 8, BUG-008; supersedes §9.2 step 6) **Every refund retry looks the refund up first.** A REFUND retry sends REFUND_LOOKUP first whatever the age of its first attempt, and resends only if no refund exists and only under its lease. No clock is involved: if the gateway has forgotten the key, the refund happened more than the key TTL ago, far past the lookup's lag, so the lookup shows it; if it still knows the key, the resend is deduplicated. The cost is one read per retry. `gatewayKeyTtl` and `lookupMargin` left `Dispatcher.Config`, the app's properties and Compose. D62's delivery bound remains. Canary C04 now removes this lookup and is still caught (89 seeds). | The age rule depended on the dispatcher's clock having been right at the first send; seed 8551's clock ran 11 minutes fast, made an expired key look fresh, and the payment was refunded twice. |
| D71 | (Shift 8, BUG-009; refines §8.2) **A duplicate capture waits for the merchant.** §5.6 keeps the payment `order.paid` names, or else the earliest, so until `order.paid` arrives the kept payment is a default. A DUPLICATE_CAPTURE is refunded only once its order is paid with another payment, or has expired or been cancelled; while the order awaits payment the break waits in AWAITING_EVIDENCE, woken by order events and re-checked every `recheckAfter`, until escalate_by; an order the merchant never reported is escalated (D15). **Results** (10,000 `pr` seeds, `docs/results/sim-campaign-pr-10k-shift8.txt`, `docs/results/auto-resolution-shift8.md`): closed without a person: DEBITED_NO_ORDER 88.4%, LATE_SUCCESS 90.9%, DUPLICATE_CAPTURE 85.1%, SHORT_SETTLEMENT 50.2%; MISSING_SETTLEMENT and unresolved batch lines are escalated by design (figures after D72). | Refunding the default before the merchant chose was a guess (D14): in seed 1298 the merchant paid with the refunded payment, the other then became the duplicate, and the customer got both back for a fulfilled order. |
| D72 | (Shift 8, BUG-010; refines §6.4 and D69) **Batch lines wait for a payment sweep.** Before a file's unreferenced lines are solved, the matcher enqueues a `PAYMENT_SWEEP` for each capture day their window covers (`D − cycle − lookback` to `D − cycle`), with a command id derived from (merchant, day, attempt). The gateway's captures API (`GET /v1/merchants/{m}/days/{date}/captures`) lists that day's captures as of a stated time; the dispatcher publishes each one Driftline has not seen succeed as an observation. The lines are solved once every day's answer is **final** (as of the day's end or later; a stale view is asked again, up to 5 times, then accepted) and **applied** (every listed capture known as SUCCEEDED, or 30 minutes after the answer, so one observation that never lands cannot hold the lines for ever); until then BATCH_SOLVE is set again a minute later on the processing clock. Sweeps happen only for files with unreferenced lines, so a merchant without batch days is never swept. Migration V8 adds the command kind; a sweep, like a probe, belongs to no break. **Limit:** one answer per day; a gateway that pages its list would need a cursor. Rejected alternative: probing every paid order once (one gateway read per order, and blind to a duplicate captured after the probe). | A capture that left no event at all is invisible to every other path (D61, D64, D69); the gateway's own list is the one record it cannot be missing from, and asking for it only where a lump sum must be explained keeps the cost to a few calls per batch day. |
| D73 | (end of Shift 8) **Known weaknesses, stated rather than hidden.** What Driftline does not do, or cannot show, after Shift 8; the README's limits section copies this list with §2. (1) **Moving money by hand** (`MANUAL_ADJUSTMENT`, §7) is designed but not built; it is scheduled for Shift 9 with the audit work, and until it ships a person can close a break (D66) but cannot post a correcting ledger entry. (2) **The Compose e2e cannot show a duplicate refund or an accepted late success:** its injected worlds lie in the past (D51), so a duplicate is past its escalate_by and a late success's order expired long ago; the simulator and `ResolverTest` prove both paths on every PR. (3) **The payment sweep takes a day's captures in one answer** (D72); a gateway that pages its list would need a cursor. (4) **Delivery bound:** a refund attempt held in the network longer than the gateway's key TTL could still be refunded twice; D62 states the assumption and the simulator enforces it. (5) **Configuration is read at start-up** (D49): a merchant onboarded or a webhook secret rotated later needs a restart. (6) **Cost:** a `pr` seed takes 244 ms on 8 threads locally, against 195 ms before the sweep. (7) **§2.1's refund row is out of date:** it says a lookup precedes a retry older than the key TTL, but since D70 a lookup precedes every refund retry; the README must state D70's rule. | A limit written down can be judged; one left out reads as a claim (§2, R20). |
| D74 | (Shift 9) **Manual ledger adjustments** (D73 (1); DESIGN §7, D8): `ManualAdjustment.post` (core, sans-I/O) appends the audit row first and posts the ledger transaction with cause `MANUAL_ADJUSTMENT:<audit seq>` in the same transaction, so both exist or neither; an unexpected ledger answer (the cause exists) rolls back. Checked before any write: entries balance (LedgerChecks, §7.3 layer 1), note required (same rule as manual resolution), a named break must be the merchant's (else 404). `POST /v1/merchants/{m}/ledger/adjustments`, body `{"note","break_id"?,"entries":{"ACCOUNT":"signed amount"}}` (an object, since StrictJson has no arrays: one entry per account), new scope `ledger:adjust` (V9 widens `api_tokens_scopes_check`), ops tokens only (a merchant does not adjust its own books); the dev ops token gets it. 201 with audit_seq and cause; 400 unbalanced or malformed; 403; 404. | D8 designed manual adjustments and no shift built them (D73 (1)). The ledger posting and its audit row commit together or not at all, so the books never move without a named token and a reason. |
| D75 | (Shift 9) **verify-audit** (§7.6, §14): `core.audit.AuditChain.verify` walks the rows from seq 1: no gap, each prev_hash = the previous row's hash (GENESIS first), each hash = SHA-256 over prev hash + canonical entry. One-shot role `verify-audit` (prints rows and verdict, exits non-zero naming the first bad row); the same check is "audit chain intact" in `driftline verify`. Privileges (app role: no UPDATE/DELETE) and triggers (owner too) stop edits; the walk catches a superuser's edit with `session_replication_role = replica` (AuditTrailIT). **The CSV export** (§14): a cell starting with `= + - @`, tab or CR gets a leading apostrophe and is quoted (OWASP CSV injection); a negative amount would therefore export as text, and the export has none. | Privileges and triggers stop edits made through the database; walking the chain finds the edits that bypass them. Escaping the CSV keeps the finance team's spreadsheet from running a formula an attacker wrote. |
| D76 | (Shift 9) **Webhook secret rotation** (§10.2, §14; supersedes the secret half of D49's limit and of D73 (5)): `POST /v1/merchants/{m}/webhook-secrets/rotate` `{"kid","secret","overlap_seconds"?}` (scope `secrets:write`, added to V9 with `ledger:adjust`; ops tokens only). `PostgresDirectory.rotateWebhookSecret` in one transaction: a per-merchant advisory lock; refused while two secrets are active (IN_PROGRESS, 409) or if the key id was ever used (KID_TAKEN, 409); every active secret's window closes at now + overlap (1 min to 7 days, default 24 h); the new secret (32–512 bytes, encrypted with its row's context, D48) is active from now; an audit row WEBHOOK_SECRET_ROTATE (new kid, retired kids, retire time; never the secret) is appended through the store's own audit code on the same connection, so both commit together. The app role gets `UPDATE (active_to)` only, so a stored secret can be superseded but never rewritten. `MerchantRegistry.refreshSecrets` reloads active secrets (a merchant with a count other than 1–2 keeps its previous ones and is logged); `SecretRefresher` runs it every `driftline.secrets.refresh-interval` (30 s) and the endpoint once at once; the plaintext is wiped after encryption. New merchants still need a restart (D49). `Secrets.toString` no longer prints the AES keys. | A leaked or old secret can be replaced while the gateway keeps sending: both secrets verify during the overlap, the old one can be retired but never rewritten, and every rotation is audited. |
| D77 | (Shift 9) **Personal data** (§14 rows "Encrypted personal fields" and "Masked logs"): a gateway webhook may carry `"customer":{"vpa","phone","email"}` beside `"payment"` (payment events only). `wire.webhook.CustomerContact` validates and normalises each field (VPA `[a-z0-9._-]{2,64}@[a-z][a-z0-9]{1,31}`, a 10-digit Indian mobile with an optional +91, an email up to 254 characters; all lowercased); an invalid field is dropped and counted, never fatal; a malformed `customer` counts as none; `toString` prints no value. The contact travels beside the event (`IngestResult.Accepted.customer`), never in it: not in the canonical payload, its hash, Kafka, the core or the simulator, so dedupe and identity ignore it. Ingest stores it before publishing, in `payment_customers` (V10, append-only by trigger, app role SELECT and INSERT; first contact per payment wins) through `store.postgres.CustomerVault`, an app-only class like SettlementInbox (not a Store method: the sim has no personal data, so Store parity does not apply); a vault outage answers 503, so the gateway retries and nothing is published first. `app.pii.PiiCipher` encrypts each field with AES-256-GCM through SecretCipher under its own keyring (`driftline.pii.active-kid`, `keys`; kid in each ciphertext, so keys rotate), separate from the webhook-secret keys, with AAD `pii/<field>/<merchant>/<payment>` so a ciphertext moved to another row or column fails; the VPA's blind index is HMAC-SHA256 under `driftline.pii.index-key` over merchant and normalised VPA (no cross-merchant linking). No keys configured: storage is off, contacts are discarded (a warning at start-up), and search answers 503. Ops API: `GET /v1/payments/search?vpa=&merchantId=` (scope `breaks:read`; a merchant token searches its own merchant, an ops token one or all; up to 1,000 per merchant) and a masked `customer` on the payment timeline; no endpoint returns a contact in clear. Logs: every log message and stack trace goes through `PiiMasking` (first as Logback pattern converters; from D78 inside the JSON log encoder) (`ab***@okaxis`, `98******21`): a net under the rule that contacts are never logged; it can miss an unusual form. Evidence: CustomerContactTest, PiiCipherTest, PiiMaskingTest (the real logback file), IngestApiTest/OpsApiTest, CustomerVaultIT (ciphertext only, append-only, search, masked timeline, all captured app output clean). In Compose the sandbox adds a fake contact to every payment webhook (`FakeContacts`, a function of the payment id; the sim never does) and `scripts/pii-scan.sh`, run from the teardown of e2e.sh and chaos.sh, fails the run if a raw fake contact appears in any service's log, the Kafka topic or any table but the vault, or if the vault is empty. Not done: no retention or erasure of vault rows (a stated weakness for the README), and a contact that arrives only on a later webhook of the same payment is stored then, but a changed contact is not (first wins). | Personal data lives in one encrypted table and nowhere else (not the event log, the core or the simulator), so no other component can leak it; masking and the scan are nets under that rule, not the rule. |
| D78 | (Shift 9) **Observability** (§13): **Metrics.** Prometheus names exactly as §13 lists, from three places. (1) Counted where the work happens: `driftline_ingest_requests_total{route,status}` by a filter ahead of Spring Security (so 401/403 count; routes `webhook`, `order_event`, `settlement_file`; paths never become labels), `driftline_webhook_rejected_total{reason}` (bounded: `unknown_merchant`, `too_large`, `malformed`, `signature_<failure>`), and, in the partition driver's listener (`MatcherObserver`), `driftline_events_{consumed,duplicate,late}_total{source}`, `driftline_event_to_match_seconds` (ingest `receivedAt` to the commit that applied the event: every applied event, not only those that change a reconciliation status), `driftline_detection_lag_seconds{type}` (from the arrival of the record that raised the break, or for a timer-raised break of the newest record in that transaction, to the commit; a break a tick raises with no record is not measured), `driftline_consumer_lag_records{partition}` (`Consumer.currentLag`), `driftline_partition_fenced_total`, `driftline_subset_sum_ops` (the cost of each search run; a cached answer runs none); and in the dispatcher's listener `driftline_outbox_attempts_total{kind,result}` (`answered`, `no_reply`, `no_result`, `lost`). For these the core's `BatchOutcome` gained `fates` (offset → APPLIED, APPLIED_LATE, DUPLICATE, KEY_REUSED), `raisedBy` (break → offset of the record being applied when it was raised) and `solveOps` (`SubsetSum.solveCounted`, `BatchMatching.match` with an ops sink); `Resolver.Outcome` became a list of `Decision(breakId, state, enqueued command ids)`; all deterministic, so the simulator is unchanged (core 373 and sim 225 tests pass). (2) Read from the books (`store.postgres.BookStats`, one REPEATABLE READ snapshot; `BookMetrics` in the resolver role every `driftline.resolver.metrics-interval`, 15 s): `breaks_detected_total{type}`, `breaks_closed_total{type,how}` and `breaks_escalated_total{type,reason}` from append-only rows (reasons mapped to 12 bounded labels by prefix, else `other`), `breaks_open{type,state}` (adds `state` to §13's `{type}`, for the board's "by type and state"; open = DETECTED, AWAITING_EVIDENCE, ACTION_PENDING, ESCALATED), `deadline_headroom_seconds{type}` (earliest deadline among open planned breaks, negative once missed), `compensation_exposure_rupees` (what the open breaks would owe under the RBI rules if resolved now), `outbox_pending{kind}`, `outbox_oldest_pending_seconds` (from first send, or due time if never sent), `settlement_lines_total{result}` (`received` and each final status, so it only grows; statuses that can still change are the extra gauge `settlement_lines_unresolved{status}`), `pricing_drift_total` (short lines explained by fees that differ from the schedule), `watermark_lag_seconds{partition,source}` (gateway, merchant, and `bank` from the settlement watermark). Read from the database so they survive restarts and count every process's work once; every resolver instance reports the same values, so the board takes `max()`. **Traces.** The OpenTelemetry Java agent 2.33.1 is in the image (fetched with a SHA-256 checksum; off unless `OTEL_JAVAAGENT_ENABLED=true`; Kafka and JDBC instrumentation off), with manual spans on the OpenTelemetry API (a no-op without the agent: `Tracing.enabled()` is then false and no trace context is read or written). `ingest.webhook` (and `ingest.order_event`) is a child of the agent's server span and its own context is what the envelope carries (D46), instead of the caller's header; `matcher.batch` per chunk links to its records' traces (at most 128); `matcher.detect` is a child of the trace of the record that raised a break; `resolver.decide` a child of the break's context; `dispatcher.call` a child of the command's context, current while the HTTP client sends, so the agent's client span joins the trace. The resolver and dispatcher may be other processes, so contexts cross through `trace_contexts` (V11: subject `brk_…`/`cmd_…`, traceparent, append-only, app SELECT/INSERT), written after each driver's own commit: best effort (a lost row loses a trace, nothing else), not a Store method (the sim has no traces). A timer-raised break continues the `matcher.batch` trace, which links to the records, not the trace of the event that set the timer; commands the matcher enqueues itself (probes, sweeps) carry no context. **Logs** are one JSON object per line (`JsonLogEncoder`: timestamp, level, logger, thread, message, `trace_id`/`span_id` from the agent's MDC, error), with message and stack trace masked as D77 requires; the D77 pattern converters are gone. **Deploy:** `deploy/prometheus` (scrape by DNS of driftline, matcher, dispatcher on 9091; 4 alert rules: oldest pending command > 10 min, an open break with < 6 h headroom, a FEED_STALL open, consumer lag > 0 and rising for 10 min; each with a promtool unit test), `deploy/grafana` (provisioned datasource and a 26-panel board generated by `scripts/gen-grafana-dashboard.py`), `deploy/otel/collector.yaml`, Compose profile `obs` (collector 0.164.0, Jaeger 2.22.0, Prometheus v3.15.1, Grafana 13.3.0; pinned by digest; about 0.8 GB of memory caps, against §15.1's 0.50 GB estimate: Grafana 13 needs about 340 MB to render the board, and at a first cap of 160 MB it thrashed, found while taking the phase 6 screenshot). Evidence: ObservabilityTest (metrics from every path; spans through an in-memory SDK; every metric the board and alerts query exists in a real Prometheus scrape, and a bogus name fails it), ObservabilityTablesIT (BookStats counts, append-only trace contexts), ObservabilityIT (real PostgreSQL and Kafka: a LATE_SUCCESS's spans from the webhook to the dispatcher's call share one trace; the scrape has the §13 metrics), `scripts/check-obs-config.sh` (CI job `obs-config`), and `OBS=1 scripts/e2e.sh` (CI's e2e job) with `scripts/obs-check.sh`: Prometheus targets up and rules loaded, Grafana board and datasource healthy, and a Jaeger trace (queried through Jaeger 2's `/api/v3`, the only query API it serves) in which `dispatcher.call` descends parent by parent from the agent's webhook server span through ingest.webhook, matcher.detect and resolver.decide, with the agent's HTTP client span under the call. The agent jar is read-only (`ADD --chmod=444`) in a directory created beforehand, since a directory `ADD` creates gets the same mode and the non-root user could not enter it (found by the first Compose run). **Shift 9's proof** (`scripts/obs-evidence.sh`, about 15 minutes, Compose `core` and `obs` with tracing on, 3 rounds of every injection type): `docs/results/grafana-shift9.png` (the board over the run's own window; the subset-sum panel is empty because the sandbox sends no batch lines, D57), `jaeger-trace-shift9.png` and `.json` (one trace: the webhook's server span, ingest.webhook, matcher.detect, then resolver.decide → dispatcher.call twice, a status query and the refund, each with the agent's HTTP client span), and `obs-evidence-shift9.txt` (the run, obs-check, the Prometheus numbers behind the board, the PII scan). Screenshots come from Playwright 1.64.1 in its pinned image, installed with `--require-hashes` (`scripts/lib/screenshot.py` waits until the page shows its charts or spans and fails after 90 s; each browser run is stopped after 6 minutes). Chrome's own `--screenshot` was tried first and is not used: with `--virtual-time-budget` it never finished on a live Grafana board (a 2-hour hang), and with `--timeout` it captured a page still loading. The run's times are for injected traffic whose events lie in the past (D51), not latency results. | §13's metrics and one trace across the processes, with book numbers that survive restarts, each backed by a test that can fail; what the numbers do not measure is written next to them. |
| D79 | (Shift 9) **Supply chain and the threat model** (§14 "Dependency scanning in CI", "secret scanning", §16): **Lockfiles.** `dependencyLocking { lockAllConfigurations() }` in the Java convention plugin; a committed `gradle.lockfile` per module (and Gradle's `settings-gradle.lockfile`). A lockfile pins every resolved version, transitive ones included, and the build resolves exactly those: a bump of an explicitly versioned dependency fails resolution until the locks are rewritten, but a version a BOM manages (Spring Boot's) silently stays at its locked value, so `scripts/check-lockfiles.sh` (CI `security`) rewrites the locks and fails if anything changed; `--write` rewrites them after a version change (both behaviours were tried by hand before relying on them). **Scanners** (each run locally first, each shown able to fail): CI `security` runs gitleaks v8.30.1 over the whole git history (four findings in commit 87e00df, fake `whsec_…` test secrets later replaced, are accepted by fingerprint in `.gitleaksignore`; the history is public and is not rewritten; the only other hits are the git-ignored generated secrets of local runs), the lockfile check, and OSV-Scanner v2.7.0 over the module lockfiles through `scripts/osv-scan.sh`, which fails on any advisory whose highest CVSS score is 7.0 or more or which has no score, and prints every finding; CI `dependency-review` submits the Gradle dependency graph (gradle/actions dependency-submission) on pushes to main and pull requests, and runs dependency-review-action v5.0.0 (`fail-on-severity: high`) on pull requests (it can only be exercised on GitHub); nightly `trivy` (`.github/workflows/nightly.yml`) builds the app and sandbox images and fails on any fixable high or critical vulnerability with Trivy 0.76.1 (`scripts/trivy-scan.sh`; unfixed ones cannot be acted on, so they do not fail it; its cache is a Docker volume, so nothing root-owned lands in the tree). **What the first scans found, all fixed:** kafka-clients 4.2.2 brings lz4-java 1.11.2 (GHSA-mcr4-qmvw-px4g, 7.3; the producer compresses with lz4) and snappy-java 1.1.10.7 (two at 7.5): app constrains them to 1.11.4 and 1.1.10.12; Flyway brings Jackson 3.1.1 (several high) into store-postgres's integration tests: the Jackson BOM lifts it to 3.1.7; opentelemetry-api 1.62.0 (5.3): the OpenTelemetry BOM 1.67.0 in app, which is also the API the agent bundles (`TextMapGetter.keys` is deprecated there and now a default method, so `Tracing` no longer overrides it); the Temurin base image `21.0.12.1_1-jre-noble` has apt 2.8.3 (CVE-2026-109518, high, fixed 2.8.4ubuntu0.1): both images move to `21.0.13_9-jre-noble` (Java 21.0.13, apt fixed), five days old at the time, taken at once because it is a security fix. **Dependabot** (`.github/dependabot.yml`, valid against the schemastore schema): weekly, grouped, Gradle, GitHub Actions, the Dockerfiles and the Compose file, with a 14-day cooldown; a Gradle update whose lockfiles Dependabot leaves unchanged fails `security` until `scripts/check-lockfiles.sh --write` is pushed to its branch. **docs/THREAT_MODEL.md:** assets, trust boundaries, a STRIDE table whose every mitigation names its test or CI job (each name checked to exist; `CodecTest` gained cases for arrays, depth and duplicate keys, the strict-JSON limits it cites), the supply-chain controls, and the residual risks: no TLS or authentication inside the deployment (Kafka, PostgreSQL), no rate limiting, tokens revocable only in the database and without expiry, keys from the environment (no KMS), contacts kept forever, an unauthenticated management port inside the network, Grafana's anonymous viewer in Compose, the dev bootstrap file. Not in this phase: the `nightly` campaign and the full canary matrix of §16's `nightly.yml` (Shift 10, with the `nightly` profile), the kind smoke test (phase 5), and read-only root filesystems and NetworkPolicy (§14 "Extras", phase 5). | Every dependency pinned, scanned and reviewed, with the real findings of the first scans fixed rather than suppressed, and the threats Driftline does not answer written down. |
| D80 | (Shift 9) **Kubernetes and container hardening** (§15.3; §14 "Extras": non-root, read-only-root containers, NetworkPolicy): **Base** (`deploy/k8s/base`, kustomize): one Deployment per role (ingest with an HPA, 2 to 6 on CPU, and a PDB of minAvailable 1; matcher with 2 replicas, at most one per partition doing work, and a PDB of maxUnavailable 1; settlement worker, resolver and dispatcher with 1 each), the API Service, the `migrate` Job, a ConfigMap of everything not secret, and `secrets.example.yaml` listing each Secret's keys with no values (not in any kustomization). Every role runs one pod template: uid 10001, `runAsNonRoot`, RuntimeDefault seccomp, no service-account token, read-only root filesystem with an in-memory `/tmp` as the only writable path, no privilege escalation, all capabilities dropped, a memory limit, and startup, liveness and readiness probes on the management port. Both images now create user and group 10001 and run as `USER 10001:10001` (a numeric user, which Kubernetes needs to check `runAsNonRoot`), and Compose runs every Driftline and sandbox container `read_only` with a tmpfs `/tmp`, so the same constraints hold there. **NetworkPolicies:** default deny for ingress in the namespace; the API (8080) from anywhere; the management port (9091) only from namespaces labelled `driftline.io/monitoring=true`; egress is not restricted (the dispatcher's targets are wherever the gateway and the merchant are). Every resource carries `driftline.io/stage` (infra, migrate, app, verify) so a deployment applies them in order and waits between stages. **Overlay `kind`:** namespace `driftline`, dev-only PostgreSQL 17 and Kafka 4 (KRaft, one node; the controller voter is `localhost:9093`, since the Service exposes only 9092) StatefulSets on emptyDirs, the topic Job, the sandbox, the `verify` Job, policies letting only Driftline's pods reach PostgreSQL and Kafka (plus the topic job for Kafka) and anyone the sandbox (a dev-only fake the e2e suite drives from outside), Secrets generated from files `scripts/kind-smoke.sh` writes into the git-ignored `.dev/`, the API and the sandbox as NodePorts that `cluster.yaml` publishes on 127.0.0.1:18080 and :18085 (port-forwarding is fragile over a long suite), one ingest replica. Kind v0.33.0 with node v1.37.0 (pinned by digest): kind enforces NetworkPolicy since v0.24.0 (kube-network-policies). **`scripts/kind-smoke.sh`** (CI `nightly.yml` job `kind-smoke`; also on pull requests that change the manifests, the Dockerfiles, the script or the workflow; it needs about 5 GB, so it does not run on the 4 GB laptop, as §15.1 planned): builds, generates secrets, creates a fresh cluster, applies stage by stage, checks from an unlabelled pod that the API is reachable and PostgreSQL and Kafka are not, runs the e2e suite against the cluster (one scenario per break type, the manual resolution, the ingest security checks), runs `driftline verify` and requires it to succeed, and always saves every pod's log, checks it for raw customer contacts (D77) and deletes the cluster. **`scripts/check-k8s.sh`** (CI `ci.yml` job `k8s-manifests`): renders both trees, kubeconform v0.8.0 strict against Kubernetes 1.37, and asserts the hardening above on every Driftline and sandbox pod, the probes on every role, a default-deny policy, and no Secret in the base (a mutation that makes one role's root writable fails it). Locally the manifests were rendered and validated and the hardened images ran read-only through the Compose e2e and chaos runs; the kind smoke itself first runs in CI. | The manifests §15.3 asks for, hardened as §14 lists, and a smoke test that deploys them and runs the same e2e suite and verify, in CI because the laptop cannot hold a cluster (§15.1). |
| D81 | (Shift 10) **The hot-account benchmark** (§7.5, §17): `bench` `HotAccountBenchmark`, run by `scripts/hot-accounts.sh` in a fresh PostgreSQL 17.12 container (768 MB, `shared_buffers=128MB`, `fsync=on`, `synchronous_commit=on`, READ COMMITTED) with production's roles: Flyway migrates as the superuser, the benchmark posts as the DML login. One merchant's two hot accounts (gateway receivable, payments clearing) take every capture, each a distinct payment, from N concurrent writers (N ∈ {1, 4, 16, 64}, pool N + 2); after a 5 s warm-up, 20 s are measured: committed transactions per second, p50 and p99 commit latency of a ledger post, and p50 and p99 of a balance read taken every 50 ms alongside; under APPEND_SNAPSHOT the snapshotter (`PostgresStore.snapshotBalances`) runs every second. Each run uses a fresh merchant and ends by checking the balance equals the sum posted. A run during which the machine swapped in for 5 consecutive seconds is rejected (vmstat). Results (`docs/results/hot-accounts.csv`, `.txt`, `.svg`; laptop of §15.1): ROW_LOCK peaks near 280 tps at 16 writers and falls to about 200 at 64 with a p99 over a second, since every writer queues on the balance row; APPEND_SNAPSHOT scales to about 1,360 tps at 64 writers with a p99 near 150 ms, and pays at read time: balance-read p50 33 ms at 64 writers against ROW_LOCK's 3 ms. **Not done, stated:** nothing in production schedules the snapshotter. Nothing in production reads a balance either (only tests do; `driftline verify` uses SQL), so today the cost is nil, but a balance read under APPEND_SNAPSHOT grows with the entries since the last snapshot; a deployment that reads balances must run `snapshotBalances` periodically, as the benchmark does. | §7.5 promised both strategies measured, not argued; a fresh container, production's roles and a balance check after every run make the numbers comparable and the posts verified. The unscheduled snapshotter is stated because nothing in production needs it yet. |
| D82 | (Shift 10) **The load test** (§17; REQUIREMENTS "events per second sustained on your laptop, and p99 time from event to match"): `bench` `LoadGenerator` is an open-model generator: one scheduler thread fires each arrival at its slot (`start + k / rate`), whatever the responses do, over the JDK HTTP client's async sends, so a slow system cannot slow the arrivals and hide its latency; at most 1,024 requests are in flight and an arrival beyond that is not sent and counts as skipped. Each order is three events through the real ingest API in arrival order: the merchant's `order.created` (bearer token, Idempotency-Key), the gateway's signed `payment.succeeded` (with a fake customer contact in the sandbox's pattern, so the vault's encrypt-and-store path is part of the load and `scripts/pii-scan.sh` still checks for leaks), and `order.paid`; merchants in turn; event times of now, truncated to milliseconds (the codecs refuse finer ones; the first smoke run was 1,198 of 1,200 requests refused for that, and a unit test now covers it); the first three refusal bodies are kept in the step's JSON. `scripts/load-test.sh` runs it against the Compose `core` profile (every role in one JVM with production's 256 MB heap cap) plus Prometheus alone, with Gradle stopped and 12 merchants whose first settlement date is two days ahead (so no settlement alert fires mid-run; `BOOTSTRAP_DAYS_BACK`); it steps the offered rate up (25, 50, 100, 200, 400 events/s, 10 minutes each) and stops at the first step that is not sustained. **Sustained** (`scripts/load-evaluate.py`), over a step's last 5 minutes: the least-squares slope of `sum(driftline_consumer_lag_records)` at most 0.1 records/s, and the generator saw no refusal, transport error, skip or unanswered request, and the machine did not swap in for 5 consecutive seconds during the step (vmstat with an epoch per line; §15.1); a step during which the machine paused (a gap of more than 5 s between vmstat's once-a-second samples, or samples not covering the step) is INVALID, since it measured the pause: the first real run slept through its first step (gaps of 575 s and 29,798 s, a laptop asleep or a suspended WSL VM), came back to timeouts, a PostgreSQL restart and swapping, and its files were discarded. Then 10-minute steps at 50% and 80% of the highest sustained rate give the p50 and p99 event-to-match (`histogram_quantile` over the last 5 minutes). Records: `load-step-<rate>.json` (the generator's counts, the lag series and slope, events consumed and ingest accepted per second, event-to-match p50 and p99, swap), `load-p99-50pct.json`, `load-p99-80pct.json`, `load-summary.json`, `load-test.txt`. The step granularity bounds the answer: the true limit lies between the highest sustained step and the next. Event-to-match here measures every applied event, as D78 states. Two script bugs the smoke runs caught: the exit handler restored a failed status under `set -e` and so aborted before the teardown (it now sets `+e`), and the core teardown did not remove Prometheus (the handler removes it). Each step's record also carries, for information only (no verdict depends on them), the JVM's GC pauses and CPU share over the window and the machine's busiest second (`jvm`, `machine.window_*`), which tell a latency tail caused by the machine from one caused by Driftline. **The second real run (2026-11-22) was stopped and its files discarded**: 25 events/s sustained, with a p99 of 2.1 s that came from one 20-second CPU surge on the machine (run queue 35, 5% idle) during which a young-generation GC pause reached 2.69 s while Driftline itself used at most about 1.4 of 12 cores; 50 events/s sustained, p99 0.16 s; 100 events/s judged sustained with a p50 of 1.25 s and a p99 of 13.8 s, while the lag gauge peaked at 49. During the 200 events/s step the backlog measured by hand (the topic's end offsets minus `partition_leases.next_offset`) grew from 10,516 to 23,225 in 95 s and no partition's stored offset moved for over 20 s (Kafka up to 230% CPU near its 640 MB limit, Driftline at 463 of 512 MB); the lag gauge was not read during that step, so what the old rule would have said there is unknown. The gauge was updated only by the stalled loop, so the run stopped to fix the measurement first (D83). **The third run (2026-11-22, with D83) was stopped too**: 25 events/s sustained (backlog at most 1 over 21 samples; p50 13 ms, p99 1.18 s, the tail matching a 1.75 s full collection, `MarkSweepCompact`, while the machine had 9% idle CPU); 50 events/s was then judged not sustained only because the swap rule fired, on 4 to 16 KB swapped in each second for 6 seconds (960 KB over the 10 minutes), while Driftline had its best step (backlog at most 6, p99 0.15 s, longest GC pause 0.05 s). That exposed two faults in the rule, both fixed after seeing this result, which is stated here: (1) any swap-in counted, so one page a second, which cannot distort a measurement, failed a step; a second now counts only if it swaps in at least 256 KB (64 pages, enough page faults to cost the JVM a visible share of its time), still for 5 consecutive seconds, and the total is recorded either way. (2) A step whose machine paused or swapped was judged *not sustained*, which named the previous rate as Driftline's limit; such a step is now INVALID (exit 2), stepping stops, and `load-summary.json` says by `stopped_by` whether the limit lies between two steps (`not_sustained`), was not found (`invalid_step`: the highest sustained rate is only a lower bound) or is above every rate tried. **The fifth run (2026-11-23, with D83 and D84) is the published one** (`docs/results/load-*.json`, `load-test.txt`): 25 events/s sustained (event-to-match p50 15 ms, p99 46 ms); 50 events/s sustained (p50 28 ms, p99 1.5 s; longest GC pause 0.18 s; backlog at most 37 while the client's estimate showed 0); 100 events/s not sustained (the backlog rose to 2,889 two-thirds into the window and drained by its end, a slope of 3.1 records/s; p99 38 s; every request accepted; the client's estimate peaked at 180, a sixteenth of the backlog); so the limit on this laptop lies between 50 and 100 events/s. At 50% and 80% of 50 events/s: 25 events/s, p50 17 ms and p99 2.7 s; 40 events/s, p50 26 ms and p99 2.0 s. The same 25 events/s gave a p99 of 46 ms as the first step and 2.7 s right after the overloaded 100 events/s step, with the same p50: the tail on this laptop varies between runs of the same rate by that much, and the README reports both rather than the better one. At the 100 events/s step's busiest second, 11.3 of 12 cores were busy: the stack's containers about 5.5 (Driftline 2.6, PostgreSQL 1.8, Kafka 1.1), the generator 0.3 on average, and the rest outside both (Docker, WSL, Windows). | An open model is the only way a slow system cannot hide its own latency (coordinated omission); stopping at the first unsustained step and naming invalid steps as such keeps a paused or swapping laptop from being reported as Driftline's limit. The discarded runs are recorded so the published one is not the best of several by choice. |
| D83 | (Shift 10) **The matcher's backlog, measured from outside its loop** (§13, §17): `driftline_consumer_lag_records{partition}` came from `consumer.currentLag()`, which only the partition driver's loop reported, once per poll; a matcher stuck inside a chunk (the load test saw one stall for over 20 s) froze the gauge at its last value, which blinded both the `ConsumerLagGrowing` alert and the load test's sustained rule. Now `BacklogProbe` (matcher role) measures it every 5 s on its own thread: for each partition this instance owns, the topic's end offset (its own Kafka admin client; the driver's consumer is not thread-safe) minus the offset stored as processed (`PartitionRepo.nextOffset`, committed in the same transaction as the records' effects, so the Store port is unchanged), never below 0; a partition no longer owned drops out and its new owner reports it, so a sum over instances counts each partition once; a failed read is logged and the previous values stay. The metric name is unchanged, so the alert, its rule tests and the Grafana board need no change; the client's estimate stays as `driftline_consumer_client_lag_records`, and the load test records both, so the README can say whether the old gauge under-reported. `PartitionDriverIT` stalls a driver at CP7 with real Kafka and PostgreSQL, produces 36 more records, and checks that the probe counts all 36 while the client's estimate does not, then that the backlog drains. Two defects found on the way: (1) `MultiGauge.register(rows, true)` removes and registers every row again on each refresh, so a scrape in between could miss a series whose value had changed (the likely cause of `ObservabilityIT` missing `driftline_deadline_headroom_seconds` once in four runs; that miss was not reproduced); `GaugeRows` keeps a row's meter and changes only its value, and `BookMetrics` and the probe use it. (2) A time bomb: `ObservabilityIT`, `SecretRotationIT` and `CustomerVaultIT` bootstrapped a merchant whose first settlement date was the literal 2026-11-18; once that date passed, `ObservabilityIT` failed on a clean checkout (its spans showed a second break, raised with the first record and escalated at once, whose call the test took for the expected one); they now use today plus two days. Neither is a simulator bug, so neither has a BUGS.md row. | A gauge updated only by the loop it watches freezes exactly when it matters; measuring from outside the loop made both the alert and the load test's sustained rule trustworthy. |
| D84 | (Shift 10) **Driftline's heap is committed from the start** (§15.1): `-Xms256m -Xmx256m` in the Compose `x-app-env` block and the Kubernetes base ConfigMap (Kafka already ran with `-Xms` equal to `-Xmx`). The fourth load run (2026-11-23, with D83) sustained 25 events/s (event-to-match p50 12 ms, p99 46 ms) and failed 50 events/s: one full collection (`MarkSweepCompact`, cause "Allocation Failure") stopped the JVM for 21.8 s, so 2,116 requests timed out at the generator's 10 s, Prometheus missed a scrape and the backlog reached about 600 before draining. With no `-Xms` the heap had started small (old generation 62 MB committed, 41 MB used; eden 25 MB) and SerialGC grows it only through full collections: a young collection needs the old generation's free space to cover what may survive it, so a burst forced a full one instead, after which the heap grew to 138 MB old and 55 MB eden. Committing the heap removes that trigger; the cap, and so the most the heap can use, is unchanged, so the containers' memory limits need no change. It does not explain why that collection took 21.8 s rather than well under a second: the whole machine was saturated at the time (3 to 8% idle CPU, 24 to 33 runnable threads, about 40% in the kernel) while Driftline's JVM used about 4 of the 12 cores, and the run recorded nothing per container. So `scripts/load-test.sh` now also samples `docker stats` every few seconds (`load-dockerstats.txt`), the generator reports its own CPU time, and each step's record gives every container's mean and peak CPU over the window and at the sample nearest the machine's busiest second, beside the machine's busy cores then; what none of them accounts for is outside the stack and the generator (Docker itself, WSL, Windows). The fourth run's results are superseded by a run on this configuration rather than published. | A small starting heap made SerialGC grow only through full collections, which turned a burst into a 21.8 s pause; committing the heap removes that trigger without raising the cap. |
| D85 | (Shift 10) **The nightly campaign, the canary matrix and the seed count** (§11.15, §16, §17): the `nightly` profile is §11.15's (5 merchants on 6 partitions, so one stays idle; 1,800 to 2,200 orders from 500 customers over 5 days, drained for 7; swarm faults). On GitHub's 4-core runners (nightly run 45183995378, 4 shards of 150 seeds on 4 threads) a `nightly` seed cost 4.5 to 6.5 s of one core, 5.6 s on average, against about 0.13 s for a `pr` seed (10 shards of 20,000). A first estimate of about 40 s, from 4 seeds on the laptop, was wrong and nearly cut the campaign: on the 4 GB laptop a `nightly` seed takes about 11 s on one thread and about 30 s each on four at once, while a `pr` seed costs about the same there as on GitHub; the CI numbers are the ones that count. §16's "200k-seed `nightly` campaign" is therefore run as written: 200,000 seeds of this profile, about 310 CPU-hours (78 runner-hours) a night, about 4.5 hours with GitHub's 20 parallel jobs all busy (free for a public repository). `nightly.yml` runs (1) `campaign-nightly`: 200,000 fresh `nightly` seeds in 40 shards of 5,000, about 2 hours each; (2) `campaign-pr`: 200,000 fresh `pr` seeds in 10 shards of 20,000 (about 11 minutes each), with the regression corpus in shard 0; (3) `canary-matrix`: every canary on both profiles from fresh seeds each run (ci.yml's `canaries-quick` always starts at seed 1), with budgets of 500 `pr` and 100 `nightly` seeds; locally, from seed 1, the `nightly` profile caught nine canaries within 1 to 6 seeds and C04 and C11 at seed 10 (against 89 and 154 `pr` seeds), each by an expected invariant. Every run's seeds are its own: CI's pull-request runs take `run id x 10,000` onward, the nightly campaigns `10^17 + run id x 200,000` (`pr`) and `2 x 10^17 + run id x 200,000` (`nightly`), the canary matrix `3` and `4 x 10^17` onward (written in the workflow as 3 and 4 times 10^17 in the shell: a literal 3 x 10^17 in the job matrix reached the shell as `3E+17`, which broke run 45183995378's canary jobs), local campaigns `10^12` onward. **The seed count** (§17 "from CI artifacts"): `sim:campaign --summary` writes each shard's counts as JSON (profile, seed range, seeds run and failed, where and when); ci.yml's and nightly.yml's campaign shards upload it as an artifact kept 90 days; `scripts/seed-count.sh` lists the runs with `gh`, downloads the summaries, adds the committed local ones (`docs/results/sim-summaries/`), refuses two summaries that cover the same seeds, and writes `seed-count.json` and `seed-count.md`. **Downloads:** in run 45189941514, four of the 40 `nightly` shards failed before running a seed: about 50 jobs started together and each fetched commons-lang3 3.20.0 (D86), which the Gradle cache restored from `main` did not hold yet (a branch's runs only read that cache), and Maven Central answered 429 Too Many Requests. Each simulator job now first runs `scripts/gradle-warm.sh :sim:classes`, which resolves and compiles with up to five tries, waiting 30 to 149 s between them, so only that cheap step is ever retried, never a campaign. That was not enough: in run 45267884726 three `nightly` shards still failed all five tries, the throttling outlasting them, so the simulator jobs now also run Gradle with `--configure-on-demand`, which configures only the projects a task needs and so never resolves `:app`'s build-script classpath, where commons-lang3 3.20.0 comes in; `:sim:campaign` and `:sim:canaries` run the same in that mode, and run 45341082943 lost no shard. **Outcome** (`seed-count.md`, 2026-11-24): four nightly runs on `shift-10`, 45183995378 (70ab564; 600 `nightly` seeds as a cost probe and 200,000 `pr`), 45189941514 (c509ddf), 45267884726 (b0489bb) and 45341082943 (b624a8b), and the local 10,000 `pr` seeds: 1,355,600 campaign seeds, 565,600 `nightly` and 790,000 `pr` (shards lost to downloads ran no seed and count for nothing: 4 `nightly` and 1 `pr` in run 45189941514, 3 `nightly` in run 45267884726). Six seeds failed, all `nightly`: two were BUG-012, a real bug (fixed in 6294dca), and four were the drain oracle's error (D87, fixed in b0489bb and b624a8b). Run 45341082943, on the final simulator and core, ran its 400,000 seeds with none failing, and its canary matrix caught every canary on both profiles (`canaries.md`). Only campaign seeds count, not corpus replays, double-runs or canary seeds; runs before Shift 10 uploaded no summary and are not counted. The campaigns run on the schedule and by hand (`workflow_dispatch`), not on pull requests. **Auto-resolution shares:** (a) 10,000 fresh `pr` seeds on the final code (`sim-campaign-pr-10k-shift10.txt`, `auto-resolution-shift10.md`, by `scripts/resolution-share.py`, whose title is now an argument; first run at a8ed8d3, then re-run on the same seeds at ab9d084, after BUG-012 and D87 changed what a seed does: a few break counts moved, by at most 14 of 121,267, and no share moved at one decimal; the Shift 8 output still reproduces byte for byte), within about a point of Shift 8's; (b) `scripts/resolution-compose.sh` injects N breaks of each one-to-one type on the Compose stack, each type on its own merchant, waits until every break the sandbox expects exists and no state has changed for 3 minutes (an orphan line stays in suspense, AWAITING_EVIDENCE, until its deadline days away, so "none open" never comes), and tabulates their states from the ops CSV export (`auto-resolution-compose.md` and `.json`); breaks of another type on those merchants are listed apart, so they cannot inflate a share. | §16 and §17 ask for the nightly campaign, the canary matrix and the cumulative seed count from CI artifacts; seed ranges per run make every seed counted once, and the download failures are recorded because they cost seeds. |
| D86 | (Shift 10) **commons-lang3 in the build tooling** (§14, D79): Dependabot alert 1 (CVE-2025-48924, uncontrolled recursion in commons-lang3 below 3.18.0, medium) points at the Spring Boot Gradle plugin 4.1.2, which brings commons-compress 1.27.1 and through it commons-lang3 3.16.0 onto `app`'s build-script classpath (the build's tooling, not the application: the app's own lockfile has no commons-lang3 at runtime). OSV-Scanner reads only the module lockfiles, so it did not see it; the submitted dependency graph did. `app/build.gradle.kts` constrains its build-script classpath to commons-lang3 3.20.0 (`./gradlew :app:buildEnvironment` shows 3.16.0 -> 3.20.0); lifted, not suppressed, as D79 requires. | D79 requires a vulnerable dependency lifted, not suppressed, even when it is only in the build tooling. |
| D87 | (Shift 10) **A message Driftline accepted is drained, even if its sender gave up** (§11.8 INV-DRAINED, §2.1): the first 200,000-seed `nightly` campaign (run 45189941514) failed three seeds (209037988302897762, 209037988302822256, 209037988302934806) with "1 world messages were never acknowledged". In 209037988302897762 a merchant's `order.paid` was accepted by ingest, answered 202 eleven times and appended (p3@730), but every answer was lost on the way back, and the merchant gave up after its 3-day retry horizon (`Delivery.HORIZON_MILLIS`), all inside the 5 faulty traffic days. The simulator counted a delivery whose 2xx never reached the sender as a failure to drain. That holds for `pr` (2 traffic days, so every delivery's retries reach the reliable drain phase) but not for `nightly`: a modelling error in the oracle, not a Driftline bug, so no BUGS.md row (as D62's seed 15). `Delivery` now records separately whether Driftline answered an attempt with a 2xx (`accepted`) and whether that answer reached the sender (`acked`, which still stops the retries); INV-DRAINED counts the messages Driftline never accepted. The extra callback draws nothing from the RNG, so every run's trajectory is unchanged; all three seeds pass, and `DeliveryTest` covers a sender that gives up on a message Driftline accepted. The same run's two INV-REFUND-ONCE seeds were a real core bug, BUG-012. **Second case** (run 45267884726, seed 209053576945360396, on the fixed code): the last day's settlement file for `m_1`, held back 50,399,912 ms by FILE_LATE (the bank's feed misbehaves after HEAL too, by design), was released 88 ms before the end of the run, and no delivery takes less than 5 ms one way plus the worker's turn; files held past the end were never counted, but one released just before it was. INV-DRAINED now judges only messages first sent at least `Delivery.DRAIN_GRACE_MILLIS` (10 minutes) before the end: after HEAL the network neither drops nor holds a message (one way at most 200 ms), so a message Driftline takes is taken within a round trip, or at its first or second retry (30 s and 2.5 min later) if answered 503 first. With traffic over 7 days earlier, what the grace exempts in practice is the zero-line settlement files of the last day released in the run's last 10 minutes. `DeliveryTest` covers the cut-off; both seeds pass. | An oracle that calls an accepted message undrained fails seeds for a reason that has nothing to do with Driftline; separating accepted from acknowledged keeps INV-DRAINED strict about what Driftline itself took. |
| D88 | (Shift 10) **The README's numbers are rendered, and CI checks them** (§17 "The README tables are generated from them by `scripts/render-results`"; SHIFTS Shift 10 Prove): `scripts/render-results` (Python, standard library only) owns everything between `<!-- results:NAME -->` and `<!-- /results:NAME -->` in README.md and writes seven blocks from the raw outputs: `load` (load-summary.json, load-step-*.json, load-p99-*.json, load-test.txt), `hot-accounts` (hot-accounts.csv, .txt), `resolution-sim` (sim-campaign-pr-10k-shift10.txt, parsed the way resolution-share.py parses it), `resolution-compose` (auto-resolution-compose.json), `seeds` (seed-count.json and the BUGS.md rows), `canaries` (canaries.md from seed 1 and every committed `canaries-matrix-run-*` table; it refuses a canary not caught by an expected invariant) and `bugs` (BUGS.md). Each block ends with the files it came from and the scripts that wrote them. Every block must appear exactly once, so no table can be dropped or typed by hand; `--check` re-renders and fails with a diff if anything differs, and runs in ci.yml's `claims-lint` job. The prose around the blocks avoids repeating their numbers, so a re-run changes only the blocks; the few numbers in prose (the seed counts' explanation, the invariant count) cite their source. The README copies DESIGN §2 word for word inside the claims-lint allow block, except the refund row, which states D70's rule (D73 (7)); a diff against DESIGN §2 shows only that row and the three headings without their numbers. The 10,000-seed auto-resolution campaign was re-run on the final code for it (D85 (a)). | §17: every README number must come from a committed raw output; rendering the tables and checking them in CI makes a hand-typed or stale number fail the build. |
| D89 | (Shift 10) **The traceability pass, and how soon a break is found** (SHIFTS traceability matrix; REQUIREMENTS "a break is found minutes after it happens"; §5.5, §13): R2's evidence was `driftline_detection_lag_seconds`, but no committed result measured it, and that metric runs from the arrival of the record that raised a break to the commit, so it leaves out the waits the rules impose. The simulator now counts, for every LATE_SUCCESS, DUPLICATE_CAPTURE and DEBITED_NO_ORDER first committed, the simulated time from the world's capture of the payment (for a duplicate, the order's second capture, if later) to that commit, in buckets of 1, 5, 10, 15, 30, 60, 360 and 1,440 minutes (`Invariants.countLag`, stats `detection_lag.TYPE.le_Nm` or `gt_1d`; `DetectionLagTest`). It is a statistic, not an invariant: no bound is enforced, because a lost webhook legitimately leaves a payment unknown until a status probe or the bank's file. The 10,000-seed `pr` campaign was re-run on the same seeds with it (every other line of its totals unchanged, so the auto-resolution shares and the seed count stand), and the same seeds were run under `calm` for the fault-free times; `render-results` gained a `detection-lag` block, whose shares print 0% and 100% only when exact (else <0.1% or >99.9%). Under `pr`, 40 of 121,281 DEBITED_NO_ORDER breaks were raised sooner than `orderGrace`: seed 1000000000258 shows why. matcher-b's clock jumped 13.9 minutes forward (CLOCK_JUMP, ≤ 15 min, D33), and its next idle tick advanced the watermark by that clock (`now − bound − skewAllowance`, §5.4) and fired pay_33_a's ORDER_CHECK 63 s after the capture. This is no bug: pay_33_a's refund passed INV-NO-REFUND-OF-FULFILLED and INV-CUSTOMER-WHOLE, the seed passes every invariant, and a break raised early on a payment whose order then arrives heals itself if no action was decided yet, or is escalated (§5.6). The README states it. Settlement breaks are not measured: they can only be found when the bank's file arrives, or after the due date (§5.6). The pass also corrected two README sentences: event-to-match ends at the commit that applied the event (D78), not one that changed a status (§13's older wording), and some of the 18 invariants are checked only at the end of a run (§11.7). Every Evidence entry in the matrix now names a test, file, result or CI job that exists; R25 is marked not met as written (D54). | The brief's headline claim needed a measured number, not only a mechanism, and the traceability matrix needed evidence that exists; the early breaks are explained from a real seed rather than assumed. |
| D90 | (Shift 10) **A message Driftline took is drained, even if every answer that came back was a lost-ack 503** (§11.8 INV-DRAINED, §2.1, D87): nightly run 45420779430 (9ef801b, 200,000 `nightly` seeds) failed one seed, 209084155886090823, with "1 world messages were never acknowledged". A gateway webhook (`payment.pending` for `pay_1004_a`) was tried 16 times over the 3-day horizon; six attempts got no answer from ingest (lost on the way, or ingest down), and at each of the other ten ingest appended the event and PRODUCE_AMBIGUOUS (142‰ in this seed's swarm) lost the log's ack, so ingest answered 503; the gateway gave up inside the 5 faulty traffic days. D87 counted a message as accepted only on a 2xx, so a message Driftline had appended ten times counted as undrained: the oracle's error again, not a Driftline bug, so no BUGS.md row (as D87). The simulated ingest and settlement worker give 503 only in that case, after the append or emit, so the answer is now a named constant, `Delivery.TAKEN_ACK_LOST`, which both nodes reply with and `Delivery` counts as taken (`accepted`); it still does not stop the retries (`acked`). The change draws nothing from the RNG and feeds only the end-of-run drain verdict, so every seed's run, statistics and committed results are unchanged, and a verdict can only move from this failure to a pass. The seed passes, as do D87's and BUG-012's `nightly` seeds; `DeliveryTest` covers a sender that gives up after only lost-ack answers, and its refusing sender now answers 401. The seed count now includes run 45420779430 and one pull-request CI run: 1,765,600 campaign seeds (765,600 `nightly`, 1,000,000 `pr`), seven with a violation (two BUG-012, five the drain oracle's error, D87 and D90). | The oracle should judge what Driftline itself took; in the simulator a 503 is only ever an append whose ack was lost, and naming it ties the two nodes and the oracle to one definition. |

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
