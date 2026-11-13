# Driftline: requirements (source of truth)

This file is the brief, copied word for word, plus the reviewer's concerns.
Every shift checks its work against this file. If `docs/DESIGN.md` and this file disagree, this file wins, and the disagreement is recorded in DESIGN.md §19 (Decisions).

---

## Part A: The brief (verbatim)

### Project 2: Driftline, payment reconciliation proven by simulation

Driftline watches three records of the same payment, detects the moment they disagree, and resolves the disagreement before a regulatory deadline expires.

### The problem

Every online payment is recorded three times: in the merchant's order database, in the payment gateway's event stream, and in the bank's settlement file. Networks time out, callbacks arrive twice or out of order, and files arrive a day late. The three records drift apart.

The customer sees "money debited, order not placed". The merchant's finance team sees a settlement that does not match its orders and opens a spreadsheet.

The RBI treats this as a deadline problem. Its 2019 framework requires automatic reversal within T+1 day for a failed UPI transfer and T+5 days for a UPI merchant payment where confirmation never reached the merchant. Beyond that, the customer is owed ₹100 per day, payable without the customer having to complain (RBI circular).

The scale makes small error rates large. UPI processed 25.47 billion transactions in October 2026, about 822 million a day, per NPCI data (report).

Reconciliation is usually a nightly batch job. Driftline does it as a stream, so a break is found minutes after it happens instead of the next morning.

### What it does

| Break | What the records show | Automatic action |
|---|---|---|
| Debited, no order | Bank or gateway says success; merchant order is pending or failed | Re-query status; if confirmed and the order has expired, refund; start the deadline clock |
| Late success | A failure event, then a success event for the same payment | Fulfil or refund, depending on the order's current state |
| Duplicate capture | Two successful payments for one order | Refund the second |
| Short settlement | Settled amount is below the expected amount | Post fee and tax entries if they explain the gap; otherwise escalate |
| Missing settlement | Successful payment absent from the settlement file by its due date | Escalate with the evidence attached |
| Orphan settlement | A bank line that matches no known payment | Hold in a suspense account and flag |

### How it works

The simulator replaces only the upper band, so the same matcher, resolver and ledger code runs in tests and in production.

1. Ingest. A REST API accepts gateway webhooks and merchant order events. Webhooks are verified with an HMAC signature. Every write carries an idempotency key. Settlement files are parsed by a separate worker.
2. Event log. Everything lands on Kafka, partitioned by merchant, so one merchant's events stay ordered.
3. Matcher. A stateful stream processor you write yourself in Java, without Flink. It keeps open payments in keyed state, joins the three sources, and uses event-time watermarks to decide when something is late versus missing.
4. Ledger. A double-entry ledger in PostgreSQL. Entries are append-only. Every transaction sums to zero.
5. Resolver. A state machine per break. Side effects such as refunds go through a transactional outbox, so a crash can never refund twice or lose a refund.
6. Simulator. A fake bank, gateway, network and clock, all driven by one random seed.
7. Operations. OpenTelemetry traces, Prometheus metrics, a Grafana board, Docker Compose locally, Kubernetes manifests, GitHub Actions for CI.

### The hard parts

- Out-of-order and duplicate events. A success can arrive after a failure. The payment state machine needs explicit rules for which transitions are legal, and a dedupe table so replays are harmless.
- Batch settlement matching. A bank often settles many payments as one line: the sum, minus fees, minus tax. When references are missing, finding which payments make up that line is a subset-sum problem. Narrow candidates by merchant, date and batch, then solve the rest exactly in integer paise. If two different subsets fit, report the line as ambiguous. Never guess.
- Absence is a question about time. A payment is only "missing" from settlement after its due date has passed in event time. This is where watermarks and timers stop being theory.
- Exactly-once effects. Outbox pattern, idempotency keys on outgoing calls, and a uniqueness constraint in the ledger. Three layers, because each one fails differently.
- Hot accounts. A large merchant's balance row is touched by every payment. Compare row locking against append-only entries with periodic snapshots, and measure both.

### Deterministic simulation: the part that sets this apart

Databases such as FoundationDB and TigerBeetle are tested by running the real code inside a simulator that controls every source of randomness. The approach replaces the clock, the network and storage with pluggable versions, so that any failure can be replayed from its seed (Antithesis overview).

Driftline applies the same idea at student scale:

- The matcher, ledger and resolver are written against three interfaces: Clock, Network, Store. Production wires in real ones. The simulator wires in fakes.
- A single-threaded scheduler with a seeded random generator decides what happens next: deliver a message, drop it, duplicate it, delay it, crash a component, jump the clock.
- After every step it checks invariants: money is conserved; no payment has two final states; no refund is issued twice; every break is resolved or escalated before its deadline.
- A violated invariant prints the seed. Running that seed again reproduces the bug exactly.

One limit to state openly: the simulator uses an in-memory store. Real PostgreSQL behaviour is covered separately by integration tests with Testcontainers.

Keep a log of every bug the simulator finds. That list is the best interview artefact in the whole portfolio.

### Security, because Goldman Sachs and Visa ask for it

Signed webhooks with a replay window, token scopes per merchant, encrypted personal fields with masked logs, an append-only audit trail, and dependency scanning in CI. Small items; write them down in the README.

### Milestones

| Milestone | You build | Done when |
|---|---|---|
| M1 | Interfaces, payment state machine, double-entry ledger | Unit tests pass; ledger rejects any unbalanced transaction |
| M2 | Simulator with fault injection and invariant checks | 10,000 seeds run in CI on every pull request |
| M3 | Ingest API, Kafka, matcher with watermarks | One-to-one breaks are detected end to end in Docker Compose |
| M4 | Batch settlement matching, resolver, outbox | All six break types handled; ambiguous matches reported as such |
| M5 | Observability, Kubernetes manifests, load test | README carries throughput, latency and simulation results |

### Numbers to report

- Events per second sustained on your laptop, and p99 time from event to match.
- Share of injected breaks resolved automatically, per break type.
- Seeds run with zero invariant violations, and the count of real bugs the simulator caught along the way.

### Who would pay, and why

Finance operations at a mid-size online merchant. The value is hours of manual reconciliation removed, compensation avoided, and fewer "where is my money" support tickets. Find one person who does this job and ask them what their spreadsheet looks like before you build M4.

### Prior art, stated plainly

Payment gateways sell reconciliation products, and wallet or gateway clones are common on student resumes. A streaming reconciler whose correctness is shown by deterministic simulation is not. There is no public bank data, so the simulator also generates your data; model the file formats on public gateway documentation.

---

## Part B: Reviewer concerns (verbatim). Each one has to be closed.

- Kafka + PostgreSQL + Kubernetes + simulation is a large scope.
- Don't claim exactly-once processing merely because you use Kafka.
- You need to demonstrate idempotent effects, crash recovery and ledger invariants.
- Settlement matching needs real test cases, not just an algorithm implementation.

How each concern is closed: `docs/SHIFTS.md` → "Reviewer concerns: how each is closed".
