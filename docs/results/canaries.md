# Canary sensitivity (DESIGN §11.10)

Seeds to first detection per planted bug, `pr` profile, seeds from 1, budget 500. A canary counts only when an expected invariant trips (D37).

- Command: `./gradlew -q :sim:canaries --args="--threads 8 --out ../docs/results/canaries.md"` (then this header was added).
- Base commit: becfc9d plus the uncommitted BUG-010 fix (D72, the payment sweep); date: 2026-11-17 (UTC). Every count is the same as before that fix.
- All eleven canaries are active from Shift 8. C02, C03, C04, C10 and C11 arrived with the resolver and dispatcher.
- C04 (`SKIP_TTL_LOOKUP`) now means "resend a refund without the lookup": since BUG-008 every refund retry looks the refund up first (D70), so the canary removes that lookup; it is caught when a retry outlives the simulated gateway's 5-minute key TTL.
- C11 plants both halves of D11 off (a break-scoped refund key, and every break acting for itself): either half alone already keeps a payment to one refund, so the canary needs both.
- Counts moved from the Shift 7 table because Shift 8 changed what every `pr` seed does (resolver, dispatcher, the new gateway faults): C06 from 117 to 100 seeds, C07 from 13 to 2, C08 from 3 to 5, now first caught by INV-MISSING-IS-REAL (also an expected invariant for C08, D57). The Shift 7 table is in git history.

| Canary | Planted bug | Seeds to detection | First failing seed | Caught by | Expected |
|---|---|---|---|---|---|
| C01 `DEDUPE_OFF` | skip the dedupe insert | 1 | 1 | INV-EVENT-ONCE | INV-EVENT-ONCE, INV-LEDGER-ONCE |
| C02 `NO_REFUND_KEY` | send refunds without an idempotency key | 3 | 3 | INV-REFUND-ONCE | INV-REFUND-ONCE |
| C03 `MARK_BEFORE_SEND` | mark the outbox command done before sending | 2 | 2 | INV-CUSTOMER-WHOLE | INV-CUSTOMER-WHOLE, INV-BOOKS-MATCH-WORLD |
| C04 `SKIP_TTL_LOOKUP` | resend a refund without the lookup | 89 | 89 | INV-REFUND-ONCE | INV-REFUND-ONCE |
| C05 `LEDGER_UNIQUE_OFF` | post without the cause uniqueness check | 1 | 1 | INV-LEDGER-ONCE | INV-LEDGER-ONCE, INV-BOOKS-MATCH-WORLD |
| C06 `SUCCESS_TO_FAILED` | allow SUCCEEDED to FAILED | 100 | 100 | INV-TRANSITIONS-LEGAL | INV-TRANSITIONS-LEGAL, INV-ONE-OUTCOME, INV-CUSTOMER-WHOLE |
| C07 `PROCESSING_TIME_WATERMARK` | treat missing by wall clock | 2 | 2 | INV-MISSING-IS-REAL | INV-MISSING-IS-REAL |
| C08 `FIRST_SOLUTION_WINS` | subset-sum returns the first fit, with no ambiguity check | 5 | 5 | INV-MISSING-IS-REAL | INV-INFERRED-HONEST, INV-MISSING-IS-REAL |
| C09 `NO_FENCING` | commit without the epoch check | 3 | 3 | INV-FENCED | INV-FENCED, INV-LEDGER-ONCE |
| C10 `IGNORE_ESCALATE_BY` | never escalate on time | 1 | 1 | INV-DEADLINE | INV-DEADLINE |
| C11 `BREAK_SCOPED_REFUND_KEY` | refund key per break, and each break acts for itself (both halves of D11 off) | 154 | 154 | INV-REFUND-ONCE | INV-REFUND-ONCE |
