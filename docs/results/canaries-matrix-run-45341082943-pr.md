| Canary | Planted bug | Seeds to detection | First failing seed | Caught by | Expected |
|---|---|---|---|---|---|
| C01 `DEDUPE_OFF` | skip the dedupe insert | 1 | 300453410829430001 | INV-EVENT-ONCE | INV-EVENT-ONCE, INV-LEDGER-ONCE |
| C02 `NO_REFUND_KEY` | send refunds without an idempotency key | 4 | 300453410829430004 | INV-REFUND-ONCE | INV-REFUND-ONCE |
| C03 `MARK_BEFORE_SEND` | mark the outbox command done before sending | 1 | 300453410829430001 | INV-CUSTOMER-WHOLE | INV-CUSTOMER-WHOLE, INV-BOOKS-MATCH-WORLD |
| C04 `SKIP_TTL_LOOKUP` | resend a refund without the lookup | 166 | 300453410829430166 | INV-REFUND-ONCE | INV-REFUND-ONCE |
| C05 `LEDGER_UNIQUE_OFF` | post without the cause uniqueness check | 1 | 300453410829430001 | INV-LEDGER-ONCE | INV-LEDGER-ONCE, INV-BOOKS-MATCH-WORLD |
| C06 `SUCCESS_TO_FAILED` | allow SUCCEEDED to FAILED | 122 | 300453410829430122 | INV-TRANSITIONS-LEGAL | INV-TRANSITIONS-LEGAL, INV-ONE-OUTCOME, INV-CUSTOMER-WHOLE |
| C07 `PROCESSING_TIME_WATERMARK` | treat missing by wall clock | 7 | 300453410829430007 | INV-MISSING-IS-REAL | INV-MISSING-IS-REAL |
| C08 `FIRST_SOLUTION_WINS` | subset-sum returns the first fit, with no ambiguity check | 1 | 300453410829430001 | INV-INFERRED-HONEST | INV-INFERRED-HONEST, INV-MISSING-IS-REAL |
| C09 `NO_FENCING` | commit without the epoch check | 1 | 300453410829430001 | INV-FENCED | INV-FENCED, INV-LEDGER-ONCE |
| C10 `IGNORE_ESCALATE_BY` | never escalate on time | 1 | 300453410829430001 | INV-DEADLINE | INV-DEADLINE |
| C11 `BREAK_SCOPED_REFUND_KEY` | refund key per break, and each break acts for itself (both halves of D11 off) | 5 | 300453410829430005 | INV-REFUND-ONCE | INV-REFUND-ONCE |
