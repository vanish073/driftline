# Wire formats

There is no public bank data, so Driftline's simulator generates its own. Its formats are modelled on public payment-gateway documentation (REQUIREMENTS, "Prior art"). This file defines the three formats Driftline accepts, and names the public document each field is modelled on. All sources were read on 2026-11-15.

| Format | Defined in code | Used by |
|---|---|---|
| Driftline Settlement File v1 (CSV) | `wire/…/settlement/SettlementFileParser.java` | settlement upload and worker (DESIGN §6.1, §6.2) |
| Driftline Gateway Webhook v1 (JSON) | `wire/…/webhook/GatewayWebhookCodec.java` | `POST /v1/webhooks/gateway/{merchantId}` (§10.1, §10.2) |
| Driftline Order Event v1 (JSON) | `wire/…/order/OrderEventCodec.java` | `POST /v1/merchants/{merchantId}/order-events` (§10.1, §10.3) |

## Sources

| # | Source | What Driftline takes from it |
|---|---|---|
| S1 | Razorpay, *Fetch Settlement Reconciliation Report* (API reference), https://razorpay.com/docs/api/settlements/fetch-recon | One row per settled transaction with `fee` ("the fees … charged to process the transaction") and `tax` ("the tax on the fee"), plus `settlement_id` and `settlement_utr` ("the unique reference number linked to the settlement"); a daily report selected by date |
| S2 | Cashfree, *Settlement Recon Reports* (partner documentation), https://www.cashfree.com/docs/partners/embedded/reports/settlement-recon-reports | Per-event `Transaction Service Charge` and `Txn ST/GST` ("tax applied on the transaction service charge"); `Event Settlement Amount` after those charges; the `UTR` of the settlement batch; amounts written as rupees with two decimals and no currency symbol |
| S3 | PhonePe, *Settlements* (developer documentation), https://developer.phonepe.com/docs/settlements | A T+1 settlement cycle; no settlement on bank holidays; per-transaction `Amount`, `Fee` and GST columns; "Settled amount = Amount + Fee + IGST + CGST + SGST", with fees shown as negatives; `BankReferenceNo` is the UTR |
| S4 | Stripe, *Payout reconciliation report*, https://docs.stripe.com/reports/payout-reconciliation | Transactions grouped into the payout batch that settled them (`automatic_payout_id`); `gross`, `fee` and `net` per transaction; amounts in major units; "a payout might arrive before its reconciliation data is ready" |
| S5 | Nacha, *ACH File Overview* (ACH developer guide), https://achdevguide.nacha.org/ach-file-overview | A header record, detail records and a control record. The control record carries "various counts … sums (debit total and credit total)" so the receiver can check that the file is complete and correct |

## Driftline Settlement File v1

One file per merchant per **settlement date**: the IST business day on which the gateway pays out. The file lists every line credited to the merchant's bank account for that date.

### Encoding

- UTF-8 without a byte-order mark. Records are separated by LF or CRLF; a final line break is optional. Blank lines are rejected.
- Comma-separated fields, following RFC 4180: a field may be wrapped in double quotes, and a quote inside a quoted field is written twice (`""`). A field that is not quoted must not contain a quote.
- Exactly one `H` record first, then zero or more `D` records, then exactly one `T` record last. Each record type has a fixed number of fields.

### Records

```
H,DLSF,1,<file_id>,<merchant_id>,<settlement_date>,<generated_at>
D,<line_no>,<value_date>,<amount>,<fee>,<tax>,<reference>,<batch_id>,<narration>
T,<line_count>,<total_amount>
```

| Record | Field | Rule | Modelled on |
|---|---|---|---|
| H | `DLSF`, `1` | Format name and version; anything else is rejected | — |
| H | `file_id` | The bank's id for this file: 1 to 64 characters from letters, digits, `.`, `_` and `-` | S1 `settlement_id` |
| H | `merchant_id` | Must equal the merchant the file was uploaded for | — |
| H | `settlement_date` | `YYYY-MM-DD`: the IST business date of the payout | S1 daily report; S3 `SettlementDate` |
| H | `generated_at` | A UTC instant ending in `Z`, as in the JSON formats; at most microsecond precision | — |
| D | `line_no` | 1, 2, 3 … with no gaps | — |
| D | `value_date` | `YYYY-MM-DD`: the date the bank credited the funds | S4 `automatic_payout_effective_at` |
| D | `amount` | Rupees credited for this line, **net** of fee and tax: digits, a point and **exactly two** decimals (`4784.36`, `0.05`). No sign, no grouping, no exponent | S2 `Event Settlement Amount`; S3 settled amount; S4 `net` |
| D | `fee` | Empty, or the itemised fee in the same rupee form | S1 `fee`; S2 `Transaction Service Charge`; S3 `Fee` |
| D | `tax` | Empty, or the itemised GST on the fee in the same rupee form. S3's IGST, CGST and SGST are summed into one column | S1 `tax`; S2 `Txn ST/GST`; S3 GST columns |
| D | `reference` | Empty, or the gateway payment id the line settles: the same token rules as `file_id` | S1 `entity_id`/`payment_id`; S3 `PhonePeReferenceId` |
| D | `batch_id` | Empty, or the payout batch (UTR or settlement id) the line belongs to | S1 `settlement_utr`; S2 `UTR`; S4 `automatic_payout_id` |
| D | `narration` | Free text, at most 140 characters; quote it if it contains a comma | bank statement narration |
| T | `line_count` | The number of `D` records | S5 control-record counts |
| T | `total_amount` | The sum of every `D` `amount`, exactly, in the same rupee form | S5 control-record totals |

### Example

```
H,DLSF,1,stl_m_0_20261114,m_0,2026-11-14,2026-11-14T04:30:00Z
D,1,2026-11-14,4784.36,98.00,17.64,pay_12_a,UTR20261114M0,"settlement pay_12_a, T+1"
D,2,2026-11-14,1961.00,,,pay_15_a,UTR20261114M0,settlement pay_15_a
T,2,6745.36
```

The first line itemises its fee and tax, so its gross is 4784.36 + 98.00 + 17.64 = 4900.00 (a 2% fee and 18% GST on the fee, the simulator's fee model). The second line does not itemise, so its gross is known only from the payment it references.

### What the parser rejects (DESIGN §6.1)

The parser is strict and all-or-nothing: if any rule fails, the whole file is rejected with the line number and the reason. It rejects:
- a missing, repeated or misplaced `H` or `T`, an unknown record type, or a wrong field count;
- a format name or version other than `DLSF,1`;
- an amount with a sign, an exponent, grouping, a leading zero (`01.00`), or anything other than two decimals (`1.5`, `1.234`);
- a gap or repeat in `line_no`;
- a `line_count` that differs from the number of `D` records, or a `total_amount` that differs from their exact sum;
- an amount too large for 64-bit paise;
- malformed quoting, a byte-order mark, a blank line, or bytes that are not valid UTF-8.

Whether a line's money makes sense (short, over, unknown reference) is not the parser's call. The matcher decides that (§6.3, §6.5).

### Where Driftline deliberately differs from the sources

- **Rupees, not paise.** S1 reports paise; S2 and S3 report rupees with two decimals. Driftline uses rupees, because bank files do, and converts to integer paise exactly at the parser (DESIGN §4.1). A third decimal is an error, not something to round.
- **One explicit file per settlement date, even when it is empty.** A gateway may simply send nothing on a day with no payouts. Driftline cannot tell "nothing was settled" from "the file is late", so it expects an empty file (`T,0,0.00`) for every date on the merchant's settlement calendar. This is what lets the settlement watermark advance on quiet days (§5.4). In Driftline v1 every calendar day is a settlement date (calendar `DAILY`); S3's bank-holiday rule would need calendar data, which v1 does not model.
- **One tax column.** S3 splits GST into IGST, CGST and SGST; Driftline only needs the total to check a short line (§6.5).
- **Refunds are not separate lines.** S1, S3 and S4 list refunds and adjustments as their own rows. In Driftline v1 a refund taken out of a payout shows up only as a smaller line amount. Shift 7 treats that as refund netting (DESIGN §6.4).
- **No partial files.** S4 warns that report data can lag the payout. Driftline only counts a file once its trailer validates (`COMPLETE`, §6.2), so a partial upload never moves the watermark.

## Driftline Gateway Webhook v1

A JSON body, signed as described in DESIGN §10.2 (`Driftline-Signature: t=<unix>,v1=<hex>`, HMAC-SHA256 over `t + "." + rawBody`). The signature is checked on the raw bytes before any parsing (D21).

```json
{"id":"evt_…","type":"payment.succeeded","occurred_at":"2026-11-13T06:00:00.123Z",
 "payment":{"id":"pay_…","order_id":"ord_…","amount_paise":49900,"currency":"INR"}}
{"id":"evt_…","type":"refund.processed","occurred_at":"…",
 "refund":{"payment_id":"pay_…","amount_paise":49900,"currency":"INR"}}
```

- `id` is the gateway's event id and Driftline's idempotency key (§10.3).
- `type` is one of `payment.pending`, `payment.succeeded`, `payment.failed`, `refund.processed` or `refund.failed`.
- Amounts are integer paise, as in S1. The currency must be `INR`.
- Parsing rules are listed in D31: unknown fields are ignored; duplicate keys, fractional numbers and non-UTC instants are rejected.

The shape (an event id, a type, an occurrence time and the affected object) follows the event payloads gateways publish. The signature header follows the common `t=…,v1=…` pattern, which binds a timestamp into the signed payload to limit replays.

## Driftline Order Event v1

A JSON body sent by the merchant with a required `Idempotency-Key` header (§10.3).

```json
{"type":"order.paid","occurred_at":"2026-11-13T06:00:00.123Z",
 "order":{"id":"ord_…","version":3,"amount_paise":49900,"currency":"INR",
          "expires_at":"2026-11-13T06:15:00Z","paid_with":"pay_…"}}
```

- Every event carries a full snapshot of the order at `version` (D27), so the events can be applied in any order.
- `paid_with` is present exactly for `order.paid` and `order.fulfilled`.
- The same strict JSON rules as the webhook apply.
