# Finance-ops desk research (public sources, 2026-11-15)

**This is not the interview.** The brief says: *"Find one person who does this job and ask them what their spreadsheet looks like before you build M4."* These notes come from public forum posts and published guides. They prepare for that interview and do not replace it ([`finance-ops-interview.md`](finance-ops-interview.md)).

**How they were gathered:** web searches on 2026-11-15. Reddit could not be read: it blocks this tool. The first-hand accounts below are Shopify Community threads, written by merchants, bookkeepers and tool builders, mostly outside India. The India-specific material comes from guides, many of them written by vendors selling reconciliation tools, so treat their numbers as illustrative. Quotes are as the thread pages showed them.

## What practitioners say (first-hand, Shopify Community)

| Who | What they said | Source |
|---|---|---|
| Store owner and indie developer, Dec 2025 | Excel "quickly turns into a nightmare"; "orders don't always match payouts 1:1"; "refunds, partial payments, and fees make things messy"; payouts "often don't clearly explain" which orders were paid. Wants missing payouts, underpaid orders, refunds and unmatched transactions flagged, with a CSV export. | [thread 580808](https://community.shopify.com/t/how-do-you-reconcile-shopify-orders-with-stripe-payouts-i-built-a-small-tool/580808) |
| Merchant team reconciling into SAP, Apr 2026 | The export has "the transaction date, payout date and available on date fields"; asks how a daily settlement amount in SAP ties to Shopify's. Shopify staff: the payout is "a net figure" of sales, refunds, disputes and fees; the payout date is the settlement date; timestamps are UTC. | [thread 607691](https://community.shopify.com/t/payout-breakdown-bank-remittance-clarity/607691) |
| Community members, Apr–Aug 2026 | Method: bank deposit → the payout's balance transactions → order detail. "Reconcile at the payout level, not the daily-sales level, and the numbers tie out", because payout batches cross calendar days. | [thread 607691](https://community.shopify.com/t/payout-breakdown-bank-remittance-clarity/607691) |
| Store owner, Aug 2026 | "I am stuck in Sheets for everything." Rebuilds sales reports weekly to close the month for the accountant; payout reconciliation is the biggest month-close rework. | [thread 673334](https://community.shopify.com/t/stuck-in-sheets-for-sales-reports-how-are-you-handling-this/673334) |
| Practitioner describing common errors, Sep 2026 | The orders CSV is one row per line item and payouts are one row per balance transaction: "The two exports don't share a grain, which is why month-close never ties on the first pass." Uses separate tabs: raw orders, raw payouts, SKU sales, payout reconciliation. | [thread 673334](https://community.shopify.com/t/stuck-in-sheets-for-sales-reports-how-are-you-handling-this/673334) |
| Advice, Sep 2026 | Two unedited raw tables, then a monthly cycle of collect, normalize, match, flag, ending in a payout **exception list**. | [thread 673334](https://community.shopify.com/t/stuck-in-sheets-for-sales-reports-how-are-you-handling-this/673334) |

## India-specific (guides; vendor framing)

- **Fees:** 2% MDR plus 18% GST on the fee only. Example: ₹1,000 → ₹23.60 deducted → ₹976.40 to the bank. Fee, tax and UTR are per-record fields of Razorpay's settlement recon API ([Razorpay recon API](https://razorpay.com/docs/api/settlements/fetch-recon.md), [Razorpay settlement entity](https://razorpay.com/docs/api/settlements/entity.md), [Tally guide](https://www.trulyinvoice.com/blog/razorpay-settlement-reconciliation-tally-prime), [ScaleSaathi](https://scalesaathi.in/guides/razorpay-charges/)).
- **Matching:** settlement report → settlement id and payout date → gross, fees, refunds, adjustments → net equals the bank credit, with the UTR as the bank-side reference ([Terra](https://www.terra-insight.com/insights/razorpay-settlement-reconciliation/), [Hellobooks](https://hellobooks.ai/blog/bank-reconciliation-steps-in-india-match-bank-entries-upi-receipts-and-razorpay)).
- **Deductions beyond fee and GST:** refunds netted, chargebacks or disputes, reserves, and TCS (one vendor says TCS is often double-counted or missed). Settlement files can mislabel UPI as wallet, which changes the rate ([Base](https://base.com/en-IN/blog/payment-reconciliation-for-d2c-brands-in-india/), [Terra, merchant fees](https://www.terra-insight.com/insights/merchant-fees/)).
- **D2C brands run two streams:** gateway payouts for prepaid orders and courier remittance for COD, reconciled separately ([Terra, COD vs prepaid](https://www.terra-insight.com/insights/d2c-cod-vs-prepaid-settlement-reconciliation-india/), [eShipz](https://www.eshipz.com/blog/cod-remittance-reconciliation)).
- **Amount-only matching is risky:** different customers place orders of the same value ([Cashfree](https://www.cashfree.com/blog/ecommerce-payment-reconciliation/), [Webgility](https://www.webgility.com/blog/payment-gateway-settlement-discrepancies)).

## Collected by the project owner (mostly Reddit posts; threads not linked)

The project owner collected these against the five open questions on 2026-11-15, mostly from Reddit posts. The threads were not linked, so the authors cannot be checked. They are anonymous public posts, not answers given to this project.

1. **A finance-team member at a US high-risk merchant using four processors (forum post).** Daily: "2 hours … downloading CSV files from 4 different processors", "3 hours … cross-referencing with our internal systems", "1.5 hours … investigating discrepancies", "2 hours … updating manual reports", plus month-end overtime. They "Found a $15K discrepancy last week that had been sitting there for 3 months". Processors have "different reporting formats, different settlement timing, different fee structures". The CFO wants "real-time payment visibility". **Do not reuse the post's totals:** it claims 7,000 hours and $280,000 a year, but its own daily breakdown (8.5 h a day) comes to about 3,100 hours a year. The pain points are usable; the figures are not.
2. **Three recurring problems:** missing links between systems (the payment succeeds, but the order system doesn't show it); net settlement (one deposit is many transactions, fees, refunds and adjustments); unexplained differences traced across earlier periods.
3. **RBI deadlines for failed transactions.** Checked against the circular itself: [RBI/2019-20/67](https://www.rbi.org.in/scripts/NotificationUser.aspx?Id=11693), 20 Sep 2019 (also [TaxGuru](https://taxguru.in/rbi/banks-pay-compensation-customer-failed-transactions.html)). UPI merchant payment with no merchant confirmation: T+5. IMPS with the beneficiary not credited: T+1. Beyond the deadline, ₹100 a day, credited automatically. The post's T+5 for card e-commerce was not visible in the excerpts checked, so it is unconfirmed here. This agrees with DESIGN §8 (DEBITED_NO_ORDER: RBI P2M, T+5).
4. **A three-level confidence rule:**

   | Level | Evidence | Action |
   |---|---|---|
   | High | Exact unique id, amount and currency agree | Auto-match |
   | Medium | Amount, payer, date and a supporting reference agree; the primary id is missing | Suggest for approval |
   | Low | Similar amount and date, several candidates | Do not match |

5. **A payout export header:** `payout_identifier, type, net, commission, gross, created_date, created_time, estimated arrival, transaction_identifier`. The processor is not known.

## What this suggests for Shift 7 (candidates only, not decisions)

1. **References come first.** Practitioners match at the payout level through the settlement or UTR reference. Matching a lump sum with no reference is the fallback for when that breakdown is missing. That supports keeping the solver conservative and saying AMBIGUOUS rather than guessing (§6.4, §6.5).
2. **Same-value orders are the real danger.** This supports AMBIGUOUS with two witnesses (INV-AMBIGUITY-WITNESSED).
3. **Netted deductions break the subset sum.** Refunds, chargebacks, reserves and TCS make a credit that no subset of payments explains. Case 30 (`noref_refund_netted → NO_SOLUTION, escalated`) covers refunds; reserves and TCS are not modelled, so they would also end as NO_SOLUTION, escalated.
4. **Per-payment fee and GST rounding** matches cases 3 and 4.
5. **Several dates and UTC timestamps** (transaction, payout, available-on) back the IST cut-off case (23) and the previous-day lookback (24).
6. **Should an inferred match need human approval?** Item 4 above puts "primary id missing, other evidence agrees" at *suggest for approval*, not auto-match. That is exactly what `MATCHED_INFERRED` is (a unique subset-sum solution with no reference). Shift 7 should decide whether `MATCHED_INFERRED` posts automatically or waits for approval, and record why in DESIGN §19. *Decided in D56: it posts automatically, because it is an internal posting backed by a uniqueness proof and challenge detection, and no external action may rest on it alone.*
7. **The payout id and the transaction id are separate** (item 5), as are gross, commission and net; this matches the Settlement File v1 layout (`docs/formats.md`).
8. **An exception list as CSV,** in columns that fit their sheet: order id, payment id, settlement id and UTR, gross, fee, tax, net, the dates, and the reason.

## What desk research cannot answer (still needs one real person)

- How many hours a week this takes, and how many people do it (Q7). Partly answered: one anonymous poster's daily breakdown (item 1), from a US multi-processor merchant.
- Their ranking of the six break types (Q4).
- How they track the T+1 and T+5 refund deadlines, and whether they have ever paid compensation (Q8).
- What evidence they want attached before trusting an automatic match (Q10, Q11).
- Their actual column list (Q1, Q12). Item 5 is a processor's export header, not a reconciliation sheet's columns.
