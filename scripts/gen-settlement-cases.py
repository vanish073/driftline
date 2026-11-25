#!/usr/bin/env python3
"""Generates the Shift 7 settlement case corpus (35 YAML cases and 6 golden Settlement File v1 CSVs) with exact
amounts, and checks every unreferenced-line expectation it can against an independent brute-force model of DESIGN
§6.4 (narrowing by batch date, forced-assignment propagation, exact subset sum). Cases 9, 12, 13, 14 and 34 are not
model-checked (an earlier file, n = 40, n = 200, n = 60, no solve); case 12 is checked by an exact meet-in-the-middle
count instead. Deterministic: fixed random seeds, so a re-run reproduces the committed files byte for byte.

Usage: python3 scripts/gen-settlement-cases.py core/src/test/resources/settlement-cases"""
import sys, os, datetime, itertools, random

OUT = sys.argv[1]
os.makedirs(OUT, exist_ok=True)
os.makedirs(os.path.join(OUT, "golden"), exist_ok=True)
D = "2026-11-14"
LOOKBACK = 1

def fee_gst(gross, fee_bps=200, gst_bps=1800):
    fee = (gross * fee_bps + 5000) // 10000
    gst = (fee * gst_bps + 5000) // 10000
    return fee, gst

def net(gross, fee_bps=200, gst_bps=1800):
    f, g = fee_gst(gross, fee_bps, gst_bps)
    return gross - f - g

def rupees(paise):
    assert paise >= 0
    return f"{paise // 100}.{paise % 100:02d}"

def batch_date(captured, cycle=1):
    # captured is an IST local "YYYY-MM-DDTHH:MM:SS"; IST date + cycle
    d = datetime.date.fromisoformat(captured[:10])
    return d + datetime.timedelta(days=cycle)

# ---- independent model of §6.4 -------------------------------------------------------------------------------------

def solutions(values, target, limit=3):
    """Brute force: up to `limit` non-empty index subsets summing to target (small n only)."""
    out = []
    n = len(values)
    for r in range(1, n + 1):
        for combo in itertools.combinations(range(n), r):
            if sum(values[i] for i in combo) == target:
                out.append(combo)
                if len(out) >= limit:
                    return out
    return out

def model(case):
    """Returns {line_key: (status, pays)} for the unreferenced lines of the last file, per §6.4."""
    fb = case.get("fees", {}).get("fee_bps", 200)
    gb = case.get("fees", {}).get("gst_bps", 1800)
    cycle = case.get("cycle_days", 1)
    pays = {p["id"]: p for p in case.get("payments", [])}
    matched = set()
    # referenced lines of every file settle their payment if it succeeded (exact or not); model only what we need
    for f in case["files"]:
        for l in f["lines"]:
            if "reference" in l and l["reference"] in pays and pays[l["reference"]].get("status") != "failed":
                matched.add(l["reference"])
    f = case["files"][-1]
    date = datetime.date.fromisoformat(f["date"])
    lines = [(i + 1, l) for i, l in enumerate(f["lines"]) if "reference" not in l]
    pool = []
    for p in case.get("payments", []):
        if p.get("status") == "failed" or p["id"] in matched:
            continue
        bd = batch_date(p["captured"], cycle)
        if date - datetime.timedelta(days=LOOKBACK) <= bd <= date:
            g = p.get("gross_paise") or int(round(float(p["gross"]) * 100))
            pool.append((p["captured"], p["id"], net(g, fb, gb)))
    pool.sort()
    open_ids = [p[1] for p in pool]
    val = {p[1]: p[2] for p in pool}
    result = {}
    def amount(l):
        a = l["amount"]; whole, frac = a.split(".")
        return int(whole) * 100 + int(frac)
    decided = {}
    changed = True
    while changed:
        changed = False
        for no, l in lines:
            if no in decided:
                continue
            if amount(l) == 0:
                decided[no] = ("ZERO_AMOUNT", [])
                continue
            cands = [i for i in open_ids]
            sols = solutions([val[i] for i in cands], amount(l))
            if len(sols) == 1:
                chosen = [cands[i] for i in sols[0]]
                decided[no] = ("INFERRED", sorted(chosen))
                open_ids = [i for i in open_ids if i not in chosen]
                changed = True
                break
    for no, l in lines:
        if no in decided:
            result[no] = decided[no]
            continue
        sols = solutions([val[i] for i in open_ids], amount(l))
        result[no] = ("NO_SOLUTION", []) if not sols else ("AMBIGUOUS", [])
    return result

# ---- writing --------------------------------------------------------------------------------------------------------

def q(s):
    return '"' + s + '"'

def write_case(num, name, about, case, expect, check=True):
    if check:
        got = model(case)
        fid = "stl_" + case["files"][-1]["date"].replace("-", "")
        for no, (status, pays) in got.items():
            key = f"{fid}:{no}"
            want = expect["lines"][key]
            assert want["status"] == status, (num, name, key, want, status)
            if status == "INFERRED":
                assert sorted(want["pays"]) == pays, (num, name, key, want["pays"], pays)
    y = [f"case: {num}", f"name: {name}", "about: >-"]
    for line in about.strip().split("\n"):
        y.append("  " + line.strip())
    if "fees" in case:
        y.append(f"fees: {{fee_bps: {case['fees']['fee_bps']}, gst_bps: {case['fees']['gst_bps']}}}")
    if "first_settlement_date" in case:
        y.append(f"first_settlement_date: {q(case['first_settlement_date'])}")
    if "watermark" in case:
        y.append(f"watermark: {case['watermark']}")
    def payment_line(p):
        parts = [f"id: {p['id']}"]
        if "gross_paise" in p:
            parts.append(f"gross_paise: {p['gross_paise']}")
        else:
            parts.append(f"gross: {q(p['gross'])}")
        parts.append(f"captured: {q(p['captured'])}")
        if p.get("status"):
            parts.append(f"status: {p['status']}")
        line = "  - {" + ", ".join(parts) + "}"
        if p.get("note"):
            line += "   # " + p["note"]
        return line
    if case.get("payments"):
        y.append("payments:")
        for p in case["payments"]:
            y.append(payment_line(p))
    y.append("files:")
    for f in case["files"]:
        if "csv" in f:
            y.append(f"  - csv: {f['csv']}")
            continue
        y.append(f"  - date: {q(f['date'])}")
        y.append("    lines:")
        for l in f["lines"]:
            parts = [f"amount: {q(l['amount'])}"]
            for k in ("fee", "tax"):
                if k in l:
                    parts.append(f"{k}: {q(l[k])}")
            if "reference" in l:
                parts.append(f"reference: {l['reference']}")
            line = "      - {" + ", ".join(parts) + "}"
            if l.get("note"):
                line += "   # " + l["note"]
            y.append(line)
    if case.get("after_solve"):
        y.append("after_solve:")
        y.append("  payments:")
        for p in case["after_solve"]:
            y.append("  " + payment_line(p))
    y.append("expect:")
    y.append("  lines:")
    for key, v in expect["lines"].items():
        parts = [f"status: {v['status']}"]
        if "pays" in v:
            parts.append("pays: [" + ", ".join(v["pays"]) + "]")
        y.append(f"    {key}: {{" + ", ".join(parts) + "}")
    if expect.get("breaks"):
        y.append("  breaks:")
        for b in expect["breaks"]:
            parts = [f"type: {b['type']}", f"subject: {q(b['subject'])}"]
            if "reason" in b:
                parts.append(f"reason: {b['reason']}")
            if "state" in b:
                parts.append(f"state: {b['state']}")
            if "evidence" in b:
                parts.append("evidence: {" + ", ".join(f"{k}: {q(v)}" for k, v in b["evidence"].items()) + "}")
            y.append("    - {" + ", ".join(parts) + "}")
    if expect.get("balances"):
        y.append("  balances: {" + ", ".join(f"{k}: {q(v)}" for k, v in expect["balances"].items()) + "}")
    path = os.path.join(OUT, f"{num:02d}_{name}.yaml")
    with open(path, "w") as fh:
        fh.write("\n".join(y) + "\n")

def P(id, gross_rupees, captured="2026-11-13T10:00:00", **kw):
    p = {"id": id, "gross": gross_rupees, "captured": captured}
    p.update(kw)
    return p

def g(r):  # rupee string -> paise
    whole, frac = r.split(".")
    return int(whole) * 100 + int(frac)

F = "stl_20261114"
CLEAN = {"SUSPENSE": "0.00", "GATEWAY_RECEIVABLE": "0.00"}

# ---- referenced lines (cases 1-9) -------------------------------------------------------------------------------------

write_case(1, "ref_single_exact", "A line names its payment and pays exactly its expected net: MATCHED (§6.3).",
    {"payments": [P("pay_1", "499.00")],
     "files": [{"date": D, "lines": [{"amount": rupees(net(49900)), "reference": "pay_1"}]}]},
    {"lines": {f"{F}:1": {"status": "MATCHED", "pays": ["pay_1"]}}, "balances": CLEAN})

write_case(2, "ref_batch_exact", """Three referenced lines in one payout batch (the same UTR), each exact: all MATCHED. A batch
    of referenced lines needs no subset search; each line names its payment (§6.3, docs/formats.md batch_id).""",
    {"payments": [P("pay_1", "499.00"), P("pay_2", "1250.00", "2026-11-13T11:00:00"), P("pay_3", "77.77", "2026-11-13T12:00:00")],
     "files": [{"date": D, "lines": [{"amount": rupees(net(49900)), "reference": "pay_1"},
                                     {"amount": rupees(net(125000)), "reference": "pay_2"},
                                     {"amount": rupees(net(7777)), "reference": "pay_3"}]}]},
    {"lines": {f"{F}:1": {"status": "MATCHED", "pays": ["pay_1"]}, f"{F}:2": {"status": "MATCHED", "pays": ["pay_2"]},
               f"{F}:3": {"status": "MATCHED", "pays": ["pay_3"]}}, "balances": CLEAN})

write_case(3, "ref_short_itemised_fee_explains", """Gross 1000.00 should net 976.40 (fee 20.00, GST 3.60). The bank pays 971.40 and itemises
    fee 25.00 and tax 3.60, which explain the gap exactly: SHORT_SETTLEMENT resolved automatically, with
    pricing_drift 5.00 (§6.5).""",
    {"payments": [P("pay_1", "1000.00")],
     "files": [{"date": D, "lines": [{"amount": "971.40", "fee": "25.00", "tax": "3.60", "reference": "pay_1"}]}]},
    {"lines": {f"{F}:1": {"status": "SHORT_EXPLAINED", "pays": ["pay_1"]}},
     "breaks": [{"type": "SHORT_SETTLEMENT", "subject": f"{F}:1", "state": "RESOLVED_AUTO",
                 "evidence": {"pricing_drift": "5.00", "itemised": "yes", "shortfall": "5.00"}}],
     "balances": CLEAN})

f4, t4 = fee_gst(49900)
write_case(4, "ref_short_itemised_gst_rounding_explains", f"""Gross 499.00: the contract gives fee {rupees(f4)} and GST {rupees(t4)} (179.64 paise rounded half up), net
    {rupees(net(49900))}. The bank rounds GST up to 1.81 and pays one paisa less; the itemised fee and tax explain it exactly:
    SHORT_SETTLEMENT resolved automatically with pricing_drift 0.01 (§6.5).""",
    {"payments": [P("pay_1", "499.00")],
     "files": [{"date": D, "lines": [{"amount": rupees(49900 - f4 - 181), "fee": rupees(f4), "tax": "1.81", "reference": "pay_1"}]}]},
    {"lines": {f"{F}:1": {"status": "SHORT_EXPLAINED", "pays": ["pay_1"]}},
     "breaks": [{"type": "SHORT_SETTLEMENT", "subject": f"{F}:1", "state": "RESOLVED_AUTO",
                 "evidence": {"pricing_drift": "0.01", "shortfall": "0.01"}}],
     "balances": CLEAN})

write_case(5, "ref_short_unitemised", """Expected net 976.40, received 970.00, nothing itemised: SHORT_SETTLEMENT stays open (the resolver
    escalates open breaks in Shift 8) and a partial posting keeps 6.40 of the receivable open (§6.5).""",
    {"payments": [P("pay_1", "1000.00")],
     "files": [{"date": D, "lines": [{"amount": "970.00", "reference": "pay_1"}]}]},
    {"lines": {f"{F}:1": {"status": "SHORT_UNEXPLAINED", "pays": ["pay_1"]}},
     "breaks": [{"type": "SHORT_SETTLEMENT", "subject": f"{F}:1", "state": "DETECTED",
                 "evidence": {"itemised": "no", "shortfall": "6.40"}}],
     "balances": {"SUSPENSE": "0.00", "GATEWAY_RECEIVABLE": "30.00"}})

write_case(6, "ref_short_itemised_not_summing", """Expected net 976.40, received 970.00, itemised fee 20.00 and tax 3.60: the itemisation covers
    23.60 of a 30.00 gap, so it does not explain the shortfall: SHORT_SETTLEMENT stays open (§6.5).""",
    {"payments": [P("pay_1", "1000.00")],
     "files": [{"date": D, "lines": [{"amount": "970.00", "fee": "20.00", "tax": "3.60", "reference": "pay_1"}]}]},
    {"lines": {f"{F}:1": {"status": "SHORT_UNEXPLAINED", "pays": ["pay_1"]}},
     "breaks": [{"type": "SHORT_SETTLEMENT", "subject": f"{F}:1", "state": "DETECTED",
                 "evidence": {"itemised": "yes", "shortfall": "6.40"}}],
     "balances": {"SUSPENSE": "0.00", "GATEWAY_RECEIVABLE": "30.00"}})

write_case(7, "ref_over_settlement", """Expected net 976.40, received 980.00: OVER_SETTLEMENT, an anomaly; the whole amount is held in
    suspense (§6.5).""",
    {"payments": [P("pay_1", "1000.00")],
     "files": [{"date": D, "lines": [{"amount": "980.00", "reference": "pay_1"}]}]},
    {"lines": {f"{F}:1": {"status": "OVER", "pays": ["pay_1"]}},
     "breaks": [{"type": "OVER_SETTLEMENT", "subject": f"{F}:1", "state": "DETECTED"}],
     "balances": {"GATEWAY_RECEIVABLE": "1000.00"}})

write_case(8, "ref_unknown_reference", """Line 2 names a payment Driftline has never seen: an ORPHAN_SETTLEMENT (UNKNOWN_REFERENCE),
    its money held in suspense until it is attributed (§6.3). Line 1 settles normally.""",
    {"payments": [P("pay_1", "499.00")],
     "files": [{"date": D, "lines": [{"amount": rupees(net(49900)), "reference": "pay_1"},
                                     {"amount": "100.00", "reference": "pay_unknown"}]}]},
    {"lines": {f"{F}:1": {"status": "MATCHED", "pays": ["pay_1"]}, f"{F}:2": {"status": "UNATTRIBUTED"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:2", "reason": "UNKNOWN_REFERENCE"}]})

write_case(9, "ref_already_settled", """pay_1 was settled by the 13 November file. The 14 November file names it again: SETTLEMENT_CONFLICT,
    and the second line's money is held in suspense (§6.3: a payment is settled at most once).""",
    {"first_settlement_date": "2026-11-13",
     "payments": [P("pay_1", "499.00", "2026-11-12T10:00:00")],
     "files": [{"date": "2026-11-13", "lines": [{"amount": rupees(net(49900)), "reference": "pay_1"}]},
               {"date": D, "lines": [{"amount": rupees(net(49900)), "reference": "pay_1"}]}]},
    {"lines": {"stl_20261113:1": {"status": "MATCHED", "pays": ["pay_1"]}, f"{F}:1": {"status": "CONFLICT"}},
     "breaks": [{"type": "SETTLEMENT_CONFLICT", "subject": f"{F}:1"}]}, check=False)

# ---- unreferenced lines (cases 10-35) ---------------------------------------------------------------------------------

def nets_line(ids, pays, fb=200, gb=1800, adjust=0):
    by = {p["id"]: p for p in pays}
    total = sum(net(p.get("gross_paise") or g(p["gross"]), fb, gb) for p in (by[i] for i in ids)) + adjust
    return rupees(total)

pays10 = [P("pay_1", "499.00", "2026-11-13T09:00:00"), P("pay_2", "1200.50", "2026-11-13T10:00:00"),
          P("pay_3", "75.00", "2026-11-13T11:00:00"), P("pay_4", "3333.33", "2026-11-13T12:00:00")]
write_case(10, "noref_all_candidates_exact", """The line is the sum of every candidate's net: the fast path (all values positive, sum equals
    the target) proves it is the only subset. MATCHED_INFERRED; the suspense is released (§6.4).""",
    {"payments": pays10, "files": [{"date": D, "lines": [{"amount": nets_line(["pay_1", "pay_2", "pay_3", "pay_4"], pays10),
                                                           "note": "net of all four"}]}]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": ["pay_1", "pay_2", "pay_3", "pay_4"]}}, "balances": CLEAN})

pays11 = [P(f"pay_{i}", r, f"2026-11-13T{9+i:02d}:00:00") for i, r in
          enumerate(["120.00", "345.50", "799.99", "1500.00", "62.25", "2400.10"], start=1)]
write_case(11, "noref_unique_subset_small", """Six candidates; exactly one subset (pay_2, pay_3, pay_5) sums to the line, so it is inferred. The
    other three stay open; they are not due until 14 November is settled (§6.4).""",
    {"payments": pays11, "files": [{"date": D, "lines": [{"amount": nets_line(["pay_2", "pay_3", "pay_5"], pays11),
                                                           "note": "net of pay_2 + pay_3 + pay_5"}]}]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": ["pay_2", "pay_3", "pay_5"]}}, "balances": {"SUSPENSE": "0.00"}})

# case 12: 40 candidates, meet-in-the-middle; uniqueness checked by an exact MITM here.
# Realistic amounts are hopeless here: 40 values of 1,000-50,000 rupees give tens of thousands of subsets per sum
# (an earlier version of this case found 38,560). Super-increasing amounts (each above the sum of all smaller ones)
# make every subset sum distinct, so the expected answer is certain.
rng = random.Random(1212)
gross12 = [2 ** k for k in range(40)]   # the largest, 2^39 paise, is under AmountLimits.MAX_PAYMENT (D93)
order12 = list(range(40))
rng.shuffle(order12)
pays12 = [{"id": f"pay_{k:02d}", "gross_paise": gross12[k], "captured": f"2026-11-13T{8 + i // 4:02d}:{(i % 4) * 15:02d}:00"}
          for i, k in enumerate(order12)]
chosen12 = sorted(rng.sample([p["id"] for p in pays12], 20))
target12 = sum(p["gross_paise"] for p in pays12 if p["id"] in chosen12)
def mitm_count(vals, target):
    h = len(vals) // 2
    from collections import Counter
    def sums(vs):
        c = Counter([0])
        for v in vs:
            c2 = Counter(c)
            for s, k in c.items():
                c2[s + v] += k
            c = c2
        return c
    L, R = sums(vals[:h]), sums(vals[h:])
    return sum(k * R.get(target - s, 0) for s, k in L.items()) - (1 if target == 0 else 0)
cnt12 = mitm_count([p["gross_paise"] for p in pays12], target12)
assert cnt12 == 1, ("case 12 must be unique", cnt12)
write_case(12, "noref_unique_n40_mitm_boundary", """Forty candidates: the largest set meet-in-the-middle takes (2 x 2^20 partial sums, within the
    20,000,000-operation budget); the dynamic programme would need ~10^13 cells. A fee-free merchant with
    super-increasing amounts (powers of two, 1 paisa to 2^39 paise, each above the sum of all smaller ones; until D93
    they were 1,000 × 2^k + k + 1, above today's largest accepted amount), so exactly one subset of 20 fits; this was also
    checked by an exact meet-in-the-middle count when the case was generated. With realistic amounts, 40 candidates
    give tens of thousands of fitting subsets, and the answer would rightly be AMBIGUOUS.""",
    {"fees": {"fee_bps": 0, "gst_bps": 0}, "payments": pays12,
     "files": [{"date": D, "lines": [{"amount": rupees(target12), "note": "20 of the 40"}]}]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": chosen12}}, "balances": {"SUSPENSE": "0.00"}}, check=False)

# case 13: 200 candidates, fee-free merchant, multiples of 100 rupees: gcd reduction then the capped-count DP
ks = [(i * 37) % 50 + 1 for i in range(200)]
pays13 = [P(f"pay_{i:03d}", f"{k * 100}.00", f"2026-11-13T{8 + i // 20:02d}:{(i % 20) * 3:02d}:00") for i, k in enumerate(ks)]
chosen13 = [p["id"] for i, p in enumerate(pays13) if i % 2 == 0]
target13 = sum(k * 10000 for i, k in enumerate(ks) if i % 2 == 0)
write_case(13, "noref_n200_dp_after_gcd", """Two hundred candidates from a fee-free merchant, every amount a multiple of 100 rupees: dividing by
    the gcd leaves values 1..50, and the capped-count dynamic programme (n × S well within budget; meet-in-the-middle
    cannot take n > 40) finds that many subsets fit: AMBIGUOUS, with two verified witnesses.""",
    {"fees": {"fee_bps": 0, "gst_bps": 0}, "payments": pays13,
     "files": [{"date": D, "lines": [{"amount": rupees(target13), "note": "the even-numbered half of the candidates"}]}]},
    {"lines": {f"{F}:1": {"status": "AMBIGUOUS"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:1", "reason": "AMBIGUOUS", "evidence": {"candidates": "200"}}]},
    check=False)

# case 14: 60 candidates with large coprime amounts: no engine fits the budget
rng14 = random.Random(1414)
gross14 = rng14.sample(range(1_000_001, 9_000_000), 60)
pays14 = [P(f"pay_{i:02d}", rupees(gv), f"2026-11-13T{8 + i // 6:02d}:{(i % 6) * 10:02d}:00") for i, gv in enumerate(gross14)]
target14 = sum(net(gv) for gv in gross14[::2])
write_case(14, "noref_budget_exceeded", """Sixty candidates with large amounts: meet-in-the-middle cannot take n > 40, and the dynamic programme
    would need about 61 × 10^8 cells against a 20,000,000-operation budget. The solver refuses instead of guessing:
    ORPHAN_SETTLEMENT with reason BUDGET_EXCEEDED, money held in suspense.""",
    {"payments": pays14, "files": [{"date": D, "lines": [{"amount": rupees(target14), "note": "net of every other candidate"}]}]},
    {"lines": {f"{F}:1": {"status": "BUDGET_EXCEEDED"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:1", "reason": "BUDGET_EXCEEDED", "evidence": {"candidates": "60"}}]},
    check=False)

pays15 = [P("pay_1", "100.00", "2026-11-13T09:00:00"), P("pay_2", "400.00", "2026-11-13T10:00:00"),
          P("pay_3", "200.00", "2026-11-13T11:00:00"), P("pay_4", "300.00", "2026-11-13T12:00:00")]
write_case(15, "noref_two_subsets_distinct_amounts", """A fee-free merchant (so the arithmetic is plain): 100 + 400 = 200 + 300 = 500. Two different subsets
    of distinct amounts fit, so the line is AMBIGUOUS, with both witnesses, and nothing is guessed (§6.4).""",
    {"fees": {"fee_bps": 0, "gst_bps": 0}, "payments": pays15, "files": [{"date": D, "lines": [{"amount": "500.00"}]}]},
    {"lines": {f"{F}:1": {"status": "AMBIGUOUS"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:1", "reason": "AMBIGUOUS", "evidence": {"candidates": "4"}}]})

pays16 = [P(f"pay_{i}", "499.00", f"2026-11-13T{9 + i}:00:00") for i in range(1, 6)]
write_case(16, "noref_equal_amounts_499xk", """Five orders at the catalogue price of 499.00; the line is three of their nets. Any three fit (ten
    subsets), so the line is AMBIGUOUS: equal amounts are exactly where guessing goes wrong (§6.4, canary C08).""",
    {"payments": pays16, "files": [{"date": D, "lines": [{"amount": rupees(3 * net(49900)), "note": "3 x net(499.00)"}]}]},
    {"lines": {f"{F}:1": {"status": "AMBIGUOUS"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:1", "reason": "AMBIGUOUS", "evidence": {"candidates": "5"}}]})

pays17 = [P("pay_1", "499.00", "2026-11-13T09:00:00"), P("pay_2", "999.00", "2026-11-13T10:00:00"), P("pay_3", "1499.00", "2026-11-13T11:00:00")]
write_case(17, "noref_no_solution", """No subset of the three candidates sums to 1000.00: NO_SOLUTION, an ORPHAN_SETTLEMENT held in suspense.""",
    {"payments": pays17, "files": [{"date": D, "lines": [{"amount": "1000.00"}]}]},
    {"lines": {f"{F}:1": {"status": "NO_SOLUTION"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:1", "reason": "NO_SOLUTION", "evidence": {"candidates": "3"}}]})

write_case(18, "noref_no_candidates", """No payment falls in the line's window: NO_SOLUTION over an empty candidate set.""",
    {"files": [{"date": D, "lines": [{"amount": "250.00"}]}]},
    {"lines": {f"{F}:1": {"status": "NO_SOLUTION"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:1", "reason": "NO_SOLUTION", "evidence": {"candidates": "0"}}]})

pays19 = [P("pay_1", "300.00", "2026-11-13T09:00:00"), P("pay_2", "500.00", "2026-11-13T10:00:00"),
          P("pay_3", "700.00", "2026-11-13T11:00:00"), P("pay_4", "1100.00", "2026-11-13T12:00:00")]
write_case(19, "noref_gcd_reduction", """A fee-free merchant whose amounts are all multiples of 100 rupees: the solver divides by the gcd
    (10,000 paise) and works on 3, 5, 7 and 11. Only 5 + 7 makes 12: inferred.""",
    {"fees": {"fee_bps": 0, "gst_bps": 0}, "payments": pays19, "files": [{"date": D, "lines": [{"amount": "1200.00"}]}]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": ["pay_2", "pay_3"]}}, "balances": CLEAN | {"GATEWAY_RECEIVABLE": "1400.00"}})

pays20 = pays19[:3]
write_case(20, "noref_target_not_divisible_by_gcd", """Every candidate is a multiple of 100 rupees but the line is 1234.56: not divisible by the gcd, so
    NO_SOLUTION without any search.""",
    {"fees": {"fee_bps": 0, "gst_bps": 0}, "payments": pays20, "files": [{"date": D, "lines": [{"amount": "1234.56"}]}]},
    {"lines": {f"{F}:1": {"status": "NO_SOLUTION"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:1", "reason": "NO_SOLUTION"}]})

write_case(21, "noref_zero_amount_line", """An unreferenced line of 0.00 settles nothing: it is rejected (ZERO_AMOUNT) and flagged as an
    ORPHAN_SETTLEMENT, with no posting.""",
    {"payments": [P("pay_1", "499.00")], "files": [{"date": D, "lines": [{"amount": "0.00"}]}]},
    {"lines": {f"{F}:1": {"status": "ZERO_AMOUNT"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:1", "reason": "ZERO_AMOUNT"}],
     "balances": {"SUSPENSE": "0.00"}})

write_case(22, "noref_single_candidate_equal", """One candidate, and the line equals its net: inferred.""",
    {"payments": [P("pay_1", "2499.00")], "files": [{"date": D, "lines": [{"amount": rupees(net(249900))}]}]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": ["pay_1"]}}, "balances": CLEAN})

pays23 = [P("pay_in", "750.00", "2026-11-13T23:59:59", note="IST date 13 Nov: batch date 14 Nov, inside the window"),
          P("pay_out", "750.00", "2026-11-14T00:00:00", note="IST date 14 Nov: batch date 15 Nov, outside (18:30 UTC on 13 Nov)")]
write_case(23, "noref_capture_at_ist_cutoff_boundary", """Two equal payments one second apart across IST midnight. Batch dates come from the IST capture
    date (§6.4), so only pay_in is a candidate and the line is inferred. A UTC date would put both on 13 November
    and make the line ambiguous.""",
    {"payments": pays23, "files": [{"date": D, "lines": [{"amount": rupees(net(75000))}]}]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": ["pay_in"]}}, "balances": {"SUSPENSE": "0.00"}})

pays24 = [P("pay_prev", "640.00", "2026-11-12T15:00:00", note="batch date 13 Nov: one day back, inside the lookback"),
          P("pay_cur", "1180.00", "2026-11-13T15:00:00")]
write_case(24, "noref_lookback_includes_previous_day", """The line also pays a capture from 12 November (batch date 13 November), one day before its own
    date: the one-day lookback keeps it a candidate, so the line is inferred and no MISSING_SETTLEMENT is raised.""",
    {"payments": pays24, "files": [{"date": D, "lines": [{"amount": nets_line(["pay_prev", "pay_cur"], pays24)}]}]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": ["pay_cur", "pay_prev"]}}, "balances": CLEAN})

pays25 = [P("pay_1", "499.00", "2026-11-12T10:00:00", note="settled by reference on 13 Nov"), P("pay_2", "499.00", "2026-11-13T10:00:00")]
write_case(25, "noref_excludes_already_matched", """pay_1 and pay_2 have equal nets, but pay_1 was settled by reference the day before, so it is not a
    candidate: the line is inferred to be pay_2.""",
    {"first_settlement_date": "2026-11-13", "payments": pays25,
     "files": [{"date": "2026-11-13", "lines": [{"amount": rupees(net(49900)), "reference": "pay_1"}]},
               {"date": D, "lines": [{"amount": rupees(net(49900))}]}]},
    {"lines": {"stl_20261113:1": {"status": "MATCHED", "pays": ["pay_1"]}, f"{F}:1": {"status": "INFERRED", "pays": ["pay_2"]}},
     "balances": CLEAN})

pays26 = [P("pay_ok", "499.00", "2026-11-13T10:00:00"), P("pay_bad", "499.00", "2026-11-13T11:00:00", status="failed")]
write_case(26, "noref_excludes_failed_payments", """A failed payment of the same amount is not a candidate (only SUCCEEDED payments are, §6.4): inferred.""",
    {"payments": pays26, "files": [{"date": D, "lines": [{"amount": rupees(net(49900))}]}]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": ["pay_ok"]}}, "balances": CLEAN})

pays27 = [P("pay_40", "40.00", "2026-11-13T09:00:00"), P("pay_90", "90.00", "2026-11-13T10:00:00"),
          P("pay_130", "130.00", "2026-11-13T11:00:00"), P("pay_300", "300.00", "2026-11-13T12:00:00")]
write_case(27, "multi_line_forced_propagation_resolves_both", """Fee-free. Line 1 (130.00) alone is ambiguous: 130 or 40 + 90. Line 2 (390.00) has exactly one
    solution, 90 + 300, so it is committed; without pay_90, line 1 is uniquely pay_130. Forced-assignment propagation
    resolves both, whatever the line order (§6.4).""",
    {"fees": {"fee_bps": 0, "gst_bps": 0}, "payments": pays27, "files": [{"date": D, "lines": [{"amount": "130.00"}, {"amount": "390.00"}]}]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": ["pay_130"]}, f"{F}:2": {"status": "INFERRED", "pays": ["pay_300", "pay_90"]}},
     "balances": {"SUSPENSE": "0.00"}})

pays28 = pays27 + [P("pay_500", "500.00", "2026-11-13T13:00:00")]
write_case(28, "multi_line_propagation_leaves_ambiguous", """Fee-free. Line 2 (500.00) is uniquely pay_500 and is committed, but that does not touch line 1
    (130.00 = 130 or 40 + 90), which stays AMBIGUOUS at the fixpoint.""",
    {"fees": {"fee_bps": 0, "gst_bps": 0}, "payments": pays28, "files": [{"date": D, "lines": [{"amount": "130.00"}, {"amount": "500.00"}]}]},
    {"lines": {f"{F}:1": {"status": "AMBIGUOUS"}, f"{F}:2": {"status": "INFERRED", "pays": ["pay_500"]}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:1", "reason": "AMBIGUOUS"}]})

pays29 = [P("pay_10", "10.00", "2026-11-13T09:00:00"), P("pay_25", "25.00", "2026-11-13T10:00:00"), P("pay_40", "40.00", "2026-11-13T11:00:00")]
write_case(29, "multi_line_jointly_infeasible", """Fee-free. Line 1 (35.00) is uniquely 10 + 25; line 2 (50.00) is uniquely 10 + 40. Both need pay_10, so
    no joint assignment exists. Line 1 comes first and is committed; line 2 then has no solution and becomes an
    ORPHAN_SETTLEMENT (§6.4).""",
    {"fees": {"fee_bps": 0, "gst_bps": 0}, "payments": pays29, "files": [{"date": D, "lines": [{"amount": "35.00"}, {"amount": "50.00"}]}]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": ["pay_10", "pay_25"]}, f"{F}:2": {"status": "NO_SOLUTION"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:2", "reason": "NO_SOLUTION"}]})

pays30 = [P("pay_1", "1000.00", "2026-11-13T09:00:00"), P("pay_2", "500.00", "2026-11-13T10:00:00")]
write_case(30, "noref_refund_netted", """The gateway netted a 200.00 refund out of an unreferenced batch: 976.40 + 488.20 − 200.00. No subset of
    payments explains a netted line, so it is NO_SOLUTION and escalated; signed netting is a stated cut line (§6.4,
    §20).""",
    {"payments": pays30, "files": [{"date": D, "lines": [{"amount": nets_line(["pay_1", "pay_2"], pays30, adjust=-20000),
                                                           "note": "nets of both minus a 200.00 refund"}]}]},
    {"lines": {f"{F}:1": {"status": "NO_SOLUTION"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:1", "reason": "NO_SOLUTION", "state": "DETECTED"}]})

pays31 = [P("pay_1", "499.00", "2026-11-13T09:00:00"), P("pay_2", "750.00", "2026-11-13T10:00:00"),
          P("pay_3", "1200.00", "2026-11-13T11:00:00"), P("pay_4", "333.00", "2026-11-13T12:00:00")]
write_case(31, "mixed_file_ref_and_noref", """One file, both kinds: line 1 names pay_1 and matches; line 2 is unreferenced and, with pay_1 already
    settled, is uniquely pay_2 + pay_3 among the rest.""",
    {"payments": pays31, "files": [{"date": D, "lines": [{"amount": rupees(net(49900)), "reference": "pay_1"},
                                                           {"amount": nets_line(["pay_2", "pay_3"], pays31)}]}]},
    {"lines": {f"{F}:1": {"status": "MATCHED", "pays": ["pay_1"]}, f"{F}:2": {"status": "INFERRED", "pays": ["pay_2", "pay_3"]}},
     "balances": {"SUSPENSE": "0.00"}})

pays32 = [P("pay_1", "499.00", "2026-11-13T09:00:00"), P("pay_2", "499.00", "2026-11-13T10:00:00")]
write_case(32, "duplicate_bank_lines_same_amount_same_day", """Two unreferenced lines of the same amount on the same day, and two payments of that net. Each line
    could be either payment, and propagation cannot split them: both lines are AMBIGUOUS (§6.4: cautious, never wrong).""",
    {"payments": pays32, "files": [{"date": D, "lines": [{"amount": rupees(net(49900))}, {"amount": rupees(net(49900))}]}]},
    {"lines": {f"{F}:1": {"status": "AMBIGUOUS"}, f"{F}:2": {"status": "AMBIGUOUS"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:1", "reason": "AMBIGUOUS"},
                {"type": "ORPHAN_SETTLEMENT", "subject": f"{F}:2", "reason": "AMBIGUOUS"}]})

pays33 = [{"id": "pay_3e11", "gross_paise": 3 * 10**11, "captured": "2026-11-13T09:00:00"},
          {"id": "pay_4e11", "gross_paise": 4 * 10**11, "captured": "2026-11-13T10:00:00"},
          {"id": "pay_1e12", "gross_paise": 10**12, "captured": "2026-11-13T11:00:00"}]
write_case(33, "largest_accepted_amounts", """Fee-free payments of 3 and 4 × 10^11 paise and one at the largest accepted amount, 10^12 paise
    (D93: larger amounts are refused at the edge, so the matcher's arithmetic cannot overflow). The line is 1.4 × 10^12
    paise, uniquely 4 × 10^11 + 10^12. Every sum is exact (Math.addExact).""",
    {"fees": {"fee_bps": 0, "gst_bps": 0}, "payments": pays33, "files": [{"date": D, "lines": [{"amount": rupees(14 * 10**11)}]}]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": ["pay_4e11", "pay_1e12"]}},
     "balances": {"SUSPENSE": "0.00", "GATEWAY_RECEIVABLE": rupees(3 * 10**11)}})

write_case(34, "watermark_gate_not_passed", """The gateway watermark has not passed the window's end plus bound_G (no tick after the first batch),
    so a payment from the window may still be on its way: the line waits (AWAITING_BATCH), held in suspense (a credit,
    so the debit-positive balance is negative), with no break (§6.4 step 5).""",
    {"watermark": "not_passed", "payments": [P("pay_1", "499.00")], "files": [{"date": D, "lines": [{"amount": rupees(net(49900))}]}]},
    {"lines": {f"{F}:1": {"status": "AWAITING_BATCH"}}, "balances": {"SUSPENSE": "-" + rupees(net(49900))}}, check=False)

pays35 = [P("pay_1", "600.00", "2026-11-13T09:00:00"), P("pay_2", "400.00", "2026-11-13T10:00:00")]
write_case(35, "late_payment_after_inferred_match", """The line is inferred to be pay_1 + pay_2. Afterwards a success for pay_3, captured inside the solved
    window, arrives: the proof was made over an incomplete candidate set, so the line gets an
    INFERRED_MATCH_CHALLENGED anomaly, to be escalated (§6.4 residual risk, mitigation 3).""",
    {"payments": pays35, "files": [{"date": D, "lines": [{"amount": nets_line(["pay_1", "pay_2"], pays35)}]}],
     "after_solve": [P("pay_3", "250.00", "2026-11-13T18:00:00")]},
    {"lines": {f"{F}:1": {"status": "INFERRED", "pays": ["pay_1", "pay_2"]}},
     "breaks": [{"type": "INFERRED_MATCH_CHALLENGED", "subject": f"{F}:1", "evidence": {"payment_id": "pay_3"}}]})

print("35 cases written to", OUT)

# ---- golden end-to-end files: real Settlement File v1 CSVs, parsed, matched, ledger balances asserted ---------------

def csv_field(s):
    return '"' + s.replace('"', '""') + '"' if ("," in s or '"' in s) else s

def write_golden(num, name, about, payments, file_id, lines, expect, crlf=False):
    """lines: dicts with amount, optional fee/tax/reference, narration."""
    model_case = {"payments": payments, "files": [{"date": D, "lines": lines}]}
    got = model(model_case)
    for no, (status, pays) in got.items():
        want = expect["lines"][f"{file_id}:{no}"]
        assert want["status"] == status, (name, no, want, status)
        if status == "INFERRED":
            assert sorted(want["pays"]) == pays, (name, no, want, pays)
    total = sum(g(l["amount"]) for l in lines)
    recs = [f"H,DLSF,1,{file_id},m_0,{D},2026-11-14T04:30:00Z"]
    for i, l in enumerate(lines, start=1):
        recs.append(",".join(["D", str(i), D, l["amount"], l.get("fee", ""), l.get("tax", ""), l.get("reference", ""),
                              "UTR20261114M0", csv_field(l["narration"])]))
    recs.append(f"T,{len(lines)},{rupees(total)}")
    sep = "\r\n" if crlf else "\n"
    csv_name = f"golden/{name}.csv"
    with open(os.path.join(OUT, csv_name), "w", newline="") as fh:
        fh.write(sep.join(recs) + sep)
    case = {"first_settlement_date": D, "payments": payments, "files": [{"csv": csv_name}]}
    # reuse the YAML writer, then drop the numbered "case:" line (golden files are not part of the 35)
    write_case(num, name, about, case, expect, check=False)
    src = os.path.join(OUT, f"{num:02d}_{name}.yaml")
    text = open(src).read().split("\n", 1)[1]
    os.remove(src)
    with open(os.path.join(OUT, f"golden_{num - 100}_{name}.yaml"), "w") as fh:
        fh.write(text)

def fees_of(ids, pays):
    by = {p["id"]: p for p in pays}
    f = sum(fee_gst(g(by[i]["gross"]))[0] for i in ids)
    t = sum(fee_gst(g(by[i]["gross"]))[1] for i in ids)
    return f, t

# G1: a referenced daily file, CRLF, mixed itemisation, one short line explained by its itemisation
g1 = [P("pay_1", "499.00", "2026-11-13T09:00:00"), P("pay_2", "1250.00", "2026-11-13T10:00:00"),
      P("pay_3", "77.77", "2026-11-13T11:00:00"), P("pay_4", "2000.00", "2026-11-13T12:00:00")]
f1, t1 = fee_gst(49900); f3, t3 = fee_gst(7777)
g1_lines = [{"amount": rupees(net(49900)), "fee": rupees(f1), "tax": rupees(t1), "reference": "pay_1", "narration": "settlement pay_1"},
            {"amount": rupees(net(125000)), "reference": "pay_2", "narration": "settlement pay_2"},
            {"amount": rupees(net(7777)), "fee": rupees(f3), "tax": rupees(t3), "reference": "pay_3", "narration": "settlement pay_3, T+1"},
            {"amount": rupees(200000 - 4500 - 720), "fee": "45.00", "tax": "7.20", "reference": "pay_4", "narration": "settlement pay_4"}]
fees_g1 = f1 + fee_gst(125000)[0] + f3 + 4500
gst_g1 = t1 + fee_gst(125000)[1] + t3 + 720
write_golden(101, "referenced_daily_crlf", """A referenced daily file with CRLF line endings and a quoted narration: three exact lines (two itemised)
    and one short line whose itemised fee explains it (pricing drift 5.00). Fees and GST post as itemised.""",
    g1, "stl_g1_20261114", g1_lines,
    {"lines": {"stl_g1_20261114:1": {"status": "MATCHED", "pays": ["pay_1"]}, "stl_g1_20261114:2": {"status": "MATCHED", "pays": ["pay_2"]},
               "stl_g1_20261114:3": {"status": "MATCHED", "pays": ["pay_3"]}, "stl_g1_20261114:4": {"status": "SHORT_EXPLAINED", "pays": ["pay_4"]}},
     "breaks": [{"type": "SHORT_SETTLEMENT", "subject": "stl_g1_20261114:4", "state": "RESOLVED_AUTO", "evidence": {"pricing_drift": "5.00"}}],
     "balances": {"BANK": rupees(sum(g(l["amount"]) for l in g1_lines)), "GATEWAY_FEES": rupees(fees_g1),
                  "GST_INPUT_CREDIT": rupees(gst_g1), "GATEWAY_RECEIVABLE": "0.00", "SUSPENSE": "0.00"}}, crlf=True)

# G2: one unreferenced batch line paying 5 of 7 candidates, inferred
g2 = [P(f"pay_{i}", r, f"2026-11-13T{8 + i:02d}:30:00") for i, r in
      enumerate(["349.00", "1875.25", "640.10", "2210.00", "99.99", "1333.33", "502.48"], start=1)]
in2 = ["pay_1", "pay_2", "pay_4", "pay_5", "pay_7"]
amt2 = nets_line(in2, g2)
fe2, gs2 = fees_of(in2, g2)
write_golden(102, "batch_inferred", """One unreferenced lump-sum credit paying five of the merchant's seven open payments. Exactly one subset
    fits, so it is MATCHED_INFERRED: the suspense is released against the five payments' contracted fees and GST,
    and the other two stay receivable.""",
    g2, "stl_g2_20261114", [{"amount": amt2, "narration": "batch payout"}],
    {"lines": {"stl_g2_20261114:1": {"status": "INFERRED", "pays": in2}},
     "balances": {"BANK": amt2, "GATEWAY_FEES": rupees(fe2), "GST_INPUT_CREDIT": rupees(gs2),
                  "GATEWAY_RECEIVABLE": rupees(g("640.10") + g("1333.33")), "SUSPENSE": "0.00"}})

# G3: catalogue prices, two batch lines, both ambiguous
g3 = [P(f"pay_{i}", "499.00", f"2026-11-13T{9 + i:02d}:00:00") for i in range(1, 5)] + [P("pay_5", "999.00", "2026-11-13T15:00:00")]
l3a = rupees(2 * net(49900)); l3b = rupees(net(49900) + net(99900))
write_golden(103, "batch_ambiguous_catalogue", """Four orders at 499.00 and one at 999.00, paid in two unreferenced lines (2 x 499 and 499 + 999). Any
    499 can stand in for any other, so both lines are AMBIGUOUS: nothing is guessed, the money stays in suspense,
    and each line keeps two witnesses.""",
    g3, "stl_g3_20261114", [{"amount": l3a, "narration": "batch payout 1"}, {"amount": l3b, "narration": "batch payout 2"}],
    {"lines": {"stl_g3_20261114:1": {"status": "AMBIGUOUS"}, "stl_g3_20261114:2": {"status": "AMBIGUOUS"}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": "stl_g3_20261114:1", "reason": "AMBIGUOUS"},
                {"type": "ORPHAN_SETTLEMENT", "subject": "stl_g3_20261114:2", "reason": "AMBIGUOUS"}],
     "balances": {"BANK": rupees(g(l3a) + g(l3b)), "SUSPENSE": "-" + rupees(g(l3a) + g(l3b)),
                  "GATEWAY_RECEIVABLE": rupees(4 * 49900 + 99900)}})

# G4: referenced and unreferenced lines in one file, quoted narration with a comma
g4 = [P("pay_1", "499.00", "2026-11-13T09:00:00"), P("pay_2", "750.00", "2026-11-13T10:00:00"),
      P("pay_3", "1200.00", "2026-11-13T11:00:00"), P("pay_4", "333.00", "2026-11-13T12:00:00")]
amt4 = nets_line(["pay_2", "pay_3"], g4)
fe4, gs4 = fees_of(["pay_1", "pay_2", "pay_3"], g4)
write_golden(104, "mixed_ref_and_batch", """A referenced line and an unreferenced batch line in one file; the batch narration contains a comma, so
    it is quoted. pay_1 matches by reference; the batch is uniquely pay_2 + pay_3; pay_4 stays receivable.""",
    g4, "stl_g4_20261114", [{"amount": rupees(net(49900)), "reference": "pay_1", "narration": "settlement pay_1"},
                            {"amount": amt4, "narration": "batch payout, 2 payments"}],
    {"lines": {"stl_g4_20261114:1": {"status": "MATCHED", "pays": ["pay_1"]}, "stl_g4_20261114:2": {"status": "INFERRED", "pays": ["pay_2", "pay_3"]}},
     "balances": {"BANK": rupees(net(49900) + g(amt4)), "GATEWAY_FEES": rupees(fe4), "GST_INPUT_CREDIT": rupees(gs4),
                  "GATEWAY_RECEIVABLE": "333.00", "SUSPENSE": "0.00"}})

# G5: two batch lines resolved by forced-assignment propagation, with the contracted fees
g5 = [P("pay_250", "250.00", "2026-11-13T09:00:00"), P("pay_400", "400.00", "2026-11-13T10:00:00"),
      P("pay_650", "650.00", "2026-11-13T11:00:00"), P("pay_1000", "1000.00", "2026-11-13T12:00:00")]
assert net(25000) + net(40000) == net(65000)
l5a = rupees(net(65000)); l5b = rupees(net(40000) + net(100000))
fe5, gs5 = fees_of(["pay_400", "pay_650", "pay_1000"], g5)
write_golden(105, "batch_propagation", f"""With the contracted fees, net(250.00) + net(400.00) = net(650.00) = {l5a} exactly, so line 1 alone is
    ambiguous. Line 2 ({l5b}) is uniquely pay_400 + pay_1000; committing it leaves only pay_650 for line 1.""",
    g5, "stl_g5_20261114", [{"amount": l5a, "narration": "batch payout 1"}, {"amount": l5b, "narration": "batch payout 2"}],
    {"lines": {"stl_g5_20261114:1": {"status": "INFERRED", "pays": ["pay_650"]}, "stl_g5_20261114:2": {"status": "INFERRED", "pays": ["pay_1000", "pay_400"]}},
     "balances": {"BANK": rupees(g(l5a) + g(l5b)), "GATEWAY_FEES": rupees(fe5), "GST_INPUT_CREDIT": rupees(gs5),
                  "GATEWAY_RECEIVABLE": "250.00", "SUSPENSE": "0.00"}})

# G6: an unknown reference and an unexplainable batch line, both held in suspense, beside a normal line
g6 = [P("pay_1", "499.00", "2026-11-13T09:00:00")]
write_golden(106, "orphans_in_suspense", """Line 1 names a payment Driftline never saw; line 2 is an unreferenced 333.33 that nothing explains; line
    3 settles pay_1 normally. Both orphans are held in suspense with their reasons.""",
    g6, "stl_g6_20261114", [{"amount": "120.00", "reference": "pay_ghost", "narration": "unidentified credit"},
                            {"amount": "333.33", "narration": "batch payout"},
                            {"amount": rupees(net(49900)), "reference": "pay_1", "narration": "settlement pay_1"}],
    {"lines": {"stl_g6_20261114:1": {"status": "UNATTRIBUTED"}, "stl_g6_20261114:2": {"status": "NO_SOLUTION"},
               "stl_g6_20261114:3": {"status": "MATCHED", "pays": ["pay_1"]}},
     "breaks": [{"type": "ORPHAN_SETTLEMENT", "subject": "stl_g6_20261114:1", "reason": "UNKNOWN_REFERENCE"},
                {"type": "ORPHAN_SETTLEMENT", "subject": "stl_g6_20261114:2", "reason": "NO_SOLUTION"}],
     "balances": {"BANK": rupees(12000 + 33333 + net(49900)), "SUSPENSE": "-453.33", "GATEWAY_RECEIVABLE": "0.00"}})

print("6 golden files written")
