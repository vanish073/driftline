#!/usr/bin/env python3
"""Waits for the injected breaks of scripts/resolution-compose.sh to settle, then tabulates their states (DESIGN §17).

    python3 resolution-compose.py BOOTSTRAP_JSON EXPECTED_JSON OUT_MD OUT_JSON [DRIFTLINE_URL] [TIMEOUT_S]

EXPECTED_JSON maps each injected type to the merchant it was played on and how many breaks the sandbox said Driftline
must show. Every POLL seconds the ops CSV export (GET /v1/exports/breaks.csv, ops token from the bootstrap file) is
read; the run settles when each type has at least its expected number of breaks of that type and no break on the
injected merchants has changed state for STABLE seconds, or fails at TIMEOUT_S. A break may stay open when it settles:
an orphan line waits in suspense (AWAITING_EVIDENCE) until its deadline, days away. Only breaks of the injected type on
its own merchant are counted for that type; anything else raised on those merchants is listed apart, so a stray break
cannot inflate a share. Each escalated break's reason (the reason of its transition to ESCALATED, from
GET /v1/breaks/{id}) is tallied per type in BookStats' categories. Writes the table (Markdown) and the counts (JSON).
Exit 0 when settled, 1 if not.
"""
import csv
import io
import json
import sys
import time
import urllib.request

OPEN = {"DETECTED", "AWAITING_EVIDENCE", "ACTION_PENDING"}
AUTO = ["RESOLVED_AUTO", "SELF_HEALED", "LINKED"]
POLL = 10
STABLE = 180
# BookStats.ESCALATION_REASONS: the reason's prefix, and its category (the driftline_breaks_escalated_total label)
REASONS = [("escalate_by ", "deadline"), ("unreferenced line ", "batch_unresolved"),
           ("evidence gathered; ", "for_a_person"), ("another break owns ", "action_owner_conflict"),
           ("the gateway reported the refund failed", "refund_failed"),
           ("the payment is no longer a clean success", "not_clean_success"),
           ("the order was paid with this payment ", "paid_after_refund_decided"),
           ("the order has an open IDEMPOTENCY_CONFLICT", "idempotency_conflict"),
           ("the duplicate names no kept payment", "duplicate_unconfirmed"),
           ("the kept payment is not confirmed ", "duplicate_unconfirmed"),
           ("the gateway does not confirm the success ", "success_unconfirmed"),
           ("the merchant has never reported this order", "order_unknown")]


def get(url, token, path):
    req = urllib.request.Request(url + path, headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8")


def breaks(url, token):
    return list(csv.DictReader(io.StringIO(get(url, token, "/v1/exports/breaks.csv"))))


def category(reason):
    for prefix, name in REASONS:
        if reason.startswith(prefix):
            return name
    return "other"


def escalation_reasons(url, token, rows, expected):
    """{type: {category: n}} over the escalated breaks of each injected type on its merchant."""
    merchant_of = {e["merchant"]: t for t, e in expected.items()}
    out = {t: {} for t in expected}
    for r in rows:
        t = merchant_of.get(r["merchant_id"])
        if t is None or r["type"] != t or r["state"] != "ESCALATED":
            continue
        detail = json.loads(get(url, token, "/v1/breaks/" + r["break_id"]))
        to_escalated = [x for x in detail["transitions"] if x["to"] == "ESCALATED"]
        c = category(to_escalated[-1]["reason"]) if to_escalated else "(no transition)"
        out[t][c] = out[t].get(c, 0) + 1
    return out


def tally(rows, expected):
    """({type: {state: n}} for each injected type on its merchant, {"merchant type": {state: n}} for the rest)."""
    by_type = {t: {} for t in expected}
    other = {}
    merchant_of = {e["merchant"]: t for t, e in expected.items()}
    for r in rows:
        t = merchant_of.get(r["merchant_id"])
        if t is None:
            continue
        if r["type"] == t:
            by_type[t][r["state"]] = by_type[t].get(r["state"], 0) + 1
        else:
            key = r["merchant_id"] + " " + r["type"]
            other.setdefault(key, {})
            other[key][r["state"]] = other[key].get(r["state"], 0) + 1
    return by_type, other


def complete(by_type, expected):
    """Every break the sandbox expects of each type exists."""
    return all(sum(by_type[t].values()) >= e["expected"] for t, e in expected.items())


def share(n, total):
    return "%.1f%%" % (100.0 * n / total) if total else "-"


def main(argv):
    bootstrap, expected_path, out_md, out_json = argv[1:5]
    url = argv[5] if len(argv) > 5 else "http://127.0.0.1:8080"
    timeout = int(argv[6]) if len(argv) > 6 else 900
    token = json.load(open(bootstrap, encoding="utf-8"))["ops_token"]
    expected = json.load(open(expected_path, encoding="utf-8"))
    started = time.time()
    last, changed_at = None, started
    while True:
        rows = breaks(url, token)
        by_type, other = tally(rows, expected)
        now = time.time()
        signature = json.dumps([by_type, other], sort_keys=True)
        if signature != last:
            last, changed_at = signature, now
        done = complete(by_type, expected) and now - changed_at >= STABLE
        if done or now - started > timeout:
            break
        time.sleep(POLL)
    waited = round(time.time() - started)
    reasons = escalation_reasons(url, token, rows, expected)
    lines = ["| Type | Injected (sandbox's expected breaks) | Breaks | Closed without a person | RESOLVED_AUTO | "
             "SELF_HEALED | LINKED | ESCALATED | Still open |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for t, e in expected.items():
        s = by_type[t]
        total = sum(s.values())
        auto = sum(s.get(a, 0) for a in AUTO)
        still = sum(n for st, n in s.items() if st in OPEN)
        lines.append("| %s | %d | %d | %s | %s | %s | %s | %s | %s |" % (
            t, e["expected"], total, share(auto, total), share(s.get("RESOLVED_AUTO", 0), total),
            share(s.get("SELF_HEALED", 0), total), share(s.get("LINKED", 0), total),
            share(s.get("ESCALATED", 0), total), share(still, total)))
    with open(out_md, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")
        f.write("\nEscalation reasons: " + "; ".join(
            "%s: %s" % (t, ", ".join("%s (%d)" % (k, n) for k, n in sorted(rs.items())))
            for t, rs in reasons.items() if rs) + "\n")
        open_states = {t: {st: n for st, n in s.items() if st in OPEN} for t, s in by_type.items()}
        if any(open_states.values()):
            f.write("\nStill open when the states had stopped changing: " + "; ".join(
                "%s %s" % (t, json.dumps(o, sort_keys=True)) for t, o in open_states.items() if o) + "\n")
        if other:
            f.write("\nOther breaks on the injected merchants (not counted above): "
                    + "; ".join("%s %s" % (k, json.dumps(v, sort_keys=True)) for k, v in sorted(other.items())) + "\n")
    with open(out_json, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"expected": expected, "states": by_type, "other": other, "escalation_reasons": reasons,
                   "settled": done, "stable_seconds": STABLE,
                   "waited_seconds": waited}, f, indent=2, sort_keys=True)
        f.write("\n")
    print("\n".join(lines))
    print("settled after %d s (no change for %d s)" % (waited, STABLE) if done else "NOT settled after %d s" % waited)
    return 0 if done else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
