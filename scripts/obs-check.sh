#!/usr/bin/env bash
# Checks the Compose `obs` profile on a running stack that has had traffic (DESIGN §13, D78):
#   1. Prometheus scrapes every Driftline process it discovers (all targets up), has loaded the 4 alert rules, and has
#      Driftline's metrics;
#   2. Grafana has provisioned the Driftline dashboard and its Prometheus datasource is healthy;
#   3. Jaeger holds a trace in which dispatcher.call descends, parent by parent, from resolver.decide, matcher.detect and
#      ingest.webhook (printed with the agent's spans around them, such as its HTTP client span under the call).
# Used by `OBS=1 scripts/e2e.sh`. Reads the Grafana admin password from deploy/compose/.dev/env.
set -euo pipefail
cd "$(dirname "$0")/.."

PROM=http://127.0.0.1:9090
GRAFANA=http://127.0.0.1:3000
JAEGER=http://127.0.0.1:16686
failed=0
fail() {
    echo "obs-check: FAIL: $*" >&2
    failed=1
}

# Prometheus scrapes every 15 s: wait until the metrics are there.
for _ in $(seq 1 30); do
    if curl -fsS "$PROM/api/v1/query?query=driftline_events_consumed_total" | grep -q '"value"'; then
        break
    fi
    sleep 2
done
python3 -I - "$(curl -fsS "$PROM/api/v1/targets")" "$(curl -fsS "$PROM/api/v1/rules")" \
        "$(curl -fsS "$PROM/api/v1/query?query=driftline_events_consumed_total")" <<'PY' || failed=1
import json, sys
targets = [t for t in json.loads(sys.argv[1])["data"]["activeTargets"] if t["labels"].get("job") == "driftline"]
rules = [r["name"] for g in json.loads(sys.argv[2])["data"]["groups"] for r in g["rules"]]
consumed = json.loads(sys.argv[3])["data"]["result"]
ok = True
if not targets or any(t["health"] != "up" for t in targets):
    print("obs-check: FAIL: Prometheus targets:", [(t["labels"]["instance"], t["health"]) for t in targets],
          file=sys.stderr)
    ok = False
else:
    print("obs-check: Prometheus scrapes", len(targets), "Driftline target(s), all up")
expected = {"OutboxCommandStuck", "BreakDeadlineNear", "SettlementFeedStalled", "ConsumerLagGrowing"}
if set(rules) != expected:
    print("obs-check: FAIL: alert rules loaded:", sorted(rules), file=sys.stderr)
    ok = False
else:
    print("obs-check: Prometheus has loaded the 4 alert rules")
if not consumed:
    print("obs-check: FAIL: no driftline_events_consumed_total samples", file=sys.stderr)
    ok = False
else:
    total = sum(float(r["value"][1]) for r in consumed)
    print("obs-check: Prometheus has driftline_events_consumed_total =", int(total))
sys.exit(0 if ok else 1)
PY

password=$(grep '^GRAFANA_ADMIN_PASSWORD=' deploy/compose/.dev/env | cut -d= -f2-)
if dashboard=$(curl -fsS "$GRAFANA/api/dashboards/uid/driftline"); then
    panels=$(printf '%s' "$dashboard" | python3 -I -c 'import json, sys; print(len(json.load(sys.stdin)["dashboard"]["panels"]))')
    echo "obs-check: Grafana serves the Driftline dashboard ($panels panels)"
else
    fail "Grafana has no dashboard with uid driftline"
fi
if curl -fsS -u "admin:$password" "$GRAFANA/api/datasources/uid/prometheus/health" | grep -q '"status":"OK"'; then
    echo "obs-check: Grafana's Prometheus datasource is healthy"
else
    fail "Grafana's Prometheus datasource is not healthy"
fi

# Prints the first trace in which dispatcher.call descends, parent by parent, from ingest.webhook through matcher.detect
# and resolver.decide, given the file holding Jaeger's /api/v3/traces answer (a file: it can exceed the argument limit).
chain_of() {
    python3 -I - "$1" <<'PY'
import json, sys
try:
    with open(sys.argv[1], encoding="utf-8") as f:
        data = json.load(f)["result"]["resourceSpans"]
except (OSError, ValueError, KeyError, TypeError):
    data = []
spans = [s for rs in data for ss in rs.get("scopeSpans", []) for s in ss.get("spans", [])]
by_trace = {}
for s in spans:
    by_trace.setdefault(s["traceId"], []).append(s)
chain = ["ingest.webhook", "matcher.detect", "resolver.decide", "dispatcher.call"]
for trace, ss in sorted(by_trace.items()):
    by_id = {s["spanId"]: s for s in ss}
    for call in (s for s in ss if s["name"] == "dispatcher.call"):
        names, node = [], call
        while node is not None:
            names.append(node["name"])
            node = by_id.get(node.get("parentSpanId", ""))
        names.reverse()
        if [n for n in names if n in chain] == chain:
            under = sorted(s["name"] for s in ss if s.get("parentSpanId") == call["spanId"])
            print(trace + ":", " -> ".join(names), "| under dispatcher.call:", ", ".join(under) or "none")
            sys.exit(0)
PY
}

# The collector batches spans: wait up to a minute for a complete chain. Jaeger 2 serves its query API under /api/v3,
# with OTLP JSON results; parentage is checked span by span, not only that the four names share a trace.
chain=""
traces=$(mktemp)
trap 'rm -f "$traces"' EXIT
for _ in $(seq 1 30); do
    since=$(date -u -d '-2 hours' +%Y-%m-%dT%H:%M:%SZ)
    upto=$(date -u -d '+5 minutes' +%Y-%m-%dT%H:%M:%SZ)
    curl -fsS -o "$traces" "$JAEGER/api/v3/traces?query.service_name=driftline&query.operation_name=dispatcher.call&query.start_time_min=$since&query.start_time_max=$upto&query.search_depth=100" || true
    chain=$(chain_of "$traces")
    if [ -n "$chain" ]; then
        break
    fi
    sleep 2
done
if [ -n "$chain" ]; then
    echo "obs-check: Jaeger trace $chain"
else
    fail "no trace in Jaeger runs parent to child from ingest.webhook through matcher.detect and resolver.decide to dispatcher.call"
fi

if [ "$failed" -ne 0 ]; then
    exit 1
fi
echo "obs-check: the obs profile works"
