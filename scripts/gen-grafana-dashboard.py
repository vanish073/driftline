#!/usr/bin/env python3
"""Writes the Driftline Grafana dashboard (DESIGN §13, D78) to deploy/grafana/dashboards/driftline.json.

    python3 scripts/gen-grafana-dashboard.py [out-file]

The board shows what §13 lists: throughput, event-to-match p50 and p99, watermark and consumer lag, breaks by type and
state, deadline headroom, compensation exposure, outbox health and settlement results. The books' metrics are reported
by every resolver instance with the same value, so the board takes max() of them, never sum(). The output is
deterministic: running the script again reproduces the committed file byte for byte.
"""
import json
import sys

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []
y = 0


def row(title):
    global y
    panels.append({"type": "row", "title": title, "collapsed": False, "id": len(panels) + 1,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})
    y += 1


def panel(kind, title, targets, x, w, h=8, unit="short", description="", thresholds=None, legend=True):
    p = {
        "type": kind, "title": title, "id": len(panels) + 1, "datasource": DS, "description": description,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": [{"refId": chr(ord("A") + i), "datasource": DS, "expr": expr, "legendFormat": legend_format,
                     "range": True} for i, (expr, legend_format) in enumerate(targets)],
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {},
    }
    if thresholds:
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
    if kind == "timeseries":
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": legend},
                        "tooltip": {"mode": "multi", "sort": "desc"}}
        if thresholds:
            p["fieldConfig"]["defaults"]["custom"] = {"thresholdsStyle": {"mode": "line"}}
    if kind == "stat":
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "value", "graphMode": "area", "textMode": "auto"}
    panels.append(p)


def next_row(h=8):
    global y
    y += h


row("Throughput")
panel("timeseries", "Events consumed per second, by source",
      [("sum by (source) (rate(driftline_events_consumed_total[1m]))", "{{source}}")], 0, 8, unit="ops")
panel("timeseries", "Ingest requests per second, by route and status",
      [("sum by (route, status) (rate(driftline_ingest_requests_total[1m]))", "{{route}} {{status}}")], 8, 8,
      unit="reqps")
panel("timeseries", "Duplicates, late events and rejected webhooks per second",
      [("sum by (source) (rate(driftline_events_duplicate_total[5m]))", "duplicate {{source}}"),
       ("sum by (source) (rate(driftline_events_late_total[5m]))", "late {{source}}"),
       ("sum by (reason) (rate(driftline_webhook_rejected_total[5m]))", "rejected {{reason}}")], 16, 8, unit="ops")
next_row()

row("Latency")
panel("timeseries", "Event to match: p50 and p99",
      [("histogram_quantile(0.5, sum by (le) (rate(driftline_event_to_match_seconds_bucket[5m])))", "p50"),
       ("histogram_quantile(0.99, sum by (le) (rate(driftline_event_to_match_seconds_bucket[5m])))", "p99")],
      0, 12, unit="s", description="From ingest receivedAt to the matcher commit that applied the event.")
panel("timeseries", "Detection lag p99, by break type",
      [("histogram_quantile(0.99, sum by (le, type) (rate(driftline_detection_lag_seconds_bucket[15m])))",
        "{{type}}")], 12, 12, unit="s",
      description="From the arrival of the evidence that completed a break to its commit.")
next_row()

row("Progress")
panel("timeseries", "Watermark lag, by partition and source",
      [("max by (partition, source) (driftline_watermark_lag_seconds)", "p{{partition}} {{source}}")], 0, 10,
      unit="s", legend=False)
panel("timeseries", "Consumer lag, by partition",
      [("sum by (partition) (driftline_consumer_lag_records)", "p{{partition}}")], 10, 10, legend=False)
panel("stat", "Fenced commits (1 h)", [("sum(increase(driftline_partition_fenced_total[1h]))", "fenced")], 20, 4)
next_row()

row("Breaks")
panel("timeseries", "Open breaks, by type and state",
      [("max by (type, state) (driftline_breaks_open)", "{{type}} {{state}}")], 0, 12)
panel("timeseries", "Breaks detected, closed and escalated per hour",
      [("max by (type) (increase(driftline_breaks_detected_total[1h]))", "detected {{type}}"),
       ("max by (type, how) (increase(driftline_breaks_closed_total[1h]))", "closed {{type}} {{how}}"),
       ("max by (type, reason) (increase(driftline_breaks_escalated_total[1h]))", "escalated {{type}} {{reason}}")],
      12, 12)
next_row()

row("Deadlines")
panel("timeseries", "Deadline headroom, by break type",
      [("min by (type) (driftline_deadline_headroom_seconds)", "{{type}}")], 0, 16, unit="s",
      description="Time left before the earliest deadline among open breaks; the alert fires below 6 hours.",
      thresholds=[{"color": "red", "value": None}, {"color": "orange", "value": 21600},
                  {"color": "green", "value": 86400}])
panel("stat", "Compensation exposure", [("max(driftline_compensation_exposure_rupees)", "INR")], 16, 8,
      unit="currencyINR", description="What the open breaks would owe under the RBI rules if resolved now.",
      thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}])
next_row()

row("Outbox")
panel("timeseries", "Pending commands, by kind", [("max by (kind) (driftline_outbox_pending)", "{{kind}}")], 0, 8)
panel("stat", "Oldest pending command", [("max(driftline_outbox_oldest_pending_seconds)", "oldest")], 8, 4,
      unit="s", thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 600}])
panel("timeseries", "Call attempts per second, by kind and result",
      [("sum by (kind, result) (rate(driftline_outbox_attempts_total[5m]))", "{{kind}} {{result}}")], 12, 12,
      unit="ops")
next_row()

row("Settlement")
panel("timeseries", "Settlement lines received and in each final status",
      [("max by (result) (driftline_settlement_lines_total)", "{{result}}")], 0, 8)
panel("timeseries", "Settlement lines still unresolved, by status",
      [("max by (status) (driftline_settlement_lines_unresolved)", "{{status}}")], 8, 6)
panel("stat", "Pricing drift (lines)", [("max(driftline_pricing_drift_total)", "lines")], 14, 4)
panel("timeseries", "Subset-sum search cost p99",
      [("histogram_quantile(0.99, sum by (le) (rate(driftline_subset_sum_ops_bucket[15m])))", "p99 ops")], 18, 6)
next_row()

dashboard = {
    "uid": "driftline", "title": "Driftline", "tags": ["driftline"], "timezone": "utc", "schemaVersion": 41,
    "version": 1, "editable": False, "refresh": "15s", "time": {"from": "now-1h", "to": "now"},
    "templating": {"list": []}, "annotations": {"list": []}, "panels": panels,
}
out = sys.argv[1] if len(sys.argv) > 1 else "deploy/grafana/dashboards/driftline.json"
with open(out, "w", encoding="utf-8", newline="\n") as f:
    json.dump(dashboard, f, indent=2, ensure_ascii=False, sort_keys=True)
    f.write("\n")
