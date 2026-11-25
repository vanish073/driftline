#!/usr/bin/env python3
"""Judges one load step for scripts/load-test.sh (DESIGN §17) and writes its JSON record.

    python3 load-evaluate.py GENERATOR_JSON STEP_END_EPOCH VMSTAT_FILE STEP_START_EPOCH OUT_JSON [PROMETHEUS_URL
        [DOCKER_STATS_FILE]]

Sustained means, over the last 5 minutes of the step: the consumer lag did not grow (least-squares slope of
sum(driftline_consumer_lag_records) at most 0.1 records/s, from at least 10 samples), and the generator saw no
rejection, transport error, skip or unanswered request. driftline_consumer_lag_records is the backlog probe's (D83): the topic's end offsets
minus the offsets stored as processed, measured outside the matcher's loop. The Kafka client's own estimate
(driftline_consumer_client_lag_records), which froze while the loop was stuck, is recorded beside it for comparison.
A step during which the machine itself failed is INVALID, whatever Driftline did: it paused (vmstat, which writes a
line every second, has a gap of more than 5 seconds, or does not cover the step: a laptop asleep or a suspended WSL VM),
or it swapped (at least 256 KB swapped in each second for 5 consecutive seconds, DESIGN §15.1; a trickle of a few pages
a second is recorded but does not count).
It also records, for information only (no verdict depends on them), the JVM's GC pauses and CPU use over the window,
the machine's busiest second, each container's CPU (mean and peak over the window, and at the sample nearest the
busiest second) and the generator's own CPU, so a busy machine can be attributed to Driftline's stack, the generator
or something outside both.
Prints the verdict; exits 0 if sustained, 1 if not, 2 if INVALID.
"""
import base64
import json
import os
import sys
import urllib.parse
import urllib.request

WINDOW = 300
MAX_LAG_SLOPE = 0.1
MAX_SAMPLE_GAP = 5
MAX_SWAP_STREAK = 4
SWAP_KB_PER_S = 256
MIN_LAG_SAMPLES = 10


def machine(vmstat_path, start, end):
    """The vmstat lines inside [start, end] (each ends with the epoch load-test.sh appends).

    Returns (longest streak of seconds that each swapped in at least SWAP_KB_PER_S, swap-in KB, samples, largest gap
    between samples, paused).
    """
    longest = run = total_kb = 0
    stamps = []
    for line in open(vmstat_path, encoding="utf-8"):
        parts = line.split()
        if len(parts) < 18 or not parts[0].isdigit():
            continue
        try:
            stamp = int(parts[-1])
        except ValueError:
            continue
        if not start <= stamp <= end:
            continue
        stamps.append(stamp)
        si = int(parts[6])
        total_kb += si
        run = run + 1 if si >= SWAP_KB_PER_S else 0
        longest = max(longest, run)
    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    largest_gap = max(gaps, default=end - start)
    paused = (not stamps or stamps[0] > start + MAX_SAMPLE_GAP or stamps[-1] < end - MAX_SAMPLE_GAP
              or largest_gap > MAX_SAMPLE_GAP)
    return longest, total_kb, len(stamps), largest_gap, paused


def busiest(vmstat_path, start, end):
    """(highest run queue, lowest idle CPU %, epoch of that second) of the vmstat lines inside [start, end]."""
    queue = idle = at = None
    for line in open(vmstat_path, encoding="utf-8"):
        parts = line.split()
        if len(parts) < 18 or not parts[0].isdigit():
            continue
        try:
            stamp = int(parts[-1])
        except ValueError:
            continue
        if start <= stamp <= end:
            queue = max(queue or 0, int(parts[0]))
            if idle is None or int(parts[14]) < idle:
                idle, at = int(parts[14]), stamp
    return queue, idle, at


def containers(stats_path, start, end, near):
    """Each container's CPU in cores from `docker stats` lines (epoch, name, CPU%, memory) inside [start, end]:
    {name: {"mean_cores", "max_cores", "samples"}}, and {name: cores} at the sample time nearest `near`."""
    by_name, at_time = {}, {}
    try:
        lines = open(stats_path, encoding="utf-8").read().splitlines()
    except OSError:
        return {}, {}
    for line in lines:
        parts = line.split()
        if len(parts) < 3 or not parts[0].isdigit() or not parts[2].endswith("%"):
            continue
        stamp, name = int(parts[0]), parts[1]
        try:
            cores = float(parts[2][:-1]) / 100
        except ValueError:
            continue
        if start <= stamp <= end:
            by_name.setdefault(name, []).append(cores)
            at_time.setdefault(stamp, {})[name] = cores
    summary = {n: {"mean_cores": round(sum(v) / len(v), 3), "max_cores": round(max(v), 3), "samples": len(v)}
               for n, v in sorted(by_name.items())}
    nearest = {}
    if near is not None and at_time:
        nearest = {n: round(c, 3) for n, c in sorted(at_time[min(at_time, key=lambda t: abs(t - near))].items())}
    return summary, nearest


def slope(series):
    """Least-squares slope of [(t, v)] per second, or None with fewer than two points."""
    if len(series) < 2:
        return None
    n = len(series)
    mt = sum(t for t, _ in series) / n
    mv = sum(v for _, v in series) / n
    den = sum((t - mt) ** 2 for t, _ in series)
    return sum((t - mt) * (v - mv) for t, v in series) / den if den else 0.0


def machine_problems(swap_streak, paused, largest_gap):
    """What makes a step INVALID: the machine, not Driftline, failed during it."""
    out = []
    if paused:
        out.append(f"the machine paused (largest gap between vmstat samples {largest_gap} s)")
    if swap_streak > MAX_SWAP_STREAK:
        out.append(f"the machine swapped in at least {SWAP_KB_PER_S} KB/s for {swap_streak} s in a row")
    return out


def problems_of(gen, lag_slope, lag_samples=MIN_LAG_SAMPLES):
    """What makes a valid step not sustained."""
    out = []
    for key in ("rejected", "transport_errors", "skipped", "still_in_flight"):
        if gen[key] != 0:
            out.append(f"{key}={gen[key]}")
    if lag_slope is None or lag_samples < MIN_LAG_SAMPLES:
        out.append(f"{lag_samples} consumer-lag samples in the window, fewer than {MIN_LAG_SAMPLES}")
    elif lag_slope > MAX_LAG_SLOPE:
        out.append(f"consumer lag grew {lag_slope:.2f} records/s")
    return out


def verdict_of(invalid, problems):
    """(verdict text, exit status)."""
    if invalid:
        return "INVALID: " + "; ".join(invalid), 2
    if problems:
        return "not sustained: " + "; ".join(problems), 1
    return "sustained", 0


def main(argv):
    gen_path, end, vmstat_path, start, out_path = argv[1], int(argv[2]), argv[3], int(argv[4]), argv[5]
    prom = argv[6] if len(argv) > 6 else "http://127.0.0.1:9090"
    stats_path = argv[7] if len(argv) > 7 else None
    gen = json.load(open(gen_path, encoding="utf-8"))

    # D98: Prometheus asks for a password; PROMETHEUS_AUTH is user:password (scripts/load-test.sh reads the stack's file)
    auth = os.environ.get("PROMETHEUS_AUTH")
    headers = {"Authorization": "Basic " + base64.b64encode(auth.encode()).decode()} if auth else {}

    def get(url):
        return json.load(urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30))

    def query(expr, at):
        url = f"{prom}/api/v1/query?" + urllib.parse.urlencode({"query": expr, "time": at})
        data = get(url)["data"]["result"]
        return float(data[0]["value"][1]) if data else None

    def query_range(expr, frm, to, step=15):
        url = f"{prom}/api/v1/query_range?" + urllib.parse.urlencode(
            {"query": expr, "start": frm, "end": to, "step": step})
        data = get(url)["data"]["result"]
        return [(int(t), float(v)) for t, v in data[0]["values"]] if data else []

    lag = query_range("sum(driftline_consumer_lag_records)", end - WINDOW, end)
    lag_slope = slope(lag)
    client_lag = query_range("sum(driftline_consumer_client_lag_records)", end - WINDOW, end)
    p99 = query("histogram_quantile(0.99, sum by (le) (increase(driftline_event_to_match_seconds_bucket[5m])))", end)
    p50 = query("histogram_quantile(0.5, sum by (le) (increase(driftline_event_to_match_seconds_bucket[5m])))", end)
    consumed = query("sum(increase(driftline_events_consumed_total[5m])) / 300", end)
    accepted = query('sum(increase(driftline_ingest_requests_total{status="202"}[5m])) / 300', end)
    swap_streak, swap_kb, samples, largest_gap, paused = machine(vmstat_path, start, end)
    queue, idle, busiest_at = busiest(vmstat_path, end - WINDOW, end)
    per_container, at_busiest = containers(stats_path, end - WINDOW, end, busiest_at) if stats_path else ({}, {})
    cores = os.cpu_count()

    def info(expr):
        try:
            return query(expr, end)
        except (OSError, ValueError, KeyError, IndexError) as e:
            print(f"load-evaluate: {expr}: {e}", file=sys.stderr)
            return None
    jvm = {
        "gc_pause_max_s": info("max(max_over_time(jvm_gc_pause_seconds_max[5m]))"),
        "gc_pause_total_s": info("sum(increase(jvm_gc_pause_seconds_sum[5m]))"),
        "process_cpu_avg_share_of_machine": info("avg_over_time(process_cpu_usage[5m])"),
        "process_cpu_max_share_of_machine": info("max_over_time(process_cpu_usage[5m])"),
        "machine_cpu_max_share": info("max_over_time(system_cpu_usage[5m])"),
    }
    invalid = machine_problems(swap_streak, paused, largest_gap)
    problems = problems_of(gen, lag_slope, len(lag))
    verdict, status = verdict_of(invalid, problems)
    record = {
        "generator": gen,
        "window": {"from": end - WINDOW, "to": end, "seconds": WINDOW},
        "prometheus": {
            "consumer_lag_series": lag,
            "consumer_lag_slope_per_s": lag_slope,
            "consumer_lag_max": max((v for _, v in lag), default=None),
            "consumer_client_lag_series": client_lag,
            "consumer_client_lag_max": max((v for _, v in client_lag), default=None),
            "events_consumed_per_s": consumed,
            "ingest_accepted_per_s": accepted,
            "event_to_match_p50_s": p50,
            "event_to_match_p99_s": p99,
        },
        "swap": {"longest_swap_in_streak_s": swap_streak, "swap_in_streak_threshold_kb_per_s": SWAP_KB_PER_S,
                 "swap_in_total_kb": swap_kb},
        "machine": {"vmstat_samples": samples, "largest_sample_gap_s": largest_gap, "cores": cores,
                    "window_max_run_queue": queue, "window_min_idle_cpu_pct": idle, "busiest_second": busiest_at,
                    "busy_cores_at_busiest": None if idle is None else round(cores * (100 - idle) / 100, 2),
                    "containers_cores_at_busiest": at_busiest,
                    "generator_cores_step_mean": None if gen.get("cpu_seconds") is None
                    else round(gen["cpu_seconds"] / gen["schedule_seconds"], 3)},
        "containers": per_container,
        "jvm": jvm,
        "verdict": verdict,
    }
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(record, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"step {gen['label']}: offered {gen['offered_rate']}/s, consumed {consumed}/s, lag slope {lag_slope}, "
          f"p50 {p50} s, p99 {p99} s, swap streak {swap_streak} s, largest sample gap {largest_gap} s -> {verdict}")
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv))
