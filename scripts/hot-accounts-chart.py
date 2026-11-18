#!/usr/bin/env python3
"""Draws docs/results/hot-accounts.svg from hot-accounts.csv (DESIGN §7.5, §17): two panels, committed transactions
per second and p99 commit latency, against the writer count, one line per balance strategy.

    python3 scripts/hot-accounts-chart.py hot-accounts.csv hot-accounts.svg

Plain SVG, no plotting library, deterministic output. Light and dark text both read on the white background.
"""
import csv
import math
import sys

rows = list(csv.DictReader(open(sys.argv[1], encoding="utf-8")))
series = {}
for r in rows:
    series.setdefault(r["strategy"], []).append(r)
writers = sorted({int(r["writers"]) for r in rows})
COLORS = {"ROW_LOCK": "#c0392b", "APPEND_SNAPSHOT": "#2471a3"}
W, H, PAD_L, PAD_R, PAD_T, PAD_B, GAP = 980, 420, 70, 20, 50, 80, 70
panel_w = (W - PAD_L - PAD_R - GAP) / 2
plot_h = H - PAD_T - PAD_B


def nice_max(v):
    if v <= 0:
        return 1
    e = 10 ** math.floor(math.log10(v))
    for m in (1, 2, 2.5, 5, 10):
        if v <= m * e:
            return m * e
    return 10 * e


def panel(x0, title, key, unit):
    out = []
    top = nice_max(max(float(r[key]) for r in rows) * 1.1)
    xs = {w: x0 + (i / max(1, len(writers) - 1)) * panel_w for i, w in enumerate(writers)}
    y = lambda v: PAD_T + plot_h - (v / top) * plot_h
    out.append(f'<text x="{x0 + panel_w / 2:.1f}" y="{PAD_T - 20}" text-anchor="middle" font-size="15" '
               f'font-weight="600">{title}</text>')
    for i in range(5):
        v = top * i / 4
        yy = y(v)
        out.append(f'<line x1="{x0:.1f}" y1="{yy:.1f}" x2="{x0 + panel_w:.1f}" y2="{yy:.1f}" stroke="#e5e5e5"/>')
        label = f"{v:,.0f}" if top >= 10 else f"{v:.1f}"
        out.append(f'<text x="{x0 - 8:.1f}" y="{yy + 4:.1f}" text-anchor="end" font-size="11" fill="#555">{label}</text>')
    for w, xx in xs.items():
        out.append(f'<text x="{xx:.1f}" y="{PAD_T + plot_h + 18}" text-anchor="middle" font-size="11" '
                   f'fill="#555">{w}</text>')
    out.append(f'<text x="{x0 + panel_w / 2:.1f}" y="{PAD_T + plot_h + 38}" text-anchor="middle" font-size="12" '
               f'fill="#333">concurrent writers on one merchant</text>')
    out.append(f'<text x="{x0 - 52:.1f}" y="{PAD_T + plot_h / 2:.1f}" text-anchor="middle" font-size="12" fill="#333" '
               f'transform="rotate(-90 {x0 - 52:.1f} {PAD_T + plot_h / 2:.1f})">{unit}</text>')
    for name, rs in sorted(series.items()):
        pts = sorted((int(r["writers"]), float(r[key])) for r in rs)
        path = " ".join(f"{'M' if i == 0 else 'L'}{xs[w]:.1f},{y(v):.1f}" for i, (w, v) in enumerate(pts))
        out.append(f'<path d="{path}" fill="none" stroke="{COLORS.get(name, "#333")}" stroke-width="2.5"/>')
        for w, v in pts:
            out.append(f'<circle cx="{xs[w]:.1f}" cy="{y(v):.1f}" r="3.5" fill="{COLORS.get(name, "#333")}"/>')
    return out


parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
         f'font-family="Helvetica, Arial, sans-serif">', f'<rect width="{W}" height="{H}" fill="#ffffff"/>']
parts += panel(PAD_L, "Committed ledger transactions per second", "tps", "transactions / s")
parts += panel(PAD_L + panel_w + GAP, "p99 commit latency", "commit_p99_ms", "milliseconds")
lx = W - PAD_R - 330
for i, name in enumerate(sorted(series)):
    parts.append(f'<rect x="{lx + i * 170}" y="{H - 18}" width="14" height="4" fill="{COLORS.get(name, "#333")}"/>')
    parts.append(f'<text x="{lx + i * 170 + 20}" y="{H - 13}" font-size="12" fill="#333">{name}</text>')
parts.append("</svg>")
with open(sys.argv[2], "w", encoding="utf-8", newline="\n") as f:
    f.write("\n".join(parts) + "\n")
