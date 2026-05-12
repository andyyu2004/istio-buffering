#!/usr/bin/env python3
"""Summarize a loop_stats.py JSONL capture as two tables: a phase timeline and a per-pod summary.

Phases are auto-detected from the gateway's downstream_tx counter:
  - "pre"     : before the first byte flows
  - "burst N" : ticks where downstream_tx grows by > burst_min bytes
  - "stall N" : ticks with zero downstream_tx delta after a burst

Usage:
  runs/summarize_stats.py runs/stats_run1.jsonl
  runs/summarize_stats.py runs/stats_run1.jsonl --start 1778627469
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from typing import Optional

# Internal Envoy listener / cluster names that double-count the same byte stream
# (in ambient mode, a single response byte is reported by both the main filter
# chain prefix AND the internal HBONE-encap listener prefix).
INTERNAL_ENVOY_NAMES = {
    "connect_originate", "encap",
    "inner_connect_originate", "outer_connect_originate",
    "main_internal",
}
# Clusters that are administrative/monitoring noise, not user traffic.
INTERNAL_CLUSTERS = INTERNAL_ENVOY_NAMES | {
    "agent", "prometheus_stats", "sds-grpc", "xds-grpc",
}

# Map metric name → which label identifies the filter chain / cluster, and whether
# the value is summable across labels (counters/gauges with disjoint values are
# summable; per-connection gauges with the same byte stream reported in multiple
# prefixes are NOT — those need to drop the internal ones).
ENVOY_METRIC_LABEL = {
    "envoy_cluster_upstream_cx_rx_bytes_total": ("cluster_name", INTERNAL_CLUSTERS),
    "envoy_cluster_upstream_cx_tx_bytes_total": ("cluster_name", INTERNAL_CLUSTERS),
    "envoy_cluster_upstream_cx_rx_bytes_buffered": ("cluster_name", INTERNAL_CLUSTERS),
    "envoy_cluster_upstream_cx_tx_bytes_buffered": ("cluster_name", INTERNAL_CLUSTERS),
    "envoy_cluster_upstream_cx_active": ("cluster_name", INTERNAL_CLUSTERS),
    "envoy_cluster_upstream_flow_control_paused_reading_total": ("cluster_name", INTERNAL_CLUSTERS),
    "envoy_cluster_upstream_flow_control_resumed_reading_total": ("cluster_name", INTERNAL_CLUSTERS),
    "envoy_tcp_downstream_cx_rx_bytes_total": ("tcp_prefix", INTERNAL_ENVOY_NAMES),
    "envoy_tcp_downstream_cx_tx_bytes_total": ("tcp_prefix", INTERNAL_ENVOY_NAMES),
    "envoy_tcp_downstream_cx_active": ("tcp_prefix", INTERNAL_ENVOY_NAMES),
}

AGG_FIELD = {
    "envoy_cluster_upstream_cx_rx_bytes_total": "upstream_rx_total",
    "envoy_cluster_upstream_cx_tx_bytes_total": "upstream_tx_total",
    "envoy_cluster_upstream_cx_rx_bytes_buffered": "upstream_rx_buffered",
    "envoy_cluster_upstream_cx_tx_bytes_buffered": "upstream_tx_buffered",
    "envoy_cluster_upstream_cx_active": "upstream_cx_active",
    "envoy_cluster_upstream_flow_control_paused_reading_total": "upstream_paused_reading",
    "envoy_cluster_upstream_flow_control_resumed_reading_total": "upstream_resumed_reading",
    "envoy_tcp_downstream_cx_rx_bytes_total": "downstream_rx_total",
    "envoy_tcp_downstream_cx_tx_bytes_total": "downstream_tx_total",
    "envoy_tcp_downstream_cx_active": "downstream_cx_active",
}

ZTUNNEL_AGG_FIELD = {
    "istio_tcp_sent_bytes_total": "tcp_sent_bytes",
    "istio_tcp_received_bytes_total": "tcp_received_bytes",
    "istio_tcp_connections_opened_total": "tcp_conns_opened",
    "istio_tcp_connections_closed_total": "tcp_conns_closed",
}


def fmt_bytes(b: float) -> str:
    if b == 0: return "0"
    if abs(b) >= 1024 * 1024 * 1024: return f"{b/1024/1024/1024:.2f} GiB"
    if abs(b) >= 1024 * 1024: return f"{b/1024/1024:.2f} MiB"
    if abs(b) >= 1024: return f"{b/1024:.1f} KiB"
    return f"{b:.0f} B"


def load_agg(path: str, start: Optional[float], end: Optional[float]) -> dict[str, list[dict]]:
    """Build per-tick aggregates from per-metric rows, filtering out internal Envoy
    listeners/clusters so the same byte stream isn't counted twice.

    Falls back to the file's _AGG_ rows if per-metric data isn't present (older
    captures or captures without --per-metric).
    """
    # Group per-metric rows by (pod, ts) so we can recompute aggregates.
    rows_per_tick: dict[tuple[str, float], list[dict]] = defaultdict(list)
    pod_meta: dict[str, dict] = {}
    agg_fallback_per_pod: dict[str, list[dict]] = defaultdict(list)
    saw_per_metric = False

    with open(path) as f:
        for line in f:
            try: r = json.loads(line)
            except json.JSONDecodeError: continue
            ts = r.get("ts")
            if ts is None: continue
            if start is not None and ts < start: continue
            if end is not None and ts > end: continue
            kind = r.get("kind")
            if kind == "metric":
                saw_per_metric = True
                rows_per_tick[(r["pod"], ts)].append(r)
                pod_meta.setdefault(r["pod"], {"ns": r["ns"], "proxy": r["proxy"]})
            elif kind == "_AGG_":
                agg_fallback_per_pod[r["pod"]].append(r)
                pod_meta.setdefault(r["pod"], {"ns": r["ns"], "proxy": r["proxy"]})

    if not saw_per_metric:
        # Old-style capture — trust the AGG rows even though they double-count.
        for pod in agg_fallback_per_pod:
            agg_fallback_per_pod[pod].sort(key=lambda r: r["ts"])
        return dict(agg_fallback_per_pod)

    # Build the tick set per pod from the original _AGG_ rows (which exist for every
    # tick the sampler ran, even when no matching per-metric rows were present —
    # e.g. ztunnel during the pre-transfer window has no istio_tcp_* rows yet).
    # Then recompute aggregates from per-metric rows, filtering internal listeners.
    per_pod: dict[str, list[dict]] = defaultdict(list)
    for pod, agg_rows in agg_fallback_per_pod.items():
        meta = pod_meta[pod]
        for original in agg_rows:
            ts = original["ts"]
            rows = rows_per_tick.get((pod, ts), [])
            agg = {"ts": ts, "ns": meta["ns"], "pod": pod, "kind": "_AGG_", "proxy": meta["proxy"]}
            if meta["proxy"] == "envoy":
                for f_name in AGG_FIELD.values():
                    agg[f_name] = 0.0
                for r in rows:
                    m = r["metric"]
                    if m not in ENVOY_METRIC_LABEL: continue
                    label_key, drop_set = ENVOY_METRIC_LABEL[m]
                    if (r.get("labels", {}).get(label_key) or "") in drop_set:
                        continue
                    agg[AGG_FIELD[m]] += r["value"]
            else:  # ztunnel
                for f_name in ZTUNNEL_AGG_FIELD.values():
                    agg[f_name] = 0.0
                for r in rows:
                    if r["metric"] in ZTUNNEL_AGG_FIELD:
                        agg[ZTUNNEL_AGG_FIELD[r["metric"]]] += r["value"]
            per_pod[pod].append(agg)
    for pod in per_pod:
        per_pod[pod].sort(key=lambda r: r["ts"])
    return dict(per_pod)


def detect_phases(
    ticks: list[dict], key: str, burst_min: float = 100_000
) -> list[tuple[str, float, float]]:
    """Return [(label, start_ts, end_ts), ...] partitioning the timeline."""
    if len(ticks) < 2: return []
    deltas = [(ticks[i]["ts"], (ticks[i].get(key, 0) or 0) - (ticks[i-1].get(key, 0) or 0))
              for i in range(1, len(ticks))]
    phases: list[tuple[str, float, float]] = []
    state = "pre"
    burst_n = 0
    stall_n = 0
    cur = [state, ticks[0]["ts"], ticks[0]["ts"]]
    for ts, dv in deltas:
        if dv >= burst_min:
            new_state = "burst"
        elif state in ("burst", "stall") and dv == 0:
            new_state = "stall"
        elif state == "pre":
            new_state = "pre"
        else:
            # small positive delta between bursts and stalls - treat as continuation of current
            new_state = state
        if new_state != state:
            phases.append(tuple(cur))
            if new_state == "burst": burst_n += 1
            elif new_state == "stall": stall_n += 1
            label = {"pre": "pre", "burst": f"burst {burst_n}", "stall": f"stall {stall_n}"}[new_state]
            cur = [label, ts, ts]
            state = new_state
        else:
            cur[2] = ts
    phases.append(tuple(cur))
    return phases


def delta_in(ticks: list[dict], start_ts: float, end_ts: float, key: str) -> float:
    """Value change during [start_ts, end_ts]. Baseline is the tick just BEFORE start_ts
    so that bursts which happen AT a phase boundary (i.e. between the previous tick and
    the first tick of this phase) are credited to this phase."""
    in_range = [t for t in ticks if start_ts <= t["ts"] <= end_ts]
    if not in_range: return 0
    before = [t for t in ticks if t["ts"] < start_ts]
    baseline = before[-1] if before else in_range[0]
    return (in_range[-1].get(key, 0) or 0) - (baseline.get(key, 0) or 0)


def value_at(ticks: list[dict], ts: float, key: str) -> float:
    """Value at the tick closest to ts (not after)."""
    candidates = [t for t in ticks if t["ts"] <= ts]
    if not candidates: return 0
    return candidates[-1].get(key, 0) or 0


def peak_in(ticks: list[dict], start_ts: float, end_ts: float, key: str) -> float:
    in_range = [t.get(key, 0) or 0 for t in ticks if start_ts <= t["ts"] <= end_ts]
    return max(in_range) if in_range else 0


def peak(ticks: list[dict], key: str) -> float:
    return max((t.get(key, 0) or 0) for t in ticks) if ticks else 0


def total_delta(ticks: list[dict], key: str) -> float:
    if not ticks: return 0
    return (ticks[-1].get(key, 0) or 0) - (ticks[0].get(key, 0) or 0)


def render_table(rows: list[list[str]], header: list[str]) -> str:
    cols = list(zip(*([header] + rows)))
    widths = [max(len(c) for c in col) for col in cols]
    def fmt(row): return "  ".join(c.ljust(w) for c, w in zip(row, widths))
    sep = "  ".join("-" * w for w in widths)
    out = [fmt(header), sep] + [fmt(r) for r in rows]
    return "\n".join(out)


def short_pod(pod: str) -> str:
    if "gateway" in pod: return "gateway"
    if "waypoint" in pod: return "waypoint"
    if "ztunnel" in pod: return "ztunnel"
    return pod[:12]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--start", type=float, default=None)
    ap.add_argument("--end", type=float, default=None)
    ap.add_argument("--burst-min", type=float, default=100_000,
                    help="Minimum bytes/tick delta to classify as a burst (default 100KB)")
    args = ap.parse_args()

    per_pod = load_agg(args.path, args.start, args.end)
    if not per_pod:
        print("no _AGG_ rows in window"); return

    # Identify gateway / waypoint / ztunnel pods
    pods = {short_pod(p): (p, ticks) for p, ticks in per_pod.items()}
    gw = pods.get("gateway")
    wp = pods.get("waypoint")
    zt = pods.get("ztunnel")
    if not gw:
        print("no gateway pod in data"); return

    ts_origin = min(t["ts"] for ticks in per_pod.values() for t in ticks)
    phases = detect_phases(gw[1], "downstream_tx_total", burst_min=args.burst_min)

    def offset(ts): return f"{ts - ts_origin:+.2f}s"

    print(f"time origin: {ts_origin:.2f}  (file: {args.path})")

    # --- Timeline table ---
    print("\n=== Phase timeline (detected from gateway downstream_tx) ===\n")
    header = ["phase", "range", "dur",
              "gw dn_tx", "gw paused (cum)",
              "wp dn_tx", "wp paused (cum)",
              "zt sent", "zt conns"]
    rows = []
    for label, s, e in phases:
        dur = e - s
        if gw:
            gw_dntx = delta_in(gw[1], s, e, "downstream_tx_total")
            gw_paused_delta = delta_in(gw[1], s, e, "upstream_paused_reading")
            gw_paused_cum = value_at(gw[1], e, "upstream_paused_reading")
        else:
            gw_dntx = gw_paused_delta = gw_paused_cum = 0
        if wp:
            wp_dntx = delta_in(wp[1], s, e, "downstream_tx_total")
            wp_paused_delta = delta_in(wp[1], s, e, "upstream_paused_reading")
            wp_paused_cum = value_at(wp[1], e, "upstream_paused_reading")
        else:
            wp_dntx = wp_paused_delta = wp_paused_cum = 0
        if zt:
            zt_sent = delta_in(zt[1], s, e, "tcp_sent_bytes")
            zt_conns = delta_in(zt[1], s, e, "tcp_conns_opened")
        else:
            zt_sent = zt_conns = 0
        rows.append([
            label, f"{offset(s)}..{offset(e)}", f"{dur:.1f}s",
            fmt_bytes(gw_dntx), f"+{int(gw_paused_delta)} ({int(gw_paused_cum)})",
            fmt_bytes(wp_dntx), f"+{int(wp_paused_delta)} ({int(wp_paused_cum)})",
            fmt_bytes(zt_sent), f"+{int(zt_conns)}",
        ])
    # Totals row
    tot_s, tot_e = phases[0][1], phases[-1][2]
    rows.append([
        "TOTAL", f"{offset(tot_s)}..{offset(tot_e)}", f"{tot_e-tot_s:.1f}s",
        fmt_bytes(delta_in(gw[1], tot_s, tot_e, "downstream_tx_total")),
        f"+{int(delta_in(gw[1], tot_s, tot_e, 'upstream_paused_reading'))}",
        fmt_bytes(delta_in(wp[1], tot_s, tot_e, "downstream_tx_total")) if wp else "-",
        f"+{int(delta_in(wp[1], tot_s, tot_e, 'upstream_paused_reading'))}" if wp else "-",
        fmt_bytes(delta_in(zt[1], tot_s, tot_e, "tcp_sent_bytes")) if zt else "-",
        f"+{int(delta_in(zt[1], tot_s, tot_e, 'tcp_conns_opened'))}" if zt else "-",
    ])
    print(render_table(rows, header))

    # --- Per-pod summary ---
    print("\n=== Per-pod summary ===\n")
    header2 = ["pod", "proxy", "peak rx_buf", "peak tx_buf",
               "max conns", "total dn_tx", "initial burst", "total pauses"]
    rows2 = []
    # Find each pod's initial burst (the first "burst N" phase)
    first_burst = next((p for p in phases if p[0].startswith("burst")), None)
    for label_key, (_pod_name, ticks) in [
        ("gateway", gw), *([("waypoint", wp)] if wp else []), *([("ztunnel", zt)] if zt else []),
    ]:
        proxy_kind = ticks[0].get("proxy", "?")
        if proxy_kind == "envoy":
            rx_buf = peak(ticks, "upstream_rx_buffered")
            tx_buf = peak(ticks, "upstream_tx_buffered")
            cx = int(peak(ticks, "upstream_cx_active"))
            dn_tx_total = total_delta(ticks, "downstream_tx_total")
            init_burst = delta_in(ticks, first_burst[1], first_burst[2],
                                  "downstream_tx_total") if first_burst else 0
            pauses = total_delta(ticks, "upstream_paused_reading")
        else:  # ztunnel
            rx_buf = tx_buf = float("nan")
            cx = int(peak(ticks, "tcp_conns_opened"))
            dn_tx_total = total_delta(ticks, "tcp_sent_bytes")
            init_burst = delta_in(ticks, first_burst[1], first_burst[2],
                                  "tcp_sent_bytes") if first_burst else 0
            pauses = float("nan")
        rows2.append([
            label_key, proxy_kind,
            "-" if rx_buf != rx_buf else fmt_bytes(rx_buf),
            "-" if tx_buf != tx_buf else fmt_bytes(tx_buf),
            str(cx),
            fmt_bytes(dn_tx_total),
            fmt_bytes(init_burst),
            "-" if pauses != pauses else f"+{int(pauses)}",
        ])
    print(render_table(rows2, header2))
    print()


if __name__ == "__main__":
    main()
