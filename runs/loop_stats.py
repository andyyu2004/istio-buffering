#!/usr/bin/env python3
"""Periodically scrape Envoy + ztunnel Prometheus stats from gateway/waypoint/ztunnel
and dump JSONL for later analysis.

This is the userspace-aware companion to loop_snap.py. Where loop_snap.py reads
/proc/net/tcp (which can't see Envoy userspace buffers), this script reads the
proxies' own admin metrics — where bytes_buffered, flow_control_paused_reading_total,
and bytes_total live.

Output: one JSONL line per (tick, pod, metric, labels) → value. Plus an "_AGG_"
summary line per pod per tick with the key totals (rx/tx bytes, buffered, paused).

Analyze later with e.g.
  jq -c 'select(.kind=="_AGG_")' stats.jsonl
  duckdb -c "select pod, ts, rx_total, tx_total, rx_buffered, paused
             from read_json_auto('stats.jsonl') where kind='_AGG_' order by pod, ts"
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

KCTX = "kind-kind"

# Prometheus text-format line: metric_name{labels} value [timestamp]
# Allow optional labels and integer/float values (with optional exponent).
PROM_RE = re.compile(
    r'^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)'
    r'(?:\{(?P<labels>[^}]*)\})?'
    r'\s+(?P<value>[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?|NaN|\+Inf|-Inf)'
)
LABEL_RE = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')

# Metric names we care about (regex). Tracked separately for "envoy" pods and ztunnel.
ENVOY_KEEP = re.compile(
    r'^envoy_('
    r'cluster_upstream_cx_(rx|tx)_bytes_(total|buffered)|'
    r'cluster_upstream_cx_active|'
    r'cluster_upstream_flow_control_(paused|resumed)_reading_total|'
    r'cluster_upstream_flow_control_(backed_up|drained)_total|'
    r'tcp_downstream_cx_(rx|tx)_bytes_total|'
    r'tcp_downstream_cx_active|'
    r'http_connect_terminate_downstream_cx_(rx|tx)_bytes_total|'
    r'http_connect_terminate_downstream_flow_control_(paused|resumed)_reading_total|'
    r'http_downstream_cx_(rx|tx)_bytes_total'
    r')$'
)
ZTUNNEL_KEEP = re.compile(
    r'^istio_tcp_(sent_bytes_total|received_bytes_total|connections_opened_total|connections_closed_total)$'
)
# Ztunnel labels we keep — discard the boilerplate region/zone/principal noise.
ZT_LABELS_KEEP = {
    "reporter", "source_workload", "destination_workload",
    "destination_service", "request_protocol",
}


def sh(cmd: list[str], check: bool = True, timeout: int = 30) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True, check=check, timeout=timeout)
    return r.stdout


def pod(ns: str, label: str) -> str:
    return sh(["kubectl", "--context", KCTX, "get", "pod", "-n", ns, "-l", label,
               "-o", "jsonpath={.items[0].metadata.name}"]).strip()


def fetch_prom(ns: str, pod_name: str, port: int, path: str) -> str:
    return sh(["kubectl", "--context", KCTX, "get", "--raw",
               f"/api/v1/namespaces/{ns}/pods/{pod_name}:{port}/proxy{path}"],
              check=False)


def parse_labels(s: str) -> dict:
    return {k: v for k, v in LABEL_RE.findall(s)}


def parse_prom(text: str, keep: re.Pattern, label_filter: Optional[set] = None) -> list[dict]:
    rows: list[dict] = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = PROM_RE.match(line)
        if not m:
            continue
        name = m.group("name")
        if not keep.match(name):
            continue
        labels = parse_labels(m.group("labels") or "")
        if label_filter:
            labels = {k: v for k, v in labels.items() if k in label_filter}
        try:
            val = float(m.group("value"))
        except ValueError:
            continue
        rows.append({"metric": name, "labels": labels, "value": val})
    return rows


def aggregate_envoy(rows: list[dict]) -> dict:
    """Sum key counters across clusters/listeners to get one number per pod per tick."""
    agg = {
        "upstream_rx_total": 0.0, "upstream_tx_total": 0.0,
        "upstream_rx_buffered": 0.0, "upstream_tx_buffered": 0.0,
        "upstream_cx_active": 0.0,
        "upstream_paused_reading": 0.0, "upstream_resumed_reading": 0.0,
        "downstream_rx_total": 0.0, "downstream_tx_total": 0.0,
        "downstream_cx_active": 0.0,
        "http_connect_terminate_rx_total": 0.0,
        "http_connect_terminate_tx_total": 0.0,
        "http_connect_terminate_paused_reading": 0.0,
        "http_connect_terminate_resumed_reading": 0.0,
    }
    for r in rows:
        n, v = r["metric"], r["value"]
        if n == "envoy_cluster_upstream_cx_rx_bytes_total": agg["upstream_rx_total"] += v
        elif n == "envoy_cluster_upstream_cx_tx_bytes_total": agg["upstream_tx_total"] += v
        elif n == "envoy_cluster_upstream_cx_rx_bytes_buffered": agg["upstream_rx_buffered"] += v
        elif n == "envoy_cluster_upstream_cx_tx_bytes_buffered": agg["upstream_tx_buffered"] += v
        elif n == "envoy_cluster_upstream_cx_active": agg["upstream_cx_active"] += v
        elif n == "envoy_cluster_upstream_flow_control_paused_reading_total": agg["upstream_paused_reading"] += v
        elif n == "envoy_cluster_upstream_flow_control_resumed_reading_total": agg["upstream_resumed_reading"] += v
        elif n == "envoy_tcp_downstream_cx_rx_bytes_total": agg["downstream_rx_total"] += v
        elif n == "envoy_tcp_downstream_cx_tx_bytes_total": agg["downstream_tx_total"] += v
        elif n == "envoy_tcp_downstream_cx_active": agg["downstream_cx_active"] += v
        elif n == "envoy_http_connect_terminate_downstream_cx_rx_bytes_total": agg["http_connect_terminate_rx_total"] += v
        elif n == "envoy_http_connect_terminate_downstream_cx_tx_bytes_total": agg["http_connect_terminate_tx_total"] += v
        elif n == "envoy_http_connect_terminate_downstream_flow_control_paused_reading_total": agg["http_connect_terminate_paused_reading"] += v
        elif n == "envoy_http_connect_terminate_downstream_flow_control_resumed_reading_total": agg["http_connect_terminate_resumed_reading"] += v
    return agg


def aggregate_ztunnel(rows: list[dict]) -> dict:
    agg = {"tcp_sent_bytes": 0.0, "tcp_received_bytes": 0.0,
           "tcp_conns_opened": 0.0, "tcp_conns_closed": 0.0}
    for r in rows:
        n, v = r["metric"], r["value"]
        if n == "istio_tcp_sent_bytes_total": agg["tcp_sent_bytes"] += v
        elif n == "istio_tcp_received_bytes_total": agg["tcp_received_bytes"] += v
        elif n == "istio_tcp_connections_opened_total": agg["tcp_conns_opened"] += v
        elif n == "istio_tcp_connections_closed_total": agg["tcp_conns_closed"] += v
    return agg


def sample_envoy(ns: str, pod_name: str) -> tuple[list[dict], dict]:
    text = fetch_prom(ns, pod_name, 15090, "/stats/prometheus")
    rows = parse_prom(text, ENVOY_KEEP)
    return rows, aggregate_envoy(rows)


def sample_ztunnel(ns: str, pod_name: str) -> tuple[list[dict], dict]:
    text = fetch_prom(ns, pod_name, 15020, "/metrics")
    rows = parse_prom(text, ZTUNNEL_KEEP, label_filter=ZT_LABELS_KEEP)
    return rows, aggregate_ztunnel(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--output", default="stats.jsonl")
    ap.add_argument("-i", "--interval", type=float, default=1.0)
    ap.add_argument("-n", "--count", type=int, default=0, help="0 = until Ctrl-C")
    ap.add_argument("--per-metric", action="store_true",
                    help="Also emit one line per (metric, labels) — verbose but enables drill-down")
    args = ap.parse_args()

    targets = [
        ("istio-gateway", pod("istio-gateway", "service.istio.io/canonical-name=istio-gateway-istio"), "envoy"),
        ("istio-waypoint", pod("istio-waypoint", "service.istio.io/canonical-name=istio-waypoint"), "envoy"),
        ("istio-system", pod("istio-system", "app=ztunnel"), "ztunnel"),
    ]
    print(f"sampling: {[(ns, p, kind) for ns, p, kind in targets]}", file=sys.stderr)
    print(f"writing to {args.output} every {args.interval}s", file=sys.stderr)

    f = open(args.output, "a", buffering=1)
    tick = 0
    try:
        while True:
            tick += 1
            ts = time.time()
            with ThreadPoolExecutor(max_workers=len(targets)) as ex:
                def go(ns, p, kind):
                    try:
                        return (ns, p, kind, *(sample_envoy(ns, p) if kind == "envoy" else sample_ztunnel(ns, p)))
                    except subprocess.SubprocessError as e:
                        return (ns, p, kind, [], {"_error": str(e)[:200]})
                futs = [ex.submit(go, ns, p, kind) for ns, p, kind in targets]
                for fut in as_completed(futs):
                    ns, p, kind, rows, agg = fut.result()
                    f.write(json.dumps({
                        "ts": ts, "ns": ns, "pod": p, "kind": "_AGG_", "proxy": kind, **agg,
                    }) + "\n")
                    if args.per_metric:
                        for r in rows:
                            f.write(json.dumps({"ts": ts, "ns": ns, "pod": p, "kind": "metric",
                                                 "proxy": kind, **r}) + "\n")
            print(f"tick {tick} ts={ts:.2f}", file=sys.stderr)
            if args.count and tick >= args.count:
                break
            sleep_for = args.interval - (time.time() - ts)
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        print(f"\nstopped after {tick} ticks", file=sys.stderr)
    finally:
        f.close()


if __name__ == "__main__":
    main()
