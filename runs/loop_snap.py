#!/usr/bin/env python3
"""Periodically sample TCP queue depths across caddy/ztunnel/gateway/waypoint and append JSONL.

Each line in the output file is one connection observed at one tick:
  {"ts": 1715600000.123, "ns": "caddy", "pod": "...", "state": "ESTAB",
   "local": "10.0.0.1:8080", "rem": "10.0.0.2:54321", "tx_queue": 1234, "rx_queue": 0}

Plus one aggregate line per pod per tick with state="_AGG_" containing tx_sum/rx_sum/conn_count.

Analyze later with e.g.
  jq -c 'select(.state=="_AGG_")' samples.jsonl
  duckdb -c "select pod, max(ts)-min(ts), max(tx_queue) from read_json_auto('samples.jsonl') group by pod"
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

KCTX = "kind-kind"


def sh(cmd: list[str], check: bool = True, timeout: int = 30) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True, check=check, timeout=timeout)
    return r.stdout


def pod(ns: str, label: str) -> str:
    return sh(["kubectl", "--context", KCTX, "get", "pod", "-n", ns, "-l", label,
               "-o", "jsonpath={.items[0].metadata.name}"]).strip()


_STATES = {
    "01": "ESTAB", "02": "SYN_SENT", "03": "SYN_RECV", "04": "FIN_W1",
    "05": "FIN_W2", "06": "TIME_W", "07": "CLOSE", "08": "CLOSE_WAIT",
    "09": "LAST_ACK", "0A": "LISTEN", "0B": "CLOSING",
}


def fmt_addr(hex_addr: str) -> str:
    ip_hex, port_hex = hex_addr.split(":")
    # IPv4-mapped IPv6 (last 8 hex chars) or plain IPv4 (8 hex chars).
    ip_hex = ip_hex[-8:] if len(ip_hex) > 8 else ip_hex
    ip = ".".join(str(int(ip_hex[i : i + 2], 16)) for i in (6, 4, 2, 0))
    return f"{ip}:{int(port_hex, 16)}"


def parse_tcp_line(line: str) -> Optional[dict]:
    parts = line.split()
    if len(parts) < 5:
        return None
    try:
        tx_hex, rx_hex = parts[4].split(":")
        return {
            "local": fmt_addr(parts[1]),
            "rem": fmt_addr(parts[2]),
            "state": _STATES.get(parts[3].upper(), parts[3]),
            "tx_queue": int(tx_hex, 16),
            "rx_queue": int(rx_hex, 16),
        }
    except (ValueError, IndexError):
        return None


def proc_net_tcp(ns: str, pod_name: str, is_distroless: bool) -> list[dict]:
    if is_distroless:
        # kubectl debug is slow + creates ephemeral containers each call; skip it for loop mode.
        # Instead, exec into ztunnel/istio-proxy via the istio-proxy container which has sh+cat.
        out = sh(["kubectl", "--context", KCTX, "exec", "-n", ns, pod_name,
                  "-c", "istio-proxy" if "ztunnel" not in pod_name else "istio-proxy",
                  "--", "sh", "-c", "cat /proc/net/tcp /proc/net/tcp6 2>/dev/null"],
                 check=False)
    else:
        out = sh(["kubectl", "--context", KCTX, "exec", "-n", ns, pod_name,
                  "--", "sh", "-c", "cat /proc/net/tcp /proc/net/tcp6 2>/dev/null"],
                 check=False)
    conns = []
    for line in out.splitlines():
        c = parse_tcp_line(line)
        if c and c["state"] in ("ESTAB", "CLOSE_WAIT", "FIN_W1", "FIN_W2"):
            conns.append(c)
    return conns


def sample_pod(ns: str, pod_name: str, is_distroless: bool) -> list[dict]:
    try:
        return proc_net_tcp(ns, pod_name, is_distroless)
    except subprocess.SubprocessError as e:
        print(f"!! sample failed for {ns}/{pod_name}: {e}", file=sys.stderr)
        return []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--output", default="samples.jsonl", help="JSONL output path")
    ap.add_argument("-i", "--interval", type=float, default=1.0, help="Seconds between samples")
    ap.add_argument("-n", "--count", type=int, default=0, help="Number of ticks (0 = until Ctrl-C)")
    ap.add_argument("--port-filter", type=int, default=0,
                    help="If set, only record connections involving this port")
    args = ap.parse_args()

    targets = [
        ("caddy", pod("caddy", "app=caddy-server"), False),
        ("istio-system", pod("istio-system", "app=ztunnel"), True),
        ("istio-gateway", pod("istio-gateway", "service.istio.io/canonical-name=istio-gateway-istio"), True),
        ("istio-waypoint", pod("istio-waypoint", "service.istio.io/canonical-name=istio-waypoint"), True),
    ]
    print(f"sampling pods: {[(ns, p) for ns, p, _ in targets]}", file=sys.stderr)
    print(f"writing to {args.output} every {args.interval}s", file=sys.stderr)

    f = open(args.output, "a", buffering=1)  # line-buffered
    tick = 0
    try:
        while True:
            tick += 1
            ts = time.time()
            # Fan out kubectl exec calls in parallel so a slow pod doesn't blow our cadence.
            with ThreadPoolExecutor(max_workers=len(targets)) as ex:
                futs = {ex.submit(sample_pod, ns, p, d): (ns, p) for ns, p, d in targets}
                for fut in as_completed(futs):
                    ns, p = futs[fut]
                    conns = fut.result()
                    if args.port_filter:
                        ps = f":{args.port_filter}"
                        conns = [c for c in conns if c["local"].endswith(ps) or c["rem"].endswith(ps)]
                    tx_sum = sum(c["tx_queue"] for c in conns)
                    rx_sum = sum(c["rx_queue"] for c in conns)
                    f.write(json.dumps({
                        "ts": ts, "ns": ns, "pod": p, "state": "_AGG_",
                        "tx_sum": tx_sum, "rx_sum": rx_sum, "conn_count": len(conns),
                    }) + "\n")
                    for c in conns:
                        f.write(json.dumps({"ts": ts, "ns": ns, "pod": p, **c}) + "\n")
            print(f"tick {tick} ts={ts:.2f}", file=sys.stderr)
            if args.count and tick >= args.count:
                break
            # Drift-correct sleep
            sleep_for = args.interval - (time.time() - ts)
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        print(f"\nstopped after {tick} ticks", file=sys.stderr)
    finally:
        f.close()


if __name__ == "__main__":
    main()
