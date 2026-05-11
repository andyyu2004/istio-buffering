#!/usr/bin/env python3
"""
Automated istio buffering sweep.

For each Config in the matrix:
  1. Apply config (helm upgrade / kubectl label / kubectl apply)
  2. Roll the affected pods
  3. Run curl --limit-rate 125k against the gateway
  4. Read 'bytes emitted by Caddy' from Prometheus over the curl window
  5. Compute: emitted, received, buffered = emitted - received
  6. Write a CSV row to runs/sweep.csv

Wireshark equivalent:
  sum(istio_tcp_sent_bytes_total{reporter="destination",destination_app="caddy-server"})
  delta over the curl run = bytes Caddy emitted = what tcpdump on caddy pod would see.

Run with: python3 runs/sweep.py
Override matrix by editing MATRIX at the bottom of this file.
"""

from __future__ import annotations

import csv
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RUNS = REPO / "runs"
RESULTS_CSV = RUNS / "sweep.csv"
PROM_URL = "http://localhost:9090"
KCTX = "kind-kind"

# Test target. Re-derived if KINDCCM_PORT env var is unset.
GATEWAY_URL_TEMPLATE = "https://127.0.0.1:{port}/testdata.bin"
CURL_RATE = "125k"
CURL_MAX_TIME = 30  # seconds
SETTLE_AFTER_ROLLOUT = 15  # let prometheus scrape after pod restart (>1 scrape interval)
SETTLE_AFTER_CURL = 15     # let prometheus scrape after curl finishes


def sh(cmd: list[str], check: bool = True, capture: bool = False, timeout: int = 300) -> subprocess.CompletedProcess:
    """Run a command. Streams to stdout/stderr unless capture=True."""
    print(f"  $ {' '.join(cmd)}", flush=True)
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture,
        text=True,
        timeout=timeout,
    )


def prom_query(query: str, t: float | None = None, attempts: int = 5) -> float | None:
    """Single-value Prometheus query. Returns None if no series. Retries on connection error."""
    params = {"query": query}
    if t is not None:
        params["time"] = str(t)
    url = f"{PROM_URL}/api/v1/query?" + urllib.parse.urlencode(params)
    last_err: Exception | None = None
    for i in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                body = json.load(resp)
            result = body.get("data", {}).get("result", [])
            if not result:
                return None
            return float(result[0]["value"][1])
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last_err = e
            if i < attempts - 1:
                time.sleep(2)
    raise RuntimeError(f"prom_query failed after {attempts} attempts: {last_err}")


def discover_gateway_port() -> str:
    """Find the host port mapped to kindccm:9999."""
    out = subprocess.check_output(["docker", "ps", "--format", "{{.Names}}"], text=True)
    name = next((n for n in out.splitlines() if "kindccm" in n), None)
    if not name:
        raise RuntimeError("no kindccm container found")
    ports = subprocess.check_output(["docker", "port", name], text=True)
    for line in ports.splitlines():
        if line.startswith("9999/tcp"):
            return line.split(":")[-1].strip()
    raise RuntimeError(f"no 9999/tcp mapping on {name}")


@dataclass
class Config:
    """One row in the sweep matrix."""

    name: str
    # pilot env vars on istiod (0 = unset → Envoy default)
    pilot_hbone_stream_window: int = 0
    pilot_hbone_conn_window: int = 0
    # ztunnel HTTP/2 sizes via proxyMetadata
    ztunnel_stream_window: int = 0  # 0 = unset
    ztunnel_conn_window: int = 0
    ztunnel_frame_size: int = 0
    # listener per_connection_buffer_limit_bytes via EnvoyFilter (0 = no filter)
    per_conn_buf: int = 0
    tcp_notsent_lowat: int = 0
    # route through waypoint or bypass it
    waypoint_in_path: bool = True
    # extra: what to label this run in CSV
    extra_notes: str = ""

    def to_row(self) -> dict[str, str]:
        return {
            "name": self.name,
            "pilot_stream_win": str(self.pilot_hbone_stream_window),
            "pilot_conn_win": str(self.pilot_hbone_conn_window),
            "ztunnel_stream_win": str(self.ztunnel_stream_window),
            "ztunnel_conn_win": str(self.ztunnel_conn_window),
            "ztunnel_frame": str(self.ztunnel_frame_size),
            "per_conn_buf": str(self.per_conn_buf),
            "tcp_notsent_lowat": str(self.tcp_notsent_lowat),
            "waypoint_in_path": str(self.waypoint_in_path),
            "notes": self.extra_notes,
        }


def wait_for_webhook() -> None:
    """Poll istiod's validation webhook until it responds. Called after helm-upgrade istiod."""
    print("  waiting for istiod validation webhook to be ready")
    for i in range(30):
        # Use --dry-run=server to force a webhook validation without actually creating anything.
        r = subprocess.run(
            ["kubectl", "--context", KCTX, "apply", "--dry-run=server", "-f", "-"],
            input="apiVersion: networking.istio.io/v1alpha3\nkind: EnvoyFilter\nmetadata:\n  name: _webhook-probe\n  namespace: istio-system\n",
            capture_output=True, text=True, timeout=15,
        )
        if r.returncode == 0:
            return
        time.sleep(2)
    print("  WARN: webhook still not happy after 60s, proceeding anyway")


def apply_istiod(cfg: Config) -> None:
    """helm upgrade istiod with the relevant pilot env vars."""
    base_args = [
        "helm", "upgrade", "istiod", "istio/istiod", "--version", "1.29.1",
        "-n", "istio-system",
        "--values", str(REPO / "improved" / "istiod-values.yaml"),
        "--set", "image=istiolocal/pilot:latest",
        "--set", "global.hub=istiolocal",
        "--set", "global.tag=latest",
        "--set", "global.imagePullPolicy=IfNotPresent",
        "--kube-context", KCTX,
        "--wait", "--timeout", "3m",
    ]
    if cfg.pilot_hbone_stream_window:
        base_args += ["--set", f"pilot.env.PILOT_HBONE_INITIAL_STREAM_WINDOW_SIZE={cfg.pilot_hbone_stream_window}"]
    if cfg.pilot_hbone_conn_window:
        base_args += ["--set", f"pilot.env.PILOT_HBONE_INITIAL_CONNECTION_WINDOW_SIZE={cfg.pilot_hbone_conn_window}"]
    sh(base_args)


def apply_ztunnel(cfg: Config) -> None:
    """helm upgrade ztunnel with HTTP/2 env vars."""
    sets = []
    if cfg.ztunnel_stream_window:
        sets += ["--set", f"meshConfig.defaultConfig.proxyMetadata.HTTP2_STREAM_WINDOW_SIZE={cfg.ztunnel_stream_window}"]
    if cfg.ztunnel_conn_window:
        sets += ["--set", f"meshConfig.defaultConfig.proxyMetadata.HTTP2_CONNECTION_WINDOW_SIZE={cfg.ztunnel_conn_window}"]
    if cfg.ztunnel_frame_size:
        sets += ["--set", f"meshConfig.defaultConfig.proxyMetadata.HTTP2_FRAME_SIZE={cfg.ztunnel_frame_size}"]
    args = [
        "helm", "upgrade", "ztunnel", "istio/ztunnel", "--version", "1.29.1",
        "-n", "istio-system",
        "--kube-context", KCTX,
        "--wait", "--timeout", "2m",
        *sets,
    ]
    sh(args)


def apply_envoyfilters(cfg: Config) -> None:
    """Apply an EnvoyFilter with per_connection_buffer_limit + TCP_NOTSENT_LOWAT, or delete."""
    if cfg.per_conn_buf == 0 and cfg.tcp_notsent_lowat == 0:
        sh(["kubectl", "--context", KCTX, "delete", "envoyfilter",
            "-n", "istio-gateway", "buffer-limits-final", "--ignore-not-found"])
        sh(["kubectl", "--context", KCTX, "delete", "envoyfilter",
            "-n", "istio-waypoint", "buffer-limits-final", "--ignore-not-found"])
        return

    socket_opts = ""
    if cfg.tcp_notsent_lowat:
        socket_opts = f"""
          socket_options:
            - description: "TCP_NOTSENT_LOWAT={cfg.tcp_notsent_lowat}"
              level: 6
              name: 25
              int_value: {cfg.tcp_notsent_lowat}
              state: STATE_LISTENING
"""

    pcb_line = f"per_connection_buffer_limit_bytes: {cfg.per_conn_buf}" if cfg.per_conn_buf else ""

    yaml_text = f"""
apiVersion: networking.istio.io/v1alpha3
kind: EnvoyFilter
metadata:
  name: buffer-limits-final
  namespace: istio-gateway
spec:
  configPatches:
    - applyTo: LISTENER
      match:
        context: ANY
      patch:
        operation: MERGE
        value:
          {pcb_line}{socket_opts}
    - applyTo: CLUSTER
      match:
        context: ANY
      patch:
        operation: MERGE
        value:
          {pcb_line}
---
apiVersion: networking.istio.io/v1alpha3
kind: EnvoyFilter
metadata:
  name: buffer-limits-final
  namespace: istio-waypoint
spec:
  targetRefs:
    - kind: Gateway
      name: istio-waypoint
      group: gateway.networking.k8s.io
  configPatches:
    - applyTo: LISTENER
      match:
        context: ANY
      patch:
        operation: MERGE
        value:
          {pcb_line}
    - applyTo: CLUSTER
      match:
        context: ANY
      patch:
        operation: MERGE
        value:
          {pcb_line}
"""
    tmp = RUNS / "_envoyfilter.yaml"
    tmp.write_text(yaml_text)
    # Retry: istiod's validation webhook can be slow right after a helm upgrade.
    last_err: Exception | None = None
    for i in range(6):
        try:
            sh(["kubectl", "--context", KCTX, "apply", "-f", str(tmp)])
            return
        except subprocess.CalledProcessError as e:
            last_err = e
            wait = 10
            print(f"  envoyfilter apply failed (attempt {i+1}/6), retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"envoyfilter apply gave up: {last_err}")


def set_waypoint_path(cfg: Config) -> None:
    """Toggle the ingress-use-waypoint label on caddy-service."""
    if cfg.waypoint_in_path:
        sh(["kubectl", "--context", KCTX, "label", "svc", "-n", "caddy", "caddy-service",
            "istio.io/ingress-use-waypoint=true", "--overwrite"])
    else:
        sh(["kubectl", "--context", KCTX, "label", "svc", "-n", "caddy", "caddy-service",
            "istio.io/ingress-use-waypoint-"], check=False)


def reset_pods() -> None:
    """rollout-restart data plane. NB: we deliberately don't restart prometheus
    here — restarting it kills any port-forward the caller has open (e.g.
    istioctl dashboard prometheus). The CSV is keyed off point-in-time queries
    so accumulated counters across configs are fine."""
    sh(["kubectl", "--context", KCTX, "rollout", "restart", "-n", "istio-system", "daemonset/ztunnel"])
    sh(["kubectl", "--context", KCTX, "rollout", "restart", "-n", "istio-gateway", "deployment/istio-gateway-istio"])
    sh(["kubectl", "--context", KCTX, "rollout", "restart", "-n", "istio-waypoint", "deployment/istio-waypoint"])
    sh(["kubectl", "--context", KCTX, "rollout", "status", "-n", "istio-gateway", "deployment/istio-gateway-istio", "--timeout=120s"])
    sh(["kubectl", "--context", KCTX, "rollout", "status", "-n", "istio-waypoint", "deployment/istio-waypoint", "--timeout=120s"])
    sh(["kubectl", "--context", KCTX, "rollout", "status", "-n", "istio-system", "daemonset/ztunnel", "--timeout=120s"])


def current_pods() -> dict[str, str]:
    """Resolve the current pod names of ztunnel/gateway/waypoint so we can label-filter."""
    def first(ns: str, label: str) -> str:
        r = subprocess.run(
            ["kubectl", "--context", KCTX, "get", "pod", "-n", ns, "-l", label,
             "-o", "jsonpath={.items[0].metadata.name}"],
            capture_output=True, text=True, check=True, timeout=15,
        )
        return r.stdout.strip()
    return {
        "ztunnel": first("istio-system", "app=ztunnel"),
        "gateway": first("istio-gateway", "service.istio.io/canonical-name=istio-gateway-istio"),
        "waypoint": first("istio-waypoint", "service.istio.io/canonical-name=istio-waypoint"),
    }


def run_curl(port: str) -> tuple[int, int, int]:
    """Run curl. Returns (t0, t1, bytes_received)."""
    url = GATEWAY_URL_TEMPLATE.format(port=port)
    t0 = int(time.time())
    print(f"  curl start @ {t0}")
    r = subprocess.run(
        [
            "curl", "-k", url, "-o", "/dev/null",
            "--limit-rate", CURL_RATE,
            "--http1.1",
            "--max-time", str(CURL_MAX_TIME),
            "-s",
            "-w", "%{size_download}",
        ],
        capture_output=True, text=True, timeout=CURL_MAX_TIME + 10,
    )
    t1 = int(time.time())
    received = int(r.stdout.strip() or 0)
    print(f"  curl done @ {t1} (received {received} bytes)")
    return t0, t1, received


def measure(t0: int, t1: int, pods: dict[str, str]) -> dict[str, float | None]:
    """Query Prometheus for run metrics.

    We point-in-time at t0 and t1 filtered to the *current* pods (by name) so
    that stale series from previous configs' pods don't contaminate the delta.
    """
    def value_at(query: str, t: int) -> float | None:
        url = f"{PROM_URL}/api/v1/query?" + urllib.parse.urlencode({
            "query": query, "time": str(t),
        })
        last_err: Exception | None = None
        for i in range(5):
            try:
                with urllib.request.urlopen(url, timeout=10) as resp:
                    body = json.load(resp)
                break
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                last_err = e
                if i < 4:
                    time.sleep(2)
        else:
            raise RuntimeError(f"query failed: {last_err}")
        result = body.get("data", {}).get("result", [])
        if not result:
            return None
        return float(result[0]["value"][1])

    def delta(metric_with_pod_filter: str, t0_: int, t1_: int) -> float | None:
        q = f'sum({metric_with_pod_filter})'
        v0 = value_at(q, t0_) or 0.0  # fresh pod → no series yet → 0
        v1 = value_at(q, t1_)
        return (v1 - v0) if v1 is not None else None

    ztunnel = pods["ztunnel"]
    gateway = pods["gateway"]

    # bytes Caddy emitted (per the destination-side ztunnel). Filter by ztunnel pod name.
    emitted = delta(
        f'istio_tcp_sent_bytes_total{{reporter="destination",destination_app="caddy-server",pod="{ztunnel}"}}',
        t0, t1,
    )
    first_burst = delta(
        f'istio_tcp_sent_bytes_total{{reporter="destination",destination_app="caddy-server",pod="{ztunnel}"}}',
        t0, t0 + 10,
    )
    # bytes gateway envoy received over HBONE from upstream
    gw_hbone_rx = delta(
        f'envoy_cluster_upstream_cx_rx_bytes_total{{cluster_name="connect_originate",pod="{gateway}"}}',
        t0, t1,
    )

    # peak memory deltas (over whole run window)
    def peak_max(query: str) -> float | None:
        url = f"{PROM_URL}/api/v1/query_range?" + urllib.parse.urlencode({
            "query": query, "start": str(t0 - 5), "end": str(t1 + 10), "step": "2",
        })
        last_err: Exception | None = None
        for i in range(5):
            try:
                with urllib.request.urlopen(url, timeout=10) as resp:
                    body = json.load(resp)
                break
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                last_err = e
                if i < 4:
                    time.sleep(2)
        else:
            raise RuntimeError(f"peak_max failed: {last_err}")
        result = body.get("data", {}).get("result", [])
        if not result:
            return None
        max_v = 0.0
        for series in result:
            for _ts, v in series["values"]:
                try:
                    fv = float(v)
                    if fv > max_v:
                        max_v = fv
                except ValueError:
                    pass
        return max_v

    gw_mem_peak = peak_max('envoy_server_memory_allocated{namespace="istio-gateway"}')
    wp_mem_peak = peak_max('envoy_server_memory_allocated{namespace="istio-waypoint"}')
    ztunnel_mem_peak = peak_max('container_memory_working_set_bytes{namespace="istio-system",pod=~"ztunnel-.*",container="istio-proxy"}')
    h2_pending_peak = peak_max('envoy_http2_pending_send_bytes')

    return {
        "emitted": emitted,
        "first_burst_10s": first_burst,
        "gw_hbone_rx": gw_hbone_rx,
        "gw_mem_peak": gw_mem_peak,
        "wp_mem_peak": wp_mem_peak,
        "ztunnel_mem_peak": ztunnel_mem_peak,
        "h2_pending_peak": h2_pending_peak,
    }


def fmt_bytes(v: float | None) -> str:
    if v is None:
        return "?"
    if v >= 1024 * 1024:
        return f"{v / 1024 / 1024:.2f} MB"
    if v >= 1024:
        return f"{v / 1024:.1f} KB"
    return f"{int(v)} B"


def run_one(cfg: Config, port: str) -> dict:
    print(f"\n=== {cfg.name} ===")
    apply_istiod(cfg)
    apply_ztunnel(cfg)
    set_waypoint_path(cfg)
    apply_envoyfilters(cfg)
    reset_pods()
    print(f"  settling {SETTLE_AFTER_ROLLOUT}s")
    time.sleep(SETTLE_AFTER_ROLLOUT)

    pods = current_pods()
    print(f"  pods: {pods}")
    t0, t1, received = run_curl(port)
    print(f"  settling {SETTLE_AFTER_CURL}s for prometheus scrape")
    time.sleep(SETTLE_AFTER_CURL)
    m = measure(t0, t1, pods)
    buffered = (m["emitted"] - received) if m["emitted"] is not None else None

    row = cfg.to_row()
    row.update({
        "t0": t0, "t1": t1,
        "received": received,
        "emitted": m["emitted"],
        "first_burst_10s": m["first_burst_10s"],
        "buffered_at_end": buffered,
        "gw_hbone_rx": m["gw_hbone_rx"],
        "gw_mem_peak": m["gw_mem_peak"],
        "wp_mem_peak": m["wp_mem_peak"],
        "ztunnel_mem_peak": m["ztunnel_mem_peak"],
        "h2_pending_peak": m["h2_pending_peak"],
    })

    print(f"  received        = {fmt_bytes(received)}")
    print(f"  emitted (total) = {fmt_bytes(m['emitted'])}")
    print(f"  first-burst 10s = {fmt_bytes(m['first_burst_10s'])}  <-- caddy-side")
    print(f"  gw_hbone_rx     = {fmt_bytes(m['gw_hbone_rx'])}  <-- gateway recv via HBONE")
    print(f"  buffered at end = {fmt_bytes(buffered)}")
    print(f"  gw_mem_peak     = {fmt_bytes(m['gw_mem_peak'])}")
    print(f"  wp_mem_peak     = {fmt_bytes(m['wp_mem_peak'])}")
    print(f"  ztunnel_peak    = {fmt_bytes(m['ztunnel_mem_peak'])}")
    print(f"  h2_pending_peak = {fmt_bytes(m['h2_pending_peak'])}")
    return row


def write_row(row: dict, header_needed: bool) -> None:
    RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_CSV.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if header_needed:
            writer.writeheader()
        writer.writerow(row)


# ---------------------------- matrix ----------------------------

K = 1024

def all_improvements(**overrides) -> dict:
    """Convenience: every improvement at the README-recommended values, then apply overrides."""
    base = dict(
        pilot_hbone_stream_window=64 * K - 1,
        pilot_hbone_conn_window=256 * K - 4,
        ztunnel_stream_window=64 * K - 1,
        ztunnel_conn_window=256 * K - 4,
        ztunnel_frame_size=16 * K,
        per_conn_buf=32 * K,
        tcp_notsent_lowat=16 * K,
        waypoint_in_path=True,
    )
    base.update(overrides)
    return base


MATRIX: list[Config] = [
    # ---------- baseline & full-stack reference ----------
    Config(name="00_baseline", waypoint_in_path=True),
    Config(name="01_all_improvements", **all_improvements()),

    # ---------- per-improvement isolation (waypoint in path) ----------
    Config(
        name="02_only_pilot_envvars",
        pilot_hbone_stream_window=64 * K - 1,
        pilot_hbone_conn_window=256 * K - 4,
        waypoint_in_path=True,
    ),
    Config(
        name="03_only_ztunnel_envvars",
        ztunnel_stream_window=64 * K - 1,
        ztunnel_conn_window=256 * K - 4,
        ztunnel_frame_size=16 * K,
        waypoint_in_path=True,
    ),
    Config(
        name="04_only_envoyfilters",
        per_conn_buf=32 * K,
        tcp_notsent_lowat=16 * K,
        waypoint_in_path=True,
    ),

    # ---------- PILOT_HBONE window sweep (all other improvements on, waypoint in path) ----------
    # Envoy proto-validation floor: InitialStreamWindowSize ∈ [65535, 2^31-1].
    # Conn window = 4 × stream window.
    Config(name="07_pilot_win_64k",   **all_improvements(pilot_hbone_stream_window=64 * K - 1, pilot_hbone_conn_window=256 * K - 4)),
    Config(name="08_pilot_win_128k",  **all_improvements(pilot_hbone_stream_window=128 * K - 1, pilot_hbone_conn_window=512 * K - 4)),
    Config(name="09_pilot_win_256k",  **all_improvements(pilot_hbone_stream_window=256 * K - 1, pilot_hbone_conn_window=1024 * K - 4)),
    Config(name="10_pilot_win_1M",    **all_improvements(pilot_hbone_stream_window=1024 * K - 1, pilot_hbone_conn_window=4 * 1024 * K - 4)),
    Config(name="11_pilot_win_4M",    **all_improvements(pilot_hbone_stream_window=4 * 1024 * K - 1, pilot_hbone_conn_window=16 * 1024 * K - 4)),
    Config(name="12_pilot_win_16M",   **all_improvements(pilot_hbone_stream_window=16 * 1024 * K - 1, pilot_hbone_conn_window=64 * 1024 * K - 4)),
]


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--list":
        for c in MATRIX:
            print(c.name)
        return 0

    # Quick sanity: prometheus reachable.
    try:
        prom_query("up")
    except Exception as e:
        print(f"prometheus at {PROM_URL} unreachable: {e}", file=sys.stderr)
        print("hint: istioctl dashboard prometheus", file=sys.stderr)
        return 1

    port = discover_gateway_port()
    print(f"gateway port: {port}")

    # Resume: skip configs already in the CSV (by name).
    done_names: set[str] = set()
    if RESULTS_CSV.exists():
        with RESULTS_CSV.open() as f:
            for r in csv.DictReader(f):
                if r.get("name"):
                    done_names.add(r["name"])
    if done_names:
        print(f"resuming, skipping already-completed: {sorted(done_names)}")
    header_needed = not RESULTS_CSV.exists()
    for cfg in MATRIX:
        if cfg.name in done_names:
            print(f"\n=== {cfg.name} === (skip, already done)")
            continue
        try:
            row = run_one(cfg, port)
            write_row(row, header_needed)
            header_needed = False
        except Exception as e:
            print(f"  FAILED: {e}", file=sys.stderr)

    print(f"\ndone. results: {RESULTS_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
