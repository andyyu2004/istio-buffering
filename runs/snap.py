#!/usr/bin/env python3
"""Snapshot TCP socket queue depths and cgroup memory across caddy/ztunnel/gateway/waypoint.

Use to inspect where bytes are buffered during a slow-client transfer.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections import defaultdict
from typing import Optional

KCTX = "kind-kind"


def sh(cmd: list[str], stdin: Optional[str] = None, check: bool = True, timeout: int = 30) -> str:
    r = subprocess.run(cmd, input=stdin, capture_output=True, text=True, check=check, timeout=timeout)
    return r.stdout


def pod(ns: str, label: str) -> str:
    return sh(["kubectl", "--context", KCTX, "get", "pod", "-n", ns, "-l", label,
               "-o", "jsonpath={.items[0].metadata.name}"]).strip()


def parse_tcp_line(line: str) -> Optional[dict]:
    """Parse a row of /proc/net/tcp."""
    parts = line.split()
    if len(parts) < 5:
        return None
    try:
        local = parts[1]
        rem = parts[2]
        st = parts[3]
        queues = parts[4]  # "txq:rxq" hex
        tx_hex, rx_hex = queues.split(":")
        tx = int(tx_hex, 16)
        rx = int(rx_hex, 16)
    except (ValueError, IndexError):
        return None
    return {
        "local": fmt_addr(local),
        "rem": fmt_addr(rem),
        "state": tcp_state(st),
        "tx_queue": tx,
        "rx_queue": rx,
    }


def fmt_addr(hex_addr: str) -> str:
    ip_hex, port_hex = hex_addr.split(":")
    ip = ".".join(str(int(ip_hex[i : i + 2], 16)) for i in (6, 4, 2, 0))
    return f"{ip}:{int(port_hex, 16)}"


_STATES = {
    "01": "ESTAB", "02": "SYN_SENT", "03": "SYN_RECV", "04": "FIN_W1",
    "05": "FIN_W2", "06": "TIME_W", "07": "CLOSE", "08": "CLOSE_WAIT",
    "09": "LAST_ACK", "0A": "LISTEN", "0B": "CLOSING",
}


def tcp_state(s: str) -> str:
    return _STATES.get(s.upper(), s)


def proc_net_tcp(ns: str, pod_name: str, is_distroless: bool = False) -> list[dict]:
    """Get /proc/net/tcp contents from a pod. Use debug container for distroless."""
    if is_distroless:
        out = sh(
            ["kubectl", "--context", KCTX, "debug", "-n", ns, pod_name,
             "--image=busybox", "--profile=general", "--quiet", "--attach=true",
             "--", "sh", "-c", "cat /proc/net/tcp /proc/net/tcp6 2>/dev/null"],
            timeout=60,
        )
    else:
        out = sh(["kubectl", "--context", KCTX, "exec", "-n", ns, pod_name,
                  "--", "sh", "-c", "cat /proc/net/tcp /proc/net/tcp6 2>/dev/null"])
    conns = []
    for line in out.splitlines():
        c = parse_tcp_line(line)
        if c and c["state"] in ("ESTAB", "CLOSE_WAIT", "FIN_W1", "FIN_W2", "TIME_W"):
            conns.append(c)
    return conns


def pod_memory(ns: str, pod_name: str) -> int:
    """Container memory.usage_in_bytes from the pod's cgroup. Returns sum across containers."""
    out = sh(
        ["kubectl", "--context", KCTX, "top", "pod", "-n", ns, pod_name,
         "--containers", "--no-headers"],
        check=False,
    )
    total = 0
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 4:
            mem_str = parts[3]  # e.g. "12Mi"
            try:
                if mem_str.endswith("Mi"):
                    total += int(mem_str[:-2]) * 1024 * 1024
                elif mem_str.endswith("Gi"):
                    total += int(mem_str[:-2]) * 1024 * 1024 * 1024
                elif mem_str.endswith("Ki"):
                    total += int(mem_str[:-2]) * 1024
            except ValueError:
                pass
    return total


def fmt_bytes(b: int) -> str:
    if b >= 1024 * 1024:
        return f"{b/1024/1024:.2f} MiB"
    if b >= 1024:
        return f"{b/1024:.1f} KiB"
    return f"{b} B"


def snapshot(label: str) -> None:
    caddy = pod("caddy", "app=caddy-server")
    gateway = pod("istio-gateway", "service.istio.io/canonical-name=istio-gateway-istio")
    waypoint = pod("istio-waypoint", "service.istio.io/canonical-name=istio-waypoint")
    ztunnel = pod("istio-system", "app=ztunnel")

    print(f"\n========== {label} ==========")
    for ns, p, is_distroless in (
        ("caddy", caddy, False),
        ("istio-system", ztunnel, True),
        ("istio-gateway", gateway, True),
        ("istio-waypoint", waypoint, True),
    ):
        print(f"\n--- {ns}/{p} ---")
        try:
            conns = proc_net_tcp(ns, p, is_distroless)
        except subprocess.CalledProcessError as e:
            print(f"  exec failed: {e.stderr[:200] if e.stderr else e}")
            continue
        # Show connections with any queue activity, plus all ESTAB on data ports.
        interesting = [c for c in conns if c["tx_queue"] > 0 or c["rx_queue"] > 0]
        if not interesting:
            interesting = conns[:5]
        tx_sum = sum(c["tx_queue"] for c in conns)
        rx_sum = sum(c["rx_queue"] for c in conns)
        print(f"  total ESTAB tx_queue={fmt_bytes(tx_sum)}  rx_queue={fmt_bytes(rx_sum)}  conns={len(conns)}")
        for c in sorted(interesting, key=lambda x: -(x["tx_queue"] + x["rx_queue"]))[:6]:
            print(f"    {c['state']:6s}  {c['local']:>22s} -> {c['rem']:<22s}  tx={fmt_bytes(c['tx_queue']):>10s}  rx={fmt_bytes(c['rx_queue']):>10s}")
        try:
            mem = pod_memory(ns, p)
            print(f"  pod memory (cgroup): {fmt_bytes(mem)}")
        except subprocess.CalledProcessError:
            pass


if __name__ == "__main__":
    label = sys.argv[1] if len(sys.argv) > 1 else "snapshot"
    snapshot(label)
