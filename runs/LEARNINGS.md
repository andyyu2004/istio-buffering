# Buffering Investigation — Learnings (2026-05-13)

## Goal

Localize where bytes pile up in the path `caddy → ztunnel → waypoint → gateway → client` when a slow consumer reads downstream, and find practical levers to reduce in-flight buffering.

## TL;DR

| Finding | Detail |
|---|---|
| Total buffer layers in this stack | **8+ distinct ones** between caddy and a client running on the host |
| Envoy userspace buffers when capped | tiny (~32 KiB peak gateway, ~76 KiB waypoint) — not the problem |
| Dominant buffer | kernel socket buffers (auto-tune up to ~6 MB at each hop), plus any user-space intermediates (docker-proxy, kubectl, toxiproxy) |
| `curl --limit-rate` as a slow consumer | inadequate — post-read sleep with large kernel absorb |
| `toxiproxy bandwidth` toxic | mediocre — has its own ~12 MiB internal buffer |
| Workable slow-client recipe | in-cluster pod + `SO_RCVBUF=4096` + Go app-level read-rate throttle |
| Biggest realization | Most "slow client" tools simulate slowness with a fast consumer + their own buffer in front of it. That doesn't expose real TCP rwnd backpressure. |

---

## Approach evolution (what could see what)

| Approach | Could see | Could NOT see | Verdict |
|---|---|---|---|
| `/proc/net/tcp` via `snap.py` / `loop_snap.py` | kernel tx_queue / rx_queue | Envoy / ztunnel userspace buffers | Useful for kernel-side queues only — completely missed Envoy buffering |
| Envoy `/stats/prometheus` via `loop_stats.py` | userspace bufs, pause events, byte totals per cluster / listener | kernel sockets | Primary signal for proxies |
| `ss -temn` inside pod (via `kubectl debug --image=nicolaka/netshoot`) | live kernel buffers (`tb`, `rb`), `Send-Q`, timers, skmem | per-connection cumulative byte counts (need ss -i for that) | Best for verifying socket options actually got applied |

---

## The double-counting trap (ambient mode)

Each user-visible byte at gateway and waypoint is recorded by **two** filter-chain prefixes in `envoy_tcp_downstream_cx_tx_bytes_total`:

| Prefix | Role |
|---|---|
| `outbound\|9999\|\|caddy-service` (gateway) / `inbound-vip\|9999\|tcp\|caddy-service` (waypoint) | The main TCP filter chain |
| `connect_originate`, `main_internal`, `encap`, `inner_connect_originate`, `outer_connect_originate` | Internal listener for HBONE encap |

Summing across these inflates by ~2x. **Always drop the internal-listener names** when aggregating. Also drop admin clusters: `agent`, `prometheus_stats`, `sds-grpc`, `xds-grpc`. `runs/summarize_stats.py` does this.

**Real impact:** the "30 MiB initial burst" we worried about was actually **~14 MiB** (gateway → client). The other half was the same byte stream double-counted through the HBONE encap chain.

---

## All buffer layers along the path (with measured sizes)

| # | Layer | Where it lives | Default | After fixes | Lever |
|---|---|---|---|---|---|
| 1 | Caddy app buffer | caddy pod userspace | varies | not measured | Caddy config |
| 2 | Caddy → ztunnel kernel TCP | caddy pod | auto-tune ~6 MB | not measured | node sysctl |
| 3 | Ztunnel internal buffer | ztunnel pod userspace | unknown — no metric | unknown | (no config exposed by ztunnel) |
| 4 | Ztunnel → waypoint kernel TCP | between pods | auto-tune ~6 MB | not measured | node sysctl |
| 5 | Waypoint upstream Envoy buffer | waypoint userspace | 1 MiB | reduced to **1111 B** | `per_connection_buffer_limit_bytes` |
| 6 | Waypoint kernel TCP (both sides) | waypoint pod | auto-tune ~6 MB | not measured | sysctl / Envoy `socket_options` |
| 7 | Waypoint → gateway kernel TCP | between pods | auto-tune ~6 MB | not measured | sysctl |
| 8 | Gateway upstream Envoy buffer | gateway userspace | 1 MiB | **32 KiB** (peak observed: 32 KiB) | `per_connection_buffer_limit_bytes` |
| 9 | Gateway kernel SNDBUF (→ client) | gateway pod | auto-tune ~6 MB | **128 KiB** observed `tb131072` | `SO_SNDBUF` via Envoy `socket_options` |
| 10 | Docker port-forward proxy | Docker Desktop's Linux VM | ~6 MB | ~6 MB | **avoid entirely — run client in-cluster** |
| 11 | `kubectl port-forward` | kubectl process | similar | similar | same — avoid for buffer tests |
| 12 | Toxiproxy internal buffer | toxiproxy container | ~12 MB observed | ~12 MB | use only for bandwidth/latency shaping, NOT slow-client simulation |
| 13 | Client kernel RCVBUF | client OS | auto-tune ~6 MB | tested down to 8 KiB | `SO_RCVBUF` — only on real network paths, **macOS loopback ignores it** |
| 14 | Client app buffer (TLS, bufio, etc.) | client process | KBs | KBs | client code |

---

## Findings: before/after per-config change

### Capture details
Slow client: `curl --limit-rate 125k --http1.1` on `testdata.bin` (1 GiB random data from Caddy). 0.5s sample interval via `runs/loop_stats.py`, ~20s captures.

### Comparison table (corrected — no double-counting)

| Run | Config delta | gw initial burst | gw paused | wp paused | gw peak rx_buf | wp peak rx_buf |
|---|---|---|---|---|---|---|
| A — baseline (32K everywhere) | starting point | 14.10 MiB | +242 | +436 | 32 KiB | 76 KiB |
| B — added 1111 B waypoint cluster limit | aggressive waypoint clamp | 9.94 MiB | +152 | **+1** | 32 KiB | 77 KiB |
| C — added SO_SNDBUF=64K + SO_RCVBUF=64K on gateway | kernel-side clamp | misleading (curl artifact) | — | — | 32 KiB | 7 KiB |
| D — same config, toxiproxy 125 KB/s slow consumer | better slow simulation | 12.25 MiB | +165 | +19 | 32 KiB | 7 KiB |

Observations:
- Envoy userspace caps work cleanly: actual peak buffered = exactly the configured limit.
- Aggressive waypoint clamp (1111 B) shifted the bottleneck — waypoint stops piling up, so `paused_reading` events crash to ~0 (counter-intuitive: the chain just runs slower overall).
- The "30 MiB → 10 MiB → 12 MiB" initial-burst journey was partly real and partly an artifact of which slow-client simulation we used.

---

## Slow-client simulation hierarchy

| Tool | What it does | Where the buffer hides | Useful for |
|---|---|---|---|
| `curl --limit-rate` | reads at full speed, sleeps to maintain rate | client OS kernel recv buffer (multi-MB) | functional tests, NOT buffering tests |
| `toxiproxy` bandwidth toxic | reads upstream fast into internal buffer, drains to client at rate | toxiproxy's internal Go channel (~12 MB) | bandwidth + latency shaping when you don't care about backpressure |
| Go client + small `SO_RCVBUF` only | reads fast from socket; small RCVBUF | works in theory; sub-ms RTT in docker means even 8K window allows 100+ MB/s | requires latency injection too |
| Go client + small `SO_RCVBUF` + app-level rate throttle | reads slowly with small kernel buf | almost none | ✅ THE RIGHT THING — paces app reads, kernel buf stays near-zero, rwnd shrinks |
| Go client run on macOS host | same as above, but loopback ignores SO_RCVBUF | macOS loopback | doesn't work on Mac |
| Go client in docker container talking to host port | same, but docker-proxy hop has ~6 MB buffer | docker-proxy in Docker Desktop VM | the wireshark trap — gateway sees fast consumer + 15 MB buffer |
| **Go client as in-cluster pod talking to gateway service** | same, direct TCP to gateway | none significant | ✅ THE GOLD STANDARD — use this |

---

## STATE_LISTENING vs STATE_PREBIND for Envoy `socket_options`

| State | Applies when | Use for |
|---|---|---|
| STATE_PREBIND | right after `socket()`, before bind/connect | most buffer options (SO_SNDBUF, SO_RCVBUF) — works for listeners AND clusters |
| STATE_BOUND | after `bind()` | rare |
| STATE_LISTENING | after `listen()` | options that only make sense on listening sockets (SO_REUSEPORT, TCP_FASTOPEN) |

For SO_SNDBUF on listeners, **both PREBIND and LISTENING work** because Linux inherits the option to accepted children at `accept()` time. PREBIND is more idiomatic and unambiguous. For clusters (outbound connections, no `listen()`), **must be PREBIND** — STATE_LISTENING wouldn't apply.

---

## Verifying live socket buffer sizes

`istioctl proxy-config listener -oyaml` shows what Envoy *configured*, not what the kernel actually applied. To verify the live socket:

```bash
GW_POD=$(kubectl get pod -n istio-gateway -l service.istio.io/canonical-name=istio-gateway-istio -o jsonpath='{.items[0].metadata.name}')
kubectl debug -n istio-gateway "$GW_POD" -it=false --image=nicolaka/netshoot --profile=netadmin --attach=true --quiet \
  -- sh -c 'ss -temn state established "( sport = :9999 or dport = :9999 )"'
```

In `skmem:(r0,rb<X>,t0,tb<Y>,...)`, `rb` is `SO_RCVBUF`, `tb` is `SO_SNDBUF`. Linux **doubles** what you set via `setsockopt`, so requested 64K → reported 131072.

---

## Concrete Envoy levers (used in `improved/envoyfilters.yaml`)

| Lever | Where | Value used | Effect |
|---|---|---|---|
| `per_connection_buffer_limit_bytes` | gateway listener + clusters | 32768 | Caps Envoy userspace per-conn buf to 32 KiB |
| `per_connection_buffer_limit_bytes` | waypoint listeners | 32768 | Same on waypoint |
| `per_connection_buffer_limit_bytes` | waypoint clusters | **1111** | Aggressive clamp — eliminated waypoint pauses |
| `TCP_NOTSENT_LOWAT` (level=6, name=25) | gateway listener | 16384 | Caps unsent kernel data; Envoy gets EAGAIN sooner |
| `SO_SNDBUF` (level=1, name=7) | gateway listener | 65536 → kernel 128K | Caps in-flight bytes per accepted connection toward client |
| `SO_RCVBUF` (level=1, name=8) | gateway clusters (via `upstream_bind_config`) | 65536 → kernel 128K | Caps incoming buf from upstream waypoint side. **Caution**: too small can break HBONE flow if smaller than the HTTP/2 connection window (`initial_connection_window_size: 262140`) |
| HTTP/2 `initial_stream_window_size` | waypoint HCM | 65535 | Per-stream window |
| HTTP/2 `initial_connection_window_size` | waypoint HCM | 262140 | Per-connection window |

---

## Tooling built today

| File | Purpose |
|---|---|
| `runs/loop_stats.py` | Periodic JSONL capture of Envoy `/stats/prometheus` + ztunnel `/metrics` |
| `runs/summarize_stats.py` | Phase-detected timeline + per-pod summary table (filters internal listeners; recomputes from per-metric rows) |
| `runs/slow_client.go` | Go HTTPS client with `SO_RCVBUF` + app-level rate throttle |
| `runs/slow-client.Dockerfile` | Builds slow_client into a small image |
| `runs/slow-client-incluster.sh` | Build + `kind load` + run as Job in cluster (gold-standard slow client) |
| `runs/slow-client.sh` | Run slow client as host-side docker (suffers docker-proxy buffer) |
| `runs/toxiproxy-setup.sh` | Toxiproxy with bandwidth (and optional latency) toxic — useful for latency injection, NOT pure slow-client tests |

Existing tools kept for reference:
- `runs/snap.py` — single-shot `/proc/net/tcp` snapshot (kernel queues only)
- `runs/loop_snap.py` — looping variant (mostly superseded by `loop_stats.py`)

---

## Open questions / next steps

1. **Re-run with the in-cluster slow client** + capture metrics + wireshark. Initial burst should now be on the order of `SO_RCVBUF` × few, not MiBs. Validate kernel-level backpressure all the way through the chain.
2. **Apply matching SO_SNDBUF/SO_RCVBUF clamps on waypoint** if more reduction is needed (currently only gateway has them).
3. **Investigate ztunnel buffer**: Rust impl, no Envoy-style metric. Possible probes: cgroup memory growth (`kubectl top`), or instrument ztunnel directly.
4. **Node sysctls**: clamping `net.ipv4.tcp_wmem max` / `tcp_rmem max` at the kind node level would lower the auto-tune ceiling for *all* TCP sockets — coarse but effective.
5. **HBONE / HTTP-2 window sizes**: currently 64 KiB stream / 256 KiB connection on waypoint. Worth tuning down to match SO_RCVBUF.
6. **"15 MiB in Wireshark" mystery**: was almost certainly the docker-proxy intermediate buffer when using the host-side slow client. Re-test with in-cluster pod should resolve.
