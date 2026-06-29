# Ambient HBONE head-of-line blocking — investigation report

## TL;DR
- A few **slow clients can starve fast traffic** sharing the same HBONE path (head-of-line blocking). Reproduced on a real reef: a fast request went from ~0.17 s to **timeout/seconds** while slow clients ran.
- **Mechanism:** HBONE multiplexes streams over a shared HTTP/2 connection whose **connection-level flow-control window** gets exhausted. `max_concurrent_streams = 100`.
- **Fix (config):** set the HBONE **connection window ≥ 100 × stream window** (ratio ≥ `max_concurrent_streams`) → provably immune. Our tuned `64K/8M` (ratio 128) is safe; the **stock defaults `16M/24M` (ratio 1.5) are vulnerable.**
- **Detection:** aggregate proxy *metrics* can't see it. The working signals are **request latency of a known-fast endpoint** — either an active **canary** (Better Uptime on a status endpoint) or a **route-scoped `istio_request_duration` p99**.

## The problem
Path: `client → gateway(Envoy) → [HBONE] → waypoint(Envoy) → [HBONE] → ztunnel → backend`. Many logical connections are multiplexed as HTTP/2 streams over a shared HBONE connection. A slow client (e.g. 1 Mbps downloading a large response) causes data to buffer and consumes the shared connection's flow-control window; once exhausted, *other* streams on that connection stall — including fast, healthy ones.

## Mechanism & the fix
- The shared HBONE connection caps at **`max_concurrent_streams = 100`**. If the **connection window** can hold a full **stream window** for all 100 possible streams (i.e. `connection ≥ 100 × stream`), no stream can be starved → **immune**.
- Window-size sweep on the real cluster (single fast stream throughput, alone vs under 40 concurrent slow clients), windows set on both istiod `PILOT_HBONE_INITIAL_{STREAM,CONNECTION}_WINDOW_SIZE` and ztunnel `HTTP2_{STREAM,CONNECTION}_WINDOW_SIZE`:

| stream | conn | ratio | fast stream under load | |
|---|---|---|---|---|
| 64K | 512K | 8× | ~0 MB/s | ❌ collapse |
| 256K | 8M | 32× | full | ✅ |
| 64K | 8M | **128×** | full | ✅ (current prod-tuned) |
| **16M** | **24M** | **1.5× (stock defaults)** | **34.7 → ~0 MB/s** | ❌ **collapse** |

- **It's the *ratio*, not absolute sizes** (same 2M conn window: ratio 32 holds, ratio 8/2 collapse). Stream window only sets single-stream throughput; the connection:stream ratio governs HOL. **Threshold = `max_concurrent_streams` (100).**
- **Recommendation:** `connection window ≥ 100 × stream window`. Keep the **stream window small** so immunity is cheap (64K → 6.4M; 256K → 25.6M). `64K/8M` is a good sweet spot. (`PILOT_HBONE_MAX_CONCURRENT_STREAMS=1` = full per-tunnel isolation, the cleanest fix, needs a patched pilot — inert on stock 1.29.3.)

## Detection — what works and what doesn't

**❌ Aggregate proxy metrics do NOT detect it.** Exhaustively checked gateway + waypoint (full Envoy stat dumps) + ztunnel — under a controlled HOL-on vs HOL-off test (fast stream `0.1` vs `35` MB/s) the metrics were **identical or anti-correlated**:
- `flow_control_paused_reading` is a *transition counter* — it's *higher when healthy* (more pause/resume cycles); a fully-blocked stream pauses once and goes quiet.
- buffered-bytes gauges, `tx_flush_timeout`, stream/connection counts, ztunnel byte/socket counters: all dominated by the slow clients (present in both states). The starved victim is one stream among ~40 and invisible.
- Root cause: "a stream waiting for flow-control window" is a **normal, uncounted** state; Envoy exposes buffer *size* but never *residence time*; ztunnel only emits coarse L4 counters.

**✅ Request latency of a known-fast endpoint detects it.** `istio_request_duration_milliseconds` p99, validated on the real instance (`andy-test-instance2`):

| state | `request_duration` p99 |
|---|---|
| healthy, no load | 28 ms |
| healthy, **+40 slow clients** | 39 ms (no false alarm) |
| **HOL** (ratio 8, 40 slow) | **2780 ms** (~70×) |

The tail (p99) carries the signal; p50 stays flat (~170 ms). It does **not** false-alarm on mere load.

**⚠️ But aggregate `request_duration` is contaminated by legitimately long requests.** `request_duration` = time to *response complete*, so **slow downloads, websockets, SSE, gRPC streams** record huge durations (minutes–hours) and would bury the signal / cause false alarms — the original "don't flag normal slow connections" problem. **The metric is only clean when scoped to traffic that should always be fast.**

## Recommended detection
Watch the **latency of a known-fast endpoint** (e.g. `/api/serverstatus/hosted/external`), one of:
1. **Active canary** — Better Uptime (or similar) probing the status endpoint, **per instance**, every 10–30 s, alert on **p90/max** (cross-tenant HOL is intermittent — a single sample can miss it; tail/any-spike-over-window catches it). Immune to download/websocket contamination because it only hits the fast endpoint. Works for **any** backend incl. non-HTTP/passthrough.
2. **Route-scoped `istio_request_duration` p99** — add a low-cardinality custom dimension matching the fast route (CEL: `probe_route = request.url_path == "/api/serverstatus/hosted/external" ? "status" : "other"`), then alert on `histogram_quantile(0.99, sum by(le,destination_service_name)(rate(istio_request_duration_milliseconds_bucket{probe_route="status"}[5m]))) > ~1s`. Passive, no prober — but **HTTP-only** (no L4/passthrough) and needs the custom dimension validated.

**Metric cost:** Istio request metrics were disabled (cardinality). `istio_request_duration` is ~**160 series/instance** with 42 default labels; mesh-wide across many instances + the other histograms (`request_bytes`, `response_bytes`) → 100K+ series. Enable **selectively**: gateway-scoped Telemetry `selector`, **only `REQUEST_DURATION`**, drop high-cardinality labels (`source_*`, `*_principal`, `*_canonical_*`, `connection_security_policy`).

## Route-scoped metric — validated config (the recommended detector)

Two pieces (validated 2026-06: `probe_route` correctly split status/other, other metrics stayed off):

**1. Declare the tag** — `istiod-values.yaml` `meshConfig.defaultConfig`:
```yaml
meshConfig:
  defaultConfig:
    extraStatTags:
    - probe_route
```

**2. Gateway-scoped Telemetry** (mesh-wide metrics stay disabled for cost; gateway re-enables only request-duration with the route dimension):
```yaml
apiVersion: telemetry.istio.io/v1
kind: Telemetry
metadata: { name: hol-request-duration, namespace: istio-gateway }
spec:
  selector:
    matchLabels: { gateway.networking.k8s.io/gateway-name: istio-gateway }
  metrics:
  - providers: [{ name: prometheus }]
    overrides:
    - match: { metric: ALL_METRICS }            # keep everything else off (cost)
      disabled: true
    - match: { metric: REQUEST_DURATION, mode: CLIENT }
      disabled: false
      tagOverrides:
        probe_route:
          value: "request.url_path.startsWith('/api/serverstatus') ? 'status' : 'other'"
        source_workload: { operation: REMOVE }
        source_principal: { operation: REMOVE }
        connection_security_policy: { operation: REMOVE }
```

**Alert:** `histogram_quantile(0.99, sum by(le,destination_service_name)(rate(istio_request_duration_milliseconds_bucket{probe_route="status"}[5m]))) > ~1000` (healthy ≈ 30 ms; HOL ≈ 2.8 s). Probe the `status` path frequently so the histogram has data (a light synthetic heartbeat, or rely on real health-check traffic).

## Open items
- Prototype + validate the **route-scoped custom dimension** (custom dimensions can be fiddly in Istio 1.29).
- Decide canary vs route-scoped metric (canary covers non-HTTP backends too).
- Persist the chosen window config (`HboneStreamWindowSize`/`HboneConnectionWindowSize` params already added to `Deploy-IstioGateway.ps1`) and the metric-enable.

## Reproduction / artifacts
- Window sweep + HOL repro scripts and the caddy/mesh-probe test backends are in `improved/` (`caddy-reef.yaml`, `mesh-probe.yaml`, `window-tuning-results.md`).
- Knobs: `PILOT_HBONE_INITIAL_{STREAM,CONNECTION}_WINDOW_SIZE` (istiod), `HTTP2_{STREAM,CONNECTION}_WINDOW_SIZE` (ztunnel), set together.
- Methodology note: full-reset proxies **including ztunnel** between configs and gate on a path-health probe — buffer state latches and contaminates later measurements if you don't.
