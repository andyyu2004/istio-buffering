# Detecting a stuck / head-of-line-blocked mesh (PromQL)

Metric-correlation detection — no synthetic canary. Goal: fire when a shared resource
(an Envoy watermark buffer, a worker event loop, ztunnel's HBONE mux, a connection pool)
stalls and holds up *many* connections at once, **without** alerting on normal slow,
long-running connections (streaming downloads).

**Principle:** never key on the duration or latency of a single connection. Alert on
**loss of forward progress across the population** AND a **shared-resource saturation**
signal. A lone slow connection keeps aggregate throughput healthy and doesn't light up
backpressure/watchdog; a stuck mesh does both at once.

Prereqs (both satisfied by `Deploy-IstioGateway.ps1`):
- `meshConfig.defaultConfig.proxyStatsMatcher` exposes the `envoy_*` stats below on the
  gateway + waypoint.
- ztunnel always emits `istio_tcp_*` regardless of the Telemetry metrics toggle.

> Metric-name caveat: `envoy_*` names depend on Envoy's tag extraction. After deploying,
> confirm the exact names with:
> `curl -s localhost:9090/api/v1/label/__name__/values | tr ',' '\n' | grep -E 'flow_control|watchdog|rq_pending'`
> and adjust below if a listener/cluster prefix leaked into the name instead of a label.

---

## Building blocks

```promql
# Active TCP connections to a destination (opened − closed), reporter pinned (no double-count)
sum by (destination_service_name) (
    istio_tcp_connections_opened_total{reporter="destination"}
  - istio_tcp_connections_closed_total{reporter="destination"}
)

# TX byte rate to a destination
sum by (destination_service_name) (
  rate(istio_tcp_sent_bytes_total{reporter="destination"}[1m])
)

# Forward-progress ratio: bytes per active connection per second.
# Near zero while connections are elevated = open but not moving = stuck.
# A slow stream still trickles bytes, so this stays well above the floor for benign traffic.
  sum(rate(istio_tcp_sent_bytes_total{reporter="destination"}[1m]))
/ clamp_min(sum(istio_tcp_connections_opened_total{reporter="destination"}
            - istio_tcp_connections_closed_total{reporter="destination"}), 1)
```

```promql
# Backpressure: Envoy only pauses reading when a watermark buffer is full.
sum(rate(envoy_tcp_downstream_flow_control_paused_reading_total[1m]))
+ sum(rate(envoy_cluster_upstream_flow_control_paused_reading_total[1m]))

# Stuck worker event loop (aggregate guard-dog counter across threads).
increase(envoy_server_watchdog_miss[2m])      # > 0 means a loop didn't tick in time
increase(envoy_server_watchdog_mega_miss[2m]) # worse: a much longer stall

# Connection-pool / queue starvation (more relevant once any L7 is terminated).
sum(envoy_cluster_upstream_rq_pending_active)
sum(rate(envoy_cluster_upstream_cx_overflow[1m]))
```

---

## Composite detector (the alert)

Fire only when forward progress has collapsed **and** a shared resource is saturated. The
AND across independent signals is what keeps a single slow connection from tripping it.

```promql
(
  # 1. Forward progress collapsed: connections elevated AND bytes-per-conn at the floor
  ( sum(istio_tcp_connections_opened_total{reporter="destination"}
        - istio_tcp_connections_closed_total{reporter="destination"}) > CONN_BASELINE )
  and
  ( sum(rate(istio_tcp_sent_bytes_total{reporter="destination"}[1m]))
    / clamp_min(sum(istio_tcp_connections_opened_total{reporter="destination"}
                - istio_tcp_connections_closed_total{reporter="destination"}), 1) < BYTES_FLOOR )
)
and
(
  # 2. Corroborating shared-resource saturation (any one)
     sum(rate(envoy_tcp_downstream_flow_control_paused_reading_total[1m])) > BP_RATE
  or increase(envoy_server_watchdog_miss[2m]) > 0
  or sum(envoy_cluster_upstream_rq_pending_active) > 0
)
```

Evaluate over a short window and require persistence (`for: 1m–2m`) to ignore momentary blips.

### Diagnosis queries (run when the composite fires, to localise the bottleneck)
- `envoy_server_watchdog_miss` rising → CPU-bound / blocked worker on gateway or waypoint.
- `*_flow_control_paused_reading_total` rising → backpressure; check which side (downstream
  vs upstream cluster) to see whether the slow reader is the client or the backend.
- `envoy_cluster_upstream_rq_pending_active` / `upstream_cx_overflow` → pool exhaustion.
- ztunnel CPU saturated with the byte rate floored → ztunnel HBONE mux is the choke point.
- Access logs (`otel` provider) `response_flags`: `UO` upstream overflow, `UF` connect fail,
  `DC` downstream disconnect, `DPE` protocol error.

---

## Setting the thresholds (baseline first — do not ship guesses)

`CONN_BASELINE`, `BYTES_FLOOR`, `BP_RATE` are environment-specific.

1. Run **normal traffic plus deliberate slow/long-running connections** (a throttled
   download). Record the steady-state range of the three building-block expressions.
2. Set `BYTES_FLOOR` below the *lowest* healthy bytes-per-conn seen with slow connections
   present (so they never trip it), and `CONN_BASELINE` at the top of the normal active-conn
   range. `BP_RATE` slightly above the normal flow-control pause rate (often ~0).
3. **Confirm no alert fires** in that run — this is the key false-positive test.
4. Inject a real stall (pause the backend mid-stream, pin ztunnel/Envoy CPU, or shrink the
   connection window so buffers fill) and confirm the composite fires.
5. Re-run step 1 after tuning to confirm slow-but-healthy traffic stays quiet.

---

## Empirical validation (andyreef1aks3, caddy repro)

A controlled HOL test on the real reef (caddy backend via `:9999` → waypoint → HBONE):
3 slow clients at **50 KB/s each (~150 KB/s combined)** ran concurrently with fast 10 MB
fetches on the same path.

| | Fast 10 MB fetch |
|---|---|
| No contention | 0.7–1.5 s (~15 MB/s) |
| With 3 slow clients | **3 of 5 failed** (timeout/000), rest crawled at 90–280 KB/s |

Waypoint memory rose 15 → 190 MB. So HOL is real here, and three findings reshape the
detection above:

1. **Aggregate throughput did NOT collapse** — ztunnel kept ~6 MB/s flowing (slow trickle +
   partial fast transfers). A detector keyed on an *absolute* throughput/bytes-per-conn floor
   MISSES this. Use a **relative drop** (per-active-conn throughput fell ~25×: 15 → 0.6 MB/s)
   baselined against the rolling norm, not a fixed floor.
2. **Connection accumulation is a strong pure-metric signal**: active conns
   (`opened - closed`) rise while `rate(istio_tcp_connections_closed_total)` falls — fast
   clients connect and hang instead of completing.
3. **A canary is the cleanest detector for this pattern.** The fast probe went 0.7 s → timeout
   while metrics stayed ambiguous. Measuring "a known-fast request is now slow" captures HOL
   directly; the metric signals corroborate. (Reversing the earlier no-canary decision is
   worth considering — this is the pattern it was designed for.)
4. The HBONE flow-control corroboration (`connect_originate` `flow_control_paused_reading`)
   was **not observable** — the production `proxyStatsMatcher` only includes `cluster.xds-grpc`,
   trimming all data-path cluster stats. Widen the matcher (see `Deploy-IstioGateway.ps1`) to
   light it up.
