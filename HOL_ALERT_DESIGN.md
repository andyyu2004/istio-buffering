# Mesh HOL / Stuck-Request Alerting — Design & Validation

Goal: alerts on Istio ambient mesh stalls with minimal false positives / false negatives,
that would **clearly have caught the pickitoo/milltech incident (CLOUDPT-11388)** at the time.

Source: istio-gateway access logs in SumoLogic (prod org `octopuscloudprod.de.sumologic.com`,
same `api.de.sumologic.com` endpoint). Cluster identity is in `_sourceCategory =
hosted/<reef>/<cluster>/istio`.

## Key insight: there are TWO failure modes, needing TWO detectors

| | Mesh-wide HBONE HOL | Per-instance streaming stall |
|---|---|---|
| Cause | connection-window exhaustion on the shared `connect_originate` pool (slow clients) | server-side buffering blocked by a slow consumer (e.g. polling tentacle); CLOUDPT-11388 |
| Symptom | *everything* on the pool slows, incl. tiny health checks | long/streaming responses hang; short requests unaffected |
| Health-check latency? | rises (detector A catches) | **stays flat** (detector A blind) |
| Fix | HBONE window ratio >=100 (mesh) | `Polling Tentacle Buffering Workaround` portal flag (per instance) |
| Validated | andyreef1 repro: cluster p99 47ms->12826ms (271x) | this doc, vs. real pickitoo incident |

Detector A alone was **verified blind** to CLOUDPT-11388: hwesteup00102 cluster-wide AND
pickitoo-specific Better Uptime p99 stayed flat (40-53ms) through the whole May 18-20 incident.

---

## Detector A — cluster-wide health-check latency tail (mesh HBONE HOL)

One line per AKS cluster; alert when the tail lifts *broadly* (p50/p95 move together).

```
_index=hostedplatformlogs _sourceCategory=hosted/*/istio _sourceName=istio-gateway
| parse field=_sourceCategory "hosted/*/*/istio" as reef, cluster nodrop
| json field=_raw "duration","user_agent","response_code" as duration_ms, ua, code nodrop
| where ua matches "*Better Uptime*"
| where code = "200"
| num(duration_ms)
| timeslice 1m
| pct(duration_ms,50) as p50_ms, pct(duration_ms,95) as p95_ms, count as reqs by _timeslice, cluster
| where reqs >= 500
| fields _timeslice, cluster, p95_ms
| transpose row _timeslice column cluster
```

- Alert on **p95** (needs >5% of a cluster's reqs slow = genuinely broad), not p99 (thin-tail/low-sample noise).
- **p50 lifting is the high-confidence confirmer** — only broad mesh degradation moves a cluster median.
- Gate `reqs >= 500`/bucket: low-traffic single-instance clusters (e.g. hwestus2p00801 = 1 instance)
  produce meaningless p99 spikes otherwise.
- `transpose` is required to get one series per cluster in Sumo's Time Series tab.

## Detector B — per-instance dead-stuck requests (THE pickitoo detector)

"Dead-stuck" signature = the request ran long, delivered **zero bytes**, and did not complete
successfully (Yun's CLOUDPT-11388 signature: server logged a response in Seq, gateway `bytes_sent=0`,
stream reset/timed-out).

### Alert form (per-instance count over a rolling window)
```
_index=hostedplatformlogs _sourceCategory=hosted/*/istio _sourceName=istio-gateway
| json field=_raw "duration","request_authority","response_code","bytes_sent","upstream_cluster_raw" as duration_ms, authority, code, bytes, upstream nodrop
| parse field=upstream "outbound|*||*" as up_port, up_svc nodrop
| where up_port = "80"                       // web/API traffic ONLY (tentacles route to Halibut/gRPC port 8443)
| num(duration_ms) | num(bytes) | num(code)
| where duration_ms > 60000                  // clearly stuck, not a normal slow request
| where bytes = 0                            // server delivered NOTHING (only server-side; a slow client has bytes>0)
| where code < 200 or code = 408 or code >= 500   // stuck/timed-out: 0/1xx=abort, 408=req timeout, 5xx=gw timeout/error; drops 2xx/3xx + other 4xx (client errors)
| timeslice 1h
| count as stuck by _timeslice, authority
| where stuck >= 12                          // ALERT threshold
| sort by stuck
```

### Why each filter (all matter — removing any reintroduces false positives / negatives)
- `up_port = "80"` — excludes polling-tentacle connections (legitimately long, hours/days; without
  this the signal drowns: 40 instances/24h, durations up to 96h). Filters on the **upstream cluster
  port** (`outbound|80||…` = Octopus web/API vs `outbound|8443||…` = Halibut/gRPC tentacle) — a routing
  fact, not a client-supplied Host header, so more durable than `!(authority matches "*:8443")`.
  Verified 100% congruent over 7d with authority-port AND grpc-go user-agent.
- `duration > 60000` — drops normal fast requests and quick client aborts.
- `bytes = 0` — the biggest FP killer. Zero bytes delivered can ONLY be server/mesh-side — a slow
  *client* download has bytes>0. Separates *stuck* from *legit long downloads/streams* (e.g. `cpsi`,
  11 long disconnects, all bytes>0 = legit, correctly dropped). NOTE: prod logs do NOT populate
  `response_duration`/`response_tx_duration`, so `bytes=0` is the only way to isolate server-side.
- `code < 200 or code = 408 or code >= 500` — keep aborts (`0`/`1xx`), request-timeout (`408`), and
  server 5xx (timeout/error); drop `2xx`/`3xx` (success/redirect) and all other `4xx` (client/auth
  errors like 401/403/404/429 — a client's problem, not an infra stall). **This replaces the old
  `details = "downstream_remote_disconnect"` filter, which was a BUG**: stuck requests mostly terminate
  as `http2.remote_reset`, not disconnect (493 vs 91 in the incident), so disconnect-only caught just
  ~15%. Codes carried by stuck (bytes=0, dur>60s) requests fleet-wide (7d): `0` 59% (reset+disconnect —
  Envoy logs `0` when the downstream aborts before a response completes; **NOT 503**), `504` 23%
  (gateway timeout), `408` 7%, `502` 5%, `100` 4% (upload abandon); dropped: `200/304/201` (2.5%,
  completed-empty = not stuck). Ordinary 4xx never reach here anyway — they return fast with a body, so
  `bytes=0 + dur>60s` excludes them; `408` is the only 4xx present and we keep it explicitly. Requires
  `num(code)`. Switching off disconnect-only made the detector ~8x more sensitive (pickitoo peak
  14/hr -> 133/hr) at the SAME baseline instance count.

### Threshold (re-derived on the corrected, more-sensitive signature)
| threshold | baseline firings | pickitoo | milltech |
|---|---|---|---|
| >=5/hr | 4.1/day | fires | fires |
| **>=12/hr (recommended)** | 1.9/day | fires May-18 09:00 UTC (**~13h before ticket**), 11h breach | fires May-18 12:00, peak 38/hr |
| >=20/hr | 1.1/day | fires | fires (peak 38) |
Baseline firings are mostly *genuine* recurring cases (standardbank, philips-ei), not noise.

### REJECTED: throughput floor (measured, does not work)
A throughput floor (e.g. `bytes < 250KB/s * duration`) to catch "loads eventually, slowly" flags
**258 of 264 instances** — ~98% of all >60s requests are already <10KB/s because streams/long-polls
are low-throughput by nature. Throughput cannot distinguish a stuck asset from a healthy SignalR stream.

### OPTIONAL secondary lens: slow static assets (catches "loads eventually slowly")
`bytes=0` misses assets that DO deliver but slowly (72 such for pickitoo). Catch those by PATH, not
throughput — a static asset is never legitimately slow:
`... | where up_port="80" | where request_path matches "*hashedasset*" | where duration_ms > 60000`
Tighter than throughput (57 instances/7d vs 258) and catches delivered-but-slow, BUT can't prove
server-side vs slow-client without `response_duration` — so treat as secondary, not the primary alert.

### Dashboard panel forms (time series — one line per instance)
Same signature as the alert, but `timeslice` + `transpose` so each instance renders as its own line
over time. `transpose` is REQUIRED — grouping `by _timeslice, authority` alone collapses to one series
in Sumo's Time Series tab.

```
_index=hostedplatformlogs _sourceCategory=hosted/*/*/istio _sourceName=istio-gateway
| json field=_raw "duration","request_authority","response_code","bytes_sent","upstream_cluster_raw" as duration_ms, authority, code, bytes, upstream nodrop
| parse field=upstream "outbound|*||*" as up_port, up_svc nodrop
| where up_port = "80"
| num(duration_ms) | num(bytes) | num(code)
| where duration_ms > 60000
| where bytes = 0
| where code < 200 or code = 408 or code >= 500
| timeslice 1h
| count as blocked by _timeslice, authority
| transpose row _timeslice column authority
```

Fleet-wide (one line per affected instance across all clusters) — swap the last three lines' scope:
```
... _sourceCategory=hosted/*/istio ...                       // all clusters
  (drop the `where authority in (...)` line)
| timeslice 1h
| count as blocked by _timeslice, authority
| where blocked >= 3                                          // keep only instances actually stalling (else the chart is a mess of singletons)
| transpose row _timeslice column authority
```

Panel setup: **Time Series** tab (not Chart); **Missing Data Display = `0`** (Panel Settings) so
gaps render as zero instead of breaking the line; `timeslice 1h` is fine for multi-day ranges (Sumo
1440-bucket cap: range ÷ timeslice ≤ 1440). Renders correctly in NZ local time (UTC+12) — pickitoo's
14→133/hr peak (UTC May-18 12:00) shows at 00:00 May-19 on the dashboard.

By-cluster rollup instead of per-instance: group `by _timeslice, cluster` (add `| parse
field=_sourceCategory "hosted/*/*/istio" as reef, cluster nodrop`) and `transpose ... column cluster`.

### Validation vs. the real incident (May 17-21, hwesteup00102) — corrected detector
| instance | first dead-stuck | alert fires (>=12/hr) | peak/hr | vs. ticket (May 18 22:26 UTC) |
|---|---|---|---|---|
| **pickitoo** | May 18 09:00 | **May 18 09:00 UTC** (11h breach) | **133** | **~13h BEFORE the P1 ticket** |
| milltech | May 18 10:00 | May 18 12:00 UTC | 38 | caught |

### False-positive floor (7d, whole prod fleet, refined signature)
- 49 instance-hours had >=1 dead-stuck; **39 were singletons** (=1) — the noise floor, ignored by >=5.
- `>= 5/hr` fires ~0.9 instance-hours/day fleet-wide, and those are mostly **real** (drbsystems
  June-25 episode w/ hanging `*.hashedasset.js`; milltech/porcupineunion residual on the incident cluster).
- Peaks: pickitoo 14/hr, milltech 13/hr → `>=5` sits cleanly between incident and noise.

### Threshold options
- **`>= 5/hr` (recommended)** — simple, fires 10h early on pickitoo, ~0.9/day FP (mostly real).
- `>= 3/hr sustained 2 consecutive hours` — more sensitive, resists single-hour flukes; also catches both.
- `>= 8/hr` — even lower FP, still catches pickitoo(14)/milltech(13); may miss milder cases.

### Tuning / open questions for tomorrow
- `bytes = 0` vs `bytes < 10000`: strict 0 = cleanest FP but could miss *partial-delivery* stalls
  (response started then hung). Incident was bytes=0, so 0 is the validated choice.
- `duration > 30000` would catch shorter (still user-hostile) stalls at some FP cost.
- Consider an asset-path-only variant (`request_path matches "*.hashedasset.*"`) for a near-zero-FP
  high-precision alert — a static asset hanging >60s is unambiguously broken (Yun's smoking gun).
- Review long-lived API paths seen in drbsystems (`/ingest`, `/api/otlp/v1/traces`) — decide if
  telemetry-ingest hangs are in-scope or should be excluded.

## Live findings (as of 2026-07-02)
- **No** cluster currently shows mesh-wide HBONE HOL (Detector A) — fleet p95 tight ~30ms.
- **No** live pickitoo-class fire right now (Detector B). drbsystems was a June-25 episode, resolved.
  milltech shows low residual (1/day).
- The 24h dashboard "spikes" the eye catches are single low-traffic instances (hudsonbaycapital 24h
  p99=29ms; the "505ms spike" = one request in a 59-sample bucket) — not incidents.
