# Istio buffering A/B results

Slow client repro: `curl --limit-rate 125k --http1.1` to `caddy-service:9999` via the istio gateway. Looking at Wireshark first-burst (bytes Caddy emits in first ~10s) and per-component memory.

## Test path

`Client ↔ Gateway (Envoy) ↔ Waypoint (Envoy, HBONE) ↔ ztunnel ↔ Caddy` (waypoint bypassed in steps 3–5)

## Results

| # | Config | Wireshark burst | Gateway Δmem | Waypoint Δmem | Ztunnel Δmem | Waypoint h2_pending |
|---|---|---|---|---|---|---|
| 1 | **Baseline** — stock 1.29.1, no improvements | ~73 MB | +28 MB | +40 MB | n/a | 16.8 MB |
| 2 | + EnvoyFilters (per_conn_buf=32k, NOTSENT_LOWAT=16k) + ztunnel HTTP/2 windows (env vars) | ~70 MB | +30 MB | +11 MB | +8 MB | 74 KB |
| 3 | (2) **+ waypoint bypassed** | 42 MB | +33 MB | 0 (idle) | +8 MB | n/a |
| 4 | (3) + custom pilot, meshConfig `hboneInitial*WindowSize` — *silently ignored* | 52 MB | +33 MB | 0 | +8 MB | n/a |
| 5 | (3) + custom pilot **with env vars** `PILOT_HBONE_INITIAL_{STREAM,CONNECTION}_WINDOW_SIZE` | **13 MB** | **+2 MB** | 0 | +3 MB | n/a |

## Key findings

1. **ztunnel HTTP/2 windows already cut the waypoint H/2 send buffer from 16.8 MB → 74 KB** (step 1→2). Total memory only drops modestly though, because buffering also moves to the gateway/ztunnel.
2. **Bypassing the waypoint** (step 2→3) cuts Wireshark by ~28 MB but still leaves ~33 MB on the gateway with no visible owner — it lives in the gateway's HBONE H/2 receive buffer (uncapped by default).
3. **The `hboneInitial*WindowSize` meshConfig fields don't exist in pilot** (step 3→4). The custom pilot we built (from istio master, your PR `a515927007`) only reads `PILOT_HBONE_INITIAL_STREAM_WINDOW_SIZE` and `PILOT_HBONE_INITIAL_CONNECTION_WINDOW_SIZE` env vars. The `improved/README.md`'s meshConfig path is misleading — those fields land in the CM but pilot ignores them. Worth fixing in the README and/or pursuing the istio/api PR.
4. **Setting the env vars caps the gateway's `connect_originate` cluster H/2 receive window to 64K stream / 256K connection** (step 5). Verified via `istioctl pc cluster -n istio-gateway deploy/istio-gateway-istio --fqdn connect_originate -o json`. This is the actual fix: gateway Δmem drops from +33 MB → +2 MB, Wireshark from 42 MB → 13 MB.

## Bottom line

The three improvements from `improved/README.md` all contribute, but the **HBONE upstream H/2 window settings on `connect_originate`** are by far the dominant fix. With all three live:

- Wireshark first-burst: **73 MB → 13 MB** (−82%)
- Total memory used across gateway/waypoint/ztunnel: **~68 MB → ~5 MB** (−93%)

## Config knobs that actually mattered (verified via `istioctl pc`)

| Component | Setting | Value | How it's set |
|---|---|---|---|
| pilot | `PILOT_HBONE_INITIAL_STREAM_WINDOW_SIZE` | 65535 | env var on istiod (NOT meshConfig) |
| pilot | `PILOT_HBONE_INITIAL_CONNECTION_WINDOW_SIZE` | 262140 | env var on istiod (NOT meshConfig) |
| ztunnel | `HTTP2_STREAM_WINDOW_SIZE` | 65535 | meshConfig.defaultConfig.proxyMetadata |
| ztunnel | `HTTP2_CONNECTION_WINDOW_SIZE` | 262140 | meshConfig.defaultConfig.proxyMetadata |
| ztunnel | `HTTP2_FRAME_SIZE` | 16384 | meshConfig.defaultConfig.proxyMetadata |
| gateway/waypoint listeners | `per_connection_buffer_limit_bytes` | 32768 | EnvoyFilter `applyTo: LISTENER` |
| gateway listener `0.0.0.0_9999` | `TCP_NOTSENT_LOWAT` | 16384 | EnvoyFilter `socket_options` |

## Verifying live config

Two gotchas to know when poking at config dumps:

1. **`istioctl pc` emits camelCase JSON** (`initialStreamWindowSize`, `perConnectionBufferLimitBytes`, `socketOptions`). Envoy's `/config_dump` admin endpoint returns snake_case. Don't mix them up.
2. **`socket_option.name` and `intValue` are JSON-encoded as strings** (`"25"` not `25`) because the proto type is uint64 and protojson stringifies 64-bit ints to avoid JS precision loss. Compare with `str(o.get('name'))=='25'`.

Example verification:

```fish
istioctl pc cluster -n istio-gateway deploy/istio-gateway-istio --fqdn connect_originate -o json \
  | jq '.[] | select(.name=="connect_originate")
              | .typedExtensionProtocolOptions["envoy.extensions.upstreams.http.v3.HttpProtocolOptions"]
              | .explicitHttpConfig.http2ProtocolOptions
              | {initialStreamWindowSize, initialConnectionWindowSize, allowConnect}'
```

## Known gaps

- `connect_originate` listener on gateway has no `per_connection_buffer_limit_bytes` (EnvoyFilter context match doesn't catch it). Worth fixing — the listener-side buffer can still hold data.
- Waypoint listeners don't accept `TCP_NOTSENT_LOWAT` (Envoy rejects socket option on internal listeners).
- Step 5 was tested with waypoint *bypassed*. Should re-run with waypoint in path to confirm the full Polling-Tentacle scenario, since production has the waypoint inline.

---

# Follow-up: which improvement matters most? (automated sweep, waypoint in path)

Driven by `runs/sweep.py` — automated curl + Prometheus metric capture across configs.

Primary metric: `sum(istio_tcp_sent_bytes_total{reporter="destination",destination_app="caddy-server",pod=<current ztunnel>})` delta over the 30s curl window. This is the bytes Caddy's response payload contributed, as reported by the destination ztunnel — the moral equivalent of what tcpdump on the Caddy pod would see emitted by Caddy on its TLS socket.

Cross-check: `envoy_cluster_upstream_cx_rx_bytes_total{cluster_name="connect_originate", pod=<current gateway>}` — bytes the gateway received over HBONE.

Filtering by current pod name handles counter resets across configs (pods restart between rows).

## Per-improvement isolation

All rows have **waypoint in path** (production scenario). Numbers are the byte delta over a single 30s curl run with `--limit-rate 125k --http1.1`.

| Config | Caddy emitted | Gateway HBONE recv | Reduction vs baseline |
|---|---|---|---|
| Baseline (no improvements) | **72.06 MB** | 38.44 MB | — |
| All 3 improvements | **24.09 MB** | 16.60 MB | **−67%** |
| Pilot HBONE envvars *only* | **28.28 MB** | 17.60 MB | **−61%** |
| ztunnel HTTP/2 envvars *only* | 72.00 MB | 40.04 MB | 0% |
| EnvoyFilters *only* | 62.85 MB | 32.03 MB | −13% |

## Headline finding

**`PILOT_HBONE_INITIAL_STREAM_WINDOW_SIZE` and `PILOT_HBONE_INITIAL_CONNECTION_WINDOW_SIZE` alone deliver ~91% of the total improvement** (44 MB cut out of the 48 MB delta between baseline and full stack).

The other two improvements move buffering around but don't shrink the total when the gateway's HBONE H/2 receive window is left at Envoy's default (256 MB-ish):

- **ztunnel envvars in isolation look like a no-op for caddy-side bytes** — but they do empty the *waypoint*'s H/2 send buffer (16.8 MB → 74 KB, observed earlier). Buffering simply shifts to the gateway, which still accepts it.
- **EnvoyFilters in isolation** cap listener-level buffers but don't constrain the H/2 receive window, so most of the in-flight bytes still flow through.

## Implications for rolling out to production (HostedScripts)

1. The **must-have** change is the istiod pilot env vars from PR `istio/istio#59979` (on `release-1.29`, shipping in `1.29.3`):
   - `PILOT_HBONE_INITIAL_STREAM_WINDOW_SIZE=65535`
   - `PILOT_HBONE_INITIAL_CONNECTION_WINDOW_SIZE=262140`
2. ztunnel envvars + EnvoyFilters are **nice-to-have**: they tidy up buffering at other layers (waypoint memory, kernel TCP send buffer on the gateway) but contribute only the marginal last ~4 MB once the pilot envvars are in place.
3. The current `improved/README.md` recommends them in the order *EnvoyFilters → ztunnel windows → HBONE windows*. **The actual priority is the reverse** — HBONE pilot envvars first, the others second.
4. The README references these as `hboneInitial*WindowSize` meshConfig fields. **That surface doesn't exist in pilot** — only the env vars do. The meshConfig path silently no-ops. Worth updating the README or pursuing the istio/api PR to add the meshConfig surface.

## Measurement notes

- Envoy proto-validates `InitialStreamWindowSize ∈ [65535, 2^31-1]`. Anything below 64 KB minus 1 is rejected and the gateway pod fails to become ready. The 64 KB / 256 KB pair from the README is at the minimum.
- The original READMEs's Wireshark "first-burst" of ~73 MB matches our automated metric (72.06 MB) within run-to-run variance — even though Wireshark counts TLS framing + ACKs that ztunnel's counters don't.
- Counter handling: pod restarts between configs reset `istio_tcp_sent_bytes_total`. The sweep filters by current pod name (resolved after `reset.sh`) to avoid summing in stale series from previous configs.

## Replicate

```fish
# matrix lives at runs/sweep.py
python3 runs/sweep.py --list   # see configs
python3 runs/sweep.py          # run (writes runs/sweep.csv incrementally)
```

Resume-friendly: if `runs/sweep.csv` already has a row for a config (by name), the sweep skips it. Delete the CSV to start fresh.
