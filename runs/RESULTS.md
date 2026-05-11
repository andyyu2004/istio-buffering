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
