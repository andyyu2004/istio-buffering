# HBONE window tuning — head-of-line-blocking on aks3

## Setup
Istio ambient: `client → gateway → [HBONE/HTTP2] → waypoint → [HBONE] → ztunnel → backend`. Many
connections are multiplexed as HTTP/2 streams over a shared HBONE connection, so a few **slow
clients** can exhaust the shared connection's flow-control window and stall *everyone* on it (HOL).

The HTTP/2 windows are set on both layers together — istiod `PILOT_HBONE_INITIAL_{STREAM,CONNECTION}_WINDOW_SIZE`
and ztunnel `HTTP2_{STREAM,CONNECTION}_WINDOW_SIZE`. Key quantities: **stream window** (caps single-stream
throughput) and the **connection:stream ratio** (how many streams can be "full" before they starve each other).

## Test
Real cluster `andyreef1aks3`, load via `curl` from a laptop to the public gateway `68.218.94.197:9999`
(caddy serving a 1 GB file). Per config: set windows → `helm upgrade` istiod+ztunnel → restart
gateway+waypoint (fresh proxies) → measure.

**Metric:** one **unthrottled** stream's throughput (MB/s) measured **alone**, then again while **40
slow clients (50 KiB/s each)** run concurrently. The slow clients use only ~2 MB/s of uplink
(negligible), but trigger HBONE HOL. Fast stream holds → no HOL; collapses → HOL. (Absolute MB/s is
laptop-uplink-capped ~35; the signal is the *degradation*, not the alone value.)

`(×3)` = the under-load measurement was repeated **3 times sequentially** (one fast stream at a
time, back-to-back), each with the 40 slow clients running throughout.
## Data

| stream | conn | ratio | fast alone | fast under 40 slow (×3) | |
|---|---|---|---|---|---|
| 64K | 128K | 2× | 39.6 | 0.0  0.0  0.0 | ❌ |
| 64K | 512K | 8× | 36.0 | 0.1  0.1  0.1 | ❌ |
| 64K | 2M | 32× | 38.2 | 43.5  42.1  40.5 | ✅ |
| 256K | 512K | 2× | 37.9 | 0.0  0.0  0.0 | ❌ |
| 256K | 2M | 8× | 36.7 | 0.1  0.1  0.1 | ❌ |
| 256K | 8M | 32× | 38.6 | 38.6  24.9  35.8 | ✅ |
| 1M | 2M | 2× | 37.1 | 0.0  0.0  0.0 | ❌ |
| 1M | 8M | 8× | 32.6 | 0.2  0.1  0.0 | ❌ |
| 1M | 32M | 32× | 32.9 | 28.3  33.8  37.5 | ✅ |
| **16M** | **24M** | **1.5× (prod defaults)** | **34.7** | **0.1  0.0  0.0** | ❌ |

