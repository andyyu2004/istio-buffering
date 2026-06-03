# istio-buffering improved!

Through experimentation I have found 3 sets of settings that significantly improve (reduce) the amount of buffering that occurs with a slow client.

With the improved configuration settings in place, buffering is reduced from ~73MB down to ~12MB during the initial few seconds of the connection.

An earlier version of this note claimed the difference measured by wireshark and curl "grows over time to ~19MB", suggesting the some buffer was growing over time. That was a measurement artifact: Wireshark reports MB (10^6) while curl reports MiB (2^20),

Manual testing indicates that having these settings do not meaningfully change the "fast client" situation. I tested from a client with a 500Mbps connection roughly ~180ms RTT away from the K8s cluster and it was able to achieve 18MB/s with the original (excessive buffering) repro setup, and with this improved setup.

Further testing is necessary as my test setup used a single node, so many of the inter-component paths are effectively near-zero latency local sockets.


## Improved settings

### 1. HTTP/2 window size tuning

Set:
- initial stream window size to 64k
- initial connection window size to 256k

This needs to be done in (at least) two places:
- the `connect_originate` clusters used by HBONE (see [istiod-values.yaml](istiod-values.yaml#L19-L22))
  - note this uses new environment variables exposed by the PR linked [here](https://github.com/istio/istio/issues/59887#issuecomment-4296304025); you'll need to build the `pilot` image then reference it [here](istiod-values.yaml#L2); alternatively the PR has been merged into the `release-1.29` branch so you could use a dev build of that (or later)
- `ztunnel` (see [ztunnel-values.yaml](ztunnel-values.yaml)).

The values of 64k and 256k feel like they make sense for us given our K8s cluster nodes are essentially on the same "LAN", i.e. high bandwidth, low latency. Whilst the initial window sizes are small, they will quickly grow as long as there is no backpressure.


### 2. `per_connection_buffer_limit_bytes`

This and the next item are both set via [envoyfilters.yaml](envoyfilters.yaml).

Set to 32k.

Note that although `per_connection_buffer_limit_bytes` is set in several places, we _haven't_ set it in these places:
- Gateway connect_originate cluster
- Gateway connect_originate listener
- Waypoint connect_originate cluster
- inbound-vip clusters

It seems likely that setting this on some or all of the above may further improve things but I have not had time to try.

### 3. TCP_NOTSENT_LOWAT

Set to 16k.

We were able to set this on the Gateway, but it would not apply on a Waypoint.

When attempting to apply on a Waypoint, we get this error:
```
2026-04-21T07:41:29.398901Z    warning    envoy config external/envoy/source/extensions/config_subscription/grpc/delta_subscription_state.cc:283    delta config for type.googleapis.com/envoy.config.listener.v3.Listener rejected: Error adding/updating listener(s) main_internal: error adding listener named 'main_internal': does not support socket option
connect_originate: error adding listener named 'connect_originate': does not support socket option
inner_connect_originate: error adding listener named 'inner_connect_originate': does not support socket option
outer_connect_originate: error adding listener named 'outer_connect_originate': does not support socket option
    thread=14
2026-04-21T07:41:29.398963Z    warning    envoy config external/envoy/source/extensions/config_subscription/grpc/grpc_subscription_impl.cc:138    gRPC
config for type.googleapis.com/envoy.config.listener.v3.Listener rejected: Error adding/updating listener(s) main_internal: error adding listener named 'main_internal': does not support socket option
connect_originate: error adding listener named 'connect_originate': does not support socket option
inner_connect_originate: error adding listener named 'inner_connect_originate': does not support socket option
outer_connect_originate: error adding listener named 'outer_connect_originate': does not support socket option
    thread=14
```
