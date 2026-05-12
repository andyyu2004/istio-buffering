#!/usr/bin/env bash
# Run toxiproxy in front of the kind-exposed gateway to simulate a real slow consumer.
#
# Usage:
#   ./toxiproxy-setup.sh                # start toxiproxy + bandwidth toxic
#   ./toxiproxy-setup.sh latency        # also add a latency toxic
#   ./toxiproxy-setup.sh teardown       # stop and remove
set -euo pipefail

ACTION=${1:-setup}
CONTAINER=istio-buffering-toxiproxy
PROXY_LISTEN_PORT=19999
ADMIN_PORT=8474
BANDWIDTH_KBPS=125  # downstream throughput cap; matches our prior `curl --limit-rate 125k`
LATENCY_MS=50

if [[ "$ACTION" == "teardown" ]]; then
  docker rm -f "$CONTAINER" 2>/dev/null || true
  echo "stopped $CONTAINER"
  exit 0
fi

KINDCCM=$(docker ps --filter "name=kindccm" --format '{{.Names}}' | head -1)
if [[ -z "$KINDCCM" ]]; then
  echo "no kindccm container running"; exit 1
fi
GATEWAY_HOST_PORT=$(docker port "$KINDCCM" 9999 | head -1 | awk -F: '{print $NF}')

echo "kind container:      $KINDCCM"
echo "gateway host port:   $GATEWAY_HOST_PORT"
echo "toxiproxy listen:    $PROXY_LISTEN_PORT"

docker rm -f "$CONTAINER" 2>/dev/null || true
docker run -d --name "$CONTAINER" \
  -p "$ADMIN_PORT:$ADMIN_PORT" \
  -p "$PROXY_LISTEN_PORT:$PROXY_LISTEN_PORT" \
  --add-host=host.docker.internal:host-gateway \
  ghcr.io/shopify/toxiproxy:2.9.0 \
  -host=0.0.0.0 >/dev/null

# Wait for admin
for i in $(seq 1 20); do
  if curl -sf "http://localhost:$ADMIN_PORT/version" >/dev/null; then break; fi
  sleep 0.25
done

# Create proxy
curl -sf -X POST "http://localhost:$ADMIN_PORT/proxies" \
  -H 'Content-Type: application/json' \
  -d "{
    \"name\": \"gateway-slow\",
    \"listen\": \"0.0.0.0:$PROXY_LISTEN_PORT\",
    \"upstream\": \"host.docker.internal:$GATEWAY_HOST_PORT\",
    \"enabled\": true
  }" >/dev/null

# Bandwidth toxic on the downstream (server → client) direction.
# This is what actually back-pressures the gateway: toxiproxy reads from the
# gateway into a small internal buffer and only drains to the client at the
# configured rate, so the gateway's TCP send window closes when the buffer fills.
curl -sf -X POST "http://localhost:$ADMIN_PORT/proxies/gateway-slow/toxics" \
  -H 'Content-Type: application/json' \
  -d "{
    \"name\": \"bw_downstream\",
    \"type\": \"bandwidth\",
    \"stream\": \"downstream\",
    \"attributes\": {\"rate\": $BANDWIDTH_KBPS}
  }" >/dev/null

if [[ "$ACTION" == "latency" ]]; then
  curl -sf -X POST "http://localhost:$ADMIN_PORT/proxies/gateway-slow/toxics" \
    -H 'Content-Type: application/json' \
    -d "{
      \"name\": \"latency_downstream\",
      \"type\": \"latency\",
      \"stream\": \"downstream\",
      \"attributes\": {\"latency\": $LATENCY_MS}
    }" >/dev/null
  echo "added latency toxic: ${LATENCY_MS}ms downstream"
fi

echo
echo "Toxiproxy ready.  Test with:"
echo
echo "  curl -k https://127.0.0.1:$PROXY_LISTEN_PORT/testdata.bin -o /dev/null --http1.1 --max-time 600"
echo
echo "(no --limit-rate needed; toxiproxy caps downstream at ${BANDWIDTH_KBPS} KB/s)"
echo
echo "Toxics:"
curl -sf "http://localhost:$ADMIN_PORT/proxies/gateway-slow/toxics" | python3 -m json.tool
echo
echo "Tear down with:  $0 teardown"
