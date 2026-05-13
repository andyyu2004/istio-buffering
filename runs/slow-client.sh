#!/usr/bin/env bash
# Run the Go slow client inside a Linux container so SO_RCVBUF is actually
# honored by the kernel (macOS loopback doesn't enforce small recv buffers).
#
# Usage:
#   ./slow-client.sh                              # defaults: rcvbuf=4096, rate=125 KB/s, 60s
#   ./slow-client.sh --rate 25 --rcvbuf 2048      # tighter slow client
#   ./slow-client.sh --port 19999                 # via toxiproxy instead of gateway
#   ./slow-client.sh --timeout 30s
set -euo pipefail

# Defaults
RCVBUF=4096
RATE=125
TIMEOUT=60s
PROGRESS=5s
PORT=32768  # default: kind's exposed gateway host port

while [[ $# -gt 0 ]]; do
  case $1 in
    --rcvbuf)   RCVBUF=$2; shift 2 ;;
    --rate)     RATE=$2; shift 2 ;;
    --timeout)  TIMEOUT=$2; shift 2 ;;
    --progress) PROGRESS=$2; shift 2 ;;
    --port)     PORT=$2; shift 2 ;;
    -h|--help)
      sed -n '2,/^set -/p' "$0" | sed 's/^# \{0,1\}//' | head -n -1
      exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
URL="https://host.docker.internal:${PORT}/testdata.bin"

echo "slow client: rcvbuf=$RCVBUF  rate=${RATE} KB/s  timeout=$TIMEOUT  -> $URL" >&2

exec docker run --rm \
  -v "$REPO_ROOT:/work" -w /work \
  --add-host=host.docker.internal:host-gateway \
  golang:1.22-alpine \
  go run runs/slow_client.go \
    -url "$URL" \
    -rcvbuf "$RCVBUF" \
    -rate "$RATE" \
    -timeout "$TIMEOUT" \
    -progress "$PROGRESS"
