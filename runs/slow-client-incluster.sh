#!/usr/bin/env bash
# Build the slow client into a container image, load it into the kind cluster,
# and run it as a one-shot Job pointing directly at the gateway service. This
# eliminates the docker-proxy hop (and its multi-MB intermediate buffer) that
# the host-side variant suffers from.
#
# Usage:
#   ./slow-client-incluster.sh                              # defaults
#   ./slow-client-incluster.sh --rate 25 --rcvbuf 2048
#   ./slow-client-incluster.sh --rebuild                    # force image rebuild
set -euo pipefail

# Defaults
RCVBUF=4096
RATE=125
TIMEOUT=60s
PROGRESS=5s
REBUILD=0
KIND_CLUSTER=kind
IMAGE=slow-client:latest
JOB_NAME=slow-client
JOB_NS=default
GATEWAY_HOST=istio-gateway-istio.istio-gateway.svc.cluster.local

while [[ $# -gt 0 ]]; do
  case $1 in
    --rcvbuf)   RCVBUF=$2; shift 2 ;;
    --rate)     RATE=$2; shift 2 ;;
    --timeout)  TIMEOUT=$2; shift 2 ;;
    --progress) PROGRESS=$2; shift 2 ;;
    --rebuild)  REBUILD=1; shift ;;
    -h|--help)  sed -n '2,/^set -/p' "$0" | sed 's/^# \{0,1\}//' | head -n -1; exit 0 ;;
    *)          echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

cd "$(dirname "$0")"

# 1) Build image (only if missing or --rebuild)
if [[ "$REBUILD" == "1" ]] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo ">>> building $IMAGE"
  docker build -f slow-client.Dockerfile -t "$IMAGE" .
else
  echo ">>> reusing existing $IMAGE (use --rebuild to force)"
fi

# 2) Load into kind
echo ">>> loading $IMAGE into kind cluster '$KIND_CLUSTER'"
kind load docker-image "$IMAGE" --name "$KIND_CLUSTER"

# 3) Delete any prior job, then apply fresh
kubectl --context "kind-$KIND_CLUSTER" -n "$JOB_NS" delete job "$JOB_NAME" --ignore-not-found >/dev/null

cat <<EOF | kubectl --context "kind-$KIND_CLUSTER" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB_NAME
  namespace: $JOB_NS
spec:
  ttlSecondsAfterFinished: 120
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: slow-client
          image: $IMAGE
          imagePullPolicy: Never
          args:
            - "-url=https://$GATEWAY_HOST:9999/testdata.bin"
            - "-rcvbuf=$RCVBUF"
            - "-rate=$RATE"
            - "-timeout=$TIMEOUT"
            - "-progress=$PROGRESS"
EOF

echo ">>> waiting for pod to start..."
for i in $(seq 1 30); do
  POD=$(kubectl --context "kind-$KIND_CLUSTER" -n "$JOB_NS" get pod -l job-name=$JOB_NAME -o name 2>/dev/null | head -1 || true)
  if [[ -n "$POD" ]]; then
    PHASE=$(kubectl --context "kind-$KIND_CLUSTER" -n "$JOB_NS" get "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [[ "$PHASE" == "Running" || "$PHASE" == "Succeeded" || "$PHASE" == "Failed" ]]; then break; fi
  fi
  sleep 0.5
done

echo ">>> streaming logs (Ctrl-C to detach; job will keep running)"
echo
exec kubectl --context "kind-$KIND_CLUSTER" -n "$JOB_NS" logs -f "$POD"
