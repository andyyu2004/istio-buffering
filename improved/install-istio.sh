#!/usr/bin/env bash

set -euo pipefail

helm repo add istio https://istio-release.storage.googleapis.com/charts
helm repo update

istioHelmChartVersion="1.29.1"
k8sGatewayAPIVersion="v1.4.0"

kubectl apply -f istio-system-namespace.yaml

helm upgrade --install istio-base istio/base --version $istioHelmChartVersion -n istio-system --wait

kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/$k8sGatewayAPIVersion/experimental-install.yaml

helm upgrade --install istiod istio/istiod --version $istioHelmChartVersion -n istio-system --values istiod-values.yaml --wait

helm upgrade --install istio-cni istio/cni --version $istioHelmChartVersion -n istio-system --set profile=ambient --wait

helm upgrade --install ztunnel istio/ztunnel --version $istioHelmChartVersion -n istio-system --values ztunnel-values.yaml --wait

kubectl apply -f gateway-and-waypoint.yaml

# Turn on ALL the Envoy metrics
# kubectl patch configmap istio -n istio-system --type merge -p '{
#   "data": {
#     "mesh": "defaultConfig:\n  proxyStatsMatcher:\n    inclusionRegexps:\n    - \".*\"\n"
#   }
# }'

# Set Gateway and Waypoint log levels to "trace"
# istioctl pc log -n istio-gateway deploy/istio-gateway-istio --level trace
# istioctl pc log -n istio-waypoint deploy/istio-waypoint --level trace

kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.29/samples/addons/prometheus.yaml