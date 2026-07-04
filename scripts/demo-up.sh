#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# CardioCare-AIOps — One-command demo startup & health check
# Author : Muhammad Raees
# Usage  : ./scripts/demo-up.sh          (run from repo root, on the EC2 box)
#
# Brings the whole stack up, waits for it to settle, verifies every container
# is healthy, then prints all the demo URLs with your public IP filled in.
# Run this BEFORE your presentation — not live.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# Resolve repo root so the script works from anywhere
cd "$(dirname "$0")/.."

# Auto-detect the instance's public IP (used in printed URLs + Kafka/Grafana env)
PUBLIC_IP="$(curl -s --max-time 5 ifconfig.me || echo 'YOUR_EC2_IP')"
export EC2_PUBLIC_IP="$PUBLIC_IP"

echo "==============================================================="
echo "  CardioCare-AIOps — bringing the stack up"
echo "  Public IP : $PUBLIC_IP"
echo "==============================================================="

# Pick docker compose v2 (plugin) or legacy docker-compose
if docker compose version >/dev/null 2>&1; then
  DC="docker compose"
else
  DC="docker-compose"
fi

echo "[1/3] Starting containers..."
$DC up -d

echo "[2/3] Waiting 25s for Kafka, Keycloak and the AIOps engine to warm up..."
sleep 25

echo "[3/3] Container health:"
echo "---------------------------------------------------------------"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo "---------------------------------------------------------------"

# Quick smoke test of the API gateway
API_CODE="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://localhost:8000/health || echo 000)"
if [ "$API_CODE" = "200" ]; then
  echo "API /health .......... OK (200)"
else
  echo "API /health .......... NOT READY (got $API_CODE) — wait a few seconds and re-check"
fi

cat <<URLS

===============================================================
  DEMO URLS  (bookmark these before presenting)
===============================================================
  API Swagger docs   : http://$PUBLIC_IP:8000/docs
  API system status  : http://$PUBLIC_IP:8000/api/v1/system/status
  Live anomalies     : http://$PUBLIC_IP:8000/api/v1/anomalies
  Critical alerts    : http://$PUBLIC_IP:8000/api/v1/alerts

  Grafana dashboards : http://$PUBLIC_IP:3000      (admin / CardioCare@2024)
  Kafka-UI (events)  : http://$PUBLIC_IP:8085
  Jaeger tracing     : http://$PUBLIC_IP:16686
  Prometheus         : http://$PUBLIC_IP:9091
  Keycloak (auth)    : http://$PUBLIC_IP:8095      (admin / CardioCare@2024)

  To fire a live cardiac emergency during the demo:
      ./scripts/demo-trigger.sh
===============================================================
URLS
