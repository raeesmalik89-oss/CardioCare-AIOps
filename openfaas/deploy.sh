#!/usr/bin/env bash
set -euo pipefail

IMAGE="${1:-mraees1989/cardiocare-alert-function:openfaas}"
GATEWAY="${OPENFAAS_GATEWAY:-http://127.0.0.1:8080}"
: "${EVENT_ENCRYPTION_KEY:?Set EVENT_ENCRYPTION_KEY before deploying}"

docker build -f openfaas/Dockerfile -t "$IMAGE" .
docker push "$IMAGE"
SECRET_FILE="$(mktemp)"
trap 'rm -f "$SECRET_FILE"' EXIT
printf '%s' "$EVENT_ENCRYPTION_KEY" > "$SECRET_FILE"
faas-cli secret create event-encryption-key \
  --gateway "$GATEWAY" --from-file "$SECRET_FILE" || true
faas-cli deploy \
  --gateway "$GATEWAY" \
  --name cardiocare-alert-handler \
  --image "$IMAGE" \
  --env FUNCTION_MODE=true \
  --label com.openfaas.scale.min=0 \
  --label com.openfaas.scale.max=5 \
  --label com.openfaas.scale.zero=true \
  --secret event-encryption-key \
  --annotation topic=cardiac.alerts.critical

echo "Deploy the OpenFaaS Kafka connector with topic cardiac.alerts.critical"
echo "and function cardiocare-alert-handler to complete event invocation."
