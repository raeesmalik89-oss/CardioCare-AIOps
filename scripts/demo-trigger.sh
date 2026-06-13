#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# CardioCare-AIOps — Live demo trigger: inject a cardiac emergency
# Author : Muhammad Raees
# Usage  : ./scripts/demo-trigger.sh [count]      (default: 5 critical events)
#
# Publishes guaranteed-CRITICAL vitals events straight into the
# `cardiac.vitals.stream` Kafka topic. The AIOps engine consumes them live,
# the IsolationForest flags them, severity = CRITICAL, and a critical alert
# fires through the serverless alert-function. This is your "wow" moment.
#
# CRITICAL thresholds (from detector.py classify_severity):
#   spo2 < 85  OR  hr > 180  OR  hr < 35  OR  sbp > 185  OR  sbp < 75
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

COUNT="${1:-5}"
TOPIC="cardiac.vitals.stream"
TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PATIENTS=("PT-001" "PT-002" "PT-003" "PT-004" "PT-005")
WARDS=("ICU" "CCU" "CARDIAC-OT")

echo "==============================================================="
echo "  Injecting $COUNT CRITICAL cardiac events into '$TOPIC'"
echo "==============================================================="

# Build the batch of JSON events (one per line) with extreme, life-threatening vitals
EVENTS=""
for i in $(seq 1 "$COUNT"); do
  PT="${PATIENTS[$((RANDOM % ${#PATIENTS[@]}))]}"
  WARD="${WARDS[$((RANDOM % ${#WARDS[@]}))]}"
  HR=$((180 + RANDOM % 25))     # 180-204  -> tachycardia (CRITICAL: hr > 180)
  SPO2=$((70 + RANDOM % 12))    # 70-81    -> hypoxemia   (CRITICAL: spo2 < 85)
  SBP=$((188 + RANDOM % 20))    # 188-207  -> hypertensive crisis (CRITICAL: sbp > 185)
  read -r -d '' EV <<JSON || true
{"event_id":"DEMO-$PT-$i","patient_id":"$PT","timestamp":"$TS","sequence":$i,"vitals":{"heart_rate":$HR,"systolic_bp":$SBP,"diastolic_bp":118,"spo2":$SPO2,"ecg_amplitude":2.8,"temperature":39.4,"respiratory_rate":38},"ecg_sample":2.6,"device_id":"DEVICE-$PT","ward":"$WARD","is_simulated_anomaly":true}
JSON
  EVENTS+="$EV"$'\n'
  echo "  -> $PT ($WARD): HR=$HR  SpO2=$SPO2%  BP=$SBP/118   [CRITICAL]"
done

# Publish the batch into Kafka via the broker container's console producer
printf '%s' "$EVENTS" | docker exec -i kafka \
  kafka-console-producer --bootstrap-server kafka:29092 --topic "$TOPIC"

echo "---------------------------------------------------------------"
echo "Events published. Now SHOW the pipeline reacting:"
echo
echo "  1) Engine detecting + firing alerts (watch this live):"
echo "       docker logs -f --tail 20 cardiocare-aiops-engine"
echo
echo "  2) Alert function receiving the critical alert:"
echo "       docker logs -f --tail 20 cardiocare-alert-fn"
echo
echo "  3) Or hit the API to see the alerts land:"
echo "       curl -s http://localhost:8000/api/v1/alerts | jq"
echo "==============================================================="
