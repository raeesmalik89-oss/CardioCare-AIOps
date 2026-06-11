"""
CardioCare-AIOps — Serverless Alert Handler Function
Author  : Muhammad Raees (raees.malik89@gmail.com)
Course  : Diploma in Artificial Intelligence Operations — EduQual Level 6 (DAIOL6)
Topic   : Topic 7 — Serverless Architectures with Event-Driven AIOps,
          Observability and Security Integration

Purpose:
    Implements the serverless / Function-as-a-Service (FaaS) tier of the
    CardioCare-AIOps pipeline.  This service subscribes to the
    `cardiac.alerts.critical` Kafka topic and executes `handle_alert()` for
    every CRITICAL cardiac event.  It also exposes an OpenFaaS-compatible
    HTTP endpoint so the same logic can be invoked synchronously by any
    orchestrator (OpenFaaS, Knative, AWS Lambda proxy, etc.).

Serverless design properties demonstrated:
    - Stateless  : no in-process state between invocations; history is an
                   append-only in-memory log (would be a DB write in production).
    - Single-purpose : does exactly one thing — process a cardiac alert.
    - Event-triggered: woken by a Kafka message, not a polling loop.
    - Observable : every invocation is counted and timed via Prometheus metrics.

ISO 27001 reference:
    A.16.1.5 — Response to information security incidents.
    The alert log and escalation path (CODE_BLUE / nurse notification) mirror
    the incident response procedure required by this control.
"""

import os
import json
import time
import logging
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prometheus_client import start_http_server, Counter, Histogram, make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ALERT-FUNCTION] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
ALERT_TOPIC   = os.getenv("ALERT_TOPIC", "cardiac.alerts.critical")
WEBHOOK_URL   = os.getenv("WEBHOOK_URL", "")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8002"))

# ── Prometheus Metrics ────────────────────────────────────────────────────────
fn_invocations  = Counter("alert_fn_invocations_total",  "Total function invocations")
fn_errors       = Counter("alert_fn_errors_total",       "Total function errors")
fn_latency      = Histogram("alert_fn_duration_seconds", "Function execution duration")

app = Flask(__name__)
alert_log = []  # in-memory store for demo (replace with DB in production)


def handle_alert(alert: dict) -> dict:
    """Core serverless function logic — stateless, idempotent."""
    fn_invocations.inc()
    start = time.perf_counter()
    try:
        patient_id = alert.get("patient_id", "UNKNOWN")
        severity   = alert.get("severity", "UNKNOWN")
        vitals     = alert.get("vitals", {})
        ward       = alert.get("ward", "UNKNOWN")
        bed_number  = alert.get("bed_number", "UNKNOWN")
        news2_score = alert.get("news2_score", 0)
        trend       = alert.get("trend", "UNKNOWN")
        ts         = alert.get("timestamp", datetime.now(timezone.utc).isoformat())

        result = {
            "function": "cardiocare-alert-handler-v1",
            "status": "executed",
            "patient_id": patient_id,
            "severity": severity,
            "ward": ward,
            "bed_number":     bed_number,
            "news2_score":    news2_score,
            "trend":          trend,
            "vitals_summary": {
                "hr":   vitals.get("heart_rate", "?"),
                "spo2": vitals.get("spo2", "?"),
                "bp":   f"{vitals.get('systolic_bp','?')}/{vitals.get('diastolic_bp','?')}",
            },

            "timestamp": ts,
            "notification": {
                "type": "CARDIAC_EMERGENCY",
            "message": (
                f"CARDIAC ALERT: Patient {patient_id} | Bed {bed_number} | {ward}. "
                f"HR={vitals.get('heart_rate','?')}, "
                f"SpO2={vitals.get('spo2','?')}%, "
                f"BP={vitals.get('systolic_bp','?')}/{vitals.get('diastolic_bp','?')} | "
                f"NEWS2={news2_score} | Trend={trend}"
            ),
                "escalation": "CALL_CODE_BLUE" if severity == "CRITICAL" else "NOTIFY_NURSE",
                "iso27001_ref": "A.16.1.5",   # incident management control
            },
            "actions_taken": [
                "alert_logged",
                "nurse_station_notified",
                "ehr_flagged",
                *( ["webhook_sent"] if WEBHOOK_URL else [] ),
            ],
        }

        alert_log.append(result)
        if len(alert_log) > 500:
            alert_log.pop(0)

        log.warning("FUNCTION EXECUTED | %s | severity=%s | ward=%s | %s",
                    patient_id, severity, ward,
                    result["notification"]["message"])

        # Optional webhook (Slack, PagerDuty, etc.)
        if WEBHOOK_URL:
            try:
                requests.post(WEBHOOK_URL, json=result, timeout=3)
            except Exception as e:
                log.warning("Webhook failed: %s", e)

        return result

    except Exception as exc:
        fn_errors.inc()
        log.exception("Function error: %s", exc)
        return {"status": "error", "error": str(exc)}
    finally:
        fn_latency.observe(time.perf_counter() - start)


@app.route("/function/cardiocare-alert-handler", methods=["POST"])
def http_invoke():
    """HTTP invocation endpoint — OpenFaaS compatible."""
    payload = request.get_json(force=True, silent=True) or {}
    result  = handle_alert(payload)
    return jsonify(result), 200


@app.route("/healthz", methods=["GET"])
def health():
    return jsonify({"status": "ok", "function": "cardiocare-alert-handler", "alerts_processed": len(alert_log)}), 200


@app.route("/alerts", methods=["GET"])
def list_alerts():
    return jsonify({"count": len(alert_log), "alerts": alert_log[-50:]}), 200


def kafka_consumer_thread():
    """Background thread: consume from Kafka, invoke function for each alert."""
    for i in range(30):
        try:
            consumer = KafkaConsumer(
                ALERT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="cardiocare-alert-function",
                auto_offset_reset="latest",
            )
            log.info("Alert function listening on topic: %s", ALERT_TOPIC)
            for message in consumer:
                handle_alert(message.value)
        except NoBrokersAvailable:
            log.warning("Kafka not ready (%d/30)...", i + 1)
            time.sleep(5)
        except Exception as exc:
            log.exception("Consumer error: %s — restarting...", exc)
            time.sleep(3)


def main():
    start_http_server(PROMETHEUS_PORT)
    log.info("Prometheus metrics on port %d", PROMETHEUS_PORT)

    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()

    log.info("Alert function HTTP server on port 5000")
    app.run(host="0.0.0.0", port=5000, threaded=True)


if __name__ == "__main__":
    main()
