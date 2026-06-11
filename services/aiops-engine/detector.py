"""
CardioCare-AIOps — AIOps Anomaly Detection Engine
Author  : Muhammad Raees (raees.malik89@gmail.com)
Course  : Diploma in Artificial Intelligence Operations — EduQual Level 6 (DAIOL6)
Topic   : Topic 7 — Serverless Architectures with Event-Driven AIOps,
          Observability and Security Integration

Purpose:
    Consumes the `cardiac.vitals.stream` Kafka topic in real time, extracts a
    seven-dimensional feature vector from each event, and scores it with an
    Isolation Forest model.  Events whose anomaly score falls below the
    configurable threshold are classified by clinical severity and forwarded
    to `cardiac.anomalies.detected`; CRITICAL events additionally trigger the
    serverless alert function via `cardiac.alerts.critical`.

Algorithm choice — why Isolation Forest:
    Cardiac monitoring data is predominantly normal (low contamination ~5%).
    Isolation Forest excels in this regime because it isolates anomalies rather
    than profiling the entire distribution.  It operates without labelled
    anomaly examples, which suits real clinical deployments where labelled
    cardiac-emergency datasets are scarce.  The model retrains automatically
    every MODEL_RETRAIN_INTERVAL seconds on the sliding observation buffer,
    adapting to patient-specific baseline drift.

AIOps integration:
    The engine closes the AIOps loop: it not only *detects* anomalies but
    *automates the response* by publishing to downstream topics, embodying the
    event-driven serverless pattern required by Topic 7.
"""

import os
import json
import time
import logging
import threading
from datetime import datetime, timezone
from collections import deque

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from sklearn.ensemble import IsolationForest
from prometheus_client import start_http_server, Counter, Gauge, Histogram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [AIOPS-ENGINE] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
INPUT_TOPIC   = os.getenv("INPUT_TOPIC",   "cardiac.vitals.stream")
ANOMALY_TOPIC = os.getenv("ANOMALY_TOPIC", "cardiac.anomalies.detected")
ALERT_TOPIC   = os.getenv("ALERT_TOPIC",   "cardiac.alerts.critical")
PROMETHEUS_PORT       = int(os.getenv("PROMETHEUS_PORT", "8001"))
ANOMALY_THRESHOLD     = float(os.getenv("ANOMALY_THRESHOLD", "-0.1"))
MODEL_RETRAIN_INTERVAL = int(os.getenv("MODEL_RETRAIN_INTERVAL", "300"))

FEATURES = ["heart_rate", "systolic_bp", "diastolic_bp", "spo2",
            "ecg_amplitude", "temperature", "respiratory_rate"]

# ── Prometheus Metrics ────────────────────────────────────────────────────────
events_processed   = Counter("cardiocare_events_total",    "Total vitals events processed")
anomalies_detected = Counter("cardiocare_anomalies_total", "Total anomalies detected")
critical_alerts    = Counter("cardiocare_alerts_total",    "Total critical alerts fired")
model_score        = Gauge("cardiocare_model_score",        "Latest anomaly score")
model_retrain_ts   = Gauge("cardiocare_model_last_retrain", "Last model retrain timestamp")
processing_latency = Histogram("cardiocare_processing_seconds", "Event processing latency",
                                buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5])
current_hr        = Gauge("cardiocare_heart_rate",   "Latest heart rate",  ["patient_id", "bed_number"])
current_spo2      = Gauge("cardiocare_spo2",          "Latest SpO2",        ["patient_id", "bed_number"])
current_systolic  = Gauge("cardiocare_systolic_bp",   "Latest systolic BP", ["patient_id", "bed_number"])
current_news2     = Gauge("cardiocare_news2_score",   "Latest NEWS2 score", ["patient_id", "bed_number"])
current_risk      = Gauge("cardiocare_risk_score",    "Latest risk score",  ["patient_id", "bed_number"])


class AIOpsEngine:
    def __init__(self):
        self.model = IsolationForest(
            n_estimators=100,
            contamination=0.05,
            random_state=42,
            warm_start=False,
        )
        self.training_buffer: deque = deque(maxlen=2000)
        self.model_trained = False
        self.model_lock = threading.Lock()
        self._bootstrap_model()

    def _bootstrap_model(self):
        """Pre-train on synthetic normal data so model is ready immediately."""
        log.info("Bootstrapping model with synthetic normal data...")
        rng = np.random.default_rng(42)
        synthetic = np.column_stack([
            rng.uniform(60,  100, 500),   # heart_rate
            rng.uniform(110, 130, 500),   # systolic_bp
            rng.uniform(70,  85,  500),   # diastolic_bp
            rng.uniform(95,  100, 500),   # spo2
            rng.uniform(0.8, 1.2, 500),   # ecg_amplitude
            rng.uniform(36.1, 37.2, 500), # temperature
            rng.uniform(12,  20,  500),   # respiratory_rate
        ])
        with self.model_lock:
            self.model.fit(synthetic)
            self.model_trained = True
        model_retrain_ts.set(time.time())
        log.info("Bootstrap model trained on %d synthetic samples.", len(synthetic))

    def extract_features(self, event: dict) -> np.ndarray:
        v = event["vitals"]
        return np.array([[v.get(f, 0.0) for f in FEATURES]])

    def score(self, features: np.ndarray) -> float:
        with self.model_lock:
            return float(self.model.decision_function(features)[0])

    def predict(self, features: np.ndarray) -> int:
        with self.model_lock:
            return int(self.model.predict(features)[0])  # 1=normal, -1=anomaly

    def add_to_buffer(self, features: np.ndarray):
        self.training_buffer.append(features[0].tolist())

    def retrain(self):
        if len(self.training_buffer) < 200:
            return
        data = np.array(list(self.training_buffer))
        with self.model_lock:
            self.model = IsolationForest(
                n_estimators=100,
                contamination=0.05,
                random_state=42,
            )
            self.model.fit(data)
            self.model_trained = True
        model_retrain_ts.set(time.time())
        log.info("Model retrained on %d real observations.", len(data))

    def classify_severity(self, vitals: dict, score: float) -> str:
        hr = vitals.get("heart_rate", 70)
        spo2 = vitals.get("spo2", 98)
        sbp = vitals.get("systolic_bp", 120)
        if spo2 < 85 or hr > 180 or hr < 35 or sbp > 185 or sbp < 75:
            return "CRITICAL"
        if score < -0.3:
            return "HIGH"
        if score < ANOMALY_THRESHOLD:
            return "MEDIUM"
        return "LOW"


def wait_for_kafka(bootstrap_servers: str, retries: int = 30):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
            )
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="cardiocare-aiops-engine",
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            log.info("Kafka connected.")
            return producer, consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (%d/%d)...", i + 1, retries)
            time.sleep(5)
    raise RuntimeError("Kafka unavailable after retries.")


def retrain_loop(engine: AIOpsEngine):
    while True:
        time.sleep(MODEL_RETRAIN_INTERVAL)
        log.info("Scheduled model retrain triggered...")
        engine.retrain()


def main():
    log.info("CardioCare-AIOps Engine starting | topic=%s | threshold=%.2f",
             INPUT_TOPIC, ANOMALY_THRESHOLD)
    start_http_server(PROMETHEUS_PORT)
    log.info("Prometheus metrics on port %d", PROMETHEUS_PORT)

    engine = AIOpsEngine()
    producer, consumer = wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    # Background retraining thread
    t = threading.Thread(target=retrain_loop, args=(engine,), daemon=True)
    t.start()

    log.info("Listening on topic: %s", INPUT_TOPIC)

    for message in consumer:
        start = time.perf_counter()
        try:
            event = message.value
            patient_id = event.get("patient_id", "UNKNOWN")
            vitals = event.get("vitals", {})
            bed_number  = event.get("bed_number", "UNKNOWN")
            news2_score = event.get("news2_score", 0)
            features = engine.extract_features(event)
            engine.add_to_buffer(features)

            score = engine.score(features)
            prediction = engine.predict(features)

            model_score.set(score)
            events_processed.inc()

            # Update per-patient gauges
            current_hr.labels(patient_id=patient_id, bed_number=bed_number).set(vitals.get("heart_rate", 0))
            current_spo2.labels(patient_id=patient_id, bed_number=bed_number).set(vitals.get("spo2", 0))
            current_systolic.labels(patient_id=patient_id, bed_number=bed_number).set(vitals.get("systolic_bp", 0))
            current_news2.labels(patient_id=patient_id, bed_number=bed_number).set(news2_score)
            current_risk.labels(patient_id=patient_id, bed_number=bed_number).set(round(abs(score), 4))

            if prediction == -1 or score < ANOMALY_THRESHOLD:
                anomalies_detected.inc()
                severity = engine.classify_severity(vitals, score)

                anomaly_event = {
                    "event_id": event.get("event_id"),
                    "patient_id": patient_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "anomaly_score": round(score, 4),
                    "severity": severity,
                    "vitals": vitals,
                    "ward": event.get("ward", "UNKNOWN"),
                    "model": "IsolationForest",
                    "features_used": FEATURES,
                }
                producer.send(ANOMALY_TOPIC, key=patient_id, value=anomaly_event)

                log.warning("ANOMALY | patient=%s | severity=%s | score=%.4f | "
                            "HR=%.0f | SpO2=%.0f%% | BP=%d/%d",
                            patient_id, severity, score,
                            vitals.get("heart_rate", 0),
                            vitals.get("spo2", 0),
                            vitals.get("systolic_bp", 0),
                            vitals.get("diastolic_bp", 0))

                if severity == "CRITICAL":
                    critical_alerts.inc()
                    alert = {
                        **anomaly_event,
                        "alert_type": "CARDIAC_EMERGENCY",
                        "action_required": "IMMEDIATE_CLINICAL_REVIEW",
                        "triggered_function": "alert-handler-v1",
                    }
                    producer.send(ALERT_TOPIC, key=patient_id, value=alert)
                    log.error("CRITICAL ALERT FIRED | patient=%s | HR=%.0f | SpO2=%.0f%%",
                              patient_id,
                              vitals.get("heart_rate", 0),
                              vitals.get("spo2", 0))

        except Exception as exc:
            log.exception("Error processing event: %s", exc)

        finally:
            elapsed = time.perf_counter() - start
            processing_latency.observe(elapsed)


if __name__ == "__main__":
    main()
