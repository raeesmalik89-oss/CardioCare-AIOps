"""
CardioCare-AIOps — AIOps Anomaly Detection Engine
Author  : Muhammad Raees (raees.malik89@gmail.com)
Course  : Diploma in Artificial Intelligence Operations — EduQual Level 6 (DAIOL6)
Topic   : Topic 7 — Serverless Architectures with Event-Driven AIOps,
          Observability and Security Integration

Purpose:
    Consumes `cardiac.vitals.stream` in real time and applies a hybrid detector:

      1. XGBoost (supervised) — classifies the REAL 187-sample MIT-BIH ECG beat
         carried by each event into one of 5 AAMI arrhythmia classes. This is
         the same model benchmarked offline (services/ml-trainer, 97.27%).
      2. Isolation Forest (unsupervised) — scores the 7-dim vitals vector for
         outliers, with scheduled retraining on the live buffer.
      3. Clinical rules — override both for life-threatening vitals.

    Ensemble = "xgboost + isolationforest + rules". Anomalies are forwarded to
    `cardiac.anomalies.detected`; CRITICAL events trigger the serverless alert
    function via `cardiac.alerts.critical`.

    Real-time replay evaluation:
      Because each replayed MIT-BIH beat carries its original annotated label,
      the engine compares every live prediction to that label and exposes a
      continuously-updating accuracy (cardiocare_xgboost_replay_accuracy, plus a
      sliding-window variant). This is REPLAY / STREAMING / ONLINE evaluation —
      the model is scored live, but the labels come from a previously annotated
      dataset, so it is NOT true production evaluation (real patient streams are
      unlabelled). See docs/LIMITATIONS.md.

    If the XGBoost model is not mounted (e.g. CI), the engine degrades
    gracefully to IsolationForest + rules so the demo never breaks.

Algorithm rationale:
    Isolation Forest suits the streaming regime (mostly-normal, no labels) and
    adapts to baseline drift; XGBoost adds supervised arrhythmia classification
    validated on labelled MIT-BIH data. Together they cover unknown anomalies
    and known arrhythmia morphology.
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
from crypto import decrypt_event, encrypt_event

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
XGBOOST_MODEL_PATH = os.getenv("XGBOOST_MODEL_PATH", "/models/xgboost_ecg_classifier.json")

FEATURES = ["heart_rate", "systolic_bp", "diastolic_bp", "spo2",
            "ecg_amplitude", "temperature", "respiratory_rate"]

# MIT-BIH AAMI classes
CLASS_NAMES = {0: "Normal", 1: "Supraventricular", 2: "Ventricular",
               3: "Fusion", 4: "Unclassifiable"}
# Classes that represent life-threatening morphology
CRITICAL_CLASSES = {2, 3}   # Ventricular, Fusion

# ── Prometheus Metrics ────────────────────────────────────────────────────────
events_processed   = Counter("cardiocare_events_total",    "Total vitals events processed")
anomalies_detected = Counter("cardiocare_anomalies_total", "Total anomalies detected")
critical_alerts    = Counter("cardiocare_alerts_total",    "Total critical alerts fired")
model_score        = Gauge("cardiocare_model_score",        "Latest anomaly score")
model_retrain_ts   = Gauge("cardiocare_model_last_retrain", "Last model retrain timestamp")
processing_latency = Histogram("cardiocare_processing_seconds", "Event processing latency",
                                buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5])
xgb_predictions    = Counter("cardiocare_xgboost_predictions_total",
                             "XGBoost ECG beat classifications", ["predicted_class"])
xgb_correct        = Counter("cardiocare_xgboost_correct_total",
                             "XGBoost predictions matching the replayed ground-truth label")
# ── Real-time REPLAY (streaming) evaluation ──────────────────────────────────
# As real MIT-BIH beats replay through the stream they carry their original
# annotated label. Comparing each live prediction to that label gives a
# continuously-updating accuracy. This is REPLAY / STREAMING / ONLINE evaluation
# (labels originate from a previously annotated dataset), NOT true production
# evaluation (real patient streams are unlabelled). See docs/LIMITATIONS.md.
xgb_evaluated      = Counter("cardiocare_xgboost_evaluated_total",
                             "Beats evaluated against a replayed MIT-BIH ground-truth label")
xgb_replay_acc     = Gauge("cardiocare_xgboost_replay_accuracy",
                           "Cumulative replay-evaluation accuracy (correct / evaluated). "
                           "Replay/streaming evaluation against MIT-BIH labels, not production accuracy.")
xgb_replay_acc_win = Gauge("cardiocare_xgboost_replay_accuracy_window",
                           "Sliding-window replay accuracy over the last N evaluated beats.")
current_hr        = Gauge("cardiocare_heart_rate",   "Latest heart rate",  ["bed_number"])
current_spo2      = Gauge("cardiocare_spo2",          "Latest SpO2",        ["bed_number"])
current_systolic  = Gauge("cardiocare_systolic_bp",   "Latest systolic BP", ["bed_number"])
current_news2     = Gauge("cardiocare_news2_score",   "Latest NEWS2 score", ["bed_number"])
current_risk      = Gauge("cardiocare_risk_score",    "Latest risk score",  ["bed_number"])


def load_xgboost_model(path: str):
    """Load the trained XGBoost classifier, or None if unavailable (fallback)."""
    try:
        import xgboost as xgb
        model = xgb.XGBClassifier()
        model.load_model(path)
        log.info("XGBoost model loaded from %s (live ECG classification ENABLED)", path)
        return model
    except Exception as exc:  # noqa: BLE001 - any failure -> IsolationForest-only
        log.warning("XGBoost model unavailable (%s): %s — running IsolationForest only", path, exc)
        return None


class AIOpsEngine:
    def __init__(self):
        self.model = IsolationForest(
            n_estimators=100, contamination=0.05, random_state=42, warm_start=False,
        )
        self.training_buffer: deque = deque(maxlen=2000)
        self.model_trained = False
        self.model_lock = threading.Lock()
        self.xgb = load_xgboost_model(XGBOOST_MODEL_PATH)
        # Real-time replay-evaluation state (cumulative + sliding window)
        self.replay_correct = 0
        self.replay_total = 0
        self.replay_window: deque = deque(maxlen=int(os.getenv("REPLAY_WINDOW", "500")))
        self._bootstrap_model()

    def _bootstrap_model(self):
        """Pre-train IsolationForest on synthetic normal vitals so it's ready now."""
        log.info("Bootstrapping IsolationForest with synthetic normal data...")
        rng = np.random.default_rng(42)
        synthetic = np.column_stack([
            rng.uniform(60,  100, 500), rng.uniform(110, 130, 500),
            rng.uniform(70,  85,  500), rng.uniform(95,  100, 500),
            rng.uniform(0.8, 1.2, 500), rng.uniform(36.1, 37.2, 500),
            rng.uniform(12,  20,  500),
        ])
        with self.model_lock:
            self.model.fit(synthetic)
            self.model_trained = True
        model_retrain_ts.set(time.time())
        log.info("Bootstrap model trained on %d synthetic samples.", len(synthetic))

    # ── Vitals path (IsolationForest) ──────────────────────────────────────────
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
            self.model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
            self.model.fit(data)
            self.model_trained = True
        model_retrain_ts.set(time.time())
        log.info("IsolationForest retrained on %d real observations.", len(data))

    # ── ECG beat path (XGBoost) ──────────────────────────────────────────────
    def classify_beat(self, beat):
        """Classify a real 187-sample ECG beat. Returns (class:int, confidence:float)
        or (None, None) if XGBoost is unavailable or the beat is malformed."""
        if self.xgb is None or not beat or len(beat) < 187:
            return None, None
        try:
            x = np.array(beat[:187], dtype=np.float32).reshape(1, -1)
            proba = self.xgb.predict_proba(x)[0]
            cls = int(np.argmax(proba))
            return cls, float(proba[cls])
        except Exception as exc:  # noqa: BLE001
            log.warning("XGBoost classification failed: %s", exc)
            return None, None

    # ── Real-time replay (streaming) evaluation ──────────────────────────────
    def record_replay(self, predicted: int, true_label) -> None:
        """Compare a live prediction with the REPLAYED MIT-BIH ground-truth label
        and update the continuously-running accuracy metrics.

        This is replay / streaming / online evaluation: the labels come from a
        previously annotated dataset replayed onto the live stream, so the model
        is scored continuously (every event) rather than in an offline batch.
        It is NOT true production evaluation — real patient streams have no
        ground-truth labels (see docs/LIMITATIONS.md).
        """
        if true_label is None:
            return
        hit = 1 if int(true_label) == int(predicted) else 0
        self.replay_total += 1
        self.replay_correct += hit
        self.replay_window.append(hit)
        xgb_evaluated.inc()
        if hit:
            xgb_correct.inc()
        xgb_replay_acc.set(self.replay_correct / self.replay_total)
        xgb_replay_acc_win.set(sum(self.replay_window) / len(self.replay_window))

    # ── Ensemble severity ────────────────────────────────────────────────────
    def classify_severity(self, vitals: dict, score: float, xgb_class) -> str:
        hr = vitals.get("heart_rate", 70)
        spo2 = vitals.get("spo2", 98)
        sbp = vitals.get("systolic_bp", 120)
        # Clinical rules + supervised arrhythmia morphology both escalate to CRITICAL
        if spo2 < 85 or hr > 180 or hr < 35 or sbp > 185 or sbp < 75:
            return "CRITICAL"
        if xgb_class in CRITICAL_CLASSES:
            return "CRITICAL"
        if xgb_class == 1:           # Supraventricular
            return "HIGH"
        if score < -0.3:
            return "HIGH"
        if xgb_class == 4 or score < ANOMALY_THRESHOLD:   # Unclassifiable / IF outlier
            return "MEDIUM"
        return "LOW"


def wait_for_kafka(bootstrap_servers: str, retries: int = 30):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
            )
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=decrypt_event,
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
    detector_name = ("ensemble:xgboost+isolationforest+rules"
                     if engine.xgb is not None else "isolationforest+rules")
    producer, consumer = wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    t = threading.Thread(target=retrain_loop, args=(engine,), daemon=True)
    t.start()

    log.info("Listening on topic: %s | detector=%s", INPUT_TOPIC, detector_name)

    for message in consumer:
        start = time.perf_counter()
        try:
            event = message.value
            patient_id = event.get("patient_id", "UNKNOWN")
            vitals = event.get("vitals", {})
            bed_number  = event.get("bed_number", "UNKNOWN")
            news2_score = event.get("news2_score", 0)

            # Vitals path (IsolationForest)
            features = engine.extract_features(event)
            engine.add_to_buffer(features)
            score = engine.score(features)
            prediction = engine.predict(features)
            model_score.set(score)
            events_processed.inc()

            # ECG beat path (XGBoost) on the REAL beat, if present
            xgb_class, xgb_conf = engine.classify_beat(event.get("ecg_beat"))
            if xgb_class is not None:
                xgb_predictions.labels(predicted_class=CLASS_NAMES[xgb_class]).inc()
                # Real-time replay evaluation against the replayed MIT-BIH label
                engine.record_replay(xgb_class, event.get("true_label"))

            # Per-bed gauges
            current_hr.labels(bed_number=bed_number).set(vitals.get("heart_rate", 0))
            current_spo2.labels(bed_number=bed_number).set(vitals.get("spo2", 0))
            current_systolic.labels(bed_number=bed_number).set(vitals.get("systolic_bp", 0))
            current_news2.labels(bed_number=bed_number).set(news2_score)
            current_risk.labels(bed_number=bed_number).set(round(abs(score), 4))

            # Anomaly if vitals outlier OR a non-Normal arrhythmia class
            vitals_anomaly = prediction == -1 or score < ANOMALY_THRESHOLD
            beat_anomaly = xgb_class is not None and xgb_class != 0
            if vitals_anomaly or beat_anomaly:
                anomalies_detected.inc()
                severity = engine.classify_severity(vitals, score, xgb_class)

                anomaly_event = {
                    "event_id": event.get("event_id"),
                    "patient_id": patient_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "anomaly_score": round(score, 4),
                    "severity": severity,
                    "vitals": vitals,
                    "ward": event.get("ward", "UNKNOWN"),
                    "model": detector_name,
                    "features_used": FEATURES,
                    "ecg_class": CLASS_NAMES.get(xgb_class) if xgb_class is not None else None,
                    "ecg_confidence": round(xgb_conf, 4) if xgb_conf is not None else None,
                    "true_label_name": event.get("true_label_name"),
                }
                producer.send(ANOMALY_TOPIC, key=patient_id, value=encrypt_event(anomaly_event))

                log.warning("ANOMALY | bed=%s | severity=%s | ecg=%s(%.2f) | score=%.4f | HR=%.0f SpO2=%.0f",
                            bed_number, severity,
                            CLASS_NAMES.get(xgb_class, "n/a"),
                            xgb_conf or 0.0, score,
                            vitals.get("heart_rate", 0), vitals.get("spo2", 0))

                if severity == "CRITICAL":
                    critical_alerts.inc()
                    alert = {
                        **anomaly_event,
                        "alert_type": "CARDIAC_EMERGENCY",
                        "action_required": "IMMEDIATE_CLINICAL_REVIEW",
                        "triggered_function": "alert-handler-v1",
                    }
                    producer.send(ALERT_TOPIC, key=patient_id, value=encrypt_event(alert))
                    log.error("CRITICAL ALERT FIRED | bed=%s | ecg=%s | HR=%.0f SpO2=%.0f",
                              bed_number, CLASS_NAMES.get(xgb_class, "n/a"),
                              vitals.get("heart_rate", 0), vitals.get("spo2", 0))

        except Exception as exc:
            log.exception("Error processing event: %s", exc)
        finally:
            processing_latency.observe(time.perf_counter() - start)


if __name__ == "__main__":
    main()
