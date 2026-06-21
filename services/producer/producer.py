"""
CardioCare-AIOps — Cardiac Vitals Stream Producer
Author  : Muhammad Raees (raees.malik89@gmail.com)
Topic   : Topic 7 — Serverless Architectures with Event-Driven AIOps,
          Observability and Security Integration

Purpose:
    Streams cardiac events to the Kafka topic `cardiac.vitals.stream`.

    Two data modes (DATA_SOURCE env):
      - "real" (default): replays REAL ECG heartbeats from the MIT-BIH
        Arrhythmia dataset (Kaggle: shayanfazeli/heartbeat). Each event carries
        the real 187-sample beat plus its ground-truth arrhythmia label, and a
        vitals layer derived from that label so the NEWS2 / IsolationForest /
        dashboard path still works. The downstream aiops-engine classifies the
        real beat live with the trained XGBoost model.
      - "simulated": generates synthetic vitals (used when the MIT-BIH CSV is
        not mounted, e.g. CI / quick local runs) so the demo never breaks.

    HONEST NOTE: no public dataset pairs an ECG waveform with matching vitals
    for the same patient-moment. The ECG beats are REAL (MIT-BIH); the vitals
    are derived from the real beat's label. We do not claim the vitals are
    recorded from the same patient as the beat.

Design rationale:
    A dedicated producer decouples ingestion from processing — a core
    event-driven serverless principle. In production this is replaced by real
    device connectors (HL7 FHIR / MQTT); the downstream pipeline is identical.
"""

import os
import json
import time
import random
import logging
import math
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from crypto import encrypt_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cardiac.vitals.stream")
EMIT_INTERVAL_MS = int(os.getenv("EMIT_INTERVAL_MS", "1000"))
SIMULATE_ANOMALIES = os.getenv("SIMULATE_ANOMALIES", "true").lower() == "true"

# Data mode: "real" replays MIT-BIH beats; "simulated" generates synthetic vitals.
DATA_SOURCE = os.getenv("DATA_SOURCE", "real").lower()
MITBIH_PATH = os.getenv("MITBIH_PATH", "/data/mitbih_test.csv")

PATIENT_IDS = ["PT-001", "PT-002", "PT-003", "PT-004", "PT-005",
               "PT-006", "PT-007", "PT-008", "PT-009"]

# MIT-BIH AAMI classes (column 187 of each row)
CLASS_NAMES = {0: "Normal", 1: "Supraventricular", 2: "Ventricular",
               3: "Fusion", 4: "Unclassifiable"}

# Normal physiological ranges
NORMAL = {
    "heart_rate":     (60,  100),
    "systolic_bp":    (110, 130),
    "diastolic_bp":   (70,  85),
    "spo2":           (95,  100),
    "ecg_amplitude":  (0.8, 1.2),
    "temperature":    (36.1, 37.2),
    "respiratory_rate": (12, 20),
}

# Anomalous ranges (cardiac emergency)
ANOMALY = {
    "heart_rate":     [(30, 45), (160, 200)],  # bradycardia or tachycardia
    "systolic_bp":    [(160, 200), (70, 85)],  # hypertensive crisis or hypotension
    "diastolic_bp":   [(100, 130), (40, 55)],
    "spo2":           [(75, 90)],              # hypoxemia
    "ecg_amplitude":  [(0.1, 0.3), (2.5, 3.5)],
    "temperature":    [(38.5, 40.5)],
    "respiratory_rate": [(30, 45), (4, 8)],
}

# ICU bed assignment — maps patient ID to physical bed number
BED_MAP = {
    "PT-001": "BED-01",
    "PT-002": "BED-02",
    "PT-003": "BED-03",
    "PT-004": "BED-04",
    "PT-005": "BED-05",
    "PT-006": "BED-06",
    "PT-007": "BED-07",
    "PT-008": "BED-08",
    "PT-009": "BED-09",
}


def ecg_waveform(t: float, amplitude: float = 1.0) -> float:
    """Simplified P-QRS-T ECG value (used only in simulated fallback)."""
    cycle = t % 1.0
    if 0.1 <= cycle < 0.15:
        return amplitude * 0.25 * math.sin(math.pi * (cycle - 0.1) / 0.05)
    elif 0.3 <= cycle < 0.35:
        if cycle < 0.32:
            return -amplitude * 0.15
        elif cycle < 0.33:
            return amplitude * 1.0
        else:
            return -amplitude * 0.25
    elif 0.4 <= cycle < 0.55:
        return amplitude * 0.35 * math.sin(math.pi * (cycle - 0.4) / 0.15)
    return 0.0


def sample_normal() -> dict:
    return {k: round(random.uniform(v[0], v[1]), 2) for k, v in NORMAL.items()}


def sample_anomaly() -> dict:
    vitals = {}
    for key, ranges in ANOMALY.items():
        r = random.choice(ranges)
        vitals[key] = round(random.uniform(r[0], r[1]), 2)
    return vitals


def vitals_for_label(label: int) -> dict:
    """Derive a plausible vitals layer from the REAL beat's arrhythmia label.

    Label 0 (Normal) -> normal vitals.
    Labels 1-4 (arrhythmias) -> anomalous vitals, so NEWS2 / IsolationForest
    and the dashboards reflect the real beat being abnormal.
    """
    return sample_normal() if label == 0 else sample_anomaly()


def compute_news2(vitals: dict) -> int:
    return (
        (3 if vitals["heart_rate"] >= 131 or vitals["heart_rate"] <= 40 else
         2 if vitals["heart_rate"] >= 111 else
         1 if vitals["heart_rate"] >= 91 else 0) +
        (3 if vitals["spo2"] <= 91 else
         2 if vitals["spo2"] <= 93 else
         1 if vitals["spo2"] <= 95 else 0) +
        (3 if vitals["systolic_bp"] <= 90 else
         2 if vitals["systolic_bp"] <= 100 else
         1 if vitals["systolic_bp"] <= 110 else
         3 if vitals["systolic_bp"] >= 220 else 0) +
        (3 if vitals["respiratory_rate"] >= 25 else
         2 if vitals["respiratory_rate"] >= 21 else
         1 if vitals["respiratory_rate"] <= 11 else 0)
    )


def load_mitbih_beats(path: str):
    """Load real MIT-BIH beats: returns (beats list[187 floats], labels list[int])
    or None if the CSV is unavailable (caller falls back to simulation)."""
    try:
        import pandas as pd
        df = pd.read_csv(path, header=None)
        beats = df.iloc[:, :187].values.astype(float).tolist()
        labels = df.iloc[:, 187].values.astype(int).tolist()
        log.info("Loaded %d REAL MIT-BIH beats from %s", len(beats), path)
        return beats, labels
    except Exception as exc:  # noqa: BLE001 - any failure -> simulation fallback
        log.warning("Could not load MIT-BIH data (%s): %s", path, exc)
        return None


def build_real_event(beats, labels, seq: int) -> dict:
    """Build an event from a REAL MIT-BIH beat + label-derived vitals."""
    idx = random.randrange(len(beats))
    beat = [round(float(x), 4) for x in beats[idx]]
    label = int(labels[idx])
    label_name = CLASS_NAMES.get(label, "Unknown")
    patient_id = PATIENT_IDS[idx % len(PATIENT_IDS)]
    vitals = vitals_for_label(label)
    return {
        "event_id": f"{patient_id}-{seq:08d}",
        "patient_id": patient_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sequence": seq,
        "vitals": vitals,
        "ecg_beat": beat,                 # REAL 187-sample MIT-BIH heartbeat
        "true_label": label,              # ground-truth arrhythmia class (0-4)
        "true_label_name": label_name,
        "ecg_sample": beat[len(beat) // 2],
        "device_id": f"DEVICE-{patient_id}",
        "ward": "ICU",
        "bed_number": BED_MAP[patient_id],
        "news2_score": compute_news2(vitals),
        "data_source": "mitbih",
    }


def build_simulated_event(patient_id: str, is_anomaly: bool, seq: int) -> dict:
    """Fallback event with synthetic vitals (no real beat available)."""
    t = time.time()
    vitals = sample_anomaly() if is_anomaly else sample_normal()
    return {
        "event_id": f"{patient_id}-{seq:08d}",
        "patient_id": patient_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sequence": seq,
        "vitals": vitals,
        "ecg_sample": round(ecg_waveform(t % 1.0, vitals["ecg_amplitude"]), 4),
        "device_id": f"DEVICE-{patient_id}",
        "ward": "ICU",
        "bed_number": BED_MAP[patient_id],
        "news2_score": compute_news2(vitals),
        "data_source": "simulated",
        "is_simulated_anomaly": is_anomaly,
    }


def wait_for_kafka(bootstrap_servers: str, retries: int = 30) -> KafkaProducer:
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
            )
            log.info("Connected to Kafka at %s", bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retry %d/%d...", i + 1, retries)
            time.sleep(5)
    raise RuntimeError("Cannot connect to Kafka after %d retries" % retries)


def main():
    log.info("CardioCare-AIOps Producer starting...")

    real = load_mitbih_beats(MITBIH_PATH) if DATA_SOURCE == "real" else None
    if real:
        beats, labels = real
        log.info("DATA MODE: REAL MIT-BIH beats (%d) -> %s", len(beats), KAFKA_TOPIC)
    else:
        beats = labels = None
        log.info("DATA MODE: SIMULATED vitals -> %s", KAFKA_TOPIC)

    log.info("Interval: %dms | Anomaly injection: %s", EMIT_INTERVAL_MS, SIMULATE_ANOMALIES)
    producer = wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)
    seq = 0

    while True:
        if beats:
            event = build_real_event(beats, labels, seq)
            is_abnormal = event["true_label"] != 0
        else:
            patient_id = random.choice(PATIENT_IDS)
            is_abnormal = SIMULATE_ANOMALIES and (random.random() < 0.05)
            event = build_simulated_event(patient_id, is_abnormal, seq)

        producer.send(KAFKA_TOPIC, key=event["patient_id"], value=encrypt_event(event))

        if is_abnormal:
            log.warning("ABNORMAL | bed=%s | label=%s | HR=%.0f SpO2=%.0f",
                        event["bed_number"],
                        event.get("true_label_name", "simulated"),
                        event["vitals"]["heart_rate"],
                        event["vitals"]["spo2"])
        elif seq % 30 == 0:
            log.info("Streaming | seq=%d | bed=%s | src=%s | HR=%.0f | SpO2=%.0f%%",
                     seq, event["bed_number"], event["data_source"],
                     event["vitals"]["heart_rate"], event["vitals"]["spo2"])

        seq += 1
        time.sleep(EMIT_INTERVAL_MS / 1000.0)


if __name__ == "__main__":
    main()
