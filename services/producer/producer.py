"""
CardioCare-AIOps — Cardiac Vitals Producer
Streams simulated real-time ECG and vitals data to Kafka topic: cardiac.vitals.stream
Periodically injects anomalous readings to trigger the AIOps detection pipeline.
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cardiac.vitals.stream")
EMIT_INTERVAL_MS = int(os.getenv("EMIT_INTERVAL_MS", "1000"))
SIMULATE_ANOMALIES = os.getenv("SIMULATE_ANOMALIES", "true").lower() == "true"

PATIENT_IDS = ["PT-001", "PT-002", "PT-003", "PT-004", "PT-005"]

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

# Simulated ECG waveform (P-QRS-T simplified)
def ecg_waveform(t: float, amplitude: float = 1.0) -> float:
    """Generate a simplified ECG waveform value at time t."""
    cycle = t % 1.0
    if 0.1 <= cycle < 0.15:      # P wave
        return amplitude * 0.25 * math.sin(math.pi * (cycle - 0.1) / 0.05)
    elif 0.3 <= cycle < 0.35:    # QRS complex
        if cycle < 0.32:
            return -amplitude * 0.15
        elif cycle < 0.33:
            return amplitude * 1.0
        else:
            return -amplitude * 0.25
    elif 0.4 <= cycle < 0.55:    # T wave
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


def build_event(patient_id: str, is_anomaly: bool, seq: int) -> dict:
    t = time.time()
    vitals = sample_anomaly() if is_anomaly else sample_normal()
    ecg_val = round(ecg_waveform(t % 1.0, vitals["ecg_amplitude"]), 4)
    return {
        "event_id": f"{patient_id}-{seq:08d}",
        "patient_id": patient_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sequence": seq,
        "vitals": vitals,
        "ecg_sample": ecg_val,
        "device_id": f"DEVICE-{patient_id}",
        "ward": random.choice(["ICU", "CCU", "CARDIAC-OT", "GENERAL"]),
        "is_simulated_anomaly": is_anomaly,
    }


def wait_for_kafka(bootstrap_servers: str, retries: int = 30) -> KafkaProducer:
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
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
    log.info("Topic: %s | Interval: %dms | Anomaly injection: %s",
             KAFKA_TOPIC, EMIT_INTERVAL_MS, SIMULATE_ANOMALIES)

    producer = wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)
    seq = 0
    anomaly_counter = 0

    while True:
        patient_id = random.choice(PATIENT_IDS)
        # Inject anomaly roughly every 20 events per patient if enabled
        is_anomaly = SIMULATE_ANOMALIES and (random.random() < 0.05)

        if is_anomaly:
            anomaly_counter += 1

        event = build_event(patient_id, is_anomaly, seq)
        producer.send(KAFKA_TOPIC, key=patient_id, value=event)

        if is_anomaly:
            log.warning("ANOMALY INJECTED | patient=%s HR=%.0f SpO2=%.0f BP=%d/%d",
                        patient_id,
                        event["vitals"]["heart_rate"],
                        event["vitals"]["spo2"],
                        event["vitals"]["systolic_bp"],
                        event["vitals"]["diastolic_bp"])
        elif seq % 30 == 0:
            log.info("Streaming | seq=%d | patient=%s | HR=%.0f | SpO2=%.0f%%",
                     seq, patient_id,
                     event["vitals"]["heart_rate"],
                     event["vitals"]["spo2"])

        seq += 1
        time.sleep(EMIT_INTERVAL_MS / 1000.0)


if __name__ == "__main__":
    main()
