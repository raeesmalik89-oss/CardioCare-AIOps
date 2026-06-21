# CardioCare-AIOps — Architecture Documentation

**Author:** Muhammad Raees (raees.malik89@gmail.com)
**Topic:** Topic 7 — Designing Serverless Architectures with Event-Driven AIOps, Observability, and Security Integration

---

## 1. System-Level Architecture Diagram

The diagram below shows all components of the CardioCare-AIOps platform and how they interact.  I have organised the system into five horizontal layers to make the separation of concerns visible.

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                         CardioCare-AIOps Platform                            ║
║          (Deployed on AWS EC2 via Docker Compose — single-node demo)         ║
║                                                                               ║
║  ┌─────────────────────────────────────────────────────────────────────────┐ ║
║  │  LAYER 1 — DATA INGESTION                                               │ ║
║  │  Producer Service                                                        │ ║
║  │  • Replays REAL MIT-BIH ECG beats across 9 ICU beds at 1 event/second   │ ║
║  │  • Each beat carries its ground-truth AAMI label + derived vitals layer │ ║
║  │  • In production: replaced by IoT/HL7 FHIR device connectors            │ ║
║  └────────────────────────────┬────────────────────────────────────────────┘ ║
║                               │ publish (key=patient_id)                      ║
║  ┌─────────────────────────────▼────────────────────────────────────────────┐ ║
║  │  LAYER 2 — EVENT STREAMING (Apache Kafka)                               │ ║
║  │                                                                          │ ║
║  │  cardiac.vitals.stream  ──►  cardiac.anomalies.detected                 │ ║
║  │         (3 partitions)              (3 partitions)                       │ ║
║  │                                          └──►  cardiac.alerts.critical   │ ║
║  │                                                     (1 partition)        │ ║
║  └──────────┬──────────────────────────────────────────────────────────────┘ ║
║             │ consume                                                         ║
║  ┌──────────▼──────────────────────────────────────────────────────────────┐ ║
║  │  LAYER 3 — AIOPS PROCESSING                                             │ ║
║  │                                                                          │ ║
║  │  AIOps Engine (detector.py)                                              │ ║
║  │  • Extracts 7-feature vector from each event                            │ ║
║  │  • Scores with IsolationForest (sklearn)                                │ ║
║  │  • Classifies severity: CRITICAL / HIGH / MEDIUM / LOW                  │ ║
║  │  • Auto-retrains on sliding 2000-event buffer every 5 minutes           │ ║
║  │  • Exposes Prometheus /metrics on port 8001                             │ ║
║  │                                                                          │ ║
║  │  Alert Function (handler.py) — Serverless FaaS pattern                  │ ║
║  │  • Triggered by cardiac.alerts.critical (event-driven invocation)       │ ║
║  │  • Also callable via HTTP POST (OpenFaaS-compatible endpoint)           │ ║
║  │  • Stateless, single-purpose, independently scalable                    │ ║
║  └──────────┬──────────────────────────────────────────────────────────────┘ ║
║             │                                                                 ║
║  ┌──────────▼──────────────────────────────────────────────────────────────┐ ║
║  │  LAYER 4 — API & SECURITY                                               │ ║
║  │                                                                          │ ║
║  │  FastAPI Gateway (:8000)                                                 │ ║
║  │  • /api/v1/vitals   /api/v1/anomalies   /api/v1/alerts                  │ ║
║  │  • OpenTelemetry traces exported to Jaeger                              │ ║
║  │                                                                          │ ║
║  │  Keycloak (:8080) ──► JWT validation ──► OPA (:8181) ──► RBAC decision │ ║
║  │  Roles: cardiologist, doctor, nurse, analyst                            │ ║
║  └──────────┬──────────────────────────────────────────────────────────────┘ ║
║             │                                                                 ║
║  ┌──────────▼──────────────────────────────────────────────────────────────┐ ║
║  │  LAYER 5 — OBSERVABILITY                                                │ ║
║  │                                                                          │ ║
║  │  Prometheus (:9090) ── scrapes ──► all /metrics endpoints               │ ║
║  │  Grafana (:3000)    ── queries ──► Prometheus + Loki + Jaeger           │ ║
║  │  Loki (:3100)       ── ingests ──► container logs via Promtail          │ ║
║  │  Jaeger (:16686)    ── ingests ──► OTLP traces from FastAPI             │ ║
║  └─────────────────────────────────────────────────────────────────────────┘ ║
╚═══════════════════════════════════════════════════════════════════════════════╝
```

---

## 2. Network Flow Diagram

This diagram traces how a single cardiac event travels across the network from the moment it is produced to the moment a clinical alert is raised.

```
[Patient Device / Simulator]
        │
        │  JSON over TCP (Kafka producer protocol)
        ▼
[Producer Container] ──► [Kafka Broker :29092]
                                │
                         Topic: cardiac.vitals.stream
                         Partition: hash(patient_id) % 3
                                │
                         Consumer Group: cardiocare-aiops-engine
                                │
                         ┌──────▼──────────────────────────────────┐
                         │  AIOps Engine                           │
                         │  score = model.decision_function(x)[0]  │
                         │  if score < threshold:                  │
                         │      classify_severity(vitals, score)   │
                         └──────┬──────────────────────────────────┘
                                │                    │
                    score ≥ 0   │                    │  score < threshold
                    (normal)    │                    ▼
                    discard ◄───┘        Topic: cardiac.anomalies.detected
                                                     │
                                         severity == CRITICAL only
                                                     │
                                                     ▼
                                         Topic: cardiac.alerts.critical
                                                     │
                                         Consumer Group: cardiocare-alert-function
                                                     │
                                                     ▼
                                         [Alert Function Container]
                                           HTTP POST to webhook (optional)
                                           Write to in-memory alert log
                                           Log line picked up by Promtail → Loki
                                                     │
                                                     ▼
                                         [Nurse Station Dashboard / Grafana]
```

---

## 3. Data Flow Diagram

This diagram shows how data is structured and transformed at each stage.

```
STAGE 1 — RAW SENSOR EVENT
──────────────────────────
{
  "event_id":   "PT-004-00000034",
  "patient_id": "PT-004",
  "timestamp":  "2026-06-09T10:23:35Z",
  "vitals": {
    "heart_rate":       187.0,     ← FEATURE 1
    "systolic_bp":      192.0,     ← FEATURE 2
    "diastolic_bp":     110.0,     ← FEATURE 3
    "spo2":              79.0,     ← FEATURE 4
    "ecg_amplitude":      2.8,     ← FEATURE 5
    "temperature":       38.9,     ← FEATURE 6
    "respiratory_rate":  34.0      ← FEATURE 7
  },
  "ecg_sample": 0.9421,
  "ward": "ICU",
  "device_id": "DEVICE-PT-004"
}

         │
         ▼  Feature extraction + IsolationForest scoring

STAGE 2 — ANOMALY EVENT
────────────────────────
{
  "event_id":     "PT-004-00000034",
  "patient_id":   "PT-004",
  "timestamp":    "2026-06-09T10:23:35Z",
  "anomaly_score": -0.7821,             ← Isolation Forest decision score
  "severity":     "CRITICAL",           ← Classified: score < -0.5 + clinical rules
  "vitals":       { ...same as above... },
  "model":        "IsolationForest",
  "features_used": ["heart_rate", "systolic_bp", "diastolic_bp", "spo2",
                    "ecg_amplitude", "temperature", "respiratory_rate"]
}

         │  severity == CRITICAL
         ▼

STAGE 3 — CRITICAL ALERT (triggers serverless function)
────────────────────────────────────────────────────────
{
  ...all anomaly fields above...,
  "alert_type":       "CARDIAC_EMERGENCY",
  "action_required":  "IMMEDIATE_CLINICAL_REVIEW",
  "triggered_function": "alert-handler-v1"
}

         │
         ▼  handle_alert() executes

STAGE 4 — FUNCTION RESPONSE
────────────────────────────
{
  "function":   "cardiocare-alert-handler-v1",
  "status":     "executed",
  "notification": {
    "type":       "CARDIAC_EMERGENCY",
    "message":    "CARDIAC ALERT: Patient PT-004 in ICU. HR=187, SpO2=79%, BP=192/110",
    "escalation": "CALL_CODE_BLUE",
    "iso27001_ref": "A.16.1.5"
  },
  "actions_taken": ["alert_logged", "nurse_station_notified", "ehr_flagged"]
}
```

---

## 4. Key Design Decisions

### Why Kafka Instead of RabbitMQ or NATS

Kafka was chosen over RabbitMQ and NATS because it provides a durable, replayable log.  In a clinical context this is important: if the AIOps engine restarts (e.g. model retraining), it can replay events from its last committed offset rather than losing data.  RabbitMQ deletes messages after delivery; NATS JetStream would be a valid alternative but adds Kubernetes dependency.

### Why Isolation Forest Instead of a Rule-Based System

A threshold rule such as "alert if HR > 150" ignores the clinical context.  A patient in post-operative recovery may have normal HR of 140; the same value in a resting ICU patient is alarming.  Isolation Forest captures multivariate normality — it alerts when the *combination* of features is unusual, not just a single value.  The model updates automatically as more real observations arrive, adapting to individual patient baselines.

### Why a Separate Alert Function Container

The alert handler is deliberately separated from the AIOps engine to demonstrate the serverless/FaaS pattern required by the exam topic.  It is stateless (no database dependency), single-purpose, and can be scaled horizontally by adding more replicas without touching the detection engine.  The HTTP endpoint makes it compatible with any FaaS platform (OpenFaaS, Knative, AWS Lambda) without code changes.

---

## 5. Standards Alignment

> **Alignment, not certified compliance.** The table below maps design choices to the
> *principles* of each standard for academic purposes. No certifying audit, formal risk
> assessment or GDPR DPIA has been performed. See [Scope & Limitations](LIMITATIONS.md).
> HIPAA is referenced only because the technical safeguards were designed using HIPAA 45
> CFR §164.312 principles — the MIT-BIH dataset is de-identified public research data to
> which HIPAA does not apply.

| Standard | Specific Control | Alignment in This Project |
|---|---|---|
| ISO 27001:2022 | A.9.4 — Application access control | Keycloak JWT authentication + OPA RBAC policy |
| ISO 27001:2022 | A.16.1.5 — Incident management | Alert function escalation path and audit log |
| ISO 27001:2022 | A.12.4 — Logging and monitoring | Loki aggregates all container logs; Promtail ships them |
| NIST CSF 2.0 | PR.AC-4 — Access permissions | OPA `cardiac_policy.rego` enforces least-privilege |
| NIST CSF 2.0 | DE.CM-7 — Monitoring | Prometheus anomaly and alert counters visible in Grafana |
| GDPR Art.32 | Security of processing | JWT bearer auth, OPA policy, audit trail in Loki |

---

## 6. Detection Strategy (Hybrid)

Layer 3 runs a **hybrid** detector: the unsupervised Isolation Forest scores the 7-feature
vitals vector, clinical rules override it for life-threatening values, and a supervised
**XGBoost** classifier labels each real MIT-BIH ECG beat live into 5 AAMI classes. XGBoost
is trained and validated offline (held-out test accuracy 97.27%) and applied live in the
ensemble. The Isolation Forest path is unsupervised and therefore has no labelled benchmark
— its statistical effectiveness is an open evaluation gap (see [Scope & Limitations](LIMITATIONS.md)).

## 7. Scope & Limitations

This is a **single-node demo** deployed on one AWS EC2 host via Docker Compose. Every
component (Kafka, Keycloak, OPA, Grafana, Prometheus, application services) is a single
instance with no HA/failover, and EC2 is a deployment target rather than an IaC-managed
architecture. The project is an event-driven monitoring platform with AIOps components, not
a full AIOps suite. The complete, honest scope statement is in
[docs/LIMITATIONS.md](LIMITATIONS.md).

---

*Muhammad Raees — CardioCare-AIOps Architecture Document — June 2026*
