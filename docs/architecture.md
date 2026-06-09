# CardioCare-AIOps — System Architecture

## Topic 7: Serverless Architecture with Event-Driven AIOps, Observability & Security

---

## System-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CardioCare-AIOps Platform                           │
│                  (EC2 Instance — Docker Compose Deployment)                 │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    EVENT STREAMING LAYER (Kafka)                    │   │
│  │                                                                     │   │
│  │  cardiac.vitals.stream ──► cardiac.anomalies.detected               │   │
│  │                               └──► cardiac.alerts.critical          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│         ▲                    ▲                         ▲                    │
│         │                    │                         │                    │
│  ┌──────┴──────┐   ┌─────────┴──────────┐   ┌────────┴────────────┐       │
│  │  PRODUCER   │   │   AIOPS ENGINE      │   │  SERVERLESS FUNC    │       │
│  │  (Simulated │   │  Isolation Forest   │   │  Alert Handler      │       │
│  │   ECG Data) │   │  Anomaly Detection  │   │  (Event-triggered)  │       │
│  │  5 patients │   │  Scikit-learn       │   │  Flask + Kafka      │       │
│  │  1 event/s  │   │  Auto-retrain 5min  │   │  HTTP + Webhook     │       │
│  └─────────────┘   └────────────────────┘   └─────────────────────┘       │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      API GATEWAY (FastAPI)                          │   │
│  │   /api/v1/vitals  /api/v1/anomalies  /api/v1/alerts  /metrics       │   │
│  │   JWT Auth (Keycloak) + OPA Policy + OpenTelemetry Tracing          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌──────────────────┐  ┌───────────────────┐  ┌──────────────────────┐    │
│  │ SECURITY LAYER   │  │ OBSERVABILITY     │  │ COMPLIANCE           │    │
│  │ Keycloak (IAM)   │  │ Prometheus        │  │ ISO 27001 A.9/A.16   │    │
│  │ OPA (AuthZ)      │  │ Grafana (viz)     │  │ NIST CSF             │    │
│  │ JWT Bearer Auth  │  │ Loki (logs)       │  │ GDPR Art.32          │    │
│  │ RBAC Roles       │  │ Jaeger (traces)   │  │                      │    │
│  └──────────────────┘  └───────────────────┘  └──────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Network Flow Diagram

```
[Cardiac Devices / Simulator]
        │  POST vitals
        ▼
[Producer Service] ──publish──► [Kafka: cardiac.vitals.stream]
                                         │
                                         │ consume (group: aiops-engine)
                                         ▼
                              [AIOps Engine: IsolationForest]
                                    │              │
                           normal   │              │ anomaly detected
                           (discard)│              ▼
                                    │   [Kafka: cardiac.anomalies.detected]
                                    │              │
                                    │              │ severity=CRITICAL
                                    │              ▼
                                    │   [Kafka: cardiac.alerts.critical]
                                    │              │
                                    │              │ consume (serverless trigger)
                                    │              ▼
                                    │   [Alert Function: handler.py]
                                    │        │ HTTP POST webhook
                                    │        ▼
                                    │   [Nurse Station / Slack / PagerDuty]
                                    │
                                    ▼
                        [Prometheus scrapes /metrics]
                                    │
                                    ▼
                              [Grafana Dashboard]
                              [Loki Log Explorer]
                              [Jaeger Trace View]
```

---

## Data Flow Diagram

```
[ECG Sensor Data]
  heart_rate, systolic_bp, diastolic_bp, spo2, ecg_amplitude, temperature, respiratory_rate
        │
        ▼
[Kafka Topic: cardiac.vitals.stream]
  - 3 partitions, replicated
  - Key: patient_id (ensures ordering per patient)
  - Value: JSON event with timestamp, device_id, ward, vitals
        │
        ▼
[AIOps Engine]
  - Feature extraction: [7 numerical features]
  - IsolationForest scoring: anomaly_score ∈ [-1, 0]
  - Severity classification: CRITICAL | HIGH | MEDIUM | LOW
        │
        ├─ normal ──► metrics only (Prometheus)
        │
        └─ anomaly ──► [Kafka: cardiac.anomalies.detected]
                              │
                       CRITICAL only
                              │
                              ▼
                    [Kafka: cardiac.alerts.critical]
                              │
                              ▼
                    [Alert Function invoked]
                    - Logs alert
                    - Notifies ward
                    - Sends webhook (optional)
                    - Writes to Loki
```

---

## Required Components Mapping

| Exam Requirement          | Implementation                        |
|---------------------------|---------------------------------------|
| Serverless Platform       | OpenFaaS-pattern (alert-function)     |
| Event Streaming           | Apache Kafka (3 topics)               |
| API Framework             | FastAPI                               |
| Observability — Metrics   | Prometheus + Grafana                  |
| Observability — Logs      | Loki + Promtail                       |
| Observability — Traces    | OpenTelemetry → Jaeger                |
| AIOps — ML               | Scikit-learn IsolationForest          |
| Security — IAM            | Keycloak (JWT + OIDC)                 |
| Security — Policy         | Open Policy Agent (OPA)               |
| Security — Testing        | OWASP ZAP (run against :8000)         |
| Standards                 | ISO 27001, NIST CSF, GDPR Art.32      |

---

## Kafka Topics

| Topic                        | Purpose                              | Partitions |
|------------------------------|--------------------------------------|------------|
| `cardiac.vitals.stream`      | Raw sensor events (1/sec per patient)| 3          |
| `cardiac.anomalies.detected` | ML-flagged anomalies                 | 3          |
| `cardiac.alerts.critical`    | Critical alerts → serverless trigger | 1          |
