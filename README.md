# CardioCare-AIOps

> **Student:** Muhammad Raees | raees.malik89@gmail.com
> **Programme:** Diploma in Artificial Intelligence Operations — EduQual Level 6 (DAIOL6)
> **Examination Topic:** Topic 7 — Designing Serverless Architectures with Event-Driven AIOps, Observability, and Security Integration

---

## What This Project Does and Why I Built It This Way

CardioCare-AIOps is a fully event-driven platform that monitors simulated cardiac patient vitals in real time, detects physiological anomalies using machine learning, and triggers automated serverless responses when a critical condition is identified.

I chose the cardiac monitoring domain because it creates a natural, high-stakes justification for each architectural decision the exam topic asks me to demonstrate:

- **Why serverless?** A cardiac alert function must be instantaneous, isolated, and independently scalable — exactly the properties a serverless FaaS model provides.
- **Why event-driven?** Vitals arrive continuously from multiple patients at irregular intervals. A Kafka event stream handles backpressure, ordering per patient, and fan-out to multiple consumers more reliably than polling an API.
- **Why AIOps?** Rule-based thresholds (e.g. HR > 100) produce too many false positives. An Isolation Forest model learns the normal baseline for each patient and flags statistically unusual combinations of vitals — a more clinically meaningful signal.

---

## Architecture at a Glance

```
[Cardiac Device / Simulator]
        │  1 event/second per patient (5 patients)
        ▼
[Producer Service]  ──publish──►  Kafka: cardiac.vitals.stream
                                          │
                                          │ consume
                                          ▼
                               [AIOps Engine — IsolationForest]
                                      │              │
                             normal   │              │ anomaly detected
                                      │              ▼
                                      │   Kafka: cardiac.anomalies.detected
                                      │              │
                                      │         CRITICAL only
                                      │              ▼
                                      │   Kafka: cardiac.alerts.critical
                                      │              │
                                      │              │ event-triggers function
                                      │              ▼
                                      │   [Serverless Alert Handler]
                                      │       notifies ward, logs, webhooks
                                      │
                                      ▼
                          [Prometheus ← FastAPI ← OPA ← Keycloak]
                                      │
                                      ▼
                            [Grafana | Loki | Jaeger]
```

### Kafka Topic Design

| Topic | Role | Partitions |
|---|---|---|
| `cardiac.vitals.stream` | Raw sensor events, keyed by `patient_id` | 3 |
| `cardiac.anomalies.detected` | ML-scored anomalies with severity label | 3 |
| `cardiac.alerts.critical` | CRITICAL events only — triggers serverless function | 1 |

Partitioning by `patient_id` preserves event ordering per patient, which matters for sequential ECG analysis.

---

## Technology Choices and Justifications

| Exam Requirement | Technology I Used | Why This Choice |
|---|---|---|
| Serverless platform | Custom FaaS pattern (OpenFaaS-compatible) | OpenFaaS runs on Docker without Kubernetes, practical on a single EC2 node |
| Event streaming | Apache Kafka | Industry-standard, supports replay, exactly-once semantics, partition-based ordering |
| API framework | FastAPI | Native async, auto-generated OpenAPI docs, integrates with OpenTelemetry natively |
| Metrics | Prometheus + Grafana | De-facto standard; pre-built dashboard provisioned automatically |
| Logs | Loki + Promtail | Unified log stack with Grafana; label-based querying matches my container naming |
| Traces | OpenTelemetry → Jaeger | Vendor-neutral instrumentation; traces show latency across API → OPA → Keycloak |
| AIOps / ML | Scikit-learn IsolationForest | Unsupervised, low-labelled-data requirement, proven for multivariate anomaly detection |
| Identity management | Keycloak | Open-source OIDC/OAuth2 provider; realm export enables reproducible deployment |
| Policy enforcement | Open Policy Agent (OPA) | Rego policies are auditable, version-controlled, and ISO 27001 A.9.4 aligned |

---

## Deploy on EC2 — 4 Steps via MobaXterm

**Recommended EC2 instance:** `t3.large` (8 GB RAM).
`t3.medium` (4 GB) works but will be slow on first startup while images pull.

**Security Group — open these inbound ports:**
`3000` (Grafana) · `8000` (FastAPI) · `8080` (Keycloak) · `8085` (Kafka UI) · `9090` (Prometheus) · `16686` (Jaeger) · `5000` (Alert function)

---

### Step 1 — Connect in MobaXterm

Open a new SSH session:
- **Host:** your EC2 public IP
- **Username:** `ec2-user` (Amazon Linux) or `ubuntu` (Ubuntu 22.04)
- **Key:** your `.pem` file

---

### Step 2 — Clone and Configure

```bash
git clone https://github.com/raeesmalik89-oss/CardioCare-AIOps.git
cd CardioCare-AIOps
cp .env.example .env
nano .env          # set EC2_PUBLIC_IP to your instance's public IPv4
```

---

### Step 3 — Bootstrap (installs Docker if needed)

```bash
chmod +x setup.sh
./setup.sh
```

Expected output:
```
  ╔═══════════════════════════════════════════╗
  ║      CardioCare-AIOps — Setup Script      ║
  ╚═══════════════════════════════════════════╝

[OK] Docker already installed: Docker version 24.0.7
[OK] Docker Compose already installed: Docker Compose version v2.23.0
[OK] EC2 public IP set: 54.123.45.67
[INFO] Pre-pulling Docker images...
[OK] Setup complete!
```

---

### Step 4 — Launch the Full Platform

```bash
docker compose up -d
```

Expected output:
```
[+] Running 16/16
 ✔ Network cardiocare-net              Created
 ✔ Container zookeeper                 Started
 ✔ Container kafka                     Started
 ✔ Container kafka-init                Started   ← creates 3 topics
 ✔ Container kafka-ui                  Started
 ✔ Container cardiocare-producer       Started
 ✔ Container cardiocare-aiops-engine   Started
 ✔ Container cardiocare-alert-fn       Started
 ✔ Container cardiocare-api            Started
 ✔ Container cardiocare-keycloak       Started
 ✔ Container cardiocare-opa            Started
 ✔ Container cardiocare-prometheus     Started
 ✔ Container cardiocare-grafana        Started
 ✔ Container cardiocare-loki           Started
 ✔ Container cardiocare-promtail       Started
 ✔ Container cardiocare-jaeger         Started
```

---

## Observing the Live Pipeline

### AIOps Engine streaming output

```bash
docker compose logs -f cardiocare-aiops-engine
```

```
2026-06-09 10:23:01 [AIOPS-ENGINE] INFO  Bootstrap model trained on 500 synthetic samples.
2026-06-09 10:23:02 [AIOPS-ENGINE] INFO  Kafka connected. Listening on: cardiac.vitals.stream
2026-06-09 10:23:03 [AIOPS-ENGINE] INFO  Streaming | seq=0 | patient=PT-001 | HR=72 | SpO2=98%
2026-06-09 10:23:04 [AIOPS-ENGINE] INFO  Streaming | seq=1 | patient=PT-003 | HR=68 | SpO2=99%
2026-06-09 10:23:07 [AIOPS-ENGINE] WARNING  ANOMALY | patient=PT-004 | severity=CRITICAL | score=-0.7821 | HR=187 | SpO2=79% | BP=192/110
2026-06-09 10:23:07 [AIOPS-ENGINE] ERROR    CRITICAL ALERT FIRED | patient=PT-004 | HR=187 | SpO2=79%
2026-06-09 10:23:15 [AIOPS-ENGINE] WARNING  ANOMALY | patient=PT-001 | severity=HIGH | score=-0.3412 | HR=156 | SpO2=88%
2026-06-09 10:28:02 [AIOPS-ENGINE] INFO  Model retrained on 287 real observations.
```

### Serverless function execution

```bash
docker compose logs -f cardiocare-alert-fn
```

```
2026-06-09 10:23:07 [ALERT-FUNCTION] WARNING FUNCTION EXECUTED | PT-004 | severity=CRITICAL | ward=ICU
2026-06-09 10:23:07 [ALERT-FUNCTION] WARNING CARDIAC ALERT: Patient PT-004 in ICU. HR=187, SpO2=79%, BP=192/110
2026-06-09 10:23:07 [ALERT-FUNCTION] WARNING Action: CALL_CODE_BLUE | ISO27001: A.16.1.5
```

### Kafka live consumer (shows raw event stream)

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cardiac.vitals.stream \
  --property print.key=true
```

```
PT-002  {"event_id":"PT-002-00000031","patient_id":"PT-002","timestamp":"2026-06-09T10:23:34Z","vitals":{"heart_rate":76.4,"systolic_bp":118.2,"diastolic_bp":74.1,"spo2":98.0,"ecg_amplitude":1.03,"temperature":36.7,"respiratory_rate":15},"ward":"CCU"}
PT-004  {"event_id":"PT-004-00000034","patient_id":"PT-004","timestamp":"2026-06-09T10:23:35Z","vitals":{"heart_rate":187.0,"systolic_bp":192.0,"diastolic_bp":110.0,"spo2":79.0,"ecg_amplitude":2.8,"temperature":38.9,"respiratory_rate":34},"ward":"ICU","is_simulated_anomaly":true}
```

---

## Access URLs

| Service | URL | Credentials |
|---|---|---|
| Grafana (pre-built dashboard) | `http://EC2_IP:3000` | admin / CardioCare@2024 |
| FastAPI Swagger UI | `http://EC2_IP:8000/docs` | open |
| Kafka UI (topic browser) | `http://EC2_IP:8085` | open |
| Keycloak Admin | `http://EC2_IP:8080` | admin / CardioCare@2024 |
| Prometheus | `http://EC2_IP:9090` | open |
| Jaeger Traces | `http://EC2_IP:16686` | open |
| Alert Function Logs | `http://EC2_IP:5000/alerts` | open |

---

## REST API Quick Verification

```bash
# System status — confirms all components
curl http://localhost:8000/api/v1/system/status | python3 -m json.tool

# Latest vitals for all patients
curl http://localhost:8000/api/v1/vitals/latest

# Critical anomalies only
curl "http://localhost:8000/api/v1/anomalies?severity=CRITICAL"

# Fired alerts
curl http://localhost:8000/api/v1/alerts

# Prometheus metrics (scraped by Grafana)
curl http://localhost:8000/metrics | grep cardiocare
```

---

## Compliance Mapping

Every design decision in this project maps to a named standard or control:

| Standard | Control Reference | Where Applied |
|---|---|---|
| ISO 27001 | A.9.4 — Application access control | Keycloak JWT + OPA policy |
| ISO 27001 | A.16.1.5 — Incident response | Alert function escalation path |
| ISO 27001 | A.12.4 — Logging and monitoring | Loki log aggregation |
| NIST CSF | PR.AC-4 — Access permissions managed | OPA Rego policy (`cardiac_policy.rego`) |
| NIST CSF | DE.CM-7 — Monitoring for unauthorised activity | Prometheus anomaly counters |
| GDPR Art.32 | Security of processing | JWT authentication, TLS endpoints |

---

## Project Structure

```
CardioCare-AIOps/
├── docker-compose.yml              ← single-file deployment (16 services)
├── setup.sh                        ← EC2 bootstrap (Docker install + image pull)
├── .env.example                    ← environment template
│
├── services/
│   ├── producer/
│   │   ├── producer.py             ← Kafka event producer, ECG simulation
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── aiops-engine/
│   │   ├── detector.py             ← IsolationForest anomaly detection + retraining
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── alert-function/
│   │   ├── handler.py              ← Serverless function: Kafka + HTTP invocation
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── api/
│       ├── main.py                 ← FastAPI gateway, JWT auth, OPA, OpenTelemetry
│       ├── Dockerfile
│       └── requirements.txt
│
├── observability/
│   ├── prometheus/prometheus.yml   ← scrape config for all services
│   ├── grafana/
│   │   ├── provisioning/           ← auto-provision Prometheus + Loki + Jaeger
│   │   └── dashboards/             ← pre-built cardiac monitoring dashboard
│   ├── loki/loki-config.yml
│   └── promtail/promtail-config.yml
│
├── security/
│   ├── keycloak/cardiocare-realm.json  ← realm, clients, roles, users (dr.raees)
│   └── opa/cardiac_policy.rego         ← RBAC policy (ISO 27001 A.9.4)
│
└── docs/
    └── architecture.md             ← system, network, and data-flow diagrams
```

---

*Muhammad Raees — CardioCare-AIOps v1.0 — DAIOL6 Capstone*
