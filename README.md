# CardioCare-AIOps

**Real-Time Cardiac Monitoring with Event-Driven Serverless AIOps**

> Diploma in Artificial Intelligence Operations (DAIOL6) — EduQual Level 6
> Topic 7: Serverless Architectures with Event-Driven AIOps, Observability & Security Integration
> Student: Muhammad Raees | raees.malik89@gmail.com

---

## Architecture Overview

```
[Cardiac Devices] → [Kafka: cardiac.vitals.stream]
                         ↓
               [AIOps Engine — IsolationForest ML]
                    ↓               ↓
         [cardiac.anomalies.detected]  [cardiac.alerts.critical]
                                            ↓
                              [Serverless Alert Function]
                                            ↓
                              [Grafana | Loki | Jaeger]
```

**Stack:** Apache Kafka · FastAPI · Scikit-learn · Prometheus · Grafana · Loki · Jaeger · Keycloak · Open Policy Agent · Docker Compose

---

## Deploy on EC2 — 4 Steps via MobaXterm

### Pre-requisite
- EC2 instance: **t3.large** (8 GB RAM recommended) or t3.medium (4 GB minimum)
- AMI: Amazon Linux 2023 or Ubuntu 22.04
- Security Group: open inbound ports **3000, 8000, 8080, 8085, 9090, 16686, 5000**

---

### Step 1 — Connect via MobaXterm SSH
```
Host:  <your-ec2-public-ip>
User:  ec2-user  (Amazon Linux) | ubuntu (Ubuntu)
Key:   your-key.pem
```

---

### Step 2 — Clone the Repository
```bash
git clone https://github.com/raeesmalik89-oss/CardioCare-AIOps.git
cd CardioCare-AIOps
cp .env.example .env
# Edit .env and set EC2_PUBLIC_IP to your instance's public IP
nano .env
```

---

### Step 3 — Run Setup Script (installs Docker if needed)
```bash
chmod +x setup.sh
./setup.sh
```

**Expected output:**
```
  ╔═══════════════════════════════════════════╗
  ║      CardioCare-AIOps — Setup Script      ║
  ║   Event-Driven Serverless AIOps Platform  ║
  ╚═══════════════════════════════════════════╝

[OK] Docker already installed: Docker version 24.0.7
[OK] Docker Compose already installed: Docker Compose version v2.23.0
[INFO] Created .env from .env.example
[OK] EC2 public IP set: 54.123.45.67
[INFO] Pre-pulling Docker images (this takes ~2 minutes)...
[OK] Images pulled successfully.

  ╔══════════════════════════════════════════════════════════════╗
  ║  Setup complete! Run the platform with:                     ║
  ║    docker compose up -d                                      ║
  ╚══════════════════════════════════════════════════════════════╝
```

---

### Step 4 — Launch the Platform
```bash
docker compose up -d
```

**Expected output:**
```
[+] Running 14/14
 ✔ Network cardiocare-net           Created
 ✔ Container zookeeper              Started
 ✔ Container kafka                  Started
 ✔ Container kafka-init             Started  ← creates 3 Kafka topics
 ✔ Container kafka-ui               Started
 ✔ Container cardiocare-producer    Started
 ✔ Container cardiocare-aiops-engine Started
 ✔ Container cardiocare-alert-fn    Started
 ✔ Container cardiocare-api         Started
 ✔ Container cardiocare-keycloak    Started
 ✔ Container cardiocare-opa         Started
 ✔ Container cardiocare-prometheus  Started
 ✔ Container cardiocare-grafana     Started
 ✔ Container cardiocare-loki        Started
 ✔ Container cardiocare-promtail    Started
 ✔ Container cardiocare-jaeger      Started
```

---

## Verify the System is Live

```bash
docker compose ps
```

```
NAME                          IMAGE                       STATUS
zookeeper                     cp-zookeeper:7.4.0          Up (healthy)
kafka                         cp-kafka:7.4.0              Up (healthy)
cardiocare-producer           cardiocare-aiops-producer   Up
cardiocare-aiops-engine       cardiocare-aiops-engine     Up
cardiocare-alert-fn           cardiocare-aiops-alert      Up
cardiocare-api                cardiocare-aiops-api        Up
cardiocare-keycloak           keycloak:22.0               Up
cardiocare-opa                opa:0.58.0                  Up
cardiocare-prometheus         prom/prometheus:v2.47.0     Up
cardiocare-grafana            grafana/grafana:10.1.0      Up
cardiocare-loki               grafana/loki:2.9.0          Up
cardiocare-jaeger             jaegertracing/all-in-one    Up
```

---

## Watch Real-Time AIOps Streaming

```bash
docker compose logs -f cardiocare-aiops-engine
```

**Simulated output (real data format):**
```
2026-06-09 10:23:01 [AIOPS-ENGINE] INFO  Bootstrap model trained on 500 synthetic samples.
2026-06-09 10:23:02 [AIOPS-ENGINE] INFO  Kafka connected.
2026-06-09 10:23:02 [AIOPS-ENGINE] INFO  Listening on topic: cardiac.vitals.stream
2026-06-09 10:23:03 [AIOPS-ENGINE] INFO  Streaming | seq=0 | patient=PT-001 | HR=72 | SpO2=98%
2026-06-09 10:23:04 [AIOPS-ENGINE] INFO  Streaming | seq=1 | patient=PT-003 | HR=68 | SpO2=99%
2026-06-09 10:23:05 [AIOPS-ENGINE] INFO  Streaming | seq=2 | patient=PT-002 | HR=85 | SpO2=97%
2026-06-09 10:23:07 [AIOPS-ENGINE] WARNING  ANOMALY | patient=PT-004 | severity=CRITICAL | score=-0.7821 | HR=187 | SpO2=79% | BP=192/110
2026-06-09 10:23:07 [AIOPS-ENGINE] ERROR    CRITICAL ALERT FIRED | patient=PT-004 | HR=187 | SpO2=79%
2026-06-09 10:23:10 [AIOPS-ENGINE] INFO  Streaming | seq=8 | patient=PT-005 | HR=91 | SpO2=96%
2026-06-09 10:23:15 [AIOPS-ENGINE] WARNING  ANOMALY | patient=PT-001 | severity=HIGH | score=-0.3412 | HR=156 | SpO2=88% | BP=168/98
```

---

```bash
docker compose logs -f cardiocare-alert-fn
```

**Simulated output:**
```
2026-06-09 10:23:07 [ALERT-FUNCTION] WARNING FUNCTION EXECUTED | PT-004 | severity=CRITICAL | ward=ICU
2026-06-09 10:23:07 [ALERT-FUNCTION] WARNING CARDIAC ALERT: Patient PT-004 in ICU. HR=187, SpO2=79%, BP=192/110
2026-06-09 10:23:07 [ALERT-FUNCTION] WARNING Action: CALL_CODE_BLUE | ISO27001 ref: A.16.1.5
```

---

```bash
docker compose logs -f cardiocare-producer
```

**Simulated output:**
```
2026-06-09 10:23:01 [PRODUCER] INFO  CardioCare-AIOps Producer starting...
2026-06-09 10:23:01 [PRODUCER] INFO  Topic: cardiac.vitals.stream | Interval: 1000ms | Anomaly injection: True
2026-06-09 10:23:02 [PRODUCER] INFO  Connected to Kafka at kafka:29092
2026-06-09 10:23:03 [PRODUCER] INFO  Streaming | seq=0 | patient=PT-002 | HR=76 | SpO2=98%
2026-06-09 10:23:34 [PRODUCER] WARNING ANOMALY INJECTED | patient=PT-003 HR=193 SpO2=82 BP=188/107
```

---

## Access URLs

| Service        | URL                              | Credentials              |
|----------------|----------------------------------|--------------------------|
| **Grafana**    | `http://EC2_IP:3000`             | admin / CardioCare@2024  |
| **FastAPI**    | `http://EC2_IP:8000/docs`        | (Swagger UI — open)      |
| **Kafka UI**   | `http://EC2_IP:8085`             | (open)                   |
| **Keycloak**   | `http://EC2_IP:8080`             | admin / CardioCare@2024  |
| **Prometheus** | `http://EC2_IP:9090`             | (open)                   |
| **Jaeger**     | `http://EC2_IP:16686`            | (open)                   |
| **Alert Fn**   | `http://EC2_IP:5000/alerts`      | (open)                   |

---

## Kafka Topics

```bash
# View topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Monitor live events
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cardiac.vitals.stream \
  --from-beginning

# Monitor anomalies only
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cardiac.anomalies.detected
```

**Expected Kafka topic list:**
```
cardiac.vitals.stream
cardiac.anomalies.detected
cardiac.alerts.critical
```

---

## API Quick Test

```bash
# System status
curl http://localhost:8000/api/v1/system/status

# Latest vitals
curl http://localhost:8000/api/v1/vitals/latest

# Detected anomalies
curl http://localhost:8000/api/v1/anomalies?severity=CRITICAL

# Prometheus metrics
curl http://localhost:8000/metrics
```

---

## Compliance & Standards

| Standard       | Control                          | Implementation              |
|----------------|----------------------------------|-----------------------------|
| ISO 27001      | A.9.4 — Access Control           | Keycloak RBAC + OPA         |
| ISO 27001      | A.16.1.5 — Incident Management   | Alert function + audit log  |
| NIST CSF       | PR.AC-4 — Access permissions     | OPA policy enforcement      |
| NIST CSF       | DE.CM-7 — Monitoring             | Prometheus + Grafana        |
| GDPR Art.32    | Security of processing           | TLS, JWT, audit logging     |

---

## Project Structure

```
CardioCare-AIOps/
├── docker-compose.yml          ← Single-file deployment
├── setup.sh                    ← EC2 bootstrap script
├── .env.example                ← Configuration template
├── services/
│   ├── producer/               ← Kafka event producer
│   ├── aiops-engine/           ← IsolationForest ML engine
│   ├── alert-function/         ← Serverless alert handler
│   └── api/                    ← FastAPI gateway
├── observability/
│   ├── prometheus/             ← Metrics scraping config
│   ├── grafana/                ← Pre-built dashboard
│   ├── loki/                   ← Log aggregation
│   └── promtail/               ← Log shipper
├── security/
│   ├── keycloak/               ← Realm + users config
│   └── opa/                    ← Authorization policy
└── docs/
    └── architecture.md         ← Diagrams (network/data/system)
```

---

*Muhammad Raees — DAIOL6 Capstone Project — CardioCare-AIOps v1.0*
