
# CardioCare-AIOps

A real-time, AI-assisted cardiac monitoring platform — an event-driven streaming pipeline with serverless alerting, full observability and fail-closed security. Deployed on AWS EC2 (Ubuntu 26.04 LTS) via Docker Compose.

---

## About

CardioCare-AIOps detects cardiac anomalies in real time from real MIT-BIH ECG beats replayed as live vital-sign streams across 9 ICU beds. Apache Kafka carries AES-256-GCM encrypted event envelopes; the AIOps engine runs a hybrid detector — Isolation Forest on vitals and XGBoost on each ECG beat — and critical events invoke a serverless alert function (OpenFaaS faasd).

## Highlights

- Adaptive Isolation Forest anomaly detection with scheduled retraining
- Live XGBoost ECG classifier — MIT-BIH dataset, 97.27% accuracy, AUC-ROC 0.9927
- Apache Kafka streaming with AES-256-GCM encryption on all payloads
- OpenFaaS serverless alert function with scale-to-zero
- Full observability — Prometheus, Grafana, Loki, Jaeger
- Fail-closed Keycloak JWT authentication and OPA RBAC authorization
- OWASP ZAP security scan in GitHub Actions CI/CD

## Technologies Used

Apache Kafka · FastAPI · XGBoost · scikit-learn · OpenFaaS · Keycloak · Open Policy Agent · Prometheus · Grafana · Loki · Jaeger · OpenTelemetry · Docker

## Machine Learning Models

| Metric | Value |
|---|---|
| Training samples | 87,554 |
| Test samples | 21,892 |
| Test accuracy | 97.27% |
| Weighted AUC-ROC | 0.9927 |

Three components run in the hybrid detection engine:
- **IsolationForest** — flags rare outliers in 7 vitals without labelled emergencies, retrains on schedule, adapts to drift
- **Clinical Rules** — safety override: SpO₂<85, HR>180/<35, SBP>185/<75 route directly to CRITICAL, taking precedence over both models
- **XGBoost** — classifies every real ECG beat live into 5 AAMI classes, trained and validated offline on MIT-BIH (97.27% accuracy)

## Repository Structure

```
CardioCare-AIOps/
├── docker-compose.yml       # orchestrates all 17 containers
├── setup.sh                 # one-command EC2 bootstrap
├── services/                # custom application images (5 Dockerfiles)
│   ├── producer/             # replays MIT-BIH beats, AES-256-GCM encrypts, publishes to Kafka
│   ├── aiops-engine/          # IsolationForest + XGBoost + clinical rules, Kafka consumer/producer
│   ├── api/                   # FastAPI gateway — Keycloak JWT auth, OPA RBAC, 7 endpoints
│   ├── alert-function/        # OpenFaaS handler, fires on cardiac.alerts.critical
│   └── ml-trainer/            # offline XGBoost training (standalone, not part of the 17-container stack)
├── kafka-config/             # topic definitions (topics.yml) and Kafka setup notes
├── security/                 # identity and policy configuration
│   ├── keycloak/              # realm export, roles (cardiologist, doctor, nurse, analyst)
│   └── opa/                   # Rego RBAC policies, default-deny
├── monitoring/               # observability stack config
│   ├── grafana/                # dashboards + datasource/provisioning config
│   ├── prometheus/             # scrape config and alerting rules
│   ├── loki/                   # log aggregation config
│   └── promtail/               # log shipping config
├── openfaas/                 # faasd deployment artifacts (Dockerfile, stack.yml, deploy.sh)
├── scripts/                  # demo helper scripts
├── tests/                    # security/API test suite
├── docs/                     # architecture.md, LIMITATIONS.md
└── evidence/                 # captured proof (ZAP scan results, live run screenshots)
```

## Prerequisites

- Docker 24+ and Docker Compose v2
- AWS EC2 — Ubuntu 26.04 LTS, t2.large or higher (4 GB RAM minimum)
- MIT-BIH dataset CSVs — download from [Kaggle](https://www.kaggle.com/datasets/shayanfazeli/heartbeat) and place `mitbih_train.csv` and `mitbih_test.csv` in `data/`

## EC2 Deployment

```bash
# SSH into EC2
ssh -i your-key.pem ubuntu@<EC2-PUBLIC-IP>

# Clone the repo
git clone https://github.com/raeesmalik89-oss/CardioCare-AIOps.git
cd CardioCare-AIOps
mkdir -p data

# Upload MIT-BIH data from your LOCAL machine, in a separate terminal
# (optional — without this the producer falls back to simulated vitals)
scp -i your-key.pem mitbih_train.csv mitbih_test.csv ubuntu@<EC2-PUBLIC-IP>:~/CardioCare-AIOps/data/

# Back on the EC2 shell — installs Docker, configures .env, pre-pulls images
chmod +x setup.sh
./setup.sh

# setup.sh does NOT start the stack itself — start all 17 containers explicitly
docker compose up -d
```

## Getting Started (Local)

```bash
git clone https://github.com/raeesmalik89-oss/CardioCare-AIOps.git
cd CardioCare-AIOps
cp .env.example .env
python -c "import base64,secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
docker compose up -d
```

## Service URLs

| Service | URL | Purpose |
|---|---|---|
| Grafana | `http://<EC2-IP>:3000` | Dashboards — vitals, ML accuracy, alerts |
| Prometheus | `http://<EC2-IP>:9091` | Metrics and alerting (9090 is used by faasd's bundled Prometheus) |
| FastAPI | `http://<EC2-IP>:8000/docs` | REST API with Swagger UI |
| Jaeger | `http://<EC2-IP>:16686` | Distributed tracing |
| Keycloak | `http://<EC2-IP>:8095` | Identity and access management (8080 is used by the OpenFaaS gateway) |
| Loki | `http://<EC2-IP>:3100` | Log aggregation |
| OpenFaaS | `http://<EC2-IP>:8080` | Serverless alert function gateway |
| Kafka UI | `http://<EC2-IP>:8085` | Kafka topic browser |

## Environment Variables

| Variable | Description |
|---|---|
| `EVENT_ENCRYPTION_KEY` | Base64-encoded 32-byte AES-256 key |
| `MITBIH_DATA_DIR` | Path to MIT-BIH CSV files |
| `KEYCLOAK_ADMIN_PASSWORD` | Keycloak admin password |
| `GRAFANA_ADMIN_PASSWORD` | Grafana admin password |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address |
| `OPA_URL` | Open Policy Agent endpoint |

## Security

- AES-256-GCM encryption on all Kafka payloads
- Keycloak JWT and OPA fail closed when unavailable
- Patient identifiers excluded from all logs and metrics
- Aligned with ISO 27001, NIST CSF, and GDPR Article 32
- OWASP ZAP baseline scan on every CI/CD run

## OpenFaaS Deployment

```bash
docker login
./openfaas/deploy.sh
```

Connect `cardiac.alerts.critical` topic to `cardiocare-alert-handler` via OpenFaaS Kafka connector.

---

## AI Usage & Transparency

I used AI tools while building this project, the same way I'd use Stack Overflow or the
official docs — as a helper, not as the one making the decisions. Here's the honest split:

- **AI helped with:** boring boilerplate — Dockerfile syntax, wiring up
  `docker-compose.yml`, and a first draft of the Rego policy in
  `security/opa/cardiac_policy.rego`.
- **I designed and decided:** the overall 8-layer architecture, how the Kafka topics are
  split up (`docs/architecture.md §2-3`), which detection models to use and why
  (IsolationForest + clinical rules + XGBoost), how the security roles and access rules
  work, where encryption happens (at the producer, before anything hits Kafka), and what's
  in scope vs. out of scope (`docs/LIMITATIONS.md`).

I read, tested, and mostly rewrote anything AI helped generate before it went into the
project — nothing was used without me understanding exactly what it does and why it's
right for this system.

---

## Academic Context

Programme: AI Operations  
Institution: Alnafi International College  
Topic: Serverless Architectures, Event-Driven AIOps, Observability and Security  
Author: Muhammad Raees  
Date: July 2026

---

## License

MIT License — Copyright (c) 2026 Muhammad Raees
```


