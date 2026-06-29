
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

Two models run in the hybrid engine:
- **Isolation Forest** — unsupervised anomaly scoring on 7 vitals, retrains every 300 seconds from live observations
- **XGBoost** — supervised ECG classifier, 5 AAMI classes, 187 features, trained offline on MIT-BIH, served live

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

# Upload MIT-BIH data from local machine (run this on your local machine)
scp -i your-key.pem mitbih_train.csv mitbih_test.csv ubuntu@<EC2-PUBLIC-IP>:~/data/

# Start all 16 containers
chmod +x setup.sh
./setup.sh
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
| Prometheus | `http://<EC2-IP>:9090` | Metrics and alerting |
| FastAPI | `http://<EC2-IP>:8000/docs` | REST API with Swagger UI |
| Jaeger | `http://<EC2-IP>:16686` | Distributed tracing |
| Keycloak | `http://<EC2-IP>:8080` | Identity and access management |
| Loki | `http://<EC2-IP>:3100` | Log aggregation |
| OpenFaaS | `http://<EC2-IP>:8081` | Serverless alert function |
| Kafka UI | `http://<EC2-IP>:8082` | Kafka topic browser |

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

## Academic Context

Programme: AI Operations  
Institution: Alnafi International College  
Topic: Serverless Architectures, Event-Driven AIOps, Observability and Security  
Author: Muhammad Raees  
Date: June 2026

---

## License

MIT License — Copyright (c) 2026 Muhammad Raees
```


