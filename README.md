
# CardioCare-AIOps

A real-time AI-powered cardiac monitoring platform built on AWS EC2 (Ubuntu 26.04 LTS) using event-driven AIOps principles.

---

## About

CardioCare-AIOps detects cardiac anomalies in real time from real MIT-BIH ECG beats replayed as live vital-sign streams across 9 ICU beds. Apache Kafka carries AES-256-GCM encrypted event envelopes; the live AIOps engine runs a hybrid detector — a scikit-learn Isolation Forest plus clinical rules on the vitals, and an XGBoost classifier on each real ECG beat — and critical events invoke a serverless alert function (OpenFaaS faasd). XGBoost is trained offline on the MIT-BIH Arrhythmia dataset (97.27%) and serves live in the ensemble.

## Highlights

- Adaptive Isolation Forest anomaly detection with scheduled retraining (live)
- Live XGBoost ECG classifier, trained on MIT-BIH (87,554 beats, 97.27% accuracy, AUC-ROC 0.9927)
- Event-driven services deployable on AWS EC2 via Docker Compose
- Real-time streaming through Apache Kafka with AES-256-GCM encryption
- OpenFaaS deployment image and scale-to-zero function manifest
- Full observability with Prometheus, Grafana, Loki, and Jaeger
- Fail-closed Keycloak authentication and OPA authorization
- OWASP ZAP baseline scan and report generation in GitHub Actions

## Technologies Used

Apache Kafka, FastAPI, Flask, scikit-learn, XGBoost, OpenFaaS, Keycloak, Open Policy Agent, Prometheus, Grafana, Loki, Jaeger, OpenTelemetry, Docker

## Machine Learning Models

CardioCare-AIOps uses a hybrid, fully-live detection strategy:

- **Vitals path — Isolation Forest + clinical rules** (`services/aiops-engine`): unsupervised anomaly scoring on the 7 streaming vitals, with clinical rules overriding the model for life-threatening values and routing severity (CRITICAL / HIGH / MEDIUM / LOW). Retrains on a schedule from real observations.
- **ECG-beat path — XGBoost** (trained by `services/ml-trainer`, served by the engine): a supervised classifier that labels every real ECG beat into 5 AAMI classes (187 features), trained on the MIT-BIH Arrhythmia dataset. It runs **live** in the ensemble — its ~97% live accuracy matches the offline test set.

| Metric | Value |
|---|---|
| Training samples | 87,554 |
| Test samples | 21,892 |
| Test accuracy | 97.27% |
| Weighted AUC-ROC | 0.9927 |

The trained model and run metrics are committed at `models/xgboost_ecg_classifier.json` and `models/training_metadata.json`; the full training log is under `evidence/`. Reproduce or rebuild the trainer via `services/ml-trainer/README.md` (prebuilt image: `mraees1989/cardiocare-ml-trainer:v1.0`).

## Getting Started

```bash
git clone https://github.com/raeesmalik89-oss/CardioCare-AIOps.git
cd CardioCare-AIOps
cp .env.example .env
# Generate a 32-byte AES key and place the printed value in .env:
python -c "import base64,secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
docker compose up -d
```

Protected API routes require a Keycloak bearer token. Health and metrics
endpoints remain available for platform probes and Prometheus.

## OpenFaaS Deployment

Install OpenFaaS or faasd and `faas-cli`, log in to the gateway, export
`EVENT_ENCRYPTION_KEY`, then run:

```bash
./openfaas/deploy.sh
```

The deployment uses the OpenFaaS watchdog and includes scale-to-zero labels.
Connect `cardiac.alerts.critical` to `cardiocare-alert-handler` with the
OpenFaaS Kafka connector for event-driven invocation.

## Security Scope

- Kafka payloads are encrypted with AES-256-GCM before entering the broker.
- Keycloak and OPA fail closed when unavailable.
- Patient identifiers are excluded from Prometheus labels and clinical logs.
- The repository maps controls to ISO 27001, NIST CSF and GDPR Article 32.
- GitHub Actions produces an OWASP ZAP baseline report; scan results are
  evidence from each workflow run rather than a hard-coded claim.
- The bundled Keycloak users are demonstration accounts with temporary
  passwords. Replace or remove them before any non-demo deployment.

---

## Academic Context

Programme:   AIOps — EduQual Level 6
Institution: Alnafi International College
Topic:       Serverless Architectures, Event-Driven AIOps, Observability and Security
Author:      Muhammad Raees
Date:        June 2026

---

## License

MIT License

Copyright (c) 2026 Muhammad Raees

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
