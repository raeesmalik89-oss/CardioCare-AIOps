
# CardioCare-AIOps

A real-time, AI-assisted cardiac monitoring platform — an event-driven streaming pipeline with serverless alerting, full observability and fail-closed security. It is deployed on a single AWS EC2 instance (Ubuntu 26.04 LTS) via Docker Compose; EC2 is the deployment target, not an IaC-managed cloud architecture (see [Scope & Limitations](docs/LIMITATIONS.md)).

---

## About

CardioCare-AIOps detects cardiac anomalies in real time from real MIT-BIH ECG beats replayed as live vital-sign streams across 9 ICU beds. Apache Kafka carries AES-256-GCM encrypted event envelopes; the live AIOps engine runs a hybrid detector — a scikit-learn Isolation Forest plus clinical rules on the vitals, and an XGBoost classifier on each real ECG beat — and critical events invoke a serverless alert function (OpenFaaS faasd). XGBoost is trained offline on the MIT-BIH Arrhythmia dataset (97.27% held-out test accuracy) and serves live in the ensemble.

**Scope note:** CardioCare-AIOps is an *event-driven monitoring platform with AIOps components* (ML-based anomaly detection, scheduled retraining, full observability) rather than a complete AIOps suite. Capabilities such as root-cause analysis, event correlation, predictive incident management and automated remediation are out of scope — see [Scope & Limitations](docs/LIMITATIONS.md).

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
- **ECG-beat path — XGBoost** (trained by `services/ml-trainer`, served by the engine): a supervised classifier that labels every real ECG beat into 5 AAMI classes (187 features), trained on the MIT-BIH Arrhythmia dataset. It runs **live** in the ensemble, applying the model trained and validated offline. **Training is offline-only** — XGBoost is trained once and served as a frozen, read-only model; it is *not* retrained from the stream. (The Isolation Forest, by contrast, *is* retrained online.)

**Offline (held-out test set) performance — 21,892 beats:**

| Metric | Value |
|---|---|
| Training samples | 87,554 |
| Test samples | 21,892 |
| Test accuracy | 97.27% |
| Weighted AUC-ROC (one-vs-rest) | 0.9927 |

**Per-class metrics (offline test set):**

| Class | Precision | Recall | F1 | Support |
|---|---|---|---|---|
| Normal | 0.99 | 0.98 | 0.98 | 18,118 |
| Supraventricular | 0.67 | 0.81 | 0.73 | 556 |
| Ventricular | 0.93 | 0.95 | 0.94 | 1,448 |
| Fusion | 0.69 | 0.85 | 0.76 | 162 |
| Unclassifiable | 0.98 | 0.98 | 0.98 | 1,608 |
| **Macro avg** | 0.85 | 0.91 | 0.88 | — |
| **Weighted avg** | 0.98 | 0.97 | 0.97 | 21,892 |

> These are **offline test-set** metrics — how the model was validated. The full per-class report and confusion matrix are in `models/training_metadata.json` and the training log under `evidence/`.

**Real-time replay evaluation.** The engine also scores the model *live*: because each replayed MIT-BIH beat still carries its annotated label, every prediction is compared to that label and exposed as a continuously-updating accuracy — `cardiocare_xgboost_replay_accuracy` (cumulative) and `cardiocare_xgboost_replay_accuracy_window` (sliding window, `REPLAY_WINDOW` beats). This is **replay / streaming / online evaluation** (labels originate from a previously annotated dataset), **not** true production accuracy — real patient streams are unlabelled. See [Scope & Limitations](docs/LIMITATIONS.md) §2 for the three-level distinction.

### Data provenance & reproducibility

The committed model was trained on the **real** MIT-BIH dataset, not the synthetic CI fallback. Evidence: the training log records **87,554 train / 21,892 test** beats with the genuine imbalanced 5-class distribution, whereas the synthetic fallback in `train.py` produces only `n=5,000` with a fixed `seed=42`. From this run forward, `train.py` also records the **SHA-256 of each dataset CSV** and the exact sample counts into `training_metadata.json` (`provenance` block) for an auditable training trail.

The trained model and run metrics are committed at `models/xgboost_ecg_classifier.json` and `models/training_metadata.json`; the full training log is under `evidence/`. The MIT-BIH CSVs (~490 MB) are not committed (GitHub size limits) — download from [PhysioNet](https://physionet.org/content/mitdb/1.0.0/) or the [Kaggle mirror](https://www.kaggle.com/datasets/shayanfazeli/heartbeat) and place under `data/`. Reproduce or rebuild via `services/ml-trainer/README.md` (prebuilt image: `mraees1989/cardiocare-ml-trainer:v1.0`).

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
- Security controls are **aligned with the principles of** ISO 27001, NIST CSF and GDPR Article 32. This is design alignment, not certified compliance — no formal audit, risk assessment or DPIA has been performed (see [Scope & Limitations](docs/LIMITATIONS.md)).
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
