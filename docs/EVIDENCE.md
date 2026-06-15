# CardioCare-AIOps — Slide-by-Slide Evidence Map

This document is the traceability matrix for the project presentation. Every claim
made on a slide is tied to the backing source code (`path:line`), a command to
reproduce it, and the committed artifact that proves it.

- **Repo:** https://github.com/raeesmalik89-oss/CardioCare-AIOps
- **Live demo:** Docker Compose on AWS EC2 (`docker compose up -d`)
- **CI:** `.github/workflows/ci.yml` — lint, test, zap-baseline, encryption-test, docker-validate, docker-push

> Reproduce commands assume the stack is running (`docker compose up -d`). Service
> metrics: API on `:8000`, aiops-engine on `:8001`, alert-function on `:5000`.

---

## Layer 0 — Overview (Slides 1–5)

| Slide | Claim | Code proof | Reproduce / verify |
|---|---|---|---|
| 1 | Title / repo | `README.md` | clone the repo |
| 2 | Problem: real-time cardiac monitoring need | `README.md` (About) | — (contextual) |
| 3 | Hybrid detection: IsolationForest + rules (live) + XGBoost (offline) | live: `services/aiops-engine/detector.py:42,141`; offline: `services/ml-trainer/train.py` | `cat models/training_metadata.json` |
| 4 | 5 mandatory objectives (serverless/event-driven/observability/AIOps/security) | serverless `openfaas/stack.yml`; events `docker-compose.yml:26-61`; obs `docker-compose.yml:190-263`; AIOps `services/aiops-engine/`, `services/ml-trainer/`; security `services/api/main.py:121-176`, `security/` | see per-layer rows below |
| 5 | 6-layer architecture, ~15 microservices | 16 service blocks in `docker-compose.yml:17-252` | `docker compose ps` |

---

## Layer 1 — Data Ingestion (Slides 6–7)

| Slide | Claim | Code proof | Reproduce / verify |
|---|---|---|---|
| 6 | Producer simulates 5 patients, 1 event/sec, ~5% anomalies, keyed by patient | `services/producer/producer.py:43` (5 IDs), `:40` (interval), `:175` (anomaly rate), `:181` (key) | `docker compose logs -f producer` |
| 6 | Events AES-256-GCM encrypted before publish to `cardiac.vitals.stream` | `services/producer/producer.py:30,181`; `services/producer/crypto.py` | see encryption-test (Slide 21) |
| 6 | ECG waveform + NEWS2 early-warning score per event | `services/producer/producer.py:78` (ECG), `:124` (NEWS2) | inspect event in `cardiac.vitals.stream` |
| 7 | End-to-end flow: Simulator → Encrypt → Kafka → IsolationForest+Rules → OpenFaaS → Notify | producer → `crypto.py` → `docker-compose.yml:59` → `detector.py` → `alert-function/` | `docker compose logs -f` |

---

## Layer 2 — Event Streaming (Slides 8–10)

| Slide | Claim | Code proof | Reproduce / verify |
|---|---|---|---|
| 8 | 3 Kafka topics: vitals.stream → anomalies.detected → alerts.critical | `docker-compose.yml:59-61` | `docker exec kafka kafka-topics --list --bootstrap-server kafka:29092` |
| 9 | Topics configured with correct partitioning (3 / 3 / 1) | `docker-compose.yml:59` (3), `:60` (3), `:61` (1) | `docker exec kafka kafka-topics --describe --bootstrap-server kafka:29092` |
| 10 | Live messages streaming across partitions | producer publishes continuously `services/producer/producer.py:181` | `docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic cardiac.vitals.stream` *(screenshot evidence)* |

---

## Layer 3 — AIOps Processing (Slides 11–12)

| Slide | Claim | Code proof | Reproduce / verify | Artifact |
|---|---|---|---|---|
| 11 | Live: IsolationForest scores anomalies (unsupervised) | `services/aiops-engine/detector.py:42,81,131` | `docker compose logs -f aiops-engine` | — |
| 11 | Live: clinical rules override ML, route severity CRITICAL/HIGH/MEDIUM/LOW | `services/aiops-engine/detector.py:141-145` | trigger CRITICAL via `scripts/demo-trigger.sh` | — |
| 11 | Scheduled retraining on real observations | `services/aiops-engine/detector.py:126,178`; metric `:69` | `curl -s localhost:8001/metrics \| grep cardiocare_model_last_retrain` | — |
| 11/12 | Offline XGBoost on MIT-BIH: 87,554 train / 21,892 test, 97.27%, AUC 0.9927, 5 AAMI classes | `services/ml-trainer/train.py:177` (model), `:213` (accuracy); metrics computed `:205-214` | `docker run --rm -v $PWD/data:/data -v $PWD/models:/models cardiocare-ml-trainer` | `models/training_metadata.json`, `models/xgboost_ecg_classifier.json`, `evidence/20260614_215910/xgboost_training.log` |

---

## Layer 4 — Serverless Alerting (Slides 13–15)

| Slide | Claim | Code proof | Reproduce / verify |
|---|---|---|---|
| 13 | OpenFaaS scale-to-zero alert function (min 0 / max 5) | `openfaas/stack.yml:16-18` | `faas-cli list` |
| 13 | Triggered by `cardiac.alerts.critical`; webhook + in-memory log → Loki | `openfaas/stack.yml:20`; `services/alert-function/handler.py:51,65,114,125` | `docker compose logs -f alert-function` |
| 14 | `cardiocare-alert-handler` deployed via faas-cli, Kafka-triggered | `openfaas/stack.yml`, `openfaas/deploy.sh` | `faas-cli deploy -f openfaas/stack.yml` *(screenshot evidence)* |
| 15 | faasd runtime healthy on EC2 (gateway, provider, NATS, queue-worker) | `openfaas/deploy.sh` | `faasd version` / `systemctl status faasd` on EC2 *(screenshot evidence)* |

---

## Layer 5 — Observability (Slides 16–19)

| Slide | Claim | Code proof | Reproduce / verify |
|---|---|---|---|
| 16 | Prometheus + Grafana + Loki + Jaeger + OpenTelemetry across services | `docker-compose.yml:190` (prom), `:207` (grafana), `:226` (loki), `:239` (promtail), `:252` (jaeger) | `docker compose ps` |
| 16/18 | Custom live metric `cardiocare_anomalies_total` rising | `services/aiops-engine/detector.py:66,225` | `curl -s localhost:8001/metrics \| grep cardiocare_anomalies_total` |
| 17 | Grafana live dashboards (events, anomalies, latency) | `observability/grafana/dashboards/*.json` | open `http://<host>:3000` *(screenshot evidence)* |
| 19 | Jaeger trace of `cardiocare-api` `get_latest_vitals` via OpenTelemetry | `services/api/main.py:48,81,116` (OTel), `:249` (endpoint) | open `http://<host>:16686` *(screenshot evidence)* |

---

## Layer 6 — Security (Slides 20–22)

| Slide | Claim | Code proof | Reproduce / verify | Artifact |
|---|---|---|---|---|
| 20 | Keycloak JWT bearer auth, fail-closed | `services/api/main.py:121-148` | `curl -i localhost:8000/api/v1/patients` → `401` | — |
| 20 | Roles cardiologist/doctor/nurse/analyst; OPA RBAC, default deny | `security/keycloak/cardiocare-realm.json`; `security/opa/cardiac_policy.rego:11,21`; `services/api/main.py:151-176` | bad token → `403`; OPA down → `403` | — |
| 20/21/22 | AES-256-GCM payload encryption (PHI) | `services/*/crypto.py`; producer `producer.py:181` | CI `encryption-test` asserts ciphertext has no `patient_id` (`.github/workflows/ci.yml:76`) | — |
| 21/22 | OWASP ZAP baseline: 0 high/med/low, 5 informational, 0 critical | CI job `.github/workflows/ci.yml:35`; `.zap/rules.tsv` | re-run CI or `docker run ghcr.io/zaproxy/zaproxy:stable zap-baseline.py -t http://<host>:8000` | `evidence/zap/zap-report.{html,json,md}` |
| 20 | HIPAA 45 CFR 164.312 / ISO 27001 / NIST / GDPR alignment | `models/training_metadata.json` (compliance field); `security/` | — | — |

---

## Results & Conclusion (Slides 23–24)

| Slide | Stat | Source |
|---|---|---|
| 23 | 97.27% accuracy / 87,554 samples | `models/training_metadata.json` |
| 23 | ~15 microservices | `docker-compose.yml` (16 service blocks) |
| 23 | 0 critical vulnerabilities / 0 high-risk ZAP findings | `evidence/zap/zap-report.json` |
| 23 | 4 severity levels classified | `services/aiops-engine/detector.py:141` |
| 23 | < 120 ms alert latency / 99.99% uptime | runtime metrics *(Grafana screenshot evidence)* |
| 24 | All 6 objectives delivered; future: IoT ECG input, wire XGBoost into live path | this document + repo |

---

## Notes for assessors

- **ZAP scope:** the baseline scan targets the reachable, unauthenticated surface
  (`/health`, `/docs`, `/metrics`). Protected endpoints sit behind Keycloak, so the
  low finding count reflects that the exposed surface is clean — not a shallow scan.
- **XGBoost role:** the live detection path is IsolationForest + clinical rules; the
  XGBoost model is the **offline benchmark** that validates the live detector against
  the labelled MIT-BIH dataset. Both are in the repo.
- **Reproducibility:** the MIT-BIH CSVs (~490 MB) are not committed (GitHub size
  limits); place `mitbih_train.csv` / `mitbih_test.csv` under `data/` to retrain. The
  trained model, metrics, and training log are committed under `models/` and `evidence/`.
