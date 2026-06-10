# CardioCare-AIOps 🫀

> AI-Powered Cardiac Monitoring Platform — Real-time ECG analysis with serverless AIOps, full observability, and enterprise security.

[![CI/CD](https://github.com/raeesmalik89-oss/CardioCare-AIOps/actions/workflows/ci.yml/badge.svg)](https://github.com/raeesmalik89-oss/CardioCare-AIOps/actions)
[![Docker Hub](https://img.shields.io/badge/Docker%20Hub-mraees1989-blue)](https://hub.docker.com/u/mraees1989)
[![HIPAA](https://img.shields.io/badge/HIPAA-45%20CFR%20164.312-green)](https://www.hhs.gov/hipaa)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

CardioCare-AIOps is a production-grade, event-driven platform for real-time cardiac monitoring built on AWS EC2 (Ubuntu 22.04, t3.large). Built for **DAIOL6 EduQual Level 6 — Topic 7: Serverless Architectures, Event-Driven AIOps, Observability & Security**.

| Metric | Value |
|---|---|
| ML Accuracy | **97.27%** XGBoost on MIT-BIH (87,554 ECG beats) |
| AUC-ROC | **0.9927** |
| Services | **15** microservices via Docker Compose |
| CI/CD | **18** consecutive green GitHub Actions runs |
| Security | OWASP ZAP: **FAIL-NEW=0, PASS=64** |
| Compliance | HIPAA 45 CFR 164.312, ISO 27001, GDPR, NIST |

---

## Architecture

```
ECG Sensors → [Kafka: 3 topics] → [AIOps Engine: XGBoost + IsolationForest + Rules]
                                          ↓
                              [HIPAA Audit] [Alert Function (FaaS)]
                                          ↓
                              [FastAPI :8000] ← Keycloak JWT + OPA RBAC
                                          ↓
                         [Prometheus + Grafana + Loki + Jaeger]
```

**5 Layers:** Security → API Gateway → AIOps Engine → Kafka Streaming → Observability

---

## Quick Start

```bash
git clone https://github.com/raeesmalik89-oss/CardioCare-AIOps.git
cd CardioCare-AIOps
cp .env.example .env   # never commit .env
docker compose up -d
```

---

## Key Services

| Service | Port | Purpose |
|---|---|---|
| cardiocare-api | 8000 | FastAPI + JWT + OPA |
| cardiocare-keycloak | 8086 | OIDC Identity Provider |
| cardiocare-grafana | 3000 | Live dashboard (6 panels) |
| cardiocare-prometheus | 9090 | Metrics scraping |
| cardiocare-jaeger | 16686 | Distributed tracing |
| kafka-ui | 8085 | Kafka management |

---

## Security & Compliance

- **Auth:** Keycloak OIDC RS256 JWT → OPA RBAC deny-by-default
- **Encryption:** AES-256-GCM PHI + PBKDF2-HMAC-SHA256 (310,000 iterations)
- **Audit:** HMAC-SHA256 tamper-evident JSONL logs, pseudonymised patient refs
- **Scan:** OWASP ZAP — FAIL-NEW=0, PASS=64 (reports in repo)

---

## CI/CD

4-job GitHub Actions pipeline — **18 green runs**:
`lint` → `encryption-test` → `docker-validate` → `docker-push` (6 images to [mraees1989](https://hub.docker.com/u/mraees1989))

---

## Author

**Muhammad Raees** | DAIOL6 EduQual Level 6 | Topic 7 | June 2026
