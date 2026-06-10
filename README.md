
# CardioCare-AIOps

A real-time AI-powered cardiac monitoring platform built on AWS EC2 using event-driven AIOps principles.

---

## About

CardioCare-AIOps detects cardiac anomalies in real time by combining machine learning with live ECG data streams. The system processes heartbeat data through Apache Kafka, classifies arrhythmias using a trained XGBoost model, and delivers critical alerts through a serverless function — all secured with enterprise-grade encryption and access control.

## Highlights

- XGBoost classifier trained on 87,554 real ECG heartbeats — 97.27% accuracy
- 15 microservices running live on AWS EC2 via Docker Compose
- Real-time streaming through Apache Kafka with AES-256-GCM encryption
- Serverless alerting via OpenFaaS faasd — scale to zero proven
- Full observability with Prometheus, Grafana, Loki, and Jaeger
- HIPAA 45 CFR 164.312 compliant with tamper-evident audit logging
- OWASP ZAP security scan — zero critical vulnerabilities, 64 checks passed

## Technologies Used

Apache Kafka, FastAPI, XGBoost, scikit-learn, OpenFaaS, Keycloak, Open Policy Agent, Prometheus, Grafana, Loki, Jaeger, OpenTelemetry, Docker

## Getting Started

git clone https://github.com/raeesmalik89-oss/CardioCare-AIOps.git
cd CardioCare-AIOps
cp .env.example .env
docker compose up -d

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
