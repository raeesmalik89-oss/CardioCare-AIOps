
# CardioCare-AIOps

A real-time AI-powered cardiac monitoring platform built on AWS EC2 using event-driven AIOps principles.

---

## About

CardioCare-AIOps detects cardiac anomalies in real time by combining machine learning with simulated ECG and vital-sign streams. Apache Kafka carries AES-256-GCM encrypted event envelopes, a scikit-learn Isolation Forest detects anomalies, and critical events invoke an alert function that can run in Docker Compose or OpenFaaS.

## Highlights

- Adaptive Isolation Forest anomaly detection with scheduled retraining
- Event-driven services deployable on AWS EC2 via Docker Compose
- Real-time streaming through Apache Kafka with AES-256-GCM encryption
- OpenFaaS deployment image and scale-to-zero function manifest
- Full observability with Prometheus, Grafana, Loki, and Jaeger
- Fail-closed Keycloak authentication and OPA authorization
- OWASP ZAP baseline scan and report generation in GitHub Actions

## Technologies Used

Apache Kafka, FastAPI, Flask, scikit-learn, OpenFaaS, Keycloak, Open Policy Agent, Prometheus, Grafana, Loki, Jaeger, OpenTelemetry, Docker

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
