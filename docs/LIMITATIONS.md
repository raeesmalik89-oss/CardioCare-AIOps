# CardioCare-AIOps — Scope & Limitations

**Author:** Muhammad Raees · **June 2026**

This document explains the scope of the project and its current limitations. It helps
ensure the project is evaluated based on what it actually delivers.

---

## 1. Project Scope

CardioCare-AIOps is an **event-driven monitoring platform with AIOps components**, not a
complete AIOps platform.

The project includes:
- ML-based anomaly detection (Isolation Forest)
- ECG classification (XGBoost)
- Clinical-rule alerts
- Scheduled model retraining
- Monitoring, logging, and tracing

The following features are **not implemented**:
- Root cause analysis
- Event correlation
- Predictive incident management
- Automated remediation
- Capacity forecasting

### Why Automated Remediation Was Not Included

In healthcare, clinical decisions should always be made by healthcare professionals. This
project is designed as a **Decision Support System (DSS)**. It detects possible problems,
classifies their severity, and sends alerts, but the final decision is always made by a
clinician.

---

## 2. Model Evaluation

The XGBoost model was trained offline using the MIT-BIH ECG dataset and achieved:

- Accuracy: **97.27%**
- Weighted AUC-ROC: **0.9927**

The model runs live during event replay, where predictions are compared with the original
dataset labels.

True production accuracy cannot be measured because real patient data has no ground-truth
labels.

The Isolation Forest model is unsupervised, so precision and recall are not currently
available.

---

## 3. Data Source

The XGBoost model was trained using the real MIT-BIH ECG dataset.

A synthetic dataset is only used when the real dataset is unavailable for testing or
development.

---

## 4. Current Limitations

This project currently does **not** include:
- Model explainability (SHAP)
- High availability or failover
- Persistent database storage
- TLS/HTTPS between internal services
- Infrastructure as Code (Terraform/CloudFormation)
- Multi-node Kubernetes deployment

---

## 5. Standards Alignment

The project follows the principles of:
- ISO 27001
- NIST CSF 2.0
- GDPR Article 32
- HIPAA Security Rule technical safeguards

This is **alignment only**, not certified compliance.

---

## 6. Deployment

The system runs on a **single AWS EC2 instance** using Docker Compose.

All services run as single instances, so this is a prototype rather than a production
deployment.

Future improvements include:
- Kafka cluster
- Kubernetes (EKS or AKS)
- High availability
- Load balancing
- Autoscaling

---

## 7. Serverless (OpenFaaS/faasd) — Scope and Limitations

Critical alerting (`cardiocare-alert-handler`) runs on **faasd**, a single-VM,
non-Kubernetes FaaS runtime, used to demonstrate the serverless/event-triggered pattern
on a single EC2 host. This is a deliberate scope choice, and it carries the limitations
any FaaS deployment does:

- **Cold starts.** A scaled-to-zero function has non-zero latency on its first
  invocation after idling. This was demonstrated directly: forcing the container to zero
  replicas and re-invoking produced a transient `500` error on the first call, then a
  clean response once the new container finished starting.
- **Execution time limits and statelessness.** The handler is a short-lived,
  stateless request/response unit — it keeps no state between invocations, and anything
  that needs to persist lives outside the function (Kafka, the log stack).
- **Portability.** The handler's business logic has no OpenFaaS-specific dependencies
  and would need no code changes to run on AWS Lambda, Azure Functions, or GCP Cloud
  Functions. Only the deployment manifest (trigger wiring, scaling config) would need to
  be re-expressed in that provider's terms.
- **CE vs Pro autoscaler maturity.** faasd's Community Edition idle-reconciler did not
  reliably auto-trigger scale-to-zero within the configured window in testing — a known
  gap versus faasd Pro or Kubernetes-based OpenFaaS. The scale-to-zero configuration
  itself was verified correct (`com.openfaas.scale.min=0`, `max=5`, `zero=true` via
  `faas-cli describe`); cold-start behaviour was demonstrated by manually forcing the
  container to zero and re-invoking, which shows the same underlying mechanism.

---

*These limitations are intentional for an academic prototype and help define the project's
current scope.*
