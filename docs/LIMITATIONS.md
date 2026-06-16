# CardioCare-AIOps — Scope, Limitations & Honest Positioning

**Author:** Muhammad Raees · **Programme:** DAIOL6 (EduQual Level 6) · **June 2026**

This document states plainly what the project *is*, what it *is not*, and where the
evidence ends. It exists so that no claim in the README, architecture document or
presentation can be read as stronger than the supporting evidence.

---

## 1. Positioning — monitoring platform with AIOps components

CardioCare-AIOps is an **event-driven monitoring platform with AIOps components**, not
a full AIOps suite. It delivers:

- ML-based anomaly detection (Isolation Forest, unsupervised)
- A supervised ECG-beat classifier (XGBoost, live in the ensemble)
- Clinical-rule severity routing
- Scheduled model retraining
- Full observability (metrics, logs, traces)

It does **not** implement the following common AIOps capabilities — they are out of
scope and listed as future work:

- Root-cause analysis
- Cross-signal event correlation
- Predictive incident management / forecasting
- Automated remediation / self-healing
- Capacity forecasting

## 2. Three levels of model evaluation — be precise about which is which

**The XGBoost model runs live** in the AIOps ensemble, classifying every real MIT-BIH
beat as it streams. There are three distinct senses of "accuracy", and we are careful not
to conflate them:

1. **Offline test accuracy (validation).** 97.27% accuracy, 0.9927 weighted AUC-ROC, and
   the per-class precision/recall/F1 in `models/training_metadata.json` — measured once on
   the held-out MIT-BIH test set (21,892 beats). This is how the model was *validated*.

2. **Replay / streaming / online evaluation (live, but on replayed labels).** Because each
   replayed MIT-BIH beat still carries its original annotated label, the engine compares
   every live prediction to that label and exposes a continuously-updating accuracy —
   `cardiocare_xgboost_replay_accuracy` (cumulative) and
   `cardiocare_xgboost_replay_accuracy_window` (sliding window). This *is* a real-time
   metric and it tracks the offline figure, **but the labels come from a previously
   annotated dataset replayed onto the stream**, so it is replay/streaming evaluation — the
   term used in the literature — and **not** production evaluation.

3. **True production accuracy (not available).** Real patient streams have **no ground-truth
   labels**, so accuracy on genuinely unlabelled production data cannot be measured and is
   **not claimed**. The replay metric is a faithful proxy under replay conditions, not a
   guarantee of behaviour on unseen real patients.

In short: we *do* report a live, continuously-updating accuracy, and we label it honestly as
**replay/streaming evaluation**, never as production accuracy.

**Evaluation is not training.** To avoid any confusion: "online/replay evaluation" means the
model is *scored* live — it does **not** mean XGBoost is *trained* live. XGBoost is trained
**once, offline** (`services/ml-trainer/train.py`) and then served as a frozen, read-only
model; it is never retrained from the stream (production beats have no labels to train on).
The only component that learns online is the unsupervised **Isolation Forest**, which the
engine retrains every ~5 minutes on a rolling buffer of live vitals. So: XGBoost = offline
training + online inference + online replay evaluation; Isolation Forest = online retraining.

## 3. Data provenance

- The committed model was trained on the **real** MIT-BIH dataset, evidenced by the
  training log (`evidence/20260614_215910/xgboost_training.log`): 87,554 train / 21,892
  test beats with the genuine imbalanced 5-class distribution. The synthetic fallback in
  `train.py` produces only `n=5,000` (fixed `seed=42`), so the recorded counts cannot
  have come from the fallback.
- From the current trainer version onward, `train.py` records the **SHA-256 of each
  dataset CSV** plus exact sample counts and class distribution into the `provenance`
  block of `training_metadata.json`. The committed run predates the checksum field; its
  provenance rests on the sample counts and committed log.
- The synthetic fallback is intentional (keeps CI and `docker compose up` working without
  the ~490 MB dataset) but is **clearly flagged** in logs and in metadata
  (`provenance.trained_on_real_data`).

## 4. Anomaly detector (Isolation Forest) — limited evaluation

Unlike the XGBoost classifier, the Isolation Forest anomaly detector has **no labelled
benchmark**: it is unsupervised and there is no ground-truth set of "true ICU
emergencies" to score it against. Consequently precision, recall, false-positive rate and
detection latency for the anomaly path are **not currently quantified**. The detector is
demonstrated functionally (it flags injected anomalies and clinical-rule breaches) but its
statistical effectiveness is an open evaluation gap. A future improvement would label a
held-out vitals stream (e.g. via NEWS2 thresholds) to produce these metrics.

## 5. No explainability layer

There is **no model explainability component** (e.g. SHAP values, feature-importance
attribution, per-prediction explanations). For clinical AI this is a recognised gap.
XGBoost exposes global feature importances and SHAP is compatible with the model, so this
is a tractable next step — but it is **not implemented today**.

## 6. Standards alignment, not certified compliance

The platform is **designed in alignment with the principles of** ISO/IEC 27001, NIST CSF
2.0 and GDPR Article 32. This is **not** certified compliance. There is no:

- ISO 27001 control-mapping audit by a certifying body
- Formal GDPR Data Protection Impact Assessment (DPIA)
- Documented risk assessment

The control-to-standard table in `docs/architecture.md` shows *intended* alignment for
academic purposes only.

## 7. HIPAA reference — principles, not applicability

MIT-BIH is de-identified **public research data**; HIPAA does not apply to it. HIPAA is
referenced only because the platform's **technical safeguards** (access control,
encryption, audit logging) were **designed using the principles of HIPAA 45 CFR §164.312**.
Metadata and documentation have been reworded to make this distinction explicit.

## 8. AWS is a deployment target, not a managed cloud architecture

The system runs on a single AWS EC2 instance via Docker Compose. There is **no**
Infrastructure-as-Code (CloudFormation/Terraform), no managed AWS services (MSK, EKS,
Lambda), no IAM policy set and no VPC/security-group definitions committed. EC2 is simply
the host; "built on AWS" should be read as "deployed on an AWS EC2 host."

## 9. Single-node deployment — no high availability

The entire stack runs on one node (single-node demo, as stated in `docs/architecture.md`).
Every component is therefore a **single point of failure**: Kafka (single broker), Keycloak,
OPA, Grafana, Prometheus and the application services all run as single instances with no
replication, clustering or failover. Kafka also runs without persistent multi-broker
replication. Production hardening (HA Kafka, replicated auth/policy, persistent storage)
is future work.

## 10. Other known gaps

- **No persistent storage** for time-series vitals or alerts beyond Prometheus/Loki
  retention and in-memory alert logs (TimescaleDB/Redis are future work).
- **No TLS/HTTPS** on inter-service or Kafka transport in the demo (payloads are
  AES-256-GCM encrypted at the application layer, but transport is plaintext on the
  private Docker network).
- **Bundled Keycloak users** are demonstration accounts with temporary passwords and must
  be replaced before any non-demo use.

---

*These limitations are deliberate scoping choices for an academic capstone. They are
documented here so reviewers can assess the project against what it actually claims.*
