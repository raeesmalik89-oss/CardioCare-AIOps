
"""
CardioCare-AIOps — ML Trainer
Author : Muhammad Raees (raees.malik89@gmail.com)


Trains XGBoost classifier on MIT-BIH ECG dataset.
Outputs: /models/xgboost_ecg_classifier.json
Run-once container — restart: no in docker-compose.
"""

import os
import sys
import json
import hashlib
import logging
import numpy as np
import pandas as pd
from datetime import datetime

# Module-level provenance record — populated at data-load time and written into
# training_metadata.json so every model ships with an auditable training origin.
PROVENANCE = {
    "trained_on_real_data": False,
    "train_samples": 0,
    "test_samples": 0,
    "class_distribution_train": {},
    "dataset_sha256": {},
}


def _sha256(path):
    """SHA-256 of a file, streamed so large CSVs don't exhaust memory."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

# XGBoost: gradient boosted trees — best-in-class for tabular ECG data

import xgboost as xgb

# scikit-learn: train/test split and evaluation metrics

from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import (
    classification_report, confusion_matrix,
    roc_auc_score, accuracy_score
)

from sklearn.preprocessing import LabelEncoder
import joblib

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ML-TRAINER] %(levelname)s %(message)s"
)

log = logging.getLogger(__name__)

# ── Config from environment ───────────────────────────────────────────────────

DATA_DIR   = os.getenv("DATA_DIR",   "/data")
MODELS_DIR = os.getenv("MODELS_DIR", "/models")


# MIT-BIH Arrhythmia Dataset column layout (Kaggle format)

# 187 ECG sample points + 1 label column

MITBIH_TRAIN = os.path.join(DATA_DIR, "mitbih_train.csv")
MITBIH_TEST  = os.path.join(DATA_DIR, "mitbih_test.csv")

# Output model path — shared with aiops-engine via models-vol Docker volume

MODEL_OUTPUT = os.path.join(MODELS_DIR, "xgboost_ecg_classifier.json")
META_OUTPUT  = os.path.join(MODELS_DIR, "training_metadata.json")

# MIT-BIH class labels (column 187)

# 0=Normal, 1=Supraventricular, 2=Ventricular, 3=Fusion, 4=Unclassifiable

CLASS_NAMES = {
    0: "Normal",
    1: "Supraventricular",
    2: "Ventricular",
    3: "Fusion",
    4: "Unclassifiable"
}


def load_mitbih_data():

    """
    Load MIT-BIH CSV files from /data volume.
    Dataset: 109,446 ECG beats, 5 classes, 187 features per beat.
    Download from: kaggle.com/shayanfazeli/heartbeat
    Falls back to synthetic data if CSVs not present (CI/dev mode).
    """

    if os.path.exists(MITBIH_TRAIN) and os.path.exists(MITBIH_TEST):
        log.info("Loading MIT-BIH dataset from %s", DATA_DIR)
        train_df = pd.read_csv(MITBIH_TRAIN, header=None)
        test_df  = pd.read_csv(MITBIH_TEST,  header=None)

        # Last column (187) is the label

        X_train = train_df.iloc[:, :187].values.astype(np.float32)
        y_train = train_df.iloc[:, 187].values.astype(int)
        X_test  = test_df.iloc[:, :187].values.astype(np.float32)
        y_test  = test_df.iloc[:, 187].values.astype(int)

        log.info("Train: %d samples | Test: %d samples", len(X_train), len(X_test))
        dist = {int(k): int(v) for k, v in zip(*np.unique(y_train, return_counts=True))}
        log.info("Class distribution: %s", dist)

        # Record auditable provenance: counts, class balance, dataset checksums.
        PROVENANCE.update({
            "trained_on_real_data": True,
            "train_samples": int(len(X_train)),
            "test_samples": int(len(X_test)),
            "class_distribution_train": dist,
            "dataset_sha256": {
                "mitbih_train.csv": _sha256(MITBIH_TRAIN),
                "mitbih_test.csv": _sha256(MITBIH_TEST),
            },
        })
        log.info("Dataset SHA-256 recorded for provenance: %s",
                 PROVENANCE["dataset_sha256"])
        return X_train, y_train, X_test, y_test

    else:

        # Synthetic fallback — allows docker compose up without real data

        log.warning("MIT-BIH CSVs not found — generating synthetic ECG data")
        log.warning("For real training: place mitbih_train.csv + mitbih_test.csv in %s", DATA_DIR)
        return generate_synthetic_data()


def generate_synthetic_data():

    """
    Synthetic ECG data mimicking MIT-BIH distribution.
    Used when real dataset not downloaded — ensures service starts cleanly.
    n=5000 samples, 187 features, 5 classes (class 0 dominates ~60%)
    """

    np.random.seed(42)
    n_samples = 5000

    # Simulate 5 ECG morphologies (one per MIT-BIH class)

    samples, labels = [], []
    class_counts = {0: 3000, 1: 500, 2: 800, 3: 300, 4: 400}

    for label, count in class_counts.items():
        t = np.linspace(0, 1, 187)
        for _ in range(count):

            # Base waveform amplitude varies by class (anomalous = different amp)

            amp   = 1.0 + label * 0.3
            noise = np.random.normal(0, 0.05 + label * 0.02, 187)

            # QRS spike at position ~62 (33% of 187)

            wave  = amp * np.exp(-((t - 0.33)**2) / (2 * 0.005**2)) + noise
            samples.append(wave.astype(np.float32))
            labels.append(label)

    X = np.array(samples)
    y = np.array(labels)

    # 80/20 train/test split, stratified to preserve class balance

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    log.info("Synthetic data: train=%d test=%d", len(X_train), len(X_test))
    PROVENANCE.update({
        "trained_on_real_data": False,
        "train_samples": int(len(X_train)),
        "test_samples": int(len(X_test)),
        "class_distribution_train": {int(k): int(v)
                                     for k, v in zip(*np.unique(y_train, return_counts=True))},
        "dataset_sha256": {"note": "synthetic fallback — not a real dataset"},
    })
    return X_train, y_train, X_test, y_test


def train_xgboost(X_train, y_train):

    """
    Train XGBoost multi-class classifier.
    Key hyperparameters justified:
      - n_estimators=200: enough trees for 187-dim ECG without overfitting
      - max_depth=6: standard for medical tabular data
      - scale_pos_weight handled by sample_weight (class imbalance)
      - eval_metric=mlogloss: multi-class log loss
      - tree_method=hist: fast histogram-based training
    """

    log.info("Training XGBoost classifier...")

    # Handle class imbalance — normal beats (class 0) dominate MIT-BIH

    classes, counts = np.unique(y_train, return_counts=True)

    # Weight each sample inversely proportional to its class frequency

    class_weight = {c: len(y_train) / (len(classes) * cnt)
                    for c, cnt in zip(classes, counts)}
    sample_weights = np.array([class_weight[y] for y in y_train])

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        use_label_encoder=False,
        eval_metric="mlogloss",
        tree_method="hist",      # memory-efficient for 187 features
        random_state=42,
        n_jobs=-1,               # use all available CPU cores
        verbosity=1,
    )

    model.fit(
        X_train, y_train,
        sample_weight=sample_weights,
        verbose=False,
    )

    log.info("XGBoost training complete")
    return model


def evaluate_model(model, X_test, y_test):

    """
    Evaluate on held-out test set.
    Reports: accuracy, per-class precision/recall/F1, confusion matrix.
    AUC-ROC calculated per class (one-vs-rest).
    These metrics are saved to training_metadata.json for compliance records.

    """
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)

    acc = accuracy_score(y_test, y_pred)
    log.info("Test accuracy: %.4f (%.1f%%)", acc, acc * 100)


    # Per-class report — both human-readable (log) and structured (metadata).

    target_names = [CLASS_NAMES[i] for i in range(5)]
    report = classification_report(
        y_test, y_pred, target_names=target_names, zero_division=0
    )
    report_dict = classification_report(
        y_test, y_pred, target_names=target_names, zero_division=0, output_dict=True
    )

    log.info("Classification report:\n%s", report)

    # AUC-ROC (one-vs-rest, handles class imbalance)

    try:
        auc = roc_auc_score(y_test, y_prob, multi_class="ovr",
                            average="weighted")
        log.info("Weighted AUC-ROC: %.4f", auc)
    except Exception as e:
        auc = 0.0
        log.warning("AUC-ROC calculation failed: %s", e)

    # Confusion matrix logged for exam defence

    cm = confusion_matrix(y_test, y_pred)
    log.info("Confusion matrix:\n%s", cm)

    # Structured per-class metrics for the audit record (precision/recall/F1).
    def _round(d):
        return {k: round(float(v), 4) for k, v in d.items()}

    per_class = {name: _round(report_dict[name]) for name in target_names}

    return {
        "accuracy":    round(float(acc), 4),
        "auc_roc":     round(float(auc), 4),
        "report":      report,
        "per_class":   per_class,
        "macro_avg":   _round(report_dict["macro avg"]),
        "weighted_avg": _round(report_dict["weighted avg"]),
        "confusion_matrix": cm.tolist(),
    }


def save_model(model, metrics):

    """
    Save trained model and metadata to shared models-vol Docker volume.
    aiops-engine reads this model at startup via the same volume.
    JSON format chosen for portability — readable without Python.

    """
    os.makedirs(MODELS_DIR, exist_ok=True)

    # Save XGBoost model in JSON format

    model.save_model(MODEL_OUTPUT)
    log.info("Model saved to %s", MODEL_OUTPUT)


    # Save training metadata for audit trail (ISO 27001 A.12.4)

    metadata = {
        "model":          "XGBoostClassifier",
        "dataset":        "MIT-BIH Arrhythmia Database (Kaggle preprocessed: shayanfazeli/heartbeat)",
        "dataset_source": {
            "physionet":      "https://physionet.org/content/mitdb/1.0.0/",
            "kaggle_mirror":  "https://www.kaggle.com/datasets/shayanfazeli/heartbeat",
            "note":           "MIT-BIH is de-identified public research data. HIPAA does "
                              "not apply to it; HIPAA technical-safeguard principles informed "
                              "the platform's security controls only.",
        },
        "features":       187,
        "classes":        CLASS_NAMES,
        "trained_at":     datetime.utcnow().isoformat() + "Z",
        "model_path":     MODEL_OUTPUT,
        "provenance": {
            "trained_on_real_data":     PROVENANCE["trained_on_real_data"],
            "train_samples":            PROVENANCE["train_samples"],
            "test_samples":             PROVENANCE["test_samples"],
            "class_distribution_train": PROVENANCE["class_distribution_train"],
            "dataset_sha256":           PROVENANCE["dataset_sha256"],
        },
        "metrics": {
            "accuracy":             metrics["accuracy"],
            "auc_roc_weighted_ovr": metrics["auc_roc"],
            "evaluation":           "Held-out test set. Offline test metrics only — "
                                    "production streams carry no ground-truth labels, so no "
                                    "live-accuracy figure is claimed.",
            "per_class":            metrics["per_class"],
            "macro_avg":            metrics["macro_avg"],
            "weighted_avg":         metrics["weighted_avg"],
            "confusion_matrix": {
                "labels": [CLASS_NAMES[i] for i in range(5)],
                "rows_true_cols_pred": metrics["confusion_matrix"],
            },
        },
        "author":         "Muhammad Raees",
        "compliance":     "Security controls designed using HIPAA 45 CFR §164.312 "
                          "technical-safeguard principles. The model is trained on "
                          "de-identified public research data (MIT-BIH); HIPAA does not "
                          "apply to that dataset.",
    }
    with open(META_OUTPUT, "w") as f:
        json.dump(metadata, f, indent=2)
    log.info("Training metadata saved to %s", META_OUTPUT)


def main():
    log.info("CardioCare ML Trainer starting")
    log.info("Data dir   : %s", DATA_DIR)
    log.info("Models dir : %s", MODELS_DIR)


    # Step 1: Load dataset (MIT-BIH or synthetic fallback)

    X_train, y_train, X_test, y_test = load_mitbih_data()

    # Step 2: Train XGBoost

    model = train_xgboost(X_train, y_train)


    # Step 3: Evaluate on held-out test set

    metrics = evaluate_model(model, X_test, y_test)

    # Step 4: Save model + metadata to shared volume

    save_model(model, metrics)

    log.info("ML training complete — model ready for aiops-engine")
    log.info("Accuracy: %.1f%% | AUC-ROC: %.4f",
             metrics["accuracy"] * 100, metrics["auc_roc"])


if __name__ == "__main__":

    main()

