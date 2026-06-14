# CardioCare-AIOps — XGBoost ML Trainer

Offline benchmark model that validates the live IsolationForest + clinical-rules engine.

- **Dataset:** MIT-BIH Arrhythmia (Kaggle `shayanfazeli/heartbeat`) — 187 features, 5 AAMI classes
- **Result:** 87,554 train / 21,892 test, accuracy **97.27%**, weighted **AUC-ROC 0.9927**
- **Outputs:** `models/xgboost_ecg_classifier.json` + `models/training_metadata.json`
- **Prebuilt image:** `mraees1989/cardiocare-ml-trainer:v1.0` (Docker Hub)

## Reproduce
```bash
# place mitbih_train.csv / mitbih_test.csv under ./data
docker build -t cardiocare-ml-trainer services/ml-trainer
docker run --rm -v "$PWD/data:/data" -v "$PWD/models:/models" cardiocare-ml-trainer
```
