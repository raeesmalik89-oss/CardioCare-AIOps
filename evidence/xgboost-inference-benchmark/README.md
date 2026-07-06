# XGBoost Single-Beat Inference Latency — Real Benchmark

Verifies the "sub-10ms CPU inference" claim on Slide 27 (Key Engineering
Trade-offs: XGBoost vs. Deep Learning) against the actual committed model
artifact (`models/xgboost_ecg_classifier.json`), rather than relying on a
general claim about XGBoost as a model family.

## How it was run

```bash
pip install -r services/aiops-engine/requirements.txt
python scripts/benchmark_xgboost_inference.py
```

The script loads the real, committed model file and times 2,000 single-beat
`predict_proba()` calls, using the identical input shape and dtype
`classify_beat()` uses in `services/aiops-engine/detector.py`
(`np.float32`, shape `(1, 187)`), after a 20-call warm-up. It does not modify
the model or touch the running system in any way.

## Result (captured 2026-07-06, ordinary CPU, no GPU)

| Statistic | Latency |
|---|---|
| Minimum | 0.71 ms |
| Median | 0.79 ms |
| Mean | 0.83 ms |
| p95 | 1.08 ms |
| **p99** | **1.37 ms** |
| Maximum (worst of 2,000 runs) | 3.11 ms |

**Under 10ms at p99? YES** — by a wide margin. Even the single worst run out
of 2,000 (3.11 ms) came in roughly 3x under the 10ms claim, and around 39x
under the project's 120ms end-to-end alert-latency budget.

## What this does and doesn't prove

- **Proves:** the trained model itself, loaded from the exact file committed
  to this repo, genuinely classifies one ECG beat in a small fraction of a
  millisecond on ordinary CPU — the "sub-10ms" claim is not just a general
  property of XGBoost as a model family, it holds for *this specific,
  trained* model.
- **Doesn't prove:** end-to-end alert latency in production. This measures
  only the `predict_proba()` call in isolation, not decryption, Kafka
  handoff, or the alert function's own execution time. The closest real,
  measured end-to-end figure is `cardiocare_processing_seconds` (the
  Prometheus histogram in `detector.py`, ~46ms p99 per the deck), which
  covers the full per-event pipeline and is what should be cited for an
  end-to-end latency claim specifically.
