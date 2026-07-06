"""
CardioCare-AIOps -- Real XGBoost single-beat inference latency benchmark
Author: Muhammad Raees (raees.malik89@gmail.com)

Purpose:
    Verifies the "sub-10ms CPU inference" claim (deck Slide 27, Key Engineering
    Trade-offs) against the actual committed model artifact, rather than
    relying on a general claim about XGBoost. Loads models/xgboost_ecg_classifier.json
    read-only and times predict_proba() on realistic-shaped input, mirroring the
    exact call classify_beat() makes in services/aiops-engine/detector.py:
        x = np.array(beat[:187], dtype=np.float32).reshape(1, -1)
        proba = self.xgb.predict_proba(x)[0]

    Does not modify the model, the running system, or any other file.

Run with the same dependency versions aiops-engine uses in production:
    pip install -r services/aiops-engine/requirements.txt
    python scripts/benchmark_xgboost_inference.py
"""
import time
import statistics
import numpy as np
import xgboost as xgb

MODEL_PATH = "models/xgboost_ecg_classifier.json"
N_WARMUP = 20
N_RUNS = 2000

print(f"Loading model from {MODEL_PATH} ...")
model = xgb.XGBClassifier()
model.load_model(MODEL_PATH)
print("Model loaded.\n")

rng = np.random.default_rng(42)

# N_RUNS distinct realistic-shaped beats (187 samples each), same dtype/shape
# as the real code path in classify_beat().
beats = [rng.normal(loc=0.0, scale=0.3, size=187).astype(np.float32).reshape(1, -1)
          for _ in range(N_RUNS)]

# Warm-up (first calls can be slower due to lazy internal setup) -- excluded from the timed result.
for i in range(N_WARMUP):
    model.predict_proba(beats[i % len(beats)])

times_ms = []
for beat in beats:
    start = time.perf_counter()
    proba = model.predict_proba(beat)[0]
    elapsed_ms = (time.perf_counter() - start) * 1000
    times_ms.append(elapsed_ms)

times_ms.sort()
n = len(times_ms)
mean_ms = statistics.mean(times_ms)
median_ms = statistics.median(times_ms)
p95_ms = times_ms[int(n * 0.95)]
p99_ms = times_ms[int(n * 0.99)]
min_ms = times_ms[0]
max_ms = times_ms[-1]

print(f"Real single-beat inference latency over {n} runs (CPU only, no GPU):")
print(f"  min    : {min_ms:.4f} ms")
print(f"  median : {median_ms:.4f} ms")
print(f"  mean   : {mean_ms:.4f} ms")
print(f"  p95    : {p95_ms:.4f} ms")
print(f"  p99    : {p99_ms:.4f} ms")
print(f"  max    : {max_ms:.4f} ms")
print()
print(f"Under 10ms at p99? {'YES' if p99_ms < 10 else 'NO'}")
