FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY train.py .
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser
# Trains XGBoost on MIT-BIH; mount data at /data, model written to /models.
# If the host ./data or ./models directories are owned by a different uid on
# native Linux, run with `docker run --user "$(id -u):$(id -g)" ...` to override.
CMD ["python", "train.py"]
