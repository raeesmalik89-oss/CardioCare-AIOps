FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY train.py .
# Trains XGBoost on MIT-BIH; mount data at /data, model written to /models
CMD ["python", "train.py"]
