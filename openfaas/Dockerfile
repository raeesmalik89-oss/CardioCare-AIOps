FROM ghcr.io/openfaas/of-watchdog:0.10.7 AS watchdog

FROM python:3.11-slim

COPY --from=watchdog /fwatchdog /usr/bin/fwatchdog
RUN chmod +x /usr/bin/fwatchdog

WORKDIR /home/app
COPY services/alert-function/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/alert-function/handler.py services/alert-function/crypto.py ./

ENV fprocess="python -u handler.py"
ENV mode="http"
ENV upstream_url="http://127.0.0.1:5000"
ENV FUNCTION_MODE="true"
ENV write_timeout="10s"
ENV read_timeout="10s"

EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/_/health')"]
CMD ["fwatchdog"]
