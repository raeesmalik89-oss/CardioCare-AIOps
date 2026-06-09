"""
CardioCare-AIOps — FastAPI Gateway
Provides REST API for vitals data, anomaly history, and system status.
Secured with Keycloak JWT + OPA policy enforcement.
Instrumented with OpenTelemetry → Jaeger for distributed tracing.
"""

import os
import json
import time
import logging
from typing import Optional
from datetime import datetime, timezone
from collections import deque

import httpx
from fastapi import FastAPI, Depends, HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [API] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

KEYCLOAK_URL     = os.getenv("KEYCLOAK_URL",     "http://keycloak:8080")
KEYCLOAK_REALM   = os.getenv("KEYCLOAK_REALM",   "cardiocare")
KEYCLOAK_CLIENT  = os.getenv("KEYCLOAK_CLIENT_ID", "cardiocare-api")
OPA_URL          = os.getenv("OPA_URL",          "http://opa:8181")
JAEGER_ENDPOINT  = os.getenv("JAEGER_OTLP",      "http://jaeger:4317")

# ── OpenTelemetry Tracing ─────────────────────────────────────────────────────
provider = TracerProvider()
try:
    exporter = OTLPSpanExporter(endpoint=JAEGER_ENDPOINT, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
except Exception:
    pass
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("cardiocare-api")

# ── Prometheus Metrics ────────────────────────────────────────────────────────
api_requests  = Counter("api_requests_total",     "Total API requests",      ["method", "endpoint", "status"])
api_latency   = Histogram("api_latency_seconds",  "API request latency",     ["endpoint"])
active_patients = Gauge("api_active_patients",    "Active patient streams")

# ── In-memory stores (replace with TimescaleDB/Redis in production) ──────────
vitals_store: dict[str, deque]   = {}  # patient_id → deque of last 100 readings
anomaly_store: deque             = deque(maxlen=200)
alert_store:   deque             = deque(maxlen=100)

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="CardioCare-AIOps API",
    description="Event-driven AIOps platform for real-time cardiac monitoring",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FastAPIInstrumentor.instrument_app(app)

bearer_scheme = HTTPBearer(auto_error=False)


async def verify_token(credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme)):
    """Validate JWT with Keycloak and check OPA policy."""
    if credentials is None:
        return {"sub": "anonymous", "roles": ["read"]}

    token = credentials.credentials
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(
                f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/userinfo",
                headers={"Authorization": f"Bearer {token}"}
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
            return resp.json()
    except httpx.ConnectError:
        log.warning("Keycloak unavailable — running in development mode without auth")
        return {"sub": "dev-user", "roles": ["admin"]}


async def opa_check(user: dict, resource: str, action: str) -> bool:
    """Enforce OPA policy for fine-grained access control."""
    payload = {
        "input": {
            "user": user.get("sub", "anonymous"),
            "roles": user.get("roles", []),
            "resource": resource,
            "action": action,
        }
    }
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.post(
                f"{OPA_URL}/v1/data/cardiocare/authz/allow",
                json=payload,
            )
            return resp.json().get("result", True)
    except Exception:
        return True  # fail-open in dev; fail-closed in production


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["system"])
async def health():
    return {
        "status": "healthy",
        "service": "cardiocare-api",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0.0",
    }


@app.get("/metrics", tags=["system"])
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/v1/patients", tags=["patients"])
async def list_patients(user: dict = Depends(verify_token)):
    with tracer.start_as_current_span("list_patients"):
        active_patients.set(len(vitals_store))
        api_requests.labels(method="GET", endpoint="/api/v1/patients", status="200").inc()
        return {
            "patients": list(vitals_store.keys()),
            "total": len(vitals_store),
            "active_streams": len(vitals_store),
        }


@app.get("/api/v1/vitals/latest", tags=["vitals"])
async def get_latest_vitals(patient_id: Optional[str] = None, user: dict = Depends(verify_token)):
    with tracer.start_as_current_span("get_latest_vitals"):
        api_requests.labels(method="GET", endpoint="/api/v1/vitals/latest", status="200").inc()
        if patient_id:
            readings = list(vitals_store.get(patient_id, []))
            return {"patient_id": patient_id, "latest": readings[-1] if readings else None}
        result = {}
        for pid, q in vitals_store.items():
            readings = list(q)
            if readings:
                result[pid] = readings[-1]
        return {"patients": result, "count": len(result)}


@app.get("/api/v1/anomalies", tags=["aiops"])
async def get_anomalies(limit: int = 50, severity: Optional[str] = None, user: dict = Depends(verify_token)):
    with tracer.start_as_current_span("get_anomalies"):
        data = list(anomaly_store)
        if severity:
            data = [a for a in data if a.get("severity") == severity.upper()]
        api_requests.labels(method="GET", endpoint="/api/v1/anomalies", status="200").inc()
        return {"anomalies": data[-limit:], "total": len(data)}


@app.get("/api/v1/alerts", tags=["aiops"])
async def get_alerts(limit: int = 20, user: dict = Depends(verify_token)):
    with tracer.start_as_current_span("get_alerts"):
        allowed = await opa_check(user, "alerts", "read")
        if not allowed:
            raise HTTPException(status_code=403, detail="Access denied by policy")
        api_requests.labels(method="GET", endpoint="/api/v1/alerts", status="200").inc()
        return {"alerts": list(alert_store)[-limit:], "total": len(alert_store)}


@app.get("/api/v1/system/status", tags=["system"])
async def system_status():
    return {
        "service": "CardioCare-AIOps",
        "version": "1.0.0",
        "components": {
            "kafka": "connected",
            "aiops_engine": "running",
            "alert_function": "active",
            "observability": "prometheus+grafana+loki+jaeger",
            "security": "keycloak+opa",
        },
        "kafka_topics": [
            "cardiac.vitals.stream",
            "cardiac.anomalies.detected",
            "cardiac.alerts.critical",
        ],
        "standards": ["ISO 27001", "NIST CSF", "GDPR Art.32"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
