"""
CardioCare-AIOps — FastAPI API Gateway
Author  : Muhammad Raees (raees.malik89@gmail.com)
Course  : Diploma in Artificial Intelligence Operations — EduQual Level 6 (DAIOL6)
Topic   : Topic 7 — Serverless Architectures with Event-Driven AIOps,
          Observability and Security Integration

Purpose:
    Provides the REST API layer for the CardioCare-AIOps platform, enabling
    external clients (clinical dashboards, mobile apps, EHR systems) to query
    patient vitals, detected anomalies, and critical alerts.

Security layers (multi-tier, defence-in-depth):
    1. Keycloak JWT bearer token — authenticates the calling user.
    2. OPA policy check        — authorises the specific resource+action pair.
    This two-layer approach satisfies ISO 27001 A.9.4 and NIST CSF PR.AC-4.

Observability:
    Every request is traced with OpenTelemetry and exported to Jaeger.
    The /metrics endpoint exposes Prometheus counters and histograms so
    Grafana can visualise API throughput and latency alongside cardiac data.

Design note — why FastAPI over Flask:
    FastAPI provides native async support (important for non-blocking Keycloak
    and OPA calls), automatic OpenAPI documentation, and built-in Pydantic
    validation.  For a real-time clinical system these properties reduce the
    risk of blocking the event loop under burst load.
"""

import os
import json
import time
import logging
import threading
from typing import Optional
from datetime import datetime, timezone
from collections import deque

import httpx
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from fastapi import FastAPI, Depends, HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from crypto import decrypt_event

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
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
VITALS_TOPIC = os.getenv("VITALS_TOPIC", "cardiac.vitals.stream")
ANOMALY_TOPIC = os.getenv("ANOMALY_TOPIC", "cardiac.anomalies.detected")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "cardiac.alerts.critical")
ENABLE_API_KAFKA_CONSUMER = os.getenv("ENABLE_API_KAFKA_CONSUMER", "true").lower() == "true"
ENABLE_TRACING = os.getenv("ENABLE_TRACING", "true").lower() == "true"
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
    if origin.strip()
]

# ── OpenTelemetry Tracing ─────────────────────────────────────────────────────
# service.name makes traces appear as "cardiocare-api" in Jaeger (not unknown_service)
provider = TracerProvider(resource=Resource.create({"service.name": "cardiocare-api"}))
if ENABLE_TRACING:
    try:
        exporter = OTLPSpanExporter(endpoint=JAEGER_ENDPOINT, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    except Exception:
        log.exception("OpenTelemetry exporter setup failed")
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
store_lock = threading.Lock()

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
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

if ENABLE_TRACING:
    FastAPIInstrumentor.instrument_app(app)

bearer_scheme = HTTPBearer(auto_error=False)


async def verify_token(credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme)):
    """Validate a bearer token with Keycloak. Authentication fails closed."""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(
                f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/userinfo",
                headers={"Authorization": f"Bearer {token}"}
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
            user = resp.json()
            realm_roles = user.get("realm_access", {}).get("roles", [])
            user["roles"] = sorted(set(user.get("roles", []) + realm_roles))
            return user
    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        log.error("Keycloak validation unavailable: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service unavailable",
        ) from exc


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
            resp.raise_for_status()
            return resp.json().get("result", False) is True
    except (httpx.HTTPError, ValueError) as exc:
        log.error("OPA authorization unavailable: %s", exc)
        return False


async def require_access(user: dict, resource: str, action: str = "read") -> None:
    if not await opa_check(user, resource, action):
        raise HTTPException(status_code=403, detail="Access denied by policy")


def consume_events() -> None:
    """Populate the API's bounded demo stores from Kafka event topics."""
    while True:
        try:
            consumer = KafkaConsumer(
                VITALS_TOPIC,
                ANOMALY_TOPIC,
                ALERT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=decrypt_event,
                group_id="cardiocare-api-view",
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            log.info("API event view connected to Kafka")
            for message in consumer:
                event = message.value
                with store_lock:
                    if message.topic == VITALS_TOPIC:
                        patient_id = event.get("patient_id")
                        if patient_id:
                            vitals_store.setdefault(patient_id, deque(maxlen=100)).append(event)
                    elif message.topic == ANOMALY_TOPIC:
                        anomaly_store.append(event)
                    elif message.topic == ALERT_TOPIC:
                        alert_store.append(event)
        except NoBrokersAvailable:
            log.warning("Kafka unavailable to API event view; retrying")
            time.sleep(5)
        except Exception:
            log.exception("API Kafka consumer failed; retrying")
            time.sleep(3)


@app.on_event("startup")
def start_event_consumer() -> None:
    if ENABLE_API_KAFKA_CONSUMER:
        threading.Thread(target=consume_events, daemon=True).start()


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
        await require_access(user, "patients", "list")
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
        await require_access(user, "vitals")
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
        await require_access(user, "anomalies")
        data = list(anomaly_store)
        if severity:
            data = [a for a in data if a.get("severity") == severity.upper()]
        api_requests.labels(method="GET", endpoint="/api/v1/anomalies", status="200").inc()
        return {"anomalies": data[-limit:], "total": len(data)}


@app.get("/api/v1/alerts", tags=["aiops"])
async def get_alerts(limit: int = 20, user: dict = Depends(verify_token)):
    with tracer.start_as_current_span("get_alerts"):
        await require_access(user, "alerts")
        api_requests.labels(method="GET", endpoint="/api/v1/alerts", status="200").inc()
        return {"alerts": list(alert_store)[-limit:], "total": len(alert_store)}


@app.get("/api/v1/system/status", tags=["system"])
async def system_status(user: dict = Depends(verify_token)):
    await require_access(user, "system")
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
