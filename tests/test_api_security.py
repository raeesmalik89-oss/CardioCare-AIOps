import os
import sys
import types
from pathlib import Path

from fastapi.testclient import TestClient


API_DIR = Path(__file__).resolve().parents[1] / "services" / "api"
sys.path.insert(0, str(API_DIR))


class FakeKafkaConsumer:
    def __init__(self, *args, **kwargs):
        pass

    def __iter__(self):
        return iter(())


kafka_module = types.ModuleType("kafka")
kafka_module.KafkaConsumer = FakeKafkaConsumer
kafka_errors = types.ModuleType("kafka.errors")
kafka_errors.NoBrokersAvailable = RuntimeError
sys.modules["kafka"] = kafka_module
sys.modules["kafka.errors"] = kafka_errors

os.environ.setdefault(
    "EVENT_ENCRYPTION_KEY",
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
)
os.environ["ENABLE_API_KAFKA_CONSUMER"] = "false"
os.environ["ENABLE_TRACING"] = "false"

from main import app  # noqa: E402


client = TestClient(app)


def test_health_is_public():
    response = client.get("/health")
    assert response.status_code == 200


def test_patient_data_requires_bearer_token():
    response = client.get("/api/v1/patients")
    assert response.status_code == 401


def test_system_status_requires_bearer_token():
    response = client.get("/api/v1/system/status")
    assert response.status_code == 401
