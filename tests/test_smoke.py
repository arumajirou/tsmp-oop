import os
os.environ.setdefault("POSTGRES_DSN", "sqlite:///ci.db")

from fastapi.testclient import TestClient
from tsmp.api.app import app

client = TestClient(app)

def test_health_ok():
    r = client.get("/health")
    assert r.status_code in (200, 503)
    body = r.json()
    assert "ok" in body and "checks" in body

def test_runs_latest_and_predictions():
    r = client.get("/runs/latest")
    assert r.status_code == 200
    rid = r.json()["run_id"]
    r2 = client.get("/predictions", params={"run_id": rid, "limit": 2})
    assert r2.status_code == 200
    j = r2.json()
    assert "items" in j and isinstance(j["items"], list)

def test_metrics_exposed():
    r = client.get("/metrics")
    assert r.status_code == 200
    assert b"http_requests_total" in r.content
