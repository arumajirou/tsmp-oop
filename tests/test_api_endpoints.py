import os, json, subprocess, sys, pathlib
from sqlalchemy import create_engine, text
ROOT = pathlib.Path(__file__).resolve().parents[1]
def run(cmd): return subprocess.run(cmd, cwd=ROOT, check=True, text=True, capture_output=True)

def setup_module(_m):
    dsn = os.environ.get("POSTGRES_DSN", f"sqlite:///{ROOT/'ci_api.db'}")
    os.environ["POSTGRES_DSN"] = dsn
    os.environ["MLFLOW_TRACKING_URI"] = "file:./mlruns"
    run([sys.executable, "scripts/setup_db.py", "--init", "--dsn", dsn, "--sql", "sql/init_schema.sql"])
    run([sys.executable, "scripts/cli.py", "run",
         "--run-config", "configs/run_spec.yaml",
         "--fe-config", "configs/fe_config.yaml",
         "--constraints", "configs/constraints.yaml",
         "--persist-db", "--dsn", dsn])

def test_runs_and_latest_and_predictions():
    import importlib, tsmp.api.app as api
    from fastapi.testclient import TestClient
    importlib.reload(api)
    c = TestClient(api.app)
    r1 = c.get("/runs", params={"limit": 2})
    assert r1.status_code == 200
    body = r1.json()
    assert body["count"] >= 1
    assert "n_predictions" in body["items"][0]
    # 最新
    r2 = c.get("/runs/latest")
    assert r2.status_code == 200
    latest = r2.json()
    assert "mlflow" in latest and "tracking_uri" in latest["mlflow"]
    run_id = latest["run_id"]
    # 予測（ISO8601 Z）
    r3 = c.get("/predictions", params={"run_id": run_id, "limit": 2})
    assert r3.status_code == 200
    items = r3.json()["items"]
    assert items and items[0]["ds"].endswith("Z")
    # unique_id フィルタ
    uid = items[0]["unique_id"]
    r4 = c.get("/predictions", params={"run_id": run_id, "unique_id": uid, "limit": 10})
    assert r4.status_code == 200
    for it in r4.json()["items"]:
        assert it["unique_id"] == uid

def test_health_ok_or_503():
    import tsmp.api.app as api
    from fastapi.testclient import TestClient
    c = TestClient(api.app)
    os.environ["KPI_DURATION_P95_MS"] = "5000"
    os.environ["KPI_PREDICTIONS_RATIO"] = "1.0"
    r = c.get("/health")
    assert r.status_code in (200, 503)
    j = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
    assert "ok" in j
