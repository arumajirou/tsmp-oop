import os, subprocess, sys, pathlib
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

def test_runs_latest_predictions_health():
    import importlib, tsmp.api.app as api
    from fastapi.testclient import TestClient
    importlib.reload(api)
    c = TestClient(api.app)

    r = c.get("/runs", params={"limit": 1})
    assert r.status_code == 200
    j = r.json()
    assert j["count"] >= 1
    it = j["items"][0]
    assert it["created_at"].endswith("Z") and "n_predictions" in it

    r2 = c.get("/runs/latest")
    assert r2.status_code == 200
    lat = r2.json()
    rid = lat["run_id"]
    assert "mlflow" in lat and "tracking_uri" in lat["mlflow"]

    r3 = c.get("/predictions", params={"run_id": rid, "limit": 2})
    assert r3.status_code == 200
    items = r3.json()["items"]
    if items:
        assert items[0]["ds"].endswith("Z")
        uid = items[0]["unique_id"]
        r4 = c.get("/predictions", params={"run_id": rid, "unique_id": uid, "limit": 10})
        assert r4.status_code == 200
        for it in r4.json()["items"]:
            assert it["unique_id"] == uid

    r5 = c.get("/health")
    assert r5.status_code in (200, 503)
    assert "ok" in r5.json()
