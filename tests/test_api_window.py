import os, subprocess, sys, pathlib, datetime as dt
ROOT = pathlib.Path(__file__).resolve().parents[1]
def run(cmd): return subprocess.run(cmd, cwd=ROOT, check=True, text=True, capture_output=True)

def setup_module(_m):
    dsn = os.environ.get("POSTGRES_DSN", f"sqlite:///{ROOT/'ci_api_window.db'}")
    os.environ["POSTGRES_DSN"] = dsn
    os.environ["MLFLOW_TRACKING_URI"] = "file:./mlruns"
    run([sys.executable, "scripts/setup_db.py", "--init", "--dsn", dsn, "--sql", "sql/init_schema.sql"])
    run([sys.executable, "scripts/cli.py", "run",
         "--run-config", "configs/run_spec.yaml",
         "--fe-config", "configs/fe_config.yaml",
         "--constraints", "configs/constraints.yaml",
         "--persist-db", "--dsn", dsn])

def test_runs_window_filters():
    import importlib, tsmp.api.app as api
    from fastapi.testclient import TestClient
    importlib.reload(api)
    c = TestClient(api.app)

    # 未来 → 0件
    fut = "2999-01-01T00:00:00Z"
    r0 = c.get("/runs/window", params={"start": fut})
    assert r0.status_code == 200 and r0.json()["count"] == 0

    # 過去 → 1件以上
    past = "2000-01-01T00:00:00Z"
    r1 = c.get("/runs/window", params={"start": past, "limit": 1})
    assert r1.status_code == 200 and r1.json()["count"] >= 1

    # /health に thresholds が入っていること
    h = c.get("/health")
    assert h.status_code in (200,503)
    body = h.json()
    assert "thresholds" in body and "duration_p95_ms" in body["thresholds"]
