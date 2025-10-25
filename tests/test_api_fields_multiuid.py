import os, subprocess, sys, pathlib
ROOT = pathlib.Path(__file__).resolve().parents[1]
def run(cmd): return subprocess.run(cmd, cwd=ROOT, check=True, text=True, capture_output=True)

def setup_module(_m):
    dsn = os.environ.get("POSTGRES_DSN", f"sqlite:///{ROOT/'ci_api_fields.db'}")
    os.environ["POSTGRES_DSN"] = dsn
    os.environ["MLFLOW_TRACKING_URI"] = "file:./mlflowsim"
    run([sys.executable, "scripts/setup_db.py", "--init", "--dsn", dsn, "--sql", "sql/init_schema.sql"])
    run([sys.executable, "scripts/cli.py", "run",
         "--run-config", "configs/run_spec.yaml",
         "--fe-config", "configs/fe_config.yaml",
         "--constraints", "configs/constraints.yaml",
         "--persist-db", "--dsn", dsn])

def test_fields_and_multiuid():
    import importlib, tsmp.api.app as api
    from fastapi.testclient import TestClient
    importlib.reload(api)
    c = TestClient(api.app)

    # fields で列を絞る
    r = c.get("/runs/window", params={"start":"2000-01-01T00:00:00Z", "limit":1, "fields":"run_id,created_at,n_predictions"})
    assert r.status_code == 200
    item = r.json()["items"][0]
    assert set(item.keys()) == {"run_id","created_at","n_predictions"}
    assert item["created_at"].endswith("Z")

    # multi uid: まず2件取得
    rid = c.get("/runs/latest").json()["run_id"]
    items = c.get("/predictions", params={"run_id": rid, "limit": 5}).json()["items"]
    uids = sorted({it["unique_id"] for it in items})[:2]
    # 配列指定
    r2 = c.get("/predictions", params=[("run_id", rid)] + [("unique_id", u) for u in uids] + [("limit", 100)])
    assert r2.status_code == 200
    for it in r2.json()["items"]:
        assert it["unique_id"] in uids
    # カンマ指定
    r3 = c.get("/predictions", params={"run_id": rid, "unique_id": ",".join(uids), "limit": 100})
    assert r3.status_code == 200
    for it in r3.json()["items"]:
        assert it["unique_id"] in uids
