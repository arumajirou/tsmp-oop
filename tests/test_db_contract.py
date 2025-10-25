import os, json, subprocess, sys, pathlib, time
from sqlalchemy import create_engine, text

ROOT = pathlib.Path(__file__).resolve().parents[1]

def run(cmd):
    return subprocess.run(cmd, cwd=ROOT, check=True, text=True, capture_output=True)

def test_e2e_runs_persist(tmp_path):
    # DSN: CI では sqlite、ローカルでは環境変数を尊重。無ければ sqlite にする。
    dsn = os.environ.get("POSTGRES_DSN", f"sqlite:///{ROOT/'ci.db'}")
    os.environ["POSTGRES_DSN"] = dsn
    os.environ["MLFLOW_TRACKING_URI"] = "file:./mlruns"

    # DB 初期化（スキーマ）
    run([sys.executable, "scripts/setup_db.py", "--init", "--dsn", dsn, "--sql", "sql/init_schema.sql"])

    # パイプライン実行（last_run.json を生成）
    run([sys.executable, "scripts/cli.py", "run",
         "--run-config", "configs/run_spec.yaml",
         "--fe-config", "configs/fe_config.yaml",
         "--constraints", "configs/constraints.yaml"])

    # last_run.json の存在と整合チェック
    out = json.loads((ROOT/"outputs/last_run.json").read_text(encoding="utf-8"))
    assert "run_id" in out and out["run_id"], "run_id missing"
    run_id = out["run_id"]

    # DB に書き戻し
    run([sys.executable, "scripts/persist_to_db.py", "--dsn", dsn,
         "--run-json", "outputs/last_run.json",
         "--run-config", "configs/run_spec.yaml"])

    # 確認
    eng = create_engine(dsn, pool_pre_ping=True)
    with eng.begin() as conn:
        n_runs = conn.execute(text("select count(*) from runs")).scalar_one()
        assert n_runs >= 1
        one = conn.execute(text("select run_id, status from runs order by created_at desc limit 1")).first()
        assert one is not None and one.run_id == run_id and one.status == "SUCCEEDED"
