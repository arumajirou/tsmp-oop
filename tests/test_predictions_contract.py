import os, json, subprocess, sys, pathlib
from sqlalchemy import create_engine, text

ROOT = pathlib.Path(__file__).resolve().parents[1]

def run(cmd): return subprocess.run(cmd, cwd=ROOT, check=True, text=True, capture_output=True)

def test_e2e_predictions_persist(tmp_path):
    dsn = os.environ.get("POSTGRES_DSN", f"sqlite:///{ROOT/'ci.db'}")
    os.environ["POSTGRES_DSN"] = dsn
    os.environ["MLFLOW_TRACKING_URI"] = "file:./mlruns"

    run([sys.executable, "scripts/setup_db.py", "--init", "--dsn", dsn, "--sql", "sql/init_schema.sql"])

    # run（predictions.parquet は runner のフックで出力される設計）
    run([sys.executable, "scripts/cli.py", "run",
         "--run-config", "configs/run_spec.yaml",
         "--fe-config", "configs/fe_config.yaml",
         "--constraints", "configs/constraints.yaml"])

    out = json.loads((ROOT/"outputs/last_run.json").read_text(encoding="utf-8"))
    assert "run_id" in out
    # 永続化（pred-file 指定）
    run([sys.executable, "scripts/persist_to_db.py", "--dsn", dsn,
         "--run-json", "outputs/last_run.json",
         "--run-config", "configs/run_spec.yaml",
         "--pred-file", "outputs/predictions.parquet"])

    eng = create_engine(dsn, pool_pre_ping=True)
    with eng.begin() as conn:
        n_pred = conn.execute(text("select count(*) from predictions")).scalar_one()
        assert n_pred >= 1
