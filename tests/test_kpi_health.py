import os, json, subprocess, sys, pathlib, statistics
from sqlalchemy import create_engine, text

ROOT = pathlib.Path(__file__).resolve().parents[1]
def run(cmd): return subprocess.run(cmd, cwd=ROOT, check=True, text=True, capture_output=True)

def percentile(data, q):
    if not data: return 0.0
    data = sorted(data)
    k = (len(data) - 1) * q
    f, c = int(k), int(k)+1
    if c >= len(data): return float(data[-1])
    d0 = data[f] * (c - k)
    d1 = data[c] * (k - f)
    return float(d0 + d1)

def test_kpi_thresholds(tmp_path):
    dsn = os.environ.get("POSTGRES_DSN", f"sqlite:///{ROOT/'ci.db'}")
    os.environ["POSTGRES_DSN"] = dsn
    os.environ["MLFLOW_TRACKING_URI"] = "file:./mlruns"

    # スキーマ → run → 永続化
    run([sys.executable, "scripts/setup_db.py", "--init", "--dsn", dsn, "--sql", "sql/init_schema.sql"])
    run([sys.executable, "scripts/cli.py", "run",
         "--run-config", "configs/run_spec.yaml",
         "--fe-config", "configs/fe_config.yaml",
         "--constraints", "configs/constraints.yaml",
         "--persist-db", "--dsn", dsn])

    eng = create_engine(dsn, pool_pre_ping=True)
    with eng.begin() as conn:
        durations = [row[0] for row in conn.execute(text("select duration_sec from runs where duration_sec is not null")).all()]
        p95 = percentile(durations, 0.95)
        assert p95 <= 5.0  # 秒

        # 最新 run の期待予測数充足率（= 実件数 / (系列数*H)）
        row = conn.execute(text("""
          WITH latest AS (
            SELECT run_id, (config #>> '{config,horizon}') as horizon_txt
            FROM runs ORDER BY created_at DESC LIMIT 1
          ),
          s AS (SELECT count(DISTINCT p.unique_id) AS n_series
                FROM predictions p JOIN latest l ON p.run_id=l.run_id),
          c AS (SELECT count(*) AS n_pred
                FROM predictions p JOIN latest l ON p.run_id=l.run_id)
          SELECT CAST(l.horizon_txt AS INT) AS h, s.n_series, c.n_pred
          FROM latest l CROSS JOIN s CROSS JOIN c
        """)).first()
        if row:
            h, n_series, n_pred = row
            if h and n_series:
                assert n_pred >= h * n_series
