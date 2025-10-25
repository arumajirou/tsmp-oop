# tests/test_kpi_thresholds.py
import os, json, subprocess, sys, pathlib
import pandas as pd
ROOT = pathlib.Path(__file__).resolve().parents[1]

def run(cmd): return subprocess.run(cmd, cwd=ROOT, check=True, text=True, capture_output=True)

def test_kpi_thresholds(tmp_path):
    dsn = os.environ.get("POSTGRES_DSN", f"sqlite:///{ROOT/'ci.db'}")
    os.environ["POSTGRES_DSN"] = dsn
    os.environ["MLFLOW_TRACKING_URI"] = "file:./mlruns"

    # 実行（DB初期化→パイプライン→DB永続化まで一発）
    run([sys.executable, "scripts/setup_db.py", "--init", "--dsn", dsn, "--sql", "sql/init_schema.sql"])
    run([sys.executable, "scripts/cli.py", "run",
         "--run-config", "configs/run_spec.yaml",
         "--fe-config", "configs/fe_config.yaml",
         "--constraints", "configs/constraints.yaml",
         "--persist-db"])

    out = json.loads((ROOT/"outputs/last_run.json").read_text(encoding="utf-8"))
    assert out["duration_sec"] < 5.0, "duration too slow for smoke"

    # 期待件数 = ユニーク系列数 × horizon
    import yaml
    cfg = yaml.safe_load((ROOT/"configs/run_spec.yaml").read_text(encoding="utf-8"))
    horizon = int(cfg["horizon"])

    # データセットから系列数を推定（列名ゆるく対応）
    df = pd.read_parquet((ROOT/cfg["dataset_path"]))
    id_col = "unique_id" if "unique_id" in df.columns else ("series" if "series" in df.columns else None)
    assert id_col is not None, "cannot infer id column"
    n_series = df[id_col].nunique()

    assert out["predictions_rows"] == n_series * horizon, "predictions size mismatch"
