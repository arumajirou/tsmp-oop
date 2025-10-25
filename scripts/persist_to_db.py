#!/usr/bin/env python
import argparse, os, json
from pathlib import Path
from datetime import datetime
import yaml
import pandas as pd
from sqlalchemy import create_engine, text

def upsert_run(engine, run_id, alias, model_name, dataset, status, duration_sec, config_dict):
    payload = {
        "run_id": run_id,
        "alias": alias,
        "model_name": model_name,
        "dataset": dataset,
        "status": status,
        "duration_sec": float(duration_sec) if duration_sec is not None else None,
        "config": json.dumps(config_dict, ensure_ascii=False),
    }
    sql = text("""
    INSERT INTO runs (run_id, alias, model_name, dataset, status, duration_sec, config, updated_at)
    VALUES (:run_id, :alias, :model_name, :dataset, :status, :duration_sec, :config, CURRENT_TIMESTAMP)
    ON CONFLICT (run_id) DO UPDATE SET
      alias = EXCLUDED.alias,
      model_name = EXCLUDED.model_name,
      dataset = EXCLUDED.dataset,
      status = EXCLUDED.status,
      duration_sec = EXCLUDED.duration_sec,
      config = EXCLUDED.config,
      updated_at = CURRENT_TIMESTAMP
    """)
    with engine.begin() as conn:
        conn.execute(sql, payload)

def upsert_predictions(engine, df: pd.DataFrame, run_id: str):
    # 期待スキーマ: unique_id(str), ds(datetime-like), y_hat(float)
    # df に run_id を足して UPSERT
    records = []
    for row in df.itertuples(index=False):
        d = row._asdict() if hasattr(row, "_asdict") else dict(row._asdict())
        # 柔軟対応
        uid = d.get("unique_id") or d.get("series") or d.get("id")
        ds = pd.to_datetime(d.get("ds") or d.get("date") or d.get("timestamp"))
        y = d.get("y_hat") or d.get("yhat") or d.get("forecast") or d.get("value")
        if uid is None or pd.isna(ds) or y is None:
            continue
        records.append({"run_id": run_id, "unique_id": str(uid), "ds": ds.to_pydatetime(), "y_hat": float(y)})

    if not records:
        print("[persist] predictions: no compatible rows; skip")
        return

    sql = text("""
    INSERT INTO predictions (run_id, unique_id, ds, y_hat)
    VALUES (:run_id, :unique_id, :ds, :y_hat)
    ON CONFLICT (run_id, unique_id, ds) DO UPDATE SET
      y_hat = EXCLUDED.y_hat
    """)
    with engine.begin() as conn:
        conn.execute(sql, records)

def main():
    ap = argparse.ArgumentParser(description="Persist run & optional predictions to Postgres")
    ap.add_argument("--dsn", default=os.getenv("POSTGRES_DSN", "postgresql:///tsmodeling"))
    ap.add_argument("--run-json", default="outputs/last_run.json")
    ap.add_argument("--run-config", default="configs/run_spec.yaml")
    ap.add_argument("--pred-file", default="outputs/predictions.parquet")
    args = ap.parse_args()

    run_json = Path(args.run_json)
    run_cfg = Path(args.run_config)

    if not run_json.exists():
        raise FileNotFoundError(f"{run_json} not found. Execute cli run first.")
    if not run_cfg.exists():
        raise FileNotFoundError(f"{run_cfg} not found.")

    result = json.loads(run_json.read_text(encoding="utf-8"))
    cfg = yaml.safe_load(run_cfg.read_text(encoding="utf-8"))

    run_id = str(result.get("run_id"))
    duration = result.get("duration_sec")
    alias = cfg.get("alias", "run")
    model_name = cfg.get("model_name", "unknown_model")
    dataset = cfg.get("dataset_path", "unknown_dataset")
    status = "SUCCEEDED"

    engine = create_engine(args.dsn, pool_pre_ping=True)

    # runs upsert
    cfg_subset = {
        "run": result,
        "config": {k: cfg.get(k) for k in ["alias","model_name","dataset_path","horizon","val_size","tuner_backend","instance_type","hyperparams"] if k in cfg}
    }
    upsert_run(engine, run_id, alias, model_name, dataset, status, duration, cfg_subset)
    print(f"[persist] runs upserted: run_id={run_id}")

    # predictions optional
    pred_path = Path(args.pred_file)
    if pred_path.exists():
        try:
            df = pd.read_parquet(pred_path)
            upsert_predictions(engine, df, run_id)
            print(f"[persist] predictions upserted from {pred_path}")
        except Exception as e:
            print(f"[persist] predictions skipped ({e})")
    else:
        print("[persist] predictions: file not found; skip")

if __name__ == "__main__":
    main()
