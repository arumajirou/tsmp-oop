#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Persist pipeline results to DB.

Rules:
- Never generate a new run_id. Always use --run-json (outputs/last_run.json).
- Upsert 'runs' row (PostgreSQL/SQLite dialect-aware).
- If --pred-file is given: DELETE all predictions for the run_id, then bulk INSERT.
- Timestamps are stored in UTC:
    - PostgreSQL: tz-aware (timestamptz)
    - SQLite    : 'YYYY-MM-DD HH:MM:SS' UTC string
- Config is normalized to {"config": {...}} (expects $.config.horizon).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Iterable, Optional, List, Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


# ---------- time utils ----------

def _utc_now_pg():
    return datetime.now(timezone.utc)

def _utc_now_sqlite_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def _to_utc(series) -> pd.Series:
    s = pd.to_datetime(series, utc=True, errors="coerce")
    return s.dt.tz_convert("UTC")

def _to_sqlite_utc_text(series_utc: pd.Series) -> pd.Series:
    return series_utc.dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")


# ---------- load / normalize ----------

def _load_last_run_json(p: Path) -> dict:
    d = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(d, dict) or not d.get("run_id"):
        raise ValueError(f"run_id missing in {p}")
    return d

def _load_run_config_yaml(p: Path | None) -> dict | None:
    if not p:
        return None
    if not p.exists():
        raise FileNotFoundError(str(p))
    if yaml is None:
        return None
    y = yaml.safe_load(p.read_text(encoding="utf-8"))
    if isinstance(y, dict):
        return y if "config" in y else {"config": y}
    return None

def _normalize_config(last_run: dict, cfg_from_yaml: dict | None) -> str:
    config_json = last_run.get("config")
    if cfg_from_yaml and not config_json:
        config_json = cfg_from_yaml
    if config_json is None:
        config_json = {"config": {}}
    return json.dumps(config_json, ensure_ascii=False)

def _read_predictions_frame(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".parquet":
        return pd.read_parquet(path)
    if ext == ".csv":
        return pd.read_csv(path)
    # 最後のフォールバック（Parquet優先）
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.read_csv(path)


# ---------- column detection ----------

def _detect_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in cols}
    for k in candidates:
        if k in lower:
            return lower[k]
    return None

_UID_CANDIDATES = ["unique_id", "uid", "series", "id"]
_TS_CANDIDATES  = ["ds", "timestamp", "time", "date"]
_VAL_CANDIDATES = [
    "yhat", "yhat_mean", "pred", "forecast",
    "y_pred", "prediction", "value", "point", "mean",
]

def _pick_uid_col(df: pd.DataFrame) -> str:
    c = _detect_col(df.columns.tolist(), _UID_CANDIDATES)
    if not c:
        raise ValueError("predictions file must have a unique_id-like column (unique_id|uid|series|id)")
    return c

def _pick_ts_series(df: pd.DataFrame) -> pd.Series:
    for cand in _TS_CANDIDATES:
        if cand in df.columns:
            s = pd.to_datetime(df[cand], utc=True, errors="coerce")
            if not s.isna().all():
                return s
    raise ValueError("predictions file must contain a datetime-like column (ds|timestamp|time|date)")

def _pick_value_col(df: pd.DataFrame) -> str:
    val = _detect_col(df.columns.tolist(), _VAL_CANDIDATES)
    if val:
        return val
    exclude = {
        "y", "y_true", "actual", "y_lo", "y_hi",
        "y_lower", "y_upper", "lower", "upper", "lo", "hi"
    }
    numeric = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    numeric = [c for c in numeric if c.lower() not in exclude]
    if numeric:
        return numeric[0]
    raise ValueError("predictions file must provide a numeric prediction column; "
                     "tried: " + ", ".join(_VAL_CANDIDATES))


# ---------- choose destination value column existing in table ----------

def _pick_dst_value_col(engine: Engine) -> str:
    dialect = engine.url.get_backend_name()
    with engine.begin() as conn:
        if dialect == "sqlite":
            rows = conn.execute(text("PRAGMA table_info(predictions)")).mappings().all()
            have = {r["name"] for r in rows}
        else:
            q = """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_name = 'predictions'
            """
            rows = conn.execute(text(q)).all()
            have = {r[0] for r in rows}
    for c in ["yhat", "yhat_mean", "pred", "forecast"]:
        if c in have:
            return c
    return "yhat"


# ---------- row builders (dict4固定: run_id, unique_id, ds, val) ----------

def _rows_for(engine: Engine, run_id: str, df: pd.DataFrame) -> Iterable[Dict[str, object]]:
    uid_col = _pick_uid_col(df)
    ts = _pick_ts_series(df)
    val_col = _pick_value_col(df)

    uid = df[uid_col].astype(str)
    ts_utc = _to_utc(ts)
    val = pd.to_numeric(df[val_col], errors="coerce")

    dialect = engine.url.get_backend_name()
    if dialect == "postgresql":
        ds_series = ts_utc.dt.to_pydatetime()
    else:
        ds_series = _to_sqlite_utc_text(ts_utc)

    mask = (~uid.isna()) & (~pd.Series(ds_series).isna()) & (~val.isna())
    # index を揃えて安全にイテレート
    for i in df.index[mask]:
        yield {
            "run_id": run_id,
            "unique_id": str(uid.loc[i]),
            "ds": ds_series[i],   # pg: datetime(tz=UTC)、sqlite: 'YYYY-MM-DD HH:MM:SS'
            "val": float(val.loc[i]),
        }


# ---------- main persist ----------

def persist(
    dsn: str,
    run_json: Path,
    run_config: Path | None,
    pred_file: Path | None,
):
    last = _load_last_run_json(run_json)
    cfg = _load_run_config_yaml(run_config)

    run_id = str(last["run_id"])
    status = str(last.get("status") or "SUCCEEDED")
    alias = str(last.get("alias") or run_id[:8])
    model_name = str(last.get("model_name") or "unknown")
    dataset = str(last.get("dataset") or "unknown")
    duration_sec = last.get("duration_sec")
    if duration_sec is not None:
        try:
            duration_sec = float(duration_sec)
        except Exception:
            duration_sec = None

    config_str = _normalize_config(last, cfg)

    eng = create_engine(dsn, pool_pre_ping=True)
    dialect = eng.url.get_backend_name()

    # ----- runs upsert -----
    if dialect == "postgresql":
        created_at = updated_at = _utc_now_pg()
        sql_runs = text(
            """
            INSERT INTO runs
                (run_id, alias, model_name, dataset, status,
                 duration_sec, created_at, updated_at, config)
            VALUES
                (:run_id, :alias, :model_name, :dataset, :status,
                 :duration_sec, :created_at, :updated_at, CAST(:config AS JSONB))
            ON CONFLICT (run_id) DO UPDATE SET
                alias = EXCLUDED.alias,
                model_name = EXCLUDED.model_name,
                dataset = EXCLUDED.dataset,
                status = EXCLUDED.status,
                duration_sec = EXCLUDED.duration_sec,
                updated_at = EXCLUDED.updated_at,
                config = EXCLUDED.config
            """
        )
        runs_params = {
            "run_id": run_id,
            "alias": alias,
            "model_name": model_name,
            "dataset": dataset,
            "status": status,
            "duration_sec": duration_sec,
            "created_at": created_at,
            "updated_at": updated_at,
            "config": config_str,
        }
    else:
        created_at = updated_at = _utc_now_sqlite_str()
        sql_runs = text(
            """
            INSERT INTO runs
                (run_id, alias, model_name, dataset, status,
                 duration_sec, created_at, updated_at, config)
            VALUES
                (:run_id, :alias, :model_name, :dataset, :status,
                 :duration_sec, :created_at, :updated_at, :config)
            ON CONFLICT(run_id) DO UPDATE SET
                alias = excluded.alias,
                model_name = excluded.model_name,
                dataset = excluded.dataset,
                status = excluded.status,
                duration_sec = excluded.duration_sec,
                updated_at = excluded.updated_at,
                config = excluded.config
            """
        )
        runs_params = {
            "run_id": run_id,
            "alias": alias,
            "model_name": model_name,
            "dataset": dataset,
            "status": status,
            "duration_sec": duration_sec,
            "created_at": created_at,
            "updated_at": updated_at,
            "config": config_str,
        }

    with eng.begin() as conn:
        conn.execute(sql_runs, runs_params)

        # ----- predictions ingest (optional) -----
        if pred_file:
            if not pred_file.exists():
                raise FileNotFoundError(str(pred_file))

            df = _read_predictions_frame(pred_file)
            # Always clear existing rows for this run_id
            conn.execute(text("DELETE FROM predictions WHERE run_id = :rid"), {"rid": run_id})

            if df is None or df.empty:
                print(run_id)
                return

            dst_val_col = _pick_dst_value_col(eng)
            rows = list(_rows_for(eng, run_id, df))
            if rows:
                # 名前付きバインド + 行辞書（4キー固定）
                conn.execute(
                    text(f"""
                        INSERT INTO predictions (run_id, unique_id, ds, {dst_val_col})
                        VALUES (:run_id, :unique_id, :ds, :val)
                    """),
                    rows,
                )

    # Useful in pipelines/tests
    print(run_id)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True, help="database DSN (postgresql://... or sqlite:///path.db)")
    ap.add_argument("--run-json", required=True, type=Path, help="outputs/last_run.json")
    ap.add_argument("--run-config", required=False, type=Path, help="configs/run_spec.yaml")
    ap.add_argument("--pred-file", required=False, type=Path, help="outputs/predictions.parquet or .csv")
    args = ap.parse_args(argv)
    persist(args.dsn, args.run_json, args.run_config, args.pred_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
