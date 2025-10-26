#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Persist pipeline results to DB.

Rules:
- Never generate a new run_id. Always use --run-json (outputs/last_run.json).
- Upsert 'runs' row (PostgreSQL/SQLite 方言対応)。
- --pred-file が指定されたら predictions を「run_id 単位で全削除 → 一括 INSERT」。
- ds は UTC で保存：
    - PostgreSQL: tz-aware (timestamptz)
    - SQLite    : 'YYYY-MM-DD HH:MM:SS' の UTC 文字列
- config の形は {"config": {...}} に正規化（$.config.horizon を想定）。
- 予測の値列は yhat|yhat_mean|pred|forecast|y_pred|prediction|value|point|mean を優先順で自動検出。
  見つからない場合は、数値列からヒューリスティックに 1 本を選択（y,y_true,actual 等は除外）。
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


# ---------- 時刻ユーティリティ ----------

def _utc_now_pg():
    return datetime.now(timezone.utc)

def _utc_now_sqlite_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def _to_utc_aware(series: pd.Series) -> pd.Series:
    """任意の日時列を tz-aware UTC に正規化（pandas Series -> datetime64[ns, UTC]）"""
    s = pd.to_datetime(series, utc=True, errors="coerce")
    return s.dt.tz_convert("UTC")

def _to_sqlite_utc_text(series_utc: pd.Series) -> pd.Series:
    """UTC aware -> 'YYYY-MM-DD HH:MM:SS'（tz-naive文字列）"""
    return series_utc.dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")


# ---------- 入力ロード／正規化 ----------

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
    # フォーマット自動判定（まず Parquet、次に CSV）
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.read_csv(path)


# ---------- カラム自動検出（robust） ----------

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
    # 優先候補で探索
    val = _detect_col(df.columns.tolist(), _VAL_CANDIDATES)
    if val:
        return val
    # 数値列ヒューリスティック（よくある GT 列は除外）
    exclude = { "y", "y_true", "actual", "y_lo", "y_hi", "y_lower", "y_upper",
                "lower", "upper", "lo", "hi" }
    numeric = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    numeric = [c for c in numeric if c.lower() not in exclude]
    if numeric:
        return numeric[0]
    raise ValueError("predictions file must provide a numeric prediction column; "
                     "tried: " + ", ".join(_VAL_CANDIDATES))


# ---------- 予測値の挿入先カラムをテーブル実在列にマップ ----------

def _pick_dst_value_col(engine: Engine) -> str:
    """
    predictions テーブルに存在する値カラムを選択。
    典型: yhat / yhat_mean / pred / forecast のいずれか。
    無ければ yhat を使用（スキーマに依存）。
    """
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


# ---------- rows 準備（PG/SQLite） ----------

def _prepare_rows_pg(run_id: str, df: pd.DataFrame, dst_val_col: str) -> Iterable[dict]:
    uid_col = _pick_uid_col(df)
    ts = _pick_ts_series(df)              # tz-aware UTC
    val_col = _pick_value_col(df)

    uid = df[uid_col].astype(str)
    ts_utc = _to_utc_aware(ts)
    val = pd.to_numeric(df[val_col], errors="coerce")

    mask = (~uid.isna()) & (~ts_utc.isna()) & (~val.isna())
    uid = uid[mask]
    ts_utc = ts_utc[mask]
    val = val[mask]

    # Python tz-aware datetime で渡す
    for u, d, y in zip(uid, ts_utc.dt.to_pydatetime(), val):
        yield {"run_id": run_id, "unique_id": u, "ds": d, dst_val_col: float(y)}

def _prepare_rows_sqlite(run_id: str, df: pd.DataFrame, dst_val_col: str) -> Iterable[dict]:
    uid_col = _pick_uid_col(df)
    ts = _pick_ts_series(df)              # tz-aware UTC
    val_col = _pick_value_col(df)

    uid = df[uid_col].astype(str)
    ts_utc = _to_utc_aware(ts)
    ts_txt = _to_sqlite_utc_text(ts_utc)  # 'YYYY-MM-DD HH:MM:SS'
    val = pd.to_numeric(df[val_col], errors="coerce")

    mask = (~uid.isna()) & (~ts_txt.isna()) & (~val.isna())
    uid = uid[mask]
    ts_txt = ts_txt[mask]
    val = val[mask]

    for u, d, y in zip(uid, ts_txt, val):
        yield {"run_id": run_id, "unique_id": u, "ds": str(d), dst_val_col: float(y)}


# ---------- メイン永続化 ----------

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

    # ---------- runs UPSERT ----------
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

        # ---------- predictions ingest (optional) ----------
        if pred_file:
            if not pred_file.exists():
                raise FileNotFoundError(str(pred_file))

            df = _read_predictions_frame(pred_file)
            if df is None or df.empty:
                # 空なら何もしない（既存は全削除済みにしておくのが安全）
                conn.execute(text("DELETE FROM predictions WHERE run_id = :rid"), {"rid": run_id})
                print(run_id)
                return

            # run_id 指定のため、既存行をクリア
            conn.execute(text("DELETE FROM predictions WHERE run_id = :rid"), {"rid": run_id})

            dst_val_col = _pick_dst_value_col(eng)

            if dialect == "postgresql":
                rows = list(_prepare_rows_pg(run_id, df, dst_val_col))
                if rows:
                    conn.execute(
                        text(f"""
                            INSERT INTO predictions (run_id, unique_id, ds, {dst_val_col})
                            VALUES (:run_id, :unique_id, :ds, :{dst_val_col})
                        """),
                        rows,
                    )
            else:
                rows = list(_prepare_rows_sqlite(run_id, df, dst_val_col))
                if rows:
                    conn.execute(
                        text(f"""
                            INSERT INTO predictions (run_id, unique_id, ds, {dst_val_col})
                            VALUES (:run_id, :unique_id, :ds, :{dst_val_col})
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
