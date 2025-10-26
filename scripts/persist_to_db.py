#!/usr/bin/env python3
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
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine, text

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


def _utc_now_pg():
    return datetime.now(timezone.utc)


def _utc_now_sqlite_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


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


def _pick_yhat_column(df: pd.DataFrame) -> str:
    # 想定される列名の優先順
    for cand in ["yhat", "yhat_mean", "pred", "forecast"]:
        if cand in df.columns:
            return cand
    raise ValueError("predictions file must provide one of: yhat, yhat_mean, pred, forecast")


def _ensure_ds_series(df: pd.DataFrame) -> pd.Series:
    # ds 列が無い場合に備えて代替を探す
    for cand in ["ds", "timestamp", "date", "time"]:
        if cand in df.columns:
            s = pd.to_datetime(df[cand], utc=True, errors="coerce")
            if s.isna().all():
                break
            return s
    raise ValueError("predictions file must contain a datetime-like column (e.g., 'ds').")


def _prepare_predictions_rows_for_pg(run_id: str, df: pd.DataFrame) -> Iterable[dict]:
    ycol = _pick_yhat_column(df)
    ds = _ensure_ds_series(df)

    # tz-aware UTC の python datetime に変換
    ds_py = [ts.to_pydatetime() for ts in ds.dt.tz_convert("UTC")]

    uid = df["unique_id"].astype(str)
    yhat = pd.to_numeric(df[ycol], errors="coerce")

    mask = (~uid.isna()) & (~pd.isna(ds_py)) & (~yhat.isna())
    for u, d, y in zip(uid[mask], [d for i, d in enumerate(ds_py) if mask.iloc[i]], yhat[mask]):
        # d はすでに tz-aware(UTC)
        yield {"run_id": run_id, "unique_id": u, "ds": d, "yhat": float(y)}


def _prepare_predictions_rows_for_sqlite(run_id: str, df: pd.DataFrame) -> Iterable[dict]:
    ycol = _pick_yhat_column(df)
    ds = _ensure_ds_series(df)

    # SQLite には 'YYYY-MM-DD HH:MM:SS' の UTC 文字列で渡す
    ds_txt = ds.dt.tz_convert("UTC").dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")

    uid = df["unique_id"].astype(str)
    yhat = pd.to_numeric(df[ycol], errors="coerce")

    mask = (~uid.isna()) & (~ds_txt.isna()) & (~yhat.isna())
    for u, d, y in zip(uid[mask], ds_txt[mask], yhat[mask]):
        yield {"run_id": run_id, "unique_id": u, "ds": str(d), "yhat": float(y)}


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

            df = pd.read_parquet(pred_file)

            # unique_id 必須
            if "unique_id" not in df.columns:
                raise ValueError("predictions file must contain 'unique_id' column")

            # run_id 列がファイルにあっても、DB へは --run-json の run_id で上書きする
            # 既存行をクリア
            conn.execute(text("DELETE FROM predictions WHERE run_id = :rid"), {"rid": run_id})

            if dialect == "postgresql":
                rows = list(_prepare_predictions_rows_for_pg(run_id, df))
                if rows:
                    conn.execute(
                        text(
                            """
                            INSERT INTO predictions (run_id, unique_id, ds, yhat)
                            VALUES (:run_id, :unique_id, :ds, :yhat)
                            """
                        ),
                        rows,
                    )
            else:
                rows = list(_prepare_predictions_rows_for_sqlite(run_id, df))
                if rows:
                    conn.execute(
                        text(
                            """
                            INSERT INTO predictions (run_id, unique_id, ds, yhat)
                            VALUES (:run_id, :unique_id, :ds, :yhat)
                            """
                        ),
                        rows,
                    )

    # Useful in pipelines/tests
    print(run_id)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True, help="database DSN (postgresql://... or sqlite:///path.db)")
    ap.add_argument("--run-json", required=True, type=Path, help="outputs/last_run.json")
    ap.add_argument("--run-config", required=False, type=Path, help="configs/run_spec.yaml")
    ap.add_argument("--pred-file", required=False, type=Path, help="outputs/predictions.parquet")
    args = ap.parse_args(argv)
    persist(args.dsn, args.run_json, args.run_config, args.pred_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
