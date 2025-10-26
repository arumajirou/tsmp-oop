#!/usr/bin/env python3
"""
Persist pipeline results to DB using the run_id from outputs/last_run.json.

- PRIMARY RULE: Never generate a new run_id here. Always use --run-json.
- UPSERT behavior:
    - PostgreSQL: ON CONFLICT (run_id) DO UPDATE ...
    - SQLite    : ON CONFLICT(run_id) DO UPDATE ...
- Timestamps:
    - PostgreSQL: timezone-aware UTC datetime
    - SQLite    : 'YYYY-MM-DD HH:MM:SS' (UTC) textual datetime
- Config column:
    - Ensure JSON like {"config": {...}} so that $.config.horizon works.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


def _utc_now_pg():
    # tz-aware for Postgres timestamptz
    return datetime.now(timezone.utc)


def _utc_now_sqlite_str():
    # textual UTC 'YYYY-MM-DD HH:MM:SS' for SQLite
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
        # optional dependency, but tests usually have it
        return None
    y = yaml.safe_load(p.read_text(encoding="utf-8"))
    if isinstance(y, dict):
        # Ensure shape has top-level "config" so $.config.horizon works
        return y if "config" in y else {"config": y}
    return None


def persist(dsn: str, run_json: Path, run_config: Path | None):
    last = _load_last_run_json(run_json)
    cfg = _load_run_config_yaml(run_config)

    run_id = str(last["run_id"])
    status = str(last.get("status") or "SUCCEEDED")

    # Optional fields with sane defaults
    alias = str(last.get("alias") or run_id[:8])
    model_name = str(last.get("model_name") or "unknown")
    dataset = str(last.get("dataset") or "unknown")
    duration_sec = last.get("duration_sec")
    if duration_sec is not None:
        try:
            duration_sec = float(duration_sec)
        except Exception:
            duration_sec = None

    # config JSON to store in 'runs.config'
    config_json = last.get("config")
    if cfg and not config_json:
        config_json = cfg
    # If still None, store at least an empty config with "config":{}
    if config_json is None:
        config_json = {"config": {}}
    config_str = json.dumps(config_json, ensure_ascii=False)

    eng = create_engine(dsn, pool_pre_ping=True)
    dialect = eng.url.get_backend_name()

    if dialect == "postgresql":
        created_at = updated_at = _utc_now_pg()
        sql = text(
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
        params = {
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
        # SQLite: store UTC textual timestamps 'YYYY-MM-DD HH:MM:SS'
        created_at = updated_at = _utc_now_sqlite_str()
        sql = text(
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
        params = {
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
        conn.execute(sql, params)

    # Optional: print the persisted run_id to stdout (useful in scripts)
    print(run_id)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True, help="database DSN (postgresql://... or sqlite:///path.db)")
    ap.add_argument("--run-json", required=True, type=Path, help="outputs/last_run.json")
    ap.add_argument("--run-config", required=False, type=Path, help="configs/run_spec.yaml")
    args = ap.parse_args(argv)
    persist(args.dsn, args.run_json, args.run_config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
