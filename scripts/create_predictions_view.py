#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Create/refresh predictions_view(run_id, unique_id, ds, yhat)
- SQLite/PG 両対応
- predictions テーブルを内省し、値列（yhat|yhat_mean|pred|forecast の優先順、
  無ければ「数値型」列の先頭）を yhat としてエイリアスするビューを作成
"""
from __future__ import annotations
import argparse
from sqlalchemy import create_engine, text

PREFERRED = ["yhat", "yhat_mean", "pred", "forecast"]

def _pick_value_col_sqlite(conn) -> str:
    rows = conn.execute(text("PRAGMA table_info(predictions)")).mappings().all()
    names = [r["name"] for r in rows]
    # 1) 優先候補
    for c in PREFERRED:
        if c in names:
            return c
    # 2) 数値型っぽい列（REAL / INT / NUMERIC / DEC）
    numeric = []
    for r in rows:
        t = (r["type"] or "").upper()
        if any(k in t for k in ["REAL", "INT", "NUM", "DEC", "DOUBLE", "FLOAT"]):
            numeric.append(r["name"])
    for c in numeric:
        if c not in ("run_id", "unique_id", "ds"):
            return c
    raise RuntimeError("numeric value column not found in predictions (sqlite)")

def _pick_value_col_postgres(conn) -> str:
    q = """
    SELECT column_name, data_type
      FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'predictions'
     ORDER BY ordinal_position
    """
    rows = conn.execute(text(q)).all()
    names = [r[0] for r in rows]
    # 1) 優先候補
    for c in PREFERRED:
        if c in names:
            return c
    # 2) 数値 data_type
    numeric_types = {"integer","bigint","smallint","real","double precision","numeric","decimal"}
    for name, dt in rows:
        if name in ("run_id","unique_id","ds"):
            continue
        if (dt or "").lower() in numeric_types:
            return name
    raise RuntimeError("numeric value column not found in predictions (postgres)")

def create_view(dsn: str) -> str:
    eng = create_engine(dsn, pool_pre_ping=True)
    dialect = eng.url.get_backend_name()
    with eng.begin() as conn:
        if dialect == "sqlite":
            val_col = _pick_value_col_sqlite(conn)
            conn.execute(text("DROP VIEW IF EXISTS predictions_view"))
            conn.execute(text(f"""
                CREATE VIEW predictions_view AS
                SELECT run_id, unique_id, ds, {val_col} AS yhat
                  FROM predictions
            """))
        else:
            val_col = _pick_value_col_postgres(conn)
            conn.execute(text("""
                CREATE OR REPLACE VIEW public.predictions_view AS
                SELECT run_id, unique_id, ds, {val} AS yhat
                  FROM public.predictions
            """.format(val=val_col)))
    return val_col

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    args = ap.parse_args()
    used = create_view(args.dsn)
    print(f"[create_predictions_view] yhat := {used}")

if __name__ == "__main__":
    main()
