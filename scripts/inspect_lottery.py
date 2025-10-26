#!/usr/bin/env python3
"""
Verify that we can retrieve per-lottery (game) data and infer n* (how many n-columns are used).
Outputs a summary table: game, cnt, min_ds, max_ds, n_cols, bonus_cols.
Exit codes:
  0: OK, 3: table missing, 4: missing columns, 5: empty table, 2: no DB URL
Usage:
  python scripts/inspect_lottery.py check  [--db URL]
  python scripts/inspect_lottery.py sample [--db URL] [--k 3]
Env:
  DATABASE_URL=postgresql://appuser:***@127.0.0.1:5432/appdb
"""
import os, sys, argparse
import pandas as pd
from sqlalchemy import create_engine, text

REQ_COLS = ['game','round','ds','n1','n2','n3','n4','n5','n6','n7','b1','b2','raw_json']

def get_engine(db_url: str):
    db = db_url or os.environ.get("DATABASE_URL")
    if not db:
        print("ERROR: set --db or DATABASE_URL", file=sys.stderr)
        sys.exit(2)
    return create_engine(db, pool_pre_ping=True)

def table_exists(engine, table='lottery_draws') -> bool:
    q = text("""
        SELECT EXISTS (
          SELECT 1 FROM information_schema.tables
          WHERE table_schema='public' AND table_name=:t
        )
    """)
    with engine.begin() as con:
        return bool(con.execute(q, {"t": table}).scalar())

def schema_missing(engine):
    q = text("""
      SELECT column_name FROM information_schema.columns
      WHERE table_schema='public' AND table_name='lottery_draws'
    """)
    with engine.begin() as con:
        have = {r[0] for r in con.execute(q)}
    return [c for c in REQ_COLS if c not in have]

def df_summary(engine) -> pd.DataFrame:
    q = text("""
    SELECT
      game,
      COUNT(*) AS cnt,
      MIN(ds) AS min_ds,
      MAX(ds) AS max_ds,
      CASE
        WHEN MAX(CASE WHEN n7 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 7
        WHEN MAX(CASE WHEN n6 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 6
        WHEN MAX(CASE WHEN n5 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 5
        WHEN MAX(CASE WHEN n4 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 4
        WHEN MAX(CASE WHEN n3 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 3
        WHEN MAX(CASE WHEN n2 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 2
        WHEN MAX(CASE WHEN n1 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 1
        ELSE 0 END AS n_cols,
      CASE
        WHEN MAX(CASE WHEN b2 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 2
        WHEN MAX(CASE WHEN b1 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 1
        ELSE 0 END AS bonus_cols
    FROM public.lottery_draws
    GROUP BY game
    ORDER BY game;
    """)
    with engine.begin() as con:
        return pd.read_sql(q, con)

def df_sample(engine, k:int=3) -> pd.DataFrame:
    q = text("""
    SELECT * FROM (
      SELECT game, round, ds, n1,n2,n3,n4,n5,n6,n7,b1,b2,
             ROW_NUMBER() OVER (PARTITION BY game ORDER BY ds DESC) rn
      FROM public.lottery_draws
    ) t
    WHERE rn <= :k
    ORDER BY game, ds DESC;
    """)
    with engine.begin() as con:
        return pd.read_sql(q, con, params={"k": k})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["check","sample"])
    ap.add_argument("--db", default=os.environ.get("DATABASE_URL"))
    ap.add_argument("--k", type=int, default=3, help="rows per game for sample")
    args = ap.parse_args()

    eng = get_engine(args.db)

    if not table_exists(eng):
        print("ERROR: table public.lottery_draws not found", file=sys.stderr)
        sys.exit(3)

    missing = schema_missing(eng)
    if missing:
        print("ERROR: missing columns: " + ", ".join(missing), file=sys.stderr)
        sys.exit(4)

    from tabulate import tabulate
    if args.action == "check":
        df = df_summary(eng)
        if df.empty:
            print("WARNING: lottery_draws is empty", file=sys.stderr)
            sys.exit(5)
        print(tabulate(df, headers="keys", tablefmt="github", showindex=False))
    else:
        print(tabulate(df_sample(eng, args.k), headers="keys", tablefmt="github", showindex=False))

if __name__ == "__main__":
    main()
