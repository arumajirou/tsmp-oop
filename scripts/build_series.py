#!/usr/bin/env python3
"""
Build per-series materialized views from lottery_draws:
- Central long view: lottery_series_long(unique_id, ds, y)
- Per-series MVs:    series_<game>_n<k>(unique_id, ds, y)
Usage:
  python scripts/build_series.py --db $DATABASE_URL [--only-game mini,loto6]
Options:
  --refresh    : refresh all created MVs after creation
  --dry-run    : print SQL only
"""
import os, sys, argparse
from sqlalchemy import create_engine, text

LONG_VIEW_SQL = """
CREATE OR REPLACE VIEW public.lottery_series_long AS
SELECT
  CONCAT(game, '_n', v.n_no) AS unique_id,
  d.ds::date                AS ds,
  v.y::integer              AS y
FROM public.lottery_draws AS d
CROSS JOIN LATERAL (
  VALUES
    (1, d.n1),
    (2, d.n2),
    (3, d.n3),
    (4, d.n4),
    (5, d.n5),
    (6, d.n6),
    (7, d.n7)
) AS v(n_no, y)
WHERE v.y IS NOT NULL;
"""

N_COLS_SQL = text("""
SELECT
  game,
  CASE
    WHEN MAX(CASE WHEN n7 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 7
    WHEN MAX(CASE WHEN n6 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 6
    WHEN MAX(CASE WHEN n5 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 5
    WHEN MAX(CASE WHEN n4 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 4
    WHEN MAX(CASE WHEN n3 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 3
    WHEN MAX(CASE WHEN n2 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 2
    WHEN MAX(CASE WHEN n1 IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 1
    ELSE 0 END AS n_cols
FROM public.lottery_draws
GROUP BY game
ORDER BY game;
""")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.environ.get("DATABASE_URL"))
    ap.add_argument("--only-game", default="", help="comma separated, e.g. mini,loto6")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.db:
        print("ERROR: set --db or DATABASE_URL", file=sys.stderr)
        sys.exit(2)

    eng = create_engine(args.db, pool_pre_ping=True)
    only = set([g.strip() for g in args.only_game.split(",") if g.strip()]) if args.only_game else None

    with eng.begin() as con:
        if args.dry_run:
            print(LONG_VIEW_SQL.strip())
        else:
            con.execute(text(LONG_VIEW_SQL))

        rows = con.execute(N_COLS_SQL).mappings().all()

    # Build per-series materialized views
    ddl_list = []
    for r in rows:
        game = r["game"]
        n_cols = int(r["n_cols"] or 0)
        if only and game not in only:
            continue
        for k in range(1, n_cols + 1):
            view_name = f"series_{game}_n{k}"
            uid = f"{game}_n{k}"
            ddl = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS public.{view_name} AS
SELECT unique_id, ds, y
FROM public.lottery_series_long
WHERE unique_id = :uid;

CREATE INDEX IF NOT EXISTS ix_{view_name}_ds ON public.{view_name}(ds);
"""
            ddl_list.append((view_name, uid, ddl))

    with eng.begin() as con:
        for view_name, uid, ddl in ddl_list:
            if args.dry_run:
                print(ddl.strip())
            else:
                for stmt in ddl.strip().split(";"):
                    s = stmt.strip()
                    if not s:
                        continue
                    # last fragment may be empty; guard with semicolon injection
                    con.execute(text(s + (";" if not s.endswith(";") else "")), {"uid": uid})

        if args.refresh and not args.dry_run:
            for view_name, uid, _ in ddl_list:
                con.execute(text(f"REFRESH MATERIALIZED VIEW public.{view_name};"))

    print(f"OK: created/verified {len(ddl_list)} materialized views and long view.")

if __name__ == "__main__":
    main()
