#!/usr/bin/env python
import argparse, os, sys
from pathlib import Path
from sqlalchemy import create_engine, text

def run_sql(dsn: str, sql_path: str) -> None:
    engine = create_engine(dsn, pool_pre_ping=True)
    sql = Path(sql_path).read_text(encoding="utf-8")
    # 素朴にステートメント分割（; 区切り、--行コメント除去）
    stmts = []
    buf = []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmts.append("\n".join(buf).rstrip(";").strip())
            buf = []
    if buf:  # 末尾に ; がない場合
        stmts.append("\n".join(buf).strip())
    with engine.begin() as conn:
        for s in stmts:
            if s:
                conn.execute(text(s))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.getenv("POSTGRES_DSN", "sqlite:///tsmodeling.db"))
    p.add_argument("--sql", default="sql/init_schema.sql")
    p.add_argument("--init", action="store_true", help="run schema init SQL")
    args = p.parse_args()
    if args.init:
        print(f"[setup_db] init: dsn={args.dsn} sql={args.sql}")
        run_sql(args.dsn, args.sql)
        print("[setup_db] done")
    else:
        p.print_help()
        sys.exit(2)

if __name__ == "__main__":
    main()
