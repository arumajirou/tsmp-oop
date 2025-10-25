import argparse, os, sqlalchemy as sa, pathlib

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.getenv("POSTGRES_DSN","sqlite:///tsmp.db"))
    parser.add_argument("--sql", default="sql/init_schema.sql")
    args = parser.parse_args()
    sql = pathlib.Path(args.sql).read_text(encoding="utf-8")
    eng = sa.create_engine(args.dsn, pool_pre_ping=True)
    with eng.begin() as conn:
        for stmt in sql.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(sa.text(s))
    print("initialized schema")

if __name__ == "__main__":
    main()
