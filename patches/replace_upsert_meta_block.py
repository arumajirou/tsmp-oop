import io, re, sys, pathlib
p = pathlib.Path('scripts/build_exog.py')
s = p.read_text(encoding='utf-8')

pat = re.compile(
    r'def\s+upsert_meta\(\s*engine,\s*meta_table,\s*idx_table,\s*futr_cols,\s*hist_cols,\s*stat_cols\s*\):'
    r'(.*?)'  # body
    r'(?=^\s*def\s+\w+\(|\Z)',  # until next def or EOF
    re.S | re.M
)

new_body = r'''
def upsert_meta(engine, meta_table, idx_table, futr_cols, hist_cols, stat_cols):
    # psycopg2 は INSERT の列型から Python list を text[] に自動適応してくれる。
    # 余計な ::text[] / CAST は付けず、ON CONFLICT で安定化。
    from sqlalchemy.sql import text
    with engine.begin() as con:
        sql = text(f"""
        INSERT INTO {meta_table} (id, futr_exog_list, hist_exog_list, stat_exog_list)
        VALUES (:id, :f, :h, :s)
        ON CONFLICT (id) DO UPDATE SET
          futr_exog_list = EXCLUDED.futr_exog_list,
          hist_exog_list = EXCLUDED.hist_exog_list,
          stat_exog_list = EXCLUDED.stat_exog_list
        """)
        con.execute(sql, {"id": 1, "f": futr_cols, "h": hist_cols, "s": stat_cols})

        # col index を張り直し（全削除→一括挿入）
        con.execute(text(f"DELETE FROM {idx_table}"))
        rows = [("futr", c) for c in futr_cols] + [("hist", c) for c in hist_cols] + [("stat", c) for c in stat_cols]
        if rows:
            values = ",".join([f"(:t{i}, :c{i})" for i in range(len(rows))])
            params = {**{f"t{i}": t for i,(t,_) in enumerate(rows)},
                      **{f"c{i}": c for i,(_,c) in enumerate(rows)}}
            con.execute(text(f"INSERT INTO {idx_table}(exog_type, colname) VALUES {values}"), params)
'''.lstrip('\n')

def repl(m):
    return new_body

s2, n = pat.subn(repl, s, count=1)
if n == 0:
    print("no match for upsert_meta; abort", file=sys.stderr)
    sys.exit(1)

# 念のため、旧来の ::text[] / CAST(:x AS text[]) 残骸を掃除
s2 = re.sub(r'::text\[\]', '', s2)
s2 = re.sub(r'CAST\(\s*:[fhs]\s+AS\s+text\[\]\s*\)', r':\g<0>', s2)  # 無害化（パラメータだけ残す）

p.write_text(s2, encoding='utf-8')
print("patched", p)
