import re
p='scripts/build_exog.py'
s=open(p,'r',encoding='utf-8').read()

# 既存の INSERT ... VALUES (1, :f..., :h..., :s...) を、CAST + ON CONFLICT に置換
pat = re.compile(
    r'text\(\s*f"INSERT INTO \{meta_table\}\(id,\s*futr_exog_list,\s*hist_exog_list,\s*stat_exog_list\)\s*VALUES\s*\(1,\s*:f.*?\)"\s*\)',
    re.S
)
new = (
    'text(f"""INSERT INTO {meta_table} (id, futr_exog_list, hist_exog_list, stat_exog_list)\n'
    'VALUES (1, CAST(:f AS text[]), CAST(:h AS text[]), CAST(:s AS text[]))\n'
    'ON CONFLICT (id) DO UPDATE SET\n'
    '  futr_exog_list = EXCLUDED.futr_exog_list,\n'
    '  hist_exog_list = EXCLUDED.hist_exog_list,\n'
    '  stat_exog_list = EXCLUDED.stat_exog_list\n'
    '""")'
)
s2, n = pat.subn(new, s)

# 置換ヒットが0なら、:f::text[] を CAST(:f AS text[]) に個別変換（保険）
if n == 0:
    s2 = re.sub(r':f::text\[\]', r'CAST(:f AS text[])', s2)
    s2 = re.sub(r':h::text\[\]', r'CAST(:h AS text[])', s2)
    s2 = re.sub(r':s::text\[\]', r'CAST(:s AS text[])', s2)

open(p,'w',encoding='utf-8').write(s2)
print("patched", p, "replacements:", n)
