import re, pathlib
p = pathlib.Path('apps/exog_ui_app.py')
s = p.read_text(encoding='utf-8')

def fix_func(name):
    # def name(engine, ...): ～ 次の def 直前までを取得して、そのブロック内だけ engine→_engine 置換
    pat = re.compile(rf'(def\s+{name}\s*\(\s*engine\b)(.*?):\n', re.S)
    s2 = pat.sub(rf'def {name}(_engine\2:\n', s)
    # ブロック置換：開始位置を見つけて次の def まで
    blk_pat = re.compile(rf'(def\s+{name}\s*\([^)]*\):)(.*?)(?=^\s*def\s+|\Z)', re.S | re.M)
    def repl(m):
        head, body = m.group(1), m.group(2)
        body = re.sub(r'\bengine\b', '_engine', body)
        return head + body
    return blk_pat.sub(repl, s2)

for fname in ["load_meta","list_unique_ids","date_range_for_uid","fetch_df"]:
    s = fix_func(fname)

# 参考ヒントに従い、デコレータの表記はそのまま（@st.cache_data）。引数名のアンダースコアで除外。
pathlib.Path(p).write_text(s, encoding='utf-8')
print("patched:", p)
