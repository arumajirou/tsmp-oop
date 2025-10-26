import re, pathlib
p = pathlib.Path('scripts/build_exog.py')
s = p.read_text(encoding='utf-8')

# a) "--fe-extra" が未登録なら、parse_args() の直前に強制注入
if '--fe-extra' not in s:
    # parse_args の行を特定
    m = re.search(r'^\s*args\s*=\s*parser\.parse_args\([^)]*\)\s*$', s, flags=re.M)
    if m:
        insert_at = m.start()
        inject = (
            '    parser.add_argument("--fe-extra", choices=["none","tsfresh","tsfel","all"], '
            'default="all", help="Add extra features from libraries (tsfresh/tsfel)")\n'
        )
        s = s[:insert_at] + inject + s[insert_at:]

# b) parse_args() の直後にフォールバック（無ければ all）
#    既に存在するなら重複挿入しない
if 'hasattr(args, "fe_extra")' not in s:
    s = re.sub(
        r'(\bargs\s*=\s*parser\.parse_args\([^)]*\)\s*\n)',
        r'\1    # fallback when older code path parsed before injection\n'
        r'    if not hasattr(args, "fe_extra"): args.fe_extra = "all"\n',
        s, count=1
    )

pathlib.Path(p).write_text(s, encoding='utf-8')
print("patched", p)
