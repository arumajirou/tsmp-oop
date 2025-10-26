import re, pathlib
p = pathlib.Path('scripts/build_exog.py'); s = p.read_text(encoding='utf-8')
if '--fe-extra' not in s:
    s = re.sub(r'(parser\.add_argument\(\s*"--gpu"[^)\n]*\)\s*\n)',
               r'\1    parser.add_argument("--fe-extra", choices=["none","tsfresh","tsfel","all"], default="all",\n'
               r'                        help="Add extra features from libraries (tsfresh/tsfel)")\n', s, count=1)
p.write_text(s, encoding='utf-8'); print("patched", p)
