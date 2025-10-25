#!/usr/bin/env python
import json, pathlib, re, sys

# 1) まず正規の成果物を読む
json_path = pathlib.Path("outputs/last_run.json")
if json_path.exists():
    data = json.loads(json_path.read_text(encoding="utf-8"))
    out = pathlib.Path(f"ops/phase1/metrics-{json_path.stat().st_mtime_ns}.json")
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(out))
    sys.exit(0)

# 2) フォールバック：ログからトップレベルの { ... } をバランス走査で抽出
logs = sorted(pathlib.Path("ops/phase1").glob("run-*.log"))
if not logs:
    sys.stderr.write("no run logs in ops/phase1\n"); sys.exit(1)

raw = logs[-1].read_text(encoding="utf-8")

# バランススキャン
start = raw.find("{")
if start == -1:
    sys.stderr.write("no dict found in last run log\n"); sys.exit(2)

depth = 0
end = -1
for i, ch in enumerate(raw[start:], start=start):
    if ch == "{": depth += 1
    elif ch == "}":
        depth -= 1
        if depth == 0:
            end = i + 1
            break

if end == -1:
    sys.stderr.write("no closed dict found in last run log\n"); sys.exit(3)

snippet = raw[start:end]

# 3) 可能なら JSON として読み、ダメなら Python 風を緩和
try:
    data = json.loads(snippet)
except Exception:
    # 単純置換で JSON 化（シングルクォート→ダブル・True/False/None → JSON）
    fixed = (snippet
             .replace("'", '"')
             .replace(" True", " true").replace("true", "true")
             .replace(" False", " false").replace("false", "false")
             .replace(" None", " null").replace("None", "null"))
    data = json.loads(fixed)

out = pathlib.Path(f"ops/phase1/metrics-{logs[-1].stem.split('run-')[-1]}.json")
out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
print(str(out))
