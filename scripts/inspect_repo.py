#!/usr/bin/env python
import ast, json, sys, pathlib
root = pathlib.Path("src")
report = {"modules": {}}
for py in root.rglob("*.py"):
    mod = str(py.relative_to(root)).replace("/", ".")[:-3]
    src = py.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except Exception as e:
        report["modules"][mod] = {"error": str(e)}; continue
    classes, funcs, imports = [], [], []
    for n in ast.walk(tree):
        if isinstance(n, ast.ClassDef): classes.append(n.name)
        elif isinstance(n, ast.FunctionDef): funcs.append(n.name)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            if isinstance(n, ast.Import):
                imports += [a.name.split(".")[0] for a in n.names]
            else:
                if n.module: imports.append(n.module.split(".")[0])
    report["modules"][mod] = {
        "path": str(py),
        "classes": sorted(set(classes)),
        "functions": sorted(set(funcs)),
        "imports_top": sorted(set(imports)),
    }
print(json.dumps(report, ensure_ascii=False, indent=2))
