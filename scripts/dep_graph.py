#!/usr/bin/env python
import ast, pathlib, sys
root = pathlib.Path("src")
edges = set()
for py in root.rglob("*.py"):
    mod = str(py.relative_to(root)).replace("/", ".")[:-3]
    tree = ast.parse(py.read_text(encoding="utf-8"))
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module and n.module.startswith("tsmp"):
            edges.add((mod, n.module))
        if isinstance(n, ast.Import):
            for a in n.names:
                if a.name.startswith("tsmp"):
                    edges.add((mod, a.name))
print("digraph G {")
print('  rankdir=LR; node [shape=box, fontsize=10];')
for a,b in sorted(edges):
    print(f'  "{a}" -> "{b}";')
print("}")
