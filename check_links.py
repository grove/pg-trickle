#!/usr/bin/env python3
import os, re

broken = []

for root, dirs, files in os.walk('docs'):
    dirs[:] = [d for d in dirs if not d.startswith('.')]
    for fname in files:
        if not fname.endswith('.md'):
            continue
        fpath = os.path.join(root, fname)
        with open(fpath) as f:
            content = f.read()
        pat = r'\[[^\]]*\]\(([^)#\s]+)\)'
        for m in re.finditer(pat, content):
            lp = m.group(1).strip()
            if lp.startswith('http') or lp.startswith('mailto:'):
                continue
            skip = ['../INSTALL', '../SECURITY', '../AGENTS', '../CONTRIBUTING',
                    '../CHANGELOG', '../ROADMAP', '../ESSENCE', '../README',
                    '../LICENSE', 'blog/']
            if any(lp.startswith(p) for p in skip):
                continue
            base = os.path.dirname(fpath)
            resolved = os.path.normpath(os.path.join(base, lp))
            if not os.path.exists(resolved):
                broken.append((os.path.relpath(fpath, 'docs'), lp))

seen = set()
dedup = []
for x in broken:
    if x not in seen:
        seen.add(x)
        dedup.append(x)

print(f'BROKEN {len(dedup)}')
for s, l in dedup[:50]:
    print(f'  {s}: {l}')
