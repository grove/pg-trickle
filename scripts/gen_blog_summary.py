#!/usr/bin/env python3
"""Generate SUMMARY.md for the blog mdBook sub-build.

Usage: gen_blog_summary.py <src_dir>
"""
import os
import sys

src = sys.argv[1]
files = sorted(
    f for f in os.listdir(src)
    if f.endswith('.md') and f not in ('README.md', 'SUMMARY.md')
)

lines = ['# Summary\n', '\n', '[Blog Index](README.md)\n', '\n']
for f in files:
    with open(os.path.join(src, f)) as fp:
        title = fp.readline().strip().lstrip('#').strip()
    lines.append(f'- [{title}]({f})\n')

out = os.path.join(src, 'SUMMARY.md')
with open(out, 'w') as fp:
    fp.writelines(lines)

print(f'Generated {out} with {len(files)} posts')
