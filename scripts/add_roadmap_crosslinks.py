#!/usr/bin/env python3
"""
Add bi-directional links between roadmap/vX.Y.Z.md and roadmap/vX.Y.Z.md-full.md.

For each vX.Y.Z.md  → insert a "Full technical details" link after the title line.
For each .md-full.md → replace the bare backlink note with one that also links to
                        the plain .md companion file(s).
"""

import re
from pathlib import Path

ROADMAP_DIR = Path("roadmap")

# Mapping: slug in .md filename → corresponding -full.md filename
# v0.1.0 through v0.1.3 all map to v0.1.x.md-full.md
SLUG_TO_FULL = {}
for f in sorted(ROADMAP_DIR.glob("*.md-full.md")):
    slug = f.name.replace(".md-full.md", "")  # e.g. "v0.1.x", "v0.17.0"
    SLUG_TO_FULL[slug] = f.name

# Build the reverse: full-file → list of plain .md files
FULL_TO_PLAIN: dict[str, list[str]] = {}
for f in sorted(ROADMAP_DIR.glob("v*.md")):
    if f.suffix != ".md" or "-full" in f.name:
        continue
    slug = f.stem  # e.g. "v0.1.0", "v0.2.0"
    # Find the -full.md for this slug
    full = SLUG_TO_FULL.get(slug)
    if full is None:
        # Try series match: v0.1.0 → v0.1.x
        parts = slug.split(".")
        if len(parts) == 3:
            series = f"{parts[0]}.{parts[1]}.x"
            full = SLUG_TO_FULL.get(series)
    if full:
        FULL_TO_PLAIN.setdefault(full, []).append(f.name)


def add_link_to_plain(path: Path, full_filename: str) -> bool:
    """Add a 'Full technical details' link after the # title line in a plain .md file."""
    text = path.read_text(encoding="utf-8")
    # Already has the link? Skip.
    if full_filename in text and "Full technical details" in text:
        return False

    lines = text.splitlines(keepends=True)
    # Find the first `# ` heading (line 0)
    insert_after = 0
    for i, line in enumerate(lines):
        if line.startswith("# "):
            insert_after = i
            break

    link_line = f"\n> **Full technical details:** [{full_filename}]({full_filename})\n"
    lines.insert(insert_after + 1, link_line)
    path.write_text("".join(lines), encoding="utf-8")
    return True


def update_full_md_header(path: Path, plain_files: list[str]) -> bool:
    """Replace the bare backlink note in a -full.md with a richer one including plain .md links."""
    text = path.read_text(encoding="utf-8")

    old_note = (
        "> This file is extracted from [ROADMAP.md](../ROADMAP.md). "
        "See that file for the full technical roadmap."
    )

    if len(plain_files) == 1:
        companion = plain_files[0]
        plain_link = f"[{companion}]({companion})"
    else:
        plain_link = ", ".join(f"[{p}]({p})" for p in plain_files)

    new_note = (
        f"> This file is extracted from [ROADMAP.md](../ROADMAP.md). "
        f"See that file for the full technical roadmap.  \n"
        f"> **Plain-language companion:** {plain_link}"
    )

    if old_note not in text:
        # May already have been updated; check
        if "Plain-language companion" in text:
            return False
        print(f"  WARNING: expected backlink note not found in {path.name}")
        return False

    new_text = text.replace(old_note, new_note, 1)
    path.write_text(new_text, encoding="utf-8")
    return True


def main():
    changed = 0

    # Step 1: update plain .md files
    for plain_file in sorted(ROADMAP_DIR.glob("v*.md")):
        if "-full" in plain_file.name:
            continue
        slug = plain_file.stem
        full = SLUG_TO_FULL.get(slug)
        if full is None:
            parts = slug.split(".")
            if len(parts) == 3:
                series = f"{parts[0]}.{parts[1]}.x"
                full = SLUG_TO_FULL.get(series)
        if full is None:
            print(f"  SKIP (no -full.md found): {plain_file.name}")
            continue
        if add_link_to_plain(plain_file, full):
            print(f"  updated plain: {plain_file.name} → {full}")
            changed += 1

    # Step 2: update -full.md files
    for full_filename, plain_files in sorted(FULL_TO_PLAIN.items()):
        full_path = ROADMAP_DIR / full_filename
        if not full_path.exists():
            print(f"  MISSING: {full_filename}")
            continue
        if update_full_md_header(full_path, plain_files):
            print(f"  updated full:  {full_filename} → {plain_files}")
            changed += 1

    print(f"\nDone. {changed} files modified.")


if __name__ == "__main__":
    import os
    os.chdir(Path(__file__).parent.parent)
    main()
