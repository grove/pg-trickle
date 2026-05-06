#!/usr/bin/env python3
"""CQ-10-01: Update scheduler/mod.rs to remove extracted sections and add module declarations."""

import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MOD_RS = os.path.join(BASE, 'src', 'scheduler', 'mod.rs')

with open(MOD_RS) as f:
    lines = f.read().split('\n')

print(f"Original mod.rs: {len(lines)} lines")

# Lines to REMOVE (0-indexed, end-exclusive range):
# loop_p1:        222..472  (lines 223-472) — register_* + launcher_main
# dispatch_p1:    473..791  (lines 474-791) — spawn + worker_main + parse helpers
# watermark:      791..1037 (lines 792-1037) — watermark functions
# dispatch_p2:   1670..2371 (lines 1671-2371) — parallel dispatch state
# scheduler_main: 2372..3611 (lines 2373-3611) — pg_trickle_scheduler_main

remove = (set(range(222, 472)) |
          set(range(473, 791)) |
          set(range(791, 1037)) |
          set(range(1670, 2371)) |
          set(range(2372, 3611)))

print(f"Lines to remove: {len(remove)}")

# Build new lines
new_lines = [line for i, line in enumerate(lines) if i not in remove]
print(f"Lines remaining: {len(new_lines)}")

# Find the insertion point: after "pub mod tier;" (line 60 in original, but now shifted)
# We look for "pub mod tier;" in new_lines
insert_after = None
for i, l in enumerate(new_lines):
    if l.strip() == 'pub mod tier;':
        insert_after = i
        break

if insert_after is None:
    print("ERROR: could not find 'pub mod tier;' in new_lines")
    exit(1)

print(f"Inserting module declarations after line {insert_after+1}")

module_decls = [
    'pub mod dispatch;',
    'pub mod scheduler_loop;',
    'pub mod watermark;',
    '',
    '// CQ-10-01: Re-export public items from decomposed scheduler submodules.',
    'pub use dispatch::{reconcile_parallel_state, spawn_refresh_worker};',
    'pub use scheduler_loop::{',
    '    pg_trickle_launcher_main, pg_trickle_scheduler_main, register_launcher_worker,',
    '    register_scheduler_worker,',
    '};',
]

# Insert after "pub mod tier;"
final_lines = new_lines[:insert_after+1] + module_decls + new_lines[insert_after+1:]

print(f"Final mod.rs: {len(final_lines)} lines")

# Write the file
with open(MOD_RS, 'w') as f:
    f.write('\n'.join(final_lines))
print("Done: wrote updated mod.rs")
