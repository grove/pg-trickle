---
name: implement-roadmap-version
description: "Implement all items for a specific version in the pg_trickle roadmap. Use when: working through a milestone, implementing a release, or completing planned features. Drives a full implementation loop — reads plan docs, implements each item, keeps status up to date, asks the user when blocked, and reports what remains at the end."
argument-hint: "Target version (e.g. 0.20.0)"
---

# Implement Roadmap Version

Structured, self-driving workflow for implementing every item in a specific
pg_trickle release milestone. The skill continues working through items
sequentially, updates plan and roadmap status as it goes, surfaces blockers
to the user with concrete questions, and produces a completion report at the
end.

## When to Use

- Starting work on a planned milestone
- Resuming an in-progress milestone after a pause
- Verifying how far along a release is and continuing from where it left off

## Inputs to Gather First

Before any implementation begins, read these files **in parallel**:

1. `ROADMAP.md` — find the target version section: theme, item list, status
2. `plans/PLAN_0_<version_underscored>.md` — implementation order, phases,
   item IDs, effort estimates, exit criteria (if the file exists)
3. `CHANGELOG.md` (last 200 lines) — avoid re-implementing already-shipped work
4. `AGENTS.md` — coding conventions, error handling rules, SPI rules
5. `plans/INDEX.md` — cross-reference any linked sub-plans

If `plans/PLAN_0_<version_underscored>.md` does not exist, create it following
the template in [Step 1b](#step-1b--create-the-plan-file-if-missing) before
proceeding.

---

## Procedure

### Step 0 — Establish Context

Run the following in parallel:

```bash
# What version are we on?
cat pg_trickle.control | grep '^default_version'
grep '^version' Cargo.toml | head -1

# Are there uncommitted changes?
git status --short

# Which branch are we on?
git branch --show-current
```

Determine:
- **Target version** (from the argument or user's message)
- **Current version** (from `pg_trickle.control`)
- **Plan file path**: `plans/PLAN_0_<VER_WITH_UNDERSCORES>.md`
  - Example: v0.20.0 → `plans/PLAN_0_20_0.md`
- **Branch**: if on `main`, a new branch must be created before making any
  changes: `git checkout -b <version>-implementation`

---

### Step 1a — Parse the Plan File

Read `plans/PLAN_0_<version>.md` and extract:

1. **All item IDs and titles** (e.g. `DOG-1`, `PG17-3`)
2. **Current status** of each item in the Implementation Status table
   (⬜ Not started / 🔄 In progress / ✅ Done / ⏭ Skipped)
3. **Phase groupings** — respect the recommended implementation order
4. **Exit criteria** — the checklist at the bottom of the plan file
5. **Items already done** — skip these, but record them for the completion
   report

---

### Step 1b — Create the Plan File if Missing

If `plans/PLAN_0_<version>.md` does not exist:

1. Read the roadmap section for the target version from `ROADMAP.md`.
2. Create a plan file using this skeleton:

```markdown
# PLAN_0_<VERSION>.md — v<VERSION> Implementation Order

**Milestone:** v<VERSION> — <Theme from ROADMAP>
**Status:** 🚧 In progress
**Last updated:** <today's date>

This document defines the recommended implementation order for all v<VERSION>
roadmap items. See [ROADMAP.md §v<VERSION>](../ROADMAP.md#v<anchor>) for the
full feature descriptions and effort estimates.

---

## Milestone Goals

<bullet list from roadmap section>

---

## Recommended Implementation Order

<phases derived from roadmap items, ordered by dependency>

---

## Implementation Status

| ID | Feature | Status |
|----|---------|--------|
<one row per item, all "⬜ Not started" initially>

---

## Release Exit Criteria

- [ ] All items marked ✅ Done
- [ ] `just test-all` passes with zero failures
- [ ] `just check-version-sync` exits 0
- [ ] CHANGELOG.md `## [<VERSION>]` entry written
- [ ] ROADMAP.md v<VERSION> section marked ✅ Released
```

3. After creating the file, add it to git staging:
   ```bash
   git add plans/PLAN_0_<version>.md
   ```

---

### Step 2 — Build the Work Queue

Construct an ordered list of items to implement:

1. Take all items from the plan file in phase order.
2. **Skip** items already marked ✅ Done or ⏭ Skipped.
3. Mark the first actionable item as `🔄 In progress` in the plan file.
4. Use `manage_todo_list` to track all items, using the item ID as the todo ID.

If no items remain (all ✅ / ⏭), jump directly to [Step 6 — Completion
Report](#step-6--completion-report).

---

### Step 3 — Implement Each Item (Loop)

For each item in the work queue, repeat this inner loop:

#### 3a. Understand the Item

Read the roadmap description for the item. Identify:
- What code/SQL/test/doc changes are needed
- Which files are affected
- Whether a schema change is required (SQL migration needed)
- Dependencies on other items (check they are ✅ Done first)

If a dependency is not yet done, reorder: implement the dependency first.

#### 3b. Ask Before Ambiguous Work

**Before making any irreversible change**, if the item is ambiguous or has
multiple valid approaches, ask the user a focused question:

> "For item `<ID>` — `<title>`, I'm planning to `<approach>`. Does that match
> your intent, or should I `<alternative>`?"

Keep questions concrete and binary where possible. Never ask about more than
two items at once.

For straightforward, clearly-specified items: **do not ask — just implement**.

#### 3c. Implement

Apply changes following `AGENTS.md` conventions:

| Item type | Required steps |
|-----------|---------------|
| New Rust feature | Edit `src/`, run `just fmt`, run `just lint` |
| New SQL function | Edit `src/api/*.rs` + upgrade migration SQL + full install SQL |
| New/modified SQL | Update `sql/pg_trickle--<prev>--<cur>.sql` and `sql/pg_trickle--<cur>.sql` |
| Schema change | Require explicit user confirmation before proceeding |
| New test | Add to `tests/e2e_*_tests.rs` following naming convention |
| Documentation | Edit the relevant file in `docs/` |
| Config/GUC | Add to `src/config.rs`, document in `docs/CONFIGURATION.md` |
| Build/CI change | Edit `justfile`, `.github/workflows/*.yml`, `Dockerfile.*` |

After each item's changes are applied:
```bash
just fmt          # Always
just lint         # Must pass with zero warnings
```

For items that add or modify SQL-facing code, also run the appropriate tier:
```bash
just test-unit           # Always run after Rust changes
just test-integration    # Run if catalog or SPI changes
```

Do **not** run `just test-e2e` or `just test-all` after every item — reserve
these for phase boundaries (see Step 3e).

#### 3d. Update Status

Immediately after each item is implemented and lint passes:

1. Update the item's row in the plan file's Implementation Status table:
   `⬜ Not started` → `✅ Done`
2. If the item was in-progress (`🔄`), also update that row.
3. Update the **Last updated** date at the top of the plan file.

```markdown
<!-- Example update in Implementation Status table -->
| DOG-3 | Create `pgtrickle.self_monitor()` stream table | ✅ Done |
```

#### 3e. Phase Boundary Validation

At the end of each **phase** (not after every item):

```bash
just test-unit
just test-integration    # if any catalog/SPI changes in the phase
```

If tests fail:
1. Fix the failures before moving to the next phase.
2. If a failure is in pre-existing code unrelated to the current item, note it
   and ask the user: "I found a pre-existing failure in `<test>`. Should I fix
   it now or log it as a separate issue?"

---

### Step 4 — Handle Blockers

If an item cannot be implemented because:

- **Missing information**: ask the user one focused question; do not guess.
- **External dependency** (e.g. a library update, a PostgreSQL version):
  mark the item `⏭ Skipped (blocked: <reason>)` in the plan file, record
  it in the completion report, and continue with the next item.
- **Scope unclear**: present two concrete options and ask the user to choose.
- **Compilation or test failure that is not fixable within the item scope**:
  pause and ask the user for guidance before continuing.

Never silently skip items without recording the skip reason in the plan file.

---

### Step 5 — Update ROADMAP.md

After **all** items in the version are either ✅ Done or ⏭ Skipped:

1. Find the version section in `ROADMAP.md`.
2. If all required items (P0/P1) are done, update the status:
   - In the overview table: `Planned` → `✅ Released`
   - In the section heading: add `**Status: Released (<date>).**`
3. If skipped items exist, update the status to reflect the gap:
   - `**Status: Partially implemented — see plan for skipped items.**`
4. Run:
   ```bash
   just check-version-sync    # Verify version consistency
   ```

---

### Step 6 — Completion Report

After the implementation loop finishes, output a structured report:

```
## v<VERSION> Implementation Summary

### Completed (<N> items)
- ✅ <ID>: <title>
- ✅ <ID>: <title>
...

### Skipped (<N> items)
- ⏭ <ID>: <title> — <reason>
...

### Remaining work before release
1. <Exit criterion not yet met>
2. <Exit criterion not yet met>
...

### Recommended next steps
- Run `just test-e2e` to validate full E2E suite
- Run `just test-all` for final gate
- Update CHANGELOG.md with `## [<VERSION>]` entry
- Bump version: `just bump-version <VERSION>`
- Create PR using the `create-pull-request` skill
```

If all exit criteria in the plan file's checklist are met, add:

> **Release-ready.** All exit criteria satisfied. Ready for `create-pull-request`.

---

## Status Symbols

| Symbol | Meaning |
|--------|---------|
| ⬜ | Not started |
| 🔄 | In progress |
| ✅ | Done |
| ⏭ | Skipped (reason recorded) |
| ❌ | Failed / blocked |

---

## Conventions Reference (from AGENTS.md)

- No `unwrap()` or `panic!()` in non-test code
- All `unsafe` blocks must have `// SAFETY:` comments
- SPI connections are short-lived
- New SQL functions use `#[pg_extern(schema = "pgtrickle")]`
- Return `Result<T, PgTrickleError>` — convert at API boundary
- Cast `name`-typed columns to `text` in SPI queries
- Tests use Testcontainers — never a local PG instance
- Use `pgrx::log!()` / `info!()` — never `println!()`

---

## Important Safeguards

- **Never drop tables or columns** without explicit user confirmation
- **Never push to `main`** directly — always work on a feature branch
- **Never run `git push --force`** without explicit user confirmation
- **Never use shell heredocs** for PR body text (Unicode corruption risk)
- **Never skip lint** — `just lint` must pass with zero warnings before
  marking any item done
- **Never mark an item done** if its associated tests are failing
