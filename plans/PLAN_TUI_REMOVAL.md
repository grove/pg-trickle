# Plan: Remove pgtrickle-tui from the project

**Status:** Proposed  
**Date:** 2026-04-28  
**Goal:** Completely remove the `pgtrickle-tui` terminal user interface crate
and all references to it across the project.

---

## Rationale

The TUI is being removed from this repository. This plan provides a
comprehensive, ordered checklist of every file and reference that must be
modified or deleted.

---

## Phase 1 — Delete the crate directory

| # | Action | Path |
|---|--------|------|
| 1.1 | Delete entire directory tree | `pgtrickle-tui/` |

This removes:
- `pgtrickle-tui/Cargo.toml`
- `pgtrickle-tui/src/main.rs`, `app.rs`, `cli.rs`, `connection.rs`, `poller.rs`, `state.rs`, `output.rs`, `theme.rs`, `error.rs`, `command_tests.rs`, `test_db.rs`, `test_fixtures.rs`
- `pgtrickle-tui/src/commands/` (19 modules: alter, cdc, completions, config, create, diag, drop, explain, export, fuse, graph, health, list, mod, refresh, status, watch, watermarks, workers)
- `pgtrickle-tui/src/views/` (16 modules + `snapshots/` directory with ~20 snapshot files)

---

## Phase 2 — Workspace configuration

| # | Action | File | Detail |
|---|--------|------|--------|
| 2.1 | Remove `"pgtrickle-tui"` from workspace members | `Cargo.toml` (line 14) | Change `members = [".", "pgtrickle-tui", "pgtrickle-relay"]` → `members = [".", "pgtrickle-relay"]` |
| 2.2 | Remove `pgtrickle-tui` entry from lock file | `Cargo.lock` | Regenerated automatically by `cargo update -w` after step 2.1 |

---

## Phase 3 — Build & CI

| # | Action | File | Detail |
|---|--------|------|--------|
| 3.1 | Remove TUI build step from release workflow | `.github/workflows/release.yml` (lines 89–90) | Delete `- name: Build pgtrickle TUI binary` and `run: cargo build --release -p pgtrickle-tui` |
| 3.2 | Remove TUI binary copy from tar.gz archive | `.github/workflows/release.yml` (line ~120) | Delete `# Copy pgtrickle TUI binary` + `cp target/release/pgtrickle "dist/${ARCHIVE_NAME}/"` |
| 3.3 | Remove TUI binary copy from zip archive | `.github/workflows/release.yml` (line ~179) | Delete `# Copy pgtrickle TUI binary` + `Copy-Item "target\release\pgtrickle.exe" "dist\${archiveName}\"` |
| 3.4 | Remove TUI CODEOWNERS entry | `.github/CODEOWNERS` (line 28) | Delete `pgtrickle-tui/              @grove @BaardBouvet` |

---

## Phase 4 — Justfile

| # | Action | File | Detail |
|---|--------|------|--------|
| 4.1 | Remove `test-tui-commands` recipe | `justfile` (lines 96–99) | Delete the `test-tui-commands:` recipe and its comment |
| 4.2 | Remove `just test-tui-commands` invocation | `justfile` (line 94) | Delete `just test-tui-commands` from the `test-all` (or parent) recipe |

---

## Phase 5 — Dockerfiles

Each of these Dockerfiles has a cache-warming layer that copies the TUI's
`Cargo.toml`, creates a stub `pgtrickle-tui/src/main.rs`, and includes it in
`mkdir -p` commands. All TUI references must be removed from the cache layer
while keeping the `pgtrickle-relay` references intact.

| # | Action | File |
|---|--------|------|
| 5.1 | Remove TUI cache-warming references | `Dockerfile.relay` (lines 22, 25, 29) |
| 5.2 | Remove TUI cache-warming references | `Dockerfile.hub` (lines 66, 70, 79) |
| 5.3 | Remove TUI cache-warming references | `Dockerfile.ghcr` (lines 70, 74, 83) |
| 5.4 | Remove TUI cache-warming references | `cnpg/Dockerfile.ext-build` (lines 46, 49, 58) |
| 5.5 | Remove TUI cache-warming references | `tests/Dockerfile.e2e` (lines 36, 40, 49) |
| 5.6 | Remove TUI cache-warming references | `tests/Dockerfile.e2e-coverage` (lines 56, 58, 66) |

---

## Phase 6 — Documentation

### 6a — Dedicated TUI docs (delete)

| # | Action | File |
|---|--------|------|
| 6a.1 | Delete TUI user guide | `docs/TUI.md` |
| 6a.2 | Delete CLI reference | `docs/CLI_REFERENCE.md` |

### 6b — Inline references (edit)

| # | Action | File | Detail |
|---|--------|------|--------|
| 6b.1 | Remove `cargo install --path pgtrickle-tui` and TUI build instructions | `README.md` (lines 65, 460) |
| 6b.2 | Remove TUI version-bump instructions | `docs/RELEASE.md` (lines 64, 75, 77, 204) |
| 6b.3 | Remove TUI reference in Getting Started | `docs/GETTING_STARTED.md` (line 1302) |
| 6b.4 | Remove TUI mention in upgrade notes | `docs/UPGRADING.md` (line 489) |
| 6b.5 | Remove TUI install instruction | `playground/README.md` (line 131) |
| 6b.6 | Remove TUI mention | `ESSENCE.md` (line 175) |

---

## Phase 7 — Metadata

| # | Action | File | Detail |
|---|--------|------|--------|
| 7.1 | Remove `"pgtrickle-tui"` from `no_index.directory` | `META.json` (line 49) |

---

## Phase 8 — Cross-crate references

| # | Action | File | Detail |
|---|--------|------|--------|
| 8.1 | Remove TUI comment in relay sink | `pgtrickle-relay/src/sink/pg_outbox.rs` (line 9) | Delete or reword `/// Uses tokio-postgres directly (same as pgtrickle-tui).` |

---

## Phase 9 — Planning & roadmap documents (edit or annotate)

These are historical/planning documents. References should be removed or
marked as obsolete with a note that the TUI was removed.

### 9a — Plan documents

| # | File | Lines |
|---|------|-------|
| 9a.1 | `plans/ui/PLAN_TUI.md` | Entire file — delete or prepend deprecation notice |
| 9a.2 | `plans/ui/PLAN_TUI_PART_2.md` | Entire file — delete or prepend deprecation notice |
| 9a.3 | `plans/ui/PLAN_TUI_PART_3.md` | Entire file — delete or prepend deprecation notice |
| 9a.4 | `plans/PLAN_OVERALL_ASSESSMENT.md` | Line 90 — remove TUI reference |
| 9a.5 | `plans/PLAN_OVERALL_ASSESSMENT_2.md` | Lines 94, 676 — remove TUI references |
| 9a.6 | `plans/PLAN_OVERALL_ASSESSMENT_3.md` | Lines 179, 214, 647, 727 — remove TUI references |
| 9a.7 | `plans/PLAN_OVERALL_ASSESSMENT_7.md` | Lines 136, 926 — remove TUI references |
| 9a.8 | `plans/PLAN_OVERALL_ASSESSMENT_8.md` | Line 138 — remove TUI reference |
| 9a.9 | `plans/PLAN_DOCUMENTATION_GAPS_1.md` | Lines 7, 84, 204, 711 — remove TUI references |
| 9a.10 | `plans/PLAN_AGENT_SKILLS.md` | Line 117 — remove TUI test skill reference |
| 9a.11 | `plans/relay/PLAN_RELAY_CLI.md` | Lines 63, 123, 2135 — remove TUI references |
| 9a.12 | `plans/relay/PLAN_RELAY_CLI_PHASE_2.md` | Lines 993, 997, 1010 — remove TUI references |

### 9b — Roadmap documents

| # | File | Lines |
|---|------|-------|
| 9b.1 | `ROADMAP.md` | Lines 47 (edit text), 103 (remove TUI row) |
| 9b.2 | `roadmap/v0.14.0.md` | Lines 91, 95 — remove or reword TUI references |
| 9b.3 | `roadmap/v0.18.0.md-full.md` | Line 355 — remove TUI reference |
| 9b.4 | `roadmap/v0.20.0.md-full.md` | Line 567 — remove TUI reference |
| 9b.5 | `roadmap/v0.29.0.md-full.md` | Line 14 — remove TUI reference |
| 9b.6 | `roadmap/v0.30.0.md-full.md` | Line 244 — remove TUI reference |

---

## Phase 10 — Changelog (preserve history, annotate)

| # | Action | File | Detail |
|---|--------|------|--------|
| 10.1 | Add deprecation notice | `CHANGELOG.md` | Add a note in the next unreleased section: "**Removed:** The `pgtrickle-tui` terminal dashboard has been removed from this repository." Historical entries (v0.14.0, v0.15.0, etc.) should be left as-is since they document what was released at the time. |

---

## Phase 11 — AGENTS.md

| # | Action | File | Detail |
|---|--------|------|--------|
| 11.1 | Remove TUI from module layout if listed | `AGENTS.md` | The module layout does not list `pgtrickle-tui/` directly, but check and remove any stray TUI references in testing tables or instructions. |

---

## Phase 12 — Verification

| # | Action | Command |
|---|--------|---------|
| 12.1 | Regenerate lockfile | `cargo update -w` |
| 12.2 | Verify workspace compiles | `cargo check` |
| 12.3 | Run fmt + lint | `just fmt && just lint` |
| 12.4 | Full-text search for stragglers | `rg -i 'pgtrickle.tui' --type-not lock` |
| 12.5 | Verify no broken doc links | `rg '\[.*\]\(.*TUI.*\)' docs/ README.md` |
| 12.6 | Run unit tests | `just test-unit` |
| 12.7 | Run integration tests | `just test-integration` |

---

## Execution order

1. Phase 1 (delete crate)
2. Phase 2 (workspace config)
3. Phase 3–5 (CI, justfile, Dockerfiles) — can be done in parallel
4. Phase 6–8 (docs, metadata, cross-refs) — can be done in parallel
5. Phase 9–11 (plans, changelog, AGENTS.md) — can be done in parallel
6. Phase 12 (verification) — must be last

---

## Risk & rollback

- **Low risk.** The TUI is a standalone binary with no compile-time
  dependencies from the main extension crate or the relay crate. Removing it
  cannot break the extension.
- **Rollback:** `git revert` the removal commit(s).
