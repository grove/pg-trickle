# PLAN_TUI.md — pg_trickle Terminal User Interface

**Milestone:** v0.14.0 (replaces E3 CLI)
**Status:** ✅ Implemented (T1–T8)
**Effort:** ~8–10 days
**Last updated:** 2026-04-02
**Crate:** `pgtrickle-tui` (workspace member, separate binary)

---

## Overview

A full-featured terminal user interface (TUI) for managing, monitoring, and
diagnosing pg_trickle stream tables from outside SQL. Built with
[ratatui](https://ratatui.rs/) in Rust, sharing the project's build
infrastructure, CI, and PostgreSQL client libraries.

The TUI replaces the originally planned minimal CLI (E3). It subsumes all E3
commands as non-interactive one-shot flags **and** adds a rich interactive
dashboard that operators can leave running in a terminal alongside their
database. Think `htop` for stream tables.

### Why TUI over CLI?

| | CLI (E3 original) | TUI (this plan) |
|-|-|-|
| Live status | ❌ Poll with `watch` | ✅ Real-time auto-refresh |
| Dependency graph | ❌ Text dump | ✅ ASCII-art DAG visualization |
| Drill-down | ❌ Different command per entity | ✅ Navigate with keyboard |
| Alerts | ❌ Must query manually | ✅ Toast notifications from NOTIFY |
| Log tailing | ❌ Must query `refresh_history` | ✅ Live scrollable log pane |
| Config editing | ❌ Requires `psql` | ✅ Inline GUC editor |
| Machine output | ❌ `--format json` flag | ✅ Same flag, plus interactive mode |

The TUI still supports every E3 one-shot command (`pgtrickle list`,
`pgtrickle status`, `pgtrickle refresh`, etc.) for scripting and CI
integration. Invoking `pgtrickle` with no subcommand launches the interactive
dashboard.

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Feature Map](#feature-map)
  - [F1 — Dashboard (Home)](#f1--dashboard-home)
  - [F2 — Stream Table Detail View](#f2--stream-table-detail-view)
  - [F3 — Dependency Graph View](#f3--dependency-graph-view)
  - [F4 — Live Refresh Log](#f4--live-refresh-log)
  - [F5 — Diagnostics Panel](#f5--diagnostics-panel)
  - [F6 — CDC Health View](#f6--cdc-health-view)
  - [F7 — Configuration Editor](#f7--configuration-editor)
  - [F8 — Alert Feed](#f8--alert-feed)
  - [F9 — Command Palette](#f9--command-palette)
  - [F10 — One-Shot CLI Mode](#f10--one-shot-cli-mode)
  - [F11 — Sparklines & Micro-Charts](#f11--sparklines--micro-charts)
  - [F12 — Help System](#f12--help-system)
  - [F13 — Scheduler & Worker Pool View](#f13--scheduler--worker-pool-view)
  - [F14 — Fuse & Circuit Breaker Panel](#f14--fuse--circuit-breaker-panel)
  - [F15 — Watermark & Source Gating View](#f15--watermark--source-gating-view)
  - [F16 — Delta SQL Inspector](#f16--delta-sql-inspector)
  - [F17 — Batch Operations](#f17--batch-operations)
  - [F18 — System Health Overview](#f18--system-health-overview)
  - [F19 — Watch Mode (Lite Dashboard)](#f19--watch-mode-lite-dashboard)
  - [F20 — DAG Health & Impact Analysis](#f20--dag-health--impact-analysis)
  - [F21 — Cascade Staleness Tracker](#f21--cascade-staleness-tracker)
- [Dashboard Improvements](#dashboard-improvements)
- [Detail View Improvements](#detail-view-improvements)
- [Graph View Improvements](#graph-view-improvements)
- [Refresh Log Improvements](#refresh-log-improvements)
- [Command Palette Improvements](#command-palette-improvements)
- [CLI Mode Improvements](#cli-mode-improvements)
- [Adaptive Polling](#adaptive-polling)
- [Navigation & Keybindings](#navigation--keybindings)
- [UI Layout](#ui-layout)
- [Theming & Aesthetics](#theming--aesthetics)
- [Connection Management](#connection-management)
- [Implementation Phases](#implementation-phases)
- [Exit Criteria](#exit-criteria)
- [Non-Goals](#non-goals)

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     pgtrickle-tui                        │
│                                                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │   App    │  │  Views   │  │  State   │  │  Input  │ │
│  │  (main)  │──│ (ratatui │──│  Store   │──│ Handler │ │
│  │          │  │  widgets)│  │ (model)  │  │ (event) │ │
│  └──────────┘  └──────────┘  └──────────┘  └─────────┘ │
│       │                           ▲                      │
│       │                           │                      │
│  ┌────▼───────────────────────────┴──────────────────┐  │
│  │              Data Layer (async)                    │  │
│  │  ┌──────────────┐  ┌────────────┐  ┌───────────┐ │  │
│  │  │  PgClient    │  │  Poller    │  │  LISTEN/  │ │  │
│  │  │ (tokio-pg)   │  │ (interval) │  │  NOTIFY   │ │  │
│  │  └──────────────┘  └────────────┘  └───────────┘ │  │
│  └───────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
        │                                      │
        │           libpq / TCP                │
        ▼                                      ▼
  ┌───────────────────────────────────────────────────┐
  │            PostgreSQL 18 + pg_trickle             │
  │  pgtrickle.pgt_status()                           │
  │  pgtrickle.recommend_refresh_mode()               │
  │  pgtrickle.refresh_efficiency()                   │
  │  pgtrickle.health_check()                         │
  │  pgtrickle.dependency_tree()                      │
  │  pgtrickle.change_buffer_sizes()                  │
  │  LISTEN pg_trickle_alert                          │
  └───────────────────────────────────────────────────┘
```

**Data flow:**
1. A background tokio task polls `pgtrickle.pgt_status()` and other views at
   a configurable interval (default: 2s for dashboard, 10s for heavy queries
   like diagnostics).
2. A separate LISTEN connection receives `pg_trickle_alert` NOTIFY payloads
   in real-time and pushes them into the alert feed.
3. The state store merges poll results and alert events into a unified model.
4. The ratatui render loop reads from the state store on every tick (60 fps
   max, typically 10–30 fps with adaptive rendering).
5. User input events (keyboard/mouse) are dispatched to the active view's
   handler, which may issue one-shot SQL commands (refresh, alter, etc.) or
   change the navigation state.

---

## Tech Stack

| Component | Crate | Purpose |
|-----------|-------|---------|
| TUI framework | `ratatui` 0.29+ | Terminal rendering, layout, widgets |
| Terminal backend | `crossterm` | Cross-platform terminal I/O |
| Async runtime | `tokio` | Background polling, LISTEN/NOTIFY |
| PostgreSQL client | `tokio-postgres` | Async queries, LISTEN/NOTIFY |
| CLI parser | `clap` 4.x | One-shot subcommands, `--url`, `--format` |
| Serialization | `serde` + `serde_json` | JSON output mode, config file |
| Color theming | `ratatui::style` | Built-in themes, user-configurable |
| Unicode widths | `unicode-width` | Correct CJK/emoji column alignment |
| Config file | `toml` | `~/.config/pgtrickle/config.toml` |

---

## Feature Map

### F1 — Dashboard (Home)

The default view when launching `pgtrickle` with no subcommand. A DAG-aware
overview designed to surface problems immediately — not just a flat list but
a layered display that shows issues in context. The dashboard has three
layout modes that adapt to terminal size.

**Wide layout (≥ 140 cols, ≥ 35 rows) — split-pane with issue sidebar:**

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — dashboard                                    mydb@localhost:5432  ⏱ 2s   ⚠ 3     ║
╠══════════════════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                                ║
║  ┌─ Stream Tables (12 total) ────────────────────────────────┐ ┌─ Issues (3) ────────────────┐ ║
║  │                                                           │ │                              │ ║
║  │  NAME           STATUS  MODE         STALE  EFF  LAST     │ │ ✗ Broken chain (3 affected)  │ ║
║  │  ────────────── ─────── ──────────── ────── ─── ──────── │ │   daily_revenue → 2 downstream║
║  │▸ order_totals   ACTIVE  DIFFERENTIAL ✓ ok   ✓   4s ago   │ │   ERROR 1h 5m ago            │ ║
║  │  big_customers  ACTIVE  DIFFERENTIAL ✓ ok   ✓   4s ago   │ │                              │ ║
║  │  daily_revenue  ERROR   FULL         ✗ yes  ✗   1h ago   │ │ ⚠ Buffer growth              │ ║
║  │  exec_dashboard ACTIVE  DIFFERENTIAL ✓ ok   ⚠   4s ago   │ │   events: 4.2 MB (1,247 rows)│ ║
║  │  monthly_rollup ACTIVE  DIFFERENTIAL ✓ ok   ⚠   4s ago   │ │                              │ ║
║  │  product_stats  ACTIVE  AUTO         ✓ ok   ✓   12s ago  │ │ ⚠ Diamond gap increasing     │ ║
║  │  user_activity  ACTIVE  IMMEDIATE    ✓ ok   ✓   0s ago   │ │   group_1: 340ms (trending ▅)│ ║
║  │  region_summary ACTIVE  DIFFERENTIAL ✓ ok   ✓   2m ago   │ │                              │ ║
║  │  inv_snapshot   ACTIVE  DIFFERENTIAL ✓ ok   ✓   6s ago   │ │                              │ ║
║  │  ...                                                      │ │                              │ ║
║  └───────────────────────────────────────────────────────────┘ └──────────────────────────────┘ ║
║                                                                                                ║
║  ┌─ DAG Mini-Map ────────────────────────────────────────────────────────────────────────────┐  ║
║  │  orders ─┬─▸ daily_revenue ✗ ─┬─▸ exec_dashboard ⚠   events ─▸ user_activity ─▸ daily.. │  ║
║  │  products┘                    └─▸ monthly_rollup ⚠   inventory ─▸ inv_snapshot           │  ║
║  └───────────────────────────────────────────────────────────────────────────────────────────┘  ║
║                                                                                                ║
╠══════════════════════════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Navigate  Enter Detail  r Refresh  / Filter  i Issues  g Graph  d Diag  ? Help             ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════╝
```

**Standard layout (80–139 cols) — table with status ribbon:**

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — dashboard                          mydb@localhost:5432  ⏱ 2s ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  ✗ 1 error  ⚠ 2 cascade-stale  ● 9 healthy           ⚠ 3 issues  [i]     ║
║                                                                            ║
║  Stream Tables (12 total, 1 error, 2 cascade-stale)      Filter: ____     ║
║                                                                            ║
║  NAME             SCHEMA  STATUS  MODE           STALE  EFF  LAST REFRESH  ║
║  ──────────────── ─────── ─────── ────────────── ────── ─── ────────────── ║
║▸ order_totals     public  ACTIVE  DIFFERENTIAL   ✓ ok   ✓   4s ago         ║
║  big_customers    public  ACTIVE  DIFFERENTIAL   ✓ ok   ✓   4s ago         ║
║  daily_revenue    public  ERROR   FULL           ✗ yes  ✗   1h ago         ║
║  exec_dashboard   public  ACTIVE  DIFFERENTIAL   ✓ ok   ⚠   4s ago         ║
║  monthly_rollup   public  ACTIVE  DIFFERENTIAL   ✓ ok   ⚠   4s ago         ║
║  product_stats    sales   ACTIVE  AUTO           ✓ ok   ✓   12s ago        ║
║  region_summary   public  ACTIVE  DIFFERENTIAL   ✓ ok   ✓   2m ago         ║
║  user_activity    public  ACTIVE  IMMEDIATE      ✓ ok   ✓   0s ago         ║
║  inv_snapshot     ops     ACTIVE  DIFFERENTIAL   ✓ ok   ✓   6s ago         ║
║  ...                                                                       ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Navigate  Enter Detail  r Refresh  / Filter  i Issues  g Graph  ? Help ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Key dashboard elements:**

1. **Status ribbon** (always visible): One-line summary showing error count,
   cascade-stale count, healthy count, and total issue count with `[i]`
   shortcut to the DAG Issues view (F20). Color-coded: red for errors,
   yellow for cascade-stale, green for healthy.

2. **EFF (Effective Staleness) column** (from F21): Shows cascade staleness.
   `✓` = data is fresh end-to-end, `⚠` = data is cascade-stale (upstream
   has issues), `✗` = table itself is in error. This is the most important
   column for DAG health — it's what distinguishes "this table refreshed
   recently" from "this table's data is actually correct."

3. **Issues sidebar** (wide layout only): Shows the top issues from F20
   inline on the dashboard. Each issue is a clickable card — `Enter` on an
   issue navigates to the relevant view. Shows severity icon, brief
   description, and affected table count.

4. **DAG mini-map** (wide + tall layout): A single-line compressed DAG
   at the bottom of the dashboard showing all chains. Error nodes marked
   with `✗`, cascade-stale nodes with `⚠`. Nodes are abbreviated to fit.
   Clicking a node selects it in the table above.

5. **Issue badge in header**: `⚠ 3` badge in the header bar is visible
   from **every view**, not just the dashboard. Provides constant awareness
   of outstanding issues.

**Columns:** Name, Schema, Status (color-coded: green=ACTIVE, red=ERROR,
yellow=SUSPENDED, grey=PAUSED/FROZEN), Refresh Mode, Staleness (per-table),
Effective Staleness (cascade-aware), Last Refresh (relative time).
Additional columns available via column picker (Tier, Buffer, Size, etc.).

**Features:**
- **Sortable:** Press column letter to sort. Active sort indicated by `▴`/`▾`.
  Default sort: errors first, then cascade-stale, then by name.
- **Filterable:** `/` opens a filter bar. Matches name, schema, status, mode.
  Special filters: `/stale` shows only stale/cascade-stale, `/error` shows
  errors, `/issues` shows tables involved in any issue.
- **Status summary ribbon:** Total count, error count, cascade-stale count,
  healthy count. Updates in real-time.
- **Auto-refresh:** Table refreshes every poll interval. Manual refresh with
  `Ctrl+R`.
- **Color indicators:** Status column uses ANSI colors: green (ACTIVE),
  red (ERROR), yellow (SUSPENDED), dim (PAUSED/FROZEN). EFF column: green
  (✓), yellow (⚠ cascade-stale), red (✗ error).
- **Cascade-stale highlight:** Rows with cascade staleness (upstream error)
  get a subtle yellow background tint — visually distinct from direct errors
  (red tint) and direct staleness.
- **Quick actions from dashboard:** `r` triggers refresh on selected ST,
  `Enter` opens detail view, `d` opens diagnostics, `g` opens DAG graph,
  `i` opens DAG Issues view.

**Data source:** `pgtrickle.pgt_status()` polled every 2s,
`pgtrickle.dependency_tree()` polled every 10s (for cascade computation).

---

### F2 — Stream Table Detail View

Deep dive into a single stream table. Opened by pressing `Enter` on a
dashboard row or via `pgtrickle status <name>`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  order_totals — detail                           mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  ┌─ Properties ──────────────────┐  ┌─ Refresh Stats ──────────────────┐  ║
║  │ Schema:    public             │  │ Total refreshes:    1,247        │  ║
║  │ Status:    ACTIVE ●           │  │ Differential:       1,239 (99%) │  ║
║  │ Mode:      DIFFERENTIAL       │  │ Full:               8 (1%)      │  ║
║  │ Tier:      hot                │  │ Avg duration:       42ms        │  ║
║  │ Schedule:  5m                 │  │ P95 duration:       128ms       │  ║
║  │ CDC mode:  trigger            │  │ Last duration:      38ms        │  ║
║  │ Fuse:      ARMED              │  │ Speedup (F→D):      12.4×       │  ║
║  │ Sources:   orders, products   │  │ Buffer rows:        42          │  ║
║  └───────────────────────────────┘  └─────────────────────────────────┘  ║
║                                                                            ║
║  ┌─ Defining Query ──────────────────────────────────────────────────────┐ ║
║  │ SELECT customer_id, SUM(amount) AS total, COUNT(*) AS cnt            │ ║
║  │ FROM orders                                                          │ ║
║  │ JOIN products ON orders.product_id = products.id                     │ ║
║  │ GROUP BY customer_id                                                 │ ║
║  └───────────────────────────────────────────────────────────────────────┘ ║
║                                                                            ║
║  ┌─ Recent Refreshes ───────────────────────────────────────────────────┐  ║
║  │ 09:42:18  DIFF  38ms   42 changes   ✓                               │  ║
║  │ 09:37:16  DIFF  41ms   18 changes   ✓                               │  ║
║  │ 09:32:14  DIFF  22ms    3 changes   ✓                               │  ║
║  │ 09:27:12  DIFF  45ms   91 changes   ✓                               │  ║
║  │ 09:22:10  FULL  470ms  —            ✓ (scheduled)                   │  ║
║  └───────────────────────────────────────────────────────────────────────┘ ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Esc Back  r Refresh  a Alter  e Export DDL  Tab Section  ? Help           ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Panes (navigable with Tab):**
1. **Properties** — Core metadata from `pgt_stream_tables` catalog.
2. **Refresh Stats** — Aggregated from `pgtrickle.st_refresh_stats()` and
   `pgtrickle.refresh_efficiency()`.
3. **Defining Query** — Syntax-highlighted SQL (keyword highlighting via
   simple regex, not a full parser).
4. **Upstream Health** — Mini dependency chain showing status of all upstream
   tables. If any upstream is in ERROR/SUSPENDED, shows the root cause, how
   long data has been cascade-stale, and the full path from root cause to
   this table. Green checkmark when all upstream is healthy.
5. **Recent Refreshes** — Scrollable list from `pgtrickle.refresh_timeline()`.
6. **Error Details** — Only shown when `status = 'ERROR'`. Displays
   `last_error_message` prominently in red. Also shows blast radius
   (downstream tables affected by this error).
7. **Sources** — Source tables with CDC status from `pgtrickle.list_sources()`.

**Actions:**
- `r` — Trigger manual refresh (`pgtrickle.refresh_stream_table()`).
- `a` — Open alter dialog (tier, mode, schedule, status).
- `e` — Export DDL via `pgtrickle.export_definition()` to clipboard or file.
- `T` — Cycle tier (hot → warm → cold → frozen → hot).
- `Enter` on a source table — Navigate to its detail view (if it's an ST).

**Data sources:** `pgtrickle.pgt_status()`, `pgtrickle.st_refresh_stats()`,
`pgtrickle.refresh_timeline()`, `pgtrickle.list_sources()`,
`pgtrickle.export_definition()`.

---

### F3 — Dependency Graph View

ASCII-art visualization of the stream table DAG with integrated issue
overlays. The primary tool for understanding the shape of your data
pipelines and spotting structural problems. Opened with `g` from the
dashboard or `pgtrickle graph`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — dependency graph                   mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║     orders ──────┐                                                         ║
║                  ├──▸ order_totals ──┐                                     ║
║     products ────┘                   │                                     ║
║                                      ├──▸ executive_dashboard ◆            ║
║     customers ───▸ big_customers ────┘                                     ║
║                                                                            ║
║     orders ──────▸ daily_revenue ✗ ─ ─▸ exec_dashboard ⚠                  ║
║                                 │                                          ║
║                                 └─ ─▸ monthly_rollup ⚠                    ║
║                                                                            ║
║     events ──────▸ user_activity ──▸ daily_metrics                         ║
║                                                                            ║
║     inventory ───▸ inv_snapshot (standalone)                               ║
║                                                                            ║
║  Legend: ──▸ healthy  ─ ─▸ broken/stale  ✗ ERROR  ⚠ cascade-stale         ║
║          ◆ diamond group  ○ cycle member                                   ║
║                                                                            ║
║  Selected: daily_revenue ✗                                                 ║
║  Depth: 2  Sources: 1  Dependents: 2  Blast radius: 3 tables              ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ←→ Navigate  Enter Detail  S Staleness map  h/l Collapse  f Fit  ? Help   ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Topological layout:** Nodes arranged left-to-right by dependency depth.
  Source tables on the left, leaf stream tables on the right.
- **Interactive navigation:** Arrow keys move between nodes. Selected node
  is highlighted. `Enter` opens the detail view.
- **Color coding:** Node color matches status (green/red/yellow). Diamond
  group members share a border style (e.g., double-line `║`). Cycle members
  get ○ marker.
- **Broken edge rendering:** Edges downstream of an ERROR node are rendered
  as dashed lines (`─ ─▸`) in red. Cascade-stale nodes show `⚠` marker.
  This makes it immediately obvious which parts of the DAG are affected by
  a single failure.
- **Blast radius in metadata:** When an ERROR or stale node is selected,
  the metadata strip shows the total blast radius (number of downstream
  tables affected). `B` key toggles highlighting of just the blast radius
  nodes.
- **Staleness heatmap overlay:** `S` key toggles a heatmap mode where
  each node is colored by effective staleness age (green = fresh,
  yellow = approaching schedule, orange = overdue, red = stale or error).
  Useful for spotting patterns in large DAGs.
- **Zoom/pan:** `+`/`-` to zoom, `f` to fit entire graph in viewport.
  For large DAGs (50+ nodes), nodes collapse by schema with `h`/`l`.
- **Layout engine:** Simple Sugiyama-style layered layout (nodes assigned to
  layers by topological depth, edges routed with minimal crossings). Uses
  data from `pgtrickle.dependency_tree()`.

**Data source:** `pgtrickle.dependency_tree()`, `pgtrickle.diamond_groups()`,
`pgtrickle.pgt_scc_status()`.

---

### F4 — Live Refresh Log

Scrollable, auto-tailing log of refresh events across all stream tables.
Opened with `l` from the dashboard or `pgtrickle log`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — refresh log                        mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  09:42:18.123  DIFF  order_totals       38ms   42 Δ   ✓                    ║
║  09:42:18.098  DIFF  big_customers      12ms    5 Δ   ✓                    ║
║  09:42:17.991  DIFF  product_stats      67ms  128 Δ   ✓                    ║
║  09:37:16.445  DIFF  order_totals       41ms   18 Δ   ✓                    ║
║  09:37:16.401  DIFF  big_customers       9ms    2 Δ   ✓                    ║
║  09:37:15.887  FULL  daily_revenue     470ms    — Δ   ✗ function max(jsonb)║
║                                                         does not exist     ║
║  09:32:14.221  DIFF  order_totals       22ms    3 Δ   ✓                    ║
║  09:32:14.198  SKIP  region_summary      —      — Δ   — (cold tier)       ║
║  ...                                                                       ║
║                                                                            ║
║  Filter: [all]  Showing: 142 events  Auto-scroll: ON                       ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Scroll  / Filter  a Auto-scroll  Enter Detail  c Copy  ? Help          ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Auto-tailing:** New entries appear at the top. `a` toggles auto-scroll.
- **Color coding:** Success (green ✓), failure (red ✗), skip (grey —).
  Error messages shown inline in red.
- **Filter:** By ST name, mode, status. `/order` shows only order_totals.
- **Scrollable:** `j`/`k` or arrow keys scroll. `G` jumps to latest,
  `gg` jumps to oldest loaded.
- **Copy:** `c` copies selected entry to clipboard (for pasting into
  bug reports / Slack).
- **Entry expansion:** `Enter` on an error entry shows the full error
  message and stack context.

**Data source:** `pgtrickle.get_refresh_history(NULL, 500)` on initial load,
then incremental polling for new entries. NOTIFY `pg_trickle_alert` for
real-time `RefreshCompleted`/`RefreshFailed` events.

---

### F5 — Diagnostics Panel

Interactive view of `recommend_refresh_mode()` and `refresh_efficiency()`
results. Opened with `d` from the dashboard or `pgtrickle diag`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — diagnostics                        mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  Refresh Mode Recommendations                                              ║
║                                                                            ║
║  NAME             CURRENT     RECOMMENDED  CONFIDENCE  SPEEDUP             ║
║  ──────────────── ─────────── ──────────── ────────── ────────             ║
║  order_totals     DIFFERENTIAL DIFFERENTIAL high ●●●   12.4×               ║
║  big_customers    DIFFERENTIAL DIFFERENTIAL high ●●●    8.7×               ║
║▸ daily_revenue    FULL         FULL         high ●●●    1.0×               ║
║  product_stats    AUTO         DIFFERENTIAL med  ●●○    3.2×               ║
║  region_summary   DIFFERENTIAL FULL         low  ●○○    0.8×               ║
║  user_activity    IMMEDIATE    IMMEDIATE    high ●●●    —                  ║
║                                                                            ║
║  ┌─ Signal Breakdown: daily_revenue ─────────────────────────────────────┐ ║
║  │                                                                       │ ║
║  │  historical_change_ratio ████████████████████░░░░░  0.82 (wt: 0.30)  │ ║
║  │  empirical_timing        ██████████████████████████  0.95 (wt: 0.35)  │ ║
║  │  current_change_ratio    █████████████████░░░░░░░░  0.71 (wt: 0.25)  │ ║
║  │  query_complexity        ████████░░░░░░░░░░░░░░░░░  0.35 (wt: 0.10)  │ ║
║  │  target_size             ██████████████░░░░░░░░░░░  0.58 (wt: 0.10)  │ ║
║  │  index_coverage          ██████████████████████████  1.00 (wt: 0.05)  │ ║
║  │  latency_variance        █████████░░░░░░░░░░░░░░░░  0.40 (wt: 0.05)  │ ║
║  │                                                                       │ ║
║  │  Composite score: 0.79 → FULL recommended (above 0.60 threshold)      │ ║
║  │  Reason: High change-to-row ratio (82%) makes differential overhead   │ ║
║  │          exceed full refresh cost. Query has no selective indexes.     │ ║
║  └───────────────────────────────────────────────────────────────────────┘ ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Navigate  Enter Signals  a Apply  Esc Back  ? Help                     ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Recommendation table:** Shows current vs. recommended mode with
  confidence indicator (filled/empty circles) and speedup factor.
- **Signal breakdown:** Bar charts for each of the 7 diagnostic signals
  with weight annotations. Human-readable reason text below.
- **Highlight mismatches:** Rows where current ≠ recommended are highlighted
  in yellow. High-confidence mismatches are highlighted in orange.
- **Apply action:** `a` on a mismatched row opens a confirmation dialog to
  `ALTER STREAM TABLE … SET (refresh_mode = '…')`.
- **Refresh:** `Ctrl+R` re-runs diagnostics (they are not auto-polled since
  the query is heavier).

**Data sources:** `pgtrickle.recommend_refresh_mode()`,
`pgtrickle.refresh_efficiency()`.

---

### F6 — CDC Health View

Overview of CDC infrastructure health. Opened with `c` from the dashboard
or `pgtrickle cdc`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — CDC health                         mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  ┌─ Overall ─────────────────────────────────────────────┐                 ║
║  │ CDC Mode: trigger (hybrid)  Triggers: 24 OK / 0 miss │                 ║
║  │ WAL Slots: 1 active, 0 lag warnings                  │                 ║
║  └───────────────────────────────────────────────────────┘                 ║
║                                                                            ║
║  Change Buffers                                                            ║
║  SOURCE TABLE       BUFFER              ROWS     SIZE     LOGGED           ║
║  ────────────────── ─────────────────── ──────── ──────── ────────         ║
║  orders             changes_16384       42       128 kB   unlogged         ║
║  products           changes_16385       0        8 kB     unlogged         ║
║  customers          changes_16386       5        32 kB    logged           ║
║  events             changes_16387       1,247    4.2 MB   unlogged ⚠       ║
║  inventory          changes_16388       0        8 kB     logged           ║
║                                                                            ║
║  Trigger Inventory                                                         ║
║  SOURCE TABLE       TRIGGER NAME               FIRING  EVENTS             ║
║  ────────────────── ──────────────────────────  ─────── ─────────────      ║
║  orders             pgt_cdc_16384_after        ALWAYS  INSERT/UPDATE/DEL   ║
║  products           pgt_cdc_16385_after        ALWAYS  INSERT/UPDATE/DEL   ║
║  ...                                                                       ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Navigate  Enter Detail  Esc Back  ? Help                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Buffer growth warnings:** Rows with buffer size above 1 MB get ⚠ marker.
- **Trigger health:** Missing triggers shown in red.
- **Logged vs. unlogged indicators:** Shows whether buffer is UNLOGGED.
- **WAL slot status:** `pgtrickle.slot_health()` results if WAL CDC is active.

**Data sources:** `pgtrickle.change_buffer_sizes()`,
`pgtrickle.trigger_inventory()`, `pgtrickle.health_check()`,
`pgtrickle.slot_health()`.

---

### F7 — Configuration Editor

Browse and edit pg_trickle GUC parameters. Opened with `C` (shift-c) from
the dashboard or `pgtrickle config`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — configuration                      mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  GUC Parameter                           Value        Default   Context    ║
║  ─────────────────────────────────────── ──────────── ───────── ───────    ║
║▸ pg_trickle.enabled                      true         true      sighup     ║
║  pg_trickle.scheduler_interval_ms        1000         1000      sighup     ║
║  pg_trickle.cdc_mode                     auto         auto      suset      ║
║  pg_trickle.tiered_scheduling            true         true      sighup     ║
║  pg_trickle.unlogged_buffers             false        false     suset      ║
║  pg_trickle.differential_max_change_ratio 0.50        0.50      suset      ║
║  pg_trickle.planner_aggressive           false        false     suset      ║
║  pg_trickle.merge_work_mem_mb            256          256       suset      ║
║  pg_trickle.max_consecutive_errors       3            3         suset      ║
║  ...                                                                       ║
║                                                                            ║
║  ┌─ pg_trickle.enabled ──────────────────────────────────────────────────┐ ║
║  │ Master switch for the background scheduler. When false, no automatic  │ ║
║  │ refreshes are performed. Manual refreshes still work.                 │ ║
║  │                                                                       │ ║
║  │ Type: boolean  Context: sighup  Requires restart: no                  │ ║
║  └───────────────────────────────────────────────────────────────────────┘ ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Navigate  Enter Edit  r Reset  Esc Back  ? Help                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Browse GUCs:** All 23+ pg_trickle GUC parameters listed.
- **Inline docs:** Description panel for the selected GUC.
- **Edit:** `Enter` opens an inline editor. Boolean GUCs toggle with `Space`.
  Numeric GUCs get a text input with validation.
- **Modified indicator:** Changed-from-default values highlighted in cyan.
- **Context awareness:** Shows whether the GUC requires a reload or restart.
- **Apply:** Changes applied via `ALTER SYSTEM SET` + `pg_reload_conf()`.
  Confirmation dialog shows the SQL before executing.
- **Reset:** `r` resets the selected GUC to its default value.

**Data source:** `SHOW <guc>` for each parameter, or
`SELECT * FROM pg_settings WHERE name LIKE 'pg_trickle.%'`.

---

### F8 — Alert Feed

Real-time stream of pg_trickle NOTIFY alerts. Opened with `!` from the
dashboard or `pgtrickle alerts`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — alerts                             mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  09:37:15  ✗ RefreshFailed        daily_revenue                            ║
║            function max(jsonb) does not exist                              ║
║                                                                            ║
║  09:35:02  ⚠ BufferGrowthWarning  events                                  ║
║            Change buffer has 1,247 rows (> 1000 threshold)                 ║
║                                                                            ║
║  09:22:10  ● RefreshCompleted     order_totals                             ║
║            FULL refresh completed in 470ms (scheduled maintenance)         ║
║                                                                            ║
║  09:00:00  ⚠ FrozenTierSkip      region_summary                           ║
║            Skipped — tier is frozen                                        ║
║                                                                            ║
║  Listening on: pg_trickle_alert  Events received: 47                       ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Scroll  / Filter  c Clear  Enter Detail  Esc Back  ? Help              ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Real-time:** Events arrive via `LISTEN pg_trickle_alert` with zero
  polling delay. JSON payloads are parsed and rendered.
- **Toast notifications:** Critical alerts (RefreshFailed, AutoSuspended)
  show a temporary toast popup regardless of which view is active.
- **Severity icons:** ✗ error, ⚠ warning, ● info.
- **Filter:** By category, ST name, severity.
- **Clear:** `c` clears the alert buffer (does not affect the database).
- **Navigate:** `Enter` on an alert opens the related ST's detail view.

**Data source:** `LISTEN pg_trickle_alert` (real-time NOTIFY channel).

---

### F9 — Command Palette

Quick-access command palette, inspired by VS Code. Opened with `:`
from any view.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  : refresh order_totals_                                                    ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▸ refresh <name>         Trigger manual refresh                           ║
║    refresh all            Refresh all active stream tables                  ║
║    alter <name> tier=cold Change tier                                       ║
║    alter <name> mode=full Change refresh mode                               ║
║    resume <name>          Resume suspended/error stream table               ║
║    repair <name>          Repair stream table                               ║
║    export <name>          Export DDL to clipboard                            ║
║    drop <name>            Drop stream table (requires confirmation)         ║
║    create                 Open create wizard                                ║
║    connect <url>          Switch database connection                        ║
║    theme <name>           Switch color theme                                ║
║    quit                   Exit pgtrickle                                    ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Fuzzy search:** Type to filter commands. ST names auto-complete.
- **Recent commands:** Most recently used commands appear first.
- **Confirmation:** Destructive commands (drop) require explicit `y/n`.
- **Inline feedback:** Command result shown as a temporary notification
  bar at the bottom of the screen.

---

### F10 — One-Shot CLI Mode

When `pgtrickle` is invoked with a subcommand, it runs non-interactively
(no TUI) and exits. This preserves all original E3 CLI functionality for
scripting and CI.

```bash
# List all stream tables
pgtrickle list
pgtrickle list --format json

# Status of one stream table
pgtrickle status order_totals
pgtrickle status order_totals --format json

# Trigger manual refresh
pgtrickle refresh order_totals
pgtrickle refresh --all

# Create a stream table
pgtrickle create my_st "SELECT id, SUM(amount) FROM orders GROUP BY id"

# Drop a stream table
pgtrickle drop my_st

# Alter tier/mode/schedule
pgtrickle alter my_st --tier cold
pgtrickle alter my_st --mode full
pgtrickle alter my_st --schedule 10m

# Export DDL
pgtrickle export my_st

# Show diagnostics
pgtrickle diag
pgtrickle diag order_totals --format json

# Show CDC health
pgtrickle cdc

# Show dependency graph (ASCII)
pgtrickle graph

# Show configuration
pgtrickle config
pgtrickle config --set pg_trickle.unlogged_buffers=true

# Launch interactive TUI (default when no subcommand)
pgtrickle
pgtrickle --url "postgres://user:pass@host:5432/mydb"
```

**Connection parameters:**
- `--url` / `-U` — Full connection URL.
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` — Standard
  libpq environment variables (used when `--url` is not set).
- `--host`, `--port`, `--dbname`, `--user` — Explicit overrides.

**Output formats:**
- Default: Human-readable tables (using `comfy-table`).
- `--format json` — Machine-parseable JSON on stdout.
- `--format csv` — CSV output for piping to other tools.
- Exit codes: 0 = success, 1 = error, 2 = not found.

---

### F11 — Sparklines & Micro-Charts

Small inline visualizations integrated into the dashboard and detail views.

**Sparklines in dashboard:**
- Refresh duration trend (last 20 refreshes) shown as a mini sparkline
  bar `▁▂▃▅▂▁▃▄▂▁` next to each ST in the dashboard when terminal width
  permits (≥ 120 columns).

**Micro-charts in detail view:**
- Refresh duration histogram (last 100 refreshes).
- Buffer size trend (last 50 poll cycles).
- Change count per refresh bar chart.

**Implementation:** Uses ratatui's `Sparkline` widget and custom `BarChart`
widgets. Data collected from polling history stored in the state store
(ring buffer of last N data points per ST).

---

### F12 — Help System

Context-sensitive help accessible from any view with `?`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  Help — Dashboard                                                          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  Navigation                                                                ║
║  ──────────                                                                ║
║  ▴/▾ or j/k     Move selection up/down                                     ║
║  Enter           Open detail view for selected stream table                ║
║  Esc             Go back / close dialog                                    ║
║  Tab             Switch between panes                                      ║
║  1-8             Jump to view (1=Dashboard, 2=Detail, 3=Graph, ...)        ║
║                                                                            ║
║  Actions                                                                   ║
║  ───────                                                                   ║
║  r               Refresh selected stream table                             ║
║  /               Open filter bar                                           ║
║  :               Open command palette                                      ║
║  Ctrl+R          Force data refresh (re-poll database)                     ║
║                                                                            ║
║  Views                                                                     ║
║  ─────                                                                     ║
║  g               Dependency graph                                          ║
║  l               Refresh log                                               ║
║  d               Diagnostics                                               ║
║  c               CDC health                                                ║
║  C               Configuration                                             ║
║  !               Alerts                                                    ║
║                                                                            ║
║  General                                                                   ║
║  ───────                                                                   ║
║  ?               Toggle this help                                          ║
║  q               Quit                                                      ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Press ? or Esc to close                                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Context-sensitive:** Help content changes based on the active view.
- **Keybinding reference:** Complete list of keyboard shortcuts.
- **Inline hints:** The footer bar in every view shows the most important
  keybindings for that view.

---

### F13 — Scheduler & Worker Pool View

Real-time view of the background scheduler and parallel worker pool.
Opened with `w` from the dashboard or `pgtrickle workers`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — scheduler & workers                mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  ┌─ Scheduler ──────────────────────────────────────────────────────────┐  ║
║  │ Status: RUNNING ●   Uptime: 4d 12h 37m   Interval: 1000ms          │  ║
║  │ Enabled: true       Tick count: 389,412   Last tick: 0.3s ago       │  ║
║  │ Stream tables: 12   Pending: 2            Skipped (frozen): 1       │  ║
║  └──────────────────────────────────────────────────────────────────────┘  ║
║                                                                            ║
║  ┌─ Worker Pool ────────────────────────────────────────────────────────┐  ║
║  │ Active workers: 3/4    Budget: 4    Mode: parallel                  │  ║
║  │                                                                     │  ║
║  │  WORKER  STATE     TABLE            STARTED    DURATION             │  ║
║  │  ─────── ───────── ──────────────── ────────── ────────             │  ║
║  │  w-1     RUNNING   order_totals     09:42:18   12ms                 │  ║
║  │  w-2     RUNNING   big_customers    09:42:18   8ms                  │  ║
║  │  w-3     RUNNING   product_stats    09:42:18   45ms                 │  ║
║  │  w-4     IDLE      —                —          —                    │  ║
║  └──────────────────────────────────────────────────────────────────────┘  ║
║                                                                            ║
║  ┌─ Job Queue ──────────────────────────────────────────────────────────┐  ║
║  │  POSITION  TABLE              PRIORITY  QUEUED AT    WAIT           │  ║
║  │  ────────  ─────────────────  ────────  ─────────    ────           │  ║
║  │  1         daily_revenue      hot       09:42:18     0.3s           │  ║
║  │  2         region_summary     cold      09:42:15     3.3s           │  ║
║  └──────────────────────────────────────────────────────────────────────┘  ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Navigate  Enter Detail  p Pause Scheduler  Esc Back  ? Help            ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Scheduler heartbeat:** Shows scheduler status, uptime, tick count, and
  how recently the last tick fired. Red indicator if scheduler appears stalled
  (last tick > 5× interval).
- **Worker pool:** Real-time display of all parallel workers — which tables
  they're refreshing, how long each job has been running. Useful for
  diagnosing stuck refreshes.
- **Job queue:** Pending refresh jobs ordered by priority. Shows wait time.
- **Pause/resume:** `p` toggles `pg_trickle.enabled` GUC (with confirmation
  dialog). Useful for maintenance windows.
- **Worker sparklines:** When width permits, shows per-worker utilization
  over the last 60 seconds as a mini chart.

**Data sources:** `pgtrickle.worker_pool_status()`,
`pgtrickle.parallel_job_status()`, `pgtrickle.pgt_status()`.

---

### F14 — Fuse & Circuit Breaker Panel

View and manage the error circuit breaker (fuse) for all stream tables.
Opened with `F` from the dashboard or `pgtrickle fuse`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — fuse status                        mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  Fuse Circuit Breakers (12 stream tables)                                  ║
║                                                                            ║
║  NAME             FUSE     ERRORS  MAX  LAST ERROR                         ║
║  ──────────────── ──────── ─────── ──── ──────────────────────────────     ║
║  order_totals     ARMED ●  0       3    —                                  ║
║  big_customers    ARMED ●  0       3    —                                  ║
║▸ daily_revenue    BLOWN ✗  1       3    function max(jsonb) does not exist ║
║  product_stats    ARMED ●  0       3    —                                  ║
║  region_summary   ARMED ●  0       3    —                                  ║
║  user_activity    ARMED ●  1       3    timeout during IMMEDIATE trigger   ║
║  ...                                                                       ║
║                                                                            ║
║  ┌─ daily_revenue — Fuse Detail ─────────────────────────────────────────┐ ║
║  │ Status:    BLOWN (ERROR state)                                        │ ║
║  │ Errors:    1 / 3 max                                                  │ ║
║  │ Blown at:  2026-04-02 09:37:15 UTC                                    │ ║
║  │ Error:     function max(jsonb) does not exist                         │ ║
║  │                                                                       │ ║
║  │ Reset options:                                                        │ ║
║  │   [A] Apply — Re-arm fuse, keep data, retry on next tick             │ ║
║  │   [R] Reinitialize — Re-arm fuse, full refresh from scratch          │ ║
║  │   [S] Skip changes — Re-arm fuse, discard pending changes            │ ║
║  └───────────────────────────────────────────────────────────────────────┘ ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Navigate  A/R/S Reset  Enter Detail  Esc Back  ? Help                  ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Fuse overview table:** Every ST's fuse state at a glance. ARMED (green),
  pre-armed with errors (yellow), BLOWN (red).
- **Error detail:** Full error message displayed for the selected ST.
- **Inline reset:** `A`/`R`/`S` keys trigger `pgtrickle.reset_fuse()` with
  the corresponding strategy — no need to remember SQL syntax.
- **Error history sparkline:** For STs with recurring errors, shows a mini
  pattern of error frequency over time.
- **Batch reset:** When multi-select is active (F17), reset all selected
  blown fuses at once.

**Data sources:** `pgtrickle.fuse_status()`, `pgtrickle.reset_fuse()`.

---

### F15 — Watermark & Source Gating View

Manage ETL coordination primitives: watermarks for external load progress
tracking and source gates for blocking/unblocking refresh triggers.
Opened with `W` from the dashboard or `pgtrickle watermarks`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — watermarks & source gates          mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  [Tab: Watermarks]  [Source Gates]                                         ║
║                                                                            ║
║  Watermark Groups                                                          ║
║  GROUP          SOURCES           TOLERANCE  ALIGNED  STATUS               ║
║  ────────────── ───────────────── ────────── ──────── ────────             ║
║  etl_batch_1    orders, products  30s        ✓ yes    ready                ║
║  etl_batch_2    events, logs      1m         ✗ no     waiting (events)     ║
║                                                                            ║
║  Per-Source Watermarks                                                      ║
║  SOURCE           WATERMARK            LAST ADVANCE     LAG               ║
║  ──────────────── ──────────────────── ──────────────── ─────             ║
║  orders           2026-04-02 09:40:00  2m ago           OK                ║
║  products         2026-04-02 09:40:00  2m ago           OK                ║
║  events           2026-04-02 09:30:00  12m ago          ⚠ stale           ║
║  logs             2026-04-02 09:38:00  4m ago           OK                ║
║                                                                            ║
║  ─────────────────────────────────────────────────────────────             ║
║                                                                            ║
║  Source Gates                                                              ║
║  SOURCE           GATED    REASON              AFFECTED STs               ║
║  ──────────────── ──────── ─────────────────── ─────────────              ║
║  events           ✓ GATED  maintenance window  user_activity, daily_met.. ║
║  orders           ✗ open   —                   order_totals, big_cust..   ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Navigate  a Advance  g Gate/Ungate  Tab Switch  Esc Back  ? Help       ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Watermark groups:** Group alignment status — are all sources in the group
  at the same watermark? Warns when a source falls behind.
- **Per-source watermarks:** Current watermark value, last advance time,
  and lag indicator. Stale watermarks (no advance in 2× expected interval)
  highlighted in yellow.
- **Advance watermark:** `a` opens inline input to advance a source's
  watermark (for manual ETL coordination).
- **Source gates:** Shows which sources are gated (blocked from triggering
  refreshes), by whom, and which stream tables are affected.
- **Gate/ungate toggle:** `g` gates or ungates the selected source with a
  reason prompt. Useful for maintenance windows or data loading pauses.
- **Tab switching:** Toggle between watermark and gate sub-views.

**Data sources:** `pgtrickle.watermarks()`, `pgtrickle.watermark_groups()`,
`pgtrickle.watermark_status()`, `pgtrickle.advance_watermark()`,
`pgtrickle.source_gates()`, `pgtrickle.bootstrap_gate_status()`,
`pgtrickle.gate_source()`, `pgtrickle.ungate_source()`.

---

### F16 — Delta SQL Inspector

View and analyze the generated DVM delta SQL for any stream table. Critical
for performance debugging — when a differential refresh is slow, this shows
exactly what SQL the engine is executing. Opened with `x` from the detail
view or `pgtrickle explain <name>`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  order_totals — delta SQL inspector              mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  ┌─ DVM Delta SQL ──────────────────────────────────────────────────────┐  ║
║  │ WITH delta_orders AS (                                                │  ║
║  │   SELECT customer_id, amount, _pgt_op                                │  ║
║  │   FROM pgtrickle_changes.changes_16384                               │  ║
║  │   WHERE _pgt_change_seq > $1                                         │  ║
║  │ ),                                                                    │  ║
║  │ agg_delta AS (                                                        │  ║
║  │   SELECT customer_id,                                                 │  ║
║  │          SUM(CASE WHEN _pgt_op = 'I' THEN amount                     │  ║
║  │                   WHEN _pgt_op = 'D' THEN -amount END) AS d_total,   │  ║
║  │          SUM(CASE WHEN _pgt_op = 'I' THEN 1                          │  ║
║  │                   WHEN _pgt_op = 'D' THEN -1 END) AS d_cnt           │  ║
║  │   FROM delta_orders                                                   │  ║
║  │   GROUP BY customer_id                                                │  ║
║  │ )                                                                     │  ║
║  │ MERGE INTO order_totals t USING agg_delta d ON ...                    │  ║
║  └───────────────────────────────────────────────────────────────────────┘  ║
║                                                                            ║
║  ┌─ EXPLAIN ANALYZE (last refresh) ─────────────────────────────────────┐  ║
║  │ Merge on order_totals (cost=0.42..89.12 rows=42 actual time=38ms)   │  ║
║  │   -> Hash Join (cost=12.50..45.00 rows=42)                           │  ║
║  │        -> Seq Scan on changes_16384 (rows=42)                        │  ║
║  │        -> Hash (rows=1000)                                            │  ║
║  │             -> Seq Scan on order_totals (rows=1000)                   │  ║
║  │ Planning Time: 0.5ms  Execution Time: 38.2ms                         │  ║
║  └───────────────────────────────────────────────────────────────────────┘  ║
║                                                                            ║
║  ┌─ DVM Operator Tree ──────────────────────────────────────────────────┐  ║
║  │ Aggregate(SUM, COUNT)                                                 │  ║
║  │  └─ InnerJoin(orders.product_id = products.id)                       │  ║
║  │       ├─ Scan(orders) [CDC: trigger]                                  │  ║
║  │       └─ Scan(products) [CDC: trigger]                                │  ║
║  └───────────────────────────────────────────────────────────────────────┘  ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Tab Pane  y Copy SQL  E Run EXPLAIN  Esc Back  ? Help                     ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Panes (navigable with Tab):**
1. **DVM Delta SQL** — The actual generated SQL with syntax highlighting.
   Scrollable. `y` copies to clipboard.
2. **EXPLAIN ANALYZE** — Query plan from the last differential refresh
   execution. Highlights sequential scans on large tables (potential
   missing index). Color-codes by cost.
3. **DVM Operator Tree** — The abstract operator tree from `explain_st()`,
   showing how the defining query was decomposed into differential operators.
   Useful for understanding *why* the delta SQL looks the way it does.
4. **Dedup Stats** — From `pgtrickle.dedup_stats()`. Shows deduplication
   ratio for each source buffer — high dedup ratios indicate write-heavy
   sources where change coalescence is working well.

**Actions:**
- `y` — Copy displayed SQL to clipboard.
- `E` — Run EXPLAIN ANALYZE interactively on the delta SQL (confirmation
  dialog, since it actually executes the query).

**Data sources:** `pgtrickle.explain_delta()`, `pgtrickle.explain_st()`,
`pgtrickle.dedup_stats()`.

---

### F17 — Batch Operations

Multi-select stream tables for bulk actions. Available in Dashboard (F1),
Diagnostics (F5), and Fuse (F14) views.

**Activation:** `Space` toggles selection on the current row. `Ctrl+A`
selects all visible (filtered) rows. `Ctrl+D` deselects all.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — dashboard                          mydb@localhost:5432  ⏱ 2s ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  Stream Tables (12 total, 3 selected)                    Filter: ____      ║
║                                                                            ║
║  ☑ order_totals     public  ACTIVE  DIFFERENTIAL   hot   ✓ no   4s ago     ║
║  ☑ big_customers    public  ACTIVE  DIFFERENTIAL   hot   ✓ no   4s ago     ║
║  ☐ daily_revenue    public  ERROR   FULL           warm  ✗ yes  1h ago     ║
║  ☑ product_stats    sales   ACTIVE  AUTO           hot   ✓ no   12s ago    ║
║  ☐ region_summary   public  ACTIVE  DIFFERENTIAL   cold  ✓ no   2m ago     ║
║  ...                                                                       ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Space Select  Ctrl+A All  r Refresh Selected  t Tier  Esc Clear  ? Help   ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Batch actions on selected STs:**
- `r` — Refresh all selected stream tables.
- `t` — Set tier on all selected (opens tier picker: hot/warm/cold/frozen).
- `m` — Set refresh mode on all selected.
- `s` — Set schedule interval on all selected.
- `p` — Pause all selected (set status = PAUSED).
- `R` — Resume all selected.
- `e` — Export DDL for all selected (to file).
- `A` (in Fuse view) — Reset fuses on all selected blown STs.
- `D` — Drop all selected (double confirmation: "Type 'yes' to confirm").

**Selection indicators:** Checkbox column (☑/☐) appears when any row is
selected. Selected count shown in the status summary bar.

**Safety:** Destructive batch operations (drop, fuse reset with skip_changes)
require explicit typed confirmation, not just `y/n`.

---

### F18 — System Health Overview

A single-screen "at a glance" system health check combining all critical
metrics. Shown at startup or opened with `H` from the dashboard or
`pgtrickle health`.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — system health                      mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  Overall: HEALTHY ●                                 Version: 0.14.0        ║
║                                                                            ║
║  ┌─ Checks ─────────────────────────────────────────────────────────────┐  ║
║  │                                                                       │  ║
║  │  ● Extension loaded          pg_trickle in shared_preload_libraries  │  ║
║  │  ● Scheduler running         Last tick 0.3s ago (interval 1000ms)    │  ║
║  │  ● Extension version match   .so = 0.14.0, SQL = 0.14.0             │  ║
║  │  ● WAL level                 wal_level = logical ✓                   │  ║
║  │  ● Stream tables healthy     11/12 ACTIVE, 1 ERROR                   │  ║
║  │  ✗ Error tables              daily_revenue in ERROR state             │  ║
║  │  ● CDC triggers present      24/24 triggers installed                 │  ║
║  │  ● No stale data             0 tables beyond 2× schedule              │  ║
║  │  ⚠ Buffer growth             events buffer at 4.2 MB (> 1 MB)        │  ║
║  │  ● WAL slot lag              0 MB retained (< 100 MB threshold)       │  ║
║  │  ● Fuses                     11/12 armed, 1 blown                     │  ║
║  │  ● Disk usage                Stream tables: 2.4 GB total              │  ║
║  │                                                                       │  ║
║  └───────────────────────────────────────────────────────────────────────┘  ║
║                                                                            ║
║  ┌─ Quick Stats ────────────────────────────────────────────────────────┐  ║
║  │ Total STs: 12   Refreshes today: 8,421   Errors today: 3             │  ║
║  │ Avg latency: 42ms   P95: 128ms   Diff ratio: 99.2%                  │  ║
║  └───────────────────────────────────────────────────────────────────────┘  ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Enter Jump to issue  r Refresh  Esc Back  ? Help                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Traffic light checks:** Green ●, yellow ⚠, red ✗ for each health
  dimension. Modeled after `pgtrickle doctor` pre-flight.
- **Actionable navigation:** `Enter` on a warning/error row jumps to the
  relevant view (e.g., error table → detail view, buffer growth → CDC view,
  blown fuse → fuse panel).
- **Quick stats:** Aggregate performance metrics for the entire system.
- **Pre-flight mode:** `pgtrickle health` as a one-shot CLI command outputs
  the same checks in text/JSON format — useful for CI, monitoring probes,
  and deployment pipelines.
- **Exit code semantics (CLI mode):** 0 = all green, 1 = warnings present,
  2 = errors present. Integrates with alerting tools and deployment gates.

**Data sources:** `pgtrickle.health_check()`, `pgtrickle.pgt_status()`,
`pgtrickle.fuse_status()`, `pgtrickle.change_buffer_sizes()`,
`pgtrickle.slot_health()`, `pgtrickle.version()`, `pg_settings`.

---

### F19 — Watch Mode (Lite Dashboard)

A simplified non-interactive auto-refreshing display for terminals that
don't support full TUI (e.g., dumb terminals, CI log output, tmux status
bars). Launched with `pgtrickle watch`.

```bash
$ pgtrickle watch --interval 5

pg_trickle status — mydb@localhost:5432 — 2026-04-02 09:42:18 — 12 tables
┌──────────────┬────────┬──────────────┬──────┬───────┬──────────┐
│ NAME         │ STATUS │ MODE         │ TIER │ STALE │ LAST     │
├──────────────┼────────┼──────────────┼──────┼───────┼──────────┤
│ order_totals │ ACTIVE │ DIFFERENTIAL │ hot  │ no    │ 4s ago   │
│ big_customers│ ACTIVE │ DIFFERENTIAL │ hot  │ no    │ 4s ago   │
│ daily_revenue│ ERROR  │ FULL         │ warm │ yes   │ 1h ago   │
│ ...          │        │              │      │       │          │
└──────────────┴────────┴──────────────┴──────┴───────┴──────────┘
Refreshing every 5s... (Ctrl+C to exit)
```

**Features:**
- **No alternate screen:** Outputs to stdout without entering ratatui's
  alternate screen mode. Works in CI logs, piped output, and limited
  terminals.
- **ANSI colors:** Uses standard ANSI colors for status (disable with
  `--no-color` or `NO_COLOR=1`).
- **Configurable interval:** `--interval <seconds>` (default: 5s).
- **Filter:** `--filter <pattern>` to show only matching STs.
- **Continuous mode:** Clears and redraws the table on each tick.
  With `--append` flag, appends new output without clearing (for log files).
- **Compact format:** `--compact` shows one line per ST with abbreviated
  columns for narrow terminals.

---

### F20 — DAG Health & Impact Analysis

The single most important view for operators managing complex DAGs. Shows
every active problem grouped by the DAG chain it affects, with blast-radius
analysis and one-click remediation. Opened with `i` from the dashboard or
`pgtrickle issues`.

The key insight: a single ERROR table in the middle of a chain makes every
downstream table **effectively stale** even if those downstream STs report
`ACTIVE` status individually. This view makes those invisible cascading
problems visible.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — DAG issues                         mydb@localhost:5432       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  3 issues across 2 DAG chains            Affected: 5/12 stream tables      ║
║                                                                            ║
║  ┌─ Issue #1 — Broken Chain ─────────────────────────────── severity: ✗ ─┐ ║
║  │                                                                       │ ║
║  │  orders ──▸ daily_revenue ✗ ──▸ executive_dashboard ⚠                │ ║
║  │                                  └──▸ monthly_rollup ⚠               │ ║
║  │                                                                       │ ║
║  │  Root cause: daily_revenue — ERROR since 09:37                        │ ║
║  │    function max(jsonb) does not exist                                  │ ║
║  │  Blast radius: 3 tables (daily_revenue + 2 downstream)                │ ║
║  │  Stale since: 1h 5m (executive_dashboard, monthly_rollup)             │ ║
║  │                                                                       │ ║
║  │  Actions: [r] Refresh  [a] Alter query  [R] Resume  [x] Explain      │ ║
║  └───────────────────────────────────────────────────────────────────────┘ ║
║                                                                            ║
║  ┌─ Issue #2 — Buffer Growth Warning ──────────────────── severity: ⚠ ─┐  ║
║  │                                                                       │  ║
║  │  events ──▸ user_activity ──▸ daily_metrics                           │  ║
║  │  ▲ buffer: 4.2 MB (1,247 rows)                                       │  ║
║  │                                                                       │  ║
║  │  The events buffer is growing faster than it is being consumed.        │  ║
║  │  Possible causes: slow DIFFERENTIAL refresh, gated source, cold tier. │  ║
║  │  Actions: [Enter] Detail  [r] Refresh  [c] CDC health                 │  ║
║  └───────────────────────────────────────────────────────────────────────┘  ║
║                                                                            ║
║  ┌─ Issue #3 — Diamond Inconsistency Window ───────────── severity: ⚠ ─┐  ║
║  │                                                                       │  ║
║  │  Diamond group: group_1 (order_totals, big_customers)                 │  ║
║  │  Last refresh gap: 340ms (order_totals 09:42:18, big_customers        │  ║
║  │  09:42:17.660) — below 1s threshold but increasing.                   │  ║
║  │  Trend: 120ms → 210ms → 340ms (last 3 cycles) ▁▃▅                    │  ║
║  │  Actions: [Enter] Graph view  [d] Diamond group detail                │  ║
║  └───────────────────────────────────────────────────────────────────────┘  ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Navigate issues  Enter Action  Esc Back  ? Help                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Issue categories detected automatically:**

| Category | Severity | Detection logic |
|----------|----------|-----------------|
| Broken chain | ✗ Error | Any ST in ERROR/SUSPENDED status; traces all downstream dependents |
| Cascade staleness | ✗ Error | ST is ACTIVE but all upstream sources include an ERROR ST |
| Blown fuse | ✗ Error | Fuse state = BLOWN; shows error message and reset options |
| Stale data | ⚠ Warning | Data age > 2× schedule interval; checks both explicit staleness and cascade staleness |
| Buffer growth | ⚠ Warning | Change buffer > configurable threshold (default 1 MB or 10K rows) |
| Diamond inconsistency | ⚠ Warning | Refresh time gap between diamond group members increasing or > threshold |
| Scheduler stall | ⚠ Warning | Last scheduler tick > 5× interval ago |
| CDC gap | ⚠ Warning | Missing triggers, WAL slot lag above threshold |
| Orphaned ST | ● Info | Stream table with no downstream dependents and no recent reads (cold candidate) |
| Mode mismatch | ● Info | Current refresh mode ≠ recommended mode (from DIAG-1) with high confidence |
| Cycle detected | ● Info | ST participates in a strongly-connected component |

**Features:**
- **Inline mini-graph:** Each issue card shows a small DAG fragment
  illustrating the affected chain. Nodes use status colors. Broken edges
  shown with `✗` markers. Downstream tables affected by cascade staleness
  shown with `⚠`.
- **Blast radius count:** For error/staleness issues, shows the total
  number of tables affected (direct + transitive downstream).
- **Root cause tracing:** For cascade staleness, traces back to the
  original root cause (the ERROR table at the top of the chain).
- **Trend indicator:** For growing problems (buffer size, inconsistency
  windows), shows a sparkline trend so operators can see if it's getting
  worse.
- **Inline actions:** Each issue card has context-specific action keys
  (refresh, alter, resume, explain, navigate to detail/graph/CDC).
- **Priority ordering:** Issues sorted by severity (errors first), then
  by blast radius (more affected tables first), then by age.
- **Auto-refresh:** Issues list updates in real time via polling + NOTIFY.
  New issues get a brief highlight animation.
- **Badge count:** The dashboard footer and header bar show an issue badge
  (`⚠ 3`) that's visible from any view — click or press `i` to jump here.

**Data sources:** `pgtrickle.pgt_status()`, `pgtrickle.dependency_tree()`,
`pgtrickle.diamond_groups()`, `pgtrickle.fuse_status()`,
`pgtrickle.change_buffer_sizes()`, `pgtrickle.health_check()`,
`pgtrickle.recommend_refresh_mode()`, `pgtrickle.pgt_scc_status()`.

---

### F21 — Cascade Staleness Tracker

Cross-cutting feature that adds cascade-awareness to the dashboard, detail
view, graph view, and health overview. This is not a standalone view but an
intelligence layer that enhances existing views with DAG propagation logic.

**The problem it solves:** Today, `pgt_status()` reports per-table staleness
based on each table's own schedule. But a table can be "on time" locally
while its data is meaningless because an upstream table is broken. The
cascade staleness tracker computes **effective staleness** by traversing the
DAG from each leaf back to its sources.

**Effective staleness rules:**
1. A table's effective staleness is the **maximum** of its own staleness
   and the effective staleness of all its upstream dependencies.
2. If any upstream table is in ERROR/SUSPENDED status, all downstream
   tables are **effectively stale** regardless of their own refresh timing.
3. If an upstream source is gated, downstream tables are marked as
   **gated-stale** (not an error, but data is frozen).

**Dashboard integration (F1):**
- New column: `EFF. STALE` — shows effective staleness considering upstream.
  Value is `✓ ok`, `⚠ 1h` (cascade stale), or `✗ err` (upstream broken).
- Rows with cascade staleness get a distinctive background tint (different
  from direct staleness).
- Status summary bar shows: "11 active, 1 error, **2 cascade-stale**".

**Detail view integration (F2):**
- New section: **Upstream Health** — shows a mini dependency chain with
  status indicators. If any upstream is unhealthy, shows the root cause
  and how long data has been cascade-stale.

```
  ┌─ Upstream Health ─────────────────────────────────────────────────────┐
  │ orders ● → daily_revenue ✗ → executive_dashboard (this table)        │
  │                                                                       │
  │ ⚠ Cascade stale since 09:37 (1h 5m) — root cause: daily_revenue     │
  │   ERROR: function max(jsonb) does not exist                           │
  └───────────────────────────────────────────────────────────────────────┘
```

**Graph view integration (F3):**
- Edges from an ERROR node to its downstream dependents are rendered in
  red dashed style (`- -▸` instead of `──▸`).
- Cascade-stale nodes get a yellow border even if their per-table status
  is ACTIVE.
- New overlay mode (`S` key): **Staleness heatmap** — colors each node by
  effective staleness age (green → yellow → orange → red gradient).

**Health overview integration (F18):**
- New check: "● Cascade health — 0 tables with cascade staleness" or
  "✗ Cascade health — 3 tables effectively stale due to upstream errors".
- `Enter` on the cascade health check jumps to the DAG Issues view (F20)
  filtered to staleness issues.

**Computation:** The cascade staleness computation runs client-side in the
TUI state store after each poll cycle. It uses the dependency tree
(already polled for F3) and per-table status (already polled for F1) to
compute effective staleness without additional database queries. The
computation is O(V+E) on the DAG — trivial for typical deployments
(< 1000 STs).

---

## Dashboard Improvements

These detail the dashboard capabilities listed in F1 (core spec):

### Pinned / Bookmarked Stream Tables

- `b` bookmarks the selected ST. Bookmarked STs sort to the top of the
  dashboard with a `★` indicator.
- Bookmarks persist in `~/.config/pgtrickle/config.toml` per database.
- Useful when managing 50+ stream tables and only a handful are critical.

### Additional Dashboard Columns

Available columns (toggle visibility with `v` → column picker):

| Column | Description |
|--------|-------------|
| Buffer | Pending change buffer row count |
| Size | Target stream table disk size (`pg_relation_size`) |
| Rows | Approximate row count (`pg_stat_user_tables.n_live_tup`) |
| Refresh# | Total refresh count |
| Err# | Total error count |
| Avg ms | Average refresh duration |
| P95 ms | P95 refresh duration |
| Sources | Number of source tables |
| Duration | Sparkline of last 20 refresh durations |
| CDC | CDC mode (trigger/wal/auto) |
| Fuse | Fuse state (armed/blown) |
| Schedule | Refresh interval |

Default visible: Name, Schema, Status, Mode, Tier, Stale, Last Refresh.
Column selection persists in config file.

### Grouping

- `G` cycles grouping mode: none → by schema → by tier → by status → by
  refresh mode → none.
- Groups are collapsible. Collapsed groups show a summary row with count
  and aggregate status.

### Row Preview Pane

When terminal height ≥ 40 rows, a preview pane appears below the table
showing a mini detail view of the selected ST without needing to press Enter.
Shows: status, mode, last refresh time, last error (if any), and a 3-point
sparkline.

---

## Detail View Improvements

### Table Size & Row Metrics

Add to the Properties pane:
- **Table size:** `pg_total_relation_size()` formatted (e.g., "2.4 GB").
- **Row count:** `n_live_tup` from `pg_stat_user_tables`.
- **Source sizes:** Each source table's size and row count.
- **Change ratio:** Current buffer rows / target table rows (as percentage).

### Refresh Group Membership

If the ST belongs to a refresh group, show:
- Group name, other members, and whether the last refresh was atomic.

### Cycle / SCC Indicator

If the ST participates in a strongly-connected component (circular DAG),
show the SCC group from `pgtrickle.pgt_scc_status()`.

---

## Graph View Improvements

These detail the graph view capabilities listed in F3 (core spec) and F20
(DAG Health):

### Path Highlighting

When a node is selected, highlight:
- **Upstream path:** All ancestor nodes and edges in blue.
- **Downstream path:** All descendant nodes and edges in orange.
- **Affected by change:** If a source table is selected, highlight all
  STs that would be refreshed (the "blast radius").

### Schema Filtering

`/` opens a filter that hides nodes not matching the pattern. Useful for
large DAGs where you only care about one schema's STs.

### Refresh Animation

During a refresh cycle, animate edges with a flowing marker (`───▸` becomes
`━━━▸`) for STs currently being refreshed. Provides a visual "pulse" of
the system's activity.

---

## Refresh Log Improvements

### Duration Distribution

At the top of the log view, show a small histogram of refresh durations
for the last 100 events:

```
Duration distribution (last 100):
<10ms ██████████████████████ 45
<50ms ██████████████████ 38
<100ms ████████ 12
<500ms ███ 5
≥500ms  0
```

### Diff Viewer for Errors

When viewing a failed refresh entry, `d` shows a diff-like view comparing
the error against the last successful refresh of the same ST — useful for
diagnosing intermittent failures.

---

## Command Palette Improvements

### Refresh Group Commands

Add to the command palette:
- `refresh-group <name>` — Refresh an entire group atomically.
- `create-refresh-group <name> <st1> <st2> ...` — Create a refresh group.
- `drop-refresh-group <name>` — Drop a refresh group.

### Maintenance Commands

- `rebuild-triggers` — Run `pgtrickle.rebuild_cdc_triggers()`.
- `convert-unlogged` — Run `pgtrickle.convert_buffers_to_unlogged()`.
- `restore` — Run `pgtrickle.restore_stream_tables()` after pg_restore.

### Diagnostic Commands

- `explain <name>` — Open delta SQL inspector (F16) for named ST.
- `auto-threshold <name>` — Show AUTO mode change ratio threshold.

---

## CLI Mode Improvements

### Additional One-Shot Commands

```bash
# System health check (CI-friendly exit codes)
pgtrickle health
pgtrickle health --format json

# Worker pool status
pgtrickle workers
pgtrickle workers --format json

# Fuse status and management
pgtrickle fuse
pgtrickle fuse reset daily_revenue --strategy reinitialize

# Watermark management
pgtrickle watermarks
pgtrickle watermarks advance orders "2026-04-02 09:45:00"

# Source gating
pgtrickle gate events --reason "maintenance window"
pgtrickle ungate events

# Delta SQL inspection
pgtrickle explain order_totals

# Watch mode (non-interactive continuous output)
pgtrickle watch
pgtrickle watch --interval 10 --filter "schema=public"
pgtrickle watch --compact --no-color >> /var/log/pgtrickle.log

# Refresh groups
pgtrickle groups
pgtrickle groups create my_group order_totals big_customers
pgtrickle groups refresh my_group

# Batch operations via stdin (pipe-friendly)
echo "order_totals\nbig_customers" | pgtrickle refresh --stdin
pgtrickle list --format json | jq -r '.[].name' | pgtrickle refresh --stdin
```

### Shell Completions

Generate shell completion scripts for bash, zsh, fish, and PowerShell:

```bash
pgtrickle completions bash > /etc/bash_completion.d/pgtrickle
pgtrickle completions zsh > ~/.zfunc/_pgtrickle
pgtrickle completions fish > ~/.config/fish/completions/pgtrickle.fish
```

Uses `clap_complete` crate. Completes subcommands, flags, and **stream
table names** (via a cached list that refreshes every 60s in the background).

---

## Adaptive Polling

The TUI uses adaptive polling rates to balance responsiveness with database
load:

| Condition | Poll interval |
|-----------|--------------|
| Dashboard active, changes detected recently | 2s |
| Dashboard active, idle (no changes for 30s) | 5s |
| Non-dashboard view active | 10s (dashboard data) |
| Diagnostics/heavy queries | On-demand only (Ctrl+R) |
| TUI in background (terminal not focused) | 30s |
| Reconnecting after connection loss | Exponential backoff |

The poll interval is shown in the header bar (e.g., `⏱ 2s`) and can be
overridden with `--poll-interval <ms>` or in the config file.

---

## Navigation & Keybindings

### Global Keys (work in every view)

| Key | Action |
|-----|--------|
| `q` / `Ctrl+C` | Quit |
| `?` | Toggle help overlay |
| `:` | Open command palette |
| `Esc` | Go back / close dialog / clear filter |
| `1` | Dashboard |
| `2` | Detail (last viewed, or first ST) |
| `3` | Dependency graph |
| `4` | Refresh log |
| `5` | Diagnostics |
| `6` | CDC health |
| `7` | Configuration |
| `8` | Alerts |
| `9` | Scheduler & workers |
| `0` | System health |
| `i` | DAG issues (F20) |
| `Ctrl+R` | Force data refresh |

### Table Navigation (Dashboard, Diagnostics, CDC, Config, Fuse)

| Key | Action |
|-----|--------|
| `j` / `↓` | Move selection down |
| `k` / `↑` | Move selection up |
| `g` / `Home` | Jump to first row |
| `G` / `End` | Jump to last row |
| `Enter` | Open detail/expand |
| `/` | Open filter bar |
| `n` / `s` / etc. | Sort by column |
| `Tab` | Next pane (in detail view) |
| `Space` | Toggle selection (batch mode) |
| `Ctrl+A` | Select all visible |
| `Ctrl+D` | Deselect all |
| `v` | Column visibility picker |
| `b` | Toggle bookmark on selected row |

### Graph View Keys

| Key | Action |
|-----|--------|
| `S` | Toggle staleness heatmap overlay |
| `B` | Toggle blast radius for selected node |
| `h` / `←` | Move to parent node |
| `l` / `→` | Move to child node |
| `x` | Expand/collapse subtree |
| `/` | Schema filter |

### Dashboard-Specific Keys

| Key | Action |
|-----|--------|
| `I` | Toggle issues sidebar |
| `M` | Toggle DAG mini-map |

### Mouse Support (Optional)

- Click to select rows.
- Scroll wheel to navigate tables.
- Click footer buttons.
- Disabled by default; enabled via `--mouse` flag or config file.

---

## UI Layout

### Responsive Layout

The TUI adapts to terminal dimensions:

| Width | Layout |
|-------|--------|
| < 80 cols | Compact: single-column, abbreviated names, no sparklines |
| 80–119 cols | Standard: full table, no sparklines |
| 120+ cols | Wide: full table + sparklines + side panels |

| Height | Layout |
|--------|--------|
| < 24 rows | Minimal: table only, no footer/header status bars |
| 24–39 rows | Standard: header + table + footer |
| 40+ rows | Tall: header + table + inline preview panel + footer |

### Header Bar

Always visible. Shows:
- Application name and version.
- Current view name.
- Connected database (`dbname@host:port`).
- Poll interval and last poll timestamp.
- Connection status indicator (●/○).

### Footer Bar

Always visible. Shows context-sensitive keybinding hints for the active view.

### Toast Notifications

Transient overlays (3-second duration) for:
- Refresh triggered/completed.
- Alert received (critical only).
- Command executed successfully.
- Errors.

---

## Theming & Aesthetics

### Built-in Themes

| Theme | Description |
|-------|-------------|
| `default` | Dark theme with blue accents. Professional, high contrast. |
| `light` | Light background. For terminals with light themes. |
| `minimal` | Monochrome. No colors, borders only. For accessibility. |
| `catppuccin` | Popular pastel palette. Mocha variant. |
| `nord` | Cool-toned Nord color scheme. |
| `dracula` | Dark theme with Dracula palette. |

### Color Semantics

Consistent across all themes:

| Element | Color |
|---------|-------|
| ACTIVE status | Green |
| ERROR status | Red |
| SUSPENDED status | Yellow |
| PAUSED / FROZEN | Dim / Grey |
| Selected row | Inverted / highlighted |
| Modified value | Cyan |
| Warning indicators | Yellow |
| Borders | Theme-specific subtle color |
| Headers | Bold |

### Custom Themes

Users can define custom themes in `~/.config/pgtrickle/config.toml`:

```toml
[theme]
name = "custom"

[theme.colors]
active = "green"
error = "red"
suspended = "yellow"
paused = "gray"
selected_bg = "#3b4252"
selected_fg = "#eceff4"
border = "#4c566a"
header = "bold white"
```

### Box Drawing

Uses Unicode box-drawing characters for borders (single-line by default:
`─│┌┐└┘├┤┬┴┼`). Falls back to ASCII (`-|+`) when `--ascii` flag is set
or `TERM=dumb` is detected.

---

## Connection Management

### Connection String

Resolved in order:
1. `--url` / `-U` CLI argument (highest priority).
2. `PGTRICKLE_URL` environment variable.
3. Standard libpq variables: `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`,
   `PGPASSWORD`, `PGPASSFILE`.
4. Default: `postgres://localhost:5432/postgres`.

### Connection Pooling

The TUI maintains two persistent connections:
1. **Query connection** — Used for polling and one-shot commands.
2. **LISTEN connection** — Dedicated to `LISTEN pg_trickle_alert`.

### Connection Recovery

- Auto-reconnect on transient connection loss (TCP timeout, server restart).
- Exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s max.
- Connection status indicator in the header bar (green ● connected,
  red ○ disconnected, yellow ◐ reconnecting).
- All views show "Reconnecting…" overlay when disconnected.

### Multi-Database

Future: `connect <url>` command in the command palette to switch databases
without restarting. Not in initial scope.

---

## Implementation Phases

### Phase T1 — Skeleton & CLI Mode (Day 1)

**Goal:** Workspace setup, binary target, clap CLI, connection, one-shot
`list` and `status` commands.

| Item | Description | Effort |
|------|-------------|--------|
| T1a | Create `pgtrickle-tui/` workspace member with `Cargo.toml`, `src/main.rs` | 1h |
| T1b | `clap` CLI with subcommands: `list`, `status`, `refresh`, `create`, `drop`, `alter`, `export`, `diag`, `cdc`, `graph`, `config`, `alerts` | 2h |
| T1c | `tokio-postgres` connection setup with libpq env var resolution | 2h |
| T1d | One-shot `list` command (table output via `comfy-table`, JSON via `serde_json`) | 2h |
| T1e | One-shot `status <name>` command | 1h |
| T1f | One-shot `refresh`, `create`, `drop`, `alter`, `export` commands | 3h |

**Exit:** ✅ Done — `pgtrickle list` and `pgtrickle status my_st` work against a live database. `--format json` and `--format csv` produce valid output. 18 one-shot subcommands implemented. Shell completions for bash/zsh/fish/PowerShell. CI builds the binary.

### Phase T2 — TUI Core & Dashboard (Day 2)

**Goal:** ratatui app loop, state store, polling, dashboard view with
sortable/filterable stream table list.

| Item | Description | Effort |
|------|-------------|--------|
| T2a | ratatui + crossterm app loop with event handling, adaptive frame rate | 2h |
| T2b | State store (in-memory model updated by async poller), including DAG topology and cascade staleness computation (F21) | 3h |
| T2c | Dashboard view (F1): stream table list with status colors, sorting, filtering, EFF column, status ribbon, issues sidebar | 5h |
| T2d | Header bar and footer bar with keybinding hints | 1h |
| T2e | LISTEN/NOTIFY background task for `pg_trickle_alert` | 2h |

**Exit:** ✅ Done — Dashboard shows live-updating stream table list with EFF column, errors-first sort, filter, cascade-stale count in status ribbon, sparklines for selected ST, and issues sidebar. Alerts arrive in real-time via LISTEN/NOTIFY with JSON payload parsing. Force poll via Ctrl+R.

### Phase T3 — Detail View & Drill-Down (Day 3)

**Goal:** Stream table detail view with all panes, quick actions, and
navigation between views.

| Item | Description | Effort |
|------|-------------|--------|
| T3a | Detail view (F2): properties, refresh stats, defining query, upstream health (F21), recent refreshes, blast radius | 5h |
| T3b | Refresh log view (F4): scrollable, filterable, auto-tailing | 3h |
| T3c | View navigation (number keys, Esc to go back, breadcrumb state) | 2h |
| T3d | Quick actions: refresh, alter tier, export DDL from detail view | 2h |

**Exit:** ✅ Done — Detail view has 4 panels: properties (with cascade staleness indicator), refresh stats (with efficiency data), recent refreshes, and error details + upstream health. Navigate between dashboard and detail. Refresh log scrolls. Deferred: defining query pane, blast radius in detail, refresh log filter.

### Phase T4 — Graph, Diagnostics & CDC (Day 4)

**Goal:** Dependency graph visualization, diagnostics panel with signal
breakdown, CDC health view.

| Item | Description | Effort |
|------|-------------|--------|
| T4a | Dependency graph view (F3): topological layout, node coloring, navigation, path highlighting, broken edge rendering, staleness heatmap overlay | 5h |
| T4b | DAG Health & Impact Analysis view (F20): issue categories, blast radius, mini-graphs | 4h |
| T4c | Diagnostics panel (F5): recommendation table, signal bar charts | 3h |
| T4d | CDC health view (F6): buffer sizes, trigger inventory, overall health | 2h |
| T4e | Sparklines (F11) in dashboard and detail view | 2h |

**Exit:** ✅ Partial — ASCII dependency graph renders with status coloring and node navigation. DAG Health / Issues view (F20) surfaces issues by category with blast radius. Diagnostics show recommendation table. CDC health shows buffer sizes and trigger inventory. Deferred: graph path highlighting, staleness heatmap overlay, signal breakdown bar charts.

### Phase T5 — Workers, Fuse, Watermarks & Delta Inspector (Day 5)

**Goal:** Ship the operational views that surface the remaining SQL API:
worker pool, fuse management, watermark/gating, and delta SQL inspection.

| Item | Description | Effort |
|------|-------------|--------|
| T5a | Scheduler & worker pool view (F13): scheduler status, worker table, job queue | 3h |
| T5b | Fuse & circuit breaker panel (F14): fuse table, inline reset (A/R/S) | 2h |
| T5c | Watermark & source gating view (F15): watermark groups, per-source watermarks, gate toggle | 3h |
| T5d | Delta SQL inspector (F16): delta SQL, EXPLAIN ANALYZE, DVM operator tree, dedup stats | 3h |

**Exit:** ✅ Partial — Workers, fuse, watermark views all functional with live data. Fuse detail panel shows reset instructions. Delta SQL inspector links to `pgtrickle explain`. Deferred: inline fuse reset execution from TUI, gate/ungate from TUI, inline EXPLAIN ANALYZE and DVM operator tree.

### Phase T6 — Batch Ops, Health, Command Palette, Polish (Day 6–7)

**Goal:** Batch operations, system health overview, command palette, theming,
help system, responsive layout, testing, watch mode.

| Item | Description | Effort |
|------|-------------|--------|
| T6a | Batch operations (F17): multi-select, batch refresh/tier/mode/pause/resume | 3h |
| T6b | System health overview (F18): traffic light checks, quick stats, actionable nav | 2h |
| T6c | Watch mode (F19): non-interactive continuous output, `--compact`, `--no-color` | 2h |
| T6d | Command palette (F9): fuzzy search, ST autocomplete, recent commands, maintenance cmds | 3h |
| T6e | Configuration editor (F7): browse GUCs, inline docs, edit + apply, grouping by category | 3h |
| T6f | Alert feed (F8): real-time NOTIFY rendering, severity icons, toast popups | 2h |

**Exit:** ✅ Partial — System health overview (F18) shows HEALTHY/DEGRADED/WARNINGS summary with check table. Watch mode (F19) outputs to stdout with `--compact`, `--no-color`, `--append`, `--filter` flags. Alert feed (F8) receives NOTIFY with severity icons. Deferred: batch operations (F17), command palette (F9), GUC inline editing.

### Phase T7 — UX Polish, Theming, Testing & Documentation (Day 8)

**Goal:** Polish all views, add theming, shell completions, adaptive polling,
responsive layout, help system, integration tests, documentation.

| Item | Description | Effort |
|------|-------------|--------|
| T7a | Help system (F12): context-sensitive overlay for all 21 features | 2h |
| T7b | Theming: 6 built-in themes, custom theme config file, `--theme` flag | 2h |
| T7c | Responsive layout: width/height adaptation, compact mode, preview pane | 2h |
| T7d | Adaptive polling: rate adjustment based on activity and view | 1h |
| T7e | Shell completions: bash, zsh, fish, PowerShell via `clap_complete` | 1h |
| T7f | Dashboard enhancements: bookmarks, column picker, grouping, row preview | 3h |
| T7g | Connection recovery: auto-reconnect, status indicator, overlay | 1h |
| T7h | Mouse support (optional, `--mouse` flag) | 1h |
| T7i | Integration tests: snapshot tests for view rendering, CI binary build | 2h |
| T7j | Documentation: `docs/TUI.md` user guide, README update, GETTING_STARTED link | 2h |

**Exit:** ✅ Partial — Context-sensitive help overlay (F12) works in every view showing per-view tips. Shell completions installable. CI builds binary. Connection recovery with auto-reconnect. Cascade staleness (F21) computed from DAG traversal; upstream health pane in detail view. Deferred: multiple themes, adaptive polling, dashboard bookmarks/column picker/grouping, toast popups.

---

## Exit Criteria

- [x] `pgtrickle` binary builds as a workspace member via `cargo build -p pgtrickle-tui`
- [x] One-shot CLI mode: `list`, `status`, `refresh`, `create`, `drop`, `alter`, `export`, `diag`, `cdc`, `graph`, `config`, `health`, `workers`, `fuse`, `watermarks`, `watch`, `explain`, `completions` subcommands all functional (`groups`, `gate`, `ungate` deferred)
- [x] `--format json` and `--format csv` produce valid output for all one-shot commands
- [x] Shell completions for bash, zsh, fish, PowerShell
- [x] Interactive TUI launches when no subcommand is given
- [~] Dashboard (F1): live-updating, filterable, EFF column, errors-first sort, cascade-stale count, sparklines, issues sidebar ✅ — bookmarks, column picker, grouping, row preview pane deferred
- [~] Detail view (F2): properties, refresh stats, upstream health, recent refreshes, error details ✅ — defining query pane, table size/row count, SCC indicators deferred
- [~] Dependency graph (F3): ASCII DAG layout, status coloring, node navigation ✅ — path highlighting, broken edge rendering, staleness heatmap overlay deferred
- [~] Refresh log (F4): scrollable ✅ — filter, auto-tailing, NOTIFY events, histogram deferred
- [~] Diagnostics (F5): recommendation table with confidence levels ✅ — signal breakdown bar charts, apply action deferred
- [x] CDC health (F6): buffer sizes, trigger inventory, overall health indicator
- [~] Configuration (F7): browse GUCs grouped by category ✅ — inline edit and apply via ALTER SYSTEM deferred
- [~] Alert feed (F8): real-time NOTIFY alerts with severity icons ✅ — toast popups for critical events deferred
- [ ] Command palette (F9): fuzzy-search command execution with ST autocomplete — not implemented
- [x] Sparklines (F11): refresh duration trends in dashboard (when terminal width allows)
- [x] Help system (F12): context-sensitive help overlay in every view with per-view tips
- [x] Scheduler & workers (F13): scheduler heartbeat, worker pool status, job queue
- [~] Fuse panel (F14): fuse overview, detail panel, reset instructions ✅ — inline TUI reset execution, batch reset deferred
- [~] Watermark & gating (F15): watermark groups and per-source watermarks ✅ — gate/ungate from TUI deferred
- [~] Delta SQL inspector (F16): links to `pgtrickle explain` CLI ✅ — inline EXPLAIN ANALYZE and DVM operator tree deferred
- [ ] Batch operations (F17): multi-select with Space, batch refresh/tier/mode/pause/resume/drop — not implemented
- [x] System health (F18): HEALTHY/DEGRADED/WARNINGS summary, severity-colored check table
- [x] Watch mode (F19): non-interactive continuous output with `--compact`, `--no-color`, `--append`, `--filter`
- [~] DAG Health & Impact Analysis (F20): issue categories (error chains, cascade stale, buffer growth, blown fuses), severity summary, blast radius ranking ✅ — inline mini-graphs deferred
- [~] Cascade Staleness Tracker (F21): DAG traversal, EFF column, upstream health pane in detail view, issue badge in header ✅ — staleness heatmap overlay in graph deferred
- [ ] 6 built-in themes + custom theme support — not implemented (single dark theme only)
- [ ] Adaptive polling adjusts rate based on activity level — not implemented
- [x] Connection recovery with auto-reconnect and status indicator
- [~] Responsive layout adapts to terminal size — basic adaptation ✅; 80-col minimum mode not fully tested
- [x] Documented in `docs/TUI.md` and linked from `docs/GETTING_STARTED.md`
- [x] `just fmt` clean; `just lint` zero warnings
- [x] CI builds the binary on Linux/macOS

---

## Non-Goals

The following are explicitly out of scope for the initial TUI release:

- **SQL REPL / query execution** — Use `psql` for ad-hoc SQL.
- **Stream table creation wizard** — The `create` subcommand is sufficient;
  a multi-step wizard is too complex for the initial release.
- **Multi-database dashboard** — Single connection only. Future: `connect`
  command to switch.
- **Log persistence** — Alert/log history lives only in memory for the TUI
  session. Use `pgtrickle.get_refresh_history()` for persistent history.
- **Remote/cloud integrations** — No direct AWS/GCP/Azure integrations.
- **Web UI** — Terminal only. A web dashboard is a separate project.
- **Plugin system** — Views and commands are hardcoded for now.
