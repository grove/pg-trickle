# PLAN_TUI.md — pg_trickle Terminal User Interface

**Milestone:** v0.14.0 (replaces E3 CLI)
**Status:** ⬜ Not started
**Effort:** ~4–6 days
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
- [UI Layout](#ui-layout)
- [Navigation & Keybindings](#navigation--keybindings)
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

The default view when launching `pgtrickle` with no subcommand. Shows a
real-time overview of all stream tables in a sortable, filterable table.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  pg_trickle — dashboard                          mydb@localhost:5432  ⏱ 2s ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  Stream Tables (12 total, 1 error, 11 active)            Filter: ____      ║
║                                                                            ║
║  NAME             SCHEMA  STATUS  MODE           TIER  STALE  LAST REFRESH ║
║  ──────────────── ─────── ─────── ────────────── ───── ────── ──────────── ║
║▸ order_totals     public  ACTIVE  DIFFERENTIAL   hot   ✓ no   4s ago       ║
║  big_customers    public  ACTIVE  DIFFERENTIAL   hot   ✓ no   4s ago       ║
║  daily_revenue    public  ERROR   FULL           warm  ✗ yes  1h ago       ║
║  product_stats    sales   ACTIVE  AUTO           hot   ✓ no   12s ago      ║
║  region_summary   public  ACTIVE  DIFFERENTIAL   cold  ✓ no   2m ago       ║
║  user_activity    public  ACTIVE  IMMEDIATE      hot   ✓ no   0s ago       ║
║  inv_snapshot     ops     ACTIVE  DIFFERENTIAL   hot   ✓ no   6s ago       ║
║  ...                                                                       ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ▴▾ Navigate  Enter Detail  r Refresh  / Filter  g Graph  d Diag  ? Help   ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Columns:** Name, Schema, Status (color-coded: green=ACTIVE, red=ERROR,
yellow=SUSPENDED, grey=PAUSED), Refresh Mode, Tier, Staleness indicator,
Last Refresh (relative time).

**Features:**
- **Sortable:** Press column letter (n/s/S/m/t/l) to sort by that column.
  Active sort indicated by `▴`/`▾` in header.
- **Filterable:** `/` opens a filter bar. Matches name, schema, status, mode.
  `Esc` clears the filter.
- **Status summary bar:** Total count, error count, active count displayed
  above the table. Updates in real-time.
- **Auto-refresh:** Table refreshes every poll interval. Manual refresh with
  `Ctrl+R`.
- **Color indicators:** Status column uses ANSI colors: green (ACTIVE),
  red (ERROR), yellow (SUSPENDED), dim (PAUSED/FROZEN).
- **Stale highlight:** Rows with stale data get a subtle background tint.
- **Quick actions from dashboard:** `r` triggers refresh on selected ST,
  `Enter` opens detail view, `d` opens diagnostics, `g` opens DAG graph.

**Data source:** `SELECT * FROM pgtrickle.pgt_status()` polled every 2s.

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
4. **Recent Refreshes** — Scrollable list from `pgtrickle.refresh_timeline()`.
5. **Error Details** — Only shown when `status = 'ERROR'`. Displays
   `last_error_message` prominently in red.
6. **Sources** — Source tables with CDC status from `pgtrickle.list_sources()`.

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

ASCII-art visualization of the stream table DAG. Opened with `g` from the
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
║     events ──────▸ user_activity ──▸ daily_metrics                         ║
║                                                                            ║
║     inventory ───▸ inv_snapshot (standalone)                               ║
║                                                                            ║
║  Legend: ─▸ dependency  ● ACTIVE  ✗ ERROR  ◆ diamond group                 ║
║                                                                            ║
║  Selected: order_totals                                                    ║
║  Depth: 2  Sources: 2  Dependents: 1  Diamond: group_1                     ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ←→ Navigate  Enter Detail  h/l Expand/Collapse  f Fit  ? Help             ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Features:**
- **Topological layout:** Nodes arranged left-to-right by dependency depth.
  Source tables on the left, leaf stream tables on the right.
- **Interactive navigation:** Arrow keys move between nodes. Selected node
  is highlighted. `Enter` opens the detail view.
- **Color coding:** Node color matches status (green/red/yellow). Diamond
  group members share a border style (e.g., double-line `║`).
- **Metadata strip:** Shows depth, source count, dependent count, and diamond
  group for the selected node.
- **Zoom/pan:** `+`/`-` to zoom, `f` to fit entire graph in viewport.
  For large DAGs (50+ nodes), nodes collapse by schema with `h`/`l`.
- **Layout engine:** Simple Sugiyama-style layered layout (nodes assigned to
  layers by topological depth, edges routed with minimal crossings). Uses
  data from `pgtrickle.dependency_tree()`.

**Data source:** `pgtrickle.dependency_tree()`, `pgtrickle.diamond_groups()`.

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
| `Ctrl+R` | Force data refresh |

### Table Navigation (Dashboard, Diagnostics, CDC, Config)

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

### Mouse Support (Optional)

- Click to select rows.
- Scroll wheel to navigate tables.
- Click footer buttons.
- Disabled by default; enabled via `--mouse` flag or config file.

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

**Exit:** `pgtrickle list` and `pgtrickle status my_st` work against a live
database. `--format json` produces valid JSON. CI builds the binary.

### Phase T2 — TUI Core & Dashboard (Day 2)

**Goal:** ratatui app loop, state store, polling, dashboard view with
sortable/filterable stream table list.

| Item | Description | Effort |
|------|-------------|--------|
| T2a | ratatui + crossterm app loop with event handling, adaptive frame rate | 2h |
| T2b | State store (in-memory model updated by async poller) | 2h |
| T2c | Dashboard view (F1): stream table list with status colors, sorting, filtering | 4h |
| T2d | Header bar and footer bar with keybinding hints | 1h |
| T2e | LISTEN/NOTIFY background task for `pg_trickle_alert` | 2h |

**Exit:** Dashboard shows live-updating stream table list. Filter and sort
work. Alerts arrive in real-time.

### Phase T3 — Detail View & Drill-Down (Day 3)

**Goal:** Stream table detail view with all panes, quick actions, and
navigation between views.

| Item | Description | Effort |
|------|-------------|--------|
| T3a | Detail view (F2): properties, refresh stats, defining query, recent refreshes | 4h |
| T3b | Refresh log view (F4): scrollable, filterable, auto-tailing | 3h |
| T3c | View navigation (number keys, Esc to go back, breadcrumb state) | 2h |
| T3d | Quick actions: refresh, alter tier, export DDL from detail view | 2h |

**Exit:** Full detail view with all panes. Navigate between dashboard and
detail. Refresh log scrolls and filters.

### Phase T4 — Graph, Diagnostics & CDC (Day 4)

**Goal:** Dependency graph visualization, diagnostics panel with signal
breakdown, CDC health view.

| Item | Description | Effort |
|------|-------------|--------|
| T4a | Dependency graph view (F3): topological layout, node coloring, navigation | 4h |
| T4b | Diagnostics panel (F5): recommendation table, signal bar charts | 3h |
| T4c | CDC health view (F6): buffer sizes, trigger inventory, overall health | 2h |
| T4d | Sparklines (F11) in dashboard and detail view | 2h |

**Exit:** Graph renders the full DAG with interactive navigation. Diagnostics
show signal breakdown with bar charts. CDC health shows buffer sizes.

### Phase T5 — Config, Alerts, Command Palette, Polish (Day 5–6)

**Goal:** Configuration editor, alert feed, command palette, theming,
help system, responsive layout, testing.

| Item | Description | Effort |
|------|-------------|--------|
| T5a | Configuration editor (F7): browse GUCs, inline docs, edit + apply | 3h |
| T5b | Alert feed (F8): real-time NOTIFY rendering, severity icons, toast popups | 2h |
| T5c | Command palette (F9): fuzzy search, ST name autocomplete, recent commands | 3h |
| T5d | Help system (F12): context-sensitive overlay, keybinding reference | 2h |
| T5e | Theming: 6 built-in themes, custom theme config file, `--theme` flag | 2h |
| T5f | Responsive layout: width/height adaptation, compact mode | 2h |
| T5g | Connection recovery: auto-reconnect, status indicator, overlay | 1h |
| T5h | Mouse support (optional, `--mouse` flag) | 1h |
| T5i | Integration tests: snapshot tests for view rendering, CI binary build | 2h |
| T5j | Documentation: `docs/TUI.md` user guide, README update, GETTING_STARTED link | 2h |

**Exit:** All 12 features functional. Help works in every view. Themes
switch cleanly. CI builds and tests the binary.

---

## Exit Criteria

- [ ] `pgtrickle` binary builds as a workspace member via `cargo build -p pgtrickle-tui`
- [ ] One-shot CLI mode: `list`, `status`, `refresh`, `create`, `drop`, `alter`, `export`, `diag`, `cdc`, `graph`, `config` subcommands all functional
- [ ] `--format json` and `--format csv` produce valid output for all one-shot commands
- [ ] Interactive TUI launches when no subcommand is given
- [ ] Dashboard (F1): live-updating sortable, filterable stream table list with status colors
- [ ] Detail view (F2): properties, refresh stats, defining query, recent refreshes, error details
- [ ] Dependency graph (F3): ASCII DAG layout with interactive node navigation
- [ ] Refresh log (F4): scrollable, filterable, auto-tailing with real-time NOTIFY events
- [ ] Diagnostics (F5): recommendation table with signal breakdown bar charts; apply action
- [ ] CDC health (F6): buffer sizes, trigger inventory, overall health indicator
- [ ] Configuration (F7): browse/edit GUCs with inline docs and apply via ALTER SYSTEM
- [ ] Alert feed (F8): real-time NOTIFY alerts with toast popups for critical events
- [ ] Command palette (F9): fuzzy-search command execution with ST autocomplete
- [ ] Sparklines (F11): refresh duration trends in dashboard (when terminal width ≥ 120)
- [ ] Help system (F12): context-sensitive help overlay in every view
- [ ] 6 built-in themes + custom theme support via config file
- [ ] Connection recovery with auto-reconnect and status indicator
- [ ] Responsive layout adapts to terminal size (80-col minimum)
- [ ] Documented in `docs/TUI.md` and linked from `docs/GETTING_STARTED.md`
- [ ] `just fmt` clean; `just lint` zero warnings
- [ ] CI builds the binary on Linux/macOS

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
