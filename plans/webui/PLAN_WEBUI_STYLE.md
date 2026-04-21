# WebUI — Style & Navigation Plan

> **Status:** Analysis & Design (DRAFT — 2026-04-21)
> **Created:** 2026-04-21
> **Category:** Tooling — Web Dashboard UI/UX
> **Related:** [PLAN_WEBUI.md](PLAN_WEBUI.md) ·
> [PLAN_RELAY_CLI.md](../relay/PLAN_RELAY_CLI.md) ·
> [PLAN_TUI.md](../ui/PLAN_TUI.md)

---

## Table of Contents

- [Design Principles](#design-principles)
- [Design System & Component Library](#design-system--component-library)
  - [Core Stack](#core-stack)
  - [Charting](#charting)
  - [Topology Graph](#topology-graph)
  - [Rejected Alternatives](#rejected-alternatives)
- [Colour System](#colour-system)
  - [Theme Modes](#theme-modes)
  - [Semantic Colours](#semantic-colours)
  - [Node Colours (Topology)](#node-colours-topology)
  - [Edge Colours (Topology)](#edge-colours-topology)
- [Typography](#typography)
- [Navigation Structure](#navigation-structure)
  - [Sidebar Layout](#sidebar-layout)
  - [Page Routing](#page-routing)
  - [Breadcrumbs](#breadcrumbs)
- [Screen Layouts](#screen-layouts)
  - [Landing: Topology Graph](#landing-topology-graph)
  - [Pipelines List](#pipelines-list)
  - [Pipeline Detail](#pipeline-detail)
  - [Stream Table Detail](#stream-table-detail)
  - [SLA Budget Dashboard](#sla-budget-dashboard)
  - [Health Scorecard](#health-scorecard)
  - [Refresh Timeline](#refresh-timeline)
  - [Alerts & Activity Feed](#alerts--activity-feed)
  - [SQL Preview Modal](#sql-preview-modal)
- [Responsive Behaviour](#responsive-behaviour)
- [Accessibility](#accessibility)
- [Open Questions](#open-questions)

---

## Design Principles

Five guiding rules for every screen:

1. **Density over whitespace.** SREs on-call want maximum information
   per screen. Card-heavy layouts with generous padding waste the
   viewport. Tables, inline badges, and compact metric rows are
   preferred over hero cards.

2. **Status colours carry meaning.** Green, amber, and red are reserved
   exclusively for health/SLA status. They must never be used for
   decorative purposes, branding, or navigation highlighting. This
   ensures an SRE scanning the screen can immediately locate problems
   by colour alone.

3. **Dark mode is the default.** The primary audience (SREs, data
   engineers, platform engineers) overwhelmingly uses dark-themed
   tools. Light mode is supported but not the design baseline.

4. **Monospace for system values.** Table names, SQL, metric values,
   durations, row counts, and column names are always rendered in a
   monospace font. This visually separates *system data* from *UI
   chrome* and improves scannability in dense tables.

5. **The graph is the hero.** The topology graph is the centrepiece
   feature — it gets full-viewport space on the landing page. Other
   pages support it, not the other way around.

---

## Design System & Component Library

### Core Stack

**shadcn/ui + Tailwind CSS + Radix UI primitives.**

Rationale:

- **shadcn/ui** provides copy-paste components — we own the code, not a
  dependency. Components are unstyled Radix primitives with Tailwind
  classes. Customizable without fighting a design system's opinions.
- **Tailwind CSS** is the standard for Next.js projects. Utility-first
  means no CSS-in-JS runtime, no specificity wars, and dark mode is a
  single `dark:` prefix.
- **Radix UI** handles accessibility (ARIA attributes, keyboard
  navigation, focus management) at the primitive level. Dialogs,
  dropdowns, tooltips, and popovers "just work" for screen readers.

Key shadcn/ui components used:

| Component | Used for |
|-----------|----------|
| `Table` | Stream table list, CDC sources, relay pipelines, alerts |
| `Badge` | Status indicators, refresh mode, CDC mode |
| `Card` | Metric summary tiles (compact, not hero-sized) |
| `Dialog` | SQL preview modal, confirmation dialogs |
| `Sheet` | Side panel for node detail (opens from topology graph) |
| `Tabs` | Stream table detail (overview / history / lineage / operator tree) |
| `Command` | Command palette (Cmd+K) |
| `Tooltip` | Hover explanations on badges, metric values |
| `DropdownMenu` | Context menus on topology nodes, table rows |
| `Separator` | Section dividers in dense layouts |
| `ScrollArea` | Scrollable panels (alerts feed, change flow) |

### Charting

**Tremor + ECharts** — two libraries, each used where it excels:

| Library | Used for | Why |
|---------|----------|-----|
| **Tremor** | Metric cards, sparklines, status badges, small inline charts | Purpose-built for dashboard metric displays. Tailwind-native. Matches the shadcn/ui aesthetic. |
| **ECharts** | Refresh timeline (stacked area + bar overlay), SLA burn-rate over time, buffer depth history | Full-featured time-series charting with zoom, brush selection, custom overlays. Tremor's chart components are too limited for multi-axis time-series. |

ECharts is loaded lazily (dynamic import) — only the pages that use
time-series charts pay the ~400 KB bundle cost. Tremor components are
included in the core bundle since they're used on nearly every page.

### Topology Graph

**Undecided — two candidates under evaluation:**

| | reactflow | Cytoscape.js |
|---|---|---|
| **React integration** | Native React components per node | Imperative API, React wrapper needed |
| **Custom node rendering** | JSX per node type — badges, status colours, tooltips trivially composable | HTML overlay or canvas rendering — more work for rich nodes |
| **Layout algorithms** | Manual positioning or dagre plugin | Built-in dagre, cola, cose-bilkent, elk |
| **Minimap** | Built-in `<MiniMap />` component | Plugin (`cytoscape-navigator`) |
| **Edge animation** | CSS animation on SVG paths | Canvas-based, custom animation code |
| **Compound nodes** | Supported via nested groups | Native compound node support |
| **Bundle size** | ~45 KB gzipped | ~85 KB gzipped (+ layout plugins) |
| **License** | MIT | MIT |

**Leaning toward reactflow** because:
- JSX-based custom nodes make the rich status badges and
  inline metrics per node trivial. Cytoscape requires HTML overlays
  or canvas drawing for the same result.
- The topology is a DAG with tens of nodes (not thousands) — reactflow's
  dagre plugin handles this scale easily.
- Edge animation (animated dots for data flow) is simpler in SVG/CSS
  than canvas.

**Decision deferred** to implementation phase. A spike (½ day) should
build the same 5-node topology in both libraries and compare DX,
rendering quality, and animation smoothness.

### Rejected Alternatives

| Library | Reason for rejection |
|---------|---------------------|
| **Material UI (MUI)** | Wrong aesthetic register — consumer app feel, not developer tool. Excessive padding, opinionated theming system fights customization. Signal says "enterprise admin panel," not "infrastructure monitoring." |
| **Ant Design** | Used by RisingWave. Viable but heavy (~1 MB), opinionated, and less modern-feeling than shadcn/ui. Data table component is excellent but overkill when we control the data shape. |
| **Chakra UI** | Similar to MUI in spirit — component library with runtime CSS-in-JS. Tailwind is a better fit for Next.js static export. |
| **Recharts** | Decent for simple charts but lacks the multi-axis, zoom, and overlay features needed for the refresh timeline. ECharts covers this with less custom code. |
| **D3 (direct)** | Too low-level for a small frontend team. ECharts and Tremor provide the same visual output with 10× less code. |

---

## Colour System

### Theme Modes

| Mode | Activation | Baseline |
|------|-----------|----------|
| **Dark** (default) | System preference or toggle | `zinc-950` background, `zinc-50` foreground |
| **Light** | Toggle in settings | `white` background, `zinc-950` foreground |

Follows the shadcn/ui theming approach: CSS variables on `:root` and
`.dark` class, toggled via `next-themes`. All colour references use
semantic CSS variables (`--background`, `--foreground`, `--muted`, etc.)
so both themes are maintained by changing ~20 variable values.

### Semantic Colours

These colours convey operational meaning. They are **never** used for
decorative or branding purposes.

| Token | Dark mode value | Light mode value | Meaning |
|-------|----------------|-----------------|---------|
| `--status-healthy` | `emerald-400` | `emerald-600` | Healthy / within SLA / connected |
| `--status-warning` | `amber-400` | `amber-600` | Warning / SLA burning / degraded |
| `--status-critical` | `red-400` | `red-600` | Breach / error / disconnected / fuse blown |
| `--status-inactive` | `zinc-500` | `zinc-400` | Disabled / paused / no data |
| `--status-info` | `blue-400` | `blue-600` | Informational / in-progress / refreshing |

Status colours are used on:
- Topology node borders and fills
- Topology edge strokes
- SLA budget progress bars
- Health scorecard status icons
- Badge backgrounds (`<Badge variant="healthy">`, etc.)
- Inline staleness text colour

### Node Colours (Topology)

| Node type | Border colour | Fill colour (dark) | Fill colour (light) |
|-----------|--------------|-------------------|-------------------|
| Schema group (Level 0) | Worst-child SLA colour | `zinc-900` | `white` |
| Relay pipeline | `blue-500` (connected) / `red-500` (disconnected) | `blue-950` / `red-950` | `blue-50` / `red-50` |
| Inbox / Outbox table | `teal-500` | `teal-950` | `teal-50` |
| Source base table | `zinc-500` | `zinc-900` | `white` |
| Stream table | `--status-*` (by SLA) | `zinc-900` | `white` |
| Self-monitoring stream table | `--status-*` (by SLA), dashed border | `zinc-900` | `white` |

Stream table nodes use their SLA status colour for the border: green
when within budget, amber when burning, red on breach. This means a
quick glance at the topology shows problem nodes by colour alone.

### Edge Colours (Topology)

| State | Stroke colour | Width | Animation |
|-------|-------------|-------|-----------|
| Healthy, flowing | `--status-healthy` | Proportional to throughput | Animated dots (data flowing) |
| Lag growing | `--status-warning` | Proportional to throughput | Animated dots (slower) |
| SLA breach on downstream | `--status-critical` | Thick | Animated dots (red) |
| No recent data | `--status-inactive` | Thin | No animation |

Edge width scales linearly between 1 px (0 rows/sec) and 6 px
(max throughput in the graph). The animated dots use CSS `stroke-dasharray`
+ `stroke-dashoffset` animation on SVG paths (reactflow) or canvas
frame animation (Cytoscape).

---

## Typography

Two font families, strictly separated by purpose:

| Font | Used for | Example |
|------|----------|---------|
| **Inter** (sans-serif) | UI chrome: nav labels, headings, descriptions, buttons, form labels | "Stream Tables", "Refresh Timeline", "Apply" |
| **JetBrains Mono** (monospace) | System values: table names, SQL, durations, row counts, column names, staleness values, metric numbers | `revenue_7d`, `45ms`, `1,204 rows`, `SELECT ...` |

**Size scale** (Tailwind defaults):

| Element | Size | Weight |
|---------|------|--------|
| Page heading | `text-xl` (20 px) | `font-semibold` |
| Section heading | `text-lg` (18 px) | `font-semibold` |
| Table header | `text-sm` (14 px) | `font-medium` |
| Table cell | `text-sm` (14 px) | `font-normal` |
| Badge text | `text-xs` (12 px) | `font-medium` |
| Metric value (large) | `text-2xl` (24 px) mono | `font-semibold` |
| Metric label | `text-xs` (12 px) | `font-normal`, `text-muted-foreground` |
| Tooltip | `text-xs` (12 px) | `font-normal` |
| Nav item | `text-sm` (14 px) | `font-medium` |
| SQL code block | `text-sm` (14 px) mono | `font-normal` |

---

## Navigation Structure

### Sidebar Layout

Left sidebar, collapsible to icons-only. Fixed-position, does not
scroll with page content. Width: 240 px expanded, 48 px collapsed.

Four top-level items based on user intent, not internal subsystems.
CDC, fuses, slots, and workers are operational details — they live
under Health as sub-sections, not as top-level navigation.

```
┌─────────────────────────────────────────────────────────┐
│ [≡]  pg-trickle                          [dark/light ☾] │
├──────────────┬──────────────────────────────────────────┤
│              │                                          │
│  ◈ Topology  │         [main content area]              │
│  ⊞ Tables    │                                          │
│  ♥ Health  2 │                                          │
│  ▲ Activity 3│                                          │
│              │                                          │
│              │                                          │
│              │                                          │
│              │                                          │
│──────────────│                                          │
│  ⚙ Settings  │                                          │
│  v0.21.0     │                                          │
└──────────────┴──────────────────────────────────────────┘
```

**Sidebar elements:**

| Item | Icon | Badge | Target |
|------|------|-------|--------|
| Topology | Graph icon | — | `/ui/topology` (landing) |
| Tables | Table icon | Count of tables in SLA breach | `/ui/tables` |
| Health | Heart icon | Count of critical + warning checks | `/ui/health` |
| Activity | Bell icon | Count of unread alerts | `/ui/activity` |
| Settings | Gear icon (bottom-pinned) | — | `/ui/settings` |

The sidebar uses Lucide icons (included with shadcn/ui). Badges on nav
items are small count pills using `--status-critical` or
`--status-warning` colours. They provide a persistent health summary
visible from any page.

**Collapse behaviour:** Clicking the hamburger icon `[≡]` collapses the
sidebar to 48 px, showing only icons. Hovering expands temporarily.
User preference is persisted in `localStorage`.

### Page Routing

Next.js App Router with the following route structure:

```
/ui                         → redirect to /ui/topology
/ui/topology                → Topology graph (landing page)
/ui/topology?focus=<name>   → Topology with node highlighted
/ui/topology?focus=<name>&depth=2 → Scoped neighbourhood graph
/ui/tables                  → Tables list (all stream tables, sorted worst SLA first)
/ui/tables/[schema]/[table]          → Table detail + bidirectional lineage
/ui/tables/[schema]/[table]/lineage  → Column-level lineage (full page)
/ui/tables/[schema]/[table]/operators → DVM operator inspector (full page)
/ui/health                  → Health scorecard (CDC, fuses, slots, workers as sub-sections)
/ui/activity                → Alerts + refresh timeline (tabbed or stacked)
/ui/settings                → Configuration view
```

### Breadcrumbs

Displayed at the top of the content area, below the page heading:

```
Topology                                  ← no breadcrumb (root page)
Tables                                    ← tables list
Tables > analytics.regional_summary       ← table detail
Tables > analytics.regional_summary > revenue_7d  ← upstream node detail
Health > CDC Sources                      ← health sub-section
```

Breadcrumbs use `text-sm text-muted-foreground` with `>` separators.
Each segment is a link except the current page.

---

## Screen Layouts

Text descriptions of each key screen. These are structural layouts,
not pixel-perfect mockups — they describe the information hierarchy
and spatial arrangement.

### Landing: Topology Graph

**Full viewport** — the graph fills the entire content area (sidebar to
right edge, top nav to bottom edge). No page heading, no padding. The
graph IS the page.

The landing view is **Level 0 — Systems overview**: one node per
PostgreSQL schema. External transports (Kafka, NATS, webhook, etc.)
are shown as icon annotations on schema nodes — not as separate graph
nodes. Layout is auto-computed from inter-schema edge direction by
dagre/elkjs — no manual tier assignment.

```
┌──────────────────────────────────────────────────────────────┐
│  [breadcrumb: Systems]                              [legend] │
│                                                              │
│  ┌─────────────────┐     ┌─────────────┐     ┌─────────────┐│
│  │   erp_raw       │────▶│  canonical  │────▶│  analytics  ││
│  │   5 tables      │     │  45 tables  │     │  8 tables   ││
│  │ ⟵kafka ⟵webhook │     │ ●43 🟡1 🔴1 │     │  ●8  kafka⟶ ││
│  └─────────────────┘     └─────────────┘     └─────────────┘│
│       ↑↓ webhook                                             │
│   (bidirectional sync)                                       │
│                                                              │
│  ┌─────────────────┐                                         │
│  │   crm_raw       │────▶  (flows into canonical …)          │
│  │   2 tables      │                                         │
│  │  ⟵kafka         │                                         │
│  └─────────────────┘                                         │
│                                                              │
│  [minimap]                              [zoom +/−] [fit]    │
└──────────────────────────────────────────────────────────────┘
```

Transport annotations (`⟵kafka`, `⟵webhook`, `kafka⟶`) are small
icon badges on the schema node border — green = connected, red =
disconnected. Hover to see which relay config(s) are active.

**Drilling in (Level 1).** Click a schema node (e.g. `erp_raw`) or
an edge between two schemas (e.g. `erp_raw → canonical`).
The graph transitions to show individual nodes within that scope:

```
┌──────────────────────────────────────────────────────────────┐
│  [breadcrumb: Systems > erp_raw → canonical]      [legend]  │
│                                                              │
│  erp_raw                        canonical                   │
│  ┌──────────────┐               ┌─────────────────────┐     │
│  │ orders_inbox │──▶ orders_raw ──▶ orders_canonical   │     │
│  │              │   3s stale       5s stale             │     │
│  └──────────────┘               └─────────────────────┘     │
│  ┌──────────────┐               ┌─────────────────────┐     │
│  │ items_inbox  │──▶ items_raw  ──▶ items_canonical    │     │
│  │              │   2s stale       4s stale             │     │
│  └──────────────┘               └─────────────────────┘     │
│  ┌──────────────┐               ┌─────────────────────┐     │
│  │ stock_inbox  │──▶ stock_raw  ──▶ stock_canonical 🟡 │     │
│  │              │   12s stale      45s stale            │     │
│  └──────────────┘               └─────────────────────┘     │
│                                                              │
│  [minimap]                              [zoom +/−] [fit]    │
└──────────────────────────────────────────────────────────────┘
```

Breadcrumb at top allows navigation back to Level 0. Individual
nodes show their staleness badges and SLA colours inline.

**Single-schema auto-navigation.** When only one schema exists, the
topology opens directly at Level 1. The breadcrumb still renders
`Systems > <schema>` so the user can navigate back to Level 0 and
discover the grouping model — useful when they later add a second
schema.

**Overlays on the graph canvas:**

- **Top-left:** Minimap (~200×120 px, semi-transparent background).
- **Top-right:** Legend toggle button.
- **Bottom-left:** Zoom controls (+/−/fit), layout reset button.
- **Bottom-right:** Time slider (for historical replay, Tier 2+).
  Hidden by default, toggled by a clock icon.

**Right panel (Sheet):** Clicking an individual node (Level 1+) opens
a `Sheet` (shadcn/ui slide-over panel) from the right edge, ~400 px
wide. Clicking a group node (Level 0) drills into Level 1 instead.
The sheet shows:

- Node name (monospace, with copy button)
- Status badge (healthy/warning/breach)
- Key metrics (staleness, buffer depth, refresh duration)
- Quick actions (Refresh, View detail, View lineage)
- Mini refresh history sparkline (last 1 h)

Clicking "View detail →" navigates to the full detail page.

### Tables List

The primary list view. Every stream table annotated with its structural
type, sorted worst SLA first by default.

```
┌──────────────────────────────────────────────────────────────┐
│  Tables                                         [search 🔍] │
│             [Schema: All ▼] [Type: All ▼] [SLA: All ▼]    │
├──────────────────────────────────────────────────────────────┤
│  Table                Type          Staleness  E2E Lag  SLA  │
│  ───────────────────  ──────────    ─────────  ───────  ─── │
│  analytics.regional_summary  leaf union  45s   61s  ████ 🔴  │
│  analytics.revenue_7d        intermediate 12s  28s  ██░ 🟡   │
│  erp_raw.orders_raw          inbox        3s    3s  █ 🟢     │
│  erp_raw.customers           orphan       —     —   —        │
│  ...                                                         │
├──────────────────────────────────────────────────────────────┤
│  24 tables                                                   │
└──────────────────────────────────────────────────────────────┘
```

- **Type column:** Badge chips (inbox / outbox / leaf / intermediate /
  union / orphan). Multiple badges allowed.
- **Staleness:** How stale this table's own data is.
- **E2E Lag:** Max cumulative staleness from any root to this table.
- **SLA column:** Progress bar of the worst SLA breach in the upstream
  chain, including this table.
- **Row click:** Navigates to table detail.
- **Sortable columns:** Default sort by SLA budget descending.
- **Faceted filters:** Schema, type badge, refresh mode (DIFF/FULL), SLA status.
- **Search:** Matches table name or schema.

### Table Detail

Shows the table's own metrics plus its **bidirectional lineage** —
upstream (root causes) on the left, this table in the centre,
downstream (blast radius) on the right.

```
┌──────────────────────────────────────────────────────────────┐
│  ← Tables                                                    │
│  analytics.regional_summary       leaf union  [View in Topology] │
│  🔴 SLA breach · 5 upstream · 2 downstream · E2E lag: 61s    │
├──────────────────────────────────────────────────────────────┤
│  [Overview]  [Lineage]  [History]  [Operators]  [SQL]          │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  Staleness       Buffer Depth      Last Refresh     Avg (1h)  │
│  45s             1,204 rows        61s ago          180ms     │
│  E2E Lag: 61s                                                  │
│                                                               │
│  Lineage (Overview tab — condensed)                           │
│                                                               │
│  UPSTREAM (root causes)        THIS       DOWNSTREAM (impact)  │
│  ○ orders ──►                                                  │
│  ├● orders_raw ──►               ┌────────────┐  ►● NATS outbox  │
│  │ └● revenue_7d ─►─►─►  │ regional  │  ►● exec_report  │
│  ○ costs ──►              │ _summary  │                    │
│  └● costs_raw ──►         └────────────┘                    │
│    └● costs_7d ─►─►─►                                       │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

- Upstream nodes are coloured by SLA status — quickly shows which
  branch is the root cause of a breach.
- Downstream nodes are coloured by SLA status — shows blast radius.
- Click any node → navigates to that table's detail (lineage re-centres on clicked node).
- "View in Topology" scopes the topology graph to this neighbourhood.
- The **Lineage** tab expands this to a full-canvas interactive graph.

### Table Detail — Full Page

All table navigation (from list, from lineage, from topology) lands on
the same page at `/ui/tables/[schema]/[table]`. Five tabs:

```
┌──────────────────────────────────────────────────────────────┐
│  ← Tables                                                    │
│  analytics.regional_summary  [leaf] [union]   [Refresh ↻][⋯]│
│  🔴 SLA breach · DIFFERENTIAL · every 60s                    │
├──────────────────────────────────────────────────────────────┤
│  [Overview]  [Lineage]  [History]  [Operators]  [SQL]        │
└──────────────────────────────────────────────────────────────┘
```

**Action buttons (top right, always visible):**
- `Refresh ↻` — triggers manual refresh (previewed if Tier 2+)
- `⋯` dropdown — Pause, Resume, View in Topology, Copy table name

---

**Overview tab** (default)

```
┌──────────────────────────────────────────────────────────────┐
│  Staleness   Buffer Depth   Last Refresh   Avg (1h)  E2E Lag │
│  45s         1,204 rows     61s ago        180ms     61s     │
│                                                               │
│  ┌─ Refresh sparkline (1h) ──────────────────────────────┐  │
│  │ ▁▂▃▂▁▂▃▃▂▁▁▂▃▂▁▂▃▃▂▁▁▂▃▂▁▂▃▃▂▁▁▂▃▂▁▂▃▃▂▁          │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
│  Mini lineage (upstream left · this centre · downstream right)│
│  ○ orders ──►                                                │
│  ├● orders_raw ──►           ┌─────────────┐  ►● NATS outbox│
│  │ └● revenue_7d ───────────►│regional_sum │  ►● exec_report│
│  ○ costs ──►                 └─────────────┘                │
│  └● costs_7d ────────────────►                              │
│                                                               │
│  Columns                                                      │
│  Name              Type       Nullable   DVM source          │
│  ──────────────    ────────   ────────   ───────────────     │
│  region            text       NO         orders.region       │
│  revenue_total     numeric    NO         SUM(orders.amount)  │
│  order_count       bigint     NO         COUNT(*)            │
└──────────────────────────────────────────────────────────────┘
```

---

**Lineage tab** — full-canvas bidirectional graph

```
┌──────────────────────────────────────────────────────────────┐
│  [Table lineage ●]  [Column lineage ○]       [depth: all ▼] │
├──────────────────────────────────────────────────────────────┐
│                                                               │
│   ○ orders           ┌─────────────┐   ● NATS outbox        │
│   ├─●orders_raw ────►│             │                         │
│   │  └─●revenue_7d ─►│regional_sum │──►● exec_report        │
│   ○ costs            │             │                         │
│   └──●costs_7d ─────►└─────────────┘                        │
│                                                               │
│  [minimap]                              [zoom +/−] [fit]    │
└──────────────────────────────────────────────────────────────┘
```

Toggle between **Table lineage** (node = table) and **Column lineage**
(node = individual column — shows which upstream columns feed each
output column). Clicking any node navigates to its detail page,
re-centering the lineage graph. Depth slider (1 hop / 2 hops / all)
controls how far upstream and downstream to render.

---

**History tab** — refresh log

```
┌──────────────────────────────────────────────────────────────┐
│  Time             Mode   Duration   Rows Δ   Status          │
│  ────────────     ─────  ────────   ──────   ──────          │
│  13:42:01         DIFF   45ms       +12      ✅              │
│  13:41:01         DIFF   180ms      +3,204   ✅              │
│  13:40:01         FULL   2,400ms    —        ✅ (fuse reset) │
│  13:39:01         DIFF   —          —        🔴 error        │
│  ...                                                         │
└──────────────────────────────────────────────────────────────┘
```

Paginated, sortable. Click a row → expands to show error detail or
delta summary.

---

**Operators tab** — DVM operator tree

Graphviz-wasm rendering of the compiled differential operator tree.
Read-only. Shows how the SQL query was decomposed into
`Filter → Join → Aggregate → Consolidate` operators. Useful for
debugging why a table uses FULL refresh (unsupported operator visible
here).

---

**SQL tab** — view definition + delta SQL

```
┌──────────────────────────────────────────────────────────────┐
│  View definition (source)      Delta SQL (compiled)          │
│  ───────────────────────────   ─────────────────────────    │
│  SELECT region,                WITH _delta AS (             │
│    SUM(amount) AS revenue,       SELECT ...                  │
│    COUNT(*) AS order_count       FROM pgtrickle_changes...   │
│  FROM orders                   )                            │
│  GROUP BY region               UPDATE regional_summary ...  │
└──────────────────────────────────────────────────────────────┘
```

Both panels are read-only syntax-highlighted code blocks. Side-by-side
on wide screens, stacked on narrow.

### SLA Budget Dashboard

Accessed from the Tables page via a "SLA Budget" tab or a dedicated
`/ui/tables?view=sla` toggle.

```
┌──────────────────────────────────────────────────────────────┐
│  SLA Budget Overview                          [worst first ▼] │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  orders_hourly ████████████████████ 102% 🔴 BREACH           │
│  61s / 60s · burning at 0.8s/s · breached 45s ago            │
│                                                               │
│  regional_summary ████████████░░░░ 75% ⚠️ WARNING            │
│  45s / 60s · burning at 0.3s/s · breach in ~50s              │
│                                                               │
│  revenue_7d ████░░░░░░░░░░░░░░░░ 20% ✅                     │
│  12s / 60s · stable · no breach expected                     │
│                                                               │
│  customer_agg █░░░░░░░░░░░░░░░░░░ 10% ✅                    │
│  3s / 30s · stable · no breach expected                      │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

Each row is a wide progress bar coloured by status. Below the bar:
staleness fraction, burn rate (from `sla_burn_rate` stream table),
and time-to-breach prediction. Sorted worst-first by default.

### Health Scorecard

```
┌──────────────────────────────────────────────────────────────┐
│  Health                                         4/6 passing  │
├──────────────────────────────────────────────────────────────┤
│  🔴 Buffer growth   orders: 15,204 rows, growing   [View →] │
│  ⚠️ CDC triggers    customers: trigger disabled     [Fix →]  │
│  ✅ Scheduler       Running · 12 tables · 3 workers          │
│  ✅ Replication     Active · 2.1 MB retained WAL             │
│  ✅ Relay: kafka    Connected · 0 lag rows                   │
│  ✅ Fuses           All armed · 0 blown                      │
└──────────────────────────────────────────────────────────────┘
```

Severity-sorted (critical → warning → healthy). Each row is a
single line with: status icon, check name, detail text, action link.
Compact — no cards, no whitespace waste.

### Refresh Timeline

Full-width ECharts time-series. Lives on the Activity page as a
prominent section above the alerts feed. Controls above the chart.

```
┌──────────────────────────────────────────────────────────────┐
│  Refresh Timeline                    [1h] [6h] [24h] [7d]   │
│                                      [All tables ▼] [⚙]     │
├──────────────────────────────────────────────────────────────┤
│  ms                                              rows        │
│  400 ┤                                           │ 2000      │
│      │    ╭╮                                     │           │
│  300 ┤   ╭╯╰╮        ╭──╮                       │ 1500      │
│      │  ╭╯  ╰╮      ╭╯  ╰╮    ▓▓                │           │
│  200 ┤──╯    ╰──────╯    ╰────▓▓───         --- │ 1000 SLA  │
│      │                        ▓▓                 │           │
│  100 ┤                                           │ 500       │
│      │                    ●                      │           │
│    0 ┼──────────────────────────────────────────>│ 0         │
│      08:00        09:00        10:00       11:00             │
├──────────────────────────────────────────────────────────────┤
│  Legend: ── duration (ms)  ▓ row delta  ● error  --- SLA     │
└──────────────────────────────────────────────────────────────┘
```

- Left Y axis: refresh duration (ms), stacked area per table
- Right Y axis: row delta count (inserts + deletes), bar chart
- SLA threshold: horizontal dashed red line
- Error markers: red dots on timeline
- Brush zoom: click-drag to zoom into a time range
- Table filter: dropdown to show one table or all

### Alerts & Activity Feed

Merges alerts and refresh timeline into a single Activity page
(two tabs, or stacked with the timeline above and the alerts feed
below).

```
┌──────────────────────────────────────────────────────────────┐
│  Alerts                              [All ▼] [Clear read]    │
├──────────────────────────────────────────────────────────────┤
│  ● 11:02:45  SLA breach    orders_hourly: 61s > 60s SLA     │
│  ● 11:02:30  Fuse blown    revenue_7d: 3 consecutive errors  │
│    11:01:15  CDC warning   customers: trigger disabled        │
│    11:00:00  Refresh ok    revenue_7d: 45ms, +3/-1 rows      │
│    10:59:45  Relay lag     nats-events: 342 rows behind      │
│    ...                                                        │
├──────────────────────────────────────────────────────────────┤
│  Showing 50 of 234 alerts             [< 1 2 3 4 5 >]       │
└──────────────────────────────────────────────────────────────┘
```

- **●** dot marks unread alerts. Reading clears them (per-session).
- Severity-filtered dropdown: All, Critical, Warning, Info.
- Timestamps in monospace, relative ("2m ago") with absolute on hover.
- Alert text links to the relevant object (table name → table detail,
  pipeline name → pipeline detail).

### SQL Preview Modal

A `Dialog` (modal) used for all write operations (Tier 2+). Also the
human review layer for LLM-generated SQL.

```
┌──────────────────────────────────────────────────────────────┐
│  SQL Preview                                          [×]    │
├──────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────┐   │
│  │  CREATE STREAM TABLE public.revenue_7d AS            │   │
│  │  SELECT                                              │   │
│  │      region,                                         │   │
│  │      SUM(amount) AS total_revenue                    │   │
│  │  FROM orders                                         │   │
│  │  JOIN customers USING (customer_id)                  │   │
│  │  WHERE order_date > now() - interval '7 days'        │   │
│  │  GROUP BY region                                     │   │
│  │  SCHEDULE '60 seconds';                              │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
│  DVM Analysis:                                                │
│  ✅ DIFFERENTIAL eligible                                    │
│  ✅ All aggregates algebraic (SUM, COUNT)                    │
│  ⚠️ WHERE clause uses now() — refreshes always see changes   │
│                                                               │
│                              [Copy SQL]  [Cancel]  [Apply]   │
└──────────────────────────────────────────────────────────────┘
```

- SQL block uses JetBrains Mono with syntax highlighting (via
  `shiki` or `prism`).
- DVM analysis results shown below the SQL (from `/api/v1/sql/preview`).
- "Copy SQL" copies to clipboard for migration files.
- "Apply" executes the SQL. Button uses `--status-healthy` colour
  (green) to signal a safe action. If DVM analysis shows warnings,
  the button text changes to "Apply (with warnings)" in amber.
- Editable (Tier 3): In Tier 3, the SQL block becomes a Monaco editor
  where the user can modify the generated SQL before applying.

---

## Responsive Behaviour

**Desktop-first. Tablet usable. Phone minimal.**

| Breakpoint | Behaviour |
|-----------|-----------|
| ≥ 1280 px (desktop) | Full layout — expanded sidebar, full topology, all columns visible |
| 768–1279 px (tablet) | Sidebar collapsed to icons by default. Tables hide low-priority columns (trend, throughput). Topology graph is full-width. |
| < 768 px (phone) | Sidebar becomes a bottom tab bar (5 key items). Tables become card-based. Topology is zoomable/pannable but not ideal. Health scorecard and alerts are the primary phone views. |

**Phone priority pages** (the views an SRE on-call actually needs):
1. Health scorecard
2. Alerts feed (Activity page)
3. SLA budget overview (Pipelines, sorted worst-first)
4. Pipeline detail (simplified)

The topology graph on phone is a "view-only, pinch-zoom" experience —
useful for a quick glance but not for exploration. A "View on desktop"
banner is shown.

---

## Accessibility

Minimum target: **WCAG 2.1 Level AA.**

| Requirement | Implementation |
|------------|----------------|
| Keyboard navigation | Radix primitives handle focus management. All interactive elements reachable via Tab. Topology graph supports arrow-key node traversal. |
| Screen reader | ARIA labels on all status badges (`aria-label="healthy"`, not just colour). Topology nodes announced with name + status. Alert feed is a live region (`aria-live="polite"`). |
| Colour contrast | All text meets 4.5:1 contrast ratio in both themes. Status colours are supplemented with icons (✅ ⚠️ 🔴) so colour-blind users don't depend on hue alone. |
| Reduced motion | `prefers-reduced-motion` disables topology edge animations and transition effects. |
| Focus indicators | Visible focus ring (2 px `--ring` colour) on all interactive elements. Never hidden. |

---

## Open Questions

### SQ-1: Brand accent colour

The accent colour is used for: active sidebar item, primary buttons
(non-status), links, focus rings, selected tab underline. It must not
conflict with the semantic status colours (green/amber/red).

| Option | Notes |
|--------|-------|
| **Blue** (`blue-500`) | Safe, professional, clear separation from status colours. Common in dev tools (VS Code, Docker Desktop). |
| **Teal/cyan** (`teal-500`) | Evokes streaming/data flow. Slightly less conventional. May be too close to the inbox/outbox node colour. |
| **Purple** (`violet-500`) | Modern dev-tool feel (Linear, Vercel). Distinctive. |
| **Green** (`emerald-500`) | PostgreSQL heritage. But conflicts with `--status-healthy` — rejected. |

### SQ-2: Command palette

A `Cmd+K` / `Ctrl+K` command palette (using shadcn/ui `Command`
component, built on `cmdk`) enables power users to:

- Jump to any stream table by name
- Trigger a manual refresh
- Navigate to any page
- Search alerts
- Toggle dark/light mode

Implementation cost is low (~1 day with `cmdk`). Question: should it
ship with Tier 1 or Tier 2?

### SQ-3: Topology graph library

reactflow vs Cytoscape.js — see the comparison table in
[Topology Graph](#topology-graph). Decision deferred to a ½-day spike
during implementation.

### SQ-4: SQL syntax highlighting

Options for the read-only SQL display (Tier 1–2) and editable SQL
(Tier 3):

| Tier | Library | Rationale |
|------|---------|-----------|
| 1–2 | **Shiki** | Static highlighting, no runtime JS. Supports PostgreSQL grammar. Used by Next.js docs. |
| 3 | **Monaco Editor** | Full editing with autocomplete, inline errors, bracket matching. Heavy (~2 MB) but loaded only on the SQL editing page. |
