# Real-time Demo

This demo shows pg_trickle doing real work: a continuous stream of events
flows into PostgreSQL, and a **DAG of stream tables** keeps a live view of
that data up to date — automatically, incrementally, and within seconds.

Two scenarios are available via the `DEMO_SCENARIO` environment variable:

| Scenario | Default? | Pipeline |
|----------|----------|----------|
| `fraud` | — | Financial fraud detection — 9-node, 4-layer DAG over a transaction stream |
| `ecommerce` | ✅ | E-commerce analytics — 6-node DAG over a continuous order stream |

Each scenario includes two purpose-built **differential efficiency showcases**:
stream tables with sub-1.0 change ratios that demonstrate when DIFFERENTIAL
mode is clearly the right choice.

It is the fastest way to see how stream tables, differential refresh, and
DAG-aware scheduling work together on data you can watch moving.

---

## Quick Start

```bash
cd demo

# E-commerce analytics (default)
docker compose up --build

# Fraud detection
DEMO_SCENARIO=fraud docker compose up --build
```

Open **http://localhost:8080** — the dashboard refreshes every 2 seconds.

To stop and remove all data:

```bash
docker compose down -v
```

> **Switching scenarios** requires removing the old data volume:
> ```bash
> docker compose down -v
> DEMO_SCENARIO=ecommerce docker compose up
> ```

---

## What the Demo Does

Three Docker services start together:

| Service | Role |
|---------|------|
| **postgres** | PostgreSQL 18 with pg_trickle; initialises the schema, seed data, and all stream tables on first boot |
| **generator** | Python script that continuously inserts events; periodically triggers bursts that stress-test differential refresh |
| **dashboard** | Flask web app served at http://localhost:8080; reads from stream tables and auto-refreshes every 2 seconds |

The `DEMO_SCENARIO` variable controls which SQL files are loaded on startup
and which generator/dashboard module is activated. Both scenarios share the
same Docker Compose services.

---

## Scenario: fraud

The demo models the data pipeline a financial institution might build to
spot suspicious activity as it happens, not hours later in a batch job.

### Source Data

Four **regular PostgreSQL tables** hold the reference data:

| Table | Contents |
|-------|----------|
| `users` | 30 users, each with a name, country, and account age |
| `merchants` | 15 merchants across categories: Retail, Electronics, Travel, Food, Pharmacy, Gambling, Crypto |
| `transactions` | The live stream — the generator inserts here continuously |
| `merchant_risk_tier` | Slowly-changing risk tier (STANDARD / ELEVATED / HIGH) for each merchant; the generator rotates one merchant's tier every ~30 cycles |

`transactions` is the only table that grows continuously. `merchant_risk_tier`
changes occasionally (about one row per minute). Everything else is static.

### Normal vs. Suspicious Traffic

The generator creates two kinds of transactions:

**Normal traffic** — a random user buys something from a random merchant at a
plausible amount for that merchant's category. Inserted at roughly one per
second.

**Suspicious burst** — every ~45 seconds, the generator picks one user and
fires 6–14 rapid transactions (0.15–0.45 s apart) at Crypto or Gambling
merchants, with amounts that escalate with each successive transaction. This
pattern is designed to cross the risk thresholds and light up the HIGH-risk
column on the dashboard.

---

## The DAG of Stream Tables (fraud)

All nine stream tables are defined in
[demo/postgres/fraud/02_stream_tables.sql](../demo/postgres/fraud/02_stream_tables.sql).

```
  Base tables             Layer 1 — Silver           Layer 2 — Gold              Layer 3 — Platinum
  ────────────            ──────────────────────     ─────────────────────       ──────────────────────

  ┌──────────┐            ┌──────────────────┐
  │  users   │───────────►│  user_velocity   │──────────────────────────────────►┌──────────────┐
  └──────────┘            │  DIFFERENTIAL 1s │                                   │ country_risk │
                          └──────┬───────────┘                                   │  DIFF, calc  │
                                 │                                                └──────────────┘
  ┌──────────────┐               │  ┌──────────────────┐
  │ transactions │───────────────┼─►│  merchant_stats  │
  │  (stream)    │               │  │  DIFFERENTIAL 1s │
  └──────────────┘               │  └──────┬───────────┘
          │                      │         │
          │          ┌───────────┴─────────┘ ← DIAMOND DEPENDENCY
          │          │
          │          ▼                                                             ┌─────────────────┐
          │     ┌────────────────────┐                                             │  alert_summary  │
          │     │    risk_scores     │────────────────────────────────────────────►│   DIFF, calc    │
          │     │   FULL, calc       │                                             └─────────────────┘
          │     └────────────────────┘
          │                                                                         ┌───────────────────────┐
          │                                                                         │  top_risky_merchants  │
          └─────────────────────────────────────────────────────────────────────►  │   DIFF, calc          │
                                                                                    └───────────┬───────────┘
  ┌──────────┐            ┌──────────────────┐                                                 │
  │merchants │───────────►│ category_volume  │          ┌──────────────────────────────────────▼──────┐
  └──────────┘            │  DIFFERENTIAL 1s │          │       top_10_risky_merchants                │
                          └──────────────────┘          │  DIFFERENTIAL 5s  ← SHOWCASE #2             │
                                                         │  change ratio ≈ 0.25 (LIMIT 10)             │
  ┌───────────────────┐   ┌──────────────────────┐      └─────────────────────────────────────────────┘
  │ merchant_risk_tier│──►│ merchant_tier_stats  │  ← SHOWCASE #1
  │ (slowly-changing) │   │   DIFFERENTIAL 5s    │     change ratio ≈ 0.07
  └───────────────────┘   │                      │
  ┌──────────┐            │                      │
  │merchants │───────────►│                      │
  └──────────┘            └──────────────────────┘
```

### Layer 1 — Silver: Direct Aggregates

These three stream tables each read directly from the base tables and refresh
every second using DIFFERENTIAL mode. pg_trickle calculates only what changed
since the last refresh — if five new transactions arrived, it adjusts exactly
the five affected aggregate buckets rather than recomputing the full table.

**`user_velocity`** — per-user transaction statistics

For each of the 30 users, keeps a running count of transactions, total spend,
average transaction amount, and how many distinct merchants they have visited.
This is the core input for detecting users who are suddenly transacting far
more than usual.

```sql
SELECT u.id, u.name, u.country,
       COUNT(t.id)               AS txn_count,
       SUM(t.amount)             AS total_spent,
       ROUND(AVG(t.amount), 2)   AS avg_txn_amount,
       COUNT(DISTINCT t.merchant_id) AS unique_merchants
FROM users u
LEFT JOIN transactions t ON t.user_id = u.id
GROUP BY u.id, u.name, u.country
```

**`merchant_stats`** — per-merchant baseline

Tracks how many transactions each merchant typically sees and at what amounts.
A transaction that is 3× the merchant's own average is more suspicious than
one that is 3× the user's average — this table supplies that context.

**`category_volume`** — industry-level view

Groups by merchant category (Crypto, Gambling, Retail, etc.) so the dashboard
can show which sectors are hot at any moment. Refreshes every second and uses
DIFFERENTIAL: a new transaction in Electronics updates only the Electronics row.

### Layer 2 — Gold: Derived Metrics

These stream tables read from the Layer 1 stream tables (stream tables reading
stream tables). pg_trickle's scheduler refreshes Layer 1 first, then triggers
Layer 2 automatically — you never schedule this yourself.

**`risk_scores`** — the diamond convergence node

This is the most interesting table in the DAG. It joins:
- `transactions` — the raw event
- `user_velocity` — that user's accumulated behaviour (Layer 1)
- `merchant_stats` — that merchant's baseline (Layer 1)

Because `transactions` feeds **both** `user_velocity` and `merchant_stats`
independently, and `risk_scores` depends on both, this creates a classic
**diamond dependency**:

```
transactions ──→ user_velocity  ──┐
                                   ├──→ risk_scores
transactions ──→ merchant_stats ──┘
```

pg_trickle detects this diamond and schedules both Layer 1 nodes before
triggering the Layer 2 refresh, so `risk_scores` always sees fresh context.

The risk scoring logic lives entirely in SQL:

```sql
CASE
    WHEN uv.txn_count > 20
         AND t.amount > 3 * uv.avg_txn_amount
        THEN 'HIGH'
    WHEN uv.txn_count > 10
         OR  t.amount > 2 * uv.avg_txn_amount
        THEN 'MEDIUM'
    ELSE 'LOW'
END AS risk_level
```

`risk_scores` uses `refresh_mode => 'FULL'` because it joins three sources
(including two stream tables) in a way that requires re-evaluating all rows to
maintain correctness. FULL mode is the right choice here — it is still fast
because it is triggered only when an upstream actually changes.

**`country_risk`** — geographic rollup

Reads `user_velocity` (Layer 1) and aggregates by country. Pure DIFFERENTIAL
because it is a simple `GROUP BY country` over the Silver layer.

### Layer 3 — Platinum: Executive Roll-Ups

These stream tables read from `risk_scores` (Layer 2) and are defined with
`schedule => 'calculated'`, meaning pg_trickle fires them automatically
whenever their upstream changes.

**`alert_summary`** — the primary KPI

Counts and totals by risk level (LOW / MEDIUM / HIGH). This is what drives the
four big counters at the top of the dashboard. Because it is a simple aggregate
over `risk_scores`, it uses DIFFERENTIAL mode and updates only the affected
risk-level row on each cycle.

**`top_risky_merchants`** — merchant triage list

Groups `risk_scores` by merchant name and category, counting how many HIGH and
MEDIUM transactions each merchant has seen, plus a risk-rate percentage.
Operationally this is where a fraud team would start when deciding which
merchants to review or block.

### Differential Efficiency Showcases

Two additional stream tables sit outside the main fraud pipeline. Their purpose
is to demonstrate that DIFFERENTIAL mode can achieve a meaningfully sub-1.0
change ratio when the output cardinality is constrained.

**`merchant_tier_stats`** — Showcase #1: slowly-changing lookup source

Joins `merchants` (static) with `merchant_risk_tier` (a 15-row lookup that the
generator updates one row per ~30 cycles). Because no fast-growing table is in
the query, only the one rotated merchant's row changes each cycle:

- Change ratio ≈ 1/15 ≈ 0.07
- Refresh Mode Advisor recommendation: ✓ KEEP DIFFERENTIAL
- Schedule: 5 s (independent of the main DAG)

This is the counterpoint to `risk_scores`. `risk_scores` correctly uses FULL
because its change ratio is ~1.0; `merchant_tier_stats` correctly uses
DIFFERENTIAL because its change ratio is ~0.07. Seeing both on the same
dashboard makes the advisor's logic concrete.

**`top_10_risky_merchants`** — Showcase #2: fixed-cardinality output

Reads `top_risky_merchants` (Layer 3) and applies `LIMIT 10`. Even though the
upstream changes heavily every cycle, only the merchants whose rank crosses the
top-10 boundary produce a net change in the output. Typically 2–3 merchants
enter or leave the top 10 per refresh cycle:

- Change ratio ≈ 0.2–0.3
- Refresh Mode Advisor recommendation: ✓ KEEP DIFFERENTIAL
- Schedule: 5 s

---

## The Dashboard

Open http://localhost:8080. Panels update every 2 seconds.

### KPI Row

Four counters: total transactions, LOW / MEDIUM / HIGH counts. Below them, a
proportional colour bar that makes the risk mix visible at a glance.

### Recent Alerts

A live table of the most recent HIGH and MEDIUM transactions — transaction ID,
user name, merchant, category, amount, and risk badge. Rows turn red for HIGH
and amber for MEDIUM. During a burst you will see this fill with HIGH rows as
the generator fires rapid transactions.

### Merchant Risk Leaderboard

Sorted by HIGH-risk count descending. Crypto and Gambling merchants will
typically appear at the top because the generator targets them during bursts.

### User Velocity, Country Overview, Category Volume

Three side-by-side tables driven by the Layer 1 stream tables. These are the
most-refreshed tables in the DAG (every second); watching user velocity change
in real time illustrates why DIFFERENTIAL mode matters — only the affected rows
move.

### Merchant Tier Stats and Tiers

Two panels driven by `merchant_tier_stats`. The left panel shows the full 15-row
output (merchant ID, name, category, current tier, risk score, and when the tier
last changed). The right panel shows a compact tier-only view. Tiers are
colour-coded: HIGH = red, ELEVATED = amber, STANDARD = green. Tiers rotate
visibly every ~30 generator cycles (roughly once per minute).

### Top 10 Risky Merchants Leaderboard

A live leaderboard driven by `top_10_risky_merchants`. Shows rank, merchant
name, category, total transactions, HIGH and MEDIUM risk counts, and a
risk-rate percentage. The percentage column is coloured green (<25%), amber
(25–49%), or red (≥50%). Watch the rankings shift as the generator's burst
patterns accumulate.

### Stream Table Status

A compact status panel showing each stream table's refresh mode, schedule, and
whether it is populated. This reads from `pgtrickle.pgt_status()`.

### DAG Topology

A collapsible ASCII diagram showing the full dependency graph. Useful as a
reference while exploring the database directly.

---

## Exploring the Database (fraud)

Connect directly to inspect the stream tables and pg_trickle internals:

```bash
docker compose exec postgres psql -U demo -d fraud_demo
```

**Check all stream table status:**

```sql
SELECT name, status, refresh_mode, is_populated, staleness
FROM pgtrickle.pgt_status();
```

**Inspect the DAG dependency graph (full tree):**

```sql
SELECT tree_line FROM pgtrickle.dependency_tree()
ORDER BY tree_line;
```

**See the most recent refresh history:**

```sql
SELECT st.pgt_name, rh.action, rh.status, 
       (rh.end_time - rh.start_time) AS duration, rh.start_time
FROM pgtrickle.pgt_refresh_history rh
JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = rh.pgt_id
ORDER BY rh.start_time DESC
LIMIT 20;
```

**Spot the diamond groups (stream tables with shared sources):**

```sql
SELECT group_id, member_name, is_convergence, schedule_policy
FROM pgtrickle.diamond_groups()
ORDER BY group_id, is_convergence DESC;
```

**Watch a HIGH-risk alert appear:**

```sql
-- In one terminal: watch for new HIGH rows
SELECT txn_id, user_name, merchant_name, amount
FROM risk_scores
WHERE risk_level = 'HIGH'
ORDER BY txn_id DESC
LIMIT 5;

-- Wait ~45 seconds for the next burst, then run the query again.
```

**Force a manual refresh:**

```sql
SELECT pgtrickle.refresh_stream_table('risk_scores');
```

---

## How the Files Are Organised

```
demo/
├── docker-compose.yml          # Service definitions; DEMO_SCENARIO selects the scenario
├── README.md                   # Quick start + scenario descriptions
│
├── postgres/
│   ├── fraud/
│   │   ├── 01_schema.sql       # Base tables + seed data (30 users, 15 merchants,
│   │   │                       # 40 initial transactions, merchant_risk_tier)
│   │   └── 02_stream_tables.sql# All 9 stream table definitions (Layers 1–3 + showcases)
│   └── ecommerce/
│       ├── 01_schema.sql       # Base tables + seed data (customers, products,
│       │                       # categories, orders, product_catalog)
│       └── 02_stream_tables.sql# All 6 stream table definitions (Layers 1–3 + showcases)
│
├── generator/
│   ├── Dockerfile
│   ├── requirements.txt        # psycopg2-binary only
│   ├── generate.py             # Scenario dispatcher; reads DEMO_SCENARIO
│   └── scenarios/
│       ├── fraud.py            # Transaction generator (normal + burst mode)
│       └── ecommerce.py        # Order generator (normal + flash sale mode)
│
└── dashboard/
    ├── Dockerfile
    ├── requirements.txt        # flask + psycopg2-binary
    ├── app.py                  # Scenario dispatcher: / → HTML, /api/data → JSON,
    │                           #   /api/internals → stream table metadata (shared)
    └── scenarios/
        ├── fraud.py            # Fraud HTML, DAG diagram, and data queries
        └── ecommerce.py        # E-commerce HTML, DAG diagram, and data queries
```

---

## Why These Design Choices

### Why seven stream tables instead of one big query?

Splitting the computation across three layers means each layer does a smaller,
cheaper differential computation. A single-query approach that joins `users`,
`transactions`, and `merchants` directly into `risk_scores` would force a FULL
refresh every cycle because no single source table captures all changes. With
the layered approach:

- L1 tables catch source changes differentially (they are cheap to maintain)
- L2/L3 tables read from already-aggregated L1 data (smaller join inputs)
- The scheduler only re-runs a layer when its inputs actually changed

### Why is `risk_scores` FULL and not DIFFERENTIAL?

`risk_scores` is a **1:1 projection of `transactions`** — one output row per
input transaction, with enrichment from the L1 stream tables. Since
`transactions` is append-only, the **change ratio is ~1.0**: every refresh adds
roughly as many new rows as existed before, making DIFFERENTIAL no more
efficient than FULL. Additionally, FULL mode simplifies the refresh logic
(avoiding complex multi-source delta algebra) while remaining fast because the
table is small and updates are infrequent (only triggered when L1 feeds new
data).

pg_trickle's diagnostic system (`recommend_refresh_mode()`) confirms this
choice: it observed an avg change ratio of 1.0 and recommended to KEEP FULL.

The L3 tables (`alert_summary`, `top_risky_merchants`) are purely
single-upstream aggregates over `risk_scores` and therefore support
DIFFERENTIAL efficiently — the change ratio there is much lower.

### Why `schedule => 'calculated'` for L2 and L3?

`calculated` means "refresh whenever an upstream stream table has new data."
This is the right choice for derived layers: there is no point refreshing
`risk_scores` if neither `user_velocity` nor `merchant_stats` has changed, and
there is no point waiting for a clock interval when they have.

### Why triggers and not logical replication?

The demo uses the default CDC mode (row-level AFTER triggers). This works with
any PostgreSQL 18 installation out of the box — no replication slot
configuration, no `wal_level = logical` requirement. For production deployments
with very high write throughput, WAL-based CDC is more efficient. pg_trickle
can switch modes transparently; see [CONFIGURATION.md](CONFIGURATION.md) for
details.

### Empirical optimization: FULL vs DIFFERENTIAL by change ratio

The demo illustrates a practical rule of thumb: when a stream table's **change
ratio** (fraction of output rows that are inserted or deleted per refresh cycle)
is high (>0.5), FULL mode is often faster than DIFFERENTIAL because the delta
overhead dominates the benefit. Use `pgtrickle.recommend_refresh_mode(table_name)`
to check — it analyzes actual refresh history and recommends the best mode with
confidence scores.

The Refresh Mode Advisor computes change ratio as:

```
change_ratio = (rows_inserted + rows_deleted) / max(reltuples, 1)
```

where `reltuples` is the stream table's current row count from `pg_class`. This
gives a meaningful fraction: 0.07 means 7% of output rows changed last cycle;
1.0 means the entire output turned over.

The two showcase tables make this concrete:

| Table | Change ratio | Advisor says |
|-------|-------------|--------------|
| `merchant_tier_stats` | ≈ 0.07 | ✓ KEEP DIFFERENTIAL |
| `top_10_risky_merchants` | ≈ 0.25 | ✓ KEEP DIFFERENTIAL |
| `risk_scores` | ≈ 1.0 | KEEP FULL (append-only source) |
| `alert_summary` | ≈ 1.0 | KEEP FULL (small table; delta overhead dominates) |

---

## Scenario: ecommerce (default)

The e-commerce scenario models a real-time **online store analytics pipeline**
with orders streaming in continuously.

```bash
cd demo
DEMO_SCENARIO=ecommerce docker compose up --build
```

### Source Data

| Table | Contents |
|-------|----------|
| `customers` | 30 customers with name and country |
| `products` | 15 products across 8 categories (Electronics, Clothing, Sports, etc.) |
| `categories` | 8 product categories |
| `orders` | The live stream — the generator inserts here continuously |
| `product_catalog` | Slowly-changing current price per product; the generator reprices one product every ~30 cycles |

`orders` is the only table that grows continuously. `product_catalog` changes
occasionally (about one row per minute). Everything else is static.

### Normal vs. Flash Sale Traffic

**Normal orders** — a random customer orders a random product in quantity 1–2 at
roughly the catalog price (±15%). Inserted at roughly one per second.

**Flash sale burst** — every ~45 seconds, the generator picks one category and
fires 8–18 rapid orders (0.10–0.35 s apart) at a 70–90% discount. This creates
a visible revenue spike in the Category Revenue panel.

### The DAG of Stream Tables (ecommerce)

All six stream tables are defined in
[demo/postgres/ecommerce/02_stream_tables.sql](../demo/postgres/ecommerce/02_stream_tables.sql).

```
  Base tables          Layer 1 — Silver           Layer 2 — Gold         Layer 3 — Platinum
  ────────────         ──────────────────────      ─────────────────────  ──────────────────────

  ┌────────────┐       ┌──────────────────┐
  │ customers  │──────►│ customer_stats   │──────────────────────────────►┌──────────────────┐
  └────────────┘       │  DIFFERENTIAL 1s │                               │ country_revenue  │
                       └──────────────────┘                               │  DIFF, calc      │
                                │                                          └──────────────────┘
  ┌────────────┐                │
  │  orders    │────────────────┘
  │ (stream)   │────────────────────────────────►┌────────────────┐
  └────────────┘       ┌────────────────┐         │ product_sales  │
  ┌────────────┐       │category_revenue│         │ DIFFERENTIAL   │
  │  products  │──────►│ DIFFERENTIAL   │         │ 1s             │
  │ categories │──────►│ 1s             │         └────────────────┘
  └─────┬──────┘       └────────────────┘
        │
  ┌─────▼──────────────┐   ┌──────────────────────┐
  │  product_catalog   │──►│ catalog_price_impact │  ← DIFFERENTIAL SHOWCASE #1
  │  (slowly-changing) │   │   DIFFERENTIAL 5s    │    change ratio ≈ 0.07
  └────────────────────┘   └──────────────────────┘

  ┌──────────────────┐   ┌──────────────────┐
  │  customer_stats  │──►│  top_10_customers│  ← DIFFERENTIAL SHOWCASE #2
  │  DIFFERENTIAL 1s │   │  DIFFERENTIAL    │    change ratio ≈ 0.1–0.2
  └──────────────────┘   │  calc, LIMIT 10  │
                         └──────────────────┘
```

### Stream Tables (ecommerce)

| Name | Layer | Mode | Schedule | What it computes |
|------|-------|------|----------|-----------------|
| `product_sales` | L1 | DIFFERENTIAL | 1 s | Per-product: units sold, revenue, avg selling price |
| `customer_stats` | L1 | DIFFERENTIAL | 1 s | Per-customer: order count, total spent, avg order value |
| `category_revenue` | L1 | DIFFERENTIAL | 1 s | Per-category: orders, units, revenue |
| `country_revenue` | L2 | DIFFERENTIAL | calculated | Per-country: roll-up from customer_stats |
| `catalog_price_impact` | showcase | DIFFERENTIAL | 5 s | Per-product: current vs. base price delta |
| `top_10_customers` | showcase | DIFFERENTIAL | calculated | Top 10 customers by total spend (LIMIT 10) |

### Differential efficiency showcases (ecommerce)

**`catalog_price_impact`** — Showcase #1: slowly-changing price catalog

Joins `products` (static) with `product_catalog` (15 rows, one repriced per
~30 cycles). Only the repriced product's row changes each cycle:

- Change ratio ≈ 1/15 ≈ 0.07
- Refresh Mode Advisor recommendation: ✓ KEEP DIFFERENTIAL
- The `price_delta` and `pct_change` columns highlight repriced products in real time.

**`top_10_customers`** — Showcase #2: fixed-cardinality leaderboard

Reads `customer_stats` (all 30 customers) and applies `LIMIT 10`. Only rank
boundary crossings produce net output changes:

- Change ratio ≈ 0.1–0.2
- Refresh Mode Advisor recommendation: ✓ KEEP DIFFERENTIAL

### Exploring the Database (ecommerce)

```bash
docker compose exec postgres psql -U demo -d ecommerce_demo
```

```sql
-- See category revenue live
SELECT category, order_count, units_sold, revenue
FROM   category_revenue ORDER BY revenue DESC;

-- Top 10 customers leaderboard
SELECT * FROM top_10_customers;

-- Price changes vs. base price
SELECT product_name, base_price, current_price, pct_change
FROM   catalog_price_impact ORDER BY ABS(pct_change) DESC;

-- Refresh efficiency comparison across all stream tables
SELECT pgt_name, avg_diff_ms, diff_speedup, avg_change_ratio
FROM   pgtrickle.refresh_efficiency() ORDER BY pgt_name;
```
