# The Five-Second Funnel — DuckLake + pg_trickle Demo

A self-contained `docker compose up` demo that streams synthetic e-commerce
events into DuckLake, computes a live visit→add-to-cart→purchase funnel using
pg_trickle stream tables, and displays it in a Grafana dashboard that refreshes
every five seconds.

Designed to run on any developer laptop in under five minutes.

---

## What It Does

```
DuckDB load generator
  │  (writes 50 events/second to events_bridge via psql)
  ▼
PostgreSQL 18 + pg_trickle
  │  events_bridge table — trigger-based CDC, sub-millisecond
  ├─ stream table: revenue_by_minute   (5s schedule, DIFFERENTIAL)
  └─ stream table: funnel_by_product   (5s schedule, DIFFERENTIAL)
  ▼
Grafana
  │  live panels: funnel bars, revenue time series, top products
  │  auto-refresh: 5s
```

No Kafka. No Flink. No separate streaming infrastructure.

---

## Quick Start

```bash
cd demos/ducklake-funnel
docker compose up
```

Then open http://localhost:3000 in your browser.
- Username: `admin`
- Password: `admin`

The dashboard appears under **Dashboards → Five-Second Funnel**.

---

## Architecture

The demo uses four Docker containers:

| Container | Image | Purpose |
|-----------|-------|---------|
| `postgres` | `ghcr.io/trickle-labs/pg-trickle:latest` | PostgreSQL 18 + pg_trickle |
| `generator` | `python:3.12-alpine` (built in compose) | Load generator (50 events/s) |
| `grafana` | `grafana/grafana:latest` | Dashboard visualisation |
| `minio` | `minio/minio` | S3-compatible object storage (simulates DuckLake data path) |

The `postgres` container runs PostgreSQL with pg_trickle installed and initialised
with the stream tables. The `generator` container runs a Python script that
inserts synthetic events at configurable rates. Grafana reads from PostgreSQL
directly via the built-in PostgreSQL data source.

---

## Configuration

Edit `.env` to change default settings:

```bash
# Event generation rate (events per second, default 50)
EVENTS_PER_SECOND=50

# Number of products in the catalogue (default 20)
PRODUCT_COUNT=20

# Number of simulated users (default 500)
USER_COUNT=500

# Grafana port (default 3000)
GRAFANA_PORT=3000

# PostgreSQL port (default 5432)
POSTGRES_PORT=5432
```

---

## Included Grafana Panels

| Panel | Stream Table | Refresh |
|-------|-------------|---------|
| Revenue per minute | `revenue_by_minute` | 5s |
| Conversion funnel (all products) | `funnel_by_product` | 5s |
| Top 10 products by purchases | `funnel_by_product` | 5s |
| Conversion rate by product | `funnel_by_product` | 5s |
| Recent purchases (live feed) | `events_bridge` | 5s |

---

## Stopping the Demo

```bash
docker compose down -v
```

The `-v` flag removes the PostgreSQL data volume so the next `docker compose up`
starts fresh.

---

## What This Demonstrates

- **pg_trickle DIFFERENTIAL refresh:** The funnel numbers update within 5 seconds
  of each new event batch. Only the changed rows are processed — not the entire
  event history.
- **DuckLake integration pattern:** The `events_bridge` table mirrors the write
  pattern that DuckLake uses for inlined data. The same stream tables work
  without modification once the real DuckLake connector is in place.
- **Zero-infrastructure overhead:** Five containers, five minutes, a complete
  analytics pipeline. No message broker, no stream processor, no JVM.

---

## Related Resources

- [Tutorial: Real-Time Dashboards on Your Data Lake](../../blog/ducklake-real-time-dashboards.md)
- [Tutorial: The Modern Data Stack in One Box](../../blog/ducklake-modern-data-stack.md)
- [Blog: Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM](../../blog/ducklake-ivm-missing-piece.md)
