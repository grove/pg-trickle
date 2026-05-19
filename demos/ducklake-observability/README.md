# DuckLake Observability in a Box

A pre-packaged monitoring stack for DuckLake deployments. Five minutes from
`git clone` to a production-quality Grafana dashboard with real-time alerts
for compaction needs, snapshot rate spikes, and storage growth anomalies.

---

## What It Does

Attaches to an **existing** DuckLake PostgreSQL catalog and creates pg_trickle
stream tables over DuckLake's metadata tables. A Grafana instance reads the
stream tables and displays live operational dashboards.

```
Your DuckLake PostgreSQL catalog
  │
  │  (pg_trickle stream tables watch DuckLake metadata tables)
  ├─ ducklake_small_file_counts   (1m schedule, DIFFERENTIAL)
  ├─ ducklake_snapshot_rate       (30s schedule, DIFFERENTIAL)
  ├─ ducklake_storage_growth      (5m schedule, DIFFERENTIAL)
  ├─ ducklake_tenant_activity     (5m schedule, DIFFERENTIAL)
  └─ ducklake_compaction_events   (5m schedule, DIFFERENTIAL)
  │
Grafana
  │  Dashboards: Overview, Small Files, Snapshot Rate, Storage Growth
  │  Alerts: small_file_count > 50, snapshot_rate > 60/min, storage quota
```

---

## Quick Start

### 1. Configure the Connection

```bash
cd demos/ducklake-observability
cp .env.example .env
$EDITOR .env   # set DATABASE_URL to your DuckLake PostgreSQL instance
```

### 2. Initialise the Stream Tables

```bash
psql "$DATABASE_URL" -f init_monitoring.sql
```

### 3. Start Grafana

```bash
docker compose up -d grafana
```

Open http://localhost:3100 → **Dashboards → DuckLake Observability**.

---

## Connecting to an Existing DuckLake Instance

The `.env` file should point to your existing DuckLake PostgreSQL catalog
database — not a new container. The `init_monitoring.sql` script creates
the stream tables in that database.

```bash
# .env
DATABASE_URL=postgresql://myuser:mypassword@myhost:5432/lake_catalog
GRAFANA_PORT=3100
```

> **Note:** pg_trickle must be installed in the target database:
> `CREATE EXTENSION pg_trickle;`

---

## Stream Tables Installed by `init_monitoring.sql`

| Stream Table | Source | Refresh | Description |
|-------------|--------|---------|-------------|
| `ducklake_small_file_counts` | `ducklake_data_file` | 1 min | Small files (<10 MB) per table |
| `ducklake_snapshot_rate` | `ducklake_snapshot`, `ducklake_table_changes` | 30 sec | Commits per minute per table |
| `ducklake_storage_growth` | `ducklake_data_file` | 5 min | Active bytes per table |
| `ducklake_tenant_activity` | `ducklake_snapshot`, `ducklake_table_changes` | 5 min | Write volume per schema/tenant |
| `ducklake_compaction_events` | `ducklake_data_file` | 5 min | Files compacted per event |

---

## Grafana Dashboards

The dashboard pack includes four pre-built panels:

**Overview Dashboard**
- Total active files across all tables
- Total storage in use (active Parquet files)
- Latest snapshot ID
- Tables with small-file warnings

**Small Files Dashboard**
- Small-file count per table (time series + current value)
- Alert: count > 50 → WARNING, count > 200 → CRITICAL
- Recommended compaction target: tables ordered by small_file_count DESC

**Snapshot Rate Dashboard**
- Commits per minute per table (time series, stacked)
- Alert: any table > 60 commits/minute

**Storage Growth Dashboard**
- Cumulative active bytes per table (time series)
- Storage growth rate (bytes/hour, computed by Grafana)

---

## Alerts Configuration

Edit `grafana/provisioning/alerting/rules.yml` to configure notification
channels. By default, alerts fire to the default Grafana notification channel.

Predefined alert rules:
- `ducklake_small_files_warning`: any table > 50 small files
- `ducklake_small_files_critical`: any table > 200 small files
- `ducklake_snapshot_rate_warning`: any table > 60 commits/minute
- `ducklake_storage_growth_warning`: projected to exceed `STORAGE_QUOTA_GB` in 7 days

---

## Standalone Mode (No Docker)

If you already have a Grafana instance, skip Docker and import the dashboards
manually:

```bash
# Install stream tables only
psql "$DATABASE_URL" -f init_monitoring.sql

# Import Grafana dashboards
for f in grafana/dashboards/*.json; do
    curl -s -X POST "$GRAFANA_URL/api/dashboards/import" \
        -H "Content-Type: application/json" \
        -u admin:admin \
        -d @"$f" | jq '.status'
done
```

---

## Architecture

This demo does not use a separate `postgres` container. It connects to your
existing DuckLake PostgreSQL catalog. Only the `grafana` container is required.

```
Your DuckLake PostgreSQL ← init_monitoring.sql installs stream tables here
      │
      └── Grafana container (reads stream tables via postgres datasource)
```

---

## Teardown

To remove the stream tables from your DuckLake database:

```bash
psql "$DATABASE_URL" -f teardown_monitoring.sql
```

The teardown script drops all stream tables created by `init_monitoring.sql`.

---

## Related Resources

- [Tutorial: Monitoring Your DuckLake with pg_trickle](../../blog/ducklake-monitoring.md)
- [Blog: Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM](../../blog/ducklake-ivm-missing-piece.md)
- [Demo: The Five-Second Funnel](../ducklake-funnel/README.md)
