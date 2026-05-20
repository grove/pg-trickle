# Demo C: Multi-Engine Leaderboard

A self-contained `docker compose up` demo that runs a real-time game leaderboard
simultaneously maintained as a pg_trickle stream table in PostgreSQL **and**
published as a DuckLake table on MinIO.

The same data — maintained once by pg_trickle — is available to both operational
and analytical workloads without duplication.

---

## What It Does

```
Score generator (Python)
  │  50 score events/second
  ▼
PostgreSQL 18 + pg_trickle
  ├─ stream table: top_players   (5s DIFFERENTIAL refresh)
  │    └─ DuckLake sink → MinIO Parquet deltas
  ├─ stream table: scores_by_game (5s DIFFERENTIAL refresh)
  │    └─ DuckLake sink → MinIO Parquet deltas
  └─ DuckLake view registration: top_players, scores_by_game
  ▼
┌─────────────────────┬───────────────────────────────┐
│  Grafana (port 3000) │  DuckDB notebook (port 8888)  │
│  Live leaderboard    │  Historical analytics          │
│  (reads from PG)     │  (reads from DuckLake/MinIO)   │
└─────────────────────┴───────────────────────────────┘
```

---

## Quick Start

```bash
cd demos/ducklake-leaderboard
docker compose up
```

- **Live leaderboard**: http://localhost:3000 (Grafana, admin/admin)
- **Analytics notebook**: http://localhost:8888 (JupyterLab, token: `demo`)
- **MinIO console**: http://localhost:9001 (minioadmin/minioadmin)

---

## What to Watch

1. **Grafana**: The top-players panel refreshes every 5 seconds. Watch scores
   change in real time as the generator inserts events.

2. **MinIO console**: Navigate to the `pg-trickle-demo` bucket. New Parquet
   files appear under `top_players/` and `scores_by_game/` every 5 seconds.

3. **JupyterLab**: Open `leaderboard_analysis.ipynb` and run all cells. DuckDB
   queries the historical Parquet files and plots ranking trends over time.

---

## Services

| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL 18 + pg_trickle | 5432 | Stream tables, DuckLake catalog |
| MinIO | 9000/9001 | Object storage (S3-compatible) |
| Grafana | 3000 | Live leaderboard dashboard |
| JupyterLab | 8888 | Historical analytics (DuckDB) |
| Score generator | — | Python script producing 50 events/s |

---

## Key SQL

The leaderboard stream table:

```sql
SELECT pgtrickle.create_stream_table(
    'top_players',
    query => $$
        SELECT
            player_id,
            player_name,
            SUM(score) AS total_score,
            COUNT(*) AS games_played,
            RANK() OVER (ORDER BY SUM(score) DESC) AS rank
        FROM game_scores
        GROUP BY player_id, player_name
    $$,
    schedule           => '5s',
    refresh_mode       => 'DIFFERENTIAL',
    sink               => 'ducklake',
    ducklake_sink_path => 's3://pg-trickle-demo/top_players/'
);
```

After creation, pg_trickle automatically registers `top_players` as a native
view in the DuckLake catalog — so the JupyterLab notebook can query it from
DuckDB with a simple `SELECT * FROM lake.top_players`.

---

## Provenance trail

Query the lineage from PostgreSQL:

```sql
SELECT
    stream_table_name,
    ducklake_snapshot_id,
    delta_row_count,
    written_at
FROM pgtrickle.pgt_ducklake_provenance
ORDER BY written_at DESC
LIMIT 10;
```
