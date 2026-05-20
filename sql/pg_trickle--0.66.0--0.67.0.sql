-- pg_trickle 0.66.0 -> 0.67.0 upgrade migration
--
-- v0.67.0 — DuckLake Phase 3b: View Registration, Provenance & Ecosystem
--
-- Changes in this release:
--
--   F-6: DuckLake view registration
--     - When a stream table with sink => 'ducklake' is created or altered,
--       pg_trickle upserts a row in `ducklake_view` so results are visible
--       to every DuckLake client as a native catalog object.
--     - When the stream table is dropped, the `ducklake_view` row is removed
--       in the same transaction.
--
--   INT-11: Snapshot provenance & audit trails
--     - New catalog table `pgtrickle.pgt_ducklake_provenance` records which
--       stream table produced each DuckLake snapshot.
--     - `ducklake_snapshot.created_by` is now populated with a structured
--       identifier: `pg_trickle/<version>/stream_table/<oid>/<name>`.

-- ── SCHEMA-1: Add pgt_ducklake_provenance table ───────────────────────────

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_ducklake_provenance (
    provenance_id        BIGSERIAL PRIMARY KEY,
    stream_table_oid     BIGINT NOT NULL,
    stream_table_name    TEXT NOT NULL,
    ducklake_snapshot_id BIGINT NOT NULL,
    refresh_id           BIGINT NOT NULL DEFAULT 0,
    delta_row_count      BIGINT NOT NULL DEFAULT 0,
    written_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE pgtrickle.pgt_ducklake_provenance IS
    'INT-11 (v0.67.0): Audit trail linking each DuckLake snapshot to the '
    'pg_trickle stream table that produced it. One row per successful '
    'DuckLake sink run. Enables end-to-end lineage from raw PostgreSQL '
    'events through the differential computation to the Parquet file on '
    'object storage.';

COMMENT ON COLUMN pgtrickle.pgt_ducklake_provenance.stream_table_oid IS
    'OID (pgt_id) of the producing stream table.';
COMMENT ON COLUMN pgtrickle.pgt_ducklake_provenance.stream_table_name IS
    'Human-readable name of the producing stream table.';
COMMENT ON COLUMN pgtrickle.pgt_ducklake_provenance.ducklake_snapshot_id IS
    'The DuckLake snapshot_id recorded in ducklake_snapshot.';
COMMENT ON COLUMN pgtrickle.pgt_ducklake_provenance.refresh_id IS
    'pg_trickle internal refresh sequence number (pgt_refresh_history.refresh_id). '
    '0 when no refresh history row exists yet.';
COMMENT ON COLUMN pgtrickle.pgt_ducklake_provenance.delta_row_count IS
    'Number of rows in the Parquet delta that produced this snapshot.';
COMMENT ON COLUMN pgtrickle.pgt_ducklake_provenance.written_at IS
    'Timestamp when this provenance row was written.';

-- ── SCHEMA-2: Indexes for provenance queries ──────────────────────────────

CREATE INDEX IF NOT EXISTS idx_provenance_st_oid
    ON pgtrickle.pgt_ducklake_provenance (stream_table_oid, written_at DESC);

CREATE INDEX IF NOT EXISTS idx_provenance_snapshot
    ON pgtrickle.pgt_ducklake_provenance (ducklake_snapshot_id);

-- ── SCHEMA-3: Include provenance in pg_dump ───────────────────────────────

SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_ducklake_provenance', '');

-- ── SCHEMA-4: Record this migration ───────────────────────────────────────

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES ('0.67.0', 'DuckLake Phase 3b: view registration (F-6), snapshot provenance (INT-11)')
ON CONFLICT (version) DO NOTHING;
