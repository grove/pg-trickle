-- pg_trickle 0.61.0 -> 0.62.0 upgrade migration
--
-- v0.62.0 — Scheduler Fan-out Cache, Per-Node Pause/Resume & Stream-Table Spec
--
-- Changes in this release:
--
--   PERF-1: Change-buffer fan-out deduplication.
--     The scheduler now issues ONE batched EXISTS query per tick across all
--     stream tables sharing the same source table, eliminating O(N) redundant
--     SPI round-trips.  Controlled by the new GUC
--     pg_trickle.enable_change_buffer_fanout (default on).
--     No SQL schema changes.
--
--   API-1/2: Per-node scheduler pause/resume.
--     New functions:
--       pgtrickle.pause_scheduler(nodes text[]) -> text
--       pgtrickle.resume_scheduler(nodes text[]) -> text
--     Uses shared-memory flags; scheduler skips paused nodes every tick.
--     Drain timeout controlled by the new GUC
--     pg_trickle.scheduler_drain_timeout (default 30 s).
--
--   API-3: stream_table_spec().
--     New overloaded function returning a jsonb spec for a stream table:
--       pgtrickle.stream_table_spec(relid oid)      -> jsonb
--       pgtrickle.stream_table_spec(qualified_name text) -> jsonb
--

-- API-1 (v0.62.0): pause_scheduler(nodes text[]) — per-node pause.
CREATE OR REPLACE FUNCTION pgtrickle."pause_scheduler"(
        "nodes" TEXT[] /* pgrx::Array<&str> */
) RETURNS TEXT /* &'static str */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pause_scheduler_wrapper';

COMMENT ON FUNCTION pgtrickle."pause_scheduler"(TEXT[]) IS
    'API-1 (v0.62.0): Mark the named stream tables as paused. '
    'The scheduler will skip dispatching refreshes for each node until '
    'pgtrickle.resume_scheduler() is called. '
    'Waits up to pg_trickle.scheduler_drain_timeout seconds for any '
    'in-flight refresh workers to drain before returning.';

-- API-2 (v0.62.0): resume_scheduler(nodes text[]) — per-node resume.
CREATE OR REPLACE FUNCTION pgtrickle."resume_scheduler"(
        "nodes" TEXT[] /* pgrx::Array<&str> */
) RETURNS TEXT /* &'static str */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'resume_scheduler_wrapper';

COMMENT ON FUNCTION pgtrickle."resume_scheduler"(TEXT[]) IS
    'API-2 (v0.62.0): Remove the named stream tables from the paused set. '
    'The scheduler will resume dispatching refreshes on the next tick.';

-- API-3 (v0.62.0): stream_table_spec(oid) — JSON metadata for a stream table.
CREATE OR REPLACE FUNCTION pgtrickle."stream_table_spec"(
        "relid" OID /* pgrx::pg_sys::Oid */
) RETURNS JSONB /* Option<pgrx::JsonB> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'stream_table_spec_wrapper';

COMMENT ON FUNCTION pgtrickle."stream_table_spec"(OID) IS
    'API-3 (v0.62.0): Return a jsonb specification object for the stream table '
    'identified by the given OID. Returns NULL if the OID is not a stream table.';

-- API-3 (v0.62.0): stream_table_spec(text) — JSON metadata by qualified name.
CREATE OR REPLACE FUNCTION pgtrickle."stream_table_spec"(
        "qualified_name" TEXT /* &str */
) RETURNS JSONB /* Option<pgrx::JsonB> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'stream_table_spec_by_name_wrapper';

COMMENT ON FUNCTION pgtrickle."stream_table_spec"(TEXT) IS
    'API-3 (v0.62.0): Return a jsonb specification object for the stream table '
    'identified by the given qualified name (schema.table or table). '
    'Returns NULL if no matching stream table is found.';
