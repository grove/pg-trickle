-- pg_trickle 0.49.1 -> 0.50.0 upgrade migration
--
-- v0.50.0 — Performance, Security & Operational Hardening
--
-- This release contains no SQL schema changes. All changes are internal:
--   PERF-10-01: Batch preflight source-table existence check (src/refresh/merge/mod.rs)
--   PERF-10-02: CDC trigger SQL string-building optimisation (src/cdc.rs)
--   PERF-10-03: Single-query watermark computation confirmed and documented (src/cdc.rs)
--   SEC-10-01:  Replace manual dblink escaping with pg_quote_literal (src/citus.rs)
--   OPS-10-01:  CNPG preStop lifecycle drain hook (cnpg/cluster-production.yaml)
--   OPS-10-02:  Prometheus reliability counters — new pgtrickle.reliability_counters()
--               function added to src/monitor.rs; new shared-memory atomics in src/shmem.rs
--   OPS-10-03:  Docker base-image digest pinning (Dockerfile.demo, Dockerfile.ghcr,
--               tests/Dockerfile.e2e)
--   SCAL-10-01: Invalidation ring capacity documentation (docs/CONFIGURATION.md)
--   COR-10-01:  Deep join chain threshold documentation (docs/CONFIGURATION.md)
--
-- OPS-10-02: New monitoring function — must be explicitly created on upgrade.
-- (pgrx does not auto-register new functions via ALTER EXTENSION UPDATE.)

-- pg_trickle::monitor::reliability_counters
CREATE OR REPLACE FUNCTION pgtrickle."reliability_counters"() RETURNS TABLE (
        "invalidation_ring_overflows" bigint,
        "dag_cycles_detected" bigint,
        "template_cache_stale_evictions" bigint
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reliability_counters_wrapper';
