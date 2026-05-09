# pg_trickle Roadmap

> **Audience:** Product managers, stakeholders, and technically curious readers
> who want to understand what each release delivers and why it matters —
> without needing to read Rust code or SQL specifications.

## Versions

### Foundation (v0.1.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.1.0](roadmap/v0.1.0.md) | The complete foundation — differential engine, CDC, scheduling, monitoring | ✅ Released | Very Large | [Full details](roadmap/v0.1.x.md-full.md) |
| [v0.1.1](roadmap/v0.1.1.md) | Change capture correctness fixes (WAL decoder, UPDATE handling) | ✅ Released | Patch | [Full details](roadmap/v0.1.x.md-full.md) |
| [v0.1.2](roadmap/v0.1.2.md) | DDL tracking improvements and PgBouncer compatibility | ✅ Released | Patch | [Full details](roadmap/v0.1.x.md-full.md) |
| [v0.1.3](roadmap/v0.1.3.md) | SQL coverage completion, WAL hardening, TPC-H 22/22 | ✅ Released | Patch | [Full details](roadmap/v0.1.x.md-full.md) |

### Early Feature Development (v0.2.x – v0.5.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.2.0](roadmap/v0.2.0.md) | Top-N views, IMMEDIATE refresh mode, diamond dependency safety | ✅ Released | Medium | [Full details](roadmap/v0.2.0.md-full.md) |
| [v0.2.1](roadmap/v0.2.1.md) | Upgrade infrastructure and documentation expansion | ✅ Released | Small | [Full details](roadmap/v0.2.1.md-full.md) |
| [v0.2.2](roadmap/v0.2.2.md) | Paginated top-N, AUTO mode default, ALTER QUERY | ✅ Released | Medium | [Full details](roadmap/v0.2.2.md-full.md) |
| [v0.2.3](roadmap/v0.2.3.md) | Non-determinism detection and operational polish | ✅ Released | Small | [Full details](roadmap/v0.2.3.md-full.md) |
| [v0.3.0](roadmap/v0.3.0.md) | Correctness for HAVING, FULL OUTER JOIN, and correlated subqueries | ✅ Released | Medium | [Full details](roadmap/v0.3.0.md-full.md) |
| [v0.4.0](roadmap/v0.4.0.md) | Parallel refresh, statement-level CDC triggers, cross-source consistency | ✅ Released | Medium | [Full details](roadmap/v0.4.0.md-full.md) |
| [v0.5.0](roadmap/v0.5.0.md) | Row-level security, ETL bootstrap gating, API polish | ✅ Released | Medium | [Full details](roadmap/v0.5.0.md-full.md) |

### Scalability and Robustness (v0.6.x – v0.9.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.6.0](roadmap/v0.6.0.md) | Partitioned source tables, idempotent DDL, circular dependency foundation | ✅ Released | Medium | [Full details](roadmap/v0.6.0.md-full.md) |
| [v0.7.0](roadmap/v0.7.0.md) | Circular DAG execution, watermarks, Prometheus/Grafana observability | ✅ Released | Large | [Full details](roadmap/v0.7.0.md-full.md) |
| [v0.8.0](roadmap/v0.8.0.md) | pg_dump backup support and multiset invariant testing | ✅ Released | Small | [Full details](roadmap/v0.8.0.md-full.md) |
| [v0.9.0](roadmap/v0.9.0.md) | Algebraic aggregate maintenance — AVG, STDDEV, COUNT(DISTINCT) | ✅ Released | Medium | [Full details](roadmap/v0.9.0.md-full.md) |

### Production Readiness (v0.10.x – v0.14.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.10.0](roadmap/v0.10.0.md) | DVM hardening, PgBouncer compatibility, "No Surprises" UX | ✅ Released | Medium | [Full details](roadmap/v0.10.0.md-full.md) |
| [v0.11.0](roadmap/v0.11.0.md) | Partitioned stream tables, event-driven scheduler (34× latency), circuit breaker | ✅ Released | Large | [Full details](roadmap/v0.11.0.md-full.md) |
| [v0.12.0](roadmap/v0.12.0.md) | Three-table join fix (EC-01), developer tools, SQLancer fuzzing | ✅ Released | Medium | [Full details](roadmap/v0.12.0.md-full.md) |
| [v0.13.0](roadmap/v0.13.0.md) | Columnar change tracking, shared buffers, TPC-H 22/22 DIFFERENTIAL | ✅ Released | Large | [Full details](roadmap/v0.13.0.md-full.md) |
| [v0.14.0](roadmap/v0.14.0.md) | Tiered scheduling, UNLOGGED buffers, diagnostics | ✅ Released | Medium | [Full details](roadmap/v0.14.0.md-full.md) |

### Performance and Integration (v0.15.x – v0.19.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.15.0](roadmap/v0.15.0.md) | Nexmark benchmark, bulk create API, watermark hold-back, dbt Hub | ✅ Released | Medium | [Full details](roadmap/v0.15.0.md-full.md) |
| [v0.16.0](roadmap/v0.16.0.md) | Append-only fast path, algebraic aggregates, auto-indexing, benchmark CI | ✅ Released | Medium | [Full details](roadmap/v0.16.0.md-full.md) |
| [v0.17.0](roadmap/v0.17.0.md) | Cost-based refresh strategy, incremental DAG rebuild, pg_ivm migration guide | ✅ Released | Large | [Full details](roadmap/v0.17.0.md-full.md) |
| [v0.18.0](roadmap/v0.18.0.md) | Z-set delta engine, consistency enforcement, safety hardening | ✅ Released | Large | [Full details](roadmap/v0.18.0.md-full.md) |
| [v0.19.0](roadmap/v0.19.0.md) | Security hardening, packaging (PGXN, Docker Hub, apt/rpm) | ✅ Released | Medium | [Full details](roadmap/v0.19.0.md-full.md) |

### Self-Monitoring and Deep Correctness (v0.20.x – v0.27.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.20.0](roadmap/v0.20.0.md) | pg_trickle monitors itself using its own stream tables | ✅ Released | Large | [Full details](roadmap/v0.20.0.md-full.md) |
| [v0.21.0](roadmap/v0.21.0.md) | Correctness hardening, zero-crash guarantee, shadow/canary mode | ✅ Released | Large | [Full details](roadmap/v0.21.0.md-full.md) |
| [v0.22.0](roadmap/v0.22.0.md) | Downstream CDC publication, parallel refresh pool, SLA tier auto-assignment | ✅ Released | Large | [Full details](roadmap/v0.22.0.md-full.md) |
| [v0.23.0](roadmap/v0.23.0.md) | TPC-H DVM scaling performance — all 22 queries at O(Δ) | ✅ Released | Large | [Full details](roadmap/v0.23.0.md-full.md) |
| [v0.24.0](roadmap/v0.24.0.md) | Join correctness complete fix, two-phase frontier, TOAST-aware CDC | ✅ Released | Large | [Full details](roadmap/v0.24.0.md-full.md) |
| [v0.25.0](roadmap/v0.25.0.md) | Thousands of stream tables, pooler cold-start fix, predictive model | ✅ Released | Large | [Full details](roadmap/v0.25.0.md-full.md) |
| [v0.26.0](roadmap/v0.26.0.md) | Concurrency testing, fuzz targets, refresh engine modularisation | ✅ Released | Large | [Full details](roadmap/v0.26.0.md-full.md) |
| [v0.27.0](roadmap/v0.27.0.md) | Snapshot/PITR, schedule recommendations, cluster observability | ✅ Released | Medium | [Full details](roadmap/v0.27.0.md-full.md) |

### Toward Stable (v0.28.x – v1.0)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.28.0](roadmap/v0.28.0.md) | Reliable event messaging built into PostgreSQL | ✅ Released | Large | [Full details](roadmap/v0.28.0.md-full.md) |
| [v0.29.0](roadmap/v0.29.0.md) | Off-the-shelf connector to Kafka, NATS, SQS, and more | ✅ Released | Large | [Full details](roadmap/v0.29.0.md-full.md) |
| [v0.30.0](roadmap/v0.30.0.md) | Quality gate before 1.0 — correctness, stability, and docs | ✅ Released | Medium | [Full details](roadmap/v0.30.0.md-full.md) |
| [v0.31.0](roadmap/v0.31.0.md) | Smarter scheduling and faster hot paths | ✅ Released | Medium | [Full details](roadmap/v0.31.0.md-full.md) |
| [v0.32.0](roadmap/v0.32.0.md) | Citus: stable object naming and per-source frontier foundation | ✅ Released | Medium | [Full details](roadmap/v0.32.0.md-full.md) |
| [v0.33.0](roadmap/v0.33.0.md) | Citus: world-class distributed source CDC and stream table support | ✅ Released | Large | [Full details](roadmap/v0.33.0.md-full.md) |
| [v0.34.0](roadmap/v0.34.0.md) | Citus: automated distributed CDC scheduler wiring and shard rebalance auto-recovery | ✅ Released | Medium | [Full details](roadmap/v0.34.0.md-full.md) |
| [v0.35.0](roadmap/v0.35.0.md) | EC-01 correctness closeout, Citus chaos hardening, reactive subscriptions, zero-downtime schema changes | ✅ Released | Large | [Full details](roadmap/v0.35.0.md-full.md) |
| [v0.36.0](roadmap/v0.36.0.md) | Structural hardening, L0 cache, WAL backpressure, temporal IVM, columnar storage | ✅ Released | Large | [Full details](roadmap/v0.36.0.md-full.md) |
| [v0.37.0](roadmap/v0.37.0.md) | Scheduler & merge modularisation, pgVectorMV (vector_avg/sum), OpenTelemetry trace propagation | ✅ Released | Medium | [Full details](roadmap/v0.37.0.md-full.md) |
| [v0.38.0](roadmap/v0.38.0.md) | EC-01 Correctness Sprint (Hard Gate): join phantom rows, property-test convergence proof — BLOCKING release gate | ✅ Released | Medium | [Full details](roadmap/v0.38.0.md-full.md) |
| [v0.39.0](roadmap/v0.39.0.md) | Operational Truthfulness & Distributed Hardening: backpressure/wake fix, generated docs, Citus chaos, SQLSTATE rollout, diagnostics | ✅ Released | Large | [Full details](roadmap/v0.39.0.md-full.md) |
| [v0.40.0](roadmap/v0.40.0.md) | Operator trust and maintainability: generated references, alerting, drain-mode proof, secret hygiene, unsafe gating | ✅ Released | Large | [Full details](roadmap/v0.40.0.md-full.md) |
| [v0.41.0](roadmap/v0.41.0.md) | DVM correctness: structural cache keys, placeholder safety, WAL transition guards | ✅ Released | Medium | [Full details](roadmap/v0.41.0.md-full.md) |
| [v0.42.0](roadmap/v0.42.0.md) | Documentation truthfulness + test quality: repair_stream_table, catalog generator, SQL reference, sleep removal, fuzz CI | ✅ Released | Large | [Full details](roadmap/v0.42.0.md-full.md) |
| [v0.43.0](roadmap/v0.43.0.md) | Performance tunability: deep-join GUCs, GROUP_RESCAN improvement, explain_stream_table diagnostics, D+I change buffer refactor | ✅ Released | Large | [Full details](roadmap/v0.43.0.md-full.md) |
| [v0.44.0](roadmap/v0.44.0.md) | Security hardening: IVM search_path fix, centralized SQL builder, RLS warnings, module decomposition | ✅ Released | Large | [Full details](roadmap/v0.44.0.md-full.md) |
| [v0.45.0](roadmap/v0.45.0.md) | Operational readiness: preflight functions, scalability infrastructure, CI completeness, CNPG production examples | ✅ Released | Large | [Full details](roadmap/v0.45.0.md-full.md) |

### `pg_tide` Extraction (v0.46.0)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.46.0](roadmap/v0.46.0.md) | Extract `pg_tide`: standalone transactional outbox, inbox, and relay into `trickle-labs/pg-tide` | ✅ Released | Large | [Full details](roadmap/v0.46.0.md-full.md) |

### Embedding & AI Programme (v0.47.x – v0.48.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.47.0](roadmap/v0.47.0.md) | Embedding pipeline infrastructure: post-refresh hooks, drift-based reindex, vector monitoring | ✅ Released | Medium | [Full details](roadmap/v0.47.0.md-full.md) |
| [v0.48.0](roadmap/v0.48.0.md) | Complete embedding programme: sparse/half-precision vector aggregates, hybrid search, embedding_stream_table() API, per-tenant ANN, embedding outbox | ✅ Released | Large | [Full details](roadmap/v0.48.0.md-full.md) |

### v1.0 Readiness Arc (v0.49.x – v0.51.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.49.0](roadmap/v0.49.0.md) | Test infrastructure hardening: concurrency synchronization overhaul, 10-module unit test sweep, merge/row_id fuzz targets, DDL-during-refresh E2E, scheduler decomposition, CI smoke breadth | ✅ Released | Large | [Full details](roadmap/v0.49.0.md-full.md) |
| [v0.49.1](roadmap/v0.49.1.md) | Repository migration to trickle-labs/pg-trickle: updated CI/CD, Docker, PGXN, dbt Hub, and CloudNativePG artifact publishing | ✅ Released | Patch | — |
| [v0.50.0](roadmap/v0.50.0.md) | Performance, security & operational hardening: SPI batching in differential refresh, dblink escaping fix, CNPG graceful-drain preStop hook, Docker image digest pinning, invalidation ring observability, deep-join drift monitoring, Prometheus secondary metrics | ✅ Released | Large | [Full details](roadmap/v0.50.0.md-full.md) |
| [v0.51.0](roadmap/v0.51.0.md) | Citus chaos resilience & documentation truth: chaos test rig (node kill/rebalance/partition), deprecated GUC removal, ARCHITECTURE.md pg_tide boundary, recursive CTE strategy docs, CDC-enabled-flag documentation | ✅ Released | Large | [Full details](roadmap/v0.51.0.md-full.md) |

### Beyond v1.0

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v1.0.0](roadmap/v1.0.0.md-full.md) | Stable release — PostgreSQL 19, package registries, signed artifacts, SBOMs, zero breaking changes | Planned | Large | [Full details](roadmap/v1.0.0.md-full.md) |
| [v1.1.0](roadmap/v1.1.0.md-full.md) | PostgreSQL 17 support | Planned | Medium | [Full details](roadmap/v1.1.0.md-full.md) |
| [v1.2.0](roadmap/v1.2.0.md-full.md) | PGlite proof of concept | Planned | Medium | [Full details](roadmap/v1.2.0.md-full.md) |
| [v1.3.0](roadmap/v1.3.0.md-full.md) | Core extraction (`pg_trickle_core`) | Planned | Large | [Full details](roadmap/v1.3.0.md-full.md) |
| [v1.4.0](roadmap/v1.4.0.md-full.md) | PGlite WASM extension | Planned | Medium | [Full details](roadmap/v1.4.0.md-full.md) |
| [v1.5.0](roadmap/v1.5.0.md-full.md) | PGlite reactive integration | Planned | Medium | [Full details](roadmap/v1.5.0.md-full.md) |

## How these versions fit together

```
v0.1.0   ─── Foundation: differential engine, CDC, scheduling, 1300+ tests
    │
v0.2–0.5 ─── TopK, IMMEDIATE mode, RLS, partitioned sources, parallel refresh
    │
v0.6–0.9 ─── Circular DAGs, watermarks, Prometheus, algebraic aggregates
    │
v0.10–14 ─── PgBouncer compat, 34× latency, partitioned outputs, tiered scheduling
    │
v0.15–19 ─── Nexmark, append-only fast path, cost model, security, packaging
    │
v0.20–23 ─── Self-monitoring, zero-crash guarantee, downstream CDC, TPC-H at scale
    │
v0.24–27 ─── Join correctness complete, thousands of STs, snapshot/PITR
    │
v0.28–29 ─── Reliable event messaging (outbox + inbox) + relay CLI
    │
v0.30    ─── Quality gate: correctness, stability, docs (required for 1.0)
    │
v0.31    ─── Scheduler intelligence and performance hot paths
    │
v0.32    ─── Citus: stable naming foundation (additive, safe for all users)
    │
v0.33    ─── Citus: distributed CDC and stream table support
    │
v0.35    ─── EC-01 fix, Citus chaos rig, reactive subscriptions, shadow-ST, relay hardening
    │
v0.36    ─── L0 cache, WAL backpressure, api split, temporal IVM, columnar, RowIdSchema
    │
v0.37    ─── Scheduler split, pgVectorMV, OpenTelemetry, pg_partman compat
    │
v0.38    ─── Correctness closeout and truthfulness: EC-01, RowIdSchema planning, backpressure, wake/docs repair
    │
v0.39    ─── Distributed hardening and diagnostics: Citus chaos, durable CDC hold, TPC-H explain artifacts, fuzzing
    │
v0.40    ─── Operator trust and maintainability: generated docs, alerting, drain proof, secret hygiene, unsafe gating
    │
v0.41    ─── DVM correctness: structural cache keys, placeholder safety, WAL transition guards
    │
v0.42    ─── Docs truthfulness + test quality: repair_stream_table, catalog generator, sleep removal, fuzz CI
    │
v0.43    ─── Performance tunability: deep-join GUCs, GROUP_RESCAN improvement, explain diagnostics, D+I CB refactor
    │
v0.44    ─── Security hardening: IVM search_path, SQL builder, RLS warnings, module decomposition
    │
v0.45    ─── Operational readiness: preflight, scalability, CI completeness, CNPG production
    │
v0.46    ─── Extract pg_tide: standalone outbox/inbox/relay → trickle-labs/pg-tide; attach_outbox() integration
    │
v0.47    ─── Embedding infrastructure: post-refresh actions, drift-based reindex, vector monitoring
    │
v0.48    ─── Complete embedding programme: sparse vectors, hybrid search, embedding_stream_table(), per-tenant ANN
    │
v0.49    ─── Test infrastructure hardening: concurrency sync overhaul, 10-module unit sweep, merge fuzz, DDL E2E, scheduler split
    │
v0.50    ─── Performance, security & ops hardening: SPI batching, dblink fix, CNPG drain hook, digest pinning, ring observability
    │
v0.51    ─── Citus chaos resilience & doc truth: chaos rig, deprecated GUC removal, pg_tide boundary, CTE strategy docs
    │
v1.0.0   ─── Stable release, PostgreSQL 19, package registries, signed artifacts, SBOMs
```

v0.1.0 through v0.27.0 build the complete core engine and harden it for
production use. v0.28.0 and v0.29.0 deliver the event-driven integration
story. v0.30.0 is a mandatory correctness and polish gate before 1.0.
v0.31.0 sharpens scheduler intelligence before new features are added.
v0.32.0 is the first of two Citus releases, shipping stable object naming
and detection helpers as an additive, non-breaking foundation. v0.33.0
delivers the full Citus integration immediately after — per-worker slot CDC,
distributed ST placement, cross-node coordination, and the Citus test suite.
Pulling v0.33.0 forward means users with Citus topologies (including
billion-row all-distributed deployments) are unblocked two releases earlier.
v0.35.0 was intended to be the single most important release before v1.0, but
the v0.37.0 overall assessment shows several of those closeout items remain
partially open or insufficiently proven. v0.36.0 and v0.37.0 still delivered
substantial structural gains: L0 cache construction, temporal IVM,
`RowIdSchema`, scheduler and merge splits, pgVectorMV, and OpenTelemetry trace
capture. The next three releases now form a hardening programme rather than an
immediate feature expansion.

**v0.38.0 is a dedicated EC-01 correctness sprint with a hard release gate:**
This release will NOT ship until join phantom rows are proven closed with a
comprehensive DIFF-vs-FULL property test suite covering Q07/Q15-style joins.
EC-01 has been labeled critical since v0.20.0 (6+ releases) and deferred multiple
times; v0.38.0 breaks that pattern by making EC-01 closure the sole release
objective. No other features, no operational docs, no SQLSTATE rollout — just the
join phantom-row fix and its proof.

**v0.39.0 absorbs the operational truthfulness items** that were originally planned
for v0.38.0: backpressure hysteresis or deprecation, wake-truthfulness repair,
generated configuration and upgrade docs, OpenTelemetry collector proof, SQLSTATE
rollout on hot paths, and the full distributed/diagnostic coverage (Citus chaos
testing, durable CDC hold semantics, per-query TPC-H explain artifacts, SQLancer
light PR mode, targeted fuzzing, and inbox/outbox reliability tests).

**v0.40.0** then focuses on operator trust and maintainability: generated SQL/GUC
references, drain-mode proof, monitoring/alert rules, security-model and
secret-handling docs, upgrade-gate coverage, unsafe-inventory PR gating, and
continued decomposition of the largest files.

**v0.41.0 through v0.45.0 form a second hardening arc** driven by the findings
in the v0.40 overall assessment (plans/PLAN_OVERALL_ASSESSMENT_9.md). These
five releases systematically close every gap identified across 10 dimensions:
correctness (P0 cache-key and placeholder fixes), documentation truthfulness
(repair function implementation, catalog generator rewrite), test quality
(sleep removal, property tests, fuzz CI — merged into v0.42.0), performance
tunability (GUC-exposed thresholds, explain diagnostics), security
(search_path hardening, centralized SQL building), and operational readiness
(preflight functions, scalability infrastructure, CI completeness). Only after
this arc does the roadmap resume the embedding programme in v0.47.0–v0.48.0,
preserving the pgvector work while aligning the release order with the
assessment's conclusion that closing correctness and operational gaps matters
more than adding new surface area. The embedding programme itself is
consolidated into two releases: v0.47.0 for infrastructure and ANN maintenance,
and v0.48.0 completing the full feature set (sparse/half-precision aggregates,
hybrid search, the ergonomic `embedding_stream_table()` API, per-tenant ANN
patterns, and outbox-emitted embedding events). v0.46.0 precedes this arc
with the extraction of `pg_tide` — moving the outbox, inbox, and relay
subsystems into a standalone extension at `trickle-labs/pg-tide`.

**v0.49.0 through v0.51.0 form the v1.0 readiness arc**, driven by the findings
in the v0.48.0 overall assessment (plans/PLAN_OVERALL_ASSESSMENT_10.md). The
assessment confirmed that every P0 correctness issue from prior assessments is
closed — EC-01 phantom rows, snapshot cache-key weakness, placeholder resolution,
and WAL transition TOCTOU are all fixed. The project has transitioned from a
capability problem to a coverage confidence problem. These three releases
systematically close the remaining gaps across test reliability, performance,
security hardening, operational polish, and documentation truth before v1.0.

**v0.49.0 targets test infrastructure quality** — the single highest-risk
category from the v10 assessment. All concurrency tests currently rely on
`sleep(50ms)` for synchronization, which provides false confidence: tests may
pass locally while missing real race conditions on slow CI runners or under
load. This release replaces sleep-based synchronization with `pg_locks`-polling
patterns throughout `tests/e2e_concurrent_tests.rs`. Alongside, ten source
modules that have zero `#[cfg(test)]` unit test coverage are systematically
addressed: `catalog.rs`, `template_cache.rs`, `ivm.rs`, `cdc/polling.rs`,
`cdc/rebuild.rs`, `diagnostics.rs`, `logging.rs`, `metrics_server.rs`, and
`otel.rs`. New fuzz targets are added for the refresh merge SQL codegen
(`src/refresh/merge/`) and row identity tracking (`src/dvm/row_id.rs`) — two
high-value surfaces with no current fuzz coverage. An E2E test for concurrent
DDL during active refresh (`ALTER STREAM TABLE` + in-flight refresh) is added.
The `src/scheduler/mod.rs` monolith (6,700+ lines) is decomposed into focused
submodule files: scheduling loop, parallel dispatch state, and cost model
each become separate files. The e2e-smoke CI filter is widened to cover join,
aggregate, and window operator regressions on every PR, and a consolidated
`just fuzz-all` recipe is added to the justfile.

**v0.50.0 targets performance, security, and operational hardening.** The
differential refresh hot path currently makes 3–4 separate SPI round-trips per
refresh cycle — buffer existence check, change count per source, and table row
estimate — that are consolidated into a single CTE query, saving 10–15ms per
multi-source refresh. The CDC trigger SQL generation loop is tightened using
`String::with_capacity()` to eliminate per-column heap allocations. The
watermark computation in the scheduler tick is consolidated into a single
compound query. On the security side, the `src/citus.rs` dblink calls that
use manual single-quote doubling for escaping are replaced with
`pg_escape_literal()` SPI calls for defense-in-depth. Operational gaps are
closed: the CNPG `cluster-production.yaml` gains a preStop lifecycle hook
that calls `pgtrickle.drain(timeout_s => 120)` before pod termination,
preventing interrupted in-flight refreshes during rolling upgrades. All Docker
base images are pinned to SHA256 digests for reproducible builds. The shared
memory invalidation ring capacity limit (1,024 entries) is documented in
`docs/CONFIGURATION.md` with a new `pg_trickle_invalidation_ring_overflow`
Prometheus counter. Two additional Prometheus metrics are added:
`pg_trickle_dag_cycles_detected` and `pg_trickle_cache_stale_evictions`.
The deep join chain Part 3 correction threshold GUC and its trade-off
between SQL complexity and correctness at >6 join tables is documented in the
configuration reference with an associated soak-test assertion.

**v0.51.0 closes the Citus resilience gap and brings documentation into full
truth.** The Citus distributed support shipped in v0.32–v0.34 has never had
a chaos test suite — there are zero tests validating behaviour under node
failure, shard rebalance, or network partition. This release delivers a
docker-compose-based chaos rig with three scenarios: coordinator restart,
worker node kill with automatic reconnect, and rolling shard rebalance during
active refresh. The deprecated `pg_trickle.event_driven_wake` GUC (non-functional
since background workers cannot use `LISTEN`) is removed entirely along with
all associated code paths and the runtime warning it emits. Documentation is
brought to full truth: `docs/ARCHITECTURE.md` is updated to clearly describe
the pg_tide boundary after v0.46.0 extraction; `docs/CONFIGURATION.md` gains
a deprecation header on the removed GUC entry; the recursive CTE strategy
selection heuristic (semi-naive vs. DRed vs. recomputation fallback) is
documented for the first time with an example EXPLAIN output; and a note is
added to `docs/CONFIGURATION.md` clarifying that CDC triggers fire even when
`pg_trickle.enabled = false` (by design, to keep buffers ready for re-enable).

