# pg_trickle — Project Roadmap

> **Last updated:** 2026-03-02
> **Current version:** 0.1.3

For a concise description of what pg_trickle is and why it exists, read
[ESSENCE.md](ESSENCE.md) — it explains the core problem (full `REFRESH
MATERIALIZED VIEW` recomputation), how the differential dataflow approach
solves it, the hybrid trigger→WAL CDC architecture, and the broad SQL
coverage, all in plain language.

---

## Overview

pg_trickle is a PostgreSQL 18 extension that implements streaming tables with
incremental view maintenance (IVM) via differential dataflow. All 13 design
phases are complete. This roadmap tracks the path from the v0.1.x series to
1.0 and beyond.

```
                                      We are here
                                           │
                                           ▼
 ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
 │  0.1.x   │   │  0.2.0   │   │  0.3.0   │──▶│  0.4.0   │──▶│  1.0.0   │──▶│  1.x+    │
 │ Released │   │ Released │   │ Prod-    │   │ Observ-  │   │ Stable   │   │ Scale &  │
 │ ✅       │   │ (merged) │   │ ready    │   │ ability  │   │ Release  │   │ Ecosystem│
 └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘
```

---

## v0.1.x Series — Released

### v0.1.0 — Released (2026-02-26)

**Status: Released — all 13 design phases implemented.**

Core engine, DVM with 21 OpTree operators, trigger-based CDC, DAG-aware
scheduling, monitoring, dbt macro package, and 1,300+ tests.

Key additions over pre-release:
- WAL decoder pgoutput edge cases (F4)
- JOIN key column change limitation docs (F7)
- Keyless duplicate-row behavior documented (F11)
- CUBE explosion guard (F14)

### v0.1.1 — Released (2026-02-27)

Patch release: WAL decoder keyless pk_hash fix (F2), old_* column population
for UPDATEs (F3), and `delete_insert` merge strategy removal (F1).

### v0.1.2 — Released (2026-02-28)

Patch release: ALTER TYPE/POLICY DDL tracking (F6), window partition key E2E
tests (F8), PgBouncer compatibility docs (F12), read replica detection (F16),
SPI retry with SQLSTATE classification (F29), and 40+ additional E2E tests.

### v0.1.3 — Released (2026-03-01)

Patch release: Completed 50/51 SQL_GAPS_7 items across all tiers. Highlights:
- Adaptive fallback threshold (F27), delta change metrics (F30)
- WAL decoder hardening: replay deduplication, slot lag alerting (F31–F38)
- TPC-H 22-query correctness baseline (22/22 pass, SF=0.01)
- 460 E2E tests (≥ 400 exit criterion met)
- CNPG extension image published to GHCR

See [CHANGELOG.md](CHANGELOG.md) for the full feature list.

---

## v0.2.0 — Correctness & Stability — Released (merged into v0.1.x)

**Status: All work delivered in the v0.1.x patch series.**

The 51-item SQL_GAPS_7 correctness plan was completed ahead of schedule: 50
items landed in v0.1.0–v0.1.3. The one remaining item (F40 — extension upgrade
migration scripts) is deferred to v0.3.0 as O1.

<details>
<summary>Completed items (click to expand)</summary>

| Tier | Items | Status |
|------|-------|--------|
| 0 — Critical | F1–F3, F5–F6 | ✅ Done in v0.1.1–v0.1.3 |
| 1 — Verification | F8–F10, F12 | ✅ Done in v0.1.2–v0.1.3 |
| 2 — Robustness | F13, F15–F16 | ✅ Done in v0.1.2–v0.1.3 |
| 3 — Test coverage | F17–F26 (62 E2E tests) | ✅ Done in v0.1.2–v0.1.3 |
| 4 — Operational hardening | F27–F39 | ✅ Done in v0.1.3 |
| 4 — Upgrade migrations | F40 | ⬜ Deferred → v0.3.0 O1 |
| 5 — Nice-to-have | F41–F51 | ✅ Done in v0.1.3 |

**TPC-H baseline:** 22/22 queries pass deterministic correctness checks across
multiple mutation cycles (`just test-tpch`, SF=0.01).

> *Queries are derived from the TPC-H Benchmark specification; results are not
> comparable to published TPC results. TPC Benchmark™ is a trademark of TPC.*

</details>

---

## v0.3.0 — Production Readiness

**Goal:** Operational polish, parallel refresh, production-grade WAL-based CDC,
and diamond dependency consistency. The extension is suitable for production
use after this milestone.

### Diamond Dependency Consistency

Diamond DAGs (where two stream tables share an upstream source and both feed a
third) currently have an inconsistency window: if B refreshes but C fails, the
fan-in table D sees a split view of the source data.

**Decision:** Option 1 — Epoch-Based Atomic Refresh Groups. Detected multi-path
groups in the DAG refresh atomically within a SAVEPOINT; on failure the group
rolls back and retries next cycle. Linear (non-diamond) STs are unaffected.

| Step | Description | Effort |
|------|-------------|--------|
| D1 | Data structures (`Diamond`, `ConsistencyGroup`) in `dag.rs` | 2–3h |
| D2 | Diamond detection algorithm in `dag.rs` (pure, unit-tested) | 3–4h |
| D3 | Consistency group computation in `dag.rs` (pure, unit-tested) | 2–3h |
| D4 | Catalog column + GUC (`diamond_consistency`) in `catalog.rs`/`config.rs`/`api.rs` | 3–4h |
| D5 | Scheduler wiring (`scheduler.rs`) with SAVEPOINT loop | 4–6h |
| D6 | Monitoring function `pgtrickle.diamond_groups()` | 2–3h |
| D7 | E2E test suite (`tests/e2e_diamond_tests.rs`, 7 tests) | 4–6h |
| D8 | Documentation (`SQL_REFERENCE.md`, `CONFIGURATION.md`, `ARCHITECTURE.md`) | 2–3h |

See [PLAN_DIAMOND_DEPENDENCY_CONSISTENCY.md](plans/sql/PLAN_DIAMOND_DEPENDENCY_CONSISTENCY.md).

> **Diamond subtotal: ~22–32 hours**

### Performance & Parallelism

| Item | Description | Effort | Ref |
|------|-------------|--------|-----|
| P1 | Verify PostgreSQL parallel query for delta SQL | 0h | [REPORT_PARALLELIZATION.md](plans/performance/REPORT_PARALLELIZATION.md) §E |
| P2 | DAG level extraction (`topological_levels()`) | 2–4h | [REPORT_PARALLELIZATION.md §B](plans/performance/REPORT_PARALLELIZATION.md) |
| P3 | Dynamic background worker dispatch per level | 12–16h | [REPORT_PARALLELIZATION.md §A+B](plans/performance/REPORT_PARALLELIZATION.md) |

### Operational

| Item | Description | Effort | Ref |
|------|-------------|--------|-----|
| O1 | Extension upgrade migrations (`ALTER EXTENSION UPDATE`) | 4–6h | [SQL_GAPS_7.md](plans/sql/SQL_GAPS_7.md) G8.2 · [PLAN_UPGRADE_MIGRATIONS.md](plans/sql/PLAN_UPGRADE_MIGRATIONS.md) |
| O2 | Prepared statement cleanup on cache invalidation | 3–4h | [SQL_GAPS_7.md](plans/sql/SQL_GAPS_7.md) G8.3 |
| O3 | ~~Adaptive fallback threshold exposure via monitoring~~ | ✅ Done in v0.1.3 (F27) | [SQL_GAPS_7.md](plans/sql/SQL_GAPS_7.md) G8.4 |
| O4 | ~~SPI SQLSTATE error classification for retry~~ | ✅ Done in v0.1.3 (F29) | [SQL_GAPS_7.md](plans/sql/SQL_GAPS_7.md) G8.6 |
| O5 | Slot lag alerting thresholds (configurable) | 2–3h | [SQL_GAPS_7.md](plans/sql/SQL_GAPS_7.md) G10 |

### WAL CDC Hardening

| Item | Description | Effort | Ref |
|------|-------------|--------|-----|
| W1 | WAL decoder fixes (F2–F3 completed in v0.1.3) | ✅ Done | [PLAN_HYBRID_CDC.md](plans/sql/PLAN_HYBRID_CDC.md) |
| W2 | WAL mode E2E test suite (parallel to trigger suite) | 8–12h | [PLAN_HYBRID_CDC.md](plans/sql/PLAN_HYBRID_CDC.md) |
| W3 | WAL→trigger automatic fallback hardening | 4–6h | [PLAN_HYBRID_CDC.md](plans/sql/PLAN_HYBRID_CDC.md) |
| W4 | Promote `pg_trickle.cdc_mode = 'auto'` to recommended | Documentation | [PLAN_HYBRID_CDC.md](plans/sql/PLAN_HYBRID_CDC.md) |

> **v0.3.0 total: ~62–90 hours**

**Exit criteria:**
- [ ] `max_concurrent_refreshes` drives real parallel refresh
- [ ] WAL CDC mode passes full E2E suite
- [ ] Extension upgrade path tested (`0.1.x → 0.3.0`)
- [ ] Diamond dependency consistency (D1–D8) implemented and E2E-tested
- [ ] Zero P0/P1 gaps remaining

---

## v0.4.0 — Observability & Integration

**Goal:** Prometheus/Grafana observability, dbt-pgtrickle formal release,
complete documentation review, and validated upgrade path. After this
milestone the product is externally visible and monitored.

### Observability

| Item | Description | Effort | Ref |
|------|-------------|--------|-----|
| M1 | Prometheus exporter configuration guide | 4–6h | [PLAN_ECO_SYSTEM.md](plans/ecosystem/PLAN_ECO_SYSTEM.md) §1 |
| M2 | Grafana dashboard (refresh latency, staleness, CDC lag) | 4–6h | [PLAN_ECO_SYSTEM.md §1](plans/ecosystem/PLAN_ECO_SYSTEM.md) |

### Integration & Release prep

| Item | Description | Effort | Ref |
|------|-------------|--------|-----|
| R5 | dbt-pgtrickle 0.1.0 formal release (PyPI) | 2–3h | [dbt-pgtrickle/](dbt-pgtrickle/) · [PLAN_DBT_MACRO.md](plans/dbt/PLAN_DBT_MACRO.md) |
| R6 | Complete documentation review & polish | 4–6h | [docs/](docs/) |
| O1 | Extension upgrade migrations (`ALTER EXTENSION UPDATE`) | 4–6h | [SQL_GAPS_7.md](plans/sql/SQL_GAPS_7.md) G8.2 · [PLAN_UPGRADE_MIGRATIONS.md](plans/sql/PLAN_UPGRADE_MIGRATIONS.md) |

> **v0.4.0 total: ~18–27 hours**

**Exit criteria:**
- [ ] Grafana dashboard published
- [ ] dbt-pgtrickle 0.1.0 on PyPI
- [ ] `ALTER EXTENSION pg_trickle UPDATE` tested (`0.3.0 → 0.4.0`)
- [ ] All public documentation current and reviewed

---

## v1.0.0 — Stable Release

**Goal:** First officially supported release. Semantic versioning locks in.
API, catalog schema, and GUC names are considered stable. Focus is
distribution — getting pg_trickle onto package registries.

### Release engineering

| Item | Description | Effort | Ref |
|------|-------------|--------|-----|
| R1 | Semantic versioning policy + compatibility guarantees | 2–3h | [PLAN_VERSIONING.md](plans/infra/PLAN_VERSIONING.md) |
| R2 | PGXN / apt / rpm packaging | 8–12h | [PLAN_PACKAGING.md](plans/infra/PLAN_PACKAGING.md) |
| R3 | ~~Docker Hub official image~~ → CNPG extension image | ✅ Done | [PLAN_CLOUDNATIVEPG.md](plans/ecosystem/PLAN_CLOUDNATIVEPG.md) |
| R4 | CNPG operator hardening (K8s 1.33+ native ImageVolume) | 4–6h | [PLAN_CLOUDNATIVEPG.md](plans/ecosystem/PLAN_CLOUDNATIVEPG.md) |

> **v1.0.0 total: ~18–27 hours**

**Exit criteria:**
- [ ] Published on PGXN and Docker Hub
- [x] CNPG extension image published to GHCR (`pg_trickle-ext`)
- [x] CNPG cluster-example.yaml validated (Image Volume approach)
- [ ] Upgrade path from v0.4.0 tested
- [ ] Semantic versioning policy in effect

---

## Post-1.0 — Scale & Ecosystem

These are not gated on 1.0 but represent the longer-term horizon.

### Ecosystem expansion

| Item | Description | Effort | Ref |
|------|-------------|--------|-----|
| E1 | dbt full adapter (`dbt-pgtrickle` extending `dbt-postgres`) | 20–30h | [PLAN_DBT_ADAPTER.md](plans/dbt/PLAN_DBT_ADAPTER.md) |
| E2 | Airflow provider (`apache-airflow-providers-pgtrickle`) | 16–20h | [PLAN_ECO_SYSTEM.md §4](plans/ecosystem/PLAN_ECO_SYSTEM.md) |
| E3 | CLI tool (`pgtrickle`) for management outside SQL | 16–20h | [PLAN_ECO_SYSTEM.md §4](plans/ecosystem/PLAN_ECO_SYSTEM.md) |
| E4 | Flyway / Liquibase migration support | 8–12h | [PLAN_ECO_SYSTEM.md §5](plans/ecosystem/PLAN_ECO_SYSTEM.md) |
| E5 | ORM integrations guide (SQLAlchemy, Django, etc.) | 8–12h | [PLAN_ECO_SYSTEM.md §5](plans/ecosystem/PLAN_ECO_SYSTEM.md) |

### Scale

| Item | Description | Effort | Ref |
|------|-------------|--------|-----|
| S1 | External orchestrator sidecar for 100+ STs | 20–40h | [REPORT_PARALLELIZATION.md](plans/performance/REPORT_PARALLELIZATION.md) §D |
| S2 | Citus / distributed PostgreSQL compatibility | ~6 months | [plans/infra/CITUS.md](plans/infra/CITUS.md) |
| S3 | Multi-database support (beyond `postgres` DB) | TBD | [PLAN_MULTI_DATABASE.md](plans/infra/PLAN_MULTI_DATABASE.md) |

### Advanced SQL

| Item | Description | Effort | Ref |
|------|-------------|--------|-----|
| A1 | Circular dependency support (SCC fixpoint iteration) | ~40h | [CIRCULAR_REFERENCES.md](plans/sql/CIRCULAR_REFERENCES.md) |
| A2 | Transactional IVM (immediate, same-transaction refresh) | TBD | [PLAN_TRANSACTIONAL_IVM.md](plans/sql/PLAN_TRANSACTIONAL_IVM.md) |
| A3 | PostgreSQL 19 forward-compatibility | TBD | [PLAN_PG19_COMPAT.md](plans/infra/PLAN_PG19_COMPAT.md) |

---

## Effort Summary

| Milestone | Effort estimate | Cumulative | Status |
|-----------|-----------------|------------|--------|
| v0.1.x — Core engine + correctness | ~30h actual | 30h | ✅ Released |
| v0.3.0 — Production ready | 62–90h | 92–120h | 🔜 Next |
| v0.4.0 — Observability & Integration | 18–27h | 110–147h | |
| v1.0.0 — Stable release | 18–27h | 128–174h | |
| Post-1.0 (ecosystem) | 88–134h | 218–308h | |
| Post-1.0 (scale) | 6+ months | — | |

---

## References

| Document | Purpose |
|----------|---------|
| [CHANGELOG.md](CHANGELOG.md) | What's been built |
| [plans/PLAN.md](plans/PLAN.md) | Original 13-phase design plan |
| [plans/sql/SQL_GAPS_7.md](plans/sql/SQL_GAPS_7.md) | 53 known gaps, prioritized |
| [plans/performance/REPORT_PARALLELIZATION.md](plans/performance/REPORT_PARALLELIZATION.md) | Parallelization options analysis |
| [plans/performance/STATUS_PERFORMANCE.md](plans/performance/STATUS_PERFORMANCE.md) | Benchmark results |
| [plans/ecosystem/PLAN_ECO_SYSTEM.md](plans/ecosystem/PLAN_ECO_SYSTEM.md) | Ecosystem project catalog |
| [plans/dbt/PLAN_DBT_ADAPTER.md](plans/dbt/PLAN_DBT_ADAPTER.md) | Full dbt adapter plan |
| [plans/infra/CITUS.md](plans/infra/CITUS.md) | Citus compatibility plan |
| [plans/infra/PLAN_VERSIONING.md](plans/infra/PLAN_VERSIONING.md) | Versioning & compatibility policy |
| [plans/infra/PLAN_PACKAGING.md](plans/infra/PLAN_PACKAGING.md) | PGXN / deb / rpm packaging |
| [plans/infra/PLAN_DOCKER_IMAGE.md](plans/infra/PLAN_DOCKER_IMAGE.md) | Official Docker image (superseded by CNPG extension image) |
| [plans/ecosystem/PLAN_CLOUDNATIVEPG.md](plans/ecosystem/PLAN_CLOUDNATIVEPG.md) | CNPG Image Volume extension image |
| [plans/infra/PLAN_MULTI_DATABASE.md](plans/infra/PLAN_MULTI_DATABASE.md) | Multi-database support |
| [plans/infra/PLAN_PG19_COMPAT.md](plans/infra/PLAN_PG19_COMPAT.md) | PostgreSQL 19 forward-compatibility |
| [plans/sql/PLAN_UPGRADE_MIGRATIONS.md](plans/sql/PLAN_UPGRADE_MIGRATIONS.md) | Extension upgrade migrations |
| [plans/sql/PLAN_TRANSACTIONAL_IVM.md](plans/sql/PLAN_TRANSACTIONAL_IVM.md) | Transactional IVM (immediate, same-transaction refresh) |
| [plans/sql/PLAN_DIAMOND_DEPENDENCY_CONSISTENCY.md](plans/sql/PLAN_DIAMOND_DEPENDENCY_CONSISTENCY.md) | Diamond dependency consistency (multi-path refresh atomicity) |
| [plans/adrs/PLAN_ADRS.md](plans/adrs/PLAN_ADRS.md) | Architectural decisions |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System architecture |
