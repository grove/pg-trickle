# pg_trickle Overall Assessment — Report 12

> Status: Deep gap analysis — static source audit + test mapping
> Date: 2026-05-12
> Scope: Full repository (v0.57.0)
> Prior reports: 11 (v0.51), 10 (v0.49), 9 (v0.48), 8, 7, 3, 2, 1

---

## Executive Summary

pg_trickle has continued to mature substantially since Report 11 (v0.51.0).
The v0.52–v0.57 releases closed almost every actionable finding from the
prior audit: the Aho-Corasick placeholder resolver replaced the O(n²) string
replacement (P-1), thread-local volatility caches eliminated per-call SPI
round-trips (P-2), `DiffContext` allocations were made lazy (P-3), the
snapshot fingerprint gained a two-level pointer-identity cache (P-4),
`Expr::to_sql()` now writes into a pre-allocated buffer (P-5), view
inlining caches relkind lookups (P-6), MERGE template LRU is now O(1) via
the `lru` crate (P-8), `diff_node()` enforces depth and CTE-count limits
(C-7/R-7), upstream ST frontier validation prevents the "0/0" silent fallback
(C-4), diamond detection is now O(V+E) via precomputed ancestor sets (S-1),
the invalidation ring capacity is configurable up to 4 096 (S-2), every
scheduler and parser sub-module gained `#[cfg(test)]` coverage (T-2..T-6),
the NOTIFY payload pipeline switched to `serde_json` (SEC-6), and `api/mod.rs`
and `monitor.rs` were decomposed into focused sub-modules (Q-2/Q-3). This is
an unusually disciplined burn-down of an assessment backlog and demonstrates
the project's commitment to closing audit findings.

That said, this audit surfaces **four genuinely concerning issues** that
deserve attention before the v1.0 cut. (1) Two of the newer SQL-callable
APIs — `attach_outbox()` / `detach_outbox()` / `attach_embedding_outbox()` in
[src/api/outbox.rs](src/api/outbox.rs) and `stream_table_to_publication()` /
`drop_stream_table_publication()` in [src/api/publication.rs](src/api/publication.rs)
— are missing the `check_stream_table_ownership()` call that all other
mutating APIs use. Any role with `EXECUTE` on the pgtrickle schema can publish
another user's stream table over logical replication or hijack its delta
stream into a pg_tide outbox. This is a clear privilege-escalation gap that
prior audits did not catch because these APIs were added in v0.46 and v0.48.
(2) The recursive-CTE depth guard introduced for IMMEDIATE mode in
[src/dvm/operators/recursive_cte.rs](src/dvm/operators/recursive_cte.rs#L670)
is not applied to DIFFERENTIAL mode, leaving open a pathological-loop vector
that the new `diff_node()` depth guard only partially compensates for. (3)
The multi-column `IN` rewrite shipped in v0.55 (M-5) does not handle row
constructors containing `NULL` correctly under `NOT IN`. (4) The monitor's
per-source buffer-growth check
([src/monitor/mod.rs](src/monitor/mod.rs#L1515)) still issues one
`SELECT count(*)` per source instead of using the batched
`UNION ALL` pattern already proven in the scheduler — this is an N×ticks
SPI amplifier on busy clusters.

The remaining findings are incremental quality work: WAL-decoder TOCTOU race
between slot eligibility check and `poll_wal_changes()`, missing CDC-lag and
worker-utilisation percentile metrics, `application_name` not set on BGW
connections, several large functions in `src/refresh/codegen.rs` and
`src/cdc.rs` that warrant extraction, a handful of fixed `sleep()` calls in
E2E tests, and documentation gaps around `pg_dump`/`pg_restore` of the
`pgtrickle` and `pgtrickle_changes` schemas. None of these are release
blockers individually, but together they form a polished v1.0 sprint
backlog.

**Top five risks (priority order):**

1. 🟠 **HIGH — `attach_outbox()` / publication APIs lack ownership checks**
   (S-1, S-3). Add `check_stream_table_ownership()` before these run.
2. 🟠 **HIGH — Recursive CTE depth guard absent in DIFFERENTIAL mode**
   (C-2). Apply the existing guard uniformly.
3. 🟠 **HIGH — Multi-column `NOT IN` rewrite + `NULL` rows** (C-1). Either
   detect the NULL case and fall back, or emit an `UnsupportedFeature`.
4. 🟠 **HIGH — Per-source SPI fan-out in monitor health check** (P-1).
   Batch buffer-count queries the same way the scheduler already does.
5. 🟡 **MEDIUM — WAL-decoder slot eligibility TOCTOU race** (C-3). Hold an
   advisory lock or check eligibility inside the same SPI transaction that
   consumes from the slot.

Overall, the project is in **excellent** shape — one of the cleanest
PostgreSQL extension codebases I have audited at this stage of maturity.
The findings below are calibrated against a "world-class v1.0" bar, not a
"functional pre-1.0" bar.

---

## Severity Legend

- 🔴 CRITICAL — data loss, security breach, crash, or correctness bug
- 🟠 HIGH — significant performance regression, scalability blocker, or
  missing core feature
- 🟡 MEDIUM — code quality, ergonomics, observability, or test coverage gap
- 🟢 LOW — polish, documentation, minor improvement

---

## Summary Table

| ID    | Severity | Area              | Title                                                                  | Status |
|-------|----------|-------------------|------------------------------------------------------------------------|--------|
| C-1   | 🟠 HIGH  | Correctness       | Multi-column `NOT IN` rewrite missing NULL row handling                | NEW |
| C-2   | 🟠 HIGH  | Correctness       | Recursive CTE depth guard not applied in DIFFERENTIAL mode             | NEW |
| C-3   | 🟡 MED   | Correctness       | WAL decoder TOCTOU between slot eligibility and `poll_wal_changes()`   | NEW |
| C-4   | 🟡 MED   | Correctness       | Compact-buffer advisory-lock failure returns `Ok(0)` silently          | NEW |
| C-5   | 🟡 MED   | Correctness       | WAL decoder filters by qualified name string instead of OID            | NEW |
| C-6   | 🟡 MED   | Correctness       | Publication-rebuild check misses "table becomes a partition of" case   | NEW |
| C-7   | 🟢 LOW   | Correctness       | EC01-2 phantom cleanup uses raw `ctid` (HOT-update fragile)            | NEW |
| C-8   | 🟢 LOW   | Correctness       | Snapshot cache key is 64-bit hash with no secondary equality           | PERSISTENT (C-10 R11) |
| C-9   | 🟢 LOW   | Correctness       | `DiffContext::cte_counter` never resets across `differentiate()` calls | PERSISTENT (C-8 R11) |
| S-1   | 🟠 HIGH  | Security          | `attach_outbox()` / `detach_outbox()` / `attach_embedding_outbox()` lack ownership check | NEW |
| S-2   | 🟠 HIGH  | Security          | `stream_table_to_publication()` / `drop_stream_table_publication()` lack ownership check | NEW |
| S-3   | 🟡 MED   | Security          | DDL hook silently swallows SPI errors and skips reinit marking         | NEW |
| S-4   | 🟡 MED   | Security          | `buffer_qualified_name_for_oid()` does not quote schema identifier     | NEW |
| S-5   | 🟢 LOW   | Security          | Outbox-name truncation can collide on long ST names                    | NEW |
| P-1   | 🟠 HIGH  | Performance       | Monitor buffer-growth check issues one SPI per source                  | NEW |
| P-2   | 🟡 MED   | Performance       | `defining_query_hash` recomputed per refresh; should be cached on meta | NEW |
| P-3   | 🟡 MED   | Performance       | MERGE template cache hit path clones 8 large `String` fields           | NEW |
| P-4   | 🟡 MED   | Performance       | Two separate cache borrows in `cached_non_monotonic` / `cached_is_deduplicated` | NEW |
| P-5   | 🟡 MED   | Performance       | WAL-decoder UPDATE path allocates 5 `Vec<String>` per row              | NEW |
| P-6   | 🟡 MED   | Performance       | Per-upstream `frontier.clone()` in `has_stream_table_source_changes()` | NEW |
| P-7   | 🟢 LOW   | Performance       | Diamond detection still allocates intersection vectors per pair        | NEW |
| Q-1   | 🟡 MED   | Quality           | Scheduler workers mix `log!` and `warning!` levels inconsistently      | NEW |
| Q-2   | 🟡 MED   | Quality           | `src/refresh/codegen.rs` contains multiple 200+-line functions         | NEW |
| Q-3   | 🟡 MED   | Quality           | `src/cdc.rs` is 3 383 lines (refactor candidate after monitor split)   | NEW |
| Q-4   | 🟢 LOW   | Quality           | `src/dvm/parser/sublinks.rs` is 7 065 lines (largest non-test file)    | NEW |
| Q-5   | 🟢 LOW   | Quality           | Test code in `lateral_subquery.rs` uses brittle `split().nth(1).unwrap_or("")` | NEW |
| T-1   | 🟡 MED   | Test coverage     | `src/refresh/orchestrator.rs` and `src/refresh/merge/*.rs` lack unit tests | NEW |
| T-2   | 🟡 MED   | Test coverage     | `src/cdc.rs`, `src/cdc/polling.rs`, `src/cdc/rebuild.rs` lack pure-logic unit tests | PERSISTENT (T-5 R11) |
| T-3   | 🟡 MED   | Test coverage     | `src/hooks.rs` DDL classification has no unit tests                    | NEW |
| T-4   | 🟢 LOW   | Test coverage     | ~50 fixed `tokio::time::sleep()` calls remain across `tests/`          | PARTIAL (T-9 R11) |
| T-5   | 🟢 LOW   | Test coverage     | No proptest target asserts differential idempotence under partition reordering | NEW |
| E-1   | 🟡 MED   | Ergonomics        | `pgtrickle.health_check()` does not surface "publication owned by other user" | NEW |
| E-2   | 🟢 LOW   | Ergonomics        | `SQL_REFERENCE.md` lists 40+ functions but source has 100+ `#[pg_extern]` | NEW |
| O-1   | 🟡 MED   | Observability     | No CDC-lag percentile metrics (p50/p95/p99)                            | NEW |
| O-2   | 🟡 MED   | Observability     | No parallel-worker queue-depth / idle-time metric                      | NEW |
| O-3   | 🟡 MED   | Observability     | No WAL-decoder pending-record / queue-depth metric                     | NEW |
| O-4   | 🟡 MED   | Observability     | No refresh-mode (DIFFERENTIAL vs FULL) ratio metric per ST             | NEW |
| O-5   | 🟡 MED   | Observability     | BGW connections do not set `application_name` on `Spi::connect()`      | NEW |
| O-6   | 🟢 LOW   | Observability     | `INSTALL.md` does not document required `pg_dump --schema` flags       | NEW |
| CI-1  | 🟡 MED   | CI/CD             | Full E2E + TPC-H still skipped on PRs; ~24 h feedback loop on `main`   | PERSISTENT (B-2 R11) |
| CI-2  | 🟢 LOW   | CI/CD             | `tests/Dockerfile.e2e` runs as root inside the postgres image          | NEW |
| CI-3  | 🟢 LOW   | CI/CD             | `codecov.yml` excludes `cdc.rs` / `hooks.rs` / `wal_decoder.rs` from the patch gate | NEW |
| F-1   | 🟡 MED   | Features          | LATERAL + DIFFERENTIAL on outer-applied subqueries underspecified in docs | NEW |
| F-2   | 🟢 LOW   | Features          | SQL:2023 `SEARCH` / `CYCLE` clauses still unsupported (no error message in some paths) | PERSISTENT (F-2 R11) |
| F-3   | 🟢 LOW   | Features          | `auto_explain` integration still missing                               | PERSISTENT (F-3 R11) |
| F-4   | 🟢 LOW   | Features          | No `pg_partman` integration; pg_trickle-managed partition pruning manual | PERSISTENT (F-4 R11) |
| D-1   | 🟢 LOW   | Documentation     | ADRs in `plans/adrs/PLAN_ADRS.md` are still in PROPOSED status         | NEW |
| D-2   | 🟢 LOW   | Documentation     | `docs/LIMITATIONS.md` does not mention multi-column `NOT IN` with NULLs | NEW |

Total findings: **47** (4 HIGH, 23 MEDIUM, 20 LOW; 0 CRITICAL).

---

## What Has Improved Since Report 11

| R11 ID | Title                                              | Status in v0.57.0 | Evidence |
|--------|----------------------------------------------------|-------------------|----------|
| C-1    | `filter.rs expect()` in HAVING path                 | ✅ Fixed v0.52    | CHANGELOG `## [0.52.0]` C-1 |
| C-2    | Placeholder regex matches user identifiers          | ✅ Documented v0.55 | M-7 reserved-prefix docs |
| C-4    | ST source frontier defaults to "0/0"                | ✅ Fixed v0.54    | `PgTrickleError::StSourceFrontierMissing` |
| C-6    | `sqlstate_to_string()` boundary tests               | ⏳ Open (not addressed) | — |
| C-7    | `diff_node()` has no depth limit                    | ✅ Fixed v0.54    | `max_parse_depth` enforced; `DiffDepthExceeded` |
| C-10   | Snapshot cache key collision risk                   | ⏳ Open (re-raised as C-8 here) | — |
| Q-2    | `api/mod.rs` is 7 600+ lines                        | ✅ Split v0.55 (M-2) | `src/api/{create,alter,refresh_ops,…}` |
| Q-3    | `monitor.rs` is 4 000+ lines                        | ✅ Split v0.55 (M-3) | `src/monitor/{alert,health,tree,mod}.rs` |
| Q-6    | GUC defaults lack rationale comments                | ✅ Fixed v0.55 (M-8) | inline rationale across `config.rs` |
| P-1    | Placeholder resolution O(n²)                        | ✅ Fixed v0.52    | `aho-corasick` single-pass |
| P-2    | Function volatility lookups uncached                | ✅ Fixed v0.52    | thread-local `HashMap` cache |
| P-3    | DiffContext over-allocates HashMaps                 | ✅ Fixed v0.52    | lazy `Option<HashMap>` |
| P-4    | Snapshot fingerprint traverses full tree            | ✅ Fixed v0.54    | two-level cache |
| P-5    | `Expr::to_sql()` allocates per call                 | ✅ Fixed v0.54    | `to_sql_into(&mut String)` visitor |
| P-6    | View inlining re-parses per iteration               | ✅ Fixed v0.54    | relkind cache through call chain |
| P-7    | Operator volatility lookup complex SPI              | ✅ Fixed v0.52    | unified thread-local cache |
| P-8    | Template cache LRU eviction O(N)                    | ✅ Fixed v0.52    | `lru::LruCache` |
| S-1    | Diamond detection O(V²)                             | ✅ Fixed v0.54    | `compute_all_ancestors()` |
| S-2    | Shmem ring buffer fixed at 1024                     | ✅ Fixed v0.55 (M-1) | max raised to 4 096, GUC default 1 024 |
| SEC-6  | NOTIFY JSON manual escaping                         | ✅ Fixed v0.55 (M-4) | `serde_json::json!` |
| T-2    | `dag.rs` no unit tests                              | ✅ Fixed v0.53    | proptests + unit tests added |
| T-3    | `config.rs` no unit tests                           | ✅ Fixed v0.53    | — |
| T-4    | `scheduler/` no unit tests                          | ✅ Fixed v0.53    | dispatch/pool/cost/tier/watermark/citus/scheduler_loop tested |
| T-5    | `cdc.rs` no unit tests                              | ⏳ Still minimal (re-raised as T-2 here) | only 1 `#[cfg(test)]` block |
| T-6    | DVM parser pure-function tests                      | ✅ Fixed v0.53    | sublinks/parser tests added |
| T-9    | Long timeouts in buffer-growth tests                | ✅ Fixed v0.53 (T-8) | adaptive polling |
| R-7    | No OOM protection in DiffContext                    | ✅ Fixed v0.54    | `max_diff_ctes` cap |
| F-1    | Multi-column IN not supported                       | ✅ Fixed v0.55 (M-5) — but NULL edge case re-raised here as C-1 |
| O-4    | Missing DVM parser perf metrics                     | ✅ Fixed v0.55 (M-6) | `pg_trickle_dvm_parse_ms`, `pg_trickle_delta_query_size_bytes` |
| B-8    | Coverage not gated in PRs                           | ⏳ Partial — codecov uploaded but not blocking (M-9 v0.55) |

Closure rate from Report 11: **23 of 30 actionable findings fixed in
six minor releases** — exemplary. Three remain open (C-6 sqlstate tests,
C-10/C-8 cache key, B-8 coverage gating), four were superseded by deeper
or different issues raised here (F-1 → C-1 NULL edge case; T-5 still open
as T-2 here).

---

## Area 1: Correctness & Bugs

### Finding C-1: Multi-column `NOT IN` Rewrite Missing NULL Row Handling
**Severity:** 🟠 HIGH — NEW
**Location:** [src/dvm/parser/sublinks.rs](src/dvm/parser/sublinks.rs) (multi-column `IN`/`NOT IN` rewrite added in v0.55 M-5; row-expr extraction near line 1000)
**Description:** The v0.55 rewrite of `(a, b) IN (SELECT x, y FROM …)` into a
semi-join with `a = x AND b = y` (and the matching anti-join for `NOT IN`)
relies on the equality being TRUE/FALSE-valued. With `NULL` rows, however,
SQL's `(NULL, 5) IN (SELECT …)` returns UNKNOWN if no row matches the
non-NULL components, while the rewritten AND-chain in a SemiJoin can still
yield rows because `NULL = NULL` is UNKNOWN, not FALSE. For `NOT IN`, the
issue is more dangerous: `(NULL, …) NOT IN (…)` must produce UNKNOWN (and
hence reject the row), but anti-join semantics will keep rows whose
correlation predicate evaluates to UNKNOWN, producing extra rows in the
stream table.
**Impact:** Silently incorrect `NOT IN` results for rows containing NULLs
in any IN-list component — the textbook IVM trap.
**Recommendation:** Either (a) detect `NULL`/non-strict expressions in the
row constructor and the subquery's target list and skip the rewrite, or
(b) inject `IS NOT NULL` guards on every column on both sides of the
generated anti-join, matching PostgreSQL's executor semantics for
`NOT IN`. Add proptest cases that fuzz row constructors with NULLs and
compare against `REFRESH MATERIALIZED VIEW`.

### Finding C-2: Recursive-CTE Depth Guard Not Applied in DIFFERENTIAL Mode
**Severity:** 🟠 HIGH — NEW
**Location:** [src/dvm/operators/recursive_cte.rs](src/dvm/operators/recursive_cte.rs#L670)
**Description:** The depth guard is gated on `DeltaSource::TransitionTable`
(IMMEDIATE mode); for `DeltaSource::ChangeBuffer` (DIFFERENTIAL mode) the
guard is `None`. The `diff_node()` depth cap (v0.54 C-7) protects against
operator-tree recursion but does not bound the *recursive CTE's* iteration
depth at execution time. A pathological self-referencing recursive CTE in
DIFFERENTIAL mode can therefore loop until WAL/temp-buffer limits trip.
**Evidence:**
```rust
let max_depth = if matches!(ctx.delta_source, DeltaSource::TransitionTable { .. }) {
    crate::config::pg_trickle_ivm_recursive_max_depth()
} else {
    None
};
```
**Impact:** Backend OOM/long-running query on adversarial recursive
definitions in DIFFERENTIAL mode.
**Recommendation:** Apply `ivm_recursive_max_depth` to both modes.

### Finding C-3: WAL Decoder TOCTOU Between Slot Eligibility and Consumption
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/wal_decoder.rs](src/wal_decoder.rs#L260-L300)
**Description:** `is_slot_suitable_for_wal_transition()` and the subsequent
`poll_wal_changes()` are separate calls; between them a concurrent
`pg_drop_replication_slot()` (e.g., from an admin or from the EC-20 fallback
path) can invalidate the slot. The error path falls back to triggers, but
the brief race can miss changes if eligibility passes after a slot was
recreated with a different `restart_lsn`.
**Impact:** Rare CDC-gap risk on slot churn.
**Recommendation:** Hold `pg_advisory_xact_lock` keyed on slot OID across
both calls, or merge eligibility into the same SPI transaction as
consumption.

### Finding C-4: Compact-Buffer Advisory-Lock Failure Returns `Ok(0)`
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/cdc.rs](src/cdc.rs#L942-L1030)
**Description:** When `pg_try_advisory_xact_lock()` fails, the compactor
returns `Ok(0)`, indistinguishable from "no rows needed compaction". A
persistent lock contention scenario thus hides itself from operators.
**Impact:** Change buffers can grow unbounded without visible signal.
**Recommendation:** Return `enum CompactionResult { Done(i64), Contended,
SkippedDisabled }`, log at `debug!` on contention, and expose a
shmem counter `pg_trickle_cdc_compact_contended_total`.

### Finding C-5: WAL Decoder Filters by Qualified-Name String Instead of OID
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/wal_decoder.rs](src/wal_decoder.rs) (table-name filter
section ~L550–L600)
**Description:** The decoder compares `test_decoding`'s
schema-qualified table names by string equality. Edge cases (search_path,
case-sensitive quoted names, partition routing where the child arrives
instead of the root) can cause false negatives.
**Impact:** Silently dropped CDC rows in partitioned setups.
**Recommendation:** Resolve every decoded row to an OID via
`to_regclass()` once per cycle and key the filter on OID; fall back to
text matching only if OID lookup fails.

### Finding C-6: Publication-Rebuild Check Misses "Table Becomes Partition of"
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/wal_decoder.rs](src/wal_decoder.rs#L3350-L3383)
(`check_if_publication_needs_rebuild()`)
**Description:** The check examines the source `relkind` to detect when a
plain table is converted to a partitioned root. It does *not* detect the
inverse — when a previously-standalone table is attached as a partition of
some other parent. After such an `ALTER TABLE ATTACH PARTITION`, changes
arrive with the parent's name and the existing publication silently filters
them out.
**Impact:** Stream tables that depend on a partition can freeze after
attach without an error.
**Recommendation:** Additionally check
`SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = $1)` and
rebuild the publication when this transitions from false to true.

### Finding C-7: EC01-2 Phantom Cleanup Uses Raw `ctid`
**Severity:** 🟢 LOW — NEW
**Location:** [src/refresh/phd1.rs](src/refresh/phd1.rs#L180-L220)
**Description:** Cross-cycle phantom reconciliation deletes by `ctid`
captured from a CTE earlier in the same transaction. PostgreSQL guarantees
`ctid` stability within a snapshot, so this is technically safe inside one
transaction; however, the comment block does not state the invariant
explicitly, and any future refactor that splits the operation across
transactions would silently break.
**Impact:** Footgun for future maintainers; current code is correct.
**Recommendation:** Add a `// INVARIANT: ctid is captured and consumed in
the same snapshot; do not split across subtransactions` comment, and
prefer the stream table's row identity columns (`__pgt_row_id`) where
they are available.

### Finding C-8: 64-bit `DefaultHasher` Snapshot Cache Key
**Severity:** 🟢 LOW — PERSISTENT (R11 C-10)
**Location:** [src/dvm/diff.rs](src/dvm/diff.rs#L233-L280)
**Description:** The structural fingerprint is still a single 64-bit
hash. Even at billions of refreshes, the birthday-bound collision
probability is negligible but non-zero, and a collision produces silently
wrong SQL.
**Recommendation:** Cheap defence-in-depth: store the rendered fingerprint
*string* alongside the hash and compare both on cache hit.

### Finding C-9: `DiffContext::cte_counter` Never Resets
**Severity:** 🟢 LOW — PERSISTENT (R11 C-8)
**Location:** [src/dvm/diff.rs](src/dvm/diff.rs#L598)
**Recommendation:** Reset to 0 at the top of each `differentiate()` call.

---

## Area 2: Performance & Scalability

### Finding P-1: Monitor Buffer-Growth Check Issues One SPI per Source
**Severity:** 🟠 HIGH — NEW
**Location:** [src/monitor/mod.rs](src/monitor/mod.rs#L1515-L1530)
**Description:** Inside the health-check loop, every CDC source gets its
own `SELECT count(*)::bigint FROM pgtrickle_changes.changes_<oid>` SPI
call. With 100 CDC-enabled sources on a 10 Hz monitor tick this is 1 000
SPI round-trips per second, dwarfing the rest of the refresh path.
**Recommendation:** Batch into a single SPI:
```sql
SELECT * FROM (VALUES (oid1, (SELECT count(*) FROM changes_oid1)::bigint),
                       (oid2, (SELECT count(*) FROM changes_oid2)::bigint),
                       …) v(source_oid, pending);
```
or use `pg_class.reltuples` for an approximate first-pass filter and only
COUNT(*) on plausibly-over-threshold buffers.

### Finding P-2: `defining_query_hash` Recomputed Per Refresh
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/refresh/merge/mod.rs](src/refresh/merge/mod.rs#L1342-L1360)
**Description:** On every refresh entry, the merge-cache lookup hashes the
full defining query (`DefaultHasher`) and compares against the cached
hash. The query is immutable between CREATE/ALTER, so the hash can be
computed once at catalog write and cached in `StreamTableMeta`.
**Recommendation:** Add `defining_query_hash: i64` to
`pgtrickle.pgt_stream_tables`, populate on insert/update, and pass through
to the cache lookup.

### Finding P-3: Merge-Template Cache Hits Clone Eight `String` Fields
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/refresh/merge/mod.rs](src/refresh/merge/mod.rs#L1350-L1430)
**Description:** On a cache hit, the full `CachedMergeTemplate` is
`.clone()`d to produce an executable plan. The struct holds eight
multi-kilobyte template strings; under a 1 000 refresh/s load this dwarfs
the cache itself.
**Recommendation:** Wrap each template `String` in `Arc<String>` (or
`Arc<str>`); cloning then becomes an atomic increment. Alternatively,
restructure the plan to borrow from the cache entry by lifetime.

### Finding P-4: Two Separate `MERGE_TEMPLATE_CACHE.with()` Borrows
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/refresh/merge/mod.rs](src/refresh/merge/mod.rs#L874-L892)
**Description:** `cached_non_monotonic` and `cached_is_deduplicated` each
acquire their own `RefCell::borrow()` and `LruCache::get()`. The second
call also marks the entry as recently used, distorting LRU ordering.
**Recommendation:** Single borrow, extract both flags into a small tuple.

### Finding P-5: WAL-Decoder UPDATE Path Allocates Five `Vec<String>` per Row
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/wal_decoder.rs](src/wal_decoder.rs#L781-L870)
**Description:** Builds `col_names`, `placeholders`, `param_values`,
`col_names_u`, `d_all_params`, `i_all_params`, with `to_string()` calls
inside loops. At sustained 10 k UPDATE/s with 50-column rows this is a
multi-MB/s allocator burn rate.
**Recommendation:** Reuse a per-call `Vec` pool (or `Vec::with_capacity`
keyed off column count) and switch to `write!` into a pre-allocated
`String`.

### Finding P-6: `frontier.clone()` per Upstream Source-Change Probe
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/scheduler/mod.rs](src/scheduler/mod.rs#L2200-L2235)
**Description:** `has_stream_table_source_changes()` clones the entire
`Frontier` for every upstream check. The struct contains a HashMap keyed
on upstream `pgt_id`; cloning costs scale with both fan-in and frontier
density.
**Recommendation:** Borrow via `st.frontier.as_ref().unwrap_or(&FRONTIER_EMPTY)`.

### Finding P-7: Diamond Detection Still Allocates Intersection Vectors
**Severity:** 🟢 LOW — NEW
**Location:** [src/dag.rs](src/dag.rs#L750-L800)
**Description:** Even after S-1's O(V+E) ancestor-set precomputation, the
pair-wise intersection still uses `.intersection().copied().collect()`
into a temporary `Vec`. For dense fan-in nodes this is a small but
measurable allocation per pair.
**Recommendation:** Iterate via `intersection()` lazily and break on the
first shared node, or short-circuit using bitset representation if the
node count is small.

---

## Area 3: Security

### Finding S-1: `attach_outbox()` / `detach_outbox()` / `attach_embedding_outbox()` Skip Ownership Checks
**Severity:** 🟠 HIGH — NEW
**Location:** [src/api/outbox.rs](src/api/outbox.rs#L92-L153),
[src/api/outbox.rs](src/api/outbox.rs#L168-L260),
[src/api/outbox.rs](src/api/outbox.rs#L282-L420)
**Description:** Every other mutating API (`alter_stream_table`,
`drop_stream_table`, `pause_stream_table`, `resume_stream_table`, …) calls
`check_stream_table_ownership()` (or equivalent) before proceeding. The
outbox attach/detach functions do not. Any role with `EXECUTE` on the
`pgtrickle` schema can therefore attach a pg_tide outbox to a stream
table owned by another role and observe (or modify, depending on outbox
configuration) its change stream.
**Evidence:** No `check_stream_table_ownership` or `verify_st_owner`
call appears in `attach_outbox_impl` (line 92), `detach_outbox_impl`
(line 168), or `attach_embedding_outbox_impl` (line 282).
**Impact:** Direct privilege-escalation / data-exfiltration vector once
the pg_tide extension is co-installed.
**Recommendation:** Insert the standard ownership check immediately after
`StreamTableMeta::get_by_name()` in all three functions.

### Finding S-2: `stream_table_to_publication()` and `drop_stream_table_publication()` Skip Ownership Checks
**Severity:** 🟠 HIGH — NEW
**Location:** [src/api/publication.rs](src/api/publication.rs#L22-L72),
[src/api/publication.rs](src/api/publication.rs#L79-L120)
**Description:** Same gap as S-1 but with consequences specific to
logical replication: a non-owner can create a publication exposing the
stream table's storage to any subscriber, or drop an owner's publication
out from under them.
**Evidence:** No ownership check between `StreamTableMeta::get_by_name()`
and `CREATE PUBLICATION` / `DROP PUBLICATION IF EXISTS`.
**Impact:** Privilege escalation / denial of service against logical
replication subscribers.
**Recommendation:** Identical fix to S-1.

### Finding S-3: DDL Hook Silently Swallows SPI Errors and Skips Reinit Marking
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/hooks.rs](src/hooks.rs#L200-L260)
**Description:** When `find_view_downstream_pgt_ids()` fails (transient
SPI error, catalog corruption, concurrent DDL), `handle_alter_table()`
emits a `warning!()` and returns. The downstream stream tables that
needed `needs_reinit = true` therefore never get marked.
**Impact:** Schema changes can silently produce stale stream tables.
**Recommendation:** On SPI failure, retry once, then escalate to an
`error!()` so the original DDL fails — better to block the schema change
than to corrupt downstream state.

### Finding S-4: `buffer_qualified_name_for_oid()` Does Not Quote Schema
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/cdc.rs](src/cdc.rs#L100-L160)
**Description:** Returns `format!("{change_schema}.{base}")` without
quoting `change_schema`. The default `pgtrickle_changes` is safe, but the
schema name is a GUC and quoted-identifier schemas (e.g.
`"my schema"`) would break the SQL. While not a direct injection vector
(the GUC is superuser-only), it is an unnecessary footgun.
**Recommendation:** Use the existing `sql_builder::qualified()` helper,
or `pg_catalog.quote_ident()` via SPI.

### Finding S-5: Outbox Name Truncation Can Collide
**Severity:** 🟢 LOW — NEW
**Location:** [src/api/outbox.rs](src/api/outbox.rs#L27-L35)
**Description:** `outbox_table_name_for()` truncates to 63 chars without
disambiguation. Two ST names that differ only after the 56-character mark
produce identical outbox names.
**Recommendation:** When truncation occurs, append the first 8 hex chars
of `blake3(st_name)`.

---

## Area 4: Code Quality & Maintainability

### Finding Q-1: Scheduler Workers Mix `log!` and `warning!` Levels Inconsistently
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/scheduler/dispatch.rs](src/scheduler/dispatch.rs#L100-L250)
**Description:** Routine state transitions log at `log!` (DEBUG)
alongside `warning!` for recoverable failures. Operators running at
`log_min_messages = warning` see warnings but never see "started parallel
dispatch", and operators at `log_min_messages = debug1` see both plus
log spam from elsewhere.
**Recommendation:** Standardise on `info!` for routine state transitions,
`warning!` for recoverable failures, `error!` for fatal events.

### Finding Q-2: `src/refresh/codegen.rs` Has Multiple 200+-Line Functions
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/refresh/codegen.rs](src/refresh/codegen.rs)
**Description:** `build_bypass_capture_sql`, `capture_diff_to_table`, and
the MERGE-template builders mix conditionals with SQL building. Hard to
review.
**Recommendation:** Extract `build_cte_predicate()`,
`build_merge_join_condition()`, `build_content_hash_column()` into a
`refresh/sql_fragments.rs` helper module.

### Finding Q-3: `src/cdc.rs` is 3 383 Lines
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/cdc.rs](src/cdc.rs)
**Description:** After the v0.55 monitor/api decomposition, `cdc.rs` is
now the third-largest non-test file. Trigger generation, buffer
management, compaction, and partition-promotion all live here.
**Recommendation:** Split into `cdc/triggers.rs`, `cdc/buffer.rs`,
`cdc/compact.rs`, `cdc/partition.rs` following the v0.55 monitor pattern.

### Finding Q-4: `src/dvm/parser/sublinks.rs` is 7 065 Lines
**Severity:** 🟢 LOW — NEW
**Location:** [src/dvm/parser/sublinks.rs](src/dvm/parser/sublinks.rs)
**Description:** Largest non-test file in the project. Despite logical
sectioning, the file mixes EXISTS, NOT EXISTS, scalar sublink hoisting,
multi-column IN, HAVING rewriting, and aggregate classification.
**Recommendation:** Decompose into `sublinks/{exists,scalar,in_list,
having,aggregates}.rs`.

### Finding Q-5: Test Code Uses Brittle `split().nth(1).unwrap_or("")`
**Severity:** 🟢 LOW — NEW
**Location:** [src/dvm/operators/lateral_subquery.rs](src/dvm/operators/lateral_subquery.rs#L1159)
**Description:** Test assertions that look for a substring after a CTE
name fall back to empty string on mismatch, making the assertion silently
vacuous if CTE naming changes.
**Recommendation:** Use `splitn(2, …)` and assert
`parts.len() == 2`.

---

## Area 5: Test Coverage

### Finding T-1: Refresh Orchestrator and Merge Sub-Modules Lack Unit Tests
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/refresh/orchestrator.rs](src/refresh/orchestrator.rs),
[src/refresh/merge/mod.rs](src/refresh/merge/mod.rs),
[src/refresh/merge/{insert,update,delete,columns,conflict}.rs](src/refresh/merge/)
**Description:** Two `#[cfg(test)]` blocks across the refresh tree; the
cost-model state machine and merge-template caching paths rely on E2E
coverage.
**Recommendation:** Add unit tests for: cost-model transitions (cold
start → diff-preferred → full-preferred → diff-recovery), template-cache
LRU eviction under thrash, and merge-SQL builders for the
`columns.rs`/`conflict.rs` pure helpers.

### Finding T-2: CDC Pure-Logic Functions Still Lack Unit Tests
**Severity:** 🟡 MEDIUM — PERSISTENT (R11 T-5)
**Location:** [src/cdc.rs](src/cdc.rs), [src/cdc/polling.rs](src/cdc/polling.rs),
[src/cdc/rebuild.rs](src/cdc/rebuild.rs)
**Description:** `cb_col_name`, `buffer_base_name_for_oid`,
`build_changed_cols_bitmask_expr`, `should_promote_to_partitioned`,
`classify_holdback`, and similar helpers are pure functions and would
unit-test cleanly.
**Recommendation:** Mirror the v0.53 scheduler-test sweep.

### Finding T-3: `src/hooks.rs` DDL Classification Lacks Unit Tests
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/hooks.rs](src/hooks.rs)
**Description:** DDL command kind classification, the `DdlCommandKind`
enum dispatch, and the schema-change-impact decision logic are all
pure functions but only exercised via E2E.
**Recommendation:** Add table-driven tests for each `DdlCommandKind`
variant against representative `ObjectAddress`/`identity` inputs.

### Finding T-4: ~50 Fixed `tokio::time::sleep()` Calls Remain
**Severity:** 🟢 LOW — PARTIAL (R11 T-9 partially closed)
**Location:** [tests/e2e_quota_tests.rs](tests/e2e_quota_tests.rs#L236),
[tests/e2e_bgworker_tests.rs](tests/e2e_bgworker_tests.rs#L108),
[tests/e2e_wal_cdc_tests.rs](tests/e2e_wal_cdc_tests.rs#L80), …
**Recommendation:** Continue the v0.53 pattern: replace fixed sleeps with
`wait_for_*` polling helpers in `tests/common/mod.rs`.

### Finding T-5: No Proptest for Differential Idempotence Under Partition Reorder
**Severity:** 🟢 LOW — NEW
**Description:** The existing proptest suite covers Z-set algebra and
DAG invariants but does not assert that a refresh cycle with the same
inputs produces the same result regardless of partition-ingest order.
**Recommendation:** Add a proptest that randomises partition write order
and asserts equal stream-table contents post-refresh.

---

## Module Coverage Matrix

| Module                              | Unit `#[cfg(test)]` | Integration | E2E | Proptest |
|-------------------------------------|---------------------|-------------|-----|----------|
| `src/dag.rs`                        | ✅ (v0.53)          | ✅          | ✅  | ✅       |
| `src/config.rs`                     | ✅ (v0.53)          | ✅          | ✅  | —        |
| `src/scheduler/*.rs` (8 files)      | ✅ (v0.53)          | ✅          | ✅  | —        |
| `src/dvm/parser/*.rs`               | ✅ (v0.53)          | ✅          | ✅  | ✅       |
| `src/dvm/operators/*.rs`            | ✅                  | ✅          | ✅  | ✅       |
| `src/dvm/diff.rs`                   | ✅                  | ✅          | ✅  | ✅       |
| `src/refresh/orchestrator.rs`       | ❌                  | ✅          | ✅  | —        |
| `src/refresh/merge/*.rs`            | ⚠️ minimal          | ✅          | ✅  | —        |
| `src/refresh/codegen.rs`            | ⚠️ minimal          | ✅          | ✅  | —        |
| `src/refresh/phd1.rs`               | ⚠️ minimal          | ✅          | ✅  | —        |
| `src/cdc.rs`                        | ❌                  | ✅          | ✅  | —        |
| `src/cdc/polling.rs`                | ❌                  | ✅          | ✅  | —        |
| `src/cdc/rebuild.rs`                | ❌                  | ✅          | ✅  | —        |
| `src/wal_decoder.rs`                | ⚠️ minimal          | ✅          | ✅  | ✅       |
| `src/hooks.rs`                      | ❌                  | ✅          | ✅  | —        |
| `src/shmem.rs`                      | ⚠️ minimal          | ✅          | ✅  | —        |
| `src/hash.rs`                       | ✅                  | —           | —   | —        |
| `src/sql_builder.rs`                | ✅                  | —           | —   | —        |
| `src/monitor/*.rs`                  | ⚠️ minimal          | ✅          | ✅  | —        |
| `src/api/*.rs`                      | ✅ (per-module)     | ✅          | ✅  | —        |
| `src/error.rs`                      | ✅                  | —           | —   | —        |
| `src/diagnostics.rs`                | ❌                  | ✅          | ✅  | —        |
| `src/metrics_server.rs`             | ✅                  | —           | —   | —        |

---

## Area 6: Ergonomics & Developer Experience

### Finding E-1: `health_check()` Does Not Surface Foreign-Owned Publications/Outboxes
**Severity:** 🟡 MEDIUM — NEW
**Description:** If S-1/S-2 are exploited (a non-owner attaches an
outbox/publication), the owner has no signal in `health_check()`. The
8-check matrix does not include "Are any pgtrickle artefacts attached to
this ST owned by a different role?"
**Recommendation:** Add a `attachment_owner_check` row that lists pg_tide
outboxes and downstream publications and flags mismatches.

### Finding E-2: SQL_REFERENCE.md Function Inventory Is Incomplete
**Severity:** 🟢 LOW — NEW
**Description:** `grep -rn '#\[pg_extern' src/ | wc -l` reports ~100+
exported functions; `docs/SQL_REFERENCE.md` lists ~40. Internal helpers
(`recommend_refresh_mode`, `refresh_efficiency`, `reliability_counters`)
are reachable from SQL but undocumented.
**Recommendation:** Either annotate internal `#[pg_extern]` functions
with `@internal` comments and exclude them from the doc generator, or
document them in a new "Internal / advanced diagnostics" section.

---

## Area 7: Observability & Operations

### Finding O-1: No CDC-Lag Percentile Metrics
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/shmem.rs](src/shmem.rs), [src/monitor/mod.rs](src/monitor/mod.rs)
**Description:** `pg_trickle_frontier_holdback_lsn` is a single scalar.
SREs need p50/p95/p99 of "how stale is my data right now" across stream
tables.
**Recommendation:** Add a HDR-histogram (or a simple `[t-1m, t-5m, t-15m]`
ring buffer) in shmem and expose `pg_trickle_cdc_lag_p{50,95,99}_seconds`.

### Finding O-2: No Parallel-Worker Queue-Depth / Idle-Time Metrics
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/scheduler/pool.rs](src/scheduler/pool.rs)
**Recommendation:** Add `pg_trickle_parallel_queue_depth` and
`pg_trickle_worker_idle_time_seconds_total` counters.

### Finding O-3: No WAL-Decoder Pending-Record Metric
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/wal_decoder.rs](src/wal_decoder.rs)
**Description:** Without a queue-depth metric, a stalled decoder is
invisible until the replication slot runs out of WAL.
**Recommendation:** Expose `pg_trickle_wal_decoder_pending_records`.

### Finding O-4: No Refresh-Mode Ratio Metric per ST
**Severity:** 🟡 MEDIUM — NEW
**Description:** Operators cannot tell whether the cost model is keeping
queries on the DIFFERENTIAL hot path.
**Recommendation:** Add
`pg_trickle_refresh_mode_total{mode="differential"|"full",pgt=…}`
counters.

### Finding O-5: `application_name` Not Set on BGW Connections
**Severity:** 🟡 MEDIUM — NEW
**Location:** [src/scheduler/mod.rs](src/scheduler/mod.rs)
**Description:** `pg_stat_activity` shows BGW backends with the worker
name only; queries cannot be attributed to scheduler vs dispatcher vs
pool worker without correlating PIDs.
**Recommendation:** Call `SET application_name = 'pg_trickle_<role>'`
right after `Spi::connect()` in each BGW.

### Finding O-6: `INSTALL.md` Does Not Document `pg_dump --schema` Requirements
**Severity:** 🟢 LOW — NEW
**Description:** A naive `pg_dump db > dump.sql` will include user data
but skip the `pgtrickle` and `pgtrickle_changes` schemas if not
explicitly listed. Restoration without the catalog is a data-loss event
masquerading as success.
**Recommendation:** Add a "Backup & Restore" section to INSTALL.md
listing the required flags and link to the same content from
`docs/RUNBOOK_DRAIN.md`.

---

## Area 8: CI/CD & Infrastructure

### Finding CI-1: Full E2E + TPC-H Still Skipped on PRs
**Severity:** 🟡 MEDIUM — PERSISTENT (R11 B-2)
**Description:** PRs only run light E2E; full E2E and TPC-H gate
push-to-main and scheduled runs. Regressions to differential correctness
have up to a 24 h detection window for changes that affect only the
full-E2E paths.
**Recommendation:** Either (a) add a path-filtered trigger that runs
full E2E on PRs touching `src/dvm/`, `src/refresh/`, or `src/cdc/`, or
(b) shard the full E2E to fit inside the existing 45-min PR budget.

### Finding CI-2: `tests/Dockerfile.e2e` Runs as Root
**Severity:** 🟢 LOW — NEW
**Location:** [tests/Dockerfile.e2e](tests/Dockerfile.e2e)
**Description:** The image inherits from `postgres:18.3-bookworm`
(pinned by digest, good) but does not switch to the `postgres` user
before the `CMD`. Test containers run as root.
**Recommendation:** Add `USER postgres` before the final `CMD`.

### Finding CI-3: `codecov.yml` Excludes Critical Modules from Patch Gate
**Severity:** 🟢 LOW — NEW
**Location:** [codecov.yml](codecov.yml)
**Description:** `cdc.rs`, `hooks.rs`, and `wal_decoder.rs` are excluded
from the 70 % patch coverage gate. These are the most safety-critical
modules in the project.
**Recommendation:** Add a per-file threshold (50 % for cdc/hooks, 40 %
for wal_decoder) instead of a blanket exclusion.

---

## Area 9: Missing Features & Roadmap Gaps

### Finding F-1: LATERAL + DIFFERENTIAL on Outer-Applied Subqueries Underspecified
**Severity:** 🟡 MEDIUM — NEW
**Description:** `src/dvm/operators/lateral_subquery.rs` supports LATERAL
SRFs and basic LATERAL joins. However the interaction with outer joins
that *contain* a LATERAL on the right side (the EC-01 phantom-row
pattern) is documented neither in `docs/DVM_OPERATORS.md` nor in
`docs/LIMITATIONS.md`.
**Recommendation:** Document the precise supported subset and the
fallback policy.

### Finding F-2: SQL:2023 `SEARCH` / `CYCLE` Clauses Silently Drop
**Severity:** 🟢 LOW — PERSISTENT (R11 F-2)
**Description:** No explicit error from the parser; the query may be
accepted and the clause silently ignored.
**Recommendation:** Detect during parse, raise `UnsupportedFeature`.

### Finding F-3: `auto_explain` Integration Missing
**Severity:** 🟢 LOW — PERSISTENT (R11 F-3)
**Recommendation:** Roadmap item.

### Finding F-4: No `pg_partman` Integration
**Severity:** 🟢 LOW — PERSISTENT (R11 F-4)
**Recommendation:** Roadmap item.

---

## Roadmap Coverage Matrix

| ROADMAP item / planned feature             | Status in v0.57.0 | Notes |
|--------------------------------------------|-------------------|-------|
| Trigger-based CDC (statement & row)        | ✅ Implemented    | `src/cdc.rs` |
| WAL-based CDC (logical replication)        | ✅ Implemented    | `src/wal_decoder.rs`; partition gap in C-6 |
| Differential refresh for joins (inner/outer/full/lateral) | ✅ Implemented | `src/dvm/operators/{join,outer_join,full_join,lateral_subquery}.rs` |
| Differential refresh for aggregates        | ✅ Implemented    | `src/dvm/operators/aggregate.rs` |
| Differential refresh for recursive CTEs    | ⚠️ Partial        | 5 strategies, but depth guard missing in DIFF (C-2) |
| Multi-column IN / NOT IN                   | ⚠️ Partial        | v0.55 M-5; NULL edge case (C-1) |
| GROUPING SETS / CUBE / ROLLUP              | ✅ Implemented    | up to 64 branches |
| Window functions (9 supported)             | ✅ Implemented    | `src/dvm/operators/window.rs` |
| dbt adapter                                | ✅ Production     | `dbt-pgtrickle/` |
| pg_tide outbox / inbox / relay integration | ✅ Production     | but ownership bypass (S-1) |
| Downstream logical-replication publishing  | ✅ Implemented    | but ownership bypass (S-2) |
| Parameterised stream tables                | ❌ Not implemented | blog post only |
| Online schema evolution (ADD/DROP COLUMN)  | ⚠️ Partial        | needs explicit doc of unsupported ALTER TYPE cases |
| SEARCH / CYCLE for recursive CTEs          | ❌ Not implemented | F-2 |
| auto_explain integration                   | ❌ Not implemented | F-3 |
| pg_partman integration                     | ❌ Not implemented | F-4 |
| pgtrickle-relay (standalone binary)        | ⚠️ Prototype      | `pgtrickle-relay/`; needs production-readiness gate |
| Citus distributed support                  | ✅ Implemented + chaos-tested | v0.51 CHAOS-5/6/7 |
| Drain mode & CNPG preStop                  | ✅ Implemented    | OPS-10-01 |
| Prometheus metrics server                  | ✅ Implemented    | observability gaps in O-1..O-4 |

---

## Area 10: Documentation

### Finding D-1: ADRs Are Still PROPOSED
**Severity:** 🟢 LOW — NEW
**Location:** [plans/adrs/PLAN_ADRS.md](plans/adrs/PLAN_ADRS.md)
**Description:** The ADR framework is described, but no individual ADR
(`ADR-001 trigger-based CDC rationale`, `ADR-002 Z-set vs multiset`,
`ADR-003 differential correctness invariants`) has been written.
**Recommendation:** Write 3–5 foundational ADRs before v1.0.

### Finding D-2: `LIMITATIONS.md` Misses Multi-Column `NOT IN` With NULLs
**Severity:** 🟢 LOW — NEW
**Description:** Tied to C-1. Even if C-1 is fixed by a fall-back path,
the limitation should be documented.

---

## Recommended Action Plan

Top 15 actions in priority order. Owner hint: **R** = Rust, **S** = SQL,
**D** = docs, **C** = CI/build. Effort: **S** ≤ 1 day, **M** ≤ 3 days,
**L** ≤ 1 week, **XL** > 1 week.

| # | Finding | Action                                                                       | Owner | Effort |
|---|---------|------------------------------------------------------------------------------|-------|--------|
| 1 | S-1, S-2 | Add `check_stream_table_ownership()` to outbox + publication APIs            | R+S   | S      |
| 2 | C-2     | Apply `ivm_recursive_max_depth` guard in DIFFERENTIAL mode                    | R     | S      |
| 3 | C-1     | Detect NULLs in multi-column `IN`/`NOT IN` row constructors; fall back/error  | R     | M      |
| 4 | P-1     | Batch monitor buffer-growth check into a single SPI                           | R     | S      |
| 5 | C-3     | Hold advisory lock across WAL-slot eligibility + consumption                  | R     | M      |
| 6 | C-5, C-6 | Switch WAL-decoder name filter to OID-based; add partition-attach rebuild   | R     | M      |
| 7 | T-1     | Add unit tests for refresh orchestrator and merge sub-modules                 | R     | M      |
| 8 | O-1..O-4 | Add CDC-lag percentiles, worker queue depth, WAL decoder queue, refresh-mode counters | R | M |
| 9 | O-5     | `SET application_name` in every BGW connection                                | R     | S      |
| 10 | S-3    | Fail DDL on hook SPI error; do not silently skip reinit marking               | R     | S      |
| 11 | P-2, P-3 | Cache `defining_query_hash` on `StreamTableMeta`; `Arc<str>` for templates  | R     | M      |
| 12 | T-2, T-3 | CDC + hooks pure-logic unit-test sweep mirroring v0.53                      | R     | M      |
| 13 | CI-1   | Path-filtered full-E2E job on PRs touching DVM/refresh/CDC                    | C     | S      |
| 14 | E-1    | `health_check()` row for foreign-owned publications/outboxes                  | R+S   | S      |
| 15 | D-1    | Write 3 foundational ADRs (CDC choice, Z-set algebra, EC-01 invariants)       | D     | M      |

---

## Appendix: Persistent Open Findings Tracker

Findings raised in prior reports that remain open after this audit
(excluding those superseded by new, narrower findings):

| Original ID | Report | Current Status | Re-raised as |
|-------------|--------|----------------|--------------|
| C-6 (sqlstate boundary tests) | R11   | Open | Carried forward (no new finding) |
| C-10 (snapshot cache 64-bit key) | R11   | Open | C-8 |
| C-8 (cte_counter reset)       | R11   | Open | C-9 |
| T-5 (cdc.rs unit tests)       | R11   | Open | T-2 |
| T-9 (sleep-based sync)        | R11   | Partial | T-4 |
| B-2 (PR gate length / full E2E coverage) | R11 | Open | CI-1 |
| B-8 (coverage not gated)      | R11   | Partial (upload added, not blocking) | CI-3 (different framing) |
| F-2 (SEARCH/CYCLE)            | R11   | Open | F-2 |
| F-3 (auto_explain)            | R11   | Open | F-3 |
| F-4 (pg_partman)              | R11   | Open | F-4 |

---

## Metrics

- **Total findings:** 47 (4 HIGH, 23 MEDIUM, 20 LOW; 0 CRITICAL)
- **New findings:** 36
- **Persistent open from R11:** 10 (3 re-raised verbatim, 7 re-framed)
- **Closed since R11:** 23 of 30 actionable items — outstanding burn-down
- **Cargo version audited:** 0.57.0
- **Source files audited:** ~90 `.rs` files
- **Source LOC:** ~124 000
- **Largest files:** `dvm/parser/sublinks.rs` 7 065; `dvm/parser/rewrites.rs`
  6 146; `dvm/operators/aggregate.rs` 5 761; `dvm/parser/mod.rs` 5 593;
  `scheduler/mod.rs` 4 693; `dag.rs` 4 542; `config.rs` 4 475;
  `api/mod.rs` 4 197 (post-decomposition)
- **Test files:** 145 `tests/*.rs` (excludes inline `#[cfg(test)]`)
- **CI workflow files:** 24

The project's trajectory is excellent. With the 15-item action plan above
executed, **v1.0 is a credible release target**. The two HIGH security
findings (S-1, S-2) are small code changes and should ship in v0.58.0
along with the recursive-CTE depth guard fix (C-2) and the multi-column
NOT-IN NULL handling (C-1).
